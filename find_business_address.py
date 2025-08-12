# file: vendor_locator.py
"""

This script reads a CSV file of vendor records, enriches each record
with contact information (address, phone, email, website) retrieved
from the Google Places API, and writes the results to a new CSV file.

Usage:
    python vendor_contacts_cli.py <input.csv> [output.csv]

Arguments:
    input.csv     Path to the input CSV containing vendor rows. Must include:
                  id, company, name, addr1, addr2, addr3, addr4,
                  phone, fax, email, notes, shipname, shipaddr1,
                  shipaddr2, shipaddr3, shipaddr4, shiphone, shipfax, shipmail

    output.csv    (Optional) Path to write the enriched CSV. If omitted,
                  defaults to <input>_with_contacts.csv in the same folder.

Processing Steps:
  1. Parse command-line arguments and validate the input file.
  2. Read all rows via csv.DictReader and verify required columns.
  3. Collect unique company names and fetch their contact info via
     the Google Places API (through vendor_locator.find_vendor_contacts()).
  4. For each row, overwrite address fields (addr1, addr2, addr4) by
     splitting the formatted address into street, city/state/zip, country.
     Preserve addr3 and copy the addrX values into shipping address fields.
  5. Overwrite phone/shipment phone and email/shipment email if found.
  6. Write the enriched rows to the output CSV with the original column order.

Requirements:
    - vendor_locator.py must be in the PYTHONPATH, providing:
        def find_vendor_contacts(vendor_names: List[str]) -> Dict[str, VendorContact]
    - loguru for logging
    - python-dotenv, requests, geopy, beautifulsoup4, pydantic for vendor_locator

"""
import os
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse
import sys
import csv
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv
from pydantic import BaseModel
from geopy.geocoders import Nominatim
import requests
from bs4 import BeautifulSoup

# Load environment variables from .env if present
load_dotenv()

class VendorContact(BaseModel):
    name: str
    address: Optional[str] = None
    phone:   Optional[str] = None
    email:   Optional[str] = None
    website: Optional[str] = None

def _geocode_location(location_str: str) -> Tuple[float, float]:
    geolocator = Nominatim(user_agent="gnucash_vendor_locator")
    loc = geolocator.geocode(location_str)
    if not loc:
        raise ValueError(f"Could not geocode location '{location_str}'")
    logger.debug(f"Geocoded '{location_str}' → ({loc.latitude}, {loc.longitude})")
    return loc.latitude, loc.longitude

def _extract_email_from_website(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        link = soup.select_one('a[href^="mailto:"]')
        if link:
            return link["href"].split("mailto:")[1].split("?")[0]
    except Exception as e:
        logger.warning(f"Couldn’t scrape email from {url}: {e}")
    return None

def _clean_domain(raw_url: str) -> str:
    """
    Strip everything after the domain from a URL.
    e.g. "https://example.com/path?foo=bar" → "https://example.com"
    """
    parts = urlparse(raw_url)
    return f"{parts.scheme}://{parts.netloc}"

def find_vendor_contacts(
    vendor_names: List[str],
    location_str: Optional[str] = None,
    radius: int = 5000,
    google_api_key: Optional[str] = None
) -> Dict[str, VendorContact]:
    """
    Look up each vendor name near `location_str` (default: your current location)
    using the Google Places API, then scrape their website for an email if present.
    """
    # 1) Determine API key from multiple sources
    api_key = (
        google_api_key
        or os.getenv("GOOGLE_PLACES_API_KEY")
        or os.getenv("GOOGLE_API_KEY")
    )
    if not api_key:
        raise EnvironmentError(
            "Google Places API key not found. "
            "Set GOOGLE_PLACES_API_KEY / GOOGLE_API_KEY in your environment or pass it in."
        )

    # Geocode if no location provided
    if location_str is None:
        location_str = "New Albany, Indiana, United States"
    lat, lng = _geocode_location(location_str)

    base_textsearch = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    base_details    = "https://maps.googleapis.com/maps/api/place/details/json"

    results: Dict[str, VendorContact] = {}

    for name in vendor_names:
        logger.info(f"Searching for '{name}' near {lat:.4f},{lng:.4f}")
        try:
            # Text Search to get a place_id
            resp = requests.get(base_textsearch, params={
                "query":    name,
                "location": f"{lat},{lng}",
                "radius":   radius,
                "key":      api_key,
            })
            resp.raise_for_status()
            items = resp.json().get("results", [])
            if not items:
                logger.warning(f"No Google Place found for '{name}'")
                results[name] = VendorContact(name=name)
                continue

            place_id = items[0]["place_id"]

            # Place Details for address / phone / website
            det = requests.get(base_details, params={
                "place_id": place_id,
                "fields":   "formatted_address,formatted_phone_number,website",
                "key":      api_key,
            }).json().get("result", {})

            raw_site = det.get("website")
            clean_site = _clean_domain(raw_site) if raw_site else None

            contact = VendorContact(
                name    = name,
                address = det.get("formatted_address"),
                phone   = det.get("formatted_phone_number"),
                website = clean_site,
            )

            # If we got a website, try scraping for an email
            if contact.website:
                contact.email = _extract_email_from_website(contact.website)

            results[name] = contact

        except Exception as e:
            logger.error(f"Error finding '{name}': {e}")
            results[name] = VendorContact(name=name)

    return results

def _parse_address_components(address: str) -> dict:
    """
    Split a Google-formatted address string into components:
      - addr1: the street portion (everything before the first comma).
      - addr2: the middle portion (city, state, zip) if present.
      - addr4: the final portion (country) if present.
    If the address has fewer than 2 commas, street is the full string,
    and addr2/addr4 may be empty.
    """
    # Split on commas and trim whitespace
    parts = [part.strip() for part in address.split(',')]
    if len(parts) >= 2:
        street = parts[0]                 # first segment → street
        country = parts[-1]               # last segment → country
        middle = ', '.join(parts[1:-1])   # everything in between → city/state/zip
    else:
        # Fallback: treat entire address as street if no comma found
        street = address
        middle = ''
        country = ''
    return {'addr1': street, 'addr2': middle, 'addr4': country}


def process_vendor_csv():
    """
    Main entry point: validate args, read input CSV, enrich each row,
    and write to output CSV.

    Ensures the input CSV exists and has the required columns, then
    performs a batch lookup for unique company names, and updates
    address and contact fields on each row.
    """
    # 1) Parse and validate command-line arguments
    if len(sys.argv) < 2:
        logger.error("Usage: python vendor_contacts_cli.py <input.csv> [output.csv]")
        sys.exit(1)

    input_path = Path(sys.argv[1])
    # Ensure the path exists and is a CSV file
    if not input_path.is_file() or input_path.suffix.lower() != '.csv':
        logger.error(f"Input file {input_path} does not exist or is not a CSV")
        sys.exit(1)

    # Determine output CSV path: explicit or default naming
    if len(sys.argv) >= 3:
        output_path = Path(sys.argv[2])
    else:
        # Default: same folder, append _with_contacts before extension
        output_path = input_path.with_name(
            f"{input_path.stem}_with_contacts{input_path.suffix}"
        )

    # 2) Read the input CSV into memory (semicolon-separated only)
    with input_path.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')  # enforce semicolon input
        fieldnames = reader.fieldnames or []
        # Define the required set of fields to operate correctly
        required_cols = {
            'company', 'addr1', 'addr2', 'addr3', 'addr4',
            'shipaddr1', 'shipaddr2', 'shipaddr3', 'shipaddr4',
            'phone', 'shiphone', 'email', 'shipmail'
        }
        # Abort if any required column is missing
        if not required_cols.issubset(set(fieldnames)):
            logger.error(
                f"Input CSV must contain columns: {sorted(required_cols)}"
            )
            sys.exit(1)
        # Load all rows for post-processing
        rows = list(reader)

    # 3) Aggregate unique company names for a single Places API batch lookup
    company_names = sorted({row['company'] for row in rows if row.get('company')})
    contacts = find_vendor_contacts(company_names)

    # 4) Write enriched data using semicolon-separated output
    out_fieldnames = fieldnames
    with output_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=out_fieldnames, delimiter=';')  # enforce semicolon output
        writer.writeheader()

        # 5) Process each row: overwrite address and contact fields if available
        for row in rows:
            name = row.get('company')
            contact = contacts.get(name)
            if contact and contact.address:
                # Parse the formatted address into components
                comps = _parse_address_components(contact.address)
                row['addr1'] = comps['addr1']
                row['addr2'] = comps['addr2']
                row['addr4'] = comps['addr4']
                # Duplicate into shipping address fields
                row['shipaddr1'] = comps['addr1']
                row['shipaddr2'] = comps['addr2']
                row['shipaddr3'] = row.get('addr3', '')  # preserve original addr3
                row['shipaddr4'] = comps['addr4']
            if contact:
                # Overwrite phone/email if found, otherwise keep existing
                row['phone']    = contact.phone   or row.get('phone', '')
                row['shiphone'] = contact.phone   or row.get('shiphone', '')
                row['email']    = contact.email   or row.get('email', '')
                row['shipmail'] = contact.email   or row.get('shipmail', '')

            # Write the enriched row to the output file
            writer.writerow(row)

    # 6) Log completion and output path for user confirmation
    logger.info(f"Wrote enriched CSV to {output_path}")


if __name__ == "__main__":
    # Example usage
    process_vendor_csv()

# This code is part of the Automate The Repetitive project
# It automates the process of finding vendor contact information
