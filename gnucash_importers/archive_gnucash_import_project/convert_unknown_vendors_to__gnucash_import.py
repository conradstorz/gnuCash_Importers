import csv
import sys
import os
import requests
import time
from pytimedinput import timedInput
from dotenv import load_dotenv

import warnings
from sqlalchemy.exc import SAWarning
# Filter out specific warnings from piecash
# This is to avoid cluttering the output with known warnings that are not relevant to this script
warnings.filterwarnings("ignore", category=SAWarning, module="piecash")

# Load environment variables from .env file if present
load_dotenv()
API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

VENDOR_FIELDS = [
    "id", "company", "name", "addr1", "addr2", "addr3", "addr4",
    "phone", "fax", "email", "notes", "shipname", "shipaddr1",
    "shipaddr2", "shipaddr3", "shipaddr4", "shiphone", "shipfax", "shipmail"
]

INPUT_TIMEOUT = 30  # seconds
PLACES_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"


def find_default_input_file():
    for fname in os.listdir('.'):
        if fname.startswith("unknown_vendors_") and fname.endswith(".csv"):
            return fname
    return None


def read_names(input_filename):
    names = []
    with open(input_filename, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        name_index = header.index('name') if 'name' in header else 0
        for row in reader:
            if len(row) > name_index and row[name_index].strip():
                names.append(row[name_index].strip())
    return names


def create_vendor_records(company_names):
    records = []
    for company in company_names:
        record = {field: "" for field in VENDOR_FIELDS}
        record["id"] = ""
        record["company"] = company
        record["name"] = "unknown contact"
        records.append(record)
    return records


def google_places_search(company_name, max_results=5):
    if not API_KEY:
        raise RuntimeError("Google Places API key not available.")
    params = {
        "query": company_name,
        "key": API_KEY
    }
    try:
        response = requests.get(PLACES_SEARCH_URL, params=params, timeout=10)
        response.raise_for_status()
        results = response.json().get("results", [])
        return results[:max_results]
    except Exception as e:
        raise RuntimeError(f"Error searching Google Places for '{company_name}': {e}")


def google_places_details(place_id):
    if not API_KEY:
        raise RuntimeError("Google Places API key not available.")
    params = {
        "place_id": place_id,
        "fields": "formatted_phone_number,formatted_address,website,opening_hours",
        "key": API_KEY
    }
    try:
        response = requests.get(PLACES_DETAILS_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json().get("result", {})
    except Exception as e:
        raise RuntimeError(f"Error fetching details for place ID {place_id}: {e}")


def enrich_vendor_record(record):
    company = record["company"]
    print(f"\nSearching Google Places for: '{company}'")
    try:
        places = google_places_search(company)
    except Exception as e:
        print(f"Warning: {e}")
        record["notes"] += " / Google Places enrichment skipped due to error."
        return record

    if not places:
        print("No results found.")
        record["notes"] += " / No Google Places matches found."
        return record

    print("Top results:")
    for idx, place in enumerate(places, 1):
        print(f"{idx}. {place.get('name')} - {place.get('formatted_address')}")

    print("0. Skip enrichment for this vendor")

    while True:
        selection, timed_out = timedInput(
            f"Select match number 1-{len(places)} or 0 to skip (30s timeout): ",
            timeout=INPUT_TIMEOUT,
        )
        if timed_out:
            print("Input timed out; skipping enrichment for this vendor.")
            return record
        if not selection.isdigit():
            print("Please enter a valid number.")
            continue
        selection_num = int(selection)
        if 0 <= selection_num <= len(places):
            break
        else:
            print("Number out of range.")

    if selection_num == 0:
        print("Skipping enrichment for this vendor.")
        return record

    chosen_place = places[selection_num - 1]
    place_id = chosen_place.get("place_id")

    try:
        details = google_places_details(place_id)
    except Exception as e:
        print(f"Warning: {e}")
        record["notes"] += " / Google Places details fetch skipped due to error."
        return record

    # Update record fields using details
    record["addr1"] = details.get("formatted_address", chosen_place.get("formatted_address", ""))
    record["phone"] = details.get("formatted_phone_number", "")
    if details.get("website"):
        record["notes"] += f" Website: {details['website']}"
    record["notes"] += " / Enriched from Google Places."
    print("Record enriched with selected place details.")
    return record


def web_enrich_vendor_data(vendor_records):
    if not API_KEY:
        print("Google Places API key not found; skipping enrichment.\n")
        return vendor_records

    proceed_input, timed_out = timedInput(
        "Attempt to enrich vendor data using Google Places API? (y/n): ", timeout=INPUT_TIMEOUT
    )
    if timed_out or not proceed_input.lower().startswith("y"):
        print("Skipping enrichment.\n")
        return vendor_records

    for record in vendor_records:
        record = enrich_vendor_record(record)
        time.sleep(2)  # polite delay to avoid rate limit

    print("Google Places enrichment done.\n")
    return vendor_records


def user_modify_records(vendor_records):
    print(
        f"Please review and optionally modify vendor details (except 'id'). You have {INPUT_TIMEOUT} seconds for each field. Press Enter to keep existing value.\n"
    )
    global_timeout = False
    fields_to_edit = [f for f in VENDOR_FIELDS if f != "id"]

    for i, record in enumerate(vendor_records, start=1):
        if global_timeout:
            break
        changes_made = False
        print(f"\nVendor #{i}:")
        for field in fields_to_edit:
            current_value = record[field]
            prompt_text = f"  {field} [{current_value}]: "
            user_input, timed_out = timedInput(prompt_text, timeout=INPUT_TIMEOUT)
            if timed_out:
                print("\nInput timed out. All further prompts are skipped for the rest of the script.\n")
                global_timeout = True
                break
            elif user_input.strip() != "":
                record[field] = user_input.strip()
                changes_made = True
        if global_timeout:
            break
        if changes_made:
            print("\nYou modified this vendor to:")
            for field in fields_to_edit:
                print(f"  {field}: {record[field]}")
            confirm_input, timed_out = timedInput(
                "Confirm changes? (y/n, 30 seconds to respond): ", timeout=INPUT_TIMEOUT
            )
            if timed_out:
                print("No response to confirmation. Skipping all further input.")
                global_timeout = True
                break
            elif not confirm_input.lower().startswith("y"):
                print("Let's modify the details again.")
                return user_modify_records(vendor_records)
    return vendor_records


def write_output_csv(output_filename, vendor_records):
    with open(output_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=VENDOR_FIELDS)
        writer.writeheader()
        writer.writerows(vendor_records)


def main():
    if len(sys.argv) > 1:
        input_filename = sys.argv[1]
        if not os.path.isfile(input_filename):
            print(f"Provided input file '{input_filename}' does not exist.")
            sys.exit(1)
    else:
        input_filename = find_default_input_file()
        if not input_filename:
            print(
                "No input file found beginning with 'unknown_vendors_' in the current directory."
            )
            sys.exit(1)

    output_filename = "gnucash_vendors.csv"
    print(f"Using input file: {input_filename}")

    company_names = read_names(input_filename)
    vendor_records = create_vendor_records(company_names)

    vendor_records = web_enrich_vendor_data(vendor_records)

    vendor_records = user_modify_records(vendor_records)

    write_output_csv(output_filename, vendor_records)
    print(f"\nFinal vendor import CSV saved to '{output_filename}'.")


if __name__ == "__main__":
    main()
