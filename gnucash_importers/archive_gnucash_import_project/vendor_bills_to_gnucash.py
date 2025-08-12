"""
GnuCash Bill Import Preprocessor (piecash-powered)

This script prepares a robust, fault-tolerant set of CSV files for importing bills into GnuCash.
It ingests a loosely formatted input CSV (default: "raw_bills.csv") and attempts to:

1. Identify vendor names, account names, and bill line details.
2. Match vendors and accounts by querying the GnuCash database directly using the `piecash` module.
   - Uses a default GnuCash file path unless overridden via --db-path.
   - Filters vendors by name to extract their internal ID.
   - Checks that accounts exist by full account name.
   - Fails gracefully if the GnuCash file is XML-based, with guidance to convert to SQLite.
3. For each input line:
   - If vendor is missing, the row is discarded with a log warning.
   - If account is unknown, it is replaced with a fallback account: "MISC EXPENSE".
   - All valid lines are output to `bills_<date>.csv`
   - Unknown vendors/accounts are listed in their respective importable CSVs.

CSV Output:
- Always includes 3 files, even if empty:
  - bills_YYYY-MM-DD.csv
  - unknown_vendors_YYYY-MM-DD.csv
  - unknown_accounts_YYYY-MM-DD.csv

Usage:
    python bill_importer.py [--db-path PATH] [--input PATH]

Dependencies:
- piecash
- loguru

If using an XML-based GnuCash file, convert to SQLite:
1. Open GnuCash.
2. File → Save As… → select SQLite format and choose a .gnucash filename.
3. Use that file with this tool.

Documentation:
https://www.gnucash.org/docs/v5/C/gnucash-guide/busnss-imp-bills-invoices.html
"""

import csv
import argparse
from pathlib import Path
from datetime import date
from typing import Dict, List
from loguru import logger
from piecash import open_book
import warnings
from sqlalchemy.exc import SAWarning
# Filter out specific warnings from piecash
# This is to avoid cluttering the output with known warnings that are not relevant to this script
warnings.filterwarnings("ignore", category=SAWarning, module="piecash")


# --- Defaults and Constants ---
DEFAULT_DB_PATH = "D:/QuickBooks/gnuCash/GnuCash_Company_Files/SPMLLC_2025_sqlite3.gnucash"
DEFAULT_INPUT = "raw_bills.csv"
DEFAULT_ACCOUNT = "MISC EXPENSE"
TODAY = date.today().isoformat()

BILL_FIELDS = [
    "id", "date_opened", "owner_id", "billingid", "notes", "date", "desc", "action",
    "account", "quantity", "price", "disc_type", "disc_how", "discount",
    "taxable", "taxincluded", "tax_table", "date_posted", "due_date",
    "account_posted", "memo_posted", "accu_splits"
]

# Output filenames
BILLS_OUT = f"bills_{TODAY}.csv"
UNKNOWN_VENDORS_OUT = f"unknown_vendors_{TODAY}.csv"
UNKNOWN_ACCOUNTS_OUT = f"unknown_accounts_{TODAY}.csv"


def load_gnucash_data(db_path: str):
    """
    Load vendor and account data from a GnuCash book.

    :param db_path: Path to the GnuCash database file.
    :return: Tuple of (vendor_lookup, account_lookup)
    :raises RuntimeError: If the file appears to be XML or unsupported.
    """
    # Check if the file appears to be XML
    try:
        with open(db_path, "rb") as f:
            head = f.read(64).lower()
            if b"<?xml" in head or b"<gnucash" in head:
                logger.error("This appears to be an XML-based GnuCash file, which is not supported.")
                logger.error("To fix this: open your file in GnuCash and choose File → Save As… → SQLite.")
                raise RuntimeError("XML-based GnuCash file detected. Please convert to SQLite.")
    except Exception as check_err:
        logger.warning(f"Unable to read file signature for XML check: {check_err}")

    vendor_lookup = {}
    account_lookup = {}

    try:
        with open_book(db_path, readonly=True) as book:
            for vendor in book.vendors:
                vendor_lookup[vendor.name.strip().lower()] = vendor.guid

            for acct in book.accounts:
                full_name = acct.fullname.strip()
                account_lookup[full_name.lower()] = full_name
    except Exception as e:
        logger.error(f"Failed to open GnuCash database: {e}")
        raise RuntimeError("Unable to load GnuCash book. Ensure the file is SQLite or a valid database URI.") from e

    return vendor_lookup, account_lookup


def write_csv(filename: str, fieldnames: List[str], rows: List[dict]) -> None:
    """
    Write a list of dictionaries to a CSV file with the given field order.

    :param filename: Path to the output file.
    :param fieldnames: Ordered list of CSV headers.
    :param rows: List of dictionaries representing rows.
    """
    with open(filename, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def print_summary(bills: list, unknown_vendors: dict, unknown_accounts: dict) -> None:
    """
    Print summary statistics of what was processed.

    :param bills: List of valid or partial bill rows.
    :param unknown_vendors: Dict of unknown vendor entries.
    :param unknown_accounts: Dict of unknown account entries.
    """
    logger.info(f"Bills written: {len(bills)}")
    logger.info(f"Unknown vendors: {len(unknown_vendors)} -> {UNKNOWN_VENDORS_OUT}")
    logger.info(f"Unknown accounts: {len(unknown_accounts)} -> {UNKNOWN_ACCOUNTS_OUT}")
    logger.info(f"Output CSV: {BILLS_OUT}")


def process_raw_bills(db_path: str, input_path: str):
    """
    Process a raw bill CSV and output GnuCash-compatible files.

    :param db_path: GnuCash file path.
    :param input_path: Raw bill CSV input file.
    """

    try:
        vendor_lookup, account_lookup = load_gnucash_data(db_path)
    except RuntimeError as e:
        logger.error(f"Error loading GnuCash data: {e}")
        return
    logger.info("GnuCash data loaded successfully.")
    logger.info(f"Vendor count: {len(vendor_lookup)}")
    logger.info(f"Account count: {len(account_lookup)}")

    logger.info(f"Processing input file: {input_path}")
    bills = []
    unknown_vendors = {}
    unknown_accounts = {}

    with open(input_path, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vendor_name = row.get("vendor", "").strip()
            if not vendor_name:
                logger.warning(f"Skipping row with missing vendor: {row}")
                continue

            account_name = row.get("account", "").strip()
            vendor_key = vendor_name.lower()
            account_key = account_name.lower()

            matched_vendor_id = vendor_lookup.get(vendor_key)
            matched_account = account_lookup.get(account_key)

            if not matched_vendor_id:
                unknown_vendors[vendor_key] = {"name": vendor_name}

            if not matched_account:
                unknown_accounts[account_key] = {"name": account_name}
                matched_account = DEFAULT_ACCOUNT

            bill_row = {
                "id": row.get("bill_id", "").strip(),
                "date_opened": row.get("date", "").strip(),
                "owner_id": matched_vendor_id if matched_vendor_id else vendor_name,
                "billingid": "",
                "notes": "",
                "date": row.get("date", "").strip(),
                "desc": row.get("description", "").strip(),
                "action": "",
                "account": matched_account,
                "quantity": row.get("quantity", "1"),
                "price": row.get("amount", "0"),
                "disc_type": "",
                "disc_how": "",
                "discount": "",
                "taxable": "",
                "taxincluded": "",
                "tax_table": "",
                "date_posted": "",
                "due_date": "",
                "account_posted": "",
                "memo_posted": "",
                "accu_splits": ""
            }

            bills.append(bill_row)

    write_csv(BILLS_OUT, BILL_FIELDS, bills)
    write_csv(UNKNOWN_VENDORS_OUT, ["name"], list(unknown_vendors.values()))
    write_csv(UNKNOWN_ACCOUNTS_OUT, ["name"], list(unknown_accounts.values()))
    print_summary(bills, unknown_vendors, unknown_accounts)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GnuCash Bill Import Preprocessor")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="Path to GnuCash file")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Input CSV with raw bills")
    args = parser.parse_args()

    logger.add("bills_processing.log", rotation="500 KB")
    # logger.info(f"Loading from DB: {args.db_path}")
    # logger.info(f"Input file: {args.input}")
    process_raw_bills(args.db_path, args.input)
