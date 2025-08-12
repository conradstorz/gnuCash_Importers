"""
Interactive GnuCash Bill Builder with Persistent Vendor Defaults

This script interactively builds a CSV file containing bills for import into GnuCash.
It connects to a GnuCash SQLite database using the `piecash` module to extract known
vendor names and assists the user in building each bill record through guided prompts.

Features:
- Real-time vendor name matching as the user types.
- Automatically remembers and reuses the last description and account used per vendor.
- Prompts for amount, description, account, and date.
- Outputs a GnuCash-compatible CSV file of bills.
- Saves vendor defaults persistently between runs in a JSON file.

Output format (semicolon-separated):
  ;vendor name;date;description;account;amount;1

All possible fields:
When importing bills into GnuCash, the following fields can be imported, provided in a CSV file:

id (Invoice/Bill ID)

date_opened (Date the invoice/bill was opened)

owner_id (Customer/Vendor number; mandatory)

billingid (Billing ID; optional)

notes (Invoice/Bill notes; optional)

date (Date of the entry; defaults to date_opened if blank)

desc (Description; optional)

action (Action; optional)

account (Account for the entry; mandatory)

quantity (Defaults to 1 if blank)

price (Mandatory for each row)

disc_type (Type of discount; optional)

disc_how (Discount calculation; optional)

discount (Discount amount; optional)

taxable (Taxable flag; optional)

taxincluded (Whether tax is included; optional)

tax_table (Tax table or rate; optional)

date_posted (Date posted; optional)

due_date (Due date; optional)

account_posted (Account posted to; optional)

memo_posted (Memo for posting; optional)

accu_splits (Accumulate splits; optional)
"""

# import readline  # Enables up-arrow history and editing in terminal input()
from datetime import datetime
from pathlib import Path
import csv
import sys
import os
import json
from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter
from piecash import open_book
import warnings
from sqlalchemy.exc import SAWarning

# Filter irrelevant SQLAlchemy warnings
warnings.filterwarnings("ignore", category=SAWarning, module="piecash")

# Persistent mapping of vendor -> {description, account}
VENDOR_DEFAULTS_FILE = Path("vendor_defaults.json")

# persistent set/list of prior descriptions (used for autocomplete)
DESCRIPTIONS_FILE = Path("description_memory.json")

def load_description_memory() -> list[str]:
    """
    Load previously used descriptions from JSON for cross-run autocomplete.
    Returns a list (kept unique by simple membership checks on save).
    """
    if DESCRIPTIONS_FILE.exists():
        try:
            with open(DESCRIPTIONS_FILE, "r") as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
        except Exception:
            pass
    return []

def save_description_memory(descriptions: list[str]) -> None:
    """
    Save unique descriptions list to disk for future runs.
    """
    # keep items unique while preserving order
    seen = set()
    unique = []
    for d in descriptions:
        if d and d not in seen:
            seen.add(d)
            unique.append(d)
    with open(DESCRIPTIONS_FILE, "w") as f:
        json.dump(unique, f, indent=2)

def is_valid_amount(value: str) -> bool:
    try:
        amount = float(value)
        # Ensure it's non-negative, <= 9999.99, and has at most 2 decimal places
        return (
            0 <= amount <= 9999.99
            and round(amount, 2) == amount
        )
    except ValueError:
        return False


def get_unique_output_path(base_path: Path) -> Path:
    """
    Given a base filename (e.g., bills_20250806.csv), return a Path that does not yet exist.
    If the file exists, appends _1, _2, etc. until it finds an available name.
    """
    if not base_path.exists():
        return base_path

    stem = base_path.stem
    suffix = base_path.suffix
    parent = base_path.parent

    counter = 1
    while True:
        new_name = f"{stem}_{counter}{suffix}"
        new_path = parent / new_name
        if not new_path.exists():
            return new_path
        counter += 1


def load_vendor_names(gnucash_file: Path) -> list[str]:
    """
    Load all known vendor names from a GnuCash SQLite database using piecash.

    Args:
        gnucash_file: Path to the SQLite GnuCash book.

    Returns:
        Sorted list of vendor names as strings.
    """
    with open_book(str(gnucash_file), readonly=True) as book:
        return sorted([vendor.name for vendor in book.vendors])


def match_vendor(partial: str, vendor_names: list[str]) -> str:
    """
    Return the first matching vendor that starts with the given input.
    If no match, return the typed input unchanged.
    """
    matches = [v for v in vendor_names if v.lower().startswith(partial.lower())]
    return matches[0] if matches else partial


def prompt_vendor(vendor_names: list[str]) -> str:
    """
    Prompt the user to select or type a vendor name with live fuzzy matching.
    Uses prompt_toolkit's WordCompleter for interactive input.
    """
    completer = WordCompleter(vendor_names, ignore_case=True, sentence=True, match_middle=True)
    vendor = prompt("Vendor name: ", completer=completer, complete_while_typing=True)
    return vendor.strip()


def load_vendor_defaults() -> dict:
    """
    Load previously saved vendor defaults from JSON file.

    Returns:
        Dictionary mapping vendor names to a dict of {description, account}.
    """
    if VENDOR_DEFAULTS_FILE.exists():
        with open(VENDOR_DEFAULTS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_vendor_defaults(defaults: dict):
    """
    Save vendor defaults dictionary to JSON for future use.

    Args:
        defaults: Dictionary to persist.
    """
    with open(VENDOR_DEFAULTS_FILE, "w") as f:
        json.dump(defaults, f, indent=2)


def main():
    """
    Main entry point for the interactive bill-building script.
    Prompts user for vendor name, amount, description, account, and date.
    Builds a GnuCash-compatible semicolon-separated CSV output file and updates vendor defaults.
    """
    # Check command-line arguments for the GnuCash DB path
    if len(sys.argv) < 2:
        print("Usage: python build_bills_csv.py /path/to/books.gnucash")
        sys.exit(1)

    gnucash_file = Path(sys.argv[1])
    if not gnucash_file.exists():
        print(f"Error: {gnucash_file} not found")
        sys.exit(1)

    # Load vendor names from GnuCash and any previously saved vendor defaults
    vendor_names = load_vendor_names(gnucash_file)
    vendor_defaults = load_vendor_defaults()
    # load cross-run descriptions and initialize session defaults
    description_memory = load_description_memory()
    session_default_desc = ""  # becomes "sticky" after first non-empty description
    last_date = datetime.now().strftime("%m/%d/%Y")  # default date for first entry

    # Generate default filename with today's date
    #default_name = f"bills_{datetime.now().strftime('%Y%m%d')}.csv"
    output_file = Path("raw_bills.csv")

    # If raw_bills.csv exists, rename it to a backup based on its last modified time
    if output_file.exists():
        mtime = output_file.stat().st_mtime
        timestamp = datetime.fromtimestamp(mtime).strftime("%Y%m%d_%H%M%S")
        backup_file = output_file.with_name(f"raw_bills_{timestamp}.csv")

        # Ensure uniqueness if somehow that backup name already exists
        counter = 1
        while backup_file.exists():
            backup_file = output_file.with_name(f"raw_bills_{timestamp}_{counter}.csv")
            counter += 1

        output_file.rename(backup_file)

    # Now write the new file as raw_bills.csv
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
    print(f"Writing to: {output_file}\n")

    # Open output CSV file and write header row
    with open(output_file, "w", newline="") as f:
        # Write semicolon-separated CSV instead of default comma-separated
        writer = csv.writer(f, delimiter=';')
        writer.writerow(["", "vendor name", "date", "description", "account", "amount", "1"])

        # Start interactive session
        while True:
            print("\n--- New Bill Entry ---")
            vendor = prompt_vendor(vendor_names)

            # Lookup saved defaults for this vendor if available
            prev_desc = vendor_defaults.get(vendor, {}).get("description", "")
            prev_account = vendor_defaults.get(vendor, {}).get("account", "Commissions Paid")
            #default_date = datetime.now().strftime("%m/%d/%Y")

            # Prompt for values, offering defaults when available
            amount_str = input("Amount (e.g. 1234.56): ").strip()
            while not is_valid_amount(float(amount_str) if amount_str.replace('.', '', 1).isdigit() else amount_str):
                print("❌ Must be a number with 2 decimal places, max value 9999.99.")
                amount_str = input("Amount (e.g. 1234.56): ").strip()

            amount = f"{float(amount_str):.2f}"  # Format to two decimals as string

            # NEW: description autocomplete + sticky default across entries
            desc_completer = WordCompleter(description_memory, ignore_case=True, sentence=True, match_middle=True)

            # Prefer sticky session default; if not set yet, fall back to vendor default
            desc_default_to_show = session_default_desc or prev_desc
            desc = prompt(
                        f"Description [{desc_default_to_show}]: ",
                        completer=desc_completer,
                        complete_while_typing=True
                    ).strip() or desc_default_to_show

            # Update session "sticky" default and memory
            if desc:
                session_default_desc = desc
                if desc not in description_memory:
                    description_memory.append(desc)

            account = input(f"Account [{prev_account}]: ").strip() or prev_account

            # NEW: default the date to the previously used date (sticky across entries)
            date = input(f"Date [{last_date}]: ").strip() or last_date
            last_date = date  # remember for the next row


            # Write the row to semicolon-separated CSV
            writer.writerow(["", vendor, date, desc, account, amount, "1"])

            # Update the vendor defaults for future use
            vendor_defaults[vendor] = {"description": desc, "account": account}

            # Prompt to continue or finish
            cont = input("Add another? [Y/n]: ").strip().lower()
            if cont == "n":
                break

    # Save vendor defaults after session ends
    save_vendor_defaults(vendor_defaults)
    # NEW: save description memory after session ends
    save_description_memory(description_memory)

    print(f"\n✅ Done. File written: {output_file}")
    print(f"📁 Vendor defaults saved to: {VENDOR_DEFAULTS_FILE}")


if __name__ == "__main__":
    main()
