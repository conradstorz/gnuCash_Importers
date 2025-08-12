#!/usr/bin/env python3
"""
csv_2_qif_for_gnucash.py

Scan for CSVs (or accept one via -i), prompt for or accept on the CLI a starting
check number (-s), optionally specify a QIF account name (-a), and write a QIF
file for import into GnuCash.

This version uses:
  - Today's system date for every check.
  - 'Commission' column as the amount.
  - 'Location' column as the payee.
  - A fixed memo: "ATM sales commission".
  - A fixed category/account: "Sales Commission Paid".
  - An optional QIF account declaration header, defaulting to "Checking Account Name".

CSV columns (header required):
  - Commission (decimal, positive)
  - Location   (string)
"""
import glob
import csv
import argparse
import sys
from datetime import date

# Fixed settings
AMOUNT_COL = "Commission"
PAYEE_COL = "Location"
MEMO       = "ATM sales commission"
CATEGORY   = "Sales Commission Paid"
DEFAULT_ACCOUNT = "BillPay Account **6241"


def format_today_qif_date() -> str:
    """Return today's date in QIF format MM/DD/YYYY."""
    return date.today().strftime('%m/%d/%Y')


def select_csv_file() -> str:
    """List all CSVs in cwd and let the user pick one."""
    csv_files = sorted(glob.glob('*.csv'))
    if not csv_files:
        print("No CSV files found in the current directory.", file=sys.stderr)
        sys.exit(1)
    if len(csv_files) == 1:
        print(f"Found one CSV: {csv_files[0]}")
        return csv_files[0]
    print("Multiple CSV files found:")
    for idx, fname in enumerate(csv_files, start=1):
        print(f"  {idx}. {fname}")
    while True:
        choice = input(f"Select a file [1–{len(csv_files)}]: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(csv_files):
            return csv_files[int(choice) - 1]
        print("Invalid selection; try again.")


def prompt_start_number() -> int:
    """Ask the user for a starting check number."""
    while True:
        try:
            val = input("Enter starting check number: ").strip()
            return int(val)
        except ValueError:
            print("Number must be an integer.")


def generate_qif(input_csv: str,
                 output_qif: str,
                 start_num: int,
                 account_name: str):
    """Read checks from CSV and write QIF with fixed settings and optional account header."""
    today_qif = format_today_qif_date()
    with open(input_csv, newline='', encoding='utf-8') as csvfile, \
         open(output_qif, 'w', encoding='utf-8') as qif:

        reader = csv.DictReader(csvfile)
        # Validate required columns
        missing = [c for c in (AMOUNT_COL, PAYEE_COL) if c not in reader.fieldnames]
        if missing:
            print(f"Error: CSV is missing required columns: {missing}", file=sys.stderr)
            print("Available columns:", reader.fieldnames, file=sys.stderr)
            sys.exit(1)

        # Optional QIF account declaration
        qif.write('!Account\n')
        qif.write(f'N{account_name}\n')
        qif.write('TBank\n')
        qif.write('^\n')

        # Transactions header
        qif.write('!Type:Bank\n')

        check_num = start_num
        for row in reader:
            raw_amt   = row[AMOUNT_COL]
            raw_payee = row[PAYEE_COL]

            # Parse amount
            try:
                amount = -abs(float(raw_amt))
            except ValueError:
                print(f"Bad amount '{raw_amt}' in row {reader.line_num}", file=sys.stderr)
                sys.exit(1)

            # Write QIF record
            qif.write(f'D{today_qif}\n')
            qif.write(f'N{check_num}\n')
            qif.write(f'T{amount:.2f}\n')
            qif.write(f'P{raw_payee}\n')
            qif.write(f'M{MEMO}\n')
            qif.write(f'L{CATEGORY}\n')
            qif.write('^\n')
            check_num += 1

    print(f"✓ Wrote {output_qif} to account '{account_name}' with checks dated {today_qif}, starting at {start_num}")


def main():
    p = argparse.ArgumentParser(
        description="Generate a QIF of ATM commission checks for GnuCash."
    )
    p.add_argument('-i', '--input-csv',
                   help="Path to your checks CSV file (scan cwd if omitted)")
    p.add_argument('-s', '--start-number', type=int,
                   help="Starting check number (prompt if omitted)")
    p.add_argument('-a', '--account-name', default=DEFAULT_ACCOUNT,
                   help=f"QIF account name (default: '{DEFAULT_ACCOUNT}')")
    p.add_argument('-o', '--output-qif', required=True,
                   help="Desired output .qif file path")
    args = p.parse_args()

    csv_path     = args.input_csv or select_csv_file()
    start_num    = args.start_number if args.start_number is not None else prompt_start_number()
    acct_name    = args.account_name

    generate_qif(
        input_csv   = csv_path,
        output_qif  = args.output_qif,
        start_num   = start_num,
        account_name= acct_name
    )

if __name__ == '__main__':
    main()
