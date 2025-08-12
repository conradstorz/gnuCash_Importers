import csv
import argparse
from pathlib import Path
from datetime import datetime
from loguru import logger

try:
    from piecash import open_book
except ImportError:
    open_book = None  # Will check later if DB is used

import warnings
from sqlalchemy.exc import SAWarning
# Filter out specific warnings from piecash
# This is to avoid cluttering the output with known warnings that are not relevant to this script
warnings.filterwarnings("ignore", category=SAWarning, module="piecash")    

DEFAULT_INPUT_PREFIX = "unknown_accounts_"
DEFAULT_PARENT_NAME = "Expenses"

def get_default_input_file() -> Path | None:
    matches = sorted(Path.cwd().glob(f"{DEFAULT_INPUT_PREFIX}*"))
    if len(matches) == 1:
        return matches[0]
    elif len(matches) == 0:
        logger.warning(f"No input files starting with '{DEFAULT_INPUT_PREFIX}' found.")
    else:
        logger.warning(f"Multiple input files starting with '{DEFAULT_INPUT_PREFIX}' found. Use --input to specify one.")
    return None

def find_default_gnucash_db() -> Path | None:
    files = list(Path.cwd().glob("*.gnucash"))
    if len(files) == 1:
        return files[0]
    elif len(files) == 0:
        logger.warning("No .gnucash files found in current directory.")
    else:
        logger.warning("Multiple .gnucash files found. Use --db to specify one.")
    return None

def get_output_file() -> Path:
    today = datetime.now().strftime("%Y%m%d")
    return Path(f"import_expense_accounts_{today}.csv")

def load_input_file(input_path: Path) -> list[str]:
    with input_path.open(newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        return [row[0].strip() for row in reader if row and row[0].strip()]

def read_existing_expense_accounts(db_path: Path) -> set[str] | None:
    if not open_book:
        logger.warning("piecash module not available. Proceeding without database check.")
        return None
    try:
        with open_book(db_path, readonly=True) as book:
            root = book.root_account
            parent = next((a for a in root.descendants if a.name == DEFAULT_PARENT_NAME and a.type.name == "EXPENSE"), None)
            if not parent:
                logger.warning(f"Parent account '{DEFAULT_PARENT_NAME}' not found. Proceeding as if all accounts are new.")
                return None
            return {a.name for a in parent.children}
    except Exception as e:
        logger.warning(f"Could not read database ({db_path}): {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Generate GnuCash-compatible import CSV for new expense accounts.")
    parser.add_argument("--input", type=Path, default=get_default_input_file(),
                        help="Input CSV with account names (default: unknown_accounts_YYYYMMDD.csv)")
    parser.add_argument("--db", type=Path, help="Path to GnuCash .gnucash SQLite database (default: auto-detect)")
    args = parser.parse_args()

    input_path = args.input or get_default_input_file()
    if not input_path:
        logger.error("No valid input file could be determined.")
        return
    
    db_path = args.db or find_default_gnucash_db()
    output_path = get_output_file()

    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return

    input_account_names = load_input_file(input_path)
    logger.info(f"Loaded {len(input_account_names)} account names from {input_path}")

    existing_account_names = read_existing_expense_accounts(db_path) if db_path else None

    if existing_account_names is not None:
        new_accounts = [name for name in input_account_names if name not in existing_account_names]
        logger.info(f"{len(new_accounts)} new accounts to generate (after checking existing).")
    else:
        new_accounts = input_account_names
        logger.info(f"No DB check performed. Assuming all {len(new_accounts)} accounts are new.")

    if not new_accounts:
        logger.info("No new accounts to generate.")
        return

    with output_path.open("w", newline='', encoding='utf-8') as out_f:
        writer = csv.writer(out_f)
        writer.writerow(["Name", "Type", "Parent Account", "Description"])
        for name in new_accounts:
            writer.writerow([name, "EXPENSE", DEFAULT_PARENT_NAME, f"Imported account for {name}"])

    logger.success(f"Wrote {len(new_accounts)} accounts to: {output_path}")

if __name__ == "__main__":
    main()
