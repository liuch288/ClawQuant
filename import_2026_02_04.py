"""Import tick data from ~/data/raw/202602, 202603, 202604 (daily zip format) into futures_db."""

import sys
import zipfile
import tempfile
import shutil
import os
from pathlib import Path

# Set data path BEFORE importing futures_db
os.environ["FUTURESDB_PATH"] = str(Path.home() / "data" / "futures_db")

sys.path.insert(0, str(Path(__file__).parent / "futures_db"))

from data_loader import load_tick_data
from futures_db import FuturesDB


DATA_ROOT = Path.home() / "data" / "raw"
MONTHS = ["202602", "202603", "202604"]


def zip_date_to_iso(zip_name: str) -> str:
    """Extract date from zip filename like '20260202.zip' -> '2026-02-02'"""
    date_raw = zip_name.stem  # "20260202"
    return f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"


def symbol_from_csv(csv_name: str) -> str:
    """Extract symbol from filename like 'a2603_20260202.csv' -> 'a2603'."""
    return csv_name.rsplit("_", 1)[0]


def tick_exists(symbol: str, date_str: str) -> bool:
    """Check if tick pkl already exists."""
    from futures_db.config import DEFAULT_DATA_PATH
    db_path = Path(os.environ.get("FUTURESDB_PATH", DEFAULT_DATA_PATH))
    return (db_path / "tick" / symbol / f"{date_str}.pkl").exists()


def import_one_zip(zip_path: Path) -> list:
    """Import all CSV files from a single zip archive."""
    date_str = zip_date_to_iso(zip_path)
    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(tmpdir)

        tmppath = Path(tmpdir)
        for csv_file in sorted(tmppath.glob("*_*.csv")):
            name = csv_file.stem  # e.g. "a2603_20260202"

            # Skip non-ASCII (主力/连续合约)
            if not name.isascii():
                continue

            parts = name.rsplit("_", 1)
            if len(parts) != 2 or not parts[1].isdigit():
                continue

            symbol = symbol_from_csv(csv_file.name)

            if tick_exists(symbol, date_str):
                results.append(f"  skip {symbol} {date_str} (exists)")
                continue

            try:
                df = load_tick_data(str(csv_file))
                actual_symbol = df["sym"].iloc[0]
                db = FuturesDB()
                db.save_tick(df, symbol=actual_symbol, date=date_str)
                results.append(f"  saved {actual_symbol} {date_str} ({len(df)} rows)")
            except Exception as e:
                results.append(f"  ERROR {csv_file.name}: {e}")

    return results


def main():
    db = FuturesDB()
    total_zip = 0
    total_csv = 0

    for month in MONTHS:
        month_dir = DATA_ROOT / month
        if not month_dir.is_dir():
            print(f"Skipping {month} (not found)")
            continue

        zip_files = sorted(month_dir.glob("*.zip"))
        print(f"\n=== {month} ({len(zip_files)} zip files) ===")

        for zip_path in zip_files:
            results = import_one_zip(zip_path)
            total_zip += 1
            for r in results:
                print(r)
                if "saved" in r:
                    total_csv += 1

    print(f"\nDone. {total_zip} zips processed, {total_csv} files imported.")


if __name__ == "__main__":
    main()
