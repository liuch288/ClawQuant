"""Import tick data from ~/data/raw/202602 into futures_db."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "futures_db"))

from data_loader import load_tick_data
from futures_db import FuturesDB


DATA_ROOT = Path.home() / "data" / "raw" / "202602"


def main():
    db = FuturesDB()
    total = 0

    # CSV files are flat in DATA_ROOT with naming: {symbol}_{YYYYMMDD}.csv
    for csv_file in sorted(DATA_ROOT.glob("*_*.csv")):
        name = csv_file.stem  # e.g. "a2603_20260202"

        # Skip files with non-ASCII chars (主力合约)
        if not name.isascii():
            continue

        parts = name.rsplit("_", 1)
        if len(parts) != 2 or not parts[1].isdigit():
            continue

        date_raw = parts[1]  # "20260202"
        date_str = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"

        df = load_tick_data(str(csv_file))
        symbol = df["sym"].iloc[0]
        db.save_tick(df, symbol=symbol, date=date_str)
        total += 1
        print(f"  saved {symbol} {date_str} ({len(df)} rows)")

    print(f"\nDone. {total} files imported.")


if __name__ == "__main__":
    main()
