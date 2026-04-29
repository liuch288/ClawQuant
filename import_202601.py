"""Import tick data from ~/data/202601 into futures_db."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "futures_db"))

from data_loader import load_tick_data
from futures_db import FuturesDB


DATA_ROOT = Path.home() / "data" / "202601"
def date_folder_to_iso(folder_name: str) -> str:
    """Convert '20260105' to '2026-01-05'."""
    return f"{folder_name[:4]}-{folder_name[4:6]}-{folder_name[6:8]}"


def main():
    db = FuturesDB()
    total = 0

    for day_folder in sorted(DATA_ROOT.iterdir()):
        if not day_folder.is_dir() or not day_folder.name.isdigit():
            continue

        date_str = date_folder_to_iso(day_folder.name)
        print(f"Processing {date_str} ...")

        for csv_file in sorted(day_folder.glob("*_*.csv")):
            if "主力" in csv_file.name:
                continue

            df = load_tick_data(str(csv_file))
            symbol = df["sym"].iloc[0]
            db.save_tick(df, symbol=symbol, date=date_str)
            total += 1
            print(f"  saved {symbol} ({len(df)} rows)")

    print(f"\nDone. {total} files imported.")


if __name__ == "__main__":
    main()
