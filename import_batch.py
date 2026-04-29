"""Batch import tick data from ~/data/raw/2025/ into futures_db using multiprocessing."""

import sys
from pathlib import Path
from multiprocessing import Pool, cpu_count

sys.path.insert(0, str(Path(__file__).parent / "futures_db"))

from data_loader import load_tick_data
from futures_db import FuturesDB
from futures_db.config import DEFAULT_DATA_PATH

DATA_BASE = Path.home() / "data" / "raw" / "2025"
MONTHS = ["202501", "202502", "202503", "202504", "202505"]
DB_BASE = Path(DEFAULT_DATA_PATH)


def date_folder_to_iso(folder_name: str) -> str:
    return f"{folder_name[:4]}-{folder_name[4:6]}-{folder_name[6:8]}"


def symbol_from_csv(csv_name: str) -> str:
    """Extract symbol from filename like 'a2507_20250701.csv' -> 'a2507'."""
    return csv_name.rsplit("_", 1)[0]


def tick_exists(symbol: str, date_str: str) -> bool:
    """Check if tick pkl already exists."""
    return (DB_BASE / "tick" / symbol / f"{date_str}.pkl").exists()


def import_one_file(args):
    """Import a single CSV file. Runs in worker process."""
    csv_path, date_str = args
    try:
        df = load_tick_data(csv_path)
        symbol = df["sym"].iloc[0]
        db = FuturesDB()
        db.save_tick(df, symbol=symbol, date=date_str)
        return f"  saved {symbol} ({len(df)} rows)"
    except Exception as e:
        return f"  ERROR {csv_path}: {e}"


def collect_tasks():
    """Collect all (csv_path, date_str) pairs, skipping already imported."""
    tasks = []
    skipped = 0
    for month in MONTHS:
        month_dir = DATA_BASE / month
        if not month_dir.is_dir():
            print(f"Skipping {month} (not found)")
            continue
        for day_folder in sorted(month_dir.iterdir()):
            if not day_folder.is_dir() or not day_folder.name.isdigit():
                continue
            date_str = date_folder_to_iso(day_folder.name)
            for csv_file in sorted(day_folder.glob("*_*.csv")):
                if "主力" in csv_file.name or "连续" in csv_file.name:
                    continue
                symbol = symbol_from_csv(csv_file.name)
                if tick_exists(symbol, date_str):
                    skipped += 1
                    continue
                tasks.append((str(csv_file), date_str))
    return tasks, skipped


def main():
    tasks, skipped = collect_tasks()
    print(f"Found {len(tasks)} files to import, {skipped} already done. Months: {MONTHS}")

    workers = min(cpu_count(), 8)
    print(f"Using {workers} worker processes\n")

    with Pool(workers) as pool:
        for result in pool.imap_unordered(import_one_file, tasks, chunksize=16):
            print(result)

    print(f"\nDone. {len(tasks)} files processed.")


if __name__ == "__main__":
    main()
