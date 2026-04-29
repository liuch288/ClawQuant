"""Data loader for raw futures tick data."""
import datetime
import pandas as pd
from pathlib import Path
from typing import List


COLUMN_RENAME = {
    'TradingDay': 'trading_day',
    'InstrumentID': 'sym',
    'UpdateTime': 'update_time',
    'UpdateMillisec': 'update_ms',
    'LastPrice': 'last_px',
    'Volume': 'tot_sz',
    'Turnover': 'tot_notional',
    'OpenInterest': 'oi',
    'BidPrice1': 'bid_px1',
    'BidVolume1': 'bid_sz1',
    'AskPrice1': 'ask_px1',
    'AskVolume1': 'ask_sz1',
    'UpperLimitPrice': 'upper_limit',
    'LowerLimitPrice': 'lower_limit',
}

# Columns to drop
COLUMNS_TO_DROP = ['AveragePrice']


def _create_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    """Create datetime index from trading_day, update_time, and update_millisec.

    Args:
        df: DataFrame with 'trading_day', 'update_time', 'update_millisec' columns.

    Returns:
        DataFrame with datetime index.
    """
    # Combine trading_day, update_time, and update_millisec into datetime
    datetime_str = (
        df['trading_day'].astype(str) + ' ' +
        df['update_time'] + '.' +
        df['update_ms'].astype(str).str.zfill(3)
    )
    datetime_index = pd.to_datetime(datetime_str, format='%Y%m%d %H:%M:%S.%f')
    df = df.set_index(datetime_index)
    df.index.name = 'datetime'
    return df


def load_tick_data(file_path: str) -> pd.DataFrame:
    """Load and process a single tick data CSV file.

    Args:
        file_path: Path to the CSV file.

    Returns:
        Processed DataFrame with snake_case columns.
    """
    df = pd.read_csv(file_path)

    # Rename columns to snake_case
    df = df.rename(columns=COLUMN_RENAME)

    # Drop AveragePrice column
    df = df.drop(columns=COLUMNS_TO_DROP, errors='ignore')

    # Create datetime index
    df = _create_datetime_index(df)

    return df


def load_day_folder(day_folder: str) -> pd.DataFrame:
    """Load and concatenate all tick data files for a given day.

    Args:
        day_folder: Path to the folder containing daily CSV files (e.g., /Users/boat/data/202601/20260105).

    Returns:
        Concatenated DataFrame for all instruments that day.
    """
    day_path = Path(day_folder)
    dfs = []

    for csv_file in day_path.glob("*_*.csv"):
        # Skip continuous contract files (contains Chinese characters)
        if '主力' in csv_file.name or '连续' in csv_file.name:
            continue
        df = load_tick_data(str(csv_file))
        dfs.append(df)

    if not dfs:
        raise ValueError(f"No valid CSV files found in {day_folder}")

    return pd.concat(dfs, ignore_index=True)


def load_date_range(data_root: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Load tick data for a date range.

    Args:
        data_root: Root directory containing date folders (e.g., /Users/boat/data/202601).
        start_date: Start date in YYYYMMDD format.
        end_date: End date in YYYYMMDD format.

    Returns:
        Concatenated DataFrame for the date range.
    """
    data_root = Path(data_root)
    start = int(start_date)
    end = int(end_date)
    dfs = []

    for folder in sorted(data_root.iterdir()):
        if folder.is_dir() and folder.name.isdigit():
            date_int = int(folder.name)
            if start <= date_int <= end:
                try:
                    df = load_day_folder(str(folder))
                    dfs.append(df)
                except ValueError:
                    continue  # Skip empty folders

    if not dfs:
        raise ValueError(f"No data found for date range {start_date}-{end_date}")

    return pd.concat(dfs, ignore_index=True)
