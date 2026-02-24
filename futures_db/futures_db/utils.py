"""工具函数模块"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
import pandas as pd

from futures_db.config import DATE_FORMAT, SUPPORTED_FREQUENCIES
from futures_db.exceptions import (
    InvalidDateFormatError,
    InvalidSymbolError,
    InvalidDataError,
    InvalidFrequencyError,
)


def validate_date_format(date_str: str) -> None:
    """
    验证日期格式是否为YYYY-MM-DD
    
    Args:
        date_str: 日期字符串
        
    Raises:
        InvalidDateFormatError: 如果日期格式无效
    """
    try:
        datetime.strptime(date_str, DATE_FORMAT)
    except ValueError:
        raise InvalidDateFormatError(
            f"Invalid date format: '{date_str}'. Expected format: YYYY-MM-DD"
        )


def validate_symbol(symbol: str) -> None:
    """
    验证品种代码是否有效
    
    Args:
        symbol: 品种代码
        
    Raises:
        InvalidSymbolError: 如果symbol为空字符串
    """
    if not symbol or not symbol.strip():
        raise InvalidSymbolError("Symbol cannot be empty")


def validate_date_range(start_date: str, end_date: str) -> None:
    """
    验证日期范围是否有效
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        
    Raises:
        ValueError: 如果start_date晚于end_date
    """
    start = datetime.strptime(start_date, DATE_FORMAT)
    end = datetime.strptime(end_date, DATE_FORMAT)
    if start > end:
        raise ValueError(
            f"Invalid date range: start_date ({start_date}) is later than end_date ({end_date})"
        )


def validate_dataframe(df: Optional[pd.DataFrame]) -> None:
    """
    验证DataFrame是否有效
    
    Args:
        df: DataFrame对象
        
    Raises:
        InvalidDataError: 如果DataFrame为None
    """
    if df is None:
        raise InvalidDataError("DataFrame cannot be None")


def validate_frequency(freq: str) -> None:
    """
    验证频率是否支持
    
    Args:
        freq: 频率字符串
        
    Raises:
        InvalidFrequencyError: 如果频率不支持
    """
    if freq not in SUPPORTED_FREQUENCIES:
        raise InvalidFrequencyError(
            f"Unsupported frequency: '{freq}'. "
            f"Supported frequencies: {SUPPORTED_FREQUENCIES}"
        )


def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """
    生成日期范围列表
    
    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        
    Returns:
        日期字符串列表，格式为YYYY-MM-DD
    """
    start = datetime.strptime(start_date, DATE_FORMAT)
    end = datetime.strptime(end_date, DATE_FORMAT)
    
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime(DATE_FORMAT))
        current += timedelta(days=1)
    
    return dates


def build_file_path(base_path: Path, freq: str, symbol: str, date: str) -> Path:
    """
    构建数据文件路径
    
    Args:
        base_path: 基础路径
        freq: 频率
        symbol: 品种代码
        date: 日期
        
    Returns:
        Path对象: {base_path}/{freq}/{symbol}/{date}.pkl
    """
    return base_path / freq / symbol / f"{date}.pkl"


def build_metadata_path(base_path: Path, metadata_type: str) -> Path:
    """
    构建元数据文件路径
    
    Args:
        base_path: 基础路径
        metadata_type: 元数据类型
        
    Returns:
        Path对象: {base_path}/metadata/{metadata_type}.pkl
    """
    return base_path / "metadata" / f"{metadata_type}.pkl"
