"""
futures_db - 期货高频行情数据管理系统

一个用于管理期货市场高频行情数据的Python包。
支持tick数据、多种频率K线数据以及元数据的存储和读取。
"""

from futures_db.database import FuturesDB
from futures_db.exceptions import (
    FuturesDBError,
    InvalidDateFormatError,
    InvalidFrequencyError,
    InvalidSymbolError,
    DataNotFoundError,
    InvalidDataError,
)
from futures_db.config import get_supported_frequencies

__version__ = "0.1.0"
__all__ = [
    "FuturesDB",
    "FuturesDBError",
    "InvalidDateFormatError",
    "InvalidFrequencyError",
    "InvalidSymbolError",
    "DataNotFoundError",
    "InvalidDataError",
    "get_supported_frequencies",
]
