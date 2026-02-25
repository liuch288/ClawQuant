"""主数据库类"""

from pathlib import Path
from typing import Optional, Union
from datetime import date
import pandas as pd

from futures_db.readers import DataReader
from futures_db.writers import DataWriter
from futures_db.metadata import MetadataManager
from futures_db.utils import (
    normalize_date,
    validate_date_format,
    validate_symbol,
    validate_date_range,
    validate_frequency,
)
from futures_db.config import CompressionType, DEFAULT_COMPRESSION, DEFAULT_DATA_PATH


class FuturesDB:
    """期货数据库主类"""

    def __init__(self, base_path: str = None, compression: CompressionType = DEFAULT_COMPRESSION):
        """
        初始化数据库实例

        Args:
            base_path: 数据存储根目录（默认从 FUTURESDB_PATH 环境变量读取，未设置则用 "./data"）
            compression: 压缩类型. 支持 None, 'gzip', 'bz2', 'zip', 'xz', 'zstd'. 默认为 'gzip'.
        """
        if base_path is None:
            base_path = DEFAULT_DATA_PATH
        self.base_path = Path(base_path)
        self.reader = DataReader(self.base_path, compression=compression)
        self.writer = DataWriter(self.base_path, compression=compression)
        self.metadata_manager = MetadataManager(self.base_path)

    
    # 读取API
    def get_tick(self, symbol: str, date: Union[str, date]) -> pd.DataFrame:
        """
        读取tick数据（仅支持单日）
        
        Args:
            symbol: 品种代码
            date: 日期 (YYYY-MM-DD字符串或datetime.date对象)
            
        Returns:
            tick数据DataFrame
            
        Raises:
            InvalidSymbolError: 如果symbol无效
            InvalidDateFormatError: 如果日期格式无效
            FileNotFoundError: 如果数据文件不存在
        """
        validate_symbol(symbol)
        date_str = normalize_date(date)
        return self.reader.read_tick(symbol, date_str)
    
    def get_kline(self, symbol: str, freq: str, 
                  date: Optional[Union[str, date]] = None,
                  start_date: Optional[Union[str, date]] = None, 
                  end_date: Optional[Union[str, date]] = None) -> pd.DataFrame:
        """
        读取K线数据（支持单日或日期范围）
        
        Args:
            symbol: 品种代码
            freq: 频率
            date: 单日日期 (YYYY-MM-DD字符串或datetime.date对象)，与start_date/end_date互斥
            start_date: 开始日期 (YYYY-MM-DD字符串或datetime.date对象)
            end_date: 结束日期 (YYYY-MM-DD字符串或datetime.date对象)
            
        Returns:
            K线数据DataFrame
            
        Raises:
            InvalidSymbolError: 如果symbol无效
            InvalidFrequencyError: 如果频率不支持
            InvalidDateFormatError: 如果日期格式无效
            ValueError: 如果参数组合无效或日期范围无效
            FileNotFoundError: 如果数据文件不存在
        """
        validate_symbol(symbol)
        validate_frequency(freq)
        
        # 验证参数组合
        if date is not None:
            if start_date is not None or end_date is not None:
                raise ValueError("Cannot specify both 'date' and 'start_date/end_date'")
            date_str = normalize_date(date)
            return self.reader.read_kline_single(symbol, freq, date_str)
        elif start_date is not None and end_date is not None:
            start_date_str = normalize_date(start_date)
            end_date_str = normalize_date(end_date)
            validate_date_range(start_date_str, end_date_str)
            return self.reader.read_kline_range(symbol, freq, start_date_str, end_date_str)
        else:
            raise ValueError("Must specify either 'date' or both 'start_date' and 'end_date'")
    
    def get_metadata(self, metadata_type: str) -> pd.DataFrame:
        """
        读取元数据
        
        Args:
            metadata_type: 元数据类型
            
        Returns:
            元数据DataFrame
            
        Raises:
            InvalidSymbolError: 如果metadata_type为空
            FileNotFoundError: 如果元数据文件不存在
        """
        validate_symbol(metadata_type)  # 复用symbol验证逻辑检查非空
        return self.reader.read_metadata(metadata_type)
    
    def get_dominant_contracts(self, date: Optional[Union[str, date]] = None) -> pd.DataFrame:
        """
        读取主力合约数据
        
        Args:
            date: 可选的日期过滤 (YYYY-MM-DD字符串或datetime.date对象)
            
        Returns:
            主力合约DataFrame
            
        Raises:
            InvalidDateFormatError: 如果日期格式无效
            FileNotFoundError: 如果元数据文件不存在
        """
        if date is not None:
            date_str = normalize_date(date)
            return self.metadata_manager.get_dominant_contracts(date_str)
        return self.metadata_manager.get_dominant_contracts(None)

    
    # 写入API
    def save_tick(self, df: pd.DataFrame, symbol: str, date: Union[str, date]) -> None:
        """
        保存tick数据

        Args:
            df: tick数据DataFrame
            symbol: 品种代码
            date: 日期 (YYYY-MM-DD字符串或datetime.date对象)

        Raises:
            InvalidSymbolError: 如果symbol无效
            InvalidDateFormatError: 如果日期格式无效
        """
        validate_symbol(symbol)
        date_str = normalize_date(date)
        self.writer.save_tick(df, symbol, date_str)

    def save_kline(self, df: pd.DataFrame, symbol: str,
                   freq: str, date: Union[str, date]) -> None:
        """
        保存K线数据

        Args:
            df: K线数据DataFrame
            symbol: 品种代码
            freq: 频率
            date: 日期 (YYYY-MM-DD字符串或datetime.date对象)

        Raises:
            InvalidSymbolError: 如果symbol无效
            InvalidFrequencyError: 如果频率不支持
            InvalidDateFormatError: 如果日期格式无效
        """
        validate_symbol(symbol)
        validate_frequency(freq)
        date_str = normalize_date(date)
        self.writer.save_kline(df, symbol, freq, date_str)

    def save_metadata(self, df: pd.DataFrame, metadata_type: str) -> None:
        """
        保存元数据

        Args:
            df: 元数据DataFrame
            metadata_type: 元数据类型

        Raises:
            InvalidSymbolError: 如果metadata_type为空
        """
        validate_symbol(metadata_type)  # 复用symbol验证逻辑检查非空
        self.writer.save_metadata(df, metadata_type)
