"""主数据库类"""

from pathlib import Path
from typing import Optional
import pandas as pd

from futures_db.readers import DataReader
from futures_db.writers import DataWriter
from futures_db.metadata import MetadataManager
from futures_db.utils import (
    validate_date_format,
    validate_symbol,
    validate_date_range,
    validate_frequency,
)


class FuturesDB:
    """期货数据库主类"""
    
    def __init__(self, base_path: str = "./data"):
        """
        初始化数据库实例
        
        Args:
            base_path: 数据存储根目录
        """
        self.base_path = Path(base_path)
        self.reader = DataReader(self.base_path)
        self.writer = DataWriter(self.base_path)
        self.metadata_manager = MetadataManager(self.base_path)

    
    # 读取API
    def get_tick(self, symbol: str, date: str) -> pd.DataFrame:
        """
        读取tick数据（仅支持单日）
        
        Args:
            symbol: 品种代码
            date: 日期 (YYYY-MM-DD)
            
        Returns:
            tick数据DataFrame
            
        Raises:
            InvalidSymbolError: 如果symbol无效
            InvalidDateFormatError: 如果日期格式无效
            FileNotFoundError: 如果数据文件不存在
        """
        validate_symbol(symbol)
        validate_date_format(date)
        return self.reader.read_tick(symbol, date)
    
    def get_kline(self, symbol: str, freq: str, 
                  date: Optional[str] = None,
                  start_date: Optional[str] = None, 
                  end_date: Optional[str] = None) -> pd.DataFrame:
        """
        读取K线数据（支持单日或日期范围）
        
        Args:
            symbol: 品种代码
            freq: 频率
            date: 单日日期 (YYYY-MM-DD)，与start_date/end_date互斥
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
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
            validate_date_format(date)
            return self.reader.read_kline_single(symbol, freq, date)
        elif start_date is not None and end_date is not None:
            validate_date_format(start_date)
            validate_date_format(end_date)
            validate_date_range(start_date, end_date)
            return self.reader.read_kline_range(symbol, freq, start_date, end_date)
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
    
    def get_dominant_contracts(self, date: Optional[str] = None) -> pd.DataFrame:
        """
        读取主力合约数据
        
        Args:
            date: 可选的日期过滤 (YYYY-MM-DD)
            
        Returns:
            主力合约DataFrame
            
        Raises:
            InvalidDateFormatError: 如果日期格式无效
            FileNotFoundError: 如果元数据文件不存在
        """
        if date is not None:
            validate_date_format(date)
        return self.metadata_manager.get_dominant_contracts(date)

    
    # 写入API
    def save_tick_from_csv(self, csv_path: str, symbol: str, date: str) -> None:
        """
        从CSV导入tick数据
        
        Args:
            csv_path: CSV文件路径
            symbol: 品种代码
            date: 日期 (YYYY-MM-DD)
            
        Raises:
            InvalidSymbolError: 如果symbol无效
            InvalidDateFormatError: 如果日期格式无效
            FileNotFoundError: 如果CSV文件不存在
            InvalidDataError: 如果CSV为空或无效
        """
        validate_symbol(symbol)
        validate_date_format(date)
        df = self.writer.read_csv(csv_path)
        self.writer.save_tick(df, symbol, date)
    
    def save_kline_from_csv(self, csv_path: str, symbol: str, 
                           freq: str, date: str) -> None:
        """
        从CSV导入K线数据
        
        Args:
            csv_path: CSV文件路径
            symbol: 品种代码
            freq: 频率
            date: 日期 (YYYY-MM-DD)
            
        Raises:
            InvalidSymbolError: 如果symbol无效
            InvalidFrequencyError: 如果频率不支持
            InvalidDateFormatError: 如果日期格式无效
            FileNotFoundError: 如果CSV文件不存在
            InvalidDataError: 如果CSV为空或无效
        """
        validate_symbol(symbol)
        validate_frequency(freq)
        validate_date_format(date)
        df = self.writer.read_csv(csv_path)
        self.writer.save_kline(df, symbol, freq, date)
    
    def save_metadata_from_csv(self, csv_path: str, 
                              metadata_type: str) -> None:
        """
        从CSV导入元数据
        
        Args:
            csv_path: CSV文件路径
            metadata_type: 元数据类型
            
        Raises:
            InvalidSymbolError: 如果metadata_type为空
            FileNotFoundError: 如果CSV文件不存在
            InvalidDataError: 如果CSV为空或无效
        """
        validate_symbol(metadata_type)  # 复用symbol验证逻辑检查非空
        df = self.writer.read_csv(csv_path)
        self.writer.save_metadata(df, metadata_type)
