"""数据写入器模块"""

import pickle
from pathlib import Path
from typing import Optional
import pandas as pd

from futures_db.utils import (
    validate_frequency,
    validate_dataframe,
    build_file_path,
    build_metadata_path,
)
from futures_db.exceptions import InvalidDataError
from futures_db.config import CompressionType, DEFAULT_COMPRESSION


class DataWriter:
    """数据写入器类"""

    def __init__(self, base_path: Path, compression: CompressionType = DEFAULT_COMPRESSION):
        """
        初始化数据写入器

        Args:
            base_path: 数据存储根目录
            compression: 压缩类型. 支持 None, 'gzip', 'bz2', 'zip', 'xz', 'zstd'. 默认为 'gzip'.
        """
        self.base_path = base_path
        self.compression = compression
    
    def _ensure_directory(self, directory: Path) -> None:
        """
        确保目录存在，不存在则创建
        
        Args:
            directory: 目录路径
        """
        directory.mkdir(parents=True, exist_ok=True)
    
    def _save_pickle(self, df: pd.DataFrame, file_path: Path) -> None:
        """
        保存DataFrame为pickle文件的通用方法

        Args:
            df: 要保存的DataFrame
            file_path: 文件路径
        """
        # 确保父目录存在
        self._ensure_directory(file_path.parent)

        # 保存为pickle (支持压缩)
        df.to_pickle(file_path, compression=self.compression)
    
    def read_csv(self, csv_path: str) -> pd.DataFrame:
        """
        从CSV读取数据
        
        Args:
            csv_path: CSV文件路径
            
        Returns:
            DataFrame对象
            
        Raises:
            FileNotFoundError: 如果CSV文件不存在
            InvalidDataError: 如果CSV为空或无效
        """
        csv_file = Path(csv_path)
        if not csv_file.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        try:
            df = pd.read_csv(csv_path)
        except pd.errors.EmptyDataError:
            raise InvalidDataError(f"CSV file is empty: {csv_path}")
        
        if df.empty:
            raise InvalidDataError(f"CSV file is empty: {csv_path}")
        
        return df
    
    def save_tick(self, df: pd.DataFrame, symbol: str, date: str) -> None:
        """
        保存tick数据
        
        Args:
            df: tick数据DataFrame
            symbol: 品种代码
            date: 日期 (YYYY-MM-DD)
        """
        validate_dataframe(df)
        file_path = build_file_path(self.base_path, "tick", symbol, date)
        self._save_pickle(df, file_path)
    
    def save_kline(self, df: pd.DataFrame, symbol: str, freq: str, date: str) -> None:
        """
        保存K线数据
        
        Args:
            df: K线数据DataFrame
            symbol: 品种代码
            freq: 频率
            date: 日期 (YYYY-MM-DD)
        """
        validate_dataframe(df)
        validate_frequency(freq)
        file_path = build_file_path(self.base_path, freq, symbol, date)
        self._save_pickle(df, file_path)
    
    def save_metadata(self, df: pd.DataFrame, metadata_type: str) -> None:
        """
        保存元数据
        
        Args:
            df: 元数据DataFrame
            metadata_type: 元数据类型
        """
        validate_dataframe(df)
        file_path = build_metadata_path(self.base_path, metadata_type)
        self._save_pickle(df, file_path)
