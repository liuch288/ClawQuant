"""数据读取器模块"""

import pickle
from pathlib import Path
from typing import List
import pandas as pd

from futures_db.utils import (
    generate_date_range,
    build_file_path,
    build_metadata_path,
)


class DataReader:
    """数据读取器类"""
    
    def __init__(self, base_path: Path):
        """
        初始化数据读取器
        
        Args:
            base_path: 数据存储根目录
        """
        self.base_path = base_path
    
    def _load_pickle(self, file_path: Path) -> pd.DataFrame:
        """
        加载pickle文件的通用方法
        
        Args:
            file_path: 文件路径
            
        Returns:
            DataFrame对象
            
        Raises:
            FileNotFoundError: 如果文件不存在
        """
        if not file_path.exists():
            raise FileNotFoundError(f"Data file not found: {file_path}")
        
        with open(file_path, 'rb') as f:
            df = pickle.load(f)
        
        return df
    
    def read_tick(self, symbol: str, date: str) -> pd.DataFrame:
        """
        读取单日tick数据
        
        Args:
            symbol: 品种代码
            date: 日期 (YYYY-MM-DD)
            
        Returns:
            tick数据DataFrame
            
        Raises:
            FileNotFoundError: 如果文件不存在
        """
        file_path = build_file_path(self.base_path, "tick", symbol, date)
        return self._load_pickle(file_path)
    
    def read_kline_single(self, symbol: str, freq: str, date: str) -> pd.DataFrame:
        """
        读取单日K线数据
        
        Args:
            symbol: 品种代码
            freq: 频率
            date: 日期 (YYYY-MM-DD)
            
        Returns:
            K线数据DataFrame
            
        Raises:
            FileNotFoundError: 如果文件不存在
        """
        file_path = build_file_path(self.base_path, freq, symbol, date)
        return self._load_pickle(file_path)
    
    def read_kline_range(self, symbol: str, freq: str, 
                        start_date: str, end_date: str) -> pd.DataFrame:
        """
        读取日期范围内的K线数据
        
        Args:
            symbol: 品种代码
            freq: 频率
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            合并后的K线数据DataFrame
        """
        # 生成日期列表
        dates = generate_date_range(start_date, end_date)
        
        # 读取所有存在的文件
        dataframes = []
        for date in dates:
            file_path = build_file_path(self.base_path, freq, symbol, date)
            if file_path.exists():
                df = self._load_pickle(file_path)
                dataframes.append(df)
        
        # 如果没有数据，返回空DataFrame
        if not dataframes:
            return pd.DataFrame()
        
        # 合并所有DataFrame
        result = pd.concat(dataframes, ignore_index=True)
        return result
    
    def read_metadata(self, metadata_type: str) -> pd.DataFrame:
        """
        读取元数据
        
        Args:
            metadata_type: 元数据类型
            
        Returns:
            元数据DataFrame
            
        Raises:
            FileNotFoundError: 如果文件不存在
        """
        file_path = build_metadata_path(self.base_path, metadata_type)
        return self._load_pickle(file_path)
