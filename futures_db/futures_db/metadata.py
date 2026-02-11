"""元数据管理器模块"""

from pathlib import Path
from typing import Optional
import pandas as pd

from futures_db.readers import DataReader
from futures_db.writers import DataWriter


class MetadataManager:
    """元数据管理器类"""
    
    def __init__(self, base_path: Path):
        """
        初始化元数据管理器
        
        Args:
            base_path: 数据存储根目录
        """
        self.base_path = base_path
        self.reader = DataReader(base_path)
        self.writer = DataWriter(base_path)
    
    def get_dominant_contracts(self, date: Optional[str] = None) -> pd.DataFrame:
        """
        获取主力合约数据
        
        Args:
            date: 可选的日期过滤 (YYYY-MM-DD)
            
        Returns:
            主力合约DataFrame
        """
        df = self.reader.read_metadata("dominant_contracts")
        
        # 如果指定了日期且DataFrame有date列，进行过滤
        if date is not None and 'date' in df.columns:
            df = df[df['date'] == date]
        
        return df
    
    def save_dominant_contracts(self, df: pd.DataFrame) -> None:
        """
        保存主力合约数据
        
        Args:
            df: 主力合约DataFrame
        """
        self.writer.save_metadata(df, "dominant_contracts")
