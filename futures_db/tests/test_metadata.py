"""MetadataManager的单元测试"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import shutil

from futures_db.metadata import MetadataManager


@pytest.fixture
def temp_manager():
    """创建临时MetadataManager"""
    temp_dir = tempfile.mkdtemp()
    manager = MetadataManager(Path(temp_dir))
    yield manager
    shutil.rmtree(temp_dir)


def test_save_and_get_dominant_contracts(temp_manager):
    """测试保存和读取主力合约"""
    df = pd.DataFrame({
        'date': ['2024-01-01', '2024-01-02'],
        'symbol': ['IF', 'IC'],
        'contract': ['IF2401', 'IC2401']
    })
    
    temp_manager.save_dominant_contracts(df)
    result = temp_manager.get_dominant_contracts()
    
    pd.testing.assert_frame_equal(df, result)


def test_get_dominant_contracts_with_date_filter(temp_manager):
    """测试日期过滤功能"""
    df = pd.DataFrame({
        'date': ['2024-01-01', '2024-01-02', '2024-01-01'],
        'symbol': ['IF', 'IF', 'IC'],
        'contract': ['IF2401', 'IF2402', 'IC2401']
    })
    
    temp_manager.save_dominant_contracts(df)
    
    # 过滤2024-01-01的数据
    result = temp_manager.get_dominant_contracts(date='2024-01-01')
    
    assert len(result) == 2
    assert all(result['date'] == '2024-01-01')
    assert set(result['symbol']) == {'IF', 'IC'}


def test_get_dominant_contracts_no_matching_date(temp_manager):
    """测试过滤不存在的日期"""
    df = pd.DataFrame({
        'date': ['2024-01-01', '2024-01-02'],
        'symbol': ['IF', 'IC'],
        'contract': ['IF2401', 'IC2401']
    })
    
    temp_manager.save_dominant_contracts(df)
    
    # 过滤不存在的日期
    result = temp_manager.get_dominant_contracts(date='2024-01-03')
    
    assert len(result) == 0


def test_get_dominant_contracts_no_date_column(temp_manager):
    """测试日期列不存在时的处理"""
    df = pd.DataFrame({
        'symbol': ['IF', 'IC'],
        'contract': ['IF2401', 'IC2401']
    })
    
    temp_manager.save_dominant_contracts(df)
    
    # 尝试过滤日期，但数据中没有date列
    # 应该返回所有数据
    result = temp_manager.get_dominant_contracts(date='2024-01-01')
    
    assert len(result) == 2
    pd.testing.assert_frame_equal(df, result)


def test_get_dominant_contracts_without_filter(temp_manager):
    """测试不过滤时返回所有数据"""
    df = pd.DataFrame({
        'date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'symbol': ['IF', 'IC', 'IH'],
        'contract': ['IF2401', 'IC2401', 'IH2401']
    })
    
    temp_manager.save_dominant_contracts(df)
    
    # 不指定日期过滤
    result = temp_manager.get_dominant_contracts()
    
    assert len(result) == 3
    pd.testing.assert_frame_equal(df, result)


def test_file_not_found_error(temp_manager):
    """测试读取不存在的元数据文件"""
    with pytest.raises(FileNotFoundError):
        temp_manager.get_dominant_contracts()
