"""集成测试"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import shutil

from futures_db import FuturesDB


@pytest.fixture
def temp_db():
    """创建临时数据库"""
    temp_dir = tempfile.mkdtemp()
    db = FuturesDB(base_path=temp_dir)
    yield db
    shutil.rmtree(temp_dir)


def test_tick_save_and_read(temp_db):
    """测试tick数据的保存和读取"""
    # 创建测试数据
    df = pd.DataFrame({
        'price': [100.0, 101.0, 102.0],
        'volume': [10, 20, 30]
    })
    
    # 保存
    temp_db.writer.save_tick(df, 'IF', '2024-01-01')
    
    # 读取
    result = temp_db.get_tick('IF', '2024-01-01')
    
    # 验证
    pd.testing.assert_frame_equal(df, result)


def test_kline_single_day(temp_db):
    """测试单日K线数据"""
    df = pd.DataFrame({
        'open': [100.0],
        'high': [105.0],
        'low': [95.0],
        'close': [102.0],
        'volume': [1000]
    })
    
    temp_db.writer.save_kline(df, 'IF', '1min', '2024-01-01')
    result = temp_db.get_kline('IF', '1min', date='2024-01-01')
    
    pd.testing.assert_frame_equal(df, result)


def test_kline_date_range(temp_db):
    """测试K线日期范围查询"""
    # 保存多天数据
    df1 = pd.DataFrame({'value': [1, 2]})
    df2 = pd.DataFrame({'value': [3, 4]})
    df3 = pd.DataFrame({'value': [5, 6]})
    
    temp_db.writer.save_kline(df1, 'IF', '1min', '2024-01-01')
    temp_db.writer.save_kline(df2, 'IF', '1min', '2024-01-02')
    temp_db.writer.save_kline(df3, 'IF', '1min', '2024-01-03')
    
    # 读取范围
    result = temp_db.get_kline('IF', '1min', 
                               start_date='2024-01-01', 
                               end_date='2024-01-03')
    
    assert len(result) == 6
    assert result['value'].tolist() == [1, 2, 3, 4, 5, 6]


def test_metadata_save_and_read(temp_db):
    """测试元数据的保存和读取"""
    df = pd.DataFrame({
        'date': ['2024-01-01', '2024-01-02'],
        'symbol': ['IF', 'IC']
    })
    
    temp_db.writer.save_metadata(df, 'dominant_contracts')
    result = temp_db.get_metadata('dominant_contracts')
    
    pd.testing.assert_frame_equal(df, result)


def test_dominant_contracts_filter(temp_db):
    """测试主力合约日期过滤"""
    df = pd.DataFrame({
        'date': ['2024-01-01', '2024-01-02', '2024-01-01'],
        'symbol': ['IF', 'IF', 'IC']
    })
    
    temp_db.metadata_manager.save_dominant_contracts(df)
    
    # 不过滤
    all_contracts = temp_db.get_dominant_contracts()
    assert len(all_contracts) == 3
    
    # 过滤特定日期
    filtered = temp_db.get_dominant_contracts(date='2024-01-01')
    assert len(filtered) == 2
    assert all(filtered['date'] == '2024-01-01')


def test_file_not_found_error(temp_db):
    """测试文件不存在错误"""
    with pytest.raises(FileNotFoundError):
        temp_db.get_tick('NONEXIST', '2024-01-01')


def test_invalid_date_format(temp_db):
    """测试无效日期格式"""
    from futures_db.exceptions import InvalidDateFormatError
    
    with pytest.raises(InvalidDateFormatError):
        temp_db.get_tick('IF', '20240101')


def test_invalid_frequency(temp_db):
    """测试无效频率"""
    from futures_db.exceptions import InvalidFrequencyError
    
    df = pd.DataFrame({'value': [1, 2, 3]})
    
    with pytest.raises(InvalidFrequencyError):
        temp_db.writer.save_kline(df, 'IF', 'invalid_freq', '2024-01-01')


def test_empty_symbol(temp_db):
    """测试空品种代码"""
    from futures_db.exceptions import InvalidSymbolError
    
    with pytest.raises(InvalidSymbolError):
        temp_db.get_tick('', '2024-01-01')


def test_csv_import(temp_db, tmp_path):
    """测试CSV导入功能"""
    # 创建CSV文件
    csv_file = tmp_path / "test.csv"
    df = pd.DataFrame({
        'price': [100.0, 101.0],
        'volume': [10, 20]
    })
    df.to_csv(csv_file, index=False)
    
    # 导入
    temp_db.save_tick_from_csv(str(csv_file), 'IF', '2024-01-01')
    
    # 验证
    result = temp_db.get_tick('IF', '2024-01-01')
    pd.testing.assert_frame_equal(df, result)
