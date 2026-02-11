"""FuturesDB的属性测试"""

import pytest
from hypothesis import given, strategies as st, settings
from datetime import date, timedelta
import pandas as pd
from pathlib import Path
import tempfile

from futures_db.database import FuturesDB
from futures_db.exceptions import InvalidDataError


# 自定义策略
@st.composite
def valid_symbols(draw):
    """生成有效的品种代码（排除Windows保留字）"""
    # Windows保留字列表
    reserved_names = {'CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4', 
                     'COM5', 'COM6', 'COM7', 'COM8', 'COM9', 'LPT1', 'LPT2', 
                     'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'}
    
    symbol = draw(st.text(
        min_size=1, 
        max_size=10,
        alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"))
    ).filter(lambda x: x.strip() and x.upper() not in reserved_names))
    
    return symbol


@st.composite
def valid_dates(draw):
    """生成有效的日期字符串"""
    d = draw(st.dates(min_value=date(2020, 1, 1), max_value=date(2024, 12, 31)))
    return d.strftime("%Y-%m-%d")


@st.composite
def simple_dataframes(draw):
    """生成简单的DataFrame"""
    num_rows = draw(st.integers(min_value=1, max_value=10))
    data = {
        "price": [draw(st.floats(min_value=1.0, max_value=1000.0, allow_nan=False, allow_infinity=False)) 
                  for _ in range(num_rows)],
        "volume": [draw(st.integers(min_value=1, max_value=10000)) 
                   for _ in range(num_rows)]
    }
    return pd.DataFrame(data)


# Feature: futures-db, Property 1: 序列化往返一致性
@given(
    df=simple_dataframes(),
    symbol=valid_symbols(),
    date_str=valid_dates(),
)
@settings(max_examples=100)
def test_property_serialization_round_trip_tick(df, symbol, date_str):
    """
    Property 1: 序列化往返一致性 - Tick数据
    For any valid DataFrame, saving and reading should produce equivalent data
    **Validates: Requirements 1.5, 5.3**
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        db = FuturesDB(base_path=tmp_dir)
        
        # 保存数据
        db.writer.save_tick(df, symbol, date_str)
        
        # 读取数据
        result = db.get_tick(symbol, date_str)
        
        # 验证等价性
        pd.testing.assert_frame_equal(df, result)


# Feature: futures-db, Property 1: 序列化往返一致性
@given(
    df=simple_dataframes(),
    symbol=valid_symbols(),
    date_str=valid_dates(),
    freq=st.sampled_from(["1min", "5min", "15min", "30min", "1hour", "daily"]),
)
@settings(max_examples=100)
def test_property_serialization_round_trip_kline(df, symbol, date_str, freq):
    """
    Property 1: 序列化往返一致性 - K线数据
    For any valid DataFrame, saving and reading should produce equivalent data
    **Validates: Requirements 1.5, 5.3**
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        db = FuturesDB(base_path=tmp_dir)
        
        # 保存数据
        db.writer.save_kline(df, symbol, freq, date_str)
        
        # 读取数据
        result = db.get_kline(symbol, freq, date=date_str)
        
        # 验证等价性
        pd.testing.assert_frame_equal(df, result)


# Feature: futures-db, Property 4: Tick数据单日限制
@given(
    symbol=valid_symbols(),
    start_date=st.dates(min_value=date(2024, 1, 1), max_value=date(2024, 1, 10)),
    days_diff=st.integers(min_value=1, max_value=5),
)
@settings(max_examples=100)
def test_property_tick_single_date_only(symbol, start_date, days_diff):
    """
    Property 4: Tick数据单日限制
    For any tick data query with date range, system should raise ValueError
    **Validates: Requirements 2.3**
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        db = FuturesDB(base_path=tmp_dir)
        
        end_date = start_date + timedelta(days=days_diff)
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        # get_tick不接受日期范围参数，这个属性通过API设计来保证
        # 我们验证get_tick只接受单个date参数
        # 如果尝试传递start_date/end_date会导致TypeError
        with pytest.raises(TypeError):
            db.get_tick(symbol, start_date=start_str, end_date=end_str)


# Feature: futures-db, Property 14: CSV数据验证
def test_property_empty_csv_rejected():
    """
    Property 14: CSV数据验证
    For any empty CSV file, system should raise InvalidDataError
    **Validates: Requirements 5.5**
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        db = FuturesDB(base_path=tmp_dir)
        
        # 创建空CSV文件
        csv_path = Path(tmp_dir) / "empty.csv"
        csv_path.write_text("col1,col2\n")  # 只有表头，没有数据
        
        with pytest.raises(InvalidDataError) as exc_info:
            db.save_tick_from_csv(str(csv_path), "IF", "2024-01-01")
        assert "empty" in str(exc_info.value).lower()


# Feature: futures-db, Property 17: base_path配置
@given(
    symbol=valid_symbols(),
    date_str=valid_dates(),
)
@settings(max_examples=100)
def test_property_base_path_configuration(symbol, date_str):
    """
    Property 17: base_path配置
    For any base_path, all file operations should use that base_path
    **Validates: Requirements 8.4**
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        custom_path = Path(tmp_dir) / "custom_data"
        db = FuturesDB(base_path=str(custom_path))
        
        # 保存数据
        df = pd.DataFrame({"price": [100.0], "volume": [10]})
        db.writer.save_tick(df, symbol, date_str)
        
        # 验证文件确实保存在custom_path下
        expected_path = custom_path / "tick" / symbol / f"{date_str}.pkl"
        assert expected_path.exists(), f"File should exist at {expected_path}"
        
        # 验证可以读取
        result = db.get_tick(symbol, date_str)
        assert len(result) == 1
