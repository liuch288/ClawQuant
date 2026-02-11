"""DataReader的属性测试"""

import pytest
from hypothesis import given, strategies as st, settings, HealthCheck
from datetime import date, timedelta
from pathlib import Path
import pandas as pd
import pickle

from futures_db.readers import DataReader
from futures_db.writers import DataWriter
from futures_db.config import SUPPORTED_FREQUENCIES


# 自定义策略：生成有效的品种代码（排除Windows保留字）
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


# Feature: futures-db, Property 5: K线数据范围读取合并
@given(
    symbol=valid_symbols(),
    freq=st.sampled_from([f for f in SUPPORTED_FREQUENCIES if f != "tick"]),
    start_date_obj=st.dates(min_value=date(2024, 1, 1), max_value=date(2024, 1, 10)),
    num_days=st.integers(min_value=1, max_value=5)
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_kline_range_concatenation(tmp_path, symbol, freq, start_date_obj, num_days):
    """
    Property 5: K线数据范围读取合并
    读取日期范围应返回按时间顺序合并的所有数据
    **Validates: Requirements 3.2, 3.3**
    """
    writer = DataWriter(tmp_path)
    reader = DataReader(tmp_path)
    
    # 创建测试数据
    all_data = []
    for i in range(num_days):
        current_date = start_date_obj + timedelta(days=i)
        date_str = current_date.strftime("%Y-%m-%d")
        df = pd.DataFrame({
            "timestamp": [current_date.isoformat()],
            "value": [float(i)]
        })
        writer.save_kline(df, symbol, freq, date_str)
        all_data.append(df)
    
    # 读取范围数据
    end_date_obj = start_date_obj + timedelta(days=num_days - 1)
    start_date_str = start_date_obj.strftime("%Y-%m-%d")
    end_date_str = end_date_obj.strftime("%Y-%m-%d")
    
    result = reader.read_kline_range(symbol, freq, start_date_str, end_date_str)
    
    # 验证
    expected = pd.concat(all_data, ignore_index=True)
    pd.testing.assert_frame_equal(result, expected)


# Feature: futures-db, Property 6: 缺失文件跳过
@given(
    symbol=valid_symbols(),
    freq=st.sampled_from([f for f in SUPPORTED_FREQUENCIES if f != "tick"]),
    start_date_obj=st.dates(min_value=date(2024, 1, 1), max_value=date(2024, 1, 10)),
    num_days=st.integers(min_value=3, max_value=7)
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_missing_files_skipped(tmp_path, symbol, freq, start_date_obj, num_days):
    """
    Property 6: 缺失文件跳过
    读取范围时应跳过不存在的文件，只返回存在的数据
    **Validates: Requirements 3.4**
    """
    # Create a unique subdirectory for this test iteration
    import uuid
    test_dir = tmp_path / str(uuid.uuid4())
    test_dir.mkdir(parents=True, exist_ok=True)
    
    writer = DataWriter(test_dir)
    reader = DataReader(test_dir)
    
    # 创建测试数据，跳过某些日期（跳过中间的日期）
    skip_indices = [i for i in range(1, num_days - 1, 2)]  # 跳过索引1, 3, 5...
    existing_data = []
    for i in range(num_days):
        if i not in skip_indices:
            current_date = start_date_obj + timedelta(days=i)
            date_str = current_date.strftime("%Y-%m-%d")
            df = pd.DataFrame({
                "timestamp": [current_date.isoformat()],
                "value": [float(i)]
            })
            writer.save_kline(df, symbol, freq, date_str)
            existing_data.append(df)
    
    # 读取范围数据（包括缺失的日期）
    end_date_obj = start_date_obj + timedelta(days=num_days - 1)
    start_date_str = start_date_obj.strftime("%Y-%m-%d")
    end_date_str = end_date_obj.strftime("%Y-%m-%d")
    
    # 不应抛出异常
    result = reader.read_kline_range(symbol, freq, start_date_str, end_date_str)
    
    # 验证只包含存在的数据
    if existing_data:
        expected = pd.concat(existing_data, ignore_index=True)
        pd.testing.assert_frame_equal(result, expected)
    else:
        assert result.empty


# Feature: futures-db, Property 8: 文件不存在错误
@given(
    symbol=valid_symbols(),
    date_obj=st.dates(min_value=date(2020, 1, 1), max_value=date(2024, 12, 31))
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_file_not_found_error_tick(tmp_path, symbol, date_obj):
    """
    Property 8: 文件不存在错误 - tick数据
    读取不存在的文件应抛出FileNotFoundError并包含完整路径
    **Validates: Requirements 2.2, 9.1**
    """
    reader = DataReader(tmp_path)
    date_str = date_obj.strftime("%Y-%m-%d")
    
    with pytest.raises(FileNotFoundError) as exc_info:
        reader.read_tick(symbol, date_str)
    
    # 验证错误消息包含路径信息
    error_msg = str(exc_info.value)
    assert "Data file not found" in error_msg
    assert symbol in error_msg
    assert date_str in error_msg


# Feature: futures-db, Property 8: 文件不存在错误
@given(
    symbol=valid_symbols(),
    freq=st.sampled_from([f for f in SUPPORTED_FREQUENCIES if f != "tick"]),
    date_obj=st.dates(min_value=date(2020, 1, 1), max_value=date(2024, 12, 31))
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_file_not_found_error_kline(tmp_path, symbol, freq, date_obj):
    """
    Property 8: 文件不存在错误 - K线数据
    读取不存在的文件应抛出FileNotFoundError并包含完整路径
    **Validates: Requirements 3.1, 9.1**
    """
    reader = DataReader(tmp_path)
    date_str = date_obj.strftime("%Y-%m-%d")
    
    with pytest.raises(FileNotFoundError) as exc_info:
        reader.read_kline_single(symbol, freq, date_str)
    
    # 验证错误消息包含路径信息
    error_msg = str(exc_info.value)
    assert "Data file not found" in error_msg
    assert symbol in error_msg
    assert date_str in error_msg


# Feature: futures-db, Property 8: 文件不存在错误
@given(
    metadata_type=st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")))
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_file_not_found_error_metadata(tmp_path, metadata_type):
    """
    Property 8: 文件不存在错误 - 元数据
    读取不存在的元数据应抛出FileNotFoundError并包含完整路径
    **Validates: Requirements 4.3, 9.1**
    """
    reader = DataReader(tmp_path)
    
    with pytest.raises(FileNotFoundError) as exc_info:
        reader.read_metadata(metadata_type)
    
    # 验证错误消息包含路径信息
    error_msg = str(exc_info.value)
    assert "Data file not found" in error_msg
    assert metadata_type in error_msg


# Feature: futures-db, Property 16: 有效输入返回DataFrame
@given(
    symbol=valid_symbols(),
    date_obj=st.dates(min_value=date(2020, 1, 1), max_value=date(2024, 12, 31)),
    data=st.lists(st.tuples(st.floats(allow_nan=False, allow_infinity=False), 
                            st.floats(allow_nan=False, allow_infinity=False)),
                  min_size=1, max_size=10)
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_valid_input_returns_dataframe_tick(tmp_path, symbol, date_obj, data):
    """
    Property 16: 有效输入返回DataFrame - tick数据
    有效的读取操作应返回DataFrame对象
    **Validates: Requirements 2.1**
    """
    writer = DataWriter(tmp_path)
    reader = DataReader(tmp_path)
    
    # 保存数据
    df = pd.DataFrame(data, columns=["price", "volume"])
    date_str = date_obj.strftime("%Y-%m-%d")
    writer.save_tick(df, symbol, date_str)
    
    # 读取数据
    result = reader.read_tick(symbol, date_str)
    
    # 验证返回DataFrame
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


# Feature: futures-db, Property 16: 有效输入返回DataFrame
@given(
    symbol=valid_symbols(),
    freq=st.sampled_from([f for f in SUPPORTED_FREQUENCIES if f != "tick"]),
    date_obj=st.dates(min_value=date(2020, 1, 1), max_value=date(2024, 12, 31)),
    data=st.lists(st.tuples(st.floats(allow_nan=False, allow_infinity=False), 
                            st.floats(allow_nan=False, allow_infinity=False)),
                  min_size=1, max_size=10)
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_valid_input_returns_dataframe_kline(tmp_path, symbol, freq, date_obj, data):
    """
    Property 16: 有效输入返回DataFrame - K线数据
    有效的读取操作应返回DataFrame对象
    **Validates: Requirements 3.1**
    """
    writer = DataWriter(tmp_path)
    reader = DataReader(tmp_path)
    
    # 保存数据
    df = pd.DataFrame(data, columns=["open", "close"])
    date_str = date_obj.strftime("%Y-%m-%d")
    writer.save_kline(df, symbol, freq, date_str)
    
    # 读取数据
    result = reader.read_kline_single(symbol, freq, date_str)
    
    # 验证返回DataFrame
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


# Feature: futures-db, Property 16: 有效输入返回DataFrame
@given(
    metadata_type=st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"))),
    data=st.lists(st.tuples(st.text(min_size=1, max_size=10), 
                            st.floats(allow_nan=False, allow_infinity=False)),
                  min_size=1, max_size=10)
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_valid_input_returns_dataframe_metadata(tmp_path, metadata_type, data):
    """
    Property 16: 有效输入返回DataFrame - 元数据
    有效的读取操作应返回DataFrame对象
    **Validates: Requirements 4.1**
    """
    writer = DataWriter(tmp_path)
    reader = DataReader(tmp_path)
    
    # 保存数据
    df = pd.DataFrame(data, columns=["name", "value"])
    writer.save_metadata(df, metadata_type)
    
    # 读取数据
    result = reader.read_metadata(metadata_type)
    
    # 验证返回DataFrame
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


# 测试空日期范围返回空DataFrame
@given(
    symbol=valid_symbols(),
    freq=st.sampled_from([f for f in SUPPORTED_FREQUENCIES if f != "tick"]),
    start_date_obj=st.dates(min_value=date(2024, 1, 1), max_value=date(2024, 1, 10)),
    num_days=st.integers(min_value=1, max_value=5)
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_empty_range_returns_empty_dataframe(tmp_path, symbol, freq, start_date_obj, num_days):
    """
    测试空日期范围返回空DataFrame
    当范围内没有文件时，应返回空DataFrame
    **Validates: Requirements 3.5**
    """
    reader = DataReader(tmp_path)
    
    # 不创建任何文件
    end_date_obj = start_date_obj + timedelta(days=num_days - 1)
    start_date_str = start_date_obj.strftime("%Y-%m-%d")
    end_date_str = end_date_obj.strftime("%Y-%m-%d")
    
    result = reader.read_kline_range(symbol, freq, start_date_str, end_date_str)
    
    # 验证返回空DataFrame
    assert isinstance(result, pd.DataFrame)
    assert result.empty
