"""DataWriter的属性测试"""

import pytest
from hypothesis import given, strategies as st, settings, HealthCheck
from datetime import date
from pathlib import Path
import pandas as pd
import pickle
import tempfile
import shutil

from futures_db.writers import DataWriter
from futures_db.config import SUPPORTED_FREQUENCIES
from futures_db.exceptions import InvalidFrequencyError


# Feature: futures-db, Property 2: 目录结构自动创建
@given(
    symbol=st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"))),
    date_obj=st.dates(min_value=date(2020, 1, 1), max_value=date(2024, 12, 31)),
    data=st.lists(st.tuples(st.floats(allow_nan=False, allow_infinity=False), 
                            st.floats(allow_nan=False, allow_infinity=False)),
                  min_size=1, max_size=5)
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_directory_auto_creation(tmp_path, symbol, date_obj, data):
    """
    Property 2: 目录结构自动创建
    保存数据时应自动创建目录结构
    **Validates: Requirements 1.2, 1.4, 5.2**
    """
    # 为每次测试创建唯一的临时目录
    import uuid
    unique_base = tmp_path / str(uuid.uuid4())
    
    writer = DataWriter(unique_base)
    df = pd.DataFrame(data, columns=["price", "volume"])
    date_str = date_obj.strftime("%Y-%m-%d")
    
    # 保存前目录不存在
    tick_dir = unique_base / "tick" / symbol
    assert not tick_dir.exists()
    
    # 保存数据
    writer.save_tick(df, symbol, date_str)
    
    # 保存后目录应存在
    assert tick_dir.exists()
    assert (tick_dir / f"{date_str}.pkl").exists()


# Feature: futures-db, Property 3: 文件路径结构一致性
@given(
    freq=st.sampled_from(SUPPORTED_FREQUENCIES),
    symbol=st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"))),
    date_obj=st.dates(min_value=date(2020, 1, 1), max_value=date(2024, 12, 31)),
    data=st.lists(st.tuples(st.floats(allow_nan=False, allow_infinity=False), 
                            st.floats(allow_nan=False, allow_infinity=False)),
                  min_size=1, max_size=5)
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_file_path_consistency(tmp_path, freq, symbol, date_obj, data):
    """
    Property 3: 文件路径结构一致性
    文件路径应遵循 {base_path}/{freq}/{symbol}/{date}.pkl 格式
    **Validates: Requirements 1.1, 1.3**
    """
    writer = DataWriter(tmp_path)
    df = pd.DataFrame(data, columns=["col1", "col2"])
    date_str = date_obj.strftime("%Y-%m-%d")
    
    # 保存数据
    if freq == "tick":
        writer.save_tick(df, symbol, date_str)
    else:
        writer.save_kline(df, symbol, freq, date_str)
    
    # 验证路径结构
    expected_path = tmp_path / freq / symbol / f"{date_str}.pkl"
    assert expected_path.exists()


# Feature: futures-db, Property 9: 文件覆盖行为
@given(
    symbol=st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"))),
    date_obj=st.dates(min_value=date(2020, 1, 1), max_value=date(2024, 12, 31)),
    data1=st.lists(st.tuples(st.floats(allow_nan=False, allow_infinity=False), 
                             st.floats(allow_nan=False, allow_infinity=False)),
                   min_size=1, max_size=5),
    data2=st.lists(st.tuples(st.floats(allow_nan=False, allow_infinity=False), 
                             st.floats(allow_nan=False, allow_infinity=False)),
                   min_size=1, max_size=5)
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_file_overwrite_behavior(tmp_path, symbol, date_obj, data1, data2):
    """
    Property 9: 文件覆盖行为
    保存到已存在的文件应覆盖而不报错
    **Validates: Requirements 5.6**
    """
    writer = DataWriter(tmp_path)
    date_str = date_obj.strftime("%Y-%m-%d")
    
    # 第一次保存
    df1 = pd.DataFrame(data1, columns=["price", "volume"])
    writer.save_tick(df1, symbol, date_str)
    
    # 第二次保存（覆盖）
    df2 = pd.DataFrame(data2, columns=["price", "volume"])
    writer.save_tick(df2, symbol, date_str)  # 不应抛出异常
    
    # 读取文件验证是新数据
    file_path = tmp_path / "tick" / symbol / f"{date_str}.pkl"
    with open(file_path, 'rb') as f:
        loaded_df = pickle.load(f)
    
    pd.testing.assert_frame_equal(loaded_df, df2)


# Feature: futures-db, Property 12: 频率验证
@given(
    symbol=st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"))),
    date_obj=st.dates(min_value=date(2020, 1, 1), max_value=date(2024, 12, 31)),
    invalid_freq=st.text(min_size=1, max_size=20).filter(lambda x: x not in SUPPORTED_FREQUENCIES),
    data=st.lists(st.tuples(st.floats(allow_nan=False, allow_infinity=False), 
                            st.floats(allow_nan=False, allow_infinity=False)),
                  min_size=1, max_size=5)
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_frequency_validation(tmp_path, symbol, date_obj, invalid_freq, data):
    """
    Property 12: 频率验证
    不支持的频率应被拒绝
    **Validates: Requirements 6.2**
    """
    writer = DataWriter(tmp_path)
    df = pd.DataFrame(data, columns=["col1", "col2"])
    date_str = date_obj.strftime("%Y-%m-%d")
    
    with pytest.raises(InvalidFrequencyError) as exc_info:
        writer.save_kline(df, symbol, invalid_freq, date_str)
    
    assert "Unsupported frequency" in str(exc_info.value)
    assert invalid_freq in str(exc_info.value)
