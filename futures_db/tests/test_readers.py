"""DataReader的单元测试"""

import pytest
from pathlib import Path
import pandas as pd

from futures_db.readers import DataReader
from futures_db.writers import DataWriter


def test_read_tick_file_not_found(tmp_path):
    """测试读取不存在的tick文件抛出FileNotFoundError"""
    reader = DataReader(tmp_path)
    
    with pytest.raises(FileNotFoundError) as exc_info:
        reader.read_tick("IF", "2024-01-01")
    
    # 验证错误消息包含完整路径信息
    error_msg = str(exc_info.value)
    assert "Data file not found" in error_msg
    assert "IF" in error_msg
    assert "2024-01-01" in error_msg


def test_read_kline_single_file_not_found(tmp_path):
    """测试读取不存在的K线文件抛出FileNotFoundError"""
    reader = DataReader(tmp_path)
    
    with pytest.raises(FileNotFoundError) as exc_info:
        reader.read_kline_single("IC", "1min", "2024-02-15")
    
    # 验证错误消息包含完整路径信息
    error_msg = str(exc_info.value)
    assert "Data file not found" in error_msg
    assert "IC" in error_msg
    assert "2024-02-15" in error_msg


def test_read_metadata_file_not_found(tmp_path):
    """测试读取不存在的元数据文件抛出FileNotFoundError"""
    reader = DataReader(tmp_path)
    
    with pytest.raises(FileNotFoundError) as exc_info:
        reader.read_metadata("dominant_contracts")
    
    # 验证错误消息包含完整路径信息
    error_msg = str(exc_info.value)
    assert "Data file not found" in error_msg
    assert "dominant_contracts" in error_msg


def test_error_message_includes_full_path(tmp_path):
    """测试错误消息包含完整文件路径"""
    reader = DataReader(tmp_path)
    
    with pytest.raises(FileNotFoundError) as exc_info:
        reader.read_tick("IH", "2024-03-20")
    
    # 验证错误消息包含完整路径
    error_msg = str(exc_info.value)
    expected_path = tmp_path / "tick" / "IH" / "2024-03-20.pkl"
    assert str(expected_path) in error_msg


def test_read_kline_range_empty_returns_empty_dataframe(tmp_path):
    """测试空日期范围返回空DataFrame"""
    reader = DataReader(tmp_path)
    
    # 不创建任何文件，直接读取范围
    result = reader.read_kline_range("IF", "1min", "2024-01-01", "2024-01-05")
    
    # 验证返回空DataFrame
    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert len(result) == 0


def test_read_tick_returns_correct_data(tmp_path):
    """测试读取tick数据返回正确的DataFrame"""
    writer = DataWriter(tmp_path)
    reader = DataReader(tmp_path)
    
    # 准备测试数据
    df = pd.DataFrame({
        "timestamp": ["2024-01-01 09:30:00", "2024-01-01 09:30:01"],
        "price": [100.0, 101.0],
        "volume": [10, 20]
    })
    writer.save_tick(df, "IF", "2024-01-01")
    
    # 读取数据
    result = reader.read_tick("IF", "2024-01-01")
    
    # 验证
    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result, df)


def test_read_kline_single_returns_correct_data(tmp_path):
    """测试读取单日K线数据返回正确的DataFrame"""
    writer = DataWriter(tmp_path)
    reader = DataReader(tmp_path)
    
    # 准备测试数据
    df = pd.DataFrame({
        "timestamp": ["2024-02-15 09:30:00"],
        "open": [100.0],
        "high": [105.0],
        "low": [99.0],
        "close": [103.0],
        "volume": [1000]
    })
    writer.save_kline(df, "IC", "5min", "2024-02-15")
    
    # 读取数据
    result = reader.read_kline_single("IC", "5min", "2024-02-15")
    
    # 验证
    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result, df)


def test_read_kline_range_concatenates_multiple_days(tmp_path):
    """测试读取K线范围数据正确合并多天数据"""
    writer = DataWriter(tmp_path)
    reader = DataReader(tmp_path)
    
    # 准备多天数据
    df1 = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [100.0]})
    df2 = pd.DataFrame({"timestamp": ["2024-01-02"], "value": [101.0]})
    df3 = pd.DataFrame({"timestamp": ["2024-01-03"], "value": [102.0]})
    
    writer.save_kline(df1, "IH", "daily", "2024-01-01")
    writer.save_kline(df2, "IH", "daily", "2024-01-02")
    writer.save_kline(df3, "IH", "daily", "2024-01-03")
    
    # 读取范围数据
    result = reader.read_kline_range("IH", "daily", "2024-01-01", "2024-01-03")
    
    # 验证
    expected = pd.concat([df1, df2, df3], ignore_index=True)
    pd.testing.assert_frame_equal(result, expected)


def test_read_kline_range_skips_missing_files(tmp_path):
    """测试读取K线范围数据跳过缺失的文件"""
    writer = DataWriter(tmp_path)
    reader = DataReader(tmp_path)
    
    # 只创建第1天和第3天的数据，跳过第2天
    df1 = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [100.0]})
    df3 = pd.DataFrame({"timestamp": ["2024-01-03"], "value": [102.0]})
    
    writer.save_kline(df1, "IF", "1min", "2024-01-01")
    writer.save_kline(df3, "IF", "1min", "2024-01-03")
    # 不创建2024-01-02的数据
    
    # 读取范围数据（包括缺失的日期）
    result = reader.read_kline_range("IF", "1min", "2024-01-01", "2024-01-03")
    
    # 验证只包含存在的数据
    expected = pd.concat([df1, df3], ignore_index=True)
    pd.testing.assert_frame_equal(result, expected)


def test_read_metadata_returns_correct_data(tmp_path):
    """测试读取元数据返回正确的DataFrame"""
    writer = DataWriter(tmp_path)
    reader = DataReader(tmp_path)
    
    # 准备测试数据
    df = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-02"],
        "symbol": ["IF", "IC"],
        "contract": ["IF2401", "IC2401"]
    })
    writer.save_metadata(df, "dominant_contracts")
    
    # 读取数据
    result = reader.read_metadata("dominant_contracts")
    
    # 验证
    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result, df)


def test_read_kline_range_single_day(tmp_path):
    """测试读取单日范围（start_date == end_date）"""
    writer = DataWriter(tmp_path)
    reader = DataReader(tmp_path)
    
    # 准备测试数据
    df = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [100.0]})
    writer.save_kline(df, "IF", "1min", "2024-01-01")
    
    # 读取单日范围
    result = reader.read_kline_range("IF", "1min", "2024-01-01", "2024-01-01")
    
    # 验证
    pd.testing.assert_frame_equal(result, df)


def test_load_pickle_preserves_dataframe_structure(tmp_path):
    """测试pickle加载保持DataFrame结构"""
    writer = DataWriter(tmp_path)
    reader = DataReader(tmp_path)
    
    # 创建包含多种数据类型的DataFrame
    df = pd.DataFrame({
        "int_col": [1, 2, 3],
        "float_col": [1.1, 2.2, 3.3],
        "str_col": ["a", "b", "c"]
    })
    writer.save_tick(df, "TEST", "2024-01-01")
    
    # 读取数据
    result = reader.read_tick("TEST", "2024-01-01")
    
    # 验证数据类型和值都保持一致
    pd.testing.assert_frame_equal(result, df)
    assert result["int_col"].dtype == df["int_col"].dtype
    assert result["float_col"].dtype == df["float_col"].dtype
    assert result["str_col"].dtype == df["str_col"].dtype
