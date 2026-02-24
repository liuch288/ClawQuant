"""DataWriter的单元测试"""

import pytest
from pathlib import Path
import pandas as pd

from futures_db.writers import DataWriter
from futures_db.exceptions import InvalidDataError


def test_read_csv_file_not_found():
    """测试CSV不存在时抛出FileNotFoundError"""
    writer = DataWriter(Path("/tmp"))
    
    with pytest.raises(FileNotFoundError) as exc_info:
        writer.read_csv("nonexistent.csv")
    
    assert "nonexistent.csv" in str(exc_info.value)


def test_read_csv_empty_file(tmp_path):
    """测试CSV为空时抛出ValueError"""
    # 创建空CSV文件
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("")
    
    writer = DataWriter(tmp_path)
    
    with pytest.raises(InvalidDataError) as exc_info:
        writer.read_csv(str(empty_csv))
    
    assert "empty" in str(exc_info.value).lower()


def test_directory_auto_creation(tmp_path):
    """测试目录自动创建"""
    writer = DataWriter(tmp_path)
    df = pd.DataFrame({"price": [100, 101], "volume": [10, 20]})
    
    # 保存前目录不存在
    assert not (tmp_path / "tick" / "IF").exists()
    
    # 保存数据
    writer.save_tick(df, "IF", "2024-01-01")
    
    # 保存后目录存在
    assert (tmp_path / "tick" / "IF").exists()
    assert (tmp_path / "tick" / "IF" / "2024-01-01.pkl").exists()


def test_save_tick_creates_correct_path(tmp_path):
    """测试保存tick数据创建正确路径"""
    writer = DataWriter(tmp_path)
    df = pd.DataFrame({"price": [100]})
    
    writer.save_tick(df, "IC", "2024-02-15")
    
    expected_path = tmp_path / "tick" / "IC" / "2024-02-15.pkl"
    assert expected_path.exists()


def test_save_kline_creates_correct_path(tmp_path):
    """测试保存K线数据创建正确路径"""
    writer = DataWriter(tmp_path)
    df = pd.DataFrame({"open": [100], "close": [101]})
    
    writer.save_kline(df, "IH", "1min", "2024-03-20")
    
    expected_path = tmp_path / "1min" / "IH" / "2024-03-20.pkl"
    assert expected_path.exists()


def test_save_metadata_creates_correct_path(tmp_path):
    """测试保存元数据创建正确路径"""
    writer = DataWriter(tmp_path)
    df = pd.DataFrame({"symbol": ["IF"], "contract": ["IF2403"]})
    
    writer.save_metadata(df, "dominant_contracts")
    
    expected_path = tmp_path / "metadata" / "dominant_contracts.pkl"
    assert expected_path.exists()
