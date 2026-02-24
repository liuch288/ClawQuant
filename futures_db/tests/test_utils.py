"""工具函数的单元测试"""

import pytest
from pathlib import Path

from futures_db.utils import (
    generate_date_range,
    build_file_path,
    build_metadata_path,
)


def test_generate_date_range_single_day():
    """测试单日日期范围"""
    dates = generate_date_range("2024-01-01", "2024-01-01")
    assert dates == ["2024-01-01"]


def test_generate_date_range_multiple_days():
    """测试多日日期范围"""
    dates = generate_date_range("2024-01-01", "2024-01-05")
    expected = ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
    assert dates == expected


def test_generate_date_range_cross_month():
    """测试跨月日期范围"""
    dates = generate_date_range("2024-01-30", "2024-02-02")
    expected = ["2024-01-30", "2024-01-31", "2024-02-01", "2024-02-02"]
    assert dates == expected


def test_build_file_path_structure():
    """测试文件路径结构"""
    base = Path("/data")
    path = build_file_path(base, "tick", "IF", "2024-01-01")
    expected = Path("/data/tick/IF/2024-01-01.pkl")
    assert path == expected


def test_build_file_path_different_freq():
    """测试不同频率的文件路径"""
    base = Path("/data")
    path = build_file_path(base, "1min", "IC", "2024-01-15")
    expected = Path("/data/1min/IC/2024-01-15.pkl")
    assert path == expected


def test_build_metadata_path_structure():
    """测试元数据路径结构"""
    base = Path("/data")
    path = build_metadata_path(base, "dominant_contracts")
    expected = Path("/data/metadata/dominant_contracts.pkl")
    assert path == expected
