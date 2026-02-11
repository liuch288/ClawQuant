"""工具函数的属性测试"""

import pytest
from hypothesis import given, strategies as st, settings
from datetime import date, timedelta
import pandas as pd

from futures_db.utils import (
    validate_date_format,
    validate_symbol,
    validate_date_range,
    validate_dataframe,
)
from futures_db.exceptions import (
    InvalidDateFormatError,
    InvalidSymbolError,
    InvalidDataError,
)


# Feature: futures-db, Property 10: 日期格式验证
@given(
    year=st.integers(min_value=1000, max_value=9999),
    month=st.integers(min_value=1, max_value=12),
    day=st.integers(min_value=1, max_value=28),  # 避免月份边界问题
)
@settings(max_examples=100)
def test_property_valid_date_format_accepted(year, month, day):
    """
    Property 10: 日期格式验证 - 有效格式应被接受
    **Validates: Requirements 2.5**
    """
    date_str = f"{year:04d}-{month:02d}-{day:02d}"
    # 不应抛出异常
    validate_date_format(date_str)


# Feature: futures-db, Property 10: 日期格式验证
@given(
    invalid_date=st.one_of(
        st.text(min_size=1, max_size=20).filter(
            lambda x: not (len(x) == 10 and x[4] == '-' and x[7] == '-')
        ),
        st.just("2024-13-01"),  # 无效月份
        st.just("2024-01-32"),  # 无效日期
        st.just("20240101"),    # 缺少分隔符
        st.just("2024/01/01"),  # 错误分隔符
    )
)
@settings(max_examples=100)
def test_property_invalid_date_format_rejected(invalid_date):
    """
    Property 10: 日期格式验证 - 无效格式应被拒绝
    **Validates: Requirements 2.5**
    """
    with pytest.raises(InvalidDateFormatError):
        validate_date_format(invalid_date)


# Feature: futures-db, Property 11: 日期范围顺序验证
@given(
    start=st.dates(min_value=date(2020, 1, 1), max_value=date(2024, 12, 31)),
    days_diff=st.integers(min_value=0, max_value=365),
)
@settings(max_examples=100)
def test_property_valid_date_range_accepted(start, days_diff):
    """
    Property 11: 日期范围顺序验证 - start <= end 应被接受
    **Validates: Requirements 3.7**
    """
    end = start + timedelta(days=days_diff)
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    # 不应抛出异常
    validate_date_range(start_str, end_str)


# Feature: futures-db, Property 11: 日期范围顺序验证
@given(
    end=st.dates(min_value=date(2020, 1, 1), max_value=date(2024, 12, 31)),
    days_diff=st.integers(min_value=1, max_value=365),
)
@settings(max_examples=100)
def test_property_invalid_date_range_rejected(end, days_diff):
    """
    Property 11: 日期范围顺序验证 - start > end 应被拒绝
    **Validates: Requirements 3.7**
    """
    start = end + timedelta(days=days_diff)
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    with pytest.raises(ValueError) as exc_info:
        validate_date_range(start_str, end_str)
    assert "Invalid date range" in str(exc_info.value)


# Feature: futures-db, Property 13: 空字符串参数拒绝
@given(
    symbol=st.text(min_size=1, max_size=20).filter(lambda x: x.strip())
)
@settings(max_examples=100)
def test_property_valid_symbol_accepted(symbol):
    """
    Property 13: 空字符串参数拒绝 - 非空symbol应被接受
    **Validates: Requirements 2.4**
    """
    # 不应抛出异常
    validate_symbol(symbol)


# Feature: futures-db, Property 13: 空字符串参数拒绝
@given(
    empty_symbol=st.one_of(
        st.just(""),
        st.just("   "),
        st.just("\t"),
        st.just("\n"),
    )
)
@settings(max_examples=100)
def test_property_empty_symbol_rejected(empty_symbol):
    """
    Property 13: 空字符串参数拒绝 - 空symbol应被拒绝
    **Validates: Requirements 2.4**
    """
    with pytest.raises(InvalidSymbolError):
        validate_symbol(empty_symbol)


# Feature: futures-db, Property 15: DataFrame非空验证
@given(
    data=st.lists(
        st.tuples(st.floats(allow_nan=False, allow_infinity=False), 
                  st.floats(allow_nan=False, allow_infinity=False)),
        min_size=1,
        max_size=10
    )
)
@settings(max_examples=100)
def test_property_valid_dataframe_accepted(data):
    """
    Property 15: DataFrame非空验证 - 有效DataFrame应被接受
    **Validates: Requirements 10.5**
    """
    df = pd.DataFrame(data, columns=["col1", "col2"])
    # 不应抛出异常
    validate_dataframe(df)


# Feature: futures-db, Property 15: DataFrame非空验证
def test_property_none_dataframe_rejected():
    """
    Property 15: DataFrame非空验证 - None应被拒绝
    **Validates: Requirements 10.5**
    """
    with pytest.raises(InvalidDataError):
        validate_dataframe(None)
