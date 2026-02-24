"""MetadataManager的属性测试"""

import pytest
from hypothesis import given, strategies as st, settings
from datetime import date
import pandas as pd
import tempfile

from futures_db.metadata import MetadataManager


@st.composite
def valid_dates(draw):
    """生成有效的日期字符串"""
    d = draw(st.dates(min_value=date(2020, 1, 1), max_value=date(2024, 12, 31)))
    return d.strftime("%Y-%m-%d")


# Feature: futures-db, Property 7: 主力合约日期过滤
@given(
    dates=st.lists(valid_dates(), min_size=3, max_size=10),
    filter_date=valid_dates(),
)
@settings(max_examples=100)
def test_property_dominant_contracts_date_filter(dates, filter_date):
    """
    Property 7: 主力合约日期过滤
    For any dominant contracts metadata and a specific date, 
    calling get_dominant_contracts with that date should return 
    only contracts matching that date
    **Validates: Requirements 4.2**
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        from pathlib import Path
        manager = MetadataManager(Path(tmp_dir))
        
        # 创建包含多个日期的主力合约数据
        df = pd.DataFrame({
            'date': dates,
            'symbol': [f'SYM{i}' for i in range(len(dates))],
            'contract': [f'C{i}' for i in range(len(dates))]
        })
        
        # 保存数据
        manager.save_dominant_contracts(df)
        
        # 不过滤 - 应返回所有数据
        all_contracts = manager.get_dominant_contracts()
        assert len(all_contracts) == len(dates)
        
        # 过滤特定日期 - 应只返回该日期的数据
        filtered = manager.get_dominant_contracts(date=filter_date)
        
        # 验证过滤结果
        if filter_date in dates:
            # 如果filter_date在数据中，应该返回匹配的行
            assert len(filtered) > 0
            assert all(filtered['date'] == filter_date)
        else:
            # 如果filter_date不在数据中，应该返回空DataFrame
            assert len(filtered) == 0


# Feature: futures-db, Property 7: 主力合约日期过滤 - 无date列的情况
@given(
    num_rows=st.integers(min_value=1, max_value=10),
    filter_date=valid_dates(),
)
@settings(max_examples=100)
def test_property_dominant_contracts_no_date_column(num_rows, filter_date):
    """
    Property 7: 主力合约日期过滤 - 无date列
    When metadata has no date column, filtering by date should return all data
    **Validates: Requirements 4.5**
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        from pathlib import Path
        manager = MetadataManager(Path(tmp_dir))
        
        # 创建没有date列的数据
        df = pd.DataFrame({
            'symbol': [f'SYM{i}' for i in range(num_rows)],
            'contract': [f'C{i}' for i in range(num_rows)]
        })
        
        # 保存数据
        manager.save_dominant_contracts(df)
        
        # 尝试过滤 - 应返回所有数据（因为没有date列）
        result = manager.get_dominant_contracts(date=filter_date)
        assert len(result) == num_rows
