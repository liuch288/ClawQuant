#!/usr/bin/env python
"""测试日期格式支持的简单脚本"""

from datetime import date
from futures_db.futures_db.utils import normalize_date
from futures_db.futures_db.exceptions import InvalidDateFormatError

print("=" * 60)
print("测试 FuturesDB 日期格式支持")
print("=" * 60)

# 测试1: 字符串格式
print("\n测试1: 字符串格式")
try:
    result = normalize_date("2024-01-15")
    print(f"✓ 输入: '2024-01-15' -> 输出: '{result}'")
    assert result == "2024-01-15"
except Exception as e:
    print(f"✗ 失败: {e}")

# 测试2: datetime.date对象
print("\n测试2: datetime.date对象")
try:
    test_date = date(2024, 1, 15)
    result = normalize_date(test_date)
    print(f"✓ 输入: date(2024, 1, 15) -> 输出: '{result}'")
    assert result == "2024-01-15"
except Exception as e:
    print(f"✗ 失败: {e}")

# 测试3: 无效的字符串格式
print("\n测试3: 无效的字符串格式")
try:
    result = normalize_date("2024/01/15")
    print(f"✗ 应该抛出异常但没有")
except InvalidDateFormatError as e:
    print(f"✓ 正确抛出异常: {e}")
except Exception as e:
    print(f"✗ 抛出了错误的异常类型: {e}")

# 测试4: 无效的类型
print("\n测试4: 无效的类型")
try:
    result = normalize_date(20240115)
    print(f"✗ 应该抛出异常但没有")
except InvalidDateFormatError as e:
    print(f"✓ 正确抛出异常: {e}")
except Exception as e:
    print(f"✗ 抛出了错误的异常类型: {e}")

# 测试5: 边界日期
print("\n测试5: 边界日期")
try:
    result1 = normalize_date(date(2024, 12, 31))
    result2 = normalize_date(date(2024, 1, 1))
    print(f"✓ 年末: {result1}")
    print(f"✓ 年初: {result2}")
    assert result1 == "2024-12-31"
    assert result2 == "2024-01-01"
except Exception as e:
    print(f"✗ 失败: {e}")

print("\n" + "=" * 60)
print("所有测试完成！")
print("=" * 60)
print("\n现在 FuturesDB 支持以下日期格式:")
print("  1. 字符串: '2024-01-15'")
print("  2. datetime.date对象: date(2024, 1, 15)")
print("\n所有接受日期参数的方法都支持这两种格式：")
print("  - get_tick(symbol, date)")
print("  - get_kline(symbol, freq, date=..., start_date=..., end_date=...)")
print("  - get_dominant_contracts(date=...)")
print("  - save_tick(df, symbol, date)")
print("  - save_kline(df, symbol, freq, date)")
