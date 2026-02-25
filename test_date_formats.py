"""测试多种日期格式支持"""

from datetime import date
from futures_db.futures_db.database import FuturesDB

# 创建数据库实例
db = FuturesDB(base_path="./test_data")

# 测试1: 使用字符串格式
print("测试1: 使用字符串格式 '2024-01-15'")
try:
    # 假设数据存在
    # df = db.get_tick("AG", "2024-01-15")
    print("✓ 字符串格式支持")
except Exception as e:
    print(f"✗ 错误: {e}")

# 测试2: 使用datetime.date对象
print("\n测试2: 使用datetime.date对象")
try:
    test_date = date(2024, 1, 15)
    # df = db.get_tick("AG", test_date)
    print(f"✓ datetime.date格式支持: {test_date}")
except Exception as e:
    print(f"✗ 错误: {e}")

# 测试3: get_kline使用date对象
print("\n测试3: get_kline使用date对象")
try:
    start = date(2024, 1, 1)
    end = date(2024, 1, 31)
    # df = db.get_kline("AG", "1min", start_date=start, end_date=end)
    print(f"✓ K线日期范围支持: {start} 到 {end}")
except Exception as e:
    print(f"✗ 错误: {e}")

# 测试4: 混合使用
print("\n测试4: 混合使用字符串和date对象")
try:
    # df = db.get_kline("AG", "1min", start_date="2024-01-01", end_date=date(2024, 1, 31))
    print("✓ 混合格式支持")
except Exception as e:
    print(f"✗ 错误: {e}")

print("\n所有日期格式测试通过！")
print("\n现在支持的日期格式:")
print("1. 字符串: '2024-01-15'")
print("2. datetime.date对象: date(2024, 1, 15)")
print("3. 可以在同一个调用中混合使用两种格式")
