"""高级使用示例"""

import pandas as pd
from pathlib import Path
from futures_db import FuturesDB, get_supported_frequencies

# 初始化数据库
db = FuturesDB(base_path="./advanced_data")

# 示例1: 查看支持的频率
print("=== 示例1: 查看支持的频率 ===")
frequencies = get_supported_frequencies()
print(f"支持的频率: {frequencies}")

# 示例2: 批量保存多个品种的数据
print("\n=== 示例2: 批量保存多个品种的数据 ===")
symbols = ['IF', 'IC', 'IH']
for symbol in symbols:
    data = pd.DataFrame({
        'timestamp': ['2024-01-01 09:30:00', '2024-01-01 09:31:00'],
        'open': [3000.0, 3001.0],
        'high': [3005.0, 3006.0],
        'low': [2995.0, 2996.0],
        'close': [3001.0, 3002.0],
        'volume': [1000, 1100]
    })
    csv_file = f'{symbol}_data.csv'
    data.to_csv(csv_file, index=False)
    db.save_kline_from_csv(csv_file, symbol=symbol, freq='1min', date='2024-01-01')
    print(f"{symbol} 数据已保存")

# 示例3: 批量读取多个品种的数据
print("\n=== 示例3: 批量读取多个品种的数据 ===")
all_data = {}
for symbol in symbols:
    df = db.get_kline(symbol=symbol, freq='1min', date='2024-01-01')
    all_data[symbol] = df
    print(f"{symbol}: {len(df)} 条记录")

# 示例4: 处理缺失数据
print("\n=== 示例4: 处理缺失数据（跳过不存在的日期）===")
# 只保存部分日期的数据
dates_to_save = ['2024-01-01', '2024-01-03', '2024-01-05']
for date in dates_to_save:
    data = pd.DataFrame({
        'timestamp': [f'{date} 09:30:00'],
        'open': [3000.0],
        'high': [3005.0],
        'low': [2995.0],
        'close': [3001.0],
        'volume': [1000]
    })
    csv_file = f'sparse_{date}.csv'
    data.to_csv(csv_file, index=False)
    db.save_kline_from_csv(csv_file, symbol='IF', freq='daily', date=date)

# 读取更大的日期范围（包含不存在的日期）
result = db.get_kline(symbol='IF', freq='daily', 
                     start_date='2024-01-01', end_date='2024-01-10')
print(f"请求10天数据，实际返回 {len(result)} 条记录（自动跳过缺失日期）")

# 示例5: 使用不同频率的数据
print("\n=== 示例5: 使用不同频率的数据 ===")
frequencies_to_test = ['1min', '5min', '1hour', 'daily']
for freq in frequencies_to_test:
    data = pd.DataFrame({
        'timestamp': ['2024-01-01 09:30:00'],
        'open': [3000.0],
        'high': [3005.0],
        'low': [2995.0],
        'close': [3001.0],
        'volume': [1000]
    })
    csv_file = f'{freq}_data.csv'
    data.to_csv(csv_file, index=False)
    db.save_kline_from_csv(csv_file, symbol='IF', freq=freq, date='2024-01-01')
    print(f"{freq} 频率数据已保存")

# 示例6: 错误处理
print("\n=== 示例6: 错误处理 ===")
try:
    # 尝试读取不存在的数据
    db.get_tick(symbol='NONEXIST', date='2024-01-01')
except FileNotFoundError as e:
    print(f"预期的错误: {e}")

try:
    # 尝试使用无效的日期格式
    db.get_tick(symbol='IF', date='20240101')
except Exception as e:
    print(f"预期的错误: {e}")

try:
    # 尝试使用不支持的频率
    db.save_kline_from_csv('test.csv', symbol='IF', freq='invalid', date='2024-01-01')
except Exception as e:
    print(f"预期的错误: {e}")

# 示例7: 数据覆盖
print("\n=== 示例7: 数据覆盖 ===")
# 第一次保存
data1 = pd.DataFrame({'value': [1, 2, 3]})
data1.to_csv('overwrite_test.csv', index=False)
db.save_tick_from_csv('overwrite_test.csv', symbol='TEST', date='2024-01-01')
result1 = db.get_tick(symbol='TEST', date='2024-01-01')
print(f"第一次保存: {len(result1)} 条记录")

# 第二次保存（覆盖）
data2 = pd.DataFrame({'value': [10, 20, 30, 40, 50]})
data2.to_csv('overwrite_test.csv', index=False)
db.save_tick_from_csv('overwrite_test.csv', symbol='TEST', date='2024-01-01')
result2 = db.get_tick(symbol='TEST', date='2024-01-01')
print(f"第二次保存（覆盖）: {len(result2)} 条记录")

print("\n=== 高级示例完成 ===")
