"""基本使用示例"""

import pandas as pd
from futures_db import FuturesDB

# 初始化数据库
db = FuturesDB(base_path="./data")

# 示例1: 保存tick数据
print("=== 示例1: 保存tick数据 ===")
# 创建示例tick数据
tick_data = pd.DataFrame({
    'timestamp': ['2024-01-01 09:30:00', '2024-01-01 09:30:01', '2024-01-01 09:30:02'],
    'price': [3000.0, 3001.0, 3002.0],
    'volume': [100, 150, 200]
})
# 保存为CSV
tick_data.to_csv('tick_sample.csv', index=False)
# 导入到数据库
db.save_tick_from_csv('tick_sample.csv', symbol='IF', date='2024-01-01')
print("Tick数据已保存")

# 示例2: 读取tick数据
print("\n=== 示例2: 读取tick数据 ===")
tick_df = db.get_tick(symbol='IF', date='2024-01-01')
print(tick_df)

# 示例3: 保存K线数据
print("\n=== 示例3: 保存K线数据 ===")
kline_data = pd.DataFrame({
    'timestamp': ['2024-01-01 09:30:00', '2024-01-01 09:31:00', '2024-01-01 09:32:00'],
    'open': [3000.0, 3001.0, 3002.0],
    'high': [3005.0, 3006.0, 3007.0],
    'low': [2995.0, 2996.0, 2997.0],
    'close': [3001.0, 3002.0, 3003.0],
    'volume': [1000, 1100, 1200]
})
kline_data.to_csv('kline_sample.csv', index=False)
db.save_kline_from_csv('kline_sample.csv', symbol='IF', freq='1min', date='2024-01-01')
print("K线数据已保存")

# 示例4: 读取单日K线数据
print("\n=== 示例4: 读取单日K线数据 ===")
kline_df = db.get_kline(symbol='IF', freq='1min', date='2024-01-01')
print(kline_df)

# 示例5: 保存多日K线数据并读取范围
print("\n=== 示例5: 保存多日K线数据并读取范围 ===")
# 保存第二天的数据
kline_data2 = pd.DataFrame({
    'timestamp': ['2024-01-02 09:30:00', '2024-01-02 09:31:00'],
    'open': [3010.0, 3011.0],
    'high': [3015.0, 3016.0],
    'low': [3005.0, 3006.0],
    'close': [3011.0, 3012.0],
    'volume': [1300, 1400]
})
kline_data2.to_csv('kline_sample2.csv', index=False)
db.save_kline_from_csv('kline_sample2.csv', symbol='IF', freq='1min', date='2024-01-02')

# 读取日期范围
kline_range = db.get_kline(symbol='IF', freq='1min', 
                           start_date='2024-01-01', end_date='2024-01-02')
print(f"读取到 {len(kline_range)} 条记录")
print(kline_range)

# 示例6: 保存和读取元数据
print("\n=== 示例6: 保存和读取元数据 ===")
metadata = pd.DataFrame({
    'date': ['2024-01-01', '2024-01-02'],
    'symbol': ['IF', 'IF'],
    'contract_code': ['IF2401', 'IF2401']
})
metadata.to_csv('metadata_sample.csv', index=False)
db.save_metadata_from_csv('metadata_sample.csv', metadata_type='dominant_contracts')

# 读取所有主力合约
all_contracts = db.get_dominant_contracts()
print("所有主力合约:")
print(all_contracts)

# 读取特定日期的主力合约
contracts_on_date = db.get_dominant_contracts(date='2024-01-01')
print("\n2024-01-01的主力合约:")
print(contracts_on_date)

print("\n=== 示例完成 ===")
