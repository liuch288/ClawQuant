# futures_db - 期货高频行情数据管理系统

一个用于管理期货市场高频行情数据的Python包。支持tick数据、多种频率K线数据以及元数据的存储和读取。

## 特性

- 📊 支持多种数据类型：tick数据、K线数据（1min、5min、15min、30min、1hour、daily）
- 💾 使用pickle格式高效存储pandas DataFrame
- 📁 自动管理目录结构
- 🔍 灵活的数据查询：单日查询、日期范围查询
- 📅 支持多种日期格式：字符串（YYYY-MM-DD）和datetime.date对象
- 📈 元数据管理：主力合约列表等
- ✅ 完整的数据验证和错误处理
- 🧪 100%测试覆盖，包括属性测试

## 安装

```bash
pip install -r requirements.txt
```

## 快速开始

### 基本使用

```python
from futures_db import FuturesDB
from datetime import date
import pandas as pd

# 初始化数据库
db = FuturesDB(base_path="./data")

# 保存tick数据
tick_df = pd.DataFrame({
    "timestamp": ["2024-01-01 09:30:00", "2024-01-01 09:30:01"],
    "price": [100.0, 101.0],
    "volume": [10, 20]
})
db.writer.save_tick(tick_df, symbol="IF", date="2024-01-01")

# 读取tick数据 - 支持字符串格式
data = db.get_tick(symbol="IF", date="2024-01-01")
print(data)

# 读取tick数据 - 支持datetime.date对象
data = db.get_tick(symbol="IF", date=date(2024, 1, 1))
print(data)
```

### 从CSV导入数据

```python
# 从CSV导入tick数据
db.save_tick_from_csv(
    csv_path="tick_data.csv",
    symbol="IF",
    date="2024-01-01"
)

# 从CSV导入K线数据
db.save_kline_from_csv(
    csv_path="kline_data.csv",
    symbol="IF",
    freq="1min",
    date="2024-01-01"
)
```

### K线数据查询

```python
from datetime import date

# 查询单日K线数据 - 字符串格式
kline = db.get_kline(symbol="IF", freq="1min", date="2024-01-01")

# 查询单日K线数据 - datetime.date对象
kline = db.get_kline(symbol="IF", freq="1min", date=date(2024, 1, 1))

# 查询日期范围K线数据 - 字符串格式
kline_range = db.get_kline(
    symbol="IF",
    freq="1min",
    start_date="2024-01-01",
    end_date="2024-01-10"
)

# 查询日期范围K线数据 - datetime.date对象
kline_range = db.get_kline(
    symbol="IF",
    freq="1min",
    start_date=date(2024, 1, 1),
    end_date=date(2024, 1, 10)
)

# 混合使用字符串和date对象
kline_range = db.get_kline(
    symbol="IF",
    freq="1min",
    start_date="2024-01-01",
    end_date=date(2024, 1, 10)
)
```

### 元数据管理

```python
from datetime import date

# 保存主力合约数据
contracts_df = pd.DataFrame({
    "date": ["2024-01-01", "2024-01-02"],
    "symbol": ["IF", "IC"],
    "contract": ["IF2401", "IC2401"]
})
db.metadata_manager.save_dominant_contracts(contracts_df)

# 读取主力合约（可选日期过滤）
all_contracts = db.get_dominant_contracts()

# 使用字符串格式过滤
contracts_on_date = db.get_dominant_contracts(date="2024-01-01")

# 使用datetime.date对象过滤
contracts_on_date = db.get_dominant_contracts(date=date(2024, 1, 1))
```

## 数据存储结构

```
data/
├── tick/
│   ├── IF/
│   │   ├── 2024-01-01.pkl
│   │   └── 2024-01-02.pkl
│   └── IC/
├── 1min/
│   ├── IF/
│   └── IC/
├── daily/
│   ├── IF/
│   └── IC/
└── metadata/
    └── dominant_contracts.pkl
```

## 支持的数据频率

- `tick` - 逐笔成交数据（仅支持单日查询）
- `1min` - 1分钟K线
- `5min` - 5分钟K线
- `15min` - 15分钟K线
- `30min` - 30分钟K线
- `1hour` - 1小时K线
- `daily` - 日K线

## 日期格式支持

所有接受日期参数的方法都支持以下两种格式：

1. **字符串格式**：`"YYYY-MM-DD"`
   ```python
   db.get_tick("IF", "2024-01-01")
   db.get_kline("IF", "1min", start_date="2024-01-01", end_date="2024-01-10")
   ```

2. **datetime.date对象**：
   ```python
   from datetime import date
   
   db.get_tick("IF", date(2024, 1, 1))
   db.get_kline("IF", "1min", start_date=date(2024, 1, 1), end_date=date(2024, 1, 10))
   ```

3. **混合使用**：
   ```python
   # 可以在同一个调用中混合使用两种格式
   db.get_kline("IF", "1min", start_date="2024-01-01", end_date=date(2024, 1, 10))
   ```

## API文档

### FuturesDB类

#### 初始化
```python
db = FuturesDB(base_path="./data")
```

#### 读取方法
- `get_tick(symbol, date)` - 读取tick数据
  - `date`: 支持字符串 "YYYY-MM-DD" 或 datetime.date 对象
- `get_kline(symbol, freq, date=None, start_date=None, end_date=None)` - 读取K线数据
  - `date`: 支持字符串 "YYYY-MM-DD" 或 datetime.date 对象
  - `start_date`, `end_date`: 支持字符串 "YYYY-MM-DD" 或 datetime.date 对象
- `get_metadata(metadata_type)` - 读取元数据
- `get_dominant_contracts(date=None)` - 读取主力合约
  - `date`: 支持字符串 "YYYY-MM-DD" 或 datetime.date 对象

#### 写入方法
- `save_tick_from_csv(csv_path, symbol, date)` - 从CSV导入tick数据
- `save_kline_from_csv(csv_path, symbol, freq, date)` - 从CSV导入K线数据
- `save_metadata_from_csv(csv_path, metadata_type)` - 从CSV导入元数据

## 错误处理

系统提供清晰的错误信息：

```python
from futures_db.exceptions import (
    InvalidDateFormatError,
    InvalidFrequencyError,
    InvalidSymbolError,
    InvalidDataError
)

try:
    db.get_tick("IF", "2024-13-01")  # 无效日期
except InvalidDateFormatError as e:
    print(f"日期格式错误: {e}")
```

## 运行测试

```bash
# 运行所有测试
pytest tests/ -v

# 运行属性测试
pytest tests/test_*_properties.py -v

# 运行单元测试
pytest tests/test_*.py -v --ignore=tests/test_*_properties.py
```

## 示例

查看 `examples/` 目录获取更多使用示例：
- `basic_usage.py` - 基本使用示例
- `advanced_usage.py` - 高级使用示例

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request！
