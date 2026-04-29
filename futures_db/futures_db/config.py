"""配置管理模块"""

import os
from typing import List, Literal

# 支持的数据频率
SUPPORTED_FREQUENCIES = [
    "tick",
    "1min",
    "5min",
    "15min",
    "30min",
    "1hour",
    "daily"
]

# 日期格式
DATE_FORMAT = "%Y-%m-%d"

# 元数据类型
METADATA_TYPES = {
    "dominant_contracts": "主力合约"
}

# 压缩方法类型
CompressionType = Literal[None, 'gzip', 'bz2', 'zip', 'xz', 'zstd']

# 默认压缩方法
DEFAULT_COMPRESSION: CompressionType = 'gzip'

# 默认数据目录（可从环境变量 FUTURESDB_PATH 覆盖）
DEFAULT_DATA_PATH = os.environ.get("FUTURESDB_PATH", os.path.join(os.path.expanduser("~"), "data", "futures_db"))


def get_supported_frequencies() -> List[str]:
    """返回支持的频率列表"""
    return SUPPORTED_FREQUENCIES.copy()
