"""配置管理模块"""

from typing import List

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


def get_supported_frequencies() -> List[str]:
    """返回支持的频率列表"""
    return SUPPORTED_FREQUENCIES.copy()
