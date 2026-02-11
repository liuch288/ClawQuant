"""自定义异常类"""


class FuturesDBError(Exception):
    """基础异常类"""
    pass


class InvalidDateFormatError(FuturesDBError):
    """日期格式错误"""
    pass


class InvalidFrequencyError(FuturesDBError):
    """不支持的频率"""
    pass


class InvalidSymbolError(FuturesDBError):
    """无效的品种代码"""
    pass


class DataNotFoundError(FuturesDBError):
    """数据不存在"""
    pass


class InvalidDataError(FuturesDBError):
    """数据无效"""
    pass
