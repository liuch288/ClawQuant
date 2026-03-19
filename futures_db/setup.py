from setuptools import setup, find_packages

setup(
    name="futures_db",
    version="0.1.2",
    description="期货高频行情数据管理系统",
    packages=find_packages(),
    install_requires=["pandas"],
    python_requires=">=3.8",
)
