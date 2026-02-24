from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="futures_db",
    version="0.1.0",
    description="期货高频行情数据管理系统",
    packages=find_packages(),
    install_requires=requirements,
    python_requires=">=3.8",
)
