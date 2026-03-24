from setuptools import setup

setup(
    name="clickhouse-sidecar",
    version="1.0.0",
    description="embedded clickhouse manager for python",
    py_modules=["clickhouse_sidecar"],
    install_requires=["clickhouse-connect>=0.6.0"],
)
