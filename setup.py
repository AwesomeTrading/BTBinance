from setuptools import setup

setup(
    name='btbinance',
    version='1.0',
    description='Backtrader Binance Websocket broker',
    url='https://github.com/AwesomeTrading/BTBinance.git',
    author='Santatic',
    license='Private',
    packages=['btbinance'],
    install_requires=[
        'backtrader',
        'unicorn_binance_websocket_api',
        'pybinance @ git+ssh://git@github.com/AwesomeTrading/PyBinance.git@main'
    ],
)