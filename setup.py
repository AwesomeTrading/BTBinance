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
        'https://github.com/AwesomeTrading/PyBinance/tarball/main#egg=pybinance-1.0'
    ],
)