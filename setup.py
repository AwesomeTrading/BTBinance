from setuptools import setup, find_packages
from pip._internal.req import parse_requirements

install_requires = parse_requirements('requirements.txt', session='hack')
install_requires = [str(ir.requirement) for ir in install_requires]

setup(
    name='btbinance',
    version='1.0',
    description='Backtrader Binance Websocket broker',
    url='https://github.com/AwesomeTrading/BTBinance.git',
    author='Santatic',
    license='Private',
    packages=find_packages(include=['btbinance', 'btbinance.*']),
    install_requires=install_requires,
)