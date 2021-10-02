import logging
import collections
import backtrader as bt

from backtrader.metabase import MetaParams
from backtrader.utils.py3 import with_metaclass
from pybinance.api import PyBinanceWS

logger = logging.getLogger('BinanceStore')


class MetaSingleton(MetaParams):
    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (super(MetaSingleton,
                                    cls).__call__(*args, **kwargs))

        return cls._singleton


class BinanceStore(with_metaclass(MetaSingleton, object)):
    _TIMEFRAME_L2E = {
        (bt.TimeFrame.Minutes, 1): '1m',
        (bt.TimeFrame.Minutes, 3): '3m',
        (bt.TimeFrame.Minutes, 5): '5m',
        (bt.TimeFrame.Minutes, 15): '15m',
        (bt.TimeFrame.Minutes, 30): '30m',
        (bt.TimeFrame.Minutes, 60): '1h',
        (bt.TimeFrame.Minutes, 90): '90m',
        (bt.TimeFrame.Minutes, 120): '2h',
        (bt.TimeFrame.Minutes, 180): '3h',
        (bt.TimeFrame.Minutes, 240): '4h',
        (bt.TimeFrame.Minutes, 360): '6h',
        (bt.TimeFrame.Minutes, 480): '8h',
        (bt.TimeFrame.Minutes, 720): '12h',
        (bt.TimeFrame.Days, 1): '1d',
        (bt.TimeFrame.Days, 3): '3d',
        (bt.TimeFrame.Weeks, 1): '1w',
        (bt.TimeFrame.Weeks, 2): '2w',
        (bt.TimeFrame.Months, 1): '1M',
        (bt.TimeFrame.Months, 3): '3M',
        (bt.TimeFrame.Months, 6): '6M',
        (bt.TimeFrame.Years, 1): '1y',
    }
    _TIMEFRAME_E2L = {v: k for k, v in _TIMEFRAME_L2E.items()}

    DataCls = None  # data class will auto register

    def __init__(self, currency, key, secret, type='spot', **kwargs):
        config = {
            'apiKey': key,
            'secret': secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': type,
                'quoteOrderQty': False,
                'warnOnFetchOpenOrdersWithoutSymbol': False
            },
        }
        self.exchange = PyBinanceWS(currency=currency, config=config, **kwargs)
        self.currency = currency
        self.notifies = collections.deque(maxlen=1000)

    # Backtrader default function
    @classmethod
    def getdata(cls, *args, **kwargs):
        return cls.DataCls(*args, **kwargs)

    def start(self, data=None, broker=None):
        if data is None and broker is None:
            return

        if data is not None:
            self._env = data._env
        elif broker is not None:
            self.broker = broker

    def onlive(self):
        self.broker.onlive()

    def put_notification(self, msg, *args, **kwargs):
        self.notifies.append((msg, args, kwargs))

    def get_notifications(self):
        """Return the pending "store" notifications"""
        self.notifies.append(None)  # put a mark / threads could still append
        return [x for x in iter(self.notifies.popleft, None)]

    def stop(self):
        self.exchange.stop()
        logger.info("BinanceStore is stopping...")

    ### Internal
    def _get_exchange_timeframe(self, timeframe, compression):
        tf = self._TIMEFRAME_L2E.get((timeframe, compression))
        if tf is None:
            raise ValueError("backtrader CCXT module doesn't support timeframe %s, comression %s" % \
                             (bt.TimeFrame.getname(timeframe), compression))

        ex = self.exchange.exchange
        if ex.timeframes and tf not in ex.timeframes:
            raise ValueError("'%s' exchange doesn't support %s time frame" %
                             (ex.name, tf))

        return tf

    ### API
    def get_time(self):
        return self.exchange.get_time()

    def get_my_balance(self):
        return self.exchange.get_my_balance()

    def fetch_ohlcv(self, timeframe, compression, **kwargs):
        tf = self._get_exchange_timeframe(timeframe, compression)
        return self.exchange.fetch_ohlcv(timeframe=tf, **kwargs)

    def get_wallet_balance(self, params=None):
        return self.exchange.get_my_wallet_balance(params)

    def fetch_my_positions(self, symbols=None, params={}):
        return self.exchange.fetch_my_positions(symbols, params)

    def fetch_my_open_orders(self, **kwargs):
        return self.exchange.fetch_my_open_orders(**kwargs)

    def create_my_order(self, symbol, **kwargs):
        return self.exchange.create_my_order(symbol=symbol, **kwargs)

    def cancel_my_order(self, id, symbol):
        return self.exchange.cancel_my_order(id, symbol)

    ### WS
    def subscribe_bars(self, markets, timeframe, compression, **kwargs):
        tf = self._get_exchange_timeframe(timeframe, compression)
        return self.exchange.subscribe_bars(markets=markets,
                                            timeframe=tf,
                                            **kwargs)

    def subscribe_my_account(self, **kwargs):
        return self.exchange.subscribe_my_account(**kwargs)

    def unsubscribe(self, stream_id):
        return self.exchange.unsubscribe(stream_id)