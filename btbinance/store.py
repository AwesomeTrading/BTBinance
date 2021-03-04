#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2017 Ed Bartosh <bartosh@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import time
import threading
from datetime import datetime
from functools import wraps

import backtrader as bt
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import queue, with_metaclass
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager


class MetaSingleton(MetaParams):
    '''Metaclass to make a metaclassed class a singleton'''
    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (super(MetaSingleton,
                                    cls).__call__(*args, **kwargs))

        return cls._singleton


class BinanceStore(with_metaclass(MetaSingleton, object)):
    '''API provider for Binance feed and broker classes.

    Added a new get_wallet_balance method. This will allow manual checking of the balance.
        The method will allow setting parameters. Useful for getting margin balances

    Added new private_end_point method to allow using any private non-unified end point

    '''

    # Supported granularities
    _INTERVALS = {
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
    _INTERVALS_REVERSED = {v: k for k, v in _INTERVALS.items()}

    BrokerCls = None  # broker class will auto register
    DataCls = None  # data class will auto register

    @classmethod
    def getdata(cls, *args, **kwargs):
        '''Returns ``DataCls`` with args, kwargs'''
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        '''Returns broker with *args, **kwargs from registered ``BrokerCls``'''
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self, config, sandbox=False):
        if sandbox:
            exchange = "binance.com-futures-testnet"
        else:
            exchange = "binance.com-futures"
        self.exchange = BinanceWebSocketApiManager(exchange=exchange)
        self.stream_id = self.subscribe(["arr"], ["!userData"],
                                        api_key=config['api_key'],
                                        api_secret=config['api_secret'])

        # self._loop_stream()

    def from_interval(self, interval):
        timeframe = self._INTERVALS_REVERSED.get(interval)
        if timeframe is None:
            raise RuntimeError(f"Interval {interval} is not support")
        return timeframe

    def to_interval(self, timeframe, compression):
        interval = self._INTERVALS.get((timeframe, compression))
        if interval is None:
            raise RuntimeError(
                f"Interval for {timeframe, compression} is not support")
        return interval

    def subscribe(self, *args, **kwargs):
        return self.exchange.create_stream(*args,
                                           **kwargs,
                                           stream_label="raw_data",
                                           output="raw_data")

    def _loop_stream(self):
        t = threading.Thread(target=self._t_loop_stream)
        t.daemon = True
        t.start()

    def _t_loop_stream(self):
        while True:
            if self.exchange.is_manager_stopping():
                self.stop()
                return

            buffer = self.exchange.pop_stream_data_from_stream_buffer()
            if buffer is False:
                time.sleep(0.01)
                continue
            if buffer is not None:
                try:
                    if buffer['event_time'] >= \
                            buffer['kline']['kline_close_time']:
                        # print only the last kline
                        print(f"UnicornFy: {buffer}")
                except KeyError:
                    print(f"dict: {buffer}")
                except TypeError:
                    print(f"raw_data: {buffer}")

    def stop(self):
        pass