#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import time
import logging
import threading

import backtrader as bt
from backtrader.utils.py3 import queue
from ccxtbt import CCXTStore
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager

logger = logging.getLogger('BTBinanceStore')


class BinanceStore(CCXTStore):
    '''API provider for Binance feed and broker classes.'''

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

    def __init__(self, currency='USDT', sandbox=False, retries=10, **kwargs):
        super().__init__(exchange='binance',
                         currency=currency,
                         sandbox=sandbox,
                         retries=retries,
                         **kwargs)

        self.subscribers = {}
        self.parsers = {
            'ACCOUNT_UPDATE': self._parse_account,
            'ORDER_TRADE_UPDATE': self._parse_order,
            'kline': self._parse_bar,
            '24hrTicker': self._parse_ticker,
            '24hrMiniTicker': self._parse_miniticker,
        }

        # binance websocket init
        exchange = 'binance.com'
        type = self.exchange.options['defaultType']
        if type == 'margin':
            exchange = f"{exchange}-margin"
        elif type == 'future':
            exchange = f"{exchange}-futures"

        if sandbox:
            exchange = f"{exchange}-testnet"
        self.ws = BinanceWebSocketApiManager(exchange=exchange)

        self._loop_stream()

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

    # subscribe
    # account
    def subscribe_account(self, **kwargs):
        return self.subscribe(['arr'], ['!userData'],
                              ['ACCOUNT_UPDATE', 'ORDER_TRADE_UPDATE'],
                              **kwargs)

    # https://binance-docs.github.io/apidocs/futures/en/#event-order-update
    def _parse_account(self, e):
        if e['e'] != 'ACCOUNT_UPDATE':
            raise RuntimeError(f"event {e} is not ACCOUNT_UPDATE")
        a = e['a']

        balances = []
        for b in a['B']:
            if b['a'] == self.currency:
                balance = dict(
                    asset=b['a'],
                    wallet=float(b['wb']),  # Wallet Balance
                    cross=float(b['cw']),  # Cross Wallet Balance
                )
                balances.append(balance)
                self._cash = balance['wallet']
                self._value = balance['cross']

        positions = []
        for p in a['P']:
            positions.append(
                dict(
                    symbol=p["s"],  # Symbol
                    amount=p["pa"],  # Position Amount
                    price=float(p["ep"]),  # Entry Price
                    accum=float(p["cr"]),  # (Pre-fee) Accumulated Realized
                    pnl=float(p["up"]),  # Unrealized PnL
                    margin_type=p["mt"],  # Margin Type
                    isolated=float(
                        p["iw"]),  # Isolated Wallet (if isolated position)
                    side=p["ps"],  # Position Side
                ))

        return dict(
            event=e['e'],
            event_time=e['E'],
            transaction_time=e['T'],
            account=dict(
                reason=a['m'],  # Event reason type
                balances=balances,
                positions=positions,
            ))

    def _parse_order(self, e):
        if e['e'] != 'ORDER_TRADE_UPDATE':
            raise RuntimeError(f"event {e} is not ORDER_TRADE_UPDATE")
        o = e['o']
        return dict(event=e['e'],
                    event_time=e['E'],
                    transaction_time=e['T'],
                    order=dict(
                        symbol=o['s'],
                        client_id=o["c"],
                        side=o["S"],
                        type=o["o"],
                        force="f",
                        quantity=float(o["q"]),
                        price=float(o["p"]),
                        avg_price=float(o["ap"]),
                        stop_price=float(o["sp"]),
                        exec_type=o["x"],
                        status=o["X"],
                        id=o["i"],
                        last_qty=float(o["l"]),
                        accum_qty=float(o["z"]),
                        last_price=float(o["L"]),
                        time=o["T"],
                        tradeid=o["t"],
                        bid=float(o["b"]),
                        ask=float(o["a"]),
                        maker=o["m"],
                        reduce_only=o["R"],
                        work_type=o["wt"],
                        origin_type=o["ot"],
                        position_side=o["ps"],
                        close_position=o["cp"],
                        profit=float(o["rp"]),
                        comm_asset=o.get("N", None),
                        comm=o.get("n", None),
                        active_price=float(o.get("AP", 0)),
                        call_rate=o.get("cr", None),
                    ))

    # bar
    def subscribe_bars(self, markets, interval, q=None, **kwargs):
        markets = [m.replace('/', '') for m in markets]
        channel = f"kline_{interval}"
        if q is not None:
            listeners = ["kline"]
        else:
            listeners = self._bar_listeners(markets, interval)

        return self.subscribe(channel, markets, listeners, q=q, **kwargs)

    def _bar_listeners(self, markets, interval):
        channels = []
        for m in markets:
            channels.append(self._bar_listener(m, interval))
        return channels

    def _bar_listener(self, market, interval):
        return f"kline_{market}_{interval}"

    def _parse_bar(self, e):
        if e['e'] != 'kline':
            raise RuntimeError(f"event {e} is not kline")
        b = e['k']

        event = dict(
            event=e['e'],
            event_time=e['E'],
            symbol=e['s'],
            bar=dict(
                start=b["t"],  # Kline start time
                end=b["T"],  # Kline close time
                symbol=b["s"],  # Symbol
                interval=b["i"],  # Interval
                first_tradeid=b["f"],  # First trade ID
                last_tradeid=b["L"],  # Last trade ID
                open=float(b["o"]),  # Open price
                close=float(b["c"]),  # Close price
                high=float(b["h"]),  # High price
                low=float(b["l"]),  # Low price
                volume=float(b["v"]),  # Base asset volume
                trades=b["n"],  # Number of trades
                closed=b["x"],  # Is this kline closed?
                quote_volume=float(b["q"]),  # Quote asset volume
                taker_base_volume=b["V"],  # Taker buy base asset volume
                taker_quote_volume=b["Q"],  # Taker buy quote asset volume
                ignore=b["B"],  # Ignore
            ))
        event['listeners'] = [
            "kline",
            self._bar_listener(event['symbol'], event['bar']['interval'])
        ]
        return event

    # ticker
    def subscribe_tickers(self, markets, **kwargs):
        markets = [m.replace('/', '') for m in markets]
        return self.subscribe('ticker', markets, ['24hrTicker'], **kwargs)

    def _parse_ticker(self, e):
        if e['e'] != '24hrTicker':
            raise RuntimeError(f"event {e} is not 24hrTicker")
        return dict(
            event=e['e'],
            event_time=e['E'],
            symbol=e["s"],  # Symbol
            change=float(e["p"]),  # Price change
            change_percent=float(e["P"]),  # Price change percent
            avg_price=float(e["w"]),  # Weighted average price
            last=float(e["c"]),  # Last price
            quantity=float(e["Q"]),  # Last quantity
            open=float(e["o"]),  # Open price
            high=float(e["h"]),  # High price
            low=float(e["l"]),  # Low price
            volume=float(e["v"]),  # Total traded base asset volume
            quote_volume=float(e["q"]),  # Total traded quote asset volume
            open_time=e["O"],  # Statistics open time
            close_time=e["C"],  # Statistics close time
            first_tradeid=e["F"],  # First trade ID
            last_tradeid=e["L"],  # Last trade Id
            trades=e["n"],  # Total number of trades
        )

    # mini ticker
    def subscribe_minitickers(self, markets, **kwargs):
        markets = [m.replace('/', '') for m in markets]
        return self.subscribe('miniTicker', markets, ['24hrMiniTicker'],
                              **kwargs)

    def _parse_miniticker(self, e):
        if e['e'] != '24hrMiniTicker':
            raise RuntimeError(f"event {e} is not 24hrMiniTicker")
        return dict(
            event=e['e'],
            event_time=e['E'],
            symbol=e["s"],  # Symbol
            close=float(e["c"]),  # Close price
            open=float(e["o"]),  # Open price
            high=float(e["h"]),  # High price
            low=float(e["l"]),  # Low price
            volume=float(e["v"]),  # Total traded base asset volume
            quote_volume=float(e["q"]),  # Total traded quote asset volume
        )

    # book ticker
    def subscribe_bookticker(self, markets, **kwargs):
        markets = [m.replace('/', '') for m in markets]
        return self.subscribe('bookTicker', markets, ['bookTicker'], **kwargs)

    def _parse_bookticker(self, e):
        if e['e'] != 'bookTicker':
            raise RuntimeError(f"event {e} is not bookTicker")
        return dict(
            event=e['e'],
            event_time=e['E'],
            transaction_time=e['T'],
            update_id=e["u"],
            symbol=e["s"],
            bid=float(e["b"]),
            bid_qty=float(e["B"]),
            ask=float(e["a"]),
            ask_qty=float(e["A"]),
        )

    # subscribe
    def subscribe(self, channels, markets, events, q=None, **kwargs):
        reused = False
        if q is not None:
            reused = True
        elif q is None:
            q = queue.Queue()

        sid = self.ws.create_stream(channels,
                                    markets,
                                    stream_label="dict",
                                    output="dict",
                                    api_key=self.exchange.apiKey,
                                    api_secret=self.exchange.secret,
                                    **kwargs)
        # set event listener
        for e in events:
            if e not in self.subscribers:
                self.subscribers[e] = []

            existed = False
            if reused:
                for sq in self.subscribers[e]:
                    if sq == q:
                        existed = True
                        break

            if not existed:
                self.subscribers[e].append(q)
        return q, sid

    def unsubscribe(self, stream_id, channels=[], markets=[], **kwargs):
        '''Return stream bool'''
        return self.ws.unsubscribe_from_stream(stream_id, channels, markets,
                                               **kwargs)

    # stream data loop
    def _loop_stream(self):
        t = threading.Thread(target=self._t_loop_stream)
        t.daemon = True
        t.start()

    def _t_loop_stream(self):
        while True:
            if self.ws.is_manager_stopping():
                self.stop()
                return

            buffer = self.ws.pop_stream_data_from_stream_buffer()
            if buffer is False:
                time.sleep(0.01)
                continue
            if buffer is not None:
                try:
                    # skip unwanted data
                    if 'result' in buffer and buffer['result'] is None:
                        continue

                    # handle msg
                    if 'data' in buffer:
                        buffer = buffer['data']

                    if 'e' in buffer:
                        name = buffer['e']
                    else:
                        raise RuntimeError(
                            f"buffer format is invalid: {buffer}")

                    if name not in self.parsers:
                        logger.info("event parser not found: %s", buffer)
                        continue

                    event = self.parsers[name](buffer)

                    if 'listeners' in event:
                        listeners = event['listeners']
                    else:
                        listeners = [name]

                    for listener in listeners:
                        if listener not in self.subscribers:
                            continue

                        for q in self.subscribers[listener]:
                            q.put(event)
                except Exception as e:
                    print(f"raw data: {buffer}")
                    print(f"exception: {e}")

    def stop(self):
        self.ws.stop_manager_with_all_streams()