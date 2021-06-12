#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import threading
import time
import logging
import random
from datetime import datetime, timedelta

from backtrader.feed import DataBase
from backtrader.utils.py3 import with_metaclass, queue
from backtrader import date2num

from .store import BinanceStore
from .utils import bar_starttime

logger = logging.getLogger('BTBinanceFeed')


class MetaBinanceFeed(DataBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaBinanceFeed, cls).__init__(name, bases, dct)

        # Register with the store
        BinanceStore.DataCls = cls


class BinanceFeed(with_metaclass(MetaBinanceFeed, DataBase)):
    """
    CryptoCurrency eXchange Trading Library Data Feed.
    Params:
      - ``historical`` (default: ``False``)
        If set to ``True`` the data feed will stop after doing the first
        download of data.
        The standard data feed parameters ``fromdate`` and ``todate`` will be
        used as reference.
      - ``backfill_start`` (default: ``True``)
        Perform backfilling at the start. The maximum possible historical data
        will be fetched in a single request.
    """

    params = dict(
        historical=False,
        backfill=True,
        tick=False,
        adjstarttime=False,
    )

    _store = BinanceStore

    # States for the Finite State Machine in _load
    _ST_LIVE, _ST_HISTORBACK, _ST_OVER = range(3)

    def __init__(self, **kwargs):
        self.store = self._store(**kwargs)
        self._data = queue.Queue()

    def haslivedata(self):
        return self._state == self._ST_LIVE and self._data

    def islive(self):
        return not self.p.historical

    def start(self):
        super().start()
        timeframe = self.store.get_exchange_timeframe(self._timeframe,
                                                      self._compression)

        self.put_notification(self.DELAYED)

        if self.p.fromdate:
            self._state = self._ST_HISTORBACK
            self._history_bars(
                self.p.dataname,
                timeframe,
                self.p.fromdate,
                self._data,
            )

        self._data.put("LIVE")
        self._state = self._ST_LIVE

        if self.p.tick:
            self.store.subscribe_bars(
                [self.p.dataname],
                timeframe,
                self._data,
            )
        else:
            self._bars_stream(
                [self.p.dataname],
                timeframe,
                self._data,
            )

    def _bars_stream(self, dataname, timeframe, q):
        t = threading.Thread(target=self._t_thread_bars_stream,
                             args=(
                                 dataname,
                                 timeframe,
                                 q,
                             ),
                             daemon=True)
        t.start()

    def _t_thread_bars_stream(self, dataname, timeframe, q):
        waitrandom = random.randint(5, 15)
        while True:
            # wait for next bar
            dtnow = datetime.utcnow()
            dtnext = bar_starttime(self._timeframe,
                                   self._compression,
                                   dt=dtnow,
                                   offset=-1)
            dtdiff = dtnext - dtnow
            waittime = (dtdiff.days * 24 * 60 * 60) + dtdiff.seconds

            # listen before new bar completes
            if waittime > waitrandom:
                waittime = waittime - waitrandom
                logger.debug("get new bars: sleep for %ss", waittime)
                time.sleep(waittime)

            # get data
            tmp_q, stream_id = self.store.subscribe_bars(
                dataname,
                timeframe,
            )
            self._get_closed_bar(tmp_q, q, stream_id)

    def _get_closed_bar(self, in_q, out_q, stream_id):
        while True:
            try:
                msg = in_q.get(timeout=1)
            except queue.Empty:
                time.sleep(0.1)
                continue

            msg = msg['bar']
            if msg['closed']:
                bar = [
                    msg['start'],
                    msg['open'],
                    msg['high'],
                    msg['low'],
                    msg['close'],
                    msg['volume'],
                ]
                out_q.put(bar)
                return self.store.unsubscribe(stream_id)

    def _history_bars(self, dataname, timeframe, fromdate, q, limit=1500):
        bars = []
        begindate = fromdate.timestamp()
        while True:
            raws = self.store.fetch_ohlcv(
                dataname,
                timeframe,
                since=begindate,
                limit=limit,
            )

            if len(raws) == 0:
                break

            bars.extend(raws)
            if len(raws) < limit:
                break

            begindate = raws[-1][0] + 1

        # remove latest uncompleted bar
        bars = bars[:-1]

        for i in range(0, len(bars)):
            if i == 0 or bars[i][0] <= bars[i - 1][0]:
                continue
            q.put(bars[i])

        return q

    def _load(self):
        if self._state == self._ST_OVER:
            return False

        while True:
            if self._state == self._ST_LIVE:
                try:
                    msg = self._data.get(timeout=self._qcheck)
                except queue.Empty:
                    time.sleep(0.1)
                    return None

                if isinstance(msg, list):
                    ret = self._put_bar(msg)
                elif isinstance(msg, str):
                    if msg == "LIVE" and \
                        self._laststatus != self.LIVE:
                        self.put_notification(self.LIVE)
                    ret = None
                else:
                    msg = msg['bar']
                    bar = [
                        msg['start'],
                        msg['open'],
                        msg['high'],
                        msg['low'],
                        msg['close'],
                        msg['volume'],
                    ]
                    if self.p.tick:
                        ret = self._put_bar(bar)
                    elif msg['closed']:
                        ret = self._put_bar(bar)
                    else:
                        ret = None
                if ret:
                    return True

    def _put_bar(self, msg):
        dtobj = datetime.utcfromtimestamp(float(msg[0] / 1000))
        if self.p.adjstarttime:
            # move time to start time of next bar
            # and subtract 0.1 miliseconds (ensures no
            # rounding issues, 10 microseconds is minimum)
            dtobj = bar_starttime(self.p.timeframe, self.p.compression, dtobj,
                                  -1) - timedelta(microseconds=100)
        dt = date2num(dtobj, tz=self.p.tz)
        dt1 = self.lines.datetime[-1]
        if dt < dt1:
            return False  # time already seen
        if dt == dt1:
            self.backwards(force=True)

        # Common fields
        self.lines.datetime[0] = dt
        self.lines.open[0] = float(msg[1])
        self.lines.high[0] = float(msg[2])
        self.lines.low[0] = float(msg[3])
        self.lines.close[0] = float(msg[4])
        self.lines.volume[0] = float(msg[5])
        self.lines.openinterest[0] = 0.0
        return True
