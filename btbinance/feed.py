import threading
import time
import logging
import random
import math
import pytz
import gc
from datetime import datetime, timedelta
import backtrader as bt
from backtrader.utils.py3 import with_metaclass, queue

from .store import BinanceStore
from .utils import bar_starttime

logger = logging.getLogger('BinanceFeed')


class MetaBinanceFeed(bt.DataBase.__class__):
    def __init__(cls, name, bases, dct):
        # Initialize the class
        super(MetaBinanceFeed, cls).__init__(name, bases, dct)

        # Register with the store
        BinanceStore.DataCls = cls


class BinanceFeed(with_metaclass(MetaBinanceFeed, bt.DataBase)):
    _StoreCls = BinanceStore

    params = dict(
        historical=False,
        backfill=True,
        tick=False,
        qcheck=1,
        tz=pytz.UTC,
    )
    store: BinanceStore = None

    _ST_LIVE, _ST_HISTORBACK, _ST_OVER = range(3)

    def __init__(self, **kwargs):
        self.store = self._StoreCls(**kwargs)
        self._q = queue.Queue()

    def haslivedata(self):
        return bool(self._laststatus == self.LIVE and self._q)

    def islive(self):
        return not self.p.historical

    def stop(self):
        self._state = self._ST_OVER
        logger.info("BinanceFeed %s is stopping...", self._name)

    def start(self):
        super().start()

        # Kickstart store and get queue to wait on
        self.store.start(data=self)

        self.put_notification(self.DELAYED)

        if self.p.fromdate:
            self._state = self._ST_HISTORBACK
            self._history_bars(self._q)

        self._q.put("LIVE")
        self._state = self._ST_LIVE

        if self.p.tick:
            self.store.subscribe_bars(
                [self.p.dataname],
                self.p.timeframe,
                self.p.compression,
                self._q,
            )
        else:
            self._bars_stream(
                [self.p.dataname],
                self.p.timeframe,
                self.p.compression,
                self._q,
            )

    def _bars_stream(self, dataname, timeframe, compression, q):
        t = threading.Thread(target=self._t_thread_bars_stream,
                             args=(
                                 dataname,
                                 timeframe,
                                 compression,
                                 q,
                             ),
                             daemon=True)
        t.start()

    def _t_thread_bars_stream(self, dataname, timeframe, compression, q):
        waitrandom = random.randint(8, 18)

        # get difference of local and server time
        dtserver = self.store.fetch_time()
        dtserver = datetime.fromtimestamp(dtserver / 1000, tz=self.p.tz)
        dtlocaldiff = dtserver - datetime.now(tz=self.p.tz)

        while self._state != self._ST_OVER:
            # wait for next bar
            dtnow = datetime.now(tz=self.p.tz) + dtlocaldiff
            dtnext = bar_starttime(timeframe, compression, dt=dtnow, ago=-1)

            waittime = (dtnext - dtnow).total_seconds()

            # listen waitrandom seconds before new bar comes
            if waittime > waitrandom:
                waittime = waittime - waitrandom
                logger.debug("Wait %ss to get new bar", waittime)
                time.sleep(waittime)

            # get data
            tmp_q, stream_id = self.store.subscribe_bars(
                dataname,
                timeframe,
                compression,
            )
            timeout_at = dtnext + timedelta(seconds=20)
            self._get_closed_bar(tmp_q,
                                 q,
                                 stream_id,
                                 at=dtnext,
                                 timeout=timeout_at)

            # Clear RAM
            gc.collect()

    def _get_closed_bar(self, in_q, out_q, stream_id, at, timeout):
        """:Param timeout: timeout waitting for new bar in seconds
                    after that will get missing bar from history.
        """
        bar_time = math.floor(at.timestamp() * 1000)
        while self._state != self._ST_OVER:
            try:
                if datetime.now(tz=self.p.tz) > timeout:
                    logger.warn("Timeout waitting for new bar[%d]", len(self))
                    break

                msg = in_q.get(timeout=0.5)
            except queue.Empty:
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
            else:
                if msg['start'] > bar_time:
                    break

        logger.warn(f'Get live bar {at} from history')
        startdt = bar_starttime(self.p.timeframe,
                                self.p.compression,
                                dt=at,
                                ago=3)
        self._history_bars(self._q, since=startdt, limit=3)

    def _history_bars(self, q, since=None, limit=1500):
        if since is None:
            since = self.p.fromdate
            since.replace(tzinfo=self.p.tz)
        since = since.timestamp()

        bars = []
        while self._state != self._ST_OVER:
            raws = self.store.fetch_ohlcv(
                symbol=self.p.dataname,
                timeframe=self.p.timeframe,
                compression=self.p.compression,
                since=since,
                limit=limit,
            )

            # empty result
            if len(raws) == 0:
                break

            # dtlast + 1 to skip duplicated last bar of old request is first bar of new request
            dtlast = raws[-1][0] + 1

            # same result
            if since == dtlast:
                break

            bars.extend(raws)

            # result is small, mean no more data
            if len(raws) < limit:
                break

            # continue with new path of data
            since = dtlast

        # remove latest uncompleted bar
        bars = bars[:-1]

        for i in range(0, len(bars)):
            q.put(bars[i])

        return q

    def _load(self):
        if self._state == self._ST_OVER:
            return False

        while self._state == self._ST_LIVE:
            try:
                msg = self._q.get(timeout=self._qcheck)
            except queue.Empty:
                return None

            if isinstance(msg, list):
                ret = self._put_bar(msg)
            elif isinstance(msg, str):
                if msg == "LIVE" and \
                    self._laststatus != self.LIVE:
                    self.put_notification(self.LIVE)
                    # broadcast on live bar event
                    self.store.onlive()
                ret = False
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
                    ret = False
            if ret:
                return True

    def _put_bar(self, msg):
        bar = self._build_bar(msg)

        dt = bar[self.DateTime]
        dt1 = self.lines.datetime[-1]

        if dt < dt1:
            logger.warn(f'Old bar {msg}')
            return False  # time already seen
        if dt == dt1:
            self._updatebar(bar, forward=False, ago=1)
            return False

        self._updatebar(bar, forward=False, ago=0)
        return True

    def _build_bar(self, msg):
        dtobj = datetime.utcfromtimestamp(float(msg[0] / 1000))
        dt = self.date2num(dtobj)

        bar = [0] * self.size()
        bar[self.DateTime] = dt
        bar[self.Open] = float(msg[1])
        bar[self.High] = float(msg[2])
        bar[self.Low] = float(msg[3])
        bar[self.Close] = float(msg[4])
        bar[self.Volume] = float(msg[5])

        return bar
