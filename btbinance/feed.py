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

logger = logging.getLogger('BinanceFeed')


class MetaBinanceFeed(DataBase.__class__):
    def __init__(cls, name, bases, dct):
        # Initialize the class
        super(MetaBinanceFeed, cls).__init__(name, bases, dct)

        # Register with the store
        BinanceStore.DataCls = cls


class BinanceFeed(with_metaclass(MetaBinanceFeed, DataBase)):
    _StoreCls = BinanceStore

    params = dict(
        historical=False,
        backfill=True,
        tick=False,
        adjstarttime=False,
        qcheck=0.1,
    )
    store: BinanceStore = None

    _ST_LIVE, _ST_HISTORBACK, _ST_OVER = range(3)

    def __init__(self, **kwargs):
        self.store = self._StoreCls(**kwargs)
        self._q = queue.Queue()

    def haslivedata(self):
        return self._state == self._ST_LIVE and self._q

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
                self._timeframe,
                self._compression,
                self._q,
            )
        else:
            self._bars_stream(
                [self.p.dataname],
                self._timeframe,
                self._compression,
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
        dtserver = self.store.get_time()
        dtserver = datetime.utcfromtimestamp(dtserver / 1000)
        dtlocaldiff = dtserver - datetime.utcnow()

        while self._state != self._ST_OVER:
            # wait for next bar
            dtnow = datetime.utcnow() + dtlocaldiff
            dtnext = bar_starttime(timeframe, compression, dt=dtnow, offset=-1)
            waittime = (dtnext - dtnow).total_seconds()

            # listen waitrandom seconds before new bar comes
            if waittime > waitrandom:
                waittime = waittime - waitrandom
                logger.debug("wait %ss to get new bar", waittime)
                time.sleep(waittime)

            # get data
            tmp_q, stream_id = self.store.subscribe_bars(
                dataname,
                timeframe,
                compression,
            )

            waittimeout = waitrandom + 5
            self._get_closed_bar(tmp_q, q, stream_id, waittimeout)

    def _get_closed_bar(self, in_q, out_q, stream_id, timeout):
        """:Param timeout: timeout waitting for new bar in seconds
                    after that will get missing bar from history.
        """
        dtstart = datetime.utcnow()
        while self._state != self._ST_OVER:
            try:
                # if if lastest bar doesn't exist, get it from history then exit
                dtdiff = datetime.utcnow() - dtstart
                if dtdiff.total_seconds() > timeout:
                    logger.warn("timeout waitting for new bar[%d]", len(self))
                    self.store.unsubscribe(stream_id)
                    self._history_bars(self._q, since=dtstart.timestamp())
                    return

                # get bar info from raw bar stream
                msg = in_q.get(timeout=0.1)
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

    def _history_bars(self, q, since=None, limit=1500):
        if since is None:
            since = self.p.fromdate.timestamp()
        bars = []
        while self._state != self._ST_OVER:
            raws = self.store.fetch_ohlcv(
                symbol=self.p.dataname,
                timeframe=self._timeframe,
                compression=self._compression,
                since=since,
                limit=limit,
            )

            # empty result
            if len(raws) == 0:
                break

            # dtlast + 1 to skip duplicated last bar of old data is first bar of new data
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
            dtobj = bar_starttime(self._timeframe, self._compression, dtobj,
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
