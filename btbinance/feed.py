import threading
import time
import logging
import random
from datetime import datetime, timedelta
from backtrader.feed import DataBase
from backtrader.utils.py3 import with_metaclass, queue

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
        qcheck=1,
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
                logger.debug("Wait %ss to get new bar", waittime)
                time.sleep(waittime)

            # get data
            tmp_q, stream_id = self.store.subscribe_bars(
                dataname,
                timeframe,
                compression,
            )

            timeout_at = dtnext + timedelta(seconds=5)
            self._get_closed_bar(tmp_q, q, stream_id, timeout=timeout_at)

    def _get_closed_bar(self, in_q, out_q, stream_id, timeout):
        """:Param timeout: timeout waitting for new bar in seconds
                    after that will get missing bar from history.
        """
        dtstart = datetime.utcnow()
        while self._state != self._ST_OVER:
            try:
                # if lastest bar doesn't exist, get it from history then exit
                if datetime.utcnow() > timeout:
                    logger.warn("Timeout waitting for new bar[%d]", len(self))
                    self.store.unsubscribe(stream_id)
                    # Get last completed bar by datetime from begin of that bar
                    fromdt = bar_starttime(self.p.timeframe,
                                           self.p.compression,
                                           dt=dtstart,
                                           offset=2)
                    self._history_bars(self._q,
                                       since=fromdt.timestamp(),
                                       limit=3)
                    return

                # get bar info from raw bar stream
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

    def _history_bars(self, q, since=None, limit=1500):
        if since is None:
            since = self.p.fromdate.timestamp()
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

        if self._laststatus == self.LIVE:
            currentdt = bar_starttime(self.p.timeframe,
                                      self.p.compression,
                                      dt=datetime.utcnow(),
                                      offset=1)
            currentdt = self.date2num(currentdt)
        else:
            currentdt = None

        while self._state == self._ST_LIVE:
            try:
                msg = self._q.get(timeout=self._qcheck)
            except queue.Empty:
                if currentdt is not None and \
                    self.lines.datetime[0] < currentdt:
                    since = self.num2date(self.lines.datetime[0])
                    logger.warn("Load missing bar[%d] from %s",
                                (len(self), since))
                    self._history_bars(self._q, since=since, limit=3)
                    continue
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
                if currentdt is not None and \
                    self.lines.datetime[0] < currentdt:
                    self.forward()
                    continue
                return True

    def _put_bar(self, msg):
        dtobj = datetime.utcfromtimestamp(float(msg[0] / 1000))
        dt = self.date2num(dtobj)
        dt1 = self.lines.datetime[-1]

        # Don't handle update current bar
        if dt <= dt1:
            logger.warn(f'Old bar {msg}')
            return False  # time already seen
        # if dt == dt1:
        #     self.backwards(force=True)

        # Common fields
        self.lines.datetime[0] = dt
        self.lines.open[0] = float(msg[1])
        self.lines.high[0] = float(msg[2])
        self.lines.low[0] = float(msg[3])
        self.lines.close[0] = float(msg[4])
        self.lines.volume[0] = float(msg[5])
        self.lines.openinterest[0] = 0.0
        return True
