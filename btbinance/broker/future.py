import collections
import itertools
import threading
import logging
from typing import Final
from datetime import datetime

import backtrader as bt
from backtrader import BrokerBase, Order, BuyOrder, SellOrder, Position
from backtrader.feed import DataBase
from backtrader.utils.py3 import queue, with_metaclass

from ..store import BinanceStore
from ..utils import _val

logger = logging.getLogger('BinanceFutureBroker')

# LIMIT', 'MARKET', 'STOP', 'STOP_MARKET', 'TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'TRAILING_STOP_MARKET
order_types: Final = {
    Order.Market: 'market',
    Order.Limit: 'limit',
    Order.Stop: 'stop_market',
    Order.StopLimit: 'stop',
}

order_statuses: Final = {
    Order.Created: 'open',
    Order.Submitted: 'open',
    Order.Accepted: 'open',
    # 'Partial',
    Order.Completed: 'closed',
    Order.Canceled: 'canceled',
    Order.Expired: 'expired',
    # 'Margin',
    Order.Rejected: 'rejected',
}
order_statuses_reversed: Final = {v: k for k, v in order_statuses.items()}
order_statuses_reversed.update({
    'open': Order.Accepted,
    'new': Order.Accepted,
    'filled': Order.Completed,
    'partially_filled': Order.Partial,
    'new_adl': Order.Margin,
    'new_insurance': Order.Margin,
})


class MetaBinanceBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaBinanceBroker, cls).__init__(name, bases, dct)


class BinanceFutureBroker(with_metaclass(MetaBinanceBroker, BrokerBase)):
    params = dict(
        rebuild=True,  # rebuild order at startup
        checkexpire=60,  # loop check order expire
    )
    store: BinanceStore = None

    def __init__(self, **kwargs):
        super().__init__()

        self.store = BinanceStore(**kwargs)
        self.expires = collections.defaultdict(list)
        self.currency = self.store.currency

        self.positions = collections.defaultdict(Position)
        self.orders = collections.OrderedDict()
        self.opending_orders = collections.defaultdict(list)
        self.brackets = dict()
        self.notifies = queue.Queue()
        self._ocos = dict()
        self._ocol = collections.defaultdict(list)
        self._isalive = True

        # balance
        self.cash, self.value = self.get_wallet_balance(self.currency)
        self.startingcash = self.cash
        self.startingvalue = self.value
        logger.info("Account init cash=%f, value=%f", self.cash, self.value)

    def start(self):
        super().start()
        self.store.start(broker=self)

    def stop(self):
        self._isalive = False

    def get_wallet_balance(self, currency, params={}):
        balance = self.store.get_wallet_balance(params=params)
        cash = balance['free'][currency] if balance['free'][currency] else 0
        value = balance['total'][currency] if balance['total'][currency] else 0
        return cash, value

    def get_balance(self):
        return self.cash, self.value

    def getcash(self):
        return self.cash

    def getvalue(self, datas=None):
        return self.value

    def get_notification(self):
        try:
            return self.notifies.get(False)
        except queue.Empty:
            return None

    def notify(self, order: Order):
        self.notifies.put(order.clone())

    def onlive(self):
        for d in self.cerebro.datas:
            if d._laststatus != DataBase.LIVE:
                return

        if self.p.rebuild:
            logger.info('rebuild positions & orders...')
            # self.p.rebuild = False
            self._rebuild_positions()
            self._rebuild_orders()

        # load dependencies for live trading
        self._loop_account()

    def next(self):
        self._check_expire()

    ### expired
    def _add_expire(self, order):
        if not order.valid:
            return
        self.expires[order.valid].append(order)

    def _datenow(self):
        tz = self.cerebro.datas[0]._tz
        return bt.date2num(tz.localize(datetime.utcnow()))

    def _check_expire(self):
        if len(self.expires) == 0:
            return

        # datetime now with server timezone
        now = self._datenow()

        # buypass key lock when delete while in loop
        expired = [k for k in self.expires.keys() if k <= now]
        for k in expired:
            for o in self.expires[k]:
                if o.alive():
                    self._expire(o)
                    self.cancel(o)
            del self.expires[k]

    ### data
    def _get_data(self, name):
        return self.cerebro.datasbyname.get(name, None)

    ### account
    def _loop_account(self):
        symbols = ",".join([d._name for d in self.cerebro.datas])
        label = "Account->" + symbols

        q, stream_id = self.store.subscribe_my_account(label=label,
                                                       symbols=symbols)
        t = threading.Thread(target=self._t_loop_account,
                             args=(q, stream_id),
                             daemon=True)
        t.start()

    def _t_loop_account(self, q, stream_id):
        while self._isalive:
            try:
                event = q.get(timeout=1)
            except queue.Empty:
                continue

            if 'account' in event:
                account = event['account']
                for balance in account['balances']:
                    self.cash = balance['wallet']
                    self.value = balance['cross']

                self._on_positions(account['positions'])

            elif 'order' in event:
                raw = event['order']
                self._on_order(raw)
            else:
                raise Exception(f"Event cannot handle: {event}")

    ### position
    def getposition(self, data, clone=True):
        pos = self.positions[data._dataname]
        if clone:
            pos = pos.clone()
        return pos

    def _rebuild_positions(self):
        symbols = self.cerebro.datasbyname.keys()
        raws = self.store.fetch_my_positions(symbols)
        self._on_positions(raws)

    def _on_positions(self, raws):
        for raw in raws:
            logger.info(f'Raw position: {raw}')

            price = _val(raw, ['price', 'entryPrice'])
            price = 0 if price is None else float(price)

            size = _val(raw, ['amount', 'info.positionAmt'])
            size = 0 if size is None else float(size)

            symbol = raw['symbol']
            pos = self.positions[symbol]
            pos.set(size, price)

    ### order
    def orderstatus(self, order):
        try:
            o = self.orders[order.ref]
        except ValueError:
            o = order

        return o.status

    # order update
    def _build_order_info(self, order: Order):
        info = f"bt-r_{order.ref}"
        if order.parent:
            info += f"-p_{order.parent.ref}"
        if order.valid:
            exp = round(order.valid * 1000000)
            info += f"-exp_{exp}"
        if order.info.get('sl', False):
            info += f"-sl"
        if order.info.get('tp', False):
            info += f"-tp"

        return info

    def _parse_order_info(self, raw):
        '''
        raw: bt-r_2-p_1-exp_1231231232-sl-tp
        '''
        info = dict(client_id=raw)
        if raw and raw.startswith("bt-"):
            splited = raw.split('bt-', 1)[1].split('-')
            pairs = dict()
            for s in splited:
                if '_' in s:
                    k, v = s.split('_', 1)
                    pairs[k] = v
                else:
                    pairs[s] = True

            # ref
            ref = pairs.get('r', None)
            if ref: info['ref'] = int(ref)

            # parent ref
            pref = pairs.get('p', None)
            if pref: info['pref'] = int(pref)

            # expire time
            expire = pairs.get('exp', None)
            if expire: info['expire'] = float(expire) / 1000000

            # stoploss
            sl = pairs.get('sl', None)
            if sl: info['sl'] = True

            # takeprofit
            tp = pairs.get('tp', None)
            if tp: info['tp'] = True

        return info

    def _rebuild_orders(self):
        raws = self.store.fetch_my_open_orders()
        for raw in raws:
            self._on_order(raw)

    def _on_order(self, raw):
        logger.info(f'Raw order: {raw}')

        symbol = raw['symbol']

        # filter symbol data
        data = self._get_data(symbol)
        if not data:
            logger.warn(f"No data for symbol {symbol}")
            return

        # order content
        status = order_statuses_reversed[raw['status'].lower()]
        price = raw['price']
        size = raw['amount']
        if status in [Order.Partial, Order.Completed]:
            size = raw['filled']
            price = raw['average']
        if not price:
            price = raw['stopPrice']
        if 'SELL' in raw['side'].upper():
            size = -size

        # order info
        client_id = raw.get('clientOrderId', None)
        info = self._parse_order_info(client_id)

        oref = info.get('ref', None)
        # find order ref by order id, order existed but it is external order, so it doesn't have info
        if not oref:
            for o in self.orders.values():
                if o.info.get('id', None) == raw['id']:
                    oref = o.ref
                    break

        # order still didn't exist before
        if not oref:
            logger.warn(f"External order {symbol} id={raw['id']}")
            if status in [Order.Partial, Order.Completed]:
                profit = raw['profit']
                commission = raw['comm']
                if commission is None: commission = 0
                if type(commission) == str: commission = float(commission)
                self._fill_external(
                    data,
                    size,
                    price,
                    profit,
                    commission,
                    id=raw['id'],
                    **info,
                )
            return

        # order ref not None, but didn't exist before
        if oref not in self.orders:
            Order.refbasis = itertools.count(oref)
            OrderObject = BuyOrder if size > 0 else SellOrder
            order = OrderObject(
                data=data,
                size=size,
                price=price,
                exectype=Order.Limit,
                simulated=True,
                valid=info.get('expire', None),
            )
            order.addinfo(
                id=raw['id'],
                sl=info.get('sl', None),
            )
            self._ocoize(order)
            self._add_expire(order)
            self.orders[oref] = order
        else:
            order = self.orders[oref]

        # skip for local expire and server cancel
        if order.info.get('expired', False) and status == Order.Canceled:
            return
        # skip for local stop market and server expire then complete
        if order.info.get('stopmarket', False) and status == Order.Accepted:
            return

        # execute and notify order
        if status == Order.Submitted:
            self._submit(order)
        elif status == Order.Accepted:
            self._accept(order)
        elif status == Order.Canceled:
            self._cancel(order)
        elif status in [Order.Partial, Order.Completed]:
            filled = status == Order.Completed
            profit = raw['profit']
            commission = raw['comm']
            if commission is None: commission = 0
            if type(commission) == str: commission = float(commission)
            self._fill(order,
                       size,
                       price,
                       filled=filled,
                       profit=profit,
                       commission=commission)
        elif status == Order.Rejected:
            self._reject(order)
        elif status == Order.Expired:
            # order expired just for execute stop market
            order.addinfo(stopmarket=True)
        else:
            raise Exception(f"Status {status} is invalid: {raw}")

    def _submit(self, order):
        if order.status == Order.Submitted:
            return

        order.submit(self)
        self.notify(order)
        # submit for stop order and limit order of bracket
        bracket = self.brackets.get(order.ref, [])
        for o in bracket:
            if o.ref != order.ref:
                self._submit(o)

    def _reject(self, order):
        order.reject(self)
        self.notify(order)
        self._bracketize(order, cancel=True)
        self._ococheck(order)

    def _accept(self, order):
        if order.status == Order.Accepted:
            return

        order.accept()
        self.notify(order)

        # accept for stop order and limit order of bracket
        if order.ref in self.brackets:
            bracket = self.brackets.get(order.ref, [])
            for o in bracket:
                if o.ref != order.ref:
                    self._accept(o)

    def _cancel(self, order):
        if order.status == Order.Canceled:
            return

        order.cancel()
        self.notify(order)
        self._bracketize(order, cancel=True)
        self._ococheck(order)

    def _expire(self, order: Order):
        # order.expire()
        # todo: quick fix while cannot set order status by function
        order.status = Order.Expired
        order.addinfo(expired=True)

        self.notify(order)
        self._bracketize(order, cancel=True)
        self._ococheck(order)

    def _fill(self,
              order: Order,
              size,
              price,
              filled=False,
              profit=0,
              commission=0,
              **kwargs):
        if size == 0 and not filled:
            return
        logger.debug("Fill order: %d, %f, %f, %f, %f, %f",
                     (order.ref, size, price, filled, profit, commission))

        if not order.alive():  # can be a bracket
            pref = getattr(order.parent, "ref", order.ref)
            if pref not in self.brackets:
                msg = (
                    f"Order fill received for {order.ref}, with price {price} and size {size} "
                    "but order is no longer alive and is not a bracket. "
                    "Unknown situation")
                self.store.put_notification(msg)
                return

            # [main, stopside, takeside], neg idx to array are -3, -2, -1
            stop_order = self.brackets[pref][-2]
            limit_order = self.brackets[pref][-1]

            # order type BUY, then stop and limit type SELL
            if order.ordtype == Order.Buy:
                if price >= limit_order.price:  # Limit order trigger when bid price over limit price
                    order = limit_order
                else:
                    order = stop_order
            # order type SELL, then stop and limit type BUY
            else:
                if price <= limit_order.price:  # Limit order trigger when ask price under limit price
                    order = limit_order
                else:
                    order = stop_order

        if filled:
            size = order.executed.remsize
            if order.issell():
                size = -size

        data = order.data
        # position update before order come, should clone=True
        pos = self.getposition(data, clone=True)
        psize, pprice, opened, closed = pos.update(size, price)
        # psize, pprice, opened, closed = pos.size, pos.price, pos.size, pos.size - size
        # comminfo = self.getcommissioninfo(data)

        closedvalue = profit
        closedcomm = commission
        openedvalue = openedcomm = 0.0
        margin = pnl = 0.0

        order.addcomminfo(self.getcommissioninfo(data))
        order.execute(data.datetime[0], size, price, closed, closedvalue,
                      closedcomm, opened, openedvalue, openedcomm, margin, pnl,
                      psize, pprice)

        # if order.executed.remsize:
        if not filled:
            order.partial()
            self.notify(order)
        else:
            if order.executed.remsize:
                logger.warn("Order %d execute remsize still existed: %s",
                            order.ref, order.executed.remsize)
            order.completed()
            self.notify(order)
            self._bracketize(order)
            self._ococheck(order)

    def _fill_external(self, data, size, price, profit, commission, **kwargs):
        logger.debug("Fill external order: {}, {}, {}, {}, {}".format(
            data._name, size, price, profit, commission))
        if size == 0:
            return

        # position update before order come, should clone=True
        pos = self.getposition(data, clone=True)
        pos.update(size, price)

        maker = BuyOrder if size > 0 else SellOrder
        order = maker(
            data=data,
            size=size,
            price=price,
            exectype=Order.Market,
            simulated=True,
        )
        order.addinfo(**kwargs)

        order.addcomminfo(self.getcommissioninfo(data))
        order.execute(0, size, price, 0, profit, commission, size, 0.0, 0.0,
                      0.0, 0.0, size, price)
        order.completed()

        self.notify(order)
        self._ococheck(order)

    # place order
    def _bracketize(self, order, cancel=False):
        pref = getattr(order.parent, "ref", order.ref)  # parent ref or self
        br = self.brackets.pop(pref, None)  # to avoid recursion
        if br is None:
            return

        if not cancel:
            if len(br) == 3:  # all 3 orders in place, parent was filled
                br = br[1:]  # discard index 0, parent
                for o in br:
                    o.activate()  # simulate activate for children
                self.brackets[pref] = br  # not done - reinsert children

            elif len(br) == 2:  # filling a children
                oidx = br.index(order)  # find index to filled (0 or 1)
                self._cancel(br[1 - oidx])  # cancel remaining (1 - 0 -> 1)
        else:
            # Any cancellation cancel the others
            for o in br:
                if o.alive():
                    self._cancel(o)

    def _ococheck(self, order: Order):
        if order.alive():
            raise Exception("Should not be called here")

        ocoref = self._ocos.pop(order.ref, order.ref)  # a parent or self
        ocol = self._ocol.pop(ocoref, None)
        if ocol:
            # cancel all order in oco group
            for oref in ocol:
                o = self.orders.get(oref, None)
                if o is not None and o.ref != order.ref:
                    self.cancel(o)

    def _ocoize(self, order):
        if order.oco is None:
            return

        ocoref = order.oco.ref
        oref = order.ref
        if ocoref not in self._ocos:
            self._ocos[oref] = ocoref
            self._ocol[ocoref].append(ocoref)  # add to group
        self._ocol[ocoref].append(oref)  # add to group

    def _transmit(self, order):
        oref = order.ref
        pref = getattr(order.parent, "ref", oref)  # parent ref or self

        if order.transmit:
            if oref != pref:  # children order
                # Put parent in orders dict, but add stopside and takeside
                # to order creation. Return the takeside order, to have 3s
                takeside = order  # alias for clarity
                parent, stopside = self.opending_orders.pop(pref)
                for o in parent, stopside, takeside:
                    self.orders[o.ref] = o  # write them down

                self.brackets[pref] = [parent, stopside, takeside]
                self._create_bracket(parent, stopside, takeside)
                return takeside  # parent was already returned

            else:  # Parent order, which is not being transmitted
                self.orders[order.ref] = order
                return self._create(order)

        # Not transmitting
        self.opending_orders[pref].append(order)
        return order

    def buy(self,
            owner,
            data,
            size,
            price=None,
            plimit=None,
            exectype=None,
            valid=None,
            tradeid=0,
            oco=None,
            trailamount=None,
            trailpercent=None,
            parent=None,
            transmit=True,
            **kwargs):

        order = BuyOrder(
            owner=owner,
            data=data,
            size=size,
            price=price,
            pricelimit=plimit,
            exectype=exectype,
            valid=valid,
            tradeid=tradeid,
            oco=oco,
            trailamount=trailamount,
            trailpercent=trailpercent,
            parent=parent,
            transmit=transmit,
        )
        return self._placing_order(order, **kwargs)

    def sell(self,
             owner,
             data,
             size,
             price=None,
             plimit=None,
             exectype=None,
             valid=None,
             tradeid=0,
             oco=None,
             trailamount=None,
             trailpercent=None,
             parent=None,
             transmit=True,
             **kwargs):

        order = SellOrder(
            owner=owner,
            data=data,
            size=size,
            price=price,
            pricelimit=plimit,
            exectype=exectype,
            valid=valid,
            tradeid=tradeid,
            oco=oco,
            trailamount=trailamount,
            trailpercent=trailpercent,
            parent=parent,
            transmit=transmit,
        )
        return self._placing_order(order, **kwargs)

    def _placing_order(self, order: Order, **kwargs):
        order.addinfo(**kwargs)
        # order.addcomminfo(self.getcommissioninfo(data))
        self._ocoize(order)
        self._add_expire(order)
        return self._transmit(order)

    def _create_bracket(self, order, stopside, takeside):
        o = self._create(order)
        sl = self._create(stopside)
        tp = self._create(takeside)

    def _create(self, order: Order):
        # param
        params = dict()
        if order.parent or \
            order.info.get('sl', False) or \
            order.info.get('tp', False):
            params['closePosition'] = True

        # order ref
        params['newClientOrderId'] = self._build_order_info(order)

        # order type
        order_type = order_types.get(order.exectype)
        if order.exectype in [Order.Limit, Order.Stop] and \
            params.get('closePosition', False):
            if order_type == 'limit':
                order_type = 'take_profit_market'
            elif order_type == 'stop':
                order_type = 'stop_market'

        if order_type in ['stop_market', 'take_profit_market']:
            params['stopPrice'] = order.price

        # order side
        side = "BUY" if order.isbuy() else "SELL"

        # amount
        amount = abs(order.size)

        o = self.store.create_my_order(symbol=order.data._name,
                                       type=order_type,
                                       side=side,
                                       amount=amount,
                                       price=order.price,
                                       params=params)
        order.addinfo(id=o['id'])
        self._submit(order)
        return order

    def modify(self, old: Order, new: Order):
        self.cancel(old)
        OrderFunc = self.buy if new.isbuy() else self.sell
        return OrderFunc(
            owner=new.owner,
            data=new.data,
            size=new.size,
            price=new.price,
            plimit=new.pricelimit,
            exectype=new.exectype,
            valid=new.valid,
            tradeid=new.tradeid,
            oco=new.oco,
            trailamount=new.trailamount,
            trailpercent=new.trailpercent,
            parent=new.parent,
            transmit=new.transmit,
            **new.info,
        )

    def cancel(self, order: Order):
        if not self.orders.get(order.ref, False):
            return
        if order.status == Order.Cancelled:  # already cancelled
            return

        id = order.info.get('id', None)
        if not id:
            raise Exception(f'Order doesnot have id {order}')

        return self.store.cancel_my_order(id, order.data._name)
