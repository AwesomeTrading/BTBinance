#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import collections
import itertools
import threading
import time
import logging
import traceback
from typing import Final

from backtrader import BrokerBase, Order, BuyOrder, SellOrder
from backtrader.position import Position
from backtrader.utils.py3 import queue, with_metaclass

from .store import BinanceStore
from .utils import _val

logger = logging.getLogger('BinanceBroker')

# LIMIT', 'MARKET', 'STOP', 'STOP_MARKET', 'TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'TRAILING_STOP_MARKET
order_types: Final = {
    Order.Market: 'market',
    Order.Limit: 'limit',
    Order.Stop: 'stop_market',
    Order.StopLimit: 'stop',
}
order_types_reversed: Final = {v: k for k, v in order_types.items()}
order_types_reversed.update({
    'take_profit': Order.Limit,
    'take_profit_market': Order.Limit,
})

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
    'open': Order.Submitted,
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
        BinanceStore.BrokerCls = cls


class BinanceBroker(with_metaclass(MetaBinanceBroker, BrokerBase)):
    '''Broker implementation for Binance cryptocurrency trading library.
    This class maps the orders/positions from Binance to the
    internal API of ``backtrader``.

    Broker mapping added as I noticed that there differences between the expected
    order_types and retuned status's from canceling an order

    Added a new mappings parameter to the script with defaults.

    Added a get_balance function. Manually check the account balance and update brokers
    self.cash and self.value. This helps alleviate rate limit issues.

    Added a new get_wallet_balance method. This will allow manual checking of the any coins
        The method will allow setting parameters. Useful for dealing with multiple assets

    Modified getcash() and getvalue():
        Backtrader will call getcash and getvalue before and after next, slowing things down
        with rest calls. As such, th

    The broker mapping should contain a new dict for order_types and mappings like below:

    broker_mapping = {
        'order_types': {
            bt.Order.Market: 'market',
            bt.Order.Limit: 'limit',
            bt.Order.Stop: 'stop-loss', #stop-loss for kraken, stop for bitmex
            bt.Order.StopLimit: 'stop limit'
        },
        'mappings':{
            'closed_order':{
                'key': 'status',
                'value':'closed'
                },
            'canceled_order':{
                'key': 'result',
                'value':1}
                }
        }

    Added new private_end_point method to allow using any private non-unified end point

    '''

    order_types = {
        Order.Market: 'market',
        Order.Limit: 'limit',
        Order.Stop: 'stop',  # stop-loss for kraken, stop for bitmex
        Order.StopLimit: 'stop limit'
    }

    mappings = {
        'closed_order': {
            'key': 'status',
            'value': 'closed'
        },
        'canceled_order': {
            'key': 'status',
            'value': 'canceled'
        }
    }

    params = dict(rebuild=True)
    store: BinanceStore = None

    def __init__(self, broker_mapping=None, **kwargs):
        super(BinanceBroker, self).__init__()

        if broker_mapping is not None:
            try:
                self.order_types = broker_mapping['order_types']
            except KeyError:  # Might not want to change the order types
                pass
            try:
                self.mappings = broker_mapping['mappings']
            except KeyError:  # might not want to change the mappings
                pass

        self.store = BinanceStore(**kwargs)
        self.expires = collections.defaultdict(list)
        self.currency = self.store.currency

        self.positions = collections.defaultdict(Position)
        self.orders = collections.OrderedDict()  # orders by order id
        self.opending_orders = collections.defaultdict(
            list)  # pending transmission
        self.brackets = dict()  # confirmed brackets
        self._ocos = dict()
        self._ocol = collections.defaultdict(list)
        self.notifies = queue.Queue()  # holds orders which are notified

        self.cash, self.value = self.get_wallet_balance(self.currency)
        self.startingcash = self.cash
        self.startingvalue = self.value

    def start(self):
        super().start()
        self.store.start(broker=self)
        self._loop_account()

    def get_balance(self):
        return self.cash, self.value

    def get_wallet_balance(self, currency, params={}):
        balance = self.store.get_wallet_balance(currency, params=params)
        cash = balance['free'][currency] if balance['free'][currency] else 0
        value = balance['total'][currency] if balance['total'][currency] else 0
        return cash, value

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

    def live(self):
        # First time live data
        if self.p.rebuild:
            self.rebuild_environement()
            self.p.rebuild = False

    def rebuild_environement(self):
        """
        Rebuild positions and orders when restart strategy
        """
        if self.p.rebuild:
            self._rebuild_positions()
            self._rebuild_orders()
            self.p.rebuild = False

    def next(self):
        data = self.cerebro.datas[0]
        self._check_expire(data.datetime[0])

    ### expired
    def _add_expire(self, order):
        if not order.valid:
            return
        self.expires[order.valid].append(order)

    def _check_expire(self, at):
        if len(self.expires) == 0:
            return

        # buypass key lock when delete while in loop
        expired = [k for k in self.expires.keys() if k <= at]
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
        q, stream_id = self.store.subscribe_my_account()
        t = threading.Thread(target=self._t_loop_account,
                             args=(
                                 q,
                                 stream_id,
                             ),
                             daemon=True)
        t.start()

    def _t_loop_account(self, q, stream_id):
        while True:
            try:
                event = q.get(timeout=999)
            except queue.Empty:
                time.sleep(1)
                continue

            if 'account' in event:
                account = event['account']
                for balance in account['balances']:
                    self.cash = balance['wallet']
                    self.value = balance['cross']

                # logger.warn("Need to handle positions")
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
        positions = self.store.fetch_my_positions(symbols)
        self._on_positions(positions)

    def _on_positions(self, raws):
        for raw in raws:
            price = _val(raw, ['price', 'entryPrice'])
            price = 0 if price is None else float(price)

            size = _val(raw, ['amount', 'info.positionAmt'])
            size = 0 if size is None else float(size)

            symbol = raw['symbol']
            pos = self.positions[symbol]
            pos.set(size, price)

    ### order
    def orderstatus(self, order):
        return order.status

    # order update
    def _build_order_info(self, order: Order):
        info = f"bt:r_{order.ref}"
        if order.parent:
            info += f":p_{order.parent.ref}"
        if order.valid:
            info += f":exp_{order.valid}"
        if order.info.get('sl', False):
            info += f":sl"

        return info

    def _parse_order_info(self, raw):
        '''
        raw: bt:r_2:p_1:exp_123123.1232:sl
        '''
        info = dict(id=raw)
        if raw and raw.startswith("bt:"):
            splited = raw.split('bt:', 1)[1].split(':')
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
            if expire: info['expire'] = float(expire)

            # stoploss
            sl = pairs.get('sl', None)
            if sl: info['sl'] = True

        return info

    def _rebuild_orders(self):
        raws = self.store.fetch_my_open_orders()
        for raw in raws:
            self._on_order(raw)

    def _on_order(self, raw):
        logger.info(f'Raw order: {raw}')

        status = order_statuses_reversed[raw['status'].lower()]
        symbol = raw['symbol']
        price = raw['price']
        size = raw['amount']

        if status in [Order.Partial, Order.Completed]:
            size = raw['filled']
            price = raw['average']
        if not price:
            price = raw['stopPrice']
        if 'SELL' in raw['side'].upper():
            size = -size

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
            logger.warn(f"External order: {raw}")
            if status in [Order.Partial, Order.Completed]:
                profit = raw['profit']
                commission = raw['comm']
                if commission is None: commission = 0
                if type(commission) == str: commission = float(commission)
                self._fill_external(symbol, size, price, profit, commission)
            return

        # order ref not None, but didn't exist before
        if oref not in self.orders:
            Order.refbasis = itertools.count(oref)
            data = self._get_data(symbol)
            if not data:
                logger.warning(f"No data for symbol {symbol}")
                return

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
        # skip for order local modified and server cancel
        if order.info.get('modified', False) and status == Order.Canceled:
            return
        if order.info.get('modifiednew', False) and \
            status in [Order.Submitted, Order.Accepted]:
            del order.info['modifiednew']
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
        logger.debug("Fill order: {}, {}, {}, {}, {}, {}".format(
            order.ref, size, price, filled, profit, commission))

        if not order.alive():  # can be a bracket
            pref = getattr(order.parent, "ref", order.ref)
            if pref not in self.brackets:
                msg = ("Order fill received for {}, with price {} and size {} "
                       "but order is no longer alive and is not a bracket. "
                       "Unknown situation").format(order.ref, price, size)
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
            size = order.size

        data = order.data
        pos = self.getposition(data, clone=False)
        # psize, pprice, opened, closed = pos.update(size, price)
        psize, pprice, opened, closed = pos.size, pos.price, pos.size, pos.size - size
        # comminfo = self.getcommissioninfo(data)

        closedvalue = profit
        closedcomm = commission
        openedvalue = openedcomm = 0.0
        margin = pnl = 0.0

        order.addcomminfo(self.getcommissioninfo(data))
        order.execute(data.datetime[0], size, price, closed, closedvalue,
                      closedcomm, opened, openedvalue, openedcomm, margin, pnl,
                      psize, pprice)

        if order.executed.remsize:
            order.partial()
            self.notify(order)
        else:
            order.completed()
            self.notify(order)
            self._bracketize(order)
            self._ococheck(order)

    def _fill_external(self, symbol, size, price, profit, commission):
        logger.debug("Fill external order: {}, {}, {}, {}, {}".format(
            symbol, size, price, profit, commission))
        if size == 0:
            return

        data = self._get_data(symbol)
        if data is None:
            logger.warning(f"No data for symbol {symbol}")
            return

        # pos = self.getposition(data, clone=False)
        # pos.update(size, price)

        maker = BuyOrder if size > 0 else SellOrder
        order = maker(
            data=data,
            size=size,
            price=price,
            exectype=Order.Market,
            simulated=True,
        )

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
        if order.parent or order.info.get('sl', False):
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
        try:
            o = self.store.create_my_order(symbol=order.data._name,
                                           type=order_type,
                                           side=side,
                                           amount=amount,
                                           price=order.price,
                                           params=params)
            order.addinfo(id=o['id'])
            self._submit(order)
            return order
        except Exception as e:
            traceback.print_stack()
            logger.error(e)

    def modify(self, old: Order, new: Order):
        old.addinfo(modified=True)
        new.addinfo(modifiednew=True)
        logger.info(f"MODIFY ORDER:{old.ref} {new.ref}")

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
