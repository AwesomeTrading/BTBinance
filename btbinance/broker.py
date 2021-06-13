#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import collections
import threading
import time
import logging
from typing import Final

from backtrader import BrokerBase, Order, BuyOrder, SellOrder
from backtrader.position import Position
from backtrader.utils.py3 import queue, with_metaclass

from .store import BinanceStore

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
    'new': Order.Accepted,
    'filled': Order.Completed,
    'partially_filled': Order.Partial,
    'new_adl': Order.Margin,
    'new_insurance': Order.Margin,
})

# class BinanceOrder(OrderBase):
#     def __init__(self, owner, data, ccxt_order):
#         self.owner = owner
#         self.data = data
#         self.ccxt_order = ccxt_order
#         self.executed_fills = []
#         self.ordtype = self.Buy if ccxt_order['side'] == 'buy' else self.Sell
#         self.size = float(ccxt_order['amount'])

#         super(BinanceOrder, self).__init__()


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

    def __init__(self, broker_mapping=None, debug=False, **kwargs):
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

        self.currency = self.store.currency
        self.debug = debug
        self.indent = 4  # For pretty printing dictionaries

        self.positions = collections.defaultdict(Position)
        self.orders = collections.OrderedDict()  # orders by order id
        # self.open_orders = list()
        self.opending_orders = collections.defaultdict(
            list)  # pending transmission
        self.brackets = dict()  # confirmed brackets
        self._ocos = dict()
        self._ocol = collections.defaultdict(list)
        self.notifies = queue.Queue()  # holds orders which are notified

        self.startingcash = self.store._cash
        self.startingvalue = self.store._value

    def start(self):
        super().start()
        self._loop_account()

    def get_balance(self):
        self.store.get_balance()
        self.cash = self.store._cash
        self.value = self.store._value
        return self.cash, self.value

    def get_wallet_balance(self, currency, params={}):
        balance = self.store.get_wallet_balance(currency, params=params)
        cash = balance['free'][currency] if balance['free'][currency] else 0
        value = balance['total'][currency] if balance['total'][currency] else 0
        return cash, value

    def getcash(self):
        # Get cash seems to always be called before get value
        # Therefore it makes sense to add getbalance here.
        # return self.store.getcash(self.currency)
        self.cash = self.store._cash
        return self.cash

    def getvalue(self, datas=None):
        # return self.store.getvalue(self.currency)
        self.value = self.store._value
        return self.value

    def get_notification(self):
        try:
            return self.notifies.get(False)
        except queue.Empty:
            return None

    def notify(self, order):
        self.notifies.put(order)

    def getposition(self, data, clone=True):
        # return self.store.getposition(data._dataname, clone=clone)
        pos = self.positions[data._dataname]
        if clone:
            pos = pos.clone()
        return pos

    # def next(self):
    #     if self.debug:
    #         print('Broker next() called')

    #     for o_order in list(self.open_orders):
    #         oID = o_order.ccxt_order['id']

    #         # Print debug before fetching so we know which order is giving an
    #         # issue if it crashes
    #         if self.debug:
    #             print('Fetching Order ID: {}'.format(oID))

    #         # Get the order
    #         ccxt_order = self.store.fetch_order(oID, o_order.data.p.dataname)

    #         # Check for new fills
    #         if 'trades' in ccxt_order:
    #             for fill in ccxt_order['trades']:
    #                 if fill not in o_order.executed_fills:
    #                     o_order.execute(fill['datetime'], fill['amount'],
    #                                     fill['price'], 0, 0.0, 0.0, 0, 0.0,
    #                                     0.0, 0.0, 0.0, 0, 0.0)
    #                     o_order.executed_fills.append(fill['id'])

    #         if self.debug:
    #             print(json.dumps(ccxt_order, indent=self.indent))

    #         # Check if the order is closed
    #         if ccxt_order[self.mappings['closed_order']
    #                       ['key']] == self.mappings['closed_order']['value']:
    #             pos = self.getposition(o_order.data, clone=False)
    #             pos.update(o_order.size, o_order.price)
    #             o_order.completed()
    #             self.notify(o_order)
    #             self.open_orders.remove(o_order)
    #             self.get_balance()

    # def _submit(self, owner, data, exectype, side, amount, price, params):
    #     order_type = self.order_types.get(exectype) if exectype else 'market'
    #     created = int(data.datetime.datetime(0).timestamp() * 1000)
    #     # Extract Binance specific params if passed to the order
    #     params = params['params'] if 'params' in params else params
    #     params[
    #         'created'] = created  # Add timestamp of order creation for backtesting
    #     ret_ord = self.store.create_order(symbol=data.p.dataname,
    #                                       order_type=order_type,
    #                                       side=side,
    #                                       amount=amount,
    #                                       price=price,
    #                                       params=params)

    #     _order = self.store.fetch_order(ret_ord['id'], data.p.dataname)

    #     order = BinanceOrder(owner, data, _order)
    #     order.price = ret_ord['price']
    #     self.open_orders.append(order)

    #     self.notify(order)
    #     return order

    # def buy(self,
    #         owner,
    #         data,
    #         size,
    #         price=None,
    #         plimit=None,
    #         exectype=None,
    #         valid=None,
    #         tradeid=0,
    #         oco=None,
    #         trailamount=None,
    #         trailpercent=None,
    #         **kwargs):
    #     del kwargs['parent']
    #     del kwargs['transmit']
    #     return self._submit(owner, data, exectype, 'buy', size, price, kwargs)

    # def sell(self,
    #          owner,
    #          data,
    #          size,
    #          price=None,
    #          plimit=None,
    #          exectype=None,
    #          valid=None,
    #          tradeid=0,
    #          oco=None,
    #          trailamount=None,
    #          trailpercent=None,
    #          **kwargs):
    #     del kwargs['parent']
    #     del kwargs['transmit']
    #     return self._submit(owner, data, exectype, 'sell', size, price, kwargs)

    # def cancel(self, order):
    #     oID = order.ccxt_order['id']

    #     if self.debug:
    #         print('Broker cancel() called')
    #         print('Fetching Order ID: {}'.format(oID))

    #     # check first if the order has already been filled otherwise an error
    #     # might be raised if we try to cancel an order that is not open.
    #     ccxt_order = self.store.fetch_order(oID, order.data.p.dataname)

    #     if self.debug:
    #         print(json.dumps(ccxt_order, indent=self.indent))

    #     if ccxt_order[self.mappings['closed_order']
    #                   ['key']] == self.mappings['closed_order']['value']:
    #         return order

    #     ccxt_order = self.store.cancel_order(oID, order.data.p.dataname)

    #     if self.debug:
    #         print(json.dumps(ccxt_order, indent=self.indent))
    #         print('Value Received: {}'.format(
    #             ccxt_order[self.mappings['canceled_order']['key']]))
    #         print('Value Expected: {}'.format(
    #             self.mappings['canceled_order']['value']))

    #     if ccxt_order[self.mappings['canceled_order']
    #                   ['key']] == self.mappings['canceled_order']['value']:
    #         self.open_orders.remove(order)
    #         order.cancel()
    #         self.notify(order)
    #     return order

    # def get_orders_open(self, safe=False):
    #     return self.store.fetch_open_orders()

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

                logger.warn("Need to handle positions")
                # positions = []
                # for p in account['positions']:
                #     position = self._parse_position(p)
                #     positions.append(position)
                # self._on_positions(positions)

            elif 'order' in event:
                # parse order
                raw = event['order']

                # broadcast
                self._on_order(raw)

    # order
    def orderstatus(self, order):
        o = self.orders[order.ref]
        return o.status

    # order update
    def _on_order(self, raw):
        client_id = raw.get('clientOrderId', None)
        if not client_id:
            logger.warn(f"Order without client id cannot be process: {raw}")
            return

        oref = int(client_id.split("|", 1)[0])
        if oref not in self.orders:
            logger.warn(f"Order with ref {oref} not found: {raw}")
            return

        size = raw['amount']
        price = raw['price']
        status = order_statuses_reversed[raw['status'].lower()]

        if status == Order.Submitted:
            self._submit(oref)
        elif status == Order.Accepted:
            self._accept(oref)
        elif status == Order.Canceled:
            self._cancel(oref)
        elif status == Order.Partial or status == Order.Completed:
            self._fill(oref, size, price, filled=status == Order.Completed)
        elif status == Order.Rejected:
            self._reject(oref)
        elif status == Order.Expired:
            self._expire(oref)
        else:
            raise Exception(f"Status {status} is invalid: {raw}")

    def _submit(self, oref):
        order = self.orders[oref]
        if order.status == Order.Submitted:
            return

        order.submit(self)
        self.notify(order)
        # submit for stop order and limit order of bracket
        bracket = self.brackets.get(oref, [])
        for o in bracket:
            if o.ref != oref:
                self._submit(o.ref)

    def _reject(self, oref):
        order = self.orders[oref]
        order.reject(self)
        self.notify(order)
        self._bracketize(order, cancel=True)
        self._ococheck(order)

    def _accept(self, oref):
        order = self.orders[oref]
        if order.status == Order.Accepted:
            return

        order.accept()
        self.notify(order)
        # accept for stop order and limit order of bracket
        bracket = self.brackets.get(oref, [])
        for o in bracket:
            if o.ref != oref:
                self._accept(o.ref)

    def _cancel(self, oref):
        order = self.orders[oref]
        if order.status == Order.Canceled:
            return

        order.cancel()
        self.notify(order)
        self._bracketize(order, cancel=True)
        self._ococheck(order)

    def _expire(self, oref):
        order = self.orders[oref]
        order.expire()
        self.notify(order)
        self._bracketize(order, cancel=True)
        self._ococheck(order)

    def _fill(self, oref, size, price, filled=False, **kwargs):
        if size == 0 and not filled:
            return
        logger.debug("Fill order: {}, {}, {}".format(oref, size, price,
                                                     filled))

        order = self.orders[oref]
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
        psize, pprice, opened, closed = pos.update(size, price)
        # comminfo = self.getcommissioninfo(data)

        closedvalue = closedcomm = 0.0
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
            self.store.get_balance()

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
                self._cancel(br[1 - oidx].ref)  # cancel remaining (1 - 0 -> 1)
        else:
            # Any cancellation cancel the others
            for o in br:
                if o.alive():
                    self._cancel(o.ref)

    def _ococheck(self, order):
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

        order.addinfo(**kwargs)
        # order.addcomminfo(self.getcommissioninfo(data))
        self._ocoize(order)
        return self._transmit(order)

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

        order.addinfo(**kwargs)
        # order.addcomminfo(self.getcommissioninfo(data))
        self._ocoize(order)
        return self._transmit(order)

    def _create_bracket(self, order, stopside, takeside):
        o = self._create(order)
        sl = self._create(stopside)
        tp = self._create(takeside)

    def _create(self, order: Order):
        # param
        params = dict()
        if order.parent:
            params['closePosition'] = True

        # if order.valid:
        #     params['timeInForce'] = order.valid

        # order ref
        client_id = f"{order.ref}"
        if order.parent:
            client_id += f":{order.parent.ref}"
        params['newClientOrderId'] = client_id

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
                                       order_type=order_type,
                                       side=side,
                                       amount=amount,
                                       price=order.price,
                                       params=params)

        order.addinfo(id=o['id'])
        self._submit(order.ref)
        return order

    def cancel(self, order: Order):
        if not self.orders.get(order.ref, False):
            return
        if order.status == Order.Cancelled:  # already cancelled
            return

        id = order.info.get('id', None)
        if not id:
            raise Exception(f'Order doesnot have id {order}')
        params = dict(orderId=id)

        return self.store.cancel_my_order(order.data._name, params)

    # def private_end_point(self, type, endpoint, params):
    #     '''
    #     Open method to allow calls to be made to any private end point.
    #     See here: https://github.com/ccxt/ccxt/wiki/Manual#implicit-api-methods

    #     - type: String, 'Get', 'Post','Put' or 'Delete'.
    #     - endpoint = String containing the endpoint address eg. 'order/{id}/cancel'
    #     - Params: Dict: An implicit method takes a dictionary of parameters, sends
    #       the request to the exchange and returns an exchange-specific JSON
    #       result from the API as is, unparsed.

    #     To get a list of all available methods with an exchange instance,
    #     including implicit methods and unified methods you can simply do the
    #     following:

    #     print(dir(ccxt.hitbtc()))
    #     '''
    #     endpoint_str = endpoint.replace('/', '_')
    #     endpoint_str = endpoint_str.replace('{', '')
    #     endpoint_str = endpoint_str.replace('}', '')

    #     method_str = 'private_' + type.lower() + endpoint_str.lower()
    #     return self.store.private_end_point(type=type,
    #                                         endpoint=method_str,
    #                                         params=params)
