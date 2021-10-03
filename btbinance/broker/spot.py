import logging
from backtrader import Order
from backtrader import Order
from .future import BinanceFutureBroker, order_types

logger = logging.getLogger('BinanceSpotBroker')


class BinanceSpotBroker(BinanceFutureBroker):
    def _rebuild_positions(self):
        pass

    def _create(self, order: Order):
        # param
        params = dict()

        # order ref
        params['newClientOrderId'] = self._build_order_info(order)

        # order type
        order_type = order_types.get(order.exectype)
        if (order.parent and order.exectype == Order.Stop) or \
            order.info.get('sl', False):
            order_type = 'stop_loss'
        if (order.parent and order.exectype == Order.Limit) or \
            order.info.get('tp', False):
            order_type = 'take_profit'

        if order_type in ['stop_loss', 'take_profit']:
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
