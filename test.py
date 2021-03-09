#!/usr/bin/env python3
from btbinance import BinanceStore

api_key = "a7cc06cad7f1f08c8454a3f2ef0886490ae12a2ff3ec3184287bccf7c1207570"
api_secret = "9da25f276b0bfc1d35720ec047cbbafc1f979426888790d1b930db094d42c4d8"


def main():
    store = BinanceStore(config=dict(
        apiKey=api_key,
        secret=api_secret,
        options={'defaultType': 'future'},
    ),
                         sandbox=True)
    store.subscribe_account()
    store._t_loop_stream()


if __name__ == '__main__':
    main()
