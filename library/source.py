from datetime import datetime, timedelta

import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException


def timestamp(look_back_seconds=0, override_now=None):
    now = override_now if isinstance(override_now, datetime) else datetime.utcnow()
    target = now - timedelta(seconds=look_back_seconds)
    return int((target - datetime(1970, 1, 1)).total_seconds() * 1000.0)


class BinanceData:

    COLUMNS = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asses_volume',
               'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
    DATETIME_COLUMNS = ['open_time', 'close_time']
    FLOAT_COLUMNS = ['open', 'high', 'low', 'close', 'volume', 'quote_asses_volume', 'number_of_trades',
                     'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']

    VALID_INTERVALS = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']

    @staticmethod
    def is_enough(data, interval, look_back_seconds):
        if len(data) < look_back_seconds/interval:
            raise Exception('Not enough data points!')

    @staticmethod
    def _format_df(df):
        for column in BinanceData.DATETIME_COLUMNS:
            df[column] = [datetime.fromtimestamp(x/1000) for x in df[column]]
        for column in BinanceData.FLOAT_COLUMNS:
            df[column] = [float(x) for x in df[column]]
        return df

    def __init__(self, api_key, secret_key):
        self._client = Client(api_key, secret_key)
        self.error = None

    def request(self, ticker, interval, look_back_seconds, force_datetime=None):
        if interval not in BinanceData.VALID_INTERVALS:
            raise Exception('Invalid interval: {}!'.format(interval))
        try:
            data = self._client.get_historical_klines(ticker, interval, timestamp(look_back_seconds, force_datetime))
            df = pd.DataFrame(data, columns=BinanceData.COLUMNS)
            return self._format_df(df)
        except BinanceAPIException as error:
            raise Exception('Bad data request!: "{}"'.format(error))
