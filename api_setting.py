import datetime
from binance.client import Client as c

aapi_key = "AibX2zigFqfEHTCmuV0pIXtM5rCCk6c2SUnBeF5Hxn78yF40V29n0imCRgV7V5Hq"
aapi_secret = "kLO5DN3Nv38W2uYzBkJrrTpnLfzjCUu8qHhi0gDlkxjk4AsqSHyyrAXxzaUkiS3N"


api_key = "xCTmpTB1thBdiHpLhtHGLbWwE3FBM9f8DeSls1ZOssHnGgh18a2Rustu1aElc41k"
api_secret = "sFq09v9KbFjjGr7QdzwYL56ZoKZmuL3R0adVttTnEUl3AbZPfi0ijo9XDwYi2YPh"

timestamp_base_time = datetime.datetime(year=1970, month=1, day=1)

interval_list = [c.KLINE_INTERVAL_1MINUTE,
                 c.KLINE_INTERVAL_3MINUTE,
                 c.KLINE_INTERVAL_5MINUTE,
                 c.KLINE_INTERVAL_15MINUTE,
                 c.KLINE_INTERVAL_30MINUTE,
                 c.KLINE_INTERVAL_1HOUR,
                 c.KLINE_INTERVAL_2HOUR,
                 c.KLINE_INTERVAL_4HOUR,
                 c.KLINE_INTERVAL_6HOUR,
                 c.KLINE_INTERVAL_8HOUR,
                 c.KLINE_INTERVAL_12HOUR,
                 c.KLINE_INTERVAL_1DAY,
                 c.KLINE_INTERVAL_3DAY,
                 c.KLINE_INTERVAL_1WEEK,
                 c.KLINE_INTERVAL_1MONTH]


symbol_list = ['BNBBUSD',
               'BTCBUSD',
               'ETHBUSD',
               'LTCBUSD',
               'TRXBUSD',
               'XRPBUSD',
               'BNBUSDT',
               'BTCUSDT',
               'ETHUSDT',
               'LTCUSDT',
               'TRXUSDT',
               'XRPUSDT',
               'BNBBTC',
               'ETHBTC',
               'LTCBTC',
               'TRXBTC',
               'XRPBTC',
               'LTCBNB',
               'TRXBNB',
               'XRPBNB'
               ]