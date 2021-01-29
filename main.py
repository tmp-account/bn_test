from binance.client import Client
from get_data import LoadData
from api_setting import *
from app_setting import *
import datetime


def get_all_data():
    cli = LoadData(api_key=api_key, api_secret=api_secret, db_info=get_db_info(db_server_id=db_server_id),
                   timestamp_base_time=timestamp_base_time, is_test=False)

    now_time = datetime.datetime.utcnow()
    end_time = None
    end_time = datetime.datetime(year=2019, month=12, day=1)
    start_time = datetime.datetime(year=2013, month=12, day=1)

    print("now time: ", now_time)
    print("start time: ", start_time)
    print("end time: ", end_time)
    print("-------------------------------------------------")

    interval = Client.KLINE_INTERVAL_3MINUTE
    symbol = "BNBUSDT"

    # cli.load_and_set_complete_candle_historical(symbol=symbol, interval=interval, start_datetime=start_time, end_datetime=None, add_to_database=False)
    # cli.load_and_set_last_real_candle_historical(symbol=symbol, interval=interval, end_datetime=None, add_to_database=False)

    for interval in interval_list:
        for symbol in symbol_list:
            earlier_valid_timestamp = cli.timestamp_to_datetime(cli.client._get_earliest_valid_timestamp(symbol, interval))
            # print(earlier_valid_timestamp)

            if start_time < earlier_valid_timestamp:
                start_time = earlier_valid_timestamp

            # interval = Client.KLINE_INTERVAL_1HOUR
            print('symbol: ', symbol, " interval: ", interval, '  earlier_valid_timestamp: ', earlier_valid_timestamp)
            # print(cli.client._get_earliest_valid_timestamp(symbol, interval))
            # print(cli.timestamp_to_datetime(cli.client._get_earliest_valid_timestamp('BNBBTC', interval)))

            res = cli.load_and_set_complete_candle_historical(symbol=symbol, interval=interval, start_datetime=start_time,
                                                              end_datetime=end_time, add_to_database=True)
            print(res)
            print("========================================================================")


if __name__ == "__main__":
    get_all_data()



    cli = LoadData(api_key=api_key, api_secret=api_secret, db_info=get_db_info(db_server_id=db_server_id),
                   timestamp_base_time=timestamp_base_time, is_test=False)
    now_time = datetime.datetime.utcnow()
    end_time = now_time - datetime.timedelta(days=2, minutes=10)
    start_time = end_time - datetime.timedelta(days=1, minutes=20)  # 26, hours=6, minutes=35)
    # start_time = datetime.datetime(year=2021, month=1,day=28, hour=10, minute=0, second=0)
    # end_time = datetime.datetime(year=2020, month=12,day=1, hour=0, minute=0, second=0)
    print("now time: ", now_time)
    print("start time: ", start_time)
    print("end time: ", end_time)
    # server_timestamp = cli.get_server_time()
    # print("server time: ", server_timestamp)
    print("-------------------------------------------------")
    interval = Client.KLINE_INTERVAL_1MINUTE
    symbol = "BNBUSDT"
    # cli.load_and_set_complete_candle_historical(symbol=symbol, interval=interval, start_datetime=start_time, end_datetime=end_time, add_to_database=False)
    # cli.load_and_set_last_real_candle_historical(symbol=symbol, interval=interval, end_datetime=None, add_to_database=False)
    res = cli.generate_complete_candle_open_time(interval=interval, start_datetime=start_time, end_datetime=end_time)
    print('lenth',len(res), res)
    # for item in res:
    #     print(item)


    a = cli.db.get_complete_candle_historical_open_time(symbol=symbol, interval=interval, start_datetime=start_time, end_datetime=end_time)
    print('a lenth:',len(a), a)

    print('res:',type(res[-6]), res[-6])
    # print('a:',type(a[-1][0]), a[-1][0])

    # print(a[-1][0], res[-10])
    # print(a[-1][0] == res[-10])

    print('----------------------------------------------')
    cli.load_and_set_complete_candle_historical(symbol=symbol, interval=interval, start_datetime=start_time,
                                                end_datetime=None, add_to_database=True)

# ----------
    # print(datetime.datetime.utcnow())
    # print(cli.get_real_last_candle(interval=Client.KLINE_INTERVAL_1MINUTE))
    # print(cli.get_real_last_candle(interval=Client.KLINE_INTERVAL_3MINUTE))
    # print(cli.get_real_last_candle(interval=Client.KLINE_INTERVAL_5MINUTE))
    # print(cli.get_real_last_candle(interval=Client.KLINE_INTERVAL_15MINUTE))
    # print(cli.get_real_last_candle(interval=Client.KLINE_INTERVAL_30MINUTE))
    # print(cli.get_real_last_candle(interval=Client.KLINE_INTERVAL_1HOUR))
    # print(cli.get_real_last_candle(interval=Client.KLINE_INTERVAL_2HOUR))
    # print(cli.get_real_last_candle(interval=Client.KLINE_INTERVAL_4HOUR))
    # print(cli.get_real_last_candle(interval=Client.KLINE_INTERVAL_6HOUR))
    # print(cli.get_real_last_candle(interval=Client.KLINE_INTERVAL_8HOUR))
    # print(cli.get_real_last_candle(interval=Client.KLINE_INTERVAL_12HOUR))
    # print(cli.get_real_last_candle(interval=Client.KLINE_INTERVAL_1DAY))
    # print(cli.get_real_last_candle(interval=Client.KLINE_INTERVAL_3DAY))
    # print(cli.get_real_last_candle(interval=Client.KLINE_INTERVAL_1WEEK))
    # print(cli.get_real_last_candle(interval=Client.KLINE_INTERVAL_1MONTH))