from binance.client import Client
from get_data import LoadData
from api_setting import *
from app_setting import *
import datetime

db_server_id = 2

# interval_dict = {Client.KLINE_INTERVAL_1MINUTE: 0,
#                  Client.KLINE_INTERVAL_3MINUTE: 1,
#                  Client.KLINE_INTERVAL_5MINUTE: 2,
#                  Client.KLINE_INTERVAL_15MINUTE: 3,
#                  Client.KLINE_INTERVAL_30MINUTE: 4,
#                  Client.KLINE_INTERVAL_1HOUR: 5,
#                  Client.KLINE_INTERVAL_2HOUR: 6,
#                  Client.KLINE_INTERVAL_4HOUR: 7,
#                  Client.KLINE_INTERVAL_6HOUR: 8,
#                  Client.KLINE_INTERVAL_8HOUR: 9,
#                  Client.KLINE_INTERVAL_12HOUR: 10,
#                  Client.KLINE_INTERVAL_1DAY: 11,
#                  Client.KLINE_INTERVAL_3DAY: 12,
#                  Client.KLINE_INTERVAL_1WEEK: 13,
#                  Client.KLINE_INTERVAL_1MONTH: 14}

def get_all_data(interval_list, coin_base_list, db_server_id):
    cli = LoadData(api_key=api_key, api_secret=api_secret, db_info=get_db_info(db_server_id=db_server_id),
                   timestamp_base_time=timestamp_base_time, is_test=False)

    now_time = datetime.datetime.utcnow()
    start_time = datetime.datetime(year=2019, month=12, day=1)
    end_time = None
    end_time = datetime.datetime(year=2020, month=12, day=1)

    print("now time: ", now_time)
    print("start time: ", start_time)
    print("end time: ", end_time)
    print("-----------------------------------------------------------------------------------")

    # symbol_list = [symbol, earlier_valid_timestamp, small_valid_interval]
    for interval in interval_list:
        for coin_base in coin_base_list:
            symbol_list = cli.db.get_symbol_list(coin_base=coin_base)
            for symbol in symbol_list:
                # print(symbol)
                # print(Client_KLINE_INTERVAL_dict[interval] , Client_KLINE_INTERVAL_dict[symbol[2]])
                if api_interval_dict[interval] < api_interval_dict[symbol[2]]:
                    print('symbol: ', symbol[0], '  earlier_valid_timestamp: ', symbol[1],
                          ' small_valid_interval: ', symbol[2], ' current interval: ', interval)
                    print('interval smallest than small_valid_interval')
                    print("========================================================================")
                    continue

                earlier_valid_timestamp = symbol[1]
                # correct start time
                if start_time < earlier_valid_timestamp:
                    start = earlier_valid_timestamp
                else:
                    start = start_time

                print('symbol: ', symbol[0], " current interval: ", interval,
                      '  earlier_valid_timestamp: ', earlier_valid_timestamp, ' start_time: ', start)

                err = cli.load_and_set_complete_candle_historical_auto_cropped_time(symbol=symbol[0],
                                                                                    interval=interval,
                                                                                    start_datetime=start,
                                                                                    end_datetime=end_time,
                                                                                    add_to_database=True,
                                                                                    earlier_valid_timestamp=earlier_valid_timestamp)
                print(err)
                print("========================================================================")
                time.sleep(0.1)


if __name__ == "__main__":
    import time

    db_server_id = 4

    get_all_data(interval_list=api_interval_list, coin_base_list=api_coin_base_list, db_server_id=db_server_id)

    print('sleep')
    time.sleep(100)


    cli = LoadData(api_key=api_key, api_secret=api_secret, db_info=get_db_info(db_server_id=db_server_id),
                   timestamp_base_time=timestamp_base_time, is_test=False)

    interval = Client.KLINE_INTERVAL_1MINUTE

    for symbol in symbol_XRP_list:
        print(symbol,'@', cli.get_symbol_earlier_valid_timestamp(symbol=symbol, interval=interval))

    print('end')
    time.sleep(50)

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
    res = cli.generate_all_complete_candle_open_time(interval=interval, start_datetime=start_time, end_datetime=end_time)
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
    cli.load_and_set_complete_candle_historical_auto_cropped_time(symbol=symbol, interval=interval, start_datetime=start_time,
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