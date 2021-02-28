from binance.client import Client
from get_data import LoadData
from api_setting import *
from app_setting import *
import datetime

db_server_id = 1

Client_KLINE_INTERVAL_dict = {Client.KLINE_INTERVAL_1MINUTE: 0,
                              Client.KLINE_INTERVAL_3MINUTE: 1,
                              Client.KLINE_INTERVAL_5MINUTE: 2,
                              Client.KLINE_INTERVAL_15MINUTE: 3,
                              Client.KLINE_INTERVAL_30MINUTE: 4,
                              Client.KLINE_INTERVAL_1HOUR: 5,
                              Client.KLINE_INTERVAL_2HOUR: 6,
                              Client.KLINE_INTERVAL_4HOUR: 7,
                              Client.KLINE_INTERVAL_6HOUR: 8,
                              Client.KLINE_INTERVAL_8HOUR: 9,
                              Client.KLINE_INTERVAL_12HOUR: 10,
                              Client.KLINE_INTERVAL_1DAY: 11,
                              Client.KLINE_INTERVAL_3DAY: 12,
                              Client.KLINE_INTERVAL_1WEEK: 13,
                              Client.KLINE_INTERVAL_1MONTH: 14}

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

    count_valid = 0
    count_invalid = 0
    # symbol_list = [symbol, earlier_valid_timestamp, small_valid_interval]
    for interval in interval_list:
        for coin_base in coin_base_list:
            symbol_list = cli.db.get_symbol_list(coin_base=coin_base)
            for symbol in symbol_list:
                # print(symbol)
                # print(Client_KLINE_INTERVAL_dict[interval] , Client_KLINE_INTERVAL_dict[symbol[2]])
                if Client_KLINE_INTERVAL_dict[interval] < Client_KLINE_INTERVAL_dict[symbol[2]]:
                    print('symbol: ', symbol[0], '  earlier_valid_timestamp: ', symbol[1],
                          ' small_valid_interval: ', symbol[2], ' current interval: ', interval)
                    count_invalid += 1
                    print('invalid count: ', count_invalid)
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
                count_valid += 1
                print('count_valid: ', count_valid)
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
    start_time = datetime.datetime.utcnow()

    get_all_data(interval_list=api_interval_list, coin_base_list=api_coin_base_list, db_server_id=db_server_id)
    end_time = datetime.datetime.utcnow()

    print('end function ==> start time: {0} end time: {1} run time: {2}'
          .format(start_time, end_time, end_time - start_time))
