from get_data_multiprocessing import load_and_set_complete_candle_historical_multi_process
from api_setting import *
from app_setting import *
import datetime
from database import DataBase
from binance.client import Client


def get_candle_open_time(interval, date_time):
    # this open time for latest candle that not complete
    # [time) --> [
    now = date_time - datetime.timedelta(seconds=date_time.second, microseconds=date_time.microsecond)

    if interval == Client.KLINE_INTERVAL_1MINUTE:
        start_end_candle_time = now
    elif interval == Client.KLINE_INTERVAL_3MINUTE:
        start_end_candle_time = now - datetime.timedelta(minutes=divmod(now.minute, 3)[1])
    elif interval == Client.KLINE_INTERVAL_5MINUTE:
        start_end_candle_time = now - datetime.timedelta(minutes=divmod(now.minute, 5)[1])
    elif interval == Client.KLINE_INTERVAL_15MINUTE:
        start_end_candle_time = now - datetime.timedelta(minutes=divmod(now.minute, 15)[1])
    elif interval == Client.KLINE_INTERVAL_30MINUTE:
        start_end_candle_time = now - datetime.timedelta(minutes=divmod(now.minute, 30)[1])

    elif interval == Client.KLINE_INTERVAL_1HOUR:
        start_end_candle_time = now - datetime.timedelta(minutes=now.minute)
    elif interval == Client.KLINE_INTERVAL_2HOUR:
        start_end_candle_time = now - datetime.timedelta(hours=divmod(now.hour, 2)[1], minutes=now.minute)
    elif interval == Client.KLINE_INTERVAL_4HOUR:
        start_end_candle_time = now - datetime.timedelta(hours=divmod(now.hour, 4)[1], minutes=now.minute)
    elif interval == Client.KLINE_INTERVAL_6HOUR:
        start_end_candle_time = now - datetime.timedelta(hours=divmod(now.hour, 6)[1], minutes=now.minute)
    elif interval == Client.KLINE_INTERVAL_8HOUR:
        start_end_candle_time = now - datetime.timedelta(hours=divmod(now.hour, 8)[1], minutes=now.minute)
    elif interval == Client.KLINE_INTERVAL_12HOUR:
        start_end_candle_time = now - datetime.timedelta(hours=divmod(now.hour, 12)[1], minutes=now.minute)

    elif interval == Client.KLINE_INTERVAL_1DAY:
        start_end_candle_time = now - datetime.timedelta(hours=now.hour, minutes=now.minute)
    elif interval == Client.KLINE_INTERVAL_3DAY:
        start_end_candle_time = now - datetime.timedelta(days=divmod(now.day - 1, 3)[1], hours=now.hour,
                                                         minutes=now.minute)

    elif interval == Client.KLINE_INTERVAL_1WEEK:
        start_end_candle_time = now - datetime.timedelta(days=now.weekday(), hours=now.hour, minutes=now.minute)

    elif interval == Client.KLINE_INTERVAL_1MONTH:
        start_end_candle_time = now - datetime.timedelta(days=now.day - 1, hours=now.hour, minutes=now.minute)
    else:
        raise Exception('invalid interval')
    # except Exception as e:
    #     return str(e)
    return start_end_candle_time


def get_next_n_interval_time(start_time, interval, n_interval):
    end = start_time
    if interval == Client.KLINE_INTERVAL_1MINUTE:
        end = start_time + datetime.timedelta(minutes=n_interval)
    elif interval == Client.KLINE_INTERVAL_3MINUTE:
        end = start_time + datetime.timedelta(minutes=3 * n_interval)
    elif interval == Client.KLINE_INTERVAL_5MINUTE:
        end = start_time + datetime.timedelta(minutes=5 * n_interval)
    elif interval == Client.KLINE_INTERVAL_15MINUTE:
        end = start_time + datetime.timedelta(minutes=15 * n_interval)
    elif interval == Client.KLINE_INTERVAL_30MINUTE:
        end = start_time + datetime.timedelta(minutes=30 * n_interval)
    elif interval == Client.KLINE_INTERVAL_1HOUR:
        end = start_time + datetime.timedelta(hours=1 * n_interval)
    elif interval == Client.KLINE_INTERVAL_2HOUR:
        end = start_time + datetime.timedelta(hours=2 * n_interval)
    elif interval == Client.KLINE_INTERVAL_4HOUR:
        end = start_time + datetime.timedelta(hours=4 * n_interval)
    elif interval == Client.KLINE_INTERVAL_6HOUR:
        end = start_time + datetime.timedelta(hours=6 * n_interval)
    elif interval == Client.KLINE_INTERVAL_8HOUR:
        end = start_time + datetime.timedelta(hours=8 * n_interval)
    elif interval == Client.KLINE_INTERVAL_12HOUR:
        end = start_time + datetime.timedelta(hours=12 * n_interval)
    elif interval == Client.KLINE_INTERVAL_1DAY:
        end = start_time + datetime.timedelta(days=1 * n_interval)
    elif interval == Client.KLINE_INTERVAL_3DAY:
        end = start_time + datetime.timedelta(days=3 * n_interval)
    elif interval == Client.KLINE_INTERVAL_1WEEK:
        end = start_time + datetime.timedelta(days=7 * n_interval)
    elif interval == Client.KLINE_INTERVAL_1MONTH:
        end = start_time + datetime.timedelta(days=30 * n_interval)
    else:
        raise Exception('invalid interval')

    end = get_candle_open_time(interval=interval, date_time=end)
    return end


def get_all_data_process_auto_cropped_time(interval_list, crypto_list, start_datetime, end_datetime, add_to_database, db_server_id, is_test):
    # create wait list
    # ---- [(symbol, interval, start_datetime, end_datetime, add_to_database, earlier_valid_timestamp)] ----
    wait_list = []
    for interval in interval_list:
        for symbol in crypto_list:  # ---- (symbol, earlier_valid_timestamp, small_valid_interval) ----
            if api_interval_dict[interval] < api_interval_dict[symbol[2]]:
                # interval smallest than small_valid_interval
                continue
            wait_list.append((symbol[0], interval, start_datetime, end_datetime, add_to_database, symbol[1]))

    cli = load_and_set_complete_candle_historical_multi_process(api_key=api_key,
                                                                api_secret=api_secret,
                                                                timestamp_base_time=timestamp_base_time,
                                                                is_test=is_test,
                                                                public_ip=None,
                                                                database_info=get_db_info(db_server_id=db_server_id),
                                                                wait_list=wait_list,
                                                                client_id=client_id,
                                                                runtime_setting_file_path_name=runtime_setting_file_path_name,
                                                                process_auto_cropped_time=True)

    result = cli.run_client()

    print("========================================================================")
    print(result)
    print("========================================================================")


def get_all_data_function_cropped_time(interval_list, crypto_list, start_datetime, end_datetime, add_to_database, db_server_id, is_test):
    # create wait list
    # ---- [(symbol, interval, start_datetime, end_datetime, add_to_database, earlier_valid_timestamp)] ----
    wait_list = []
    for interval in interval_list:
        for symbol in crypto_list:  # ---- (symbol, earlier_valid_timestamp, small_valid_interval) ----
            if api_interval_dict[interval] < api_interval_dict[symbol[2]]:
                # interval smallest than small_valid_interval
                continue
            # create time list
            start = get_candle_open_time(interval=interval, date_time=start_datetime)
            time_list = []
            while True:
                end = get_next_n_interval_time(start_time=start, interval=interval, n_interval=1440)
                if end > end_datetime:
                    end = get_candle_open_time(interval=interval, date_time=end_datetime)
                    time_list.append([start, end])
                    break
                time_list.append([start, end])
                start = end
            for time in time_list:
                wait_list.append((symbol[0], interval, time[0], time[1], add_to_database, symbol[1]))

    cli = load_and_set_complete_candle_historical_multi_process(api_key=api_key,
                                                                api_secret=api_secret,
                                                                timestamp_base_time=timestamp_base_time,
                                                                is_test=is_test,
                                                                public_ip=None,
                                                                database_info=get_db_info(db_server_id=db_server_id),
                                                                wait_list=wait_list,
                                                                client_id=client_id,
                                                                runtime_setting_file_path_name=runtime_setting_file_path_name,
                                                                process_auto_cropped_time=False)

    result = cli.run_client()

    print("========================================================================")
    print(result)
    print("========================================================================")


if __name__ == "__main__":
    start_main_time = datetime.datetime.utcnow()
    # ---------------
    start_datetime = datetime.datetime(year=2009, month=12, day=1)
    # end_datetime = None
    end_datetime = datetime.datetime(year=2020, month=12, day=1)
    # ---------------
    interval_list = api_interval_list
    interval_list.remove('3d')
    # interval_list.reverse()
    add_to_database = True
    is_test = False
    # ---------------
    db_server_id = 1
    db = DataBase(db_info=get_db_info(db_server_id=db_server_id), log_obj=None)
    crypto_list = []
    for coin_base in api_coin_base_list:
        try:
            symbol_list = db.get_symbol_list(coin_base=coin_base)
            # print(symbol_list)
            for symbol in symbol_list:
                crypto_list.append(symbol)
        except Exception as e:
            print('fail to load symbol:', str(e))
    # ---------------
    get_all_data_function_cropped_time(interval_list=interval_list,
                                           crypto_list=crypto_list,
                                           start_datetime=start_datetime,
                                           end_datetime=end_datetime,
                                           add_to_database=add_to_database,
                                           db_server_id=db_server_id,
                                           is_test=is_test
                                           )

    end_main_time = datetime.datetime.utcnow()

    print('end function ==> start time: {0} end time: {1} run time: {2}'
          .format(start_main_time, end_main_time, end_main_time - start_main_time))
