from get_data_multiprocessing import load_and_set_complete_candle_historical_multi_process
from api_setting import *
from app_setting import *
import datetime
from database import DataBase
# from time import sleep


def get_all_data(interval_list, crypto_list, start_datetime, end_datetime, add_to_database, db_server_id, is_test):
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
                                                                runtime_setting_file_path_name=runtime_setting_file_path_name)

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
    get_all_data(interval_list=interval_list,
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
