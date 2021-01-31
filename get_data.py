from binance.client import Client
from database import DataBase
import datetime
from time import sleep


class LoadData:

    def __init__(self, api_key, api_secret, db_info, timestamp_base_time, is_test=True):
        self.timestamp_base_time = timestamp_base_time
        self.client = Client(api_key, api_secret)
        # for test unit
        if is_test is True:
            self.client.API_URL = 'https://testnet.binance.vision/api'

        self.db = DataBase(db_info=db_info, log_obj=None)

    def timestamp_to_datetime(self, timestamp):
        return self.timestamp_base_time + datetime.timedelta(seconds=timestamp / 1000)

    def get_server_time(self):
        server_timestamp = self.client.get_server_time()
        return self.timestamp_to_datetime(server_timestamp['serverTime'])

    def _load_and_set_complete_candle_historical(self, symbol, interval, start_datetime, end_datetime, add_to_database = True):
        try:
            result = []
            data = self.client.get_historical_klines(symbol=symbol, interval=interval, start_str=str(start_datetime),
                                                     end_str=str(end_datetime))
            for item in data:
                # print(data)
                # result.append([str(timestamp_to_datetime(item[0])),  # open time
                result.append([symbol,
                               self.timestamp_to_datetime(item[0]),  # open time
                               item[1],  # open
                               item[2],  # high
                               item[3],  # low
                               item[4],  # close
                               item[5],  # volume
                               item[8]  # number of trade
                               ])
            if add_to_database is True:
                err = self.db.set_complete_candle_historical_data(interval=interval, data=result)
                print('database error: ', err)
            return result

        except Exception as e:
            print(str(e))
            sleep(10)
            return str(e)

    def get_candle_open_time(self, interval, date_time):
        # this open time for latest candle that not complete
        # [time) --> [
        try:
            now = date_time - datetime.timedelta(seconds=date_time.second, microseconds=date_time.microsecond)

            if interval == Client.KLINE_INTERVAL_1MINUTE:
                start_end_candle_time = now
            elif interval == Client.KLINE_INTERVAL_3MINUTE:
                start_end_candle_time = now - datetime.timedelta(minutes=divmod(now.minute,3)[1])
            elif interval == Client.KLINE_INTERVAL_5MINUTE:
                start_end_candle_time = now - datetime.timedelta(minutes=divmod(now.minute,5)[1])
            elif interval == Client.KLINE_INTERVAL_15MINUTE:
                start_end_candle_time = now - datetime.timedelta(minutes=divmod(now.minute,15)[1])
            elif interval == Client.KLINE_INTERVAL_30MINUTE:
                start_end_candle_time = now - datetime.timedelta(minutes=divmod(now.minute,30)[1])

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
                start_end_candle_time = now - datetime.timedelta(days=divmod(now.day - 1,3)[1], hours=now.hour, minutes=now.minute)

            elif interval == Client.KLINE_INTERVAL_1WEEK:
                start_end_candle_time = now - datetime.timedelta(days=now.weekday(), hours=now.hour, minutes=now.minute)

            elif interval == Client.KLINE_INTERVAL_1MONTH:
                start_end_candle_time = now - datetime.timedelta(days=now.day - 1, hours=now.hour, minutes=now.minute)
            else:
                err = 'invalid interval'
                return err
        except Exception as e:
            return str(e)

        return start_end_candle_time

    def get_real_last_candle_open_time(self, interval):
        return self.get_candle_open_time(interval=interval, date_time=datetime.datetime.utcnow())

    def get_symbol_earlier_valid_timestamp(self, symbol, interval):
        return self.timestamp_to_datetime(self.client._get_earliest_valid_timestamp(symbol, interval))

    def load_and_set_complete_candle_historical(self, symbol, interval, start_datetime, end_datetime=None, add_to_database = True, earlier_valid_timestamp=None):
        # load all candle that start and end time completely in time range
        start_def = datetime.datetime.utcnow()
        # print('000000 ', 'start_datetime', start_datetime, 'end_datetime', end_datetime, 'earlier_valid_timestamp', earlier_valid_timestamp)

        # مشخص کردن زمان شروع
        if earlier_valid_timestamp is None:
            earlier_valid_timestamp = self.get_symbol_earlier_valid_timestamp(symbol=symbol, interval=interval)
        if start_datetime < earlier_valid_timestamp:
            start = earlier_valid_timestamp
        else:
            start = start_datetime
        # زمان شروع بزرگتر از زمان پایان
        if end_datetime is None:
            if start > datetime.datetime.utcnow():
                return 'no eny data in time range'
        else:
            if start > end_datetime:
                return 'no eny data in time range'
        # -------------------------------------
        end = start
        a = []
        beta = 24
        i = 0
        t_sum = 0
        exit_loop = False
        round_number = 0
        while exit_loop == False:
            print('')
            print('* load_and_set_complete_candle_historical round_number: ', round_number)
            # calculate end time on 60 * beta candle
            if interval == Client.KLINE_INTERVAL_1MINUTE:
                end = start + datetime.timedelta(hours=1 * beta)
            elif interval == Client.KLINE_INTERVAL_3MINUTE:
                end = start + datetime.timedelta(hours=3 * beta)
            elif interval == Client.KLINE_INTERVAL_5MINUTE:
                end = start + datetime.timedelta(hours=5 * beta)
            elif interval == Client.KLINE_INTERVAL_15MINUTE:
                end = start + datetime.timedelta(hours=15 * beta)
            elif interval == Client.KLINE_INTERVAL_30MINUTE:
                end = start + datetime.timedelta(hours=30 * beta)
            elif interval == Client.KLINE_INTERVAL_1HOUR:
                end = start + datetime.timedelta(hours=60 * beta)
            elif interval == Client.KLINE_INTERVAL_2HOUR:
                end = start + datetime.timedelta(hours=120 * beta)
            elif interval == Client.KLINE_INTERVAL_4HOUR:
                end = start + datetime.timedelta(hours=240 * beta)
            elif interval == Client.KLINE_INTERVAL_6HOUR:
                end = start + datetime.timedelta(hours=360 * beta)
            elif interval == Client.KLINE_INTERVAL_8HOUR:
                end = start + datetime.timedelta(hours=480 * beta)
            elif interval == Client.KLINE_INTERVAL_12HOUR:
                end = start + datetime.timedelta(hours=720 * beta)
            elif interval == Client.KLINE_INTERVAL_1DAY:
                end = start + datetime.timedelta(days=60 * beta)
            elif interval == Client.KLINE_INTERVAL_3DAY:
                end = start + datetime.timedelta(days=180 * beta)
            elif interval == Client.KLINE_INTERVAL_1WEEK:
                end = start + datetime.timedelta(days=420 * beta)
            elif interval == Client.KLINE_INTERVAL_1MONTH:
                end = start + datetime.timedelta(days=1800 * beta)

            end = self.get_candle_open_time(interval=interval, date_time=end)

            # correct end time
            now = datetime.datetime.utcnow()
            if end_datetime is None:  # end_datetime = mow time
                # latest_valid_end = self.get_last_candle_open_time(interval=interval, date_time=now)
                min_end = self.get_candle_open_time(interval=interval, date_time=now)
            else:
                if end_datetime < now:
                    min_end = self.get_candle_open_time(interval=interval, date_time=end_datetime)
                else:
                    min_end = self.get_candle_open_time(interval=interval, date_time=now)

            if end >= min_end:
                # set one candle back
                end = min_end  #  - datetime.timedelta(seconds=1)
                # print('= end load_and_set_complete_candle_historical while loop =', end)
                exit_loop = True
            # ------------------
            # print(start)
            # print(end)
            # print('start', start, 'end', end)

            complete_open_time_list = self.generate_all_complete_candle_open_time(interval=interval, start_datetime=start, end_datetime=end)
            db_open_time_list = self.db.get_complete_candle_historical_open_time(symbol=symbol, interval=interval, start_datetime=start, end_datetime=end)

            print('complete_open_time_list:', len(complete_open_time_list))
            print('db_open_time_list:', len(db_open_time_list))
            db_have_data = True
            if len(complete_open_time_list) != len(db_open_time_list):
                db_have_data = False
                # print('len(complete_open_time_list) != len(db_open_time_list)', len(complete_open_time_list) , len(db_open_time_list))
                print('start get data: ===>', '  (start_time: ', start, '  end_time: ', end, ')')
                sum = 0
                a = self._load_and_set_complete_candle_historical(symbol=symbol, interval=interval, start_datetime=start,
                                                                  end_datetime=end- datetime.timedelta(seconds=1),
                                                                  add_to_database=add_to_database)
                for item in a:
                    # print(item)
                    sum += 1
                    t_sum += 1
                print('sum round records ', sum)

            else:  # len(complete_open_time_list) == len(res)
                temp = []
                for item in db_open_time_list:
                    temp.append(item[0])
                db_open_time_list = temp
                j = 0
                while j < len(complete_open_time_list):
                    if complete_open_time_list[j] != db_open_time_list[j]:  # not equal list
                        db_have_data = False
                        # print('complete_open_time_list[i] != db_open_time_list[i]', complete_open_time_list[i] , db_open_time_list[i])
                        print('start get data: ===>', '  (start_time: ', start, '  end_time: ', end, ')')

                        sum = 0
                        a = self._load_and_set_complete_candle_historical(symbol=symbol, interval=interval,
                                                                          start_datetime=start,
                                                                          end_datetime=end- datetime.timedelta(seconds=1),
                                                                          add_to_database=add_to_database)
                        for item in a:
                            print(item)
                            sum += 1
                            t_sum += 1
                        print('sum round records ', sum)
                        break
                    j += 1

            if db_have_data == True:
                print('** database have data **')

            # print('11111 ', 'start', start, 'end', end)
            start = end
            # print('22222 ', 'start', start, 'end', end)

            round_number += 1

        print("-- end all round --")
        print("total record:", t_sum)
        print("total time:", datetime.datetime.utcnow() - start_def)
        return True

    def get_next_candel_open_time(self, interval, date_time):
        start = date_time
        if interval == Client.KLINE_INTERVAL_1MINUTE:
            start += datetime.timedelta(minutes=1)
        elif interval == Client.KLINE_INTERVAL_3MINUTE:
            start += datetime.timedelta(minutes=3)
        elif interval == Client.KLINE_INTERVAL_5MINUTE:
            start += datetime.timedelta(minutes=5)
        elif interval == Client.KLINE_INTERVAL_15MINUTE:
            start += datetime.timedelta(minutes=15)
        elif interval == Client.KLINE_INTERVAL_30MINUTE:
            start += datetime.timedelta(minutes=30)
        elif interval == Client.KLINE_INTERVAL_1HOUR:
            start += datetime.timedelta(hours=1)
        elif interval == Client.KLINE_INTERVAL_2HOUR:
            start += datetime.timedelta(hours=2)
        elif interval == Client.KLINE_INTERVAL_4HOUR:
            start += datetime.timedelta(hours=4)
        elif interval == Client.KLINE_INTERVAL_6HOUR:
            start += datetime.timedelta(hours=6)
        elif interval == Client.KLINE_INTERVAL_8HOUR:
            start += datetime.timedelta(hours=8)
        elif interval == Client.KLINE_INTERVAL_12HOUR:
            start += datetime.timedelta(hours=12)
        elif interval == Client.KLINE_INTERVAL_1DAY:
            start += datetime.timedelta(days=1)
        elif interval == Client.KLINE_INTERVAL_3DAY:
            start += datetime.timedelta(days=3)
        elif interval == Client.KLINE_INTERVAL_1WEEK:
            start += datetime.timedelta(days=7)

        elif interval == Client.KLINE_INTERVAL_1MONTH:
            if start.month == 12:
                temp = datetime.datetime(year=start.year + 1, month=1, day=start.day)
            else:
                temp = datetime.datetime(year=start.year, month=start.month + 1, day=start.day)

            start = temp

        return start

    def generate_all_complete_candle_open_time(self, interval, start_datetime, end_datetime):
        # when start and end in range
        result = []
        start = self.get_candle_open_time(interval=interval, date_time=start_datetime)
        if start < start_datetime:
            start = self.get_next_candel_open_time(interval=interval, date_time=start)

        end = self.get_candle_open_time(interval=interval, date_time=end_datetime)
        # go back one candle
        end = self.get_candle_open_time(interval=interval, date_time=end - datetime.timedelta(seconds=-1))

        while True:
            result.append(start)
            start = self.get_next_candel_open_time(interval=interval, date_time=start)
            if start >= end:
                break
        return result

    def generate_all_candle_open_time(self, interval, start_datetime, end_datetime):
        # when start in range
        result = []
        start = self.get_candle_open_time(interval=interval, date_time=start_datetime)
        if start < start_datetime:
            start = self.get_next_candel_open_time(interval=interval, date_time=start)

        end = self.get_candle_open_time(interval=interval, date_time=end_datetime)

        while True:
            result.append(start)
            start = self.get_next_candel_open_time(interval=interval, date_time=start)
            if start >= end:
                break
        return result
# ===================================================================================================================
    def is_in_real_last_candle(self, date_time, interval):
        now_time = datetime.datetime.utcnow()
        now = now_time - datetime.timedelta(seconds=now_time.second, microseconds=now_time.microsecond)

        if interval == Client.KLINE_INTERVAL_1MINUTE:
            start_end_candle_time = now - datetime.timedelta(microseconds=1)
        elif interval == Client.KLINE_INTERVAL_3MINUTE:
            start_end_candle_time = now - datetime.timedelta(minutes=divmod(now.minute,3)[1]) - datetime.timedelta(microseconds=1)
        elif interval == Client.KLINE_INTERVAL_5MINUTE:
            start_end_candle_time = now - datetime.timedelta(minutes=divmod(now.minute,5)[1]) - datetime.timedelta(microseconds=1)
        elif interval == Client.KLINE_INTERVAL_15MINUTE:
            start_end_candle_time = now - datetime.timedelta(minutes=divmod(now.minute,15)[1]) - datetime.timedelta(microseconds=1)
        elif interval == Client.KLINE_INTERVAL_30MINUTE:
            start_end_candle_time = now - datetime.timedelta(minutes=divmod(now.minute,30)[1]) - datetime.timedelta(microseconds=1)

        elif interval == Client.KLINE_INTERVAL_1HOUR:
            start_end_candle_time = now - datetime.timedelta(minutes=now.minute) - datetime.timedelta(microseconds=1)
        elif interval == Client.KLINE_INTERVAL_2HOUR:
            start_end_candle_time = now - datetime.timedelta(hours=divmod(now.hour, 2)[1], minutes=now.minute) - datetime.timedelta( microseconds=1)
        elif interval == Client.KLINE_INTERVAL_4HOUR:
            start_end_candle_time = now - datetime.timedelta(hours=divmod(now.hour, 4)[1], minutes=now.minute) - datetime.timedelta( microseconds=1)
        elif interval == Client.KLINE_INTERVAL_6HOUR:
            start_end_candle_time = now - datetime.timedelta(hours=divmod(now.hour, 6)[1], minutes=now.minute) - datetime.timedelta( microseconds=1)
        elif interval == Client.KLINE_INTERVAL_8HOUR:
            start_end_candle_time = now - datetime.timedelta(hours=divmod(now.hour, 8)[1], minutes=now.minute) - datetime.timedelta( microseconds=1)
        elif interval == Client.KLINE_INTERVAL_12HOUR:
            start_end_candle_time = now - datetime.timedelta(hours=divmod(now.hour, 12)[1], minutes=now.minute) - datetime.timedelta( microseconds=1)

        elif interval == Client.KLINE_INTERVAL_1DAY:
            start_end_candle_time = now - datetime.timedelta(hours=now.hour, minutes=now.minute) - datetime.timedelta( microseconds=1)
        elif interval == Client.KLINE_INTERVAL_3DAY:
            start_end_candle_time = now - datetime.timedelta(days=divmod(now.day - 1,3)[1], hours=now.hour, minutes=now.minute) - datetime.timedelta( microseconds=1)

        elif interval == Client.KLINE_INTERVAL_1WEEK:
            start_end_candle_time = now - datetime.timedelta(days=now.weekday(), hours=now.hour, minutes=now.minute) - datetime.timedelta( microseconds=1)

        elif interval == Client.KLINE_INTERVAL_1MONTH:
            start_end_candle_time = now - datetime.timedelta(days=now.day - 1, hours=now.hour, minutes=now.minute) - datetime.timedelta( microseconds=1)
        else:
            err = 'invalid interval'
            return err

        if date_time >= start_end_candle_time:
            return True
        return False

    def _load_and_set_last_real_candle_historical(self, symbol, interval, start_datetime, end_datetime, add_to_database = True):
        # not tested and complete
        try:
            result = []
            data = self.client.get_historical_klines(symbol=symbol, interval=interval, start_str=str(start_datetime),
                                                     end_str=str(end_datetime))
            for item in data:
                # print(data)
                # result.append([str(timestamp_to_datetime(item[0])),  # open time
                result.append([symbol,
                               self.timestamp_to_datetime(item[0]),  # open time
                               item[1],  # open
                               item[2],  # high
                               item[3],  # low
                               item[4],  # close
                               item[5],  # volume
                               item[8]  # number of trade
                               ])
            if add_to_database is True:
                err = self.db.set_last_real_candle_historical_data(interval=interval, data=result)
                print('err:', err)
            return result

        except Exception as e:
            print(str(e))
            sleep(10)
            return str(e)

    def load_and_set_last_real_candle_historical(self, symbol, interval, end_datetime=None, add_to_database = False):
        # not complete and tested
        start_def = datetime.datetime.utcnow()
        now = datetime.datetime.utcnow()
        if end_datetime is None:
            start = self.get_candle_open_time(interval=interval, date_time=now)
            end = now
        else:
            start = self.get_candle_open_time(interval=interval, date_time=end_datetime)
            end = end_datetime

        # ------------------
        print('start:',start)
        print("end",end)
        a = self._load_and_set_last_real_candle_historical(symbol=symbol, interval=interval, start_datetime=start,
                                                           end_datetime=end, add_to_database=add_to_database)
        sum = 0
        for item in a:
            print(item)
            sum += 1
        print(sum)

        print("total time:", datetime.datetime.utcnow() - start_def)
        return a



if __name__ == "__main__":
    from api_setting import *
    from app_setting import get_db_info, client_id

    cli = LoadData(api_key=api_key,api_secret=api_secret,db_info=get_db_info(db_server_id=client_id),
                   timestamp_base_time=timestamp_base_time,is_test=False)

    symbol = 'NANOBTC'
    interval = Client.KLINE_INTERVAL_1DAY
    start_datetime = datetime.datetime(year=2019, month=1, day=1, hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(seconds=-1, microseconds=0)
    end_datetime = datetime.datetime(year=2019, month=5, day=1, hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(seconds=0, microseconds=0)
    # print('get_last_candle_open_time: ', cli.get_candle_open_time(interval=interval, date_time=end_datetime))
    #
    # t = start_datetime + datetime.timedelta(seconds=0, microseconds=0)
    # print(t)
    # print(cli.get_last_candle_open_time(interval=interval, date_time=t))
    #
    # t = start_datetime + datetime.timedelta(seconds=1, microseconds=0)
    # print(t)
    # print(cli.get_last_candle_open_time(interval=interval, date_time=t))
    #
    # t = start_datetime + datetime.timedelta(seconds=-1, microseconds=0)
    # print(t)
    # print(cli.get_last_candle_open_time(interval=interval, date_time=t))

    print('start', start_datetime, 'end', end_datetime)
    res = cli.load_and_set_complete_candle_historical(symbol=symbol, interval=interval, start_datetime=start_datetime, end_datetime=end_datetime,add_to_database=True)

    try:
        print('###', len(res), res[0][1], res[-1][1])
    except Exception as e:
        print(res)
        print(e)
    print('===')
    # res = cli.generate_all_candle_open_time(interval=interval, start_datetime=start_datetime, end_datetime=end_datetime)
    # print(len(res), res[0], res[-1])

    # res = cli.generate_all_complete_candle_open_time(interval=interval, start_datetime=start_datetime, end_datetime=end_datetime)
    # print(len(res), res[0], res[-1])


    print('sleep')
    sleep(100)

    print('1 ===============')
    generate_complete_candle_open_time = cli.generate_all_complete_candle_open_time(interval=interval, start_datetime=start_datetime,
                                                                                    end_datetime=end_datetime)
    print('generate_complete_candle_open_time: ', len(generate_complete_candle_open_time), generate_complete_candle_open_time)


    complete_open_time_list = cli.generate_all_candle_open_time(interval=interval, start_datetime=start_datetime,
                                                                end_datetime=end_datetime)
    print('generate_all_candle_open_time: ', len(complete_open_time_list),complete_open_time_list)

    print('2 ===============')

    db_open_time_list = cli.db.get_complete_candle_historical_open_time(symbol=symbol, interval=interval,
                                                                        start_datetime=start_datetime,
                                                                        end_datetime=end_datetime)
    print('db_open_time_list: ', len(db_open_time_list), db_open_time_list)
    print('3 ===============')

    res=cli.load_and_set_complete_candle_historical(symbol=symbol, start_datetime=start_datetime, interval=interval,
                                                    end_datetime=end_datetime, add_to_database=True,
                                                    earlier_valid_timestamp=None)
    print('load_and_set_complete_candle_historical: ', len(res), res)


    # for item in complete_open_time_list:
    #     print(str(item))
    #
    # print('--------------')
    # for item in db_open_time_list:
    #     print(str(item[0]))

