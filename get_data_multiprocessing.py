from Log import Log_Mod, Logging
import database
# from termcolor import colored
from time import sleep
from my_time import get_now_time_second
from multiprocessing import Process, Lock, Manager
import psutil
from get_data import LoadData
import ast
from colored_print import colored_print


class load_and_set_complete_candle_historical_process_obj(Process):

    def __init__(self, data):
        self.database_info = data['database_info']
        self.api_key = data['api_key']
        self.api_secret = data['api_secret']
        self.timestamp_base_time = data['timestamp_base_time']
        self.is_test = data['is_test']
        self.public_ip = data['public_ip']
        self.process_auto_cropped_time = data['process_auto_cropped_time']

        self.process_id = data['process_id']
        self.all_process_status = data['all_process_status']
        self.lock = data['lock']
        self.wait_list = data['wait_list']
        self.complete_list = data['complete_list']
        self.running_list = data['running_list']
        self.fail_list = data['fail_list']

        # self.max_thread = data['max_thread']
        # self.max_run_time = data['max_run_time']
        self.lock_acquire_wait = data['lock_acquire_wait']

        # self.db = database.DataBase(db_info=self.database_info)

        Process.__init__(self, name=str(self.process_id))
        self.is_hang = False

    def process(self):
        log_file_name = 'log.txt'
        log_table_name = 'bot_log'
        logging_mod = Log_Mod.console_file

        obj = get_data_multiprocessing(process_id=self.process_id,
                                       db_info=self.database_info,
                                       lock=self.lock,
                                       lock_acquire_wait=self.lock_acquire_wait,
                                       wait_list=self.wait_list,
                                       complete_list=self.complete_list,
                                       running_list=self.running_list,
                                       fail_list=self.fail_list,
                                       all_process_status=self.all_process_status,
                                       api_key=self.api_key,
                                       api_secret=self.api_secret,
                                       timestamp_base_time=self.timestamp_base_time,
                                       is_test=self.is_test,
                                       public_ip=self.public_ip,
                                       log_file_name=log_file_name,
                                       log_table_name=log_table_name,
                                       logging_mod=logging_mod,
                                       log_obj=None,
                                       process_auto_cropped_time=self.process_auto_cropped_time)

        return obj.run()

    def run(self):
        self.process()

        return


class load_and_set_complete_candle_historical_multi_process:

    def __init__(self, api_key, api_secret, timestamp_base_time, is_test, public_ip,
                 database_info, wait_list, client_id, runtime_setting_file_path_name, process_auto_cropped_time):
        # self.print_color = 'cyan'
        self.p = colored_print(default_text_color='cyan')
        self.api_key = api_key
        self.api_secret = api_secret
        self.timestamp_base_time = timestamp_base_time
        self.is_test = is_test
        self.public_ip = public_ip
        self.runtime_setting_file_path_name = runtime_setting_file_path_name
        self.process_auto_cropped_time = process_auto_cropped_time
        self.database_info = database_info
        self.db = database.DataBase(db_info=self.database_info)
        # self.client_id = client_id
        self.lock = Lock()
        self.manager = Manager()
        self.all_process_status = self.manager.dict()
        self.wait_list = self.manager.list()
        self.complete_list = self.manager.list()
        self.running_list = self.manager.list()
        self.fail_list = self.manager.list()
        # self.hang_list = list()

        self.process_id = -1
        self.max_run_time = 600
        self.lock_acquire_wait = 15

        for item in wait_list:
            self.wait_list.append(item)

        self.data = dict()  # common data from process

        self.process_list = list()
        self.lock_status = False
        self.first_start_flag = True

        # check runtime setting file exiting
        self.check_runtime_setting_file()
        runtime_setting, error = self.get_runtime_setting()
        if error is not None:
            raise Exception(error)

        self.max_process = runtime_setting['max_process']
        self.set_data()

    def set_data(self):
        self.data['database_info'] = self.database_info
        # self.data['client_id'] = self.client_id
        self.data['all_process_status'] = self.all_process_status

        self.data['lock'] = self.lock
        self.data['wait_list'] = self.wait_list
        self.data['complete_list'] = self.complete_list
        self.data['running_list'] = self.running_list
        self.data['fail_list'] = self.fail_list

        self.data['max_process'] = self.max_process
        self.data['process_id'] = self.process_id
        self.data['max_run_time'] = self.max_run_time
        self.data['lock_acquire_wait'] = self.lock_acquire_wait

        self.data['api_key'] = self.api_key
        self.data['api_secret'] = self.api_secret
        self.data['timestamp_base_time'] = self.timestamp_base_time
        self.data['is_test'] = self.is_test
        self.data['public_ip'] = self.public_ip

        self.data['process_auto_cropped_time'] = self.process_auto_cropped_time

    def set_status(self, obj_name, item, value):
        s = self.all_process_status[obj_name]
        s[item] = value
        self.all_process_status[obj_name] = s

    # @staticmethod
    def terminate_process_tree(self, process, include_parent=True, timeout=None):
        # print(colored('process id: {}'.format(process.pid), 'blue'))
        self.p.print('process id: {}'.format(process.pid), 'blue')

        process_child = psutil.Process(process.pid).children(recursive=True)

        # print(colored(process_child, 'red'))
        self.p.print(process_child, 'red')

        # send SIGTERM
        for ch_p in process_child:
            try:
                # print(colored(ch_p, 'red'))
                self.p.print(ch_p, 'red')
                ch_p.terminate()
            except psutil.NoSuchProcess:
                pass
        gone, alive = psutil.wait_procs(process_child, timeout=timeout)
        if alive:
            # send SIGKILL
            for ch_p in alive:
                print("process {} survived SIGTERM; trying SIGKILL" % process)
                try:
                    ch_p.kill()
                except psutil.NoSuchProcess:
                    pass
            gone, alive = psutil.wait_procs(alive, timeout=timeout)
            if alive:
                # give up
                for ch_p in alive:
                    print("process {} survived SIGKILL; giving up" % process)
                    return False

        if include_parent is True:
            process.terminate()
            process.join()
        # self.hang_process_list.remove(process)

        return True

    # def print_c(self, text, color=None):
    #     try:
    #         if color is None:
    #             print(colored(text, self.print _color))
    #         else:
    #             print(colored('| ', self.print _color) + colored(text, color))
    #             # print(colored(text, color))
    #     except Exception as e:
    #         self.print _c(str(e), 'red')

    def stop_all_process(self):
        if self.lock.acquire(timeout=self.lock_acquire_wait) is True:
            self.lock_status = True
            for p in self.process_list:
                try:
                    self.set_status(p.name, 'stop_flag', True)

                except Exception as e:
                    self.p.print('cant stop process: {0} ; error: {1}'.format(p.name, str(e)), 'red')
            self.lock.release()
            self.lock_status = False

    def check_runtime_setting_file(self):
        try:
            f = open(self.runtime_setting_file_path_name, 'x')
            f.close()
            # add default setting
            default_runtime_setting = {"main_stop_flag": False,
                                       "max_process": 1
                                       }
            f = open(self.runtime_setting_file_path_name, 'w')
            f.write(str(default_runtime_setting))
            f.close()
        except Exception as e:
            # file exist
            self.p.print(str(e))

        return True

    def get_runtime_setting(self):
        try:
            f = open(self.runtime_setting_file_path_name, 'r')
            setting = f.readline()
            f.close()
            a = ast.literal_eval(setting)
            return a, None
        except Exception as e:
            print('get_runtime_setting error: ', str(e))
            return {}, str(e)

    def run_client(self):
        self.first_start_flag = True
        self.lock_status = False
        # self.hang_time = self.max_run_time * 3 * self.max_process
        while True:
            try:
                self.p.print('get setting')
                runtime_setting, error = self.get_runtime_setting()
                if error is not None:
                    raise Exception(error)
                # print(runtime_setting)
                main_stop_flag = runtime_setting['main_stop_flag']
                self.max_process = runtime_setting['max_process']

                # check exit condition
                self.p.print('check exit condition')
                if self.first_start_flag is True:
                    self.first_start_flag = False
                else:
                    if len(self.process_list) == 0 and (main_stop_flag is True or len(self.wait_list) == 0):
                        self.p.print('exit function from condition: 1')  # end of try
                        result = True
                        break

                self.p.print('check stop flag')
                if main_stop_flag is True:
                    self.p.print('stop all process from user')
                    self.stop_all_process()

                # ---- create process object and run it ----
                else:
                    if len(self.process_list) < self.max_process and \
                            (len(self.wait_list) > 0 or
                             (len(self.complete_list) + len(self.running_list) + len(self.fail_list)) == 0):
                        need_process = self.max_process - len(self.process_list) - 2
                        if need_process > 0:
                            for i in range(need_process):
                                if len(self.wait_list) <= 1:
                                    print('empty wait list')
                                    break
                                # ایجاد پروسس جدید
                                self.process_id += 1
                                self.p.print('create new process id:{0}'.format(self.process_id))
                                self.set_data()
                                p = load_and_set_complete_candle_historical_process_obj(self.data)
                                sleep(0.5)
                                p.start()
                                self.process_list.append(p)

                        # ایجاد پروسس جدید
                        self.process_id += 1
                        self.p.print('create new process id:{0}'.format(self.process_id))
                        self.set_data()
                        p = load_and_set_complete_candle_historical_process_obj(self.data)
                        sleep(0.5)
                        p.start()
                        self.process_list.append(p)

                    elif len(self.process_list) > self.max_process:
                        self.p.print('stop all process because upper than max process')
                        self.stop_all_process()

                # ---- check process runtime ----
                # if self.lock.acquire(timeout=self.lock_acquire_wait) is True:
                #     self.lock_status = True
                #     self.p.print('check process runtime')
                #     for p in self.process_list:
                #         # check started process
                #         if p.name in self.status:
                #             if 'last_run_time' in self.status[p.name]:
                #                 # check process runtime
                #                 try:
                #                     if (get_now_time_second() - self.status[p.name]['last_run_time']) > self.max_run_time:
                #                         self.p.print('stop process {0} because max runtime'.format(p.name))
                #                         self.set_status(p.name, 'stop_flag', True)
                #
                #                     # if  (get_now_time_second() - self.status[p.name]['last_run_time']) > self.hang_time:
                #                     #     self.p.print('terminate process {0} because hanged')
                #                     #
                #                     #     hang_item = self.status[p.name]['current_running_share']
                #                     #     if hang_item not in self.hang_list:
                #                     #         self.hang_list.append(hang_item)
                #                     #         self.p.print('terminate process: {0} ; en_symbol_12_digit_code: {1} ; tsetmc_id: {2} ; date_m: {3} ; process: {4}'.format(p.name, hang_item[0], hang_item[1],hang_item[2], p))
                #                     #         # time.sleep(15)
                #                     #         if self.terminate_process_tree(process=p, include_parent=True, timeout=10) is True:
                #                     #             # self.db.add_share_to_fail_hang_share(en_symbol_12_digit_code=hang_item[0], date_m=str(hang_item[2]))
                #                     #             self.db.collect_all_share_data_rollback(en_symbol_12_digit_code=hang_item[0], tsetmc_id=str(hang_item[1]), date_m=hang_item[2], error_msg='hang process', error_code=9000)
                #                     #
                #                     #             self.running_list.remove(hang_item)
                #                     #             # self.hang_list.remove(hang_item)
                #                     #
                #                     #         else:
                #                     #             self.db.add_share_to_fail_hang_share(en_symbol_12_digit_code=hang_item[0], date_m=hang_item[1])
                #
                #                 except Exception as e:
                #                     self.p.print('except: {0} ; error: {1} ; process: {2}'.format('cant check process runtime', str(e), p))
                #
                #     self.lock.release()
                #     self.lock_status = False

                # ---- check not alive process ----
                self.p.print('check not alive process')
                for p in self.process_list:
                    if p.is_alive() is False:
                        self.p.print('terminate process: {}'.format(p.name))
                        p.terminate()
                        p.join()
                        self.process_list.remove(p)

                # print status
                self.lock.acquire(timeout=self.lock_acquire_wait)
                self.lock_status = True
                process_symbols = list()
                for p in self.process_list:
                    try:
                        a = self.all_process_status[p.name]['current_running_share']
                        process_symbols.append('{0}: {1}: {2}: {3}: {4}'.format(a[0], a[1], a[2], a[3], a[5]))
                    except:
                        pass

                running_list_symbol = list()
                for p in self.running_list:
                    running_list_symbol.append('{0}:{1}:{2}'.format(p[0], p[1], p[2]))

                # hang_symbol = list()
                # for p in self.hang_list:
                #     hang_symbol.append('{0}:{1}:{2}'.format(p[0], p[1], p[2]))

                color = 'magenta'
                self.p.print('wait_list:{0}  complete_list:{1}  running_list:{2}  fail_list:{3}  '
                             'alive_process:{4}  symbols:{5}'
                             .format(len(self.wait_list), len(self.complete_list), len(self.running_list),
                                     len(self.fail_list), len(self.process_list),
                                     process_symbols), color)

                self.lock.release()
                self.lock_status = False

                sleep(5)
                # ----------------------

            except Exception as e:
                if self.lock_status is True:
                    self.p.print('main except: lock status: True :' + str(3) + ' : ' + str(e))
                    for p in self.process_list:
                        try:
                            self.p.print('terminate process: ' + p.name)
                            self.set_status(p.name, 'stop_flag', True)
                        except Exception as e:
                            self.p.print('main except: ' + str(4) + ' : ' + str(e))
                    self.lock.release()
                else:
                    self.p.print('main except: lock status: False :' + str(5) + ' : ' + str(e))
                    self.lock.acquire()
                    for p in self.process_list:
                        try:
                            self.p.print('terminate process: ' + p.name)
                            self.set_status(p.name, 'stop_flag', True)
                        except Exception as e:
                            self.p.print('main except: ' + str(6) + ' : ' + str(e))
                    self.lock.release()

                self.p.print('wait to exit all process')
                while len(self.process_list) > 0:
                    try:
                        for p in self.process_list:
                            self.p.print('terminate process: {}'.format(p.name))
                            p.terminate()

                        for p in self.process_list:
                            self.p.print('wait to exit thread: {}'.format(p.name))
                            p.join()
                    except:
                        pass

                self.p.print('exit main: 2')
                result = False
                break
        return result


class get_data_multiprocessing:

    def __init__(self, process_id, db_info, lock, lock_acquire_wait,
                 wait_list, complete_list, running_list, fail_list, all_process_status,
                 api_key, api_secret, timestamp_base_time, is_test, public_ip,process_auto_cropped_time,
                 log_file_name=None, log_table_name=None, logging_mod=None, log_obj=None):

        self.p = colored_print(default_text_color='cyan')
        self.process_id = process_id

        self.wait_list = wait_list
        self.complete_list = complete_list
        self.running_list = running_list
        self.fail_list = fail_list
        self.lock = lock
        self.lock_acquire_wait = lock_acquire_wait
        self.process_auto_cropped_time = process_auto_cropped_time
        # ---- define status dict and setup it ----
        self.obj_status = dict()
        self.all_process_status = all_process_status
        self.all_process_status[str(self.process_id)] = self.obj_status
        self.set_status('stop_flag', False)
        self.set_status('last_run_time', get_now_time_second())
        self.set_status('current_running_share', None)
        self.set_status('state', 'initialing')

        # ------------------------------
        # log param
        default_log_file_name = 'Logging.log'
        default_log_table_name = 'bot_log'
        default_logging_mod = Log_Mod.console
        self.log_obj = None
        # ----- create log object -------------------------
        if log_obj is None:
            if log_file_name is not None:
                log_file_name = default_log_file_name
            if log_table_name is not None:
                log_table_name = default_log_table_name
            if logging_mod is not None:
                logging_mod = default_logging_mod

            self.log_obj = Logging()
            self.log_obj.logConfig(log_file_name=log_file_name, log_table_name=log_table_name,
                                   logging_mod=logging_mod, db_obj=None)
        else:
            self.log_obj = log_obj

        # ----- create database object -------------------------
        # self.db = database.DataBase(db_info=db_info, log_obj=self.log)
        # if self.db is None:
        #     return
        # ------------------------------
        # self.print_color = 'green'

        self.client_obj = LoadData(api_key=api_key, api_secret=api_secret, db_info=db_info,
                                   timestamp_base_time=timestamp_base_time, is_test=is_test,
                                   log_obj=self.log_obj, public_ip=public_ip)

    # -------------------------------
    # def print_c(self, text, color=None):
    #     try:
    #         if color is None:
    #             print(colored(text, self.print _color))
    #         else:
    #             print(colored('| ', self.print _color) + colored(text, color))
    #     except Exception as e:
    #         self.print _c(str(e), 'red')

    def set_status(self, item, value):
        self.obj_status = self.all_process_status[str(self.process_id)]
        self.obj_status[item] = value
        self.all_process_status[str(self.process_id)] = self.obj_status

    def get_status(self, item):
        self.obj_status = self.all_process_status[str(self.process_id)]
        return self.obj_status[item]

    def run(self):
        lock_status = False
        # error = None
        # error_code = None

        self.p.print('worker: {0} :{1}'.format(self.process_id, 'start run function'))

        start_time = get_now_time_second()

        while self.get_status('stop_flag') is False:
            self.set_status('last_run_time', get_now_time_second())

            self.lock.acquire()
            lock_status = True

            # check exit condition
            if len(self.wait_list) <= 0:
                self.lock.release()
                lock_status = False
                self.p.print('worker: {0} :{1}'.format(self.process_id, 'wait list empty'))
                break

            #  گرفتن یک آیتم
            # get new item
            try:
                self.current_running_crypto = None
                self.current_running_crypto = self.wait_list.pop()
                self.running_list.append(self.current_running_crypto)
                self.lock.release()
                lock_status = False

            except Exception as e:
                if self.current_running_crypto is not None:  # error in running_list append item
                    if self.current_running_crypto not in self.wait_list:
                        self.wait_list.append(self.current_running_crypto)

                self.current_running_crypto = None
                self.lock.release()
                lock_status = False
                self.p.print('worker: {0} :{1} ;{2}'.format(self.process_id, 'get new crypto : fail', str(e)))
                continue

            self.set_status('current_running_share', self.current_running_crypto)

            symbol = self.current_running_crypto[0]
            interval = self.current_running_crypto[1]
            start_datetime = self.current_running_crypto[2]
            end_datetime = self.current_running_crypto[3]
            add_to_database = self.current_running_crypto[4]
            earlier_valid_timestamp = self.current_running_crypto[5]

            try:
                self.p.print('worker: {0} :{1}'.format(self.process_id, 'start collect data'))
                self.set_status('state', 'running')

                if self.process_auto_cropped_time is True:
                    error = self.client_obj.load_and_set_complete_candle_historical_auto_cropped_time(
                        symbol=symbol,
                        interval=interval,
                        start_datetime=start_datetime,
                        end_datetime=end_datetime,
                        add_to_database=add_to_database,
                        earlier_valid_timestamp=earlier_valid_timestamp)
                else:
                    error = self.client_obj.load_and_set_complete_candle_historical_do_not_cropped_time(
                        symbol=symbol,
                        interval=interval,
                        start_datetime=start_datetime,
                        end_datetime=end_datetime,
                        add_to_database=add_to_database,
                        earlier_valid_timestamp=earlier_valid_timestamp)

                if error is not True:
                    self.p.print('---  error: {}'.format(error))
                    if error != 'no eny data in time range':
                        raise Exception(error)

                self.lock.acquire()
                lock_status = True
                self.running_list.remove(self.current_running_crypto)
                self.complete_list.append(self.current_running_crypto)
                self.lock.release()
                lock_status = False

                self.p.print('worker: {0} :{1}'.format(self.process_id, 'end collect data'))

            except Exception as e:
                # sleep(5)
                if lock_status is True:
                    self.p.print(
                        'worker {0} except: lock status: {1} ; error: {2}'.format(str(self.process_id), True, str(e)))
                    self.set_status('state', 'failing')
                    self.lock.release()
                else:
                    # self.p.print('worker {0} except: lock status: {1} : {2} :{3}'.format(str(self.id), False, 14, e))
                    self.p.print(
                        'worker {0} except: lock status: {1} ; error: {2}'.format(str(self.process_id), False, str(e)))
                    self.set_status('state', 'failing')

                # try:
                #     self.p.print('worker: {0} : except: {1}'.format(current_process().name, 'rollback data'))
                #     self.db.collect_all_share_data_rollback(en_symbol_12_digit_code, tsetmc_id, date_m, error_msg=error, error_code=error_code)
            # finally:
                self.lock.acquire()
                self.running_list.remove(self.current_running_crypto)
                self.fail_list.append(self.current_running_crypto)
                self.lock.release()
                self.p.print('worker: {0} :{1}'.format(str(self.process_id), 'fail'))

        self.p.print('worker: {0} :{1}'.format(str(self.process_id), 'quit'))

        end_time = get_now_time_second()

        self.p.print('runtime:{0}'.format(end_time - start_time), color='red')

        return True
