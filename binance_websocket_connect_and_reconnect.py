import pandas as pd
import datetime as dt
from threading import Thread
import time, json, traceback, threading, websocket, ctypes, inspect


class BAFWebsocketServer:
    # 币安 ws 数据链接示例，以 aggtrade 为例
    def __init__(self, coin, host='vps', ping_interval=60):
        url = f'wss://fstream.binance.com/ws/{coin}usdt@aggTrade'
        self.ws = websocket.WebSocketApp(url=url, on_message=self._on_message, on_error=self._on_error, on_close=self._on_close, on_open=self._on_open)
        self.host = host
        self.coin = coin
        self.key = f'bi_f_aggtrade_{coin}'
        self.ping_interval = ping_interval

    @property
    def now_sh(self):
        return dt.datetime.now()

    def _on_open(self):
        print(f"--- ws start {self.key} ---\n")

    def _on_error(self, error):
        print(error, {self.key})

    def _on_close(self):
        print(f"--- close {self.key} ---\n")

    def _on_message(self, message):
        info = json.loads(message)
        print(self.now_sh, info)

    def run(self):
        if self.host == 'vps':
            self.ws.run_forever(ping_interval=self.ping_interval)  # 可以直接连接币安行情数据
        else:
            self.ws.run_forever(http_proxy_host='127.0.0.1', http_proxy_port=33210, ping_interval=self.ping_interval)  # 如果需要代理连接币安行情数据，需要指定代理参数。


class Clock:
    # 定时工具
    @staticmethod
    def tick_set(millisec=200):
        now_sec = time.time() * 1000
        next_sec = (now_sec // millisec) * millisec + millisec
        seconds_sleep = (next_sec - now_sec) / 1000
        time_next_run = pd.to_datetime(next_sec, unit='ms') + dt.timedelta(hours=8)
        return time_next_run, seconds_sleep

    @staticmethod
    def run_next_second(frame_second='1s', lc_utc=8):
        frame_second = int(frame_second[:-1])
        assert 60 % frame_second == 0, 'frame_second must be divided by 60 with no remainder'
        time_now = dt.datetime.now() + dt.timedelta(hours=8 - lc_utc)
        time_second = time_now.second
        remain = time_second % frame_second
        time_last_run = time_now - dt.timedelta(microseconds=time_now.microsecond) - dt.timedelta(seconds=remain)
        time_next_run = time_last_run + dt.timedelta(seconds=frame_second)
        seconds_sleep = (time_next_run - time_now).total_seconds()
        return time_next_run, seconds_sleep


class SystemToolKit:
    # 线程控制工具
    @staticmethod
    def __async_raise(tid, exctype):
        """
        raises the exception, performs cleanup if needed
        https://stackoverflow.com/questions/323972/is-there-any-way-to-kill-a-thread
        """
        tid = ctypes.c_long(tid)
        if not inspect.isclass(exctype):
            exctype = type(exctype)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
        if res == 0:
            raise ValueError("invalid thread id")
        elif res != 1:
            # """if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the effect"""
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    @staticmethod
    def stop_thread(thread):
        # 通过线程名称终止线程
        SystemToolKit.__async_raise(thread.ident, SystemExit)

    @staticmethod
    def stop_thread_byident(threadident):
        # 通过线程id终止线程
        SystemToolKit.__async_raise(threadident, SystemExit)

    @staticmethod
    def cur_threads():
        # 获取当前线程的所有线程名称和id
        threads = threading.enumerate()
        threads_names = [thr.name for thr in threads]
        threads_idents = [thr.ident for thr in threads]
        return threads, threads_names, threads_idents


class BAFWebsocketServerForever:
    def __init__(self, coin, data_length=1000, frame_parser='5s', frame_check='5s', thresh_check=6, host='vps', ping_interval=60):
        url = f'wss://fstream.binance.com/ws/{coin}usdt@aggTrade'
        self.ws = websocket.WebSocketApp(url=url, on_message=self._on_message, on_error=self._on_error, on_close=self._on_close, on_open=self._on_open)
        self.coin = coin
        self.host = host  # 主机类型，如果是可以接通币安的服务器，填写vps，否则在 run_forever 处指定代理参数
        self.key = f'bi_f_aggtrade_{coin}'
        self.ping_interval = ping_interval  # 使用ws链接ping一次的间隔秒数

        # 数据解析整理时间频率
        self.frame_parser = frame_parser
        # 更新检查时间频率
        self.frame_check = frame_check

        # 更新检测相关变量
        self.t_update = 0  # 最近一次更新的时间戳
        self.updating = True  # 数据是否更新
        self.id_restart = 0
        self.thresh_check = thresh_check  # 时间戳的更新抽样数目

        # 数据存储相关变量
        self.bar_latest = {'vb': 0, 'vs': 0, 'vol': 0, 'amt': 0}
        self.bar_increment_df = pd.DataFrame()
        self.data_length = data_length

    @property
    def now_sh(self):
        return dt.datetime.now()

    def _kill_thread(self, thread):
        # 通过线程对象终止线程
        try:
            SystemToolKit.stop_thread(thread=thread)
            print(self.now_sh, thread.name, 'killed')
        except:
            exc_info = traceback.format_exc()
            print(self.now_sh, thread.name, 'except_info:\n', exc_info)

    def _kill_thread_byident(self, threadident):
        # 通过线程 id 终止线程
        try:
            SystemToolKit.stop_thread_byident(threadident=threadident)
        except:
            exc_info = traceback.format_exc()
            print(self.now_sh, f'threadident {threadident}', 'except_info:\n', exc_info)

    def _on_open(self):
        if not self.id_restart:
            print(f"--- ws start {self.key} ---\n")
        else:
            print(f"--- ws restart {self.key} ---\n")
        self.id_restart += 1

    def _on_error(self, error):
        print(error, {self.key})

    def _on_close(self):
        print(f"--- close {self.key} ---\n")

    def _on_message(self, message):
        # region 接收到ws数据推送后每一条推送的处理逻辑
        info = json.loads(message)
        price_trade = float(info['p'])
        quantity_trade = float(info['q'])
        amt_trade = price_trade * quantity_trade
        if info['m']:
            self.bar_latest['vs'] += quantity_trade
        else:
            self.bar_latest['vb'] += quantity_trade
        self.bar_latest['vol'] += quantity_trade
        self.bar_latest['amt'] += amt_trade
        # endregion

        self.t_update = info['T']  # 更新最新的时间戳

    @property
    def frame_latest(self):
        frame = pd.Series(self.bar_latest)
        return frame

    # 间隔一段时间进行数据整理和解析，非必须，可根据自己的需求定制
    def parser(self):
        while True:
            if self.bar_latest['vol']:
                break
            time.sleep(1)

        frame_previous = self.frame_latest
        while True:
            time_next_run, seconds_sleep = Clock.run_next_second(frame_second=self.frame_parser)
            time.sleep(seconds_sleep)

            frame_latest = self.frame_latest
            increment = frame_latest - frame_previous
            ser_sec = increment.loc[['vb', 'vs']]
            ser_sec['vwap'] = increment.loc['amt'] / increment.loc['vol']
            frame_previous = frame_latest

            ser_sec.name = time_next_run
            self.bar_increment_df = pd.concat([self.bar_increment_df, ser_sec.to_frame().T], axis=0, sort=False).fillna(method='ffill').iloc[-self.data_length:, :]
            self.bar_increment_df.index.name = 'time'
            print(time_next_run, self.now_sh)
            print(self.bar_increment_df)

    # 数据更新检查，通过 self.t_update 的更新情况判断 ws 数据流是否断开
    def updated_check(self):
        while True:
            if self.bar_latest['vol']:
                break
            time.sleep(1)
        update_t_list = []
        while True:
            time_next_run, seconds_sleep = Clock.run_next_second(frame_second=self.frame_check)
            time.sleep(seconds_sleep)
            update_t_list.append(self.t_update)
            update_t_list = update_t_list[-self.thresh_check:]
            print('update_t_list')
            print(update_t_list)
            print(set(update_t_list))
            print(len(set(update_t_list)))

            # 当列表 update_t_list 中有 self.thresh_check 个等时间间隔（self.frame_check）的 self.t_update 抽样时，且所有的 self.t_update 值都相等时，认为ws数据没有更新，所以此时认为ws已经断开。
            not_updated = len(set(update_t_list)) == 1 and len(update_t_list) == self.thresh_check
            if not_updated:
                info = f'{self.key} not updated'
                print(self.now_sh, info)
                self.updating = False

    # ws 链接线程，使得程序可以从ws接收数据，让 self._on_message 中的逻辑正常运行
    @property
    def gen_worker(self):
        if self.host == 'vps':
            worker = Thread(target=lambda: self.ws.run_forever(ping_interval=self.ping_interval))
        else:
            worker = Thread(target=lambda: self.ws.run_forever(http_proxy_host='127.0.0.1', http_proxy_port=33210, ping_interval=self.ping_interval))
        worker.name = f'worker {str(self.now_sh)[:19]}'
        return worker

    # 数据解析线程，非必须，可根据自己的需求定制
    @property
    def gen_parser(self):
        parser = Thread(target=self.parser)
        parser.name = f'parser {str(self.now_sh)[:19]}'
        return parser

    # ws 数据更新检查线程，唯一的作用是当我们认为 self.t_update 一段时间 （self.thresh_check - 1 个 self.frame_check）没有更新时，将 self.updating 设置为 False
    @property
    def gen_surveillance(self):
        surveillance = Thread(target=self.updated_check)
        surveillance.name = f'surveillance {str(self.now_sh)[:19]}'
        return surveillance

    # 主线程，让子线程 worker 收取 ws 数据，让子线程 parser 进一步解析数据，让子线程 surveillance 检查 ws 数据的更新情况并以此判断 ws 是否正常链接还是断开，并且处理 ws 的断开重连任务
    def run(self):
        worker = self.gen_worker
        parser = self.gen_parser
        surveillance = self.gen_surveillance
        worker.start()
        parser.start()
        surveillance.start()

        while True:
            time_next_run, seconds_sleep = Clock.run_next_second(frame_second=self.frame_check)
            time.sleep(seconds_sleep)
            if not self.updating:
                # 当程序认为 ws 链接已经断开时
                threads, threads_names, threads_idents = SystemToolKit.cur_threads()  # 获取当前所有线程列表，线程名列表，线程id列表

                # region 终止所有的非主线程，所有的有 worker 或 parser 或 surveillance 标识的子线程
                sub_threads_ident = [ident for ident, name in zip(threads_idents, threads_names) if 'MainThread' not in name]
                sur_threads_ident = [ident for ident, name in zip(threads_idents, threads_names) if 'surveillance' in name or 'parser' in name or 'worker' in name]
                self.ws.close()
                for ident in sub_threads_ident:
                    self._kill_thread_byident(threadident=ident)
                for ident in sur_threads_ident:
                    self._kill_thread_byident(threadident=ident)
                # endregion

                # region 通过对象将之前定义好的子线程终止
                self._kill_thread(thread=worker)
                self._kill_thread(thread=parser)
                self._kill_thread(thread=surveillance)
                # endregion
                # 在以上终止子线程的过程中会有重复，可能一个线程会被程序尝试多次终止，这个没什么影响，目的是将所有的非主线程杀干净，（强迫症 + 洁癖理解一下）

                # region 旧的子线程终止之后重新定义新的子线程并启动，完成 ws 的重启任务
                worker = self.gen_worker
                parser = self.gen_parser
                surveillance = self.gen_surveillance
                worker.start()
                parser.start()
                surveillance.start()
                self.updating = True
                info = f'{self.key} ws restart'
                print(self.now_sh, info)
                # endregion


if __name__ == '__main__':
    # 直接连接 ws 获取数据
    # BAFWebsocketServer(coin='btc', host='local').run()

    # 通过多线程连接 ws 获取数据，同时检查更新 ws 的连接情况，必要时通过终止旧进程，开启新进程的方式处理 ws 断开重连
    BAFWebsocketServerForever(coin='btc', data_length=1000, host='local', frame_parser='5s', frame_check='1s', thresh_check=10).run()
