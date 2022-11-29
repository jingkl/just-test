import time
import gevent
import threading
import random
from locust import User, events
from locust.stats import print_stats, print_percentile_stats
from locust.env import Environment

from client.common.common_func import get_spawn_rate
from client.common.common_type import Precision
from client.parameters.params import ConcurrentTasksParams, ConcurrentObjParams
from utils.util_log import log
from client.concurrent.locust_client import ClientTask, MyTaskSet
from client.util.api_request import func_time_catch


class TickStatsPrinter:
    """
    Timing print interface status
    """

    def __init__(self, env_stats, interval=20):
        self.env_stats = env_stats
        self.interval = interval
        self.stop_flag = False
        self.print_flag = False

    def start_print_stats(self):
        def print_stats_func():
            while self.stop_flag is False:
                self.print_flag = True
                print_stats(self.env_stats)
                print_percentile_stats(self.env_stats)
                self.print_flag = False
                gevent.sleep(self.interval)

        return gevent.spawn(print_stats_func)

    def _start_print_stats(self):
        self.print_flag = True
        print_stats(self.env_stats)
        self.print_flag = False
        if not self.stop_flag:
            t = threading.Timer(self.interval, self._start_print_stats)
            t.start()

    def stop_print_stats(self):
        self.stop_flag = True
        while self.print_flag is True:
            time.sleep(1)
        time.sleep(1)
        # print final stats
        log.info("Print locust final stats.")
        print_stats(self.env_stats)


class MyUser(User):
    pass


class LocustRunner:
    def __init__(self, obj: callable, obj_params: ConcurrentTasksParams, interval: int = 20, during_time: int = 60,
                 concurrent_number: int = 5, spawn_rate: int = None, request_type="grpc"):
        """
        :param obj: callable object of test
        :param obj_params: parameters of callable object
        :param interval: interval for printing statistics / second
        :param during_time: concurrency lasts time / second
        :param concurrent_number: int
        :param spawn_rate: int
        :param request_type: type of test request
        """
        self.obj = obj
        self.obj_params = obj_params
        self.interval = interval
        self.during_time = during_time
        self.concurrent_number = concurrent_number
        self.spawn_rate = spawn_rate if spawn_rate is not None else get_spawn_rate(self.concurrent_number)
        self.request_type = request_type

    def get_client_tasks(self):
        # MyUser.tasks = [MyTaskSet.search, MyTaskSet.query]
        for o in self.obj_params.all_obj:
            for w in range(eval("self.obj_params.{0}.weight".format(o))):
                MyUser.tasks.append(eval("MyTaskSet.{}".format(o)))
        log.debug("[LocustRunner] All concurrent tasks: {}".format(MyUser.tasks))

    @staticmethod
    def _quit(during_time, func):
        t = threading.Timer(during_time, func)
        t.start()

    def start_runner(self):
        MyUser.client = ClientTask(self.obj, request_type=self.request_type)
        MyUser.tasks_params = self.obj_params
        self.get_client_tasks()

        env = Environment(events=events, user_classes=[MyUser])
        runner = env.create_local_runner()

        # start to print statistical results regularly
        tick_stats = TickStatsPrinter(env_stats=env.stats, interval=self.interval)
        tick_stats.start_print_stats()
        # tick_stats._start_print_stats()

        # start the concurrency test
        runner.start(self.concurrent_number, spawn_rate=self.spawn_rate)
        gevent.spawn_later(self.during_time, lambda: runner.quit())
        # self._quit(self.during_time, runner.quit)
        runner.greenlet.join()
        # runner.stop()

        # Statistics for all interfaces
        api_result = {
            "Requests": round(env.stats.total.current_rps, Precision.CONCURRENT_PRECISION),
            "Fails": round(env.stats.total.fail_ratio, Precision.CONCURRENT_PRECISION),
            "RT_max": round(env.stats.total.max_response_time, Precision.CONCURRENT_PRECISION),
            "RT_avg": round(env.stats.total.avg_response_time, Precision.CONCURRENT_PRECISION)
        }

        # Stop printing interface results and runner
        runner.stop()
        tick_stats.stop_print_stats()

        log.info("Start sleep 600s")
        time.sleep(600)

        result = True if api_result["Fails"] == float(0) else False
        return api_result, result
