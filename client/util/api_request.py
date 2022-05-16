import traceback
import time
import functools
from pprint import pformat
from typing import Tuple

from commons.common_type import PRECISION
from commons.common_func import truncated_output
from utils.util_log import log

log_row_length = 300
info_logout = ["Collection.insert", "Index", "Collection.load", "Collection.search", "Collection.query"]


def time_catch():
    def wrapper(func):
        # @functools.wraps(func)
        def inner_wrapper(*args, **kwargs) -> Tuple[tuple, bool]:
            try:
                start = time.perf_counter()
                res = func(*args, **kwargs)
                rt = time.perf_counter() - start

                log.debug("(api_response) : %s " % truncated_output(res, log_row_length))

                func_name = args[0][0].__qualname__
                msg = "[Time] {0} run in {1}s".format(func_name, round(rt, PRECISION.COMMON_PRECISION))
                if callable(args[0][0]) and func_name in info_logout:
                    log.info(msg)
                else:
                    log.debug(msg)

                return (res, rt), True
            except Exception as e:
                log.error(traceback.format_exc())
                log.error("(api_response) : %s" % truncated_output(e, log_row_length))
                return (e, 0), False

        return inner_wrapper
    return wrapper


@time_catch()
def api_request(_list, **kwargs):
    if isinstance(_list, list):
        func = _list[0]
        if callable(func):
            arg = []
            if len(_list) > 1:
                for a in _list[1:]:
                    arg.append(a)

            log.debug("(api_request)  : [%s] args: %s, kwargs: %s" % (func.__qualname__,
                                                                      truncated_output(arg, log_row_length),
                                                                      str(kwargs)))

            return func(*arg, **kwargs)
    return (False, 0), False


def time_wrapper(func):
    """
    This decorator prints the execution time for the decorated function.
    """

    def wrapper(*args, **kwargs):
        _rt = kwargs.get("_rt", False)
        kwargs.pop("_rt", None)

        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()

        response_time = end - start
        msg = "[time_wrapper] {} run in {}s".format(func.__name__, round(response_time, PRECISION.COMMON_PRECISION))

        if _rt:
            log.info(msg)
            return result, response_time
        else:
            log.debug(msg)
            return result

    return wrapper

