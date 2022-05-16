import functools


from client.common.common_func import check_key_exist, get_must_params, get_required_params, check_params_type
from utils.util_log import log


def check_params(param_template: dict):
    def wrapper(func):
        @functools.wraps(func)
        def inner_wrapper(*args, **kwargs):
            params = kwargs.get("params", None)
            log.info("[check_params] {0} input params: {1}".format(func.__name__, params))
            msg = "[check_params] Check Params Failed!"

            # Check whether the required parameters are included in the input parameters
            must_params = get_must_params(param_template)
            res = check_key_exist(must_params, params) if isinstance(params, dict) else False
            if res is False:
                log.error(msg)
                raise Exception(msg)

            required_params = get_required_params(params, param_template)
            # Check if the required parameter type conforms to the definition
            check_res = check_params_type(required_params, param_template)
            if check_res is False:
                log.error(msg)
                raise Exception(msg)

            kwargs.update(params=required_params)
            log.info("[check_params] {0} required params: {1}".format(func.__name__, required_params))
            return func(*args, **kwargs)

        return inner_wrapper
    return wrapper
