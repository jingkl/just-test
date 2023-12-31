import requests
import urllib3

from commons.common_func import hide_dict_value
from commons.common_type import HttpStatusCode, LogLevel
from utils.util_log import log


TIMEOUT = 60
KEYWORDS = ["Authorization", "Email", "Password", "Token"]


def request_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                res = func(*args, **kwargs)
                return res
            except requests.exceptions.ConnectTimeout:
                raise Exception("[request_catch] CONNECTION TIMEOUT")
            except requests.exceptions.ConnectionError:
                raise Exception("[request_catch] CONNECTION ERROR")
            except urllib3.exceptions.ProtocolError:
                raise Exception("[request_catch] CONNECTION ERROR")
            except Exception as e:
                raise Exception(e)
        return inner_wrapper
    return wrapper


class Request:
    Log_Level = LogLevel.DEBUG

    def __init__(self, header: dict = None):
        self.header = header or {"Content-type": "application/json"}

    def headers_update(self, headers):
        if headers is None:
            return self.header
        elif isinstance(headers, dict):
            headers.update(self.header)
        return headers

    @request_catch()
    def get(self, url, headers=None,  timeout=TIMEOUT, verify=False, log_level=Log_Level):
        headers = self.headers_update(headers)
        log.customize(log_level)(self.parser_request("GET", url, headers))
        response = requests.get(url, headers=headers, timeout=timeout, verify=verify)
        log.customize(log_level)(self.parser_response("GET", response))
        return response.json()

    @request_catch()
    def post(self, url, body=None, headers=None, timeout=TIMEOUT, verify=False, log_level=Log_Level):
        headers = self.headers_update(headers)
        log.customize(log_level)(self.parser_request("POST", url, headers, body))
        response = requests.post(url, json=body, headers=headers, timeout=timeout, verify=verify)
        log.customize(log_level)(self.parser_response("POST", response))
        return response.json()

    @request_catch()
    def delete(self, url, body=None, headers=None, timeout=TIMEOUT, verify=False, log_level=Log_Level):
        headers = self.headers_update(headers)
        log.customize(log_level)(self.parser_request("DELETE", url, headers, body))
        response = requests.delete(url, json=body, headers=headers, timeout=timeout, verify=verify)
        log.customize(log_level)(self.parser_response("DELETE", response))
        return response.json()

    @request_catch()
    def patch(self, url, body=None, headers=None, timeout=TIMEOUT, verify=False, log_level=Log_Level):
        headers = self.headers_update(headers)
        log.customize(log_level)(self.parser_request("PATCH", url, headers, body))
        response = requests.patch(url, json=body, headers=headers, timeout=timeout, verify=verify)
        log.customize(log_level)(self.parser_response("PATCH", response))
        return response.json()

    @staticmethod
    def parser_request(request_type, url, headers, body=None):
        h = hide_dict_value(headers, KEYWORDS)
        msg = "[Request] {0} request, url:{1}, headers:{2}".format(request_type, url, h)
        if body is not None:
            b = hide_dict_value(body, KEYWORDS)
            msg += ", body:{0}".format(b)
        return msg

    @staticmethod
    def parser_response(request_type, response):
        if response.status_code == HttpStatusCode.OK:
            r = hide_dict_value(response.json(), KEYWORDS)
            return "[Request] {0} response:{1}".format(request_type, r)
        msg = "[Request] {0} response, status_code:{1}, reason:{2}"
        raise Exception(msg.format(request_type, response.status_code, response.reason))
