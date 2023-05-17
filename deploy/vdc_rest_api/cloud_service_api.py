import uuid
import time

from deploy.commons.request_catch import request_catch, RequestResponseParser

from commons.request_handler import Request
from commons.common_type import LogLevel
from configs.config_info import config_info
from utils.util_log import log


class CloudServiceApi:
    Log_Level = LogLevel.DEBUG

    def __init__(self, host: str, user_id: str = config_info.vdc_user.user_id,
                 email=config_info.vdc_user.email, password=config_info.vdc_user.password):
        self.email = email
        self.password = password
        self.host = host
        self.UserId = user_id
        self.req = Request()

        self.current_time = time.perf_counter()
        # self.TOKEN = self.get_token()

    # @property
    # def headers(self):
    #     if self.TOKEN is None or time.perf_counter() - self.current_time >= 1800:
    #         log.info("[CloudServiceApi] Refresh token.")
    #         self.TOKEN = self.get_token()
    #     return {
    #         "Authorization": "Bearer " + self.TOKEN
    #     }

    @property
    def headers(self):
        return {
            "UserId": self.UserId
        }

    @request_catch()
    def create(self, log_level=Log_Level) -> RequestResponseParser:
        pass

    @request_catch()
    def delete(self, instance_id: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/instance/delete"
        body = {
            "instanceId": instance_id,
        }
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    @request_catch()
    def describe(self, instance_id: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/instance/describe?InstanceId={0}".format(instance_id)
        return self.req.get(url=url, headers=self.headers, log_level=log_level)

    @request_catch()
    def list(self, current_page: int = 1, page_size: int = 50, search_key=None,
             log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/instance/list?CurrentPage={0}&PageSize={1}".format(current_page, page_size)

        if search_key is not None:
            url += "&SearchKey={0}".format(search_key)

        return self.req.get(url=url, headers=self.headers, log_level=log_level)

    @request_catch()
    def modify(self, instance_id: str, class_id: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/instance/modify"
        # body = {
        #     "classId": "",
        #     "instanceId": "",
        #     "storeSize": 3145728 # ignore now
        # }
        body = {
            "classId": class_id,
            "instanceId": instance_id
        }
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    @request_catch()
    def resume(self, instance_id: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/instance/resume"
        body = {
            "instanceId": instance_id,
        }
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    @request_catch()
    def stop(self, instance_id: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/instance/stop"
        body = {
            "instanceId": instance_id,
        }
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    @request_catch()
    def oos_login(self, email: str, password: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/account/v1/account/login"
        headers = {
            "RequestId": str(uuid.uuid1()),
        }
        body = {
            "Email": email,
            "Password": password,
        }
        return self.req.post(url=url, body=body, headers=headers, log_level=log_level)

    def get_token(self, log_level=Log_Level):
        self.current_time = time.perf_counter()

        if not self.email or not self.password:
            raise Exception("[CloudServiceApi] Can not get email and password from vdc_config_file, please check.")

        res = self.oos_login(email=self.email, password=self.password, log_level=log_level)
        if "Token" in res.data:
            return res.data["Token"]
        raise Exception("[CloudServiceApi] Can not get token, please check.")
