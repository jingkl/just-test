from deploy.commons.request_catch import request_catch, RequestResponseParser

from commons.request_handler import Request
from commons.common_type import LogLevel


class CloudServiceTestApi:
    Log_Level = LogLevel.DEBUG

    def __init__(self, host: str):
        self.host = host
        self.req = Request()

        self.headers = {}

    @request_catch()
    def get_root_pwd(self, instance_id: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/test/getRootPwd?instanceId={0}".format(instance_id)
        return self.req.get(url=url, headers=self.headers, log_level=log_level)
