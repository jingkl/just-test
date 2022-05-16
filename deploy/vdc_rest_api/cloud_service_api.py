import uuid

from configs.config_info import config_info
from deploy.vdc_rest_api.request_handler import Request


class CloudServiceApi:

    def __init__(self, host: str, user_id: str):
        self.host = host
        self.UserId = user_id
        self.req = Request()

        self.TOKEN = self.get_token()
        self.headers = {
            "Authorization": "Bearer " + self.TOKEN
        }

    def create(self):
        pass

    def delete(self, instance_id: str):
        url = self.host + "/cloud/v1/instance/delete"
        body = {
            "instanceId": instance_id,
        }
        return self.req.post(url=url, body=body, headers=self.headers)

    def describe(self, instance_id: str):
        url = self.host + "/cloud/v1/instance/describe?InstanceId={0}".format(instance_id)
        return self.req.get(url=url, headers=self.headers)

    def list(self, current_page=1, page_size=50, search_key=None):
        url = self.host + "/cloud/v1/instance/list?CurrentPage={0}&PageSize={1}".format(current_page, page_size)

        if search_key is not None:
            url += "&SearchKey={0}".format(search_key)

        return self.req.get(url=url, headers=self.headers)

    def modify(self):
        pass

    def resume(self, instance_id: str):
        url = self.host + "/cloud/v1/instance/resume"
        body = {
            "instanceId": instance_id,
        }
        return self.req.post(url=url, body=body, headers=self.headers)

    def stop(self, instance_id: str):
        url = self.host + "/cloud/v1/instance/stop"
        body = {
            "instanceId": instance_id,
        }
        return self.req.post(url=url, body=body, headers=self.headers)

    def oos_login(self, email: str, password: str):
        url = self.host + "/account/v1/account/login"
        headers = {
            "RequestId": str(uuid.uuid1()),
        }
        body = {
            "Email": email,
            "Password": password,
        }
        return self.req.post(url=url, body=body, headers=headers)

    def get_token(self):
        if "email" not in config_info.vdc_global_params and "password" not in config_info.vdc_global_params:
            raise Exception("[CloudServiceApi] Can not get email and password from vdc_config_file, please check.")

        res = self.oos_login(email=config_info.vdc_global_params["email"],
                             password=config_info.vdc_global_params["password"])
        if "Data" in res and "Token" in res["Data"]:
            return res["Data"]["Token"]
        raise Exception("[CloudServiceApi] Can not get token, please check.")
