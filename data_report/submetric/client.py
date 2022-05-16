from datetime import datetime
import time
import random


from parameters.input_params import param_info


class ClientMetric:

    def __init__(self, test_case_type: str = "", test_case_name: str = "", test_case_params: dict = {},
                 client_version: str = ""):
        self.test_case_type = test_case_type
        self.test_case_name = test_case_name
        self.test_case_params = test_case_params

        self.run_id = int(time.time()) + int(random.randint(1, 9999))
        self.datetime = str(datetime.now())
        self.client_version = client_version or param_info.client_version

    def update(self, test_case_type: str = "", test_case_name: str = "", test_case_params: dict = {}):
        self.test_case_type = test_case_type or self.test_case_type
        self.test_case_name = test_case_name or self.test_case_name
        self.test_case_params = test_case_params or self.test_case_params

        self.client_version = self.client_version or param_info.client_version

    def clear_property(self):
        self.test_case_type = ""
        self.test_case_name = ""
        self.test_case_params = {}

        self.run_id = int(time.time()) + int(random.randint(1, 9999))
        self.datetime = str(datetime.now())
