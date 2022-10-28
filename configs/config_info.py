from commons.common_func import read_json_file
from commons.common_params import EnvVariable


class ConfigInfo:

    def __init__(self, home_dir=''):
        self.home_dir = "/test/fouram/" if home_dir == "" else home_dir
        self.log_dir = self.home_dir + "log/"
        self.config_dir = self.home_dir + "config/"
        self.config_file = self.config_dir + "config.json"
        self.vdc_config_file = self.config_dir + "vdc_config.json"

        self.influx_db_params = {}
        self.influx_db_v1_params = {}
        self.mongo_db_servers = {}
        self.parser_config(self.config_file)

        self.sit = {}
        self.uat = {}
        self.vdc_global_params = {}
        self.parser_vdc_config(self.vdc_config_file)

    def parser_config(self, config_file):
        config_dict = read_json_file(config_file, out_put=False)
        for k, v in config_dict.items():
            if str(k).startswith("influx_db_v2"):
                self.influx_db_params.update({k: v})

            elif str(k).startswith("influx_db_v1"):
                self.influx_db_v1_params.update({k: v})

            elif str(k).startswith("mongo_db"):
                self.mongo_db_servers.update({k: v})

    def parser_vdc_config(self, config_file):
        config_dict = read_json_file(config_file, out_put=False)

        if "SIT" in config_dict:
            self.sit = config_dict["SIT"]
            for i in ["user_id", "rm_host", "cloud_service_host", "infra_host", "region_id", "infra_token"]:
                assert i in self.sit

        if "UAT" in config_dict:
            self.uat = config_dict["UAT"]
            for i in ["user_id", "rm_host", "cloud_service_host", "infra_host", "region_id", "infra_token"]:
                assert i in self.sit
            for j in ["host", "user", "password"]:
                assert j in self.uat["mysql"]

        if "global" in config_dict:
            self.vdc_global_params = config_dict["global"]
            for i in ["email", "password"]:
                assert i in self.vdc_global_params


config_info = ConfigInfo(EnvVariable.WORK_DIR)

