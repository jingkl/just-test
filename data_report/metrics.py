
from data_report.submetric import ServerMetric, ClientMetric, ResultMetric
from utils.util_log import log


class ReportMetrics(object):

    __slots__ = ("server", "client", "result")

    def __init__(self, server: ServerMetric = None, client: ClientMetric = None, result: ResultMetric = None):
        self.server = server or ServerMetric()
        self.client = client or ClientMetric()
        self.result = result or ResultMetric()

    def reset(self):
        self.server.clear_property()
        self.client.clear_property()
        self.result.clear_property()

    def update_server(self, deploy_tool: str = "", deploy_mode: str = "", config_name: str = "", config: dict = {},
                      host: str = ""):
        self.server.update(deploy_tool, deploy_mode, config_name, config, host)

    def update_client(self, test_case_type: str = "", test_case_name: str = "", test_case_params: dict = {}):
        self.client.update(test_case_type, test_case_name, test_case_params)

    def update_result(self, test_result: dict = {}):
        self.result.update(test_result)

    def to_dict(self):
        return {
            "server": vars(self.server),
            "client": vars(self.client),
            "result": vars(self.result)
        }

    def __getattr__(self, name):
        log.error("[ReportMetrics] The property:{0} does not exist, please check".format(name))
        return {}

    def __getattribute__(self, name):
        return super(ReportMetrics, self).__getattribute__(name)

    def __setattr__(self, name, value):
        return super(ReportMetrics, self).__setattr__(name, value)

    def __getitem__(self, item):
        return self.__dict__[item]


Report_Metric_Object = ReportMetrics()

