from parameters.input_params import param_info


class ServerMetric:

    def __init__(self, deploy_tool: str = "", deploy_mode: str = "", config_name: str = "", config: dict = {},
                 host: str = ""):
        self.deploy_tool = deploy_tool
        self.deploy_mode = deploy_mode
        self.config_name = config_name
        self.config = config
        self.host = host or param_info.param_host

    def update(self, deploy_tool: str = "", deploy_mode: str = "", config_name: str = "", config: dict = {},
               host: str = "", **kwargs):
        self.deploy_tool = deploy_tool or self.deploy_tool
        self.deploy_mode = deploy_mode or self.deploy_mode
        self.config_name = config_name or self.config_name
        self.config = config or self.config
        self.host = host or param_info.param_host

        # update extra configs
        for k, v in kwargs.items():
            setattr(self, k, v)

    def clear_property(self):
        self.deploy_tool = ""
        self.deploy_mode = ""
        self.config_name = ""
        self.config = {}
        self.host = ""

    def add_server_metrics(self, **kwargs):
        """
        add extra configs
        """
        for k, v in kwargs.items():
            setattr(self, k, v)

    def del_server_metrics(self, *args):
        """
        del extra configs
        """
        for k in args:
            delattr(self, k)
