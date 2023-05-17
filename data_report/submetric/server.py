from parameters.input_params import param_info


class ServerMetric:

    def __init__(self, deploy_tool: str = "", deploy_mode: str = "", config_name: str = "", config: dict = {},
                 host: str = "", port: str = "", uri: str = ""):
        self.deploy_tool = deploy_tool
        self.deploy_mode = deploy_mode
        self.config_name = config_name
        self.config = config
        self.host = host or param_info.param_host
        self.port = port or param_info.param_port
        self.uri = uri or param_info.param_uri

    def update(self, deploy_tool: str = "", deploy_mode: str = "", config_name: str = "", config: dict = {},
               host: str = "", port: str = "", uri: str = ""):
        self.deploy_tool = deploy_tool or self.deploy_tool
        self.deploy_mode = deploy_mode or self.deploy_mode
        self.config_name = config_name or self.config_name
        self.config = config or self.config
        self.host = host or param_info.param_host
        self.port = port or param_info.param_port
        self.uri = uri or param_info.param_uri

    def clear_property(self):
        self.deploy_tool = ""
        self.deploy_mode = ""
        self.config_name = ""
        self.config = {}
        self.host = ""
        self.port = ""
        self.uri = ""
