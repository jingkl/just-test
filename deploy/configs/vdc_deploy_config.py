from utils.util_log import log

from deploy.commons.common_params import CLUSTER, STANDALONE
from deploy.commons.common_func import get_class_mode, update_dict_value


class VDCDeployConfig:
    def __init__(self, cluster=True, deploy_class=""):
        self.deploy_mode = CLUSTER if cluster is True else STANDALONE
        self.class_mode, self.deploy_mode = get_class_mode(deploy_mode=self.deploy_mode, deploy_class=deploy_class)
        self.resource_spec = {}

    def set_image(self, image_tag: str):
        self.resource_spec = update_dict_value({"imageTag": image_tag}, self.resource_spec)

    def set_pod_resource(self, resource: dict):
        self.resource_spec = update_dict_value(resource, self.resource_spec)
