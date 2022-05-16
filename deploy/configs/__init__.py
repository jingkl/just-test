from deploy.configs.helm_chart_config import HelmConfig
from deploy.configs.operator_config import OperatorConfig
from deploy.commons.common_params import Helm, Operator
from utils.util_log import log


def get_config_obj(name, **kwargs):
    log.debug("[get_config_obj] Initialize the class object of %s， params: %s" % (name, kwargs))
    return {
        Helm: HelmConfig(**kwargs),
        Operator: OperatorConfig(**kwargs),
    }.get(name)


# __all__ = [HelmConfig, OperatorConfig, get_config_obj]
