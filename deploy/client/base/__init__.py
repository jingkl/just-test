from deploy.client.base.cli_client import CliClient
from deploy.client.base.operator_client import OperatorClient
from deploy.commons.common_params import Helm, Operator

from utils.util_log import log


def get_client_obj(name, **kwargs):
    log.debug("[get_client_obj] Initialize the class object of %s， params: %s" % (name, kwargs))
    # return {
    #     Helm: CliClient(**kwargs),
    #     Operator: OperatorClient(**kwargs),
    # }.get(name)
    if name == Helm:
        return CliClient(**kwargs)
    elif name == Operator:
        return OperatorClient(**kwargs)
    else:
        msg = "[get_client_obj] Class object:{0} not support, please check.".format(name)
        log.error(msg)
        raise Exception(msg)
