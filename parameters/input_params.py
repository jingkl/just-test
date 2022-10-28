from dataclasses import dataclass
from typing import Optional, Union

from pymilvus import DefaultConfig
from commons.common_type import DefaultParams as dp
from deploy.commons.common_params import Helm, STANDALONE, DefaultRepository


class ParamInfo:
    """ global params """
    def __init__(self):
        self.test_status = True
        self.client_version = ""
        self.param_host = DefaultConfig.DEFAULT_HOST
        self.param_port = DefaultConfig.DEFAULT_PORT
        self.param_handler = ""
        self.param_secure = False
        self.param_user = "root"
        self.param_password = "Milvus123"
        self.param_replica_num = dp.default_replica_num
        self.milvus_tag = None
        self.milvus_tag_prefix = ""
        self.tag_repository = DefaultRepository
        self.update_helm_file = False
        self.deploy_skip = False
        self.deploy_retain = False
        self.client_test_skip = False
        self.release_name_prefix = ""
        self.sync_report = False
        self.async_report = False

    def prepare_param_info(self, client_version, host, port, handler="", secure="", replica_num=1, milvus_tag=None,
                           tag_repository=None, deploy_skip=False, deploy_retain=False, milvus_tag_prefix="",
                           update_helm_file=False, client_test_skip=False, release_name_prefix="",
                           sync_report=False, async_report=False):
        self.client_version = client_version
        self.param_host = host
        self.param_port = port
        self.param_handler = handler
        self.param_secure = secure
        self.param_replica_num = replica_num
        self.milvus_tag = milvus_tag or self.milvus_tag
        self.milvus_tag_prefix = milvus_tag_prefix or self.milvus_tag_prefix
        self.tag_repository = tag_repository or self.tag_repository
        self.update_helm_file = update_helm_file or self.update_helm_file
        self.deploy_skip = deploy_skip or self.deploy_skip
        self.deploy_retain = deploy_retain or self.deploy_retain
        self.client_test_skip = client_test_skip or self.client_test_skip
        self.release_name_prefix = release_name_prefix
        self.sync_report = sync_report
        self.async_report = async_report

    def to_dict(self):
        return vars(self)


param_info = ParamInfo()


# define input parameters
@dataclass
class InputParamsBase:
    deploy_tool: Optional[str] = Helm
    deploy_mode: Optional[str] = ""
    # deploy_config: Optional[str] = str
    deploy_config: Union[str, dict] = str
    # case_params: Optional[str] = str
    case_params: Union[str, dict] = str
    case_skip_prepare: Optional[bool] = False
    case_skip_prepare_clean: Optional[bool] = False
    case_skip_build_index: Optional[bool] = False
    case_skip_clean_collection: Optional[bool] = False
