from typing import Optional, Union, List

from deploy.configs import get_config_obj
from deploy.commons.common_params import (
    CLUSTER, STANDALONE, Helm, Operator, VDC, DefaultRepository, pulsar, kafka,
    ClassID, ClassIDMemCluster, ClassIDMemStandalone, ClassIDDiskCluster, ClassIDDiskStandalone)
from deploy.commons.common_func import server_resource_check, gen_server_config_name, update_dict_value

from parameters.input_params import param_info
from commons.common_params import EnvVariable
from utils.util_log import log


class NodeResource:
    def __init__(self, nodes: list, replicas: Union[int] = 1, cpu: Union[int, float] = None,
                 mem: Union[int, float] = None, custom: Union[dict] = {}):
        self.nodes = nodes
        self.replicas = replicas
        self.cpu = cpu
        self.mem = mem
        self.custom = custom

    def custom_resource(self, limits_cpu=None, requests_cpu=None, limits_mem=None, requests_mem=None):
        self.custom = {"limits_cpu": limits_cpu, "requests_cpu": requests_cpu,
                       "limits_mem": limits_mem, "requests_mem": requests_mem}
        return self


class SetDependence:
    def __init__(self, mq_type: Union[pulsar, kafka] = pulsar, disk_size: Union[int, float] = None):
        self.mq_type = mq_type
        self.disk_size = disk_size


class DefaultConfigs:

    def __init__(self, deploy_tool=Helm, deploy_mode=CLUSTER, **kwargs):
        """
        :param deploy_tool: Helm or Operator or VDC
        :param deploy_mode: cluster or standalone or class_id's name
        :param kwargs: escape for helm, api_version for operator
        """
        self.deploy_tool = str(deploy_tool).lower()
        self.deploy_mode = deploy_mode

        self.cluster = self.check_cluster(self.deploy_mode)
        self.api_version = kwargs.get("api_version", "milvus.io/v1beta1")
        self.escape = kwargs.get("escape", True)
        self.obj = get_config_obj(self.deploy_tool, cluster=self.cluster, api_version=self.api_version,
                                  escape=self.escape, deploy_mode=self.deploy_mode)

        self._default_config = [self.obj.base_config_dict, self.obj.storage_local_path, self.obj.etcd_local_path,
                                self.obj.etcd_node_selector, self.obj.log_level]

        self.cluster_default_configs = [self.obj.pulsar_local_path, self.obj.kafka_local_path] + self._default_config

        self.standalone_default_configs = [self.obj.standalone_local_path] + self._default_config

        self.vdc_default_configs = []

        self.default_configs = {
            CLUSTER: self.cluster_default_configs,
            STANDALONE: self.standalone_default_configs,
            VDC: self.vdc_default_configs
        }

    def check_deploy_mode(self, deploy_mode: str):
        if self.deploy_tool in [Helm, Operator]:
            if deploy_mode in [CLUSTER, STANDALONE]:
                return deploy_mode
            log.warning("[DefaultConfigs] Deploy tool:%s not support deploy mode:%s, using deploy mode:%s" % (
                self.deploy_tool, deploy_mode, STANDALONE))
            return STANDALONE

        if self.deploy_tool == VDC:
            if hasattr(ClassID, deploy_mode):
                return deploy_mode
            log.warning("[DefaultConfigs] Deploy tool:%s not support deploy mode:%s, using deploy mode:%s" % (
                self.deploy_tool, deploy_mode, ClassID.class1cu))
            return ClassID.class1cu

        raise ValueError("[DefaultConfigs] Not support deploy tool:%s, please check." % self.deploy_tool)

    @staticmethod
    def check_cluster(deploy_mode):
        if deploy_mode == CLUSTER or hasattr(ClassIDMemCluster, deploy_mode) or \
                hasattr(ClassIDDiskCluster, deploy_mode):
            return True
        elif deploy_mode == STANDALONE or hasattr(ClassIDMemStandalone, deploy_mode) or \
                hasattr(ClassIDDiskStandalone, deploy_mode):
            return False
        raise Exception("[DefaultConfigs] Can't parser deploy mode:%s, please check." % deploy_mode)

    def get_default_configs(self, deploy_mode):
        _deploy_type = deploy_mode
        if hasattr(ClassID, deploy_mode):
            _deploy_type = VDC
        if _deploy_type not in self.default_configs.keys():
            raise ValueError("[DefaultConfigs] Not support deploy mode:%s, please check." % deploy_mode)
        return self.default_configs.get(_deploy_type, [{}])

    def set_image(self, tag=None, repository=DefaultRepository, prefix="master-"):
        return self.obj.set_image(tag=tag, repository=repository, prefix=prefix)

    def custom_resource(self, limits_cpu=None, requests_cpu=None, limits_mem=None, requests_mem=None):
        _resource = self.obj.custom_resource(limits_cpu=limits_cpu, requests_cpu=requests_cpu,
                                             limits_mem=limits_mem, requests_mem=requests_mem)
        return _resource

    def set_nodes_resource(self, node_resources: List[NodeResource]):
        totals = []
        for node_resource in node_resources:
            _resource = self.obj.set_nodes_resource(node_resource.cpu, node_resource.mem,
                                                    custom_resource=self.custom_resource(**node_resource.custom),
                                                    nodes=node_resource.nodes)
            _replicas = self.obj.set_replicas(**{n: node_resource.replicas for n in node_resource.nodes})
            _all = self.obj.config_merge([_resource, _replicas])
            totals.append(_all)
        return self.obj.config_merge(totals)

    def set_mq(self, set_dependence: SetDependence):
        return self.obj.set_mq(_pulsar=(set_dependence.mq_type == pulsar), _kafka=(set_dependence.mq_type == kafka))

    def setting_configs(self, node_resources: List[NodeResource], set_dependence: SetDependence):
        _nodes = self.set_nodes_resource(node_resources=node_resources) if node_resources else {}
        _mq = self.set_mq(set_dependence=set_dependence) if set_dependence else {}
        _disk_resource = self.obj.set_custom_config(disk_size=set_dependence.disk_size) if set_dependence else {}
        return self.obj.config_merge([_nodes, _mq, _disk_resource])

    def server_resource(self, cpu=8, mem=16, use_default_config=True, deploy_mode=None, other_configs=[],
                        update_helm_file=False, values_file_path='', **kwargs):
        """
        :param cpu:
            cluster: for all nodes
            standalone: for standalone
        :param mem:
            cluster: for all nodes
            standalone: for standalone
        :param use_default_config: True or False
        :param deploy_mode: cluster or standalone or class_id's name
        :param other_configs: [{}], configuration of custom dictionary format
        :param update_helm_file: bool
        :param values_file_path: str
        :param kwargs: support setting replicas for dataNode, queryNode, indexNode, proxy
        :return: dictionary format configuration, config name, configuration -> dict
        Configure priority: default value -> default setting -> params setting -> other_configs -> cmd arguments
        """
        deploy_mode = deploy_mode or self.deploy_mode

        # update params from outside
        update_helm_file = update_helm_file or param_info.update_helm_file
        values_file_path = values_file_path or EnvVariable.FOURAM_HELM_CHART_PATH + "/values.yaml"

        config_name = gen_server_config_name(cpu=cpu, mem=mem, cluster=self.cluster, deploy_mode=deploy_mode, **kwargs)
        deploy_mode = self.check_deploy_mode(deploy_mode)
        _other_configs = self.obj.config_merge(server_resource_check(other_configs))

        _configs_list = [self.obj.set_nodes_resource(cpu, mem), self.obj.set_replicas(**kwargs),
                         self.obj.base_config_dict]
        if use_default_config:
            _configs_list += self.get_default_configs(deploy_mode=deploy_mode)
        _configs_dict = self.obj.config_merge(_configs_list)

        _configs = update_dict_value(_other_configs, _configs_dict)

        log.debug("[DefaultConfigs] server resource: \n {}".format(_configs))
        return self.config_conversion(_configs, update_helm_file, values_file_path), config_name, _configs

    def config_conversion(self, config, update_helm_file=False, values_file_path=''):
        if self.deploy_tool == Helm:
            if update_helm_file:
                self.obj.update_values_file(values_file_path, config)
                return config
            else:
                return self.obj.config_to_set_params(config)
        elif self.deploy_tool in [Operator, VDC]:
            return config
        log.warning("[DefaultConfigs] Deployment tool may not be supported:{}, please check.".format(self.deploy_tool))
        return config
