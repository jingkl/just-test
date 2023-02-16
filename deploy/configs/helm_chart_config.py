from deploy.commons.common_func import get_latest_tag, get_image_tag, update_dict_value
from deploy.commons.common_params import IDC_NAS_URL, dataNode, queryNode, indexNode, all_pods, minio, etcd, pulsar, \
    kafka, DefaultRepository

from deploy.configs.base_config import BaseConfig


class HelmConfig(BaseConfig):

    def __init__(self, cluster=True, escape=True, **kwargs):
        super().__init__()
        self.escape = escape

        # deploy mode
        self.cluster = cluster
        self._deploy_mode = self.set_deploy_mode(self.cluster)

        # healthy check disabled
        self.healthy_check = self.healthy_check_dict

        # minio local-path and metrics
        self.storage_local_path = {minio: self.storage_dict}

        # etcd local-path and metrics
        self.etcd_local_path = {etcd: self.etcd_dict,
                                "metrics": {"serviceMonitor": {"enabled": True}}}

        # etcd node selector
        self.etcd_node_selector = self.set_etcd_node_selector(self.escape)

        # pulsar local-path
        self.pulsar_local_path = {pulsar: self.pulsar_dict}

        # kafka local-path
        self.kafka_local_path = {kafka: self.kafka_dict}

        # standalone local-path
        self.standalone_local_path = {"standalone": self.standalone_dict}

        # add extra volumes
        self.extra_volumes = {"extraVolumes": [{'name': 'test',
                                                'flexVolume': {'driver': "fstab/cifs",
                                                               'fsType': "cifs",
                                                               'secretRef': {'name': "cifs-test-secret"},
                                                               'options': {'networkPath': IDC_NAS_URL,
                                                                           'mountOptions': "vers=1.0"}}}],
                              "extraVolumeMounts": [{'name': 'test',
                                                     'mountPath': '/test'}]}

        # base config
        self.base_config_dict = self.config_merge([self._deploy_mode])

    @staticmethod
    def set_deploy_mode(cluster):
        return {"cluster": {"enabled": False},
                "etcd": {"replicaCount": 1},
                "minio": {"mode": "standalone"},
                "pulsar": {"enabled": False}} if cluster is False else {"cluster": {"enabled": True}}

    def set_etcd_node_selector(self, escape=None):
        escape = escape if escape is not None else self.escape
        if escape:
            node_selector = {}
            # node_selector = {"etcd": {"nodeSelector": {"\"node-role\\.kubernetes\\.io/etcd\"": "etcd"}}}
        else:
            node_selector = self.etcd_node_selector_dict
        return node_selector

    @staticmethod
    def get_server_tag(tag=None, prefix="master"):
        if tag is None or not isinstance(tag, str):
            tag = get_image_tag()
            if prefix not in tag or tag == prefix + "-latest":
                tag = get_latest_tag()
        return {"image": {"all": {"tag": tag}}}

    @staticmethod
    def set_image_repository(repository=DefaultRepository):
        if repository is not None:
            return {"image": {"all": {"repository": repository}}}
        else:
            return {}

    # common funcs
    def set_image(self, tag=None, repository=DefaultRepository, prefix="master"):
        tag_dict = self.get_server_tag(tag, prefix=prefix)

        repository_dict = self.set_image_repository(repository)
        return update_dict_value(tag_dict, repository_dict)

    @staticmethod
    def set_mq(pulsar=True, kafka=False):
        return {"pulsar": {"enabled": pulsar},
                "kafka": {"enabled": kafka}}

    def set_nodes_resource(self, cpu=None, mem=None):
        # cluster_resource = self.gen_nodes_resource(cpu, mem)

        if self.cluster:
            return {queryNode: {"resources": self.gen_nodes_resource(cpu, mem)},
                    indexNode: {"resources": self.gen_nodes_resource(cpu, mem)},
                    dataNode: {"resources": self.gen_nodes_resource(cpu, mem)}}
        else:
            return {"standalone": {"resources": self.gen_nodes_resource(cpu, mem)}}

    @staticmethod
    def set_replicas(**kwargs):
        """
        only support setting [rootCoord, dataCoord, queryCoord, dataNode, queryNode, indexNode, proxy]
        e.g.: set_replicas(dataNode=1, queryNode=5, indexNode=3)
        """
        keys = kwargs.keys()
        set_dict = {}

        for key in keys:
            if key in all_pods and str(kwargs[key]).isdigit():
                set_dict = update_dict_value({key: {"replicas": int(kwargs[key])}}, set_dict)
        return set_dict
