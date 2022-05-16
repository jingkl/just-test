from kubernetes import config, dynamic
from kubernetes.client import api_client

from utils.util_catch import func_catch
from utils.util_log import log


class DynamicClient:
    """
    - Creation of a custom resource definition (CRD) using dynamic-client
    - List, create, patch (update), delete the custom resources
    """

    def __init__(self, kubeconfig=None, namespace=None, api_version='milvus.io/v1alpha1', kind='MilvusCluster'):
        self.kubeconfig = kubeconfig  # kubeconfig of load_kube_config is None if passed no param
        self.namespace = namespace

        self.client = self.init_client()
        self.api = self.fetch_server_crd(api_version, kind)

    def init_client(self):
        res, result = self.create_dynamic_client(result_check=True)
        if result:
            return res
        else:
            msg = "[OperatorClient] create dynamic client failed: {}".format(res)
            log.error(msg)
            return None

    @func_catch()
    def create_dynamic_client(self):
        c = dynamic.DynamicClient(api_client.ApiClient(configuration=config.load_kube_config(self.kubeconfig)))
        if type(c).__name__ == "DynamicClient":
            return c
        else:
            msg = "[OperatorClient] create dynamic client failed: {}".format(c)
            log.error(msg)
            raise Exception(msg)

    @func_catch()
    def fetch_server_crd(self, api_version, kind):
        """ Fetching the CRD api """
        if self.client is not None:
            c = self.client.resources.get(api_version=api_version, kind=kind)
            if type(c).__name__ == "Resource":
                return c
            else:
                msg = "[OperatorClient] fetch server crd failed: {}".format(c)
                log.error(msg)
                return None
        else:
            msg = "[OperatorClient] client init failed, please check."
            log.error(msg)
            return None

    @staticmethod
    def check_crd_api(crd_api):
        if crd_api is not None:
            return True
        else:
            msg = "[OperatorClient] crd_api init failed, please check."
            log.error(msg)
            return False

    @staticmethod
    def result_to_dict(_res):
        if type(_res).__name__ == "ResourceInstance":
            return _res.to_dict()
        else:
            return _res

    @func_catch()
    def create(self, body, namespace=None):
        namespace = namespace or self.namespace
        return self.api.create(body=body, namespace=namespace) if self.check_crd_api(self.api) else False

    @func_catch()
    def get(self, namespace=None, label_selector=None):
        """
        :param namespace: str
        :param label_selector: label_selector="release=<release_name>"
        :return: ResourceInstance
        """
        namespace = namespace or self.namespace
        return self.api.get(namespace=namespace, label_selector=label_selector) if self.check_crd_api(self.api) else False

    @func_catch()
    def patch(self, body, namespace=None, content_type="application/merge-patch+json"):
        namespace = namespace or self.namespace
        return self.api.patch(body=body, namespace=namespace, content_type=content_type) if self.check_crd_api(
            self.api) else False

    @func_catch()
    def delete(self, name, namespace=None):
        namespace = namespace or self.namespace
        return self.api.delete(name=name, namespace=namespace) if self.check_crd_api(self.api) else False

    @func_catch()
    def watch(self, namespace=None, timeout=5):
        """
        todo: update func
        :param namespace: The namespace to query
        :param timeout: The amount of time in seconds to wait before terminating the stream
        :return: Event object with these keys:
                   'type': The type of event such as "ADDED", "DELETED", etc.
                   'raw_object': a dict representing the watched object.
                   'object': A ResourceInstance wrapping raw_object.
        """
        namespace = namespace or self.namespace
        return self.api.watch(namespace=namespace, timeout=timeout) if self.check_crd_api(self.api) else False
