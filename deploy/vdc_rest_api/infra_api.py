from deploy.vdc_rest_api.request_handler import Request


class InfraApi:

    def __init__(self, host: str, token: str, namespace="ns"):
        self.host = host
        self.namespace = namespace
        self.req = Request()

        self.headers = {
            "Authorization": "Bearer " + token
        }

    def get_milvus(self, instance_id="", ns=""):
        ns = self.namespace if ns == "" else ns
        url = self.host + "/api/v1/cloud/namespace/{0}/milvus/{1}".format(ns, instance_id)
        return self.req.get(url=url, headers=self.headers)

    def upgrade_milvus(self, instance_id="", body=None, ns=""):
        ns = self.namespace if ns == "" else ns
        url = self.host + "/api/v1/cloud/namespace/{0}/milvus/{1}".format(ns, instance_id)
        return self.req.post(url=url, body=body, headers=self.headers)

    def pre_apply(self, instance_id="", body=None, ns=""):
        ns = self.namespace if ns == "" else ns
        url = self.host + "/api/v1/cloud/namespace/{0}/milvus/{1}/pre-apply".format(ns, instance_id)
        return self.req.post(url=url, body=body, headers=self.headers)
