import time

from configs.config_info import config_info
from deploy.commons.common_params import InstanceStatus, ClassID
from deploy.commons.common_func import update_dict_value, add_resource
from deploy.commons.status_code import RMErrorCode
from deploy.vdc_rest_api import CloudRMApi, InfraApi, CloudServiceApi
from deploy.client.base.base_client import BaseClient
from utils.util_cmd import CmdExe
from utils.util_log import log


class VDCDeployClient(BaseClient):

    def __init__(self, instance_name="", env="UAT"):
        super().__init__()

        params = config_info.uat if env == "UAT" else config_info.sit

        user_id = params["user_id"]
        rm_host = params["rm_host"]
        cloud_service_host = params["cloud_service_host"]
        infra_host = params["infra_host"]
        region_id = params["region_id"]
        infra_token = params["infra_token"]
        ns = "milvus-" + user_id.lower()

        self.ns = ns
        self.env = env.lower()
        self.instance_name = instance_name
        self.instance_id = ""
        self.end_point = ""
        self.cloud_rm_api = CloudRMApi(user_id=user_id, host=rm_host, region_id=region_id)
        self.cloud_service_api = CloudServiceApi(user_id=user_id, host=cloud_service_host)
        self.infra_api = InfraApi(host=infra_host, token=infra_token, namespace=ns)

    def install(self, instance_name="", image_tag="", class_id=ClassID.Small, timeout=600):
        instance_name = instance_name if instance_name != "" else self.instance_name

        assert not self.check_instance_exist(instance_name=instance_name)

        res = self.cloud_rm_api.create(class_id=class_id, instance_name=instance_name, db_version=image_tag)
        if "Code" in res and res["Code"] == RMErrorCode.RequestBusy:
            start_time = time.time()
            while time.time() < start_time + timeout:
                time.sleep(1)
                log.info("[VDCDeployClient] Recreate instance {0}".format(instance_name))

                res = self.cloud_rm_api.create(class_id=class_id, instance_name=instance_name, db_version=image_tag)
                if "Code" in res and res["Code"] == RMErrorCode.RequestBusy:
                    continue
                else:
                    break

        if "Data" in res and res["Data"] is not None and "InstanceId" in res["Data"]:
            self.instance_id = res["Data"]["InstanceId"]
        else:
            raise Exception("[VDCDeployClient] Can not create instance {0}, please check.".format(instance_name))

        assert self.check_server_status(instance_name=instance_name, instance_id=self.instance_id)

    def upgrade(self, resource, instance_name=""):
        instance_id = self.get_instance_id(instance_name=instance_name)
        res = self.infra_api.get_milvus(instance_id=instance_id)

        data = {}
        cluster = ""
        if "data" in res:
            data = res["data"]
            if "cluster" in data:
                cluster = data["cluster"]

        assert data != {} and cluster != ""

        s_dict = {"spec": resource}
        t_dict = update_dict_value(s_dict, data)
        log.debug("[VDCDeployClient] Update resource:{0} to target:{1}".format(s_dict, t_dict))

        res = self.infra_api.pre_apply(instance_id=instance_id,
                                       body={"cluster": cluster, "cpu": add_resource(t_dict["spec"], "cpu"),
                                             "memory": add_resource(t_dict["spec"], "memory")})
        self.check_api_result(res)

        res_upgrade = self.infra_api.upgrade_milvus(instance_id=instance_id, body=t_dict)
        self.check_api_result(res_upgrade)

        res_milvus = self.infra_api.get_milvus(instance_id=instance_id)
        self.check_api_result(res_milvus)

    def uninstall(self, instance_name=""):
        assert self.check_instance_exist(instance_name=instance_name)
        instance_id = self.get_instance_id(instance_name=instance_name)
        # display server values and pods
        self.get_all_values(instance_id=instance_id)
        self.get_pods(instance_id=instance_id)
        # delete server and check status
        res = self.cloud_service_api.delete(instance_id=self.instance_id)
        self.check_api_result(res)
        assert self.check_server_delete(instance_name=instance_name)

    def delete_pvc(self, *args, **kwargs):
        pass

    def endpoint(self, instance_name=""):
        instance_name = instance_name if instance_name != "" else self.instance_name
        res = self.cloud_service_api.list()
        if "Data" in res and "List" in res["Data"]:
            instances = res["Data"]["List"]
            for i in instances:
                if i["InstanceName"] == instance_name and "ConnectAddress" in i:
                    self.end_point = i["ConnectAddress"]
                    port = str(self.end_point).split(':')[-1]
                    host = str(self.end_point).rstrip(':' + port)
                    log.info("[VDCDeployClient] Instance:{0}, host:{1}, port:{2}".format(instance_name, host, port))
                    return host, port
        raise Exception("[VDCDeployClient] Can not get endpoint of instance:{0}".format(instance_name))

    def get_pvc(self, *args, **kwargs):
        pass

    def get_pods(self, instance_id=""):
        instance_id = instance_id if instance_id != "" else self.instance_id
        _cmd = "kubectl get pod -o wide -n {0} | grep -E \"NAME|{1}\"".format(self.ns, instance_id)

        log.info("[VDCDeployClient] {0}".format(_cmd))
        res_cmd = CmdExe(_cmd).run_cmd()
        log.info("[VDCDeployClient] Result of execution: \n {0}".format(res_cmd))

    def get_all_values(self, instance_id=""):
        instance_id = instance_id if instance_id != "" else self.instance_id
        _cmd = "kubectl get mi {0} -o yaml -n {1}".format(instance_id, self.ns)

        log.info("[VDCDeployClient] {0}".format(_cmd))
        res_cmd = CmdExe(_cmd).run_cmd()
        log.info("[VDCDeployClient] Result of execution: \n {0}".format(res_cmd))

    @staticmethod
    def check_api_result(res, default_check_code=0):
        if "code" in res:
            assert res["code"] == default_check_code
        elif "Code" in res:
            assert res["Code"] == default_check_code
        else:
            raise Exception("[VDCDeployClient] The API response does not contain the 'code' field, please check.")

    def get_instance_id(self, instance_name=""):
        instance_name = instance_name if instance_name != "" else self.instance_name

        res = self.cloud_service_api.list()
        if "Data" in res and "List" in res["Data"]:
            instances = res["Data"]["List"]
            for i in instances:
                if i["InstanceName"] == instance_name:
                    self.instance_id = i["InstanceId"]
                    return self.instance_id
        raise Exception("[VDCDeployClient] Can not get instance id for {0}".format(instance_name))

    def check_server_status(self, instance_name="", instance_id="", timeout=1800, status=InstanceStatus.RUNNING,
                            interval_time=60):
        instance_name = instance_name if instance_name != "" else self.instance_name
        instance_id = instance_id if instance_id != "" else self.get_instance_id(instance_name=instance_name)

        start_time = time.time()
        while time.time() < start_time + timeout:
            time.sleep(interval_time)
            res = self.cloud_service_api.describe(instance_id=instance_id)
            if "Data" in res and res["Data"] is not None and "Status" in res["Data"]:
                if res["Data"]["Status"] == status:
                    log.info("[VDCDeployClient] Instance:{0}, Status:{1}".format(instance_name,
                                                                                 res["Data"]["StatusName"]))
                    return True
        msg = "[VDCDeployClient] Instance:{0}, Status not RUNNING.".format(instance_name)
        log.error(msg)
        raise Exception(msg)

    def infra_check_server_status(self, instance_name="", timeout=1800, interval_time=60):
        instance_name = instance_name if instance_name != "" else self.instance_name
        instance_id = self.get_instance_id(instance_name=instance_name)

        status = True
        start_time = time.time()
        time.sleep(interval_time)

        while time.time() < start_time + timeout:
            _res = self.infra_api.get_milvus(instance_id=instance_id)
            res = _res.json()
            if "data" in res and res["data"] is not None and "status" in res["data"]:
                if "ready" in res["data"]["status"] and "isReady" in res["data"]["status"]["ready"]:
                    if res["data"]["status"]["ready"]["isReady"] is status:
                        log.info("[VDCDeployClient] Instance:{0}, Status:{1}".format(instance_name,
                                                                                     res["data"]["status"]["ready"][
                                                                                         "isReady"]))
                        return True
            time.sleep(interval_time)
        msg = "[VDCDeployClient] Instance:{0}, Status not RUNNING.".format(instance_name)
        log.error(msg)
        raise Exception(msg)

    def check_server_delete(self, instance_name="", timeout=1800, interval_time=20):
        instance_name = instance_name if instance_name != "" else self.instance_name
        start_time = time.time()
        while time.time() < start_time + timeout:
            time.sleep(interval_time)
            res = self.cloud_service_api.list(search_key="")
            if "Data" in res and "List" in res["Data"]:
                instances = res["Data"]["List"]
                instances_names = []
                for i in instances:
                    instances_names.append(i["InstanceName"])
                if instance_name not in instances_names:
                    log.info("[VDCDeployClient] Instance:{0} has been deleted.".format(instance_name))
                    return True
        log.info("[VDCDeployClient] Instance:{0} has not been deleted.".format(instance_name))
        return False

    def check_instance_exist(self, instance_name=""):
        instance_name = instance_name if instance_name != "" else self.instance_name

        res = self.cloud_service_api.list(search_key="")
        if "Data" in res and "List" in res["Data"]:
            instances = res["Data"]["List"]
            instances_names = []
            for i in instances:
                instances_names.append(i["InstanceName"])
            if instance_name in instances_names:
                log.info("[VDCDeployClient] Instance:{0} has been created.".format(instance_name))
                return True
        log.info("[VDCDeployClient] Instance:{0} has not been created.".format(instance_name))
        return False

