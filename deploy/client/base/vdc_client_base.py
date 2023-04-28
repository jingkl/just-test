import time
import operator
from datetime import datetime

from deploy.client.base.dynamic_client import DynamicClient
from deploy.vdc_rest_api import CloudRMApi, InfraApi, CloudServiceApi, CloudServiceTestApi
from deploy.commons.status_code import RMErrorCode, InstanceStatus
from deploy.commons.common_params import ClassID, CLASS_RESOURCES_MAP, RMNodeCategory, Pod
from deploy.commons.common_func import (
    update_dict_value, add_resource, get_child_class_id, parser_modify_params, check_multi_keys_exist, get_api_version,
    get_class_key_name)
from deploy.commons.sql_statement import (
    sql_insert_instance_class, sql_insert_cust_instance_node, query_cust_instance_node, delete_cust_instance_node,
    sql_insert_instance_class_oversold)

from commons.common_params import EnvVariable
from commons.common_type import LogLevel
from db_client.client_mysql import ClientMySql
from parameters.input_params import param_info
from configs.config_info import config_info
from utils.util_cmd import CmdExe
from utils.util_log import log


class VDCClientBase:
    email = ""
    password = ""
    user_id = ""

    region_id = ""
    rm_host = ""
    cloud_service_host = ""
    infra_host = ""
    infra_token = ""
    cloud_service_test_host = ""
    mysql_params = {}

    def __init__(self, instance_name="", kubeconfig=EnvVariable.KUBECONFIG, **kwargs):
        # super().__init__()
        self.kubeconfig = kubeconfig

        self.set_all_property()
        self.reset_region_id()

        self.instance_name = instance_name
        self.ns = self.get_ns(self.user_id.lower())
        self.instance_id = ""
        self.end_point = ""

        self._cloud_rm_api = None
        self._cloud_service_api = None
        self._infra_api = None
        self._cloud_service_test = None
        self._mysql = None
        self._dc_pod_client = None

    @property
    def cloud_rm_api(self) -> CloudRMApi:
        if not isinstance(self._cloud_rm_api, CloudRMApi):
            self._cloud_rm_api = CloudRMApi(user_id=self.user_id, host=self.rm_host, region_id=self.region_id)
        return self._cloud_rm_api

    @property
    def cloud_service_api(self) -> CloudServiceApi:
        if not isinstance(self._cloud_service_api, CloudServiceApi):
            self._cloud_service_api = CloudServiceApi(user_id=self.user_id, host=self.cloud_service_host,
                                                      email=self.email, password=self.password)
        return self._cloud_service_api

    @property
    def infra_api(self) -> InfraApi:
        if not isinstance(self._infra_api, InfraApi):
            self._infra_api = InfraApi(host=self.infra_host, token=self.infra_token, namespace=self.ns)
        return self._infra_api

    @property
    def cloud_service_test(self) -> CloudServiceTestApi:
        if not isinstance(self._cloud_service_test, CloudServiceTestApi):
            self._cloud_service_test = CloudServiceTestApi(host=self.cloud_service_test_host)
        return self._cloud_service_test

    @property
    def mysql(self) -> ClientMySql:
        if not isinstance(self._mysql, ClientMySql):
            self._mysql = ClientMySql(**self.mysql_params)
        return self._mysql

    @property
    def dc_pod_client(self) -> DynamicClient:
        if not isinstance(self._dc_pod_client, DynamicClient):
            self._dc_pod_client = DynamicClient(self.kubeconfig, self.ns, api_version=get_api_version(Pod), kind=Pod)
        return self._dc_pod_client

    @staticmethod
    def _raise(msg: str):
        log.error(msg)
        raise Exception(msg)

    def set_all_property(self):
        vdc_user, vdc_env = config_info.set_vdc_config(vdc_user=param_info.vdc_user, vdc_env=param_info.vdc_env)

        self.email = vdc_user.email
        self.password = vdc_user.password
        self.user_id = vdc_user.user_id

        self.region_id = param_info.vdc_region_id or vdc_env.region_id
        self.rm_host = vdc_env.rm_host
        self.cloud_service_host = vdc_env.cloud_service_host
        self.infra_host = vdc_env.infra_host
        self.infra_token = vdc_env.infra_token
        self.cloud_service_test_host = vdc_env.cloud_service_test_host
        self.mysql_params = vdc_env.mysql

    @staticmethod
    def get_ns(instance_id=""):
        return "milvus-" + instance_id

    def reset_region_id(self):
        if self.region_id.startswith("gcp"):
            self.rm_host = self.rm_host.replace("aws-us-west-2", "gcp-us-west1")

    def reset_auto_params(self, instance_name: str = "", instance_id: str = ""):
        """
        support rewrite: ns, instance_id, instance_name
        """
        if instance_name and not instance_id:
            """ Make sure that the instance is running """
            self.instance_name = instance_name
            self.instance_id = self.get_instance_id(instance_name=self.instance_name)
        elif instance_name and instance_id:
            """ Make sure that the instance name passed in corresponds to the instance id """
            self.instance_name = instance_name
            self.instance_id = instance_id
        else:
            self._raise(
                f"[VDCClientBase] Can't reset params, instance name: {instance_name}, instance id: {instance_id}")

        self.ns = self.get_ns(self.instance_id)
        self.infra_api.reset_ns(self.ns)
        return self.instance_name, self.instance_id

    def get_instance_id(self, instance_name=""):
        instance_name = instance_name or self.instance_name

        res = self.cloud_service_api.list()
        for instance in check_multi_keys_exist(res.data, ["List"]):
            if instance["InstanceName"] == instance_name:
                return instance["InstanceId"]
        self._raise("[VDCClientBase] Can not get instance id for {0}".format(instance_name))

    def create_server(self, instance_name="", image_tag=None, deploy_mode="", max_create_num: int = 10):
        """
        :param instance_name: str
        :param image_tag: Equivalent to db_version
        :param deploy_mode: <class_id>, such as class-1 ... class-1-disk ...
        :param max_create_num: Maximum number of rebuilds when creation fails
        """
        instance_name = instance_name or self.instance_name

        log.info(f"[VDCClientBase] Check instance exists: {self.instance_name}")
        assert not self.check_instance_exist(self.instance_name)

        log.info("[VDCClientBase] Start create instance: %s, image_tag: %s deploy_mode: %s" % (
            instance_name, image_tag, get_class_key_name(ClassID, deploy_mode)))
        res = self.cloud_rm_api.create(class_id=deploy_mode, db_version=image_tag, instance_name=instance_name,
                                       check_result=False)

        # recreate instance when create failed
        count = 1
        if res.code == RMErrorCode.RequestBusy:
            while True:
                time.sleep(1)

                log.info("[VDCClientBase] Recreate instance: %s, class_id: %s, db_version: %s, count: %d" % (
                    instance_name, deploy_mode, image_tag, count))
                res = self.cloud_rm_api.create(class_id=deploy_mode, db_version=image_tag, instance_name=instance_name,
                                               check_result=False)
                if res.code != RMErrorCode.RequestBusy:
                    # set create result
                    if res.code == 0 and "InstanceId" in res.data:
                        instance_id = res.data["InstanceId"]
                        break
                    self._raise(f"[VDCClientBase] Can't create instance: {instance_name}, response: {res.to_dict}")

                # check create counts
                count += 1
                if count >= max_create_num:
                    self._raise(f"[VDCClientBase] Can't create instance: {instance_name}, response: {res.to_dict}")
        elif res.code == 0 and "InstanceId" in res.data:
            instance_id = res.data["InstanceId"]
            log.info(f"[VDCClientBase] The instance: {instance_name} created successfully, instance_id: {instance_id}")
        else:
            instance_id = ""
            self._raise(f"[VDCClientBase] Can't create instance: {instance_name}, response:{res.to_dict}")

        # setting global params
        self.reset_auto_params(instance_name=instance_name, instance_id=instance_id)

        # check server is creating
        assert self.check_server_status()

        return instance_name, instance_id

    def delete_server(self):
        log.info(f"[VDCClientBase] Cloud service API delete instance: {self.instance_id}")
        self.cloud_service_api.delete(instance_id=self.instance_id)
        assert self.check_server_delete()

    def stop_server(self):
        log.info(f"[VDCClientBase] Cloud service API stop instance: {self.instance_id}")
        # stop server and wait stopped
        self.cloud_service_api.stop(self.instance_id)
        # check server status stopped
        assert self.check_server_status(status=InstanceStatus.STOPPED)

    def resume_server(self):
        log.info(f"[VDCClientBase] Cloud service API resume instance: {self.instance_id}")
        # resume server
        self.cloud_service_api.resume(self.instance_id)
        assert self.check_server_status()

    def rm_stop_server(self):
        log.info(f"[VDCClientBase] RM API stop instance: {self.instance_id}")
        # stop server and wait stopped
        self.cloud_rm_api.stop(self.instance_id)
        # check server status stopped
        assert self.check_server_status(status=InstanceStatus.STOPPED)

    def rm_resume_server(self):
        # resume server
        log.info(f"[VDCClientBase] RM API resume instance: {self.instance_id}")
        self.cloud_rm_api.resume(self.instance_id)
        assert self.check_server_status()

    def rm_update_image(self, image_tag: str):
        """ Update server's image """
        _db_version = self.get_server_image()
        # # update msg in db
        # self.rewrite_db_msg()

        self.cloud_rm_api.upgrade_version(instance_id=self.instance_id, db_version=image_tag)

        assert self.rm_check_server_status() and self.rm_check_server_image(image_tag=image_tag)
        log.info(f"[VDCClientBase] Update instance's:{self.instance_name} image from {_db_version} to {image_tag} done")

    def modify_instance(self, class_mode):
        """ Upgrade server's class mode """
        class_id = eval(f"ClassID.{class_mode}")

        # Record server init status
        self.display_server(log_level=LogLevel.DEBUG)

        log.info(f"[VDCClientBase] Start upgrading the instance: {self.instance_name} to class_mode: {class_mode}.")
        res = self.cloud_service_api.modify(instance_id=self.instance_id, class_id=class_id)

        # verify instance status is running after modify
        assert self.check_server_status()

        # verify milvus component resources upgrade
        assert self.check_instance_resources(class_mode=class_mode)

        # TODO verify etcd and dmlChannelNum config upgrade

        log.info(f"[VDCClientBase] Instance: {self.instance_name} upgrade to class_mode: {class_mode} completed.")

    def rm_modify_instance_parameters(self, modify_params_dict: dict):
        """
        Update milvus.yaml configs, and follow the configuration format of milvus.yaml
        """
        # parser modify yaml params to simple properties string: example {"log.level": "debug"}
        modify_params = parser_modify_params(modify_params_dict)

        # Record server init status
        self.display_server(log_level=LogLevel.DEBUG)

        # modify instance parameters
        log.debug(f"[VDCClientBase] modify params: {modify_params}")
        for param_name, param_value in modify_params.items():
            self.cloud_rm_api.modify_instance_params(self.instance_id, param_name, param_value)

        # stop instance and check stopped
        self.stop_server()

        # resume stopped instance and wait running
        self.resume_server()
        log.info(f"[VDCClientBase] Modify instance's: {self.instance_name} parameters completed.")

    def infra_update_resource(self, resource: dict):
        """
        Upgrade Pods's resources

        resource example as follow:
          imageTag: v2.2.0-20230329-2f52d66-28175ab
          <name>:  # support mixCoord, dataNode, indexNode, queryNode, proxy, standalone
            replicas: 1
            paused: false
            resources:
              limits:
                cpu: '2'
                memory: 9Gi
              requests:
                cpu: '1'
                memory: 2Gi
            cpu: 0
            memory: 0
          etcd:
            replicas: 3
            resources:
              limits:
                cpu: '1'
                memory: 1536Mi
              requests:
                cpu: '0.13'
                memory: 100Mi
        """
        res = self.infra_api.get_milvus(instance_id=self.instance_id)
        cluster = res.data["cluster"] if "cluster" in res.data else ""
        assert cluster

        s_dict = {"spec": resource}
        t_dict = update_dict_value(s_dict, res.data)
        if operator.eq(res.data, t_dict):
            log.info("[VDCClientBase] No resource updates.")
            return True

        log.debug(f"[VDCClientBase] Update resource: {s_dict} to \nres.data:{res.data}, \ntarget: {t_dict}")

        self.infra_api.pre_apply(instance_id=self.instance_id,
                                 body={"ownerId": "milvus-" + self.user_id.lower(),
                                       "cluster": cluster,
                                       "qaTestIgnoreQuota": True,
                                       "resources": {
                                           "limits": {
                                               "cpu": add_resource(t_dict["spec"], "cpu"),
                                               "memory": str(add_resource(t_dict["spec"], "memory")) + 'Gi'},
                                           "requests": {
                                               "cpu": add_resource(t_dict["spec"], "cpu"),
                                               "memory": str(add_resource(t_dict["spec"], "memory")) + 'Gi'}
                                       }
                                       })
        self.infra_api.upgrade_milvus(instance_id=self.instance_id, body=t_dict)
        self.infra_api.get_milvus(instance_id=self.instance_id)
        assert self.infra_check_server_status()
        log.info(f"[VDCClientBase] Update resource done.")

        # update msg in db
        self.rewrite_db_msg()

    def rewrite_db_msg(self):
        # update msg of resource.instance_class, resource.cust_instance_node and resource.instance_class_oversold
        log.info("[VDCClientBase] Start rewriting messages in the database.")
        res = self.infra_api.get_milvus(instance_id=self.instance_id)
        spec = check_multi_keys_exist(res.data, ["spec"])

        class_ids, db_raws, oversold = get_child_class_id(spec=spec)
        log.info(f"[VDCClientBase] class_ids: {class_ids}, db_raws: {db_raws}, oversold: {oversold}")

        # check child class_id in resource.instance_class
        for class_id in class_ids:
            log.debug("[VDCClientBase] Check class id: {0}, insert to db if not exist.".format(class_id[0]))
            sql_insert_instance_class(self.mysql, class_id=class_id[0],
                                      cpu_cores=class_id[1], mem_size=class_id[2], region_id=self.region_id)

        # get milvus create time from resource.cust_instance_node and delete msg
        query_result = query_cust_instance_node(self.mysql, self.instance_id)
        create_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if len(query_result) != 0:
            create_time = query_result[0]["create_time"]
            delete_cust_instance_node(self.mysql, self.instance_id)

        # update oversold table resource.instance_class_oversold
        for o in oversold:
            log.debug("[VDCClientBase] Update oversold table: {0}".format(o))
            if hasattr(RMNodeCategory, o[0]):
                category = eval(f"RMNodeCategory.{o[0]}")
            else:
                raise Exception(f"[VDCClientBase] Can not get category: {o[0]}")

            sql_insert_instance_class_oversold(self.mysql, instance_id=self.instance_id, user_id=self.user_id,
                                               class_id=o[1], node_category=category, extend_fields=o[2])

        # insert new msg into resource.cust_instance_node
        for db_raw in db_raws:
            msg = "[VDCClientBase] Update resource of {1} {0}: {3}cpu, {4}memory, class_id: {2}"
            log.debug(msg.format(db_raw[0], db_raw[1], db_raw[2], db_raw[3], db_raw[4]))
            if hasattr(RMNodeCategory, db_raw[0]):
                category = eval(f"RMNodeCategory.{db_raw[0]}")
            else:
                raise Exception(f"[VDCClientBase] Can not get category: {db_raw[0]}")
            for k in range(db_raw[1]):
                sql_insert_cust_instance_node(
                    self.mysql, instance_id=self.instance_id, cpu_cores=db_raw[3], mem_size=db_raw[4],
                    class_id=db_raw[2], category=category, region_id=self.region_id, create_time=create_time)

        query_cust_instance_node(self.mysql, self.instance_id)
        log.info("[VDCClientBase] Rewrite message in database complete.")

    """ Check status functions """

    # Cloud Service API
    def check_server_status(self, timeout=1800, status: InstanceStatus = InstanceStatus.RUNNING, interval_time=60):
        start_time = time.time()
        while time.time() < start_time + timeout:
            log.info("[VDCClientBase] Waiting for instance: %s to be %s using cloud_service_api ..." % (
                self.instance_name, get_class_key_name(InstanceStatus, status)))
            time.sleep(interval_time)

            res = self.cloud_service_api.describe(instance_id=self.instance_id, check_result=False)
            if isinstance(res.data, dict) and "Status" in res.data and res.data["Status"] == status:
                log.info(f'[VDCClientBase] Instance: {self.instance_name} is {res.data["StatusName"]}')
                return True

        self._raise(f"[VDCClientBase] Instance: {self.instance_name} is not ready, Status is not RUNNING.")

    def check_server_delete(self, timeout=1800, interval_time=20):
        start_time = time.time()
        while time.time() < start_time + timeout:
            time.sleep(interval_time)

            res = self.cloud_service_api.list(search_key="", check_result=False)
            if self.instance_name not in [i["InstanceName"] for i in check_multi_keys_exist(res.data, ["List"])]:
                log.info("[VDCClientBase] Instance: {0} has been deleted.".format(self.instance_name))
                return True
        log.info("[VDCClientBase] Instance: {0} has not been deleted.".format(self.instance_name))
        return False

    def check_instance_exist(self, instance_name: str = ""):
        instance_name = instance_name or self.instance_name

        res = self.cloud_service_api.list(search_key="")
        if instance_name in [instance["InstanceName"] for instance in check_multi_keys_exist(res.data, ["List"])]:
            log.info("[VDCClientBase] Instance exists: {0}".format(instance_name))
            return True

        log.info("[VDCClientBase] Instance doesn't exist: {0}".format(instance_name))
        return False

    # RM API
    def rm_check_server_status(self, timeout=1800, interval_time=60):
        start_time = time.time()
        while time.time() < start_time + timeout:
            log.info(f"[VDCClientBase] Waiting for instance: {self.instance_name} to be ready using cloud_rm_api ...")
            time.sleep(interval_time)

            res = self.cloud_rm_api.describe(instance_id=self.instance_id, check_result=False)
            if check_multi_keys_exist(res.data, ["Status"]) == InstanceStatus.RUNNING:
                log.info("[VDCClientBase] Instance: %s is ready, Status: %s" % (
                    self.instance_name, get_class_key_name(InstanceStatus, InstanceStatus.RUNNING)))
                return True

        self._raise(f"[VDCClientBase] Instance: {self.instance_name}, Status is not RUNNING.")

    def rm_check_server_image(self, image_tag):
        _db_version = self.get_server_image()
        if _db_version == image_tag:
            log.info(f"[VDCClientBase] Image: {image_tag} for instance: {self.instance_name} check done.")
            return True

        self._raise("[VDCClientBase] Image: %s for instance: %s check failed, and the actual image used is %s." % (
            image_tag, self.instance_name, _db_version))

    # Infra API
    def infra_check_server_status(self, timeout=1800, interval_time=60, status=True):
        start_time = time.time()
        while time.time() < start_time + timeout:
            log.info(f"[VDCClientBase] Waiting for instance: {self.instance_name} to be ready using infra_api ...")
            time.sleep(interval_time)

            res = self.infra_api.get_milvus(instance_id=self.instance_id, check_result=False)
            if check_multi_keys_exist(res.data, ["status", "ready", "isReady"]) is status:
                log.info(f'[VDCClientBase] Instance: {self.instance_name} is ready, Status:{status}')
                return True

        self._raise(f"[VDCClientBase] Instance: {self.instance_name} is not ready, Status is not RUNNING.")

    # Others
    def check_instance_resources(self, class_mode: str):
        class_id = eval(f"ClassID.{class_mode}")
        component = CLASS_RESOURCES_MAP[class_id]["component"]
        log.info("[VDCClientBase] Check resource for instance:%s, component:%s, class_mode:%s, class_id:%s" % (
            self.instance_name, component, class_mode, class_id))

        res = self.get_specified_pod_resources(component=component, instance_id=self.instance_id, namespace=self.ns)
        pods = res["items"]

        if 0 == len(pods):
            raise Exception(f"[VDCClientBase] Not pods need to check resources: {res}")

        # check pod numbers
        assert len(pods) == CLASS_RESOURCES_MAP[class_id]['replicas']

        # check pod resource
        for pod in pods:
            pod_resource = pod["spec"]["containers"][0]["resources"]["limits"]
            cpu, memory = [pod_resource.get(i, None) for i in ["cpu", "memory"]]
            assert cpu == CLASS_RESOURCES_MAP[class_id]['cpu'] and memory == CLASS_RESOURCES_MAP[class_id]['memory']
        return True

    # Common functions
    def get_release_version(self, db_version_prefix: str, current_page: int = 1, page_size: int = 20,
                            max_page: int = 100):
        while max_page > current_page:
            res = self.cloud_rm_api.release_version(current_page=current_page, page_size=page_size)

            for db_version in [i["dbVersion"] for i in check_multi_keys_exist(res.data, ["list"]) if i["insType"] == 1]:
                if str(db_version).startswith(db_version_prefix):
                    log.info(f"[VDCClientBase] Auto get prefix: {db_version_prefix}, dbVersion: {db_version}")
                    return db_version

            current_page += 1

        self._raise(f"[VDCClientBase] Unable to get release version, maximum page: {max_page} for search exceeded.")

    def get_server_image(self):
        res = self.cloud_rm_api.describe(instance_id=self.instance_id)
        return check_multi_keys_exist(res.data, ["DBVersion"])

    def get_pwd(self):
        return self.cloud_service_test.get_root_pwd(instance_id=self.instance_id).data

    def get_endpoint(self):
        res = self.cloud_service_api.list()
        for instance in check_multi_keys_exist(res.data, ["List"]):
            if instance["InstanceName"] == self.instance_name and "ConnectAddress" in instance:
                self.end_point = instance["ConnectAddress"]
                port = str(self.end_point).split(':')[-1]
                host = str(self.end_point).rstrip(':' + port).lstrip("https://")
                log.info(f"[VDCClientBase] Instance: {self.instance_name}, host: {host}, port: {port}")
                return host, port
        self._raise(f"[VDCClientBase] Can not get endpoint of instance: {self.instance_name}")

    def get_all_values(self, instance_id=""):
        instance_id = instance_id or self.instance_id
        return CmdExe(f"kubectl get mi {instance_id} -o yaml -n {self.get_ns(instance_id)}").run_cmd()

    def get_pods(self, instance_id=""):
        instance_id = instance_id or self.instance_id
        return CmdExe(
            f"kubectl get pod -o wide -n {self.get_ns(instance_id)} | grep -E \"NAME|{instance_id}\"").run_cmd()

    def get_pvc(self, instance_id=""):
        return CmdExe(f"kubectl get pvc -n {self.get_ns((instance_id or self.instance_id))}").run_cmd()

    def display_server(self, log_level=LogLevel.DEBUG):
        log.customize(log_level)(self.get_all_values())
        log.customize(log_level)(self.get_pods())

    def get_specified_pod_resources(self, component: str = "", instance_id: str = "", namespace: str = ""):
        namespace = namespace or self.ns
        instance_id = instance_id or self.instance_id
        label_selectors = ""

        if component:
            label_selectors = f"app.kubernetes.io/component={component}"

        if instance_id:
            _instance_label = f"app.kubernetes.io/instance={instance_id}"
            label_selectors += "," + _instance_label if label_selectors else _instance_label

        return self.dc_pod_client.get(namespace=namespace, label_selector=label_selectors)
