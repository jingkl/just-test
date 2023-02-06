import time

from deploy.configs.default_configs import DefaultConfigs
from deploy.commons.common_params import CLUSTER, STANDALONE, Helm, Operator, DefaultRepository
from deploy.client.default_client import DefaultClient

from utils.util_log import log
from parameters.input_params import param_info
from commons.common_func import parser_input_config, execute_funcs, update_dict_value, check_deploy_config
from commons.auto_get import AutoGetTag
from data_report.metrics import Report_Metric_Object


class Base:
    """ Initialize the properties of the Base class """
    teardown_funcs = []

    deploy_client = None
    deploy_config = {}
    upgrade_config = {}
    deploy_release_name = ""
    deploy_initial_state = ""
    deploy_end_state = ""

    def setup_class(self):
        log.info(" Start setup class ".center(100, "~"))
        msg = "The parameters that can be passed in the cmd line of workflow are as follows"
        log.info("[setup_class] {0}: {1}".format(msg, self.__str__(self)))

    def teardown_class(self):
        log.info(" Start teardown class ".center(100, "~"))

    def setup_method(self, method):
        log.reset_log_file_path(subfolder=method.__name__)
        log.info(" setup ".center(100, "*"))
        log.info(log.log_msg)

        log.info("[setup_method] Start setup test case {0}, test document:{1}".format(method.__name__, method.__doc__))

        self.teardown_funcs = []

        self.deploy_client = None
        self.deploy_config = {}
        self.upgrade_config = {}
        self.deploy_release_name = ""
        # record deploy status
        self.deploy_initial_state = ""
        self.deploy_end_state = ""

        # reset data report object
        Report_Metric_Object.reset()
        Report_Metric_Object.update_client(test_case_name=method.__name__)

        # Delete the service after the test is over
        if not param_info.deploy_retain and not param_info.deploy_skip:
            self.set_teardown_funcs(self.deploy_delete)

        log.info("[setup_method] Test case: {0}, Test run_id: {1}".format(Report_Metric_Object.client.test_case_name,
                                                                          Report_Metric_Object.client.run_id))

    def teardown_method(self, method):
        log.info(" teardown ".center(100, "*"))
        log.info("[teardown_method] Start teardown test case %s." % method.__name__)
        log.info("[teardown_method] Execute teardown functions: {0}".format(self.teardown_funcs))
        execute_funcs(self.teardown_funcs)
        log.info("[teardown_method] Teardown test case %s done." % method.__name__)

        if param_info.test_status is False:
            msg = "Test result is False, please check!!!"
            log.error(msg)
            param_info.test_status = True
            assert False

    def set_teardown_funcs(self, callable_obj: callable, *args, **kwargs):
        c = [callable_obj, ]
        c.extend(list(args))
        self.teardown_funcs.append((c, kwargs))

    @staticmethod
    def parser_endpoint_to_global(endpoint):
        _e = str(endpoint).split(":")
        if len(_e) == 2:
            param_info.param_host = _e[0]
            param_info.param_port = int(_e[1])
        elif len(_e) == 1:
            param_info.param_host = _e[0]
        else:
            raise Exception("[Base] Can not parser endpoint: {0}, type: {1}, please check.".format(endpoint,
                                                                                                   type(endpoint)))

    def init_server_client(self, deploy_tool=Operator, deploy_mode=STANDALONE):
        self.deploy_client = DefaultClient(deploy_tool=deploy_tool, deploy_mode=deploy_mode)

    def deploy_default(self, deploy_tool=Operator, deploy_mode=STANDALONE, cpu=8, mem=16, other_config=None,
                       tag=None, repository=None):
        tag = tag or param_info.milvus_tag or AutoGetTag().auto_tag()
        repository = repository or param_info.tag_repository

        # parser configs
        other_configs = parser_input_config(input_content=other_config)

        # init server config
        config_obj = DefaultConfigs(deploy_tool=deploy_tool, deploy_mode=deploy_mode)
        set_image = config_obj.set_image(tag=tag, repository=repository) if tag is not None else {}
        self.deploy_config = config_obj.server_resource(cpu=cpu, mem=mem, other_configs=[other_configs, set_image])
        log.info("[Base] deploy config: {}".format(self.deploy_config))

        # init server client
        self.deploy_client = DefaultClient(deploy_tool=deploy_tool, deploy_mode=deploy_mode)

        # install server and get endpoint
        server_install_params = check_deploy_config(deploy_tool=deploy_tool, configs=self.deploy_config[0])
        self.deploy_release_name = self.deploy_client.install(server_install_params)
        self.deploy_client.wait_for_healthy(release_name=self.deploy_release_name)
        endpoint = self.deploy_client.endpoint(release_name=self.deploy_release_name)

        # display server values
        log.debug(self.deploy_client.get_all_values(release_name=self.deploy_release_name))
        self.deploy_initial_state = self.deploy_client.get_pods(release_name=self.deploy_release_name)

        # set global host and port
        self.parser_endpoint_to_global(endpoint)

        log.info("[Base] Service deployed successfully:{0}".format(self.deploy_release_name))
        return self.deploy_release_name, endpoint

    def upgrade_service(self, release_name=None, deploy_tool=Operator, deploy_mode=STANDALONE, upgrade_config=None):
        release_name = release_name or param_info.release_name
        if release_name in ["", None]:
            raise Exception("[Base] Can not upgrade empty release name, please check.")

        # init server client and default config
        self.deploy_client = DefaultClient(deploy_tool=deploy_tool, deploy_mode=deploy_mode, release_name=release_name)
        config_obj = DefaultConfigs(deploy_tool=deploy_tool, deploy_mode=deploy_mode)

        # parser configs and install server
        upgrade_configs = parser_input_config(input_content=upgrade_config)

        # format upgrade_config helm -> str, operator -> dict
        format_upgrade_config = config_obj.config_conversion(upgrade_configs, update_helm_file=False)
        self.upgrade_config = [format_upgrade_config, upgrade_configs]

        # check upgrade config and upgrade service
        server_upgrade_params = check_deploy_config(deploy_tool=deploy_tool, configs=self.upgrade_config[0])
        log.info("[Base] upgrade configs: {}".format(server_upgrade_params))
        self.deploy_client.upgrade(server_upgrade_params)

        # wait healthy and get endpoint
        wait_time_after_scale = 10
        time.sleep(wait_time_after_scale)
        self.deploy_client.wait_for_healthy(release_name=release_name)
        endpoint = self.deploy_client.endpoint(release_name=release_name)

        # display server values
        log.debug(self.deploy_client.get_all_values(release_name=release_name))
        self.deploy_initial_state = self.deploy_client.get_pods(release_name=release_name)

        # set global host and port
        self.parser_endpoint_to_global(endpoint)
        log.info("[Base] Service upgraded successfully:{0}".format(self.deploy_release_name))
        return server_upgrade_params

    def deploy_delete(self, deploy_client=None, deploy_release_name=""):
        deploy_client = deploy_client or self.deploy_client
        deploy_release_name = deploy_release_name or self.deploy_release_name or param_info.release_name

        if deploy_client:
            # display server values before delete
            # log.info(self.deploy_client.get_all_values(release_name=self.deploy_release_name))
            log.info("[Base] Deploy initial state: \n{}".format(self.deploy_initial_state))
            self.deploy_end_state = self.deploy_client.get_pods(release_name=self.deploy_release_name)

            log.info("[Base] Start deleting services: {0}".format(deploy_release_name))
            deploy_client.get_pvc(release_name=deploy_release_name)
            deploy_client.uninstall(release_name=deploy_release_name)
            deploy_client.delete_pvc(release_name=deploy_release_name)
            log.info("[Base] Service deleted successfully: {0}".format(deploy_release_name))

    def run_perf_case(self, callable_obj: callable, default_case_params, case_params, case_prepare, case_prepare_clean,
                      case_rebuild_index, case_clean_collection):
        # parser case params
        _case_params = parser_input_config(input_content=case_params)

        # update case params
        case_parameters = update_dict_value(_case_params, default_case_params)

        return callable_obj(params=case_parameters, prepare=case_prepare, prepare_clean=case_prepare_clean,
                            rebuild_index=case_rebuild_index, clean_collection=case_clean_collection)
