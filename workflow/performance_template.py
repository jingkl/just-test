from typing import List, Tuple
from collections import Iterable, Iterator
from pprint import pformat
import copy

from workflow.base import Base
from parameters.input_params import param_info, InputParamsBase
from utils.util_log import log
from utils.util_catch import func_request
from commons.common_func import get_sync_report_flag
from data_report.metrics import Report_Metric_Object
from deploy.commons.common_params import CLUSTER, STANDALONE
from db_client.client_db import Database_Client
from check.data_check import DataCheck


class PerfTemplate(Base):

    @staticmethod
    def clear_deploy_report_params(input_params: InputParamsBase):
        input_params.deploy_tool = ""
        input_params.deploy_mode = ""
        input_params.deploy_config = ""

    def serial_template(self, input_params: InputParamsBase, case_callable_obj: callable,
                        default_case_params: dict = {}, cpu=8, mem=16, deploy_mode=STANDALONE):
        log.info("[PerfTemplate] Input parameters: {0}".format(vars(input_params)))
        input_params = copy.deepcopy(input_params)

        # server
        if not param_info.deploy_skip:
            input_params.deploy_mode = input_params.deploy_mode or deploy_mode
            self.deploy_default(deploy_tool=input_params.deploy_tool,
                                deploy_mode=input_params.deploy_mode,
                                other_config=input_params.deploy_config, cpu=cpu,
                                mem=mem)
            # update server metric
            Report_Metric_Object.update_server(deploy_tool=input_params.deploy_tool,
                                               deploy_mode=input_params.deploy_mode,
                                               config_name=self.deploy_config[1], config=self.deploy_config[2])
        else:
            self.clear_deploy_report_params(input_params=input_params)
        Report_Metric_Object.update_server(host=param_info.param_host)

        if param_info.client_test_skip:
            log.info("[PerfTemplate] Skip client test, display server host:{0}, port:{1}".format(param_info.param_host,
                                                                                                 param_info.param_port))
            return param_info.param_host, param_info.param_port

        # client
        run_perf_case = self.run_perf_case(callable_obj=case_callable_obj,
                                           default_case_params=default_case_params,
                                           case_params=input_params.case_params,
                                           case_prepare=not input_params.case_skip_prepare,
                                           case_rebuild_index=not input_params.case_skip_build_index,
                                           case_clean_collection=not input_params.case_skip_clean_collection,
                                           case_prepare_clean=not input_params.case_skip_prepare_clean)
        for case in next(run_perf_case):
            log.info("[PerfTemplate] Actual parameters used: {}".format(case.ActualParamsUsed))
            Report_Metric_Object.update_client(test_case_type=case.CaseType,
                                               test_case_params=case.ActualParamsUsed)
            if callable(case.CallableObject):
                res = case.CallableObject(*case.ObjectArgs, **case.ObjectKwargs)
                if len(res) == 2 and res[1] is True:
                    Report_Metric_Object.update_result(test_result=res[0])
                    # report data to mongodb
                    log.info("[PerfTemplate] Report data: \n{}".format(pformat(Report_Metric_Object.to_dict(),
                                                                               sort_dicts=False)))
                    Database_Client.mongo_insert(Report_Metric_Object.to_dict())
                elif len(res) == 2 and res[1] is False:
                    param_info.test_status = False

        if isinstance(run_perf_case, Iterator):
            return next(run_perf_case)

        # todo server status check

    def concurrency_template(self, input_params: InputParamsBase, case_callable_obj: callable,
                             default_case_params: dict = {}, cpu=8, mem=16, deploy_mode=STANDALONE, interval=30,
                             sync_report=False, old_version_format=True):
        log.info("[PerfTemplate] Input parameters: {0}".format(vars(input_params)))
        input_params = copy.deepcopy(input_params)

        # server
        if not param_info.deploy_skip:
            input_params.deploy_mode = input_params.deploy_mode or deploy_mode
            self.deploy_default(deploy_tool=input_params.deploy_tool,
                                deploy_mode=input_params.deploy_mode,
                                other_config=input_params.deploy_config, cpu=cpu,
                                mem=mem)
            # update server metric
            Report_Metric_Object.update_server(deploy_tool=input_params.deploy_tool,
                                               deploy_mode=input_params.deploy_mode,
                                               config_name=self.deploy_config[1], config=self.deploy_config[2])
        else:
            self.clear_deploy_report_params(input_params=input_params)
        Report_Metric_Object.update_server(host=param_info.param_host)

        if param_info.client_test_skip:
            log.info("[PerfTemplate] Skip client test, display server host:{0}, port:{1}".format(param_info.param_host,
                                                                                                 param_info.param_port))
            return param_info.param_host, param_info.param_port

        # init report
        report_client = DataCheck(tags={"case_name": Report_Metric_Object.client.test_case_name,
                                        "run_id": Report_Metric_Object.client.run_id}, file_path=log.log_info,
                                  interval=interval, old_version_format=old_version_format)
        report_client.start_stream_read(sync_report=get_sync_report_flag(sync_report,
                                                                         sync_report=param_info.sync_report,
                                                                         async_report=param_info.async_report))

        # client
        run_perf_case = self.run_perf_case(callable_obj=case_callable_obj,
                                           default_case_params=default_case_params,
                                           case_params=input_params.case_params,
                                           case_prepare=not input_params.case_skip_prepare,
                                           case_rebuild_index=not input_params.case_skip_build_index,
                                           case_clean_collection=not input_params.case_skip_clean_collection,
                                           case_prepare_clean=not input_params.case_skip_prepare_clean)
        for case in next(run_perf_case):
            log.info("[PerfTemplate] Actual parameters used: {}".format(case.ActualParamsUsed))
            Report_Metric_Object.update_client(test_case_type=case.CaseType,
                                               test_case_params=case.ActualParamsUsed)
            if callable(case.CallableObject):
                res = case.CallableObject(*case.ObjectArgs, **case.ObjectKwargs)
                if len(res) == 2:
                    Report_Metric_Object.update_result(test_result=res[0])
                    # report data to mongodb
                    log.info("[PerfTemplate] Report data: \n{}".format(pformat(Report_Metric_Object.to_dict(),
                                                                               sort_dicts=False)))
                    Database_Client.mongo_insert(Report_Metric_Object.to_dict())
                    if res[1] is False:
                        param_info.test_status = False

        # stop report
        report_client.finish_stream_read()

        if isinstance(run_perf_case, Iterator):
            return next(run_perf_case)

        # todo server status check
