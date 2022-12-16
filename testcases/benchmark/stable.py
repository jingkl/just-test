import pytest

from client.cases import ConcurrentClientBase
from client.parameters.input_params import ConcurrentParams
from deploy.commons.common_params import CLUSTER, STANDALONE, Helm, Operator
from workflow.performance_template import PerfTemplate
from parameters.input_params import InputParamsBase
from commons.common_type import DefaultParams as dp
import client.parameters.input_params.define_params as cdp


class TestConcurrentCases(PerfTemplate):
    """
    Concurrent test cases
    Author: ting.wang@zilliz.com
    """

    def __str__(self):
        return """
            :param input_params: Input parameters
                deploy_tool: Optional[str]
                deploy_mode: Optional[str]
                deploy_config: Union[str, dict]
                case_params: Union[str, dict]
                case_skip_prepare: Optional[bool]
                case_skip_prepare_clean: Optional[bool]
                case_skip_clean_collection: Optional[bool]
            :type input_params: InputParamsBase

            :roughly follow the steps:
                1. deployment service or use an already deployed service
                2. connect service and start test
                    a. concurrent test
                3. check test result and report
                4. clean env"""

    def test_concurrent_locust_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        # concurrent_tasks = [ConcurrentParams.params_search(), ConcurrentParams.params_query(ids=[1, 10, 100, 1000]),
        #                     ConcurrentParams.params_scene_insert_delete_flush(random_id=True, random_vector=True)]
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_ivf_sq8_search_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_search(search_param={"nprobe": 16})],
            concurrent_number=[100],
            during_time=1800, interval=20,
            **cdp.DefaultIndexParams.IVF_SQ8)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_ivf_sq8_search_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_search(search_param={"nprobe": 16})],
            concurrent_number=[100],
            during_time=1800, interval=20,
            **cdp.DefaultIndexParams.IVF_SQ8)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_ivf_sq8_search_high_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_search(nq=10000, top_k=10, search_param={"nprobe": 16})],
            concurrent_number=[1000],
            during_time=1800, interval=20,
            **cdp.DefaultIndexParams.IVF_SQ8)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_ivf_sq8_search_high_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_search(nq=10000, top_k=10, search_param={"nprobe": 16})],
            concurrent_number=[1000],
            during_time=1800, interval=20,
            **cdp.DefaultIndexParams.IVF_SQ8)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_flat_random_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [ConcurrentParams.params_search(weight=20, nq=10, top_k=10, search_param={"nprobe": 16}),
                            ConcurrentParams.params_query(weight=2, ids=[i for i in range(10)]),
                            ConcurrentParams.params_load(weight=1),
                            ConcurrentParams.params_flush(weight=1),
                            ConcurrentParams.params_insert(weight=20, nb=1, random_id=True, random_vector=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(concurrent_tasks, concurrent_number=[20],
                                                                         during_time=600, interval=20,
                                                                         **cdp.DefaultIndexParams.FLAT)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_flat_random_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [ConcurrentParams.params_search(weight=20, nq=10, top_k=10, search_param={"nprobe": 16}),
                            ConcurrentParams.params_query(weight=2, ids=[i for i in range(10)]),
                            ConcurrentParams.params_load(weight=1),
                            ConcurrentParams.params_flush(weight=1),
                            ConcurrentParams.params_insert(weight=20, nb=1, random_id=True, random_vector=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(concurrent_tasks, concurrent_number=[20],
                                                                         during_time=600, interval=20,
                                                                         **cdp.DefaultIndexParams.FLAT)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_diskann_dql_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [ConcurrentParams.params_search(weight=10, nq=10, top_k=10,
                                                           search_param={"search_list": 30}),
                            ConcurrentParams.params_query(weight=2, ids=[i for i in range(10)]),
                            ConcurrentParams.params_load(weight=1)]
        default_case_params = ConcurrentParams().params_scene_concurrent(concurrent_tasks, concurrent_number=[20],
                                                                         during_time="1h", interval=20,
                                                                         dataset_size="10m",
                                                                         **cdp.DefaultIndexParams.DISKANN)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_diskann_dql_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [ConcurrentParams.params_search(weight=10, nq=10, top_k=10,
                                                           search_param={"search_list": 30}),
                            ConcurrentParams.params_query(weight=2, ids=[i for i in range(10)]),
                            ConcurrentParams.params_load(weight=1)]
        default_case_params = ConcurrentParams().params_scene_concurrent(concurrent_tasks, concurrent_number=[20],
                                                                         during_time="1h", interval=20,
                                                                         dataset_size="10m",
                                                                         **cdp.DefaultIndexParams.DISKANN)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_hnsw_dml_dql_filter_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [ConcurrentParams.params_search(weight=10, nq=10, top_k=10, search_param={"ef": 16},
                                                           expr="{'float_1': {'GT': -1.0, 'LT': 100000 * 0.5}}"),
                            ConcurrentParams.params_query(weight=5, ids=[i for i in range(10)]),
                            ConcurrentParams.params_load(weight=2),
                            ConcurrentParams.params_delete(weight=1, delete_length=1),
                            ConcurrentParams.params_insert(weight=1, nb=1, random_id=True, random_vector=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(concurrent_tasks, concurrent_number=[20],
                                                                         during_time="5h", interval=20,
                                                                         dataset_size="10w", other_fields=["float_1"],
                                                                         **cdp.DefaultIndexParams.HNSW)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_hnsw_dml_dql_filter_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [ConcurrentParams.params_search(weight=10, nq=10, top_k=10, search_param={"ef": 16},
                                                           expr="{'float_1': {'GT': -1.0, 'LT': 100000 * 0.5}}"),
                            ConcurrentParams.params_query(weight=5, ids=[i for i in range(10)]),
                            ConcurrentParams.params_load(weight=2),
                            ConcurrentParams.params_delete(weight=1, delete_length=1),
                            ConcurrentParams.params_insert(weight=1, nb=1, random_id=True, random_vector=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(concurrent_tasks, concurrent_number=[20],
                                                                         during_time="5h", interval=20,
                                                                         dataset_size="10w", other_fields=["float_1"],
                                                                         **cdp.DefaultIndexParams.HNSW)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_diskann_dml_dql_filter_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [ConcurrentParams.params_search(weight=10, nq=10, top_k=10, search_param={"search_list": 30},
                                                           expr="{'float_1': {'GT': -1.0, 'LT': 100000 * 0.5}}"),
                            ConcurrentParams.params_query(weight=5, ids=[i for i in range(10)]),
                            ConcurrentParams.params_load(weight=2),
                            ConcurrentParams.params_delete(weight=1, delete_length=1),
                            ConcurrentParams.params_insert(weight=1, nb=1, random_id=True, random_vector=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(concurrent_tasks, concurrent_number=[20],
                                                                         during_time="5h", interval=20,
                                                                         dataset_size="10w", other_fields=["float_1"],
                                                                         **cdp.DefaultIndexParams.DISKANN)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_diskann_dml_dql_filter_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [ConcurrentParams.params_search(weight=10, nq=10, top_k=10, search_param={"search_list": 30},
                                                           expr="{'float_1': {'GT': -1.0, 'LT': 100000 * 0.5}}"),
                            ConcurrentParams.params_query(weight=5, ids=[i for i in range(10)]),
                            ConcurrentParams.params_load(weight=2),
                            ConcurrentParams.params_delete(weight=1, delete_length=1),
                            ConcurrentParams.params_insert(weight=1, nb=1, random_id=True, random_vector=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(concurrent_tasks, concurrent_number=[20],
                                                                         during_time="5h", interval=20,
                                                                         dataset_size="10w", other_fields=["float_1"],
                                                                         **cdp.DefaultIndexParams.DISKANN)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_hnsw_compaction_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [ConcurrentParams.params_search(weight=20, nq=10, top_k=10, search_param={"ef": 16},
                                                           expr="{'float_1': {'GT': -1.0, 'LT': 100000 * 0.5}}"),
                            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
                            ConcurrentParams.params_load(weight=1),
                            ConcurrentParams.params_scene_insert_delete_flush(weight=1, insert_length=1,
                                                                              delete_length=1, random_id=True,
                                                                              random_vector=True, varchar_filled=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(concurrent_tasks, concurrent_number=[20],
                                                                         during_time="5h", interval=20,
                                                                         dataset_size="10w", other_fields=["float_1"],
                                                                         **cdp.DefaultIndexParams.HNSW)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_hnsw_compaction_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [ConcurrentParams.params_search(weight=20, nq=10, top_k=10, search_param={"ef": 16},
                                                           expr="{'float_1': {'GT': -1.0, 'LT': 100000 * 0.5}}"),
                            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
                            ConcurrentParams.params_load(weight=1),
                            ConcurrentParams.params_scene_insert_delete_flush(weight=1, insert_length=1,
                                                                              delete_length=1, random_id=True,
                                                                              random_vector=True, varchar_filled=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(concurrent_tasks, concurrent_number=[20],
                                                                         during_time="5h", interval=20,
                                                                         dataset_size="10w", other_fields=["float_1"],
                                                                         **cdp.DefaultIndexParams.HNSW)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_diskann_compaction_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [ConcurrentParams.params_search(weight=20, nq=10, top_k=10, search_param={"search_list": 30},
                                                           expr="{'float_1': {'GT': -1.0, 'LT': 100000 * 0.5}}"),
                            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
                            ConcurrentParams.params_load(weight=1),
                            ConcurrentParams.params_scene_insert_delete_flush(weight=1, insert_length=1,
                                                                              delete_length=1, random_id=True,
                                                                              random_vector=True, varchar_filled=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(concurrent_tasks, concurrent_number=[20],
                                                                         during_time="5h", interval=20,
                                                                         dataset_size="10w", other_fields=["float_1"],
                                                                         **cdp.DefaultIndexParams.DISKANN)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_diskann_compaction_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [ConcurrentParams.params_search(weight=20, nq=10, top_k=10, search_param={"search_list": 30},
                                                           expr="{'float_1': {'GT': -1.0, 'LT': 100000 * 0.5}}"),
                            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
                            ConcurrentParams.params_load(weight=1),
                            ConcurrentParams.params_scene_insert_delete_flush(weight=1, insert_length=1,
                                                                              delete_length=1, random_id=True,
                                                                              random_vector=True, varchar_filled=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(concurrent_tasks, concurrent_number=[20],
                                                                         during_time="5h", interval=20,
                                                                         dataset_size="10w", other_fields=["float_1"],
                                                                         **cdp.DefaultIndexParams.DISKANN)
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, old_version_format=False, sync_report=True,
                                  case_callable_obj=ConcurrentClientBase().scene_concurrent_locust,
                                  default_case_params=default_case_params)
