import sys
import pytest

from client.cases import AccCases, InsertBatch, BuildIndex, Load, Query, Search
from client.parameters.input_params import AccParams, InsertBatchParams, BuildIndexParams, LoadParams, QueryParams, \
    SearchParams
from deploy.commons.common_params import CLUSTER, STANDALONE, Helm, Operator
from workflow.base import Base
from workflow.performance_template import PerfTemplate
from utils.util_log import log
from parameters.input_params import param_info, InputParamsBase
from commons.common_func import update_dict_value
from commons.common_type import DefaultParams as dp


class TestPerformanceCases(PerfTemplate):
    """
    Performance test cases
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
            case_skip_build_index: Optional[bool]
            case_skip_clean_collection: Optional[bool]
        :type input_params: InputParamsBase
        
        :roughly follow the steps:
            1. deployment service or use an already deployed service
            2. connect service and start test
                a. serial test
            3. check test result and report
            4. clean env"""

    def test_recall_scene_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=CLUSTER,
                             case_callable_obj=AccCases().scene_recall)

    @pytest.mark.recall
    @pytest.mark.parametrize("p", ["sift_128_euclidean_hnsw",
                                   "sift_128_euclidean_annoy",
                                   "sift_128_euclidean_flat",
                                   "sift_128_euclidean_ivf_flat",
                                   "sift_128_euclidean_ivf_sq8",
                                   "sift_128_euclidean_ivf_pq"])
    @pytest.mark.parametrize("deploy_mode", [STANDALONE, CLUSTER])
    def test_recall_scene_sift(self, input_params: InputParamsBase, p, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=eval("AccParams().{0}()".format(p)))

    @pytest.mark.recall
    @pytest.mark.parametrize("p", ["glove_200_angular_hnsw",
                                   "glove_200_angular_ivf_flat"])
    @pytest.mark.parametrize("deploy_mode", [STANDALONE, CLUSTER])
    def test_recall_scene_glove(self, input_params: InputParamsBase, p, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=eval("AccParams().{0}()".format(p)))

    # @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/19321")
    @pytest.mark.recall
    @pytest.mark.parametrize("p", ["kosarak_27983_jaccard_bin_ivf_flat"])
    @pytest.mark.parametrize("deploy_mode", [STANDALONE, CLUSTER])
    def test_recall_scene_jaccard(self, input_params: InputParamsBase, p, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=eval("AccParams().{0}()".format(p)))

    def test_batch_insert_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. batch insert and calculation of insert time
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=CLUSTER,
                             case_callable_obj=InsertBatch().scene_insert_batch)

    @pytest.mark.insert
    @pytest.mark.parametrize("deploy_mode", [STANDALONE, CLUSTER])
    def test_batch_insert_time(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. batch insert and calculation of insert time
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=InsertBatch().scene_insert_batch,
                             default_case_params=InsertBatchParams().params_insert_batch())

    def test_build_index_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. insert and calculation of build index time
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=CLUSTER,
                             case_callable_obj=BuildIndex().scene_build_index)

    @pytest.mark.index
    @pytest.mark.parametrize("p", ["params_build_index_hnsw",
                                   "params_build_index_ivf_flat"])
    @pytest.mark.parametrize("deploy_mode", [STANDALONE, CLUSTER])
    def test_build_index_time(self, input_params: InputParamsBase, p, deploy_mode):
        """
        :test steps:
            1. insert and calculation of build index time
        """
        self.serial_template(input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
                             case_callable_obj=BuildIndex().scene_build_index,
                             default_case_params=eval("BuildIndexParams().{0}()".format(p)))

    def test_load_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. insert and calculation of load time
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=CLUSTER,
                             case_callable_obj=Load().scene_load)

    @pytest.mark.load
    @pytest.mark.parametrize("deploy_mode", [STANDALONE, CLUSTER])
    def test_load_time(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of load time
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=Load().scene_load, default_case_params=LoadParams().params_load())

    def test_query_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. insert and calculation of query time
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=CLUSTER,
                             case_callable_obj=Query().scene_query_ids)

    @pytest.mark.query
    @pytest.mark.parametrize("p", ["params_scene_query_ids_sift"])
    @pytest.mark.parametrize("deploy_mode", [STANDALONE, CLUSTER])
    def test_query_by_ids_sift_time(self, input_params: InputParamsBase, p, deploy_mode):
        """
        :test steps:
            1. insert and calculation of query time
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=Query().scene_query_ids,
                             default_case_params=eval("QueryParams().{0}()".format(p)))

    # @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/19102")
    @pytest.mark.query
    @pytest.mark.parametrize("p", ["params_scene_query_ids_local"])
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_query_by_ids_local_time(self, input_params: InputParamsBase, p, deploy_mode):
        """
        :test steps:
            1. insert and calculation of query time
        """
        self.serial_template(input_params=input_params, cpu=32, mem=128, deploy_mode=deploy_mode,
                             case_callable_obj=Query().scene_query_ids,
                             default_case_params=eval("QueryParams().{0}()".format(p)))

    def test_search_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. insert and calculation of search time
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=CLUSTER,
                             case_callable_obj=Search().scene_search)

    @pytest.mark.search
    @pytest.mark.parametrize("deploy_mode", [STANDALONE, CLUSTER])
    def test_search_time(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of search time
        """
        self.serial_template(input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
                             case_callable_obj=Search().scene_search,
                             default_case_params=SearchParams().params_scene_search())
