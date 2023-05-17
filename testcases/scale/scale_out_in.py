import copy
import time

from client.cases import InsertBatch, BuildIndex, Search
from client.parameters.input_params import BuildIndexParams, InsertBatchParams, SearchParams
from parameters.input_params import InputParamsBase
from utils.util_log import log
from workflow.scale_template import ScaleTemplate
from deploy.commons.common_params import CLUSTER


class TestScaleOutInCases(ScaleTemplate):
    """
    Scale-out-in test cases
    Author: yufen.zong@zilliz.com
    """

    def __str__(self):
        return """
        :param input_params: Input parameters
            deploy_tool: Optional[str] = Helm
            deploy_mode: Optional[str] = ""
            deploy_config: Union[str, dict] = str
            upgrade_config: Optional[Union[str, dict]] = str
            case_params: Union[str, dict] = str
            case_skip_prepare: Optional[bool] = False
            case_skip_prepare_clean: Optional[bool] = False
            case_rebuild_index: Optional[bool] = False
            case_skip_clean_collection: Optional[bool] = False
        :type input_params: InputParamsBase
        
        :roughly follow the steps:
            1. deployment service or use an already deployed service
            2. milvus test before scale
            3. Scale-out or scale-in service
            4. milvus test after scale
            5. clean env"""

    def test_scale_in_indexnode_serial(self, input_params: InputParamsBase):
        """
        :test steps:
            1. deploy and build index
            2. upgrade server: scale-in indexnode
            3. search test
        """
        input_params = copy.deepcopy(input_params)

        # before scale: deploy and test
        input_params.case_skip_clean_collection = True
        self.serial_template(input_params=input_params, cpu=1, mem=2, deploy_mode=CLUSTER,
                             case_callable_obj=BuildIndex().scene_build_index,
                             default_case_params=BuildIndexParams().params_build_index_ivf_flat(dataset_size="1m",
                                                                                                index_param={
                                                                                                    "nlist": 128}))

        # scale and test
        input_params.case_skip_prepare = True
        input_params.case_skip_prepare_clean = True
        self.scale_serial_template(input_params, deploy_mode=CLUSTER, case_callable_after_scale=Search().scene_search,
                                   default_case_params=SearchParams().params_scene_search_ivf_flat(
                                       top_k=[10], nq=[1],
                                       search_param={"nprobe": [32]},
                                       search_expr=None,
                                       req_run_counts=10))

    def test_scale_out_indexnode_parallel(self, input_params: InputParamsBase):
        """
        :test steps:
            1. deploy without client test
            2. build index and scale-out indexnode in-parallel
        """
        input_params = copy.deepcopy(input_params)

        # before scale: deploy
        input_params.case_skip_clean_collection = True
        self.serial_template(input_params=input_params, cpu=1, mem=3, deploy_mode=CLUSTER,
                             case_callable_obj=InsertBatch().scene_insert_batch,
                             default_case_params=InsertBatchParams().params_insert_batch(dataset_size="3m",
                                                                                         ni_per=[50000]))

        # scale and test in parallel
        input_params.case_skip_prepare = True
        input_params.case_skip_prepare_clean = True
        self.scale_parallel_template(input_params, deploy_mode=CLUSTER,
                                     case_callable_during_scale=BuildIndex().scene_build_index,
                                     default_case_params=BuildIndexParams().params_build_index_ivf_flat(
                                         dataset_size="3m",
                                         index_param={
                                             "nlist": 512}),
                                     time_scale_after_callable=30)
