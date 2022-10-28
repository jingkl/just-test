import sys
import numpy as np
import copy

from client.cases.base import Base
from client.cases.case_report import CasesReport
from client.parameters.params import ParamsFormat, ParamsBase
from client.parameters import params_name as pn
from client.common.common_type import Precision, CaseIterParams
from client.common.common_func import get_source_file, read_ann_hdf5_file, normalize_data, get_acc_metric_type, \
    gen_combinations, update_dict_value, get_vector_type, get_default_field_name, get_search_ids, get_recall_value, \
    get_vectors_from_binary, parser_search_params_expr, go_bench, GoSearchParams
from utils.util_log import log
from client.util.params_check import check_params
from client.cases.common_cases import CommonCases


class GoBenchCases(CommonCases):

    def __str__(self):
        return """
        1. create a collection or use an existing collection
        2. build index on vector column
        3. insert a certain number of vectors
        4. flush collection
        5. build index on vector column with the same parameters
        6. build index on on scalars column or not
        7. count the total number of rows
        8. load collection
        9. call the go program to perform search concurrent operations
        10. clean all collections or not
        """

    def prepare_go_search(self, index_type: str, go_search_params: GoSearchParams, concurrent_number: int,
                          during_time: int, interval=20):
        res_go = self.go_search(index_type=index_type, go_search_params=go_search_params,
                                concurrent_number=concurrent_number, during_time=during_time, interval=interval)
        self.case_report.add_attr(**{"go_bench": res_go})
        result_check = True if "response" in res_go and res_go["response"] is True else False
        return self.case_report.to_dict(), result_check

    def parser_go_search_params(self):
        return gen_combinations(self.params_obj.go_search_params)

    @check_params(ParamsFormat.common_scene_go_search)
    def scene_go_search(self, **kwargs):
        """
        :param kwargs:
            params: dict
            prepare: bool
            prepare_clean: bool
            clean_collection: bool
        :return:
        """

        # params prepare
        params = kwargs.get("params", None)
        prepare = kwargs.get("prepare", True)
        prepare_clean = kwargs.get("prepare_clean", True)
        clean_collection = kwargs.get("clean_collection", True)
        log.info("[GoBenchCases] The detailed test steps are as follows: {}".format(self))

        # params parsing
        self.parsing_params(params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(vector_type)

        # prepare data
        self.prepare_collection(vector_default_field_name, prepare, prepare_clean)
        if prepare is True:
            self.prepare_index(vector_field_name=vector_default_field_name,
                               metric_type=self.params_obj.dataset_params[pn.metric_type],
                               clean_index_before=True)
            self.prepare_insert(data_type=self.params_obj.dataset_params[pn.dataset_name],
                                dim=self.params_obj.dataset_params[pn.dim],
                                size=self.params_obj.dataset_params[pn.dataset_size],
                                ni=self.params_obj.dataset_params[pn.ni_per])
            self.prepare_flush()
            self.count_entities()
            self.prepare_index(vector_field_name=vector_default_field_name,
                               metric_type=self.params_obj.dataset_params[pn.metric_type])
            self.prepare_scalars_index()
        else:
            self.release_collection()

        # load collection
        self.prepare_load(**self.params_obj.load_params)

        # search
        s_params = self.parser_search_params()
        g_params = self.parser_go_search_params()
        params_list = []
        for g_p in g_params:
            for s_p in s_params:
                search_params, nq, top_k, expr = self.search_param_analysis(s_p, vector_default_field_name,
                                                                            self.params_obj.dataset_params[
                                                                                pn.metric_type])
                go_search_params = GoSearchParams(dim=self.params_obj.dataset_params[pn.dim], **search_params)

                actual_params_used = copy.deepcopy(params)
                actual_params_used[pn.search_params] = {
                    pn.nq: nq,
                    "param": search_params["param"],
                    pn.top_k: top_k,
                    pn.expr: expr
                }
                actual_params_used[pn.go_search_params] = {
                    pn.concurrent_number: g_p[pn.concurrent_number],
                    pn.during_time: g_p[pn.during_time],
                    pn.interval: g_p[pn.interval]
                }
                p = CaseIterParams(callable_object=self.prepare_go_search,
                                   object_args=[self.params_obj.index_params[pn.index_type], go_search_params],
                                   object_kwargs=actual_params_used[pn.go_search_params],
                                   actual_params_used=actual_params_used, case_type=self.__class__.__name__)
                params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=clean_collection)
        yield True
