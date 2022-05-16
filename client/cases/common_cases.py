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
    get_vectors_from_binary, parser_search_params_expr
from utils.util_log import log
from client.util.params_check import check_params


class CommonCases(Base):
    def __init__(self):
        super().__init__()
        self.params_obj = ParamsBase()
        self.case_report = CasesReport()

    def parsing_params(self, params):
        if not isinstance(params, dict):
            log.error("[CommonCases] Params({}) do not match to dict.".format(type(params)))
        self.params_obj = ParamsBase(**params)

    def prepare_collection(self, vector_field_name, prepare, prepare_clean=True):
        self.connect()

        if prepare:
            self.clean_all_collection(clean=prepare_clean)

            # create collection
            _collection_params = update_dict_value({
                pn.vector_field_name: vector_field_name,
                pn.dim: self.params_obj.dataset_params[pn.dim]
            }, self.params_obj.collection_params)
            self.create_collection(**_collection_params)

        else:
            collection_names = self.utility_wrap.list_collections()[0][0]
            if len(collection_names) == 0 or len(collection_names) > 1:
                msg = "[CommonCases] There can only be one collection in the database: {}".format(collection_names)
                log.error(msg)
                raise Exception(msg)

            self.connect_collection(collection_names[0])
        self.get_collection_schema()
        log.info("[CommonCases] Prepare collection {0} done.".format(self.collection_wrap.name))

    def prepare_insert(self, data_type, dim, size, ni):
        res_insert = self.insert(data_type=data_type, dim=dim, size=size, ni=ni)
        self.case_report.add_attr(**res_insert)

    def prepare_load(self, **kwargs):
        res_load = self.load_collection(**kwargs)
        self.case_report.add_attr(**{"load": {"load_RT": round(res_load[0][1], Precision.LOAD_PRECISION)}})

    def prepare_index(self, vector_field_name, metric_type, clean_index_before=False):
        self.show_index()
        if clean_index_before:
            self.clean_index()

        if self.params_obj.index_params != {}:
            _index_params = update_dict_value({
                pn.field_name: vector_field_name,
                pn.metric_type: metric_type,
            }, self.params_obj.index_params)

            result, check_result = self.build_index(**_index_params)
            rt = round(result[1], Precision.INDEX_PRECISION)
            # set report data
            self.case_report.add_attr(**{"index": {"build_RT": rt}})

            log.info(
                "[CommonCases] RT of build index {1}: {0}s".format(rt, self.params_obj.index_params[pn.index_type]))
            self.show_index()
            log.info("[CommonCases] Prepare index {0} done.".format(self.params_obj.index_params[pn.index_type]))

    def prepare_query(self, **kwargs):
        res_query = self.query(**kwargs)
        self.case_report.add_attr(**{"query": {"query_RT": round(res_query[0][1], Precision.QUERY_PRECISION)}})

    def prepare_search(self, req_run_counts, **kwargs):
        search_rt = []
        for i in range(req_run_counts):
            res_search = self.search(**kwargs)
            search_rt.append(round(res_search[0][1], Precision.SEARCH_PRECISION))

        self.case_report.add_attr(**{"search": {"RT": round(float(np.mean(search_rt)), Precision.SEARCH_PRECISION),
                                                "MinRT": round(float(np.min(search_rt)), Precision.SEARCH_PRECISION),
                                                "MaxRT": round(float(np.max(search_rt)), Precision.SEARCH_PRECISION),
                                                "TP99": round(np.percentile(search_rt, 99), Precision.SEARCH_PRECISION),
                                                "TP95": round(np.percentile(search_rt, 95), Precision.SEARCH_PRECISION)}})
        return self.case_report.to_dict(), True

    def parser_search_params(self):
        return gen_combinations(self.params_obj.search_params_parser(self.params_obj.search_params))

    def search_param_analysis(self, _search_params: dict, default_field_name: str, metric_type: str):
        _params = copy.deepcopy(_search_params)
        nq = _params.pop(pn.nq)
        top_k = _params.pop(pn.top_k)
        search_param = _params.pop(pn.search_param)
        expr = _params.pop(pn.expr)

        data = get_vectors_from_binary(nq=nq, dimension=self.params_obj.dataset_params[pn.dim],
                                       dataset_name=self.params_obj.dataset_params[pn.dataset_name])
        limit = top_k

        result = update_dict_value({
            "data": data,
            "anns_field": default_field_name,
            "param": update_dict_value({"params": search_param}, {"metric_type": metric_type}),
            "limit": limit,
            "expr": parser_search_params_expr(expr),
        }, _params)
        return result, nq, top_k, expr


class InsertBatch(CommonCases):

    @check_params(ParamsFormat.common_scene_insert_batch)
    def scene_insert_batch(self, **kwargs):
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

        # params parsing
        self.parsing_params(params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(vector_type)
        ni_per = self.params_obj.dataset_params[pn.ni_per]

        def run(ni):
            try:
                self.prepare_collection(vector_default_field_name, prepare, prepare_clean)
                self.prepare_insert(data_type=self.params_obj.dataset_params[pn.dataset_name],
                                    dim=self.params_obj.dataset_params[pn.dim],
                                    size=self.params_obj.dataset_params[pn.dataset_size], ni=ni)
                self.count_entities()
                return self.case_report.to_dict(), True
            except Exception as e:
                log.error("[InsertBatch] Insert batch raise error: {}".format(e))
                return {}, False

        params_list = []
        for i in ni_per:
            actual_params_used = update_dict_value({pn.dataset_params: {pn.ni_per: i}}, params)
            p = CaseIterParams(callable_object=run, object_args=[i],
                               actual_params_used=actual_params_used, case_type=self.__class__.__name__)
            params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=clean_collection)
        yield True


class BuildIndex(CommonCases):

    @check_params(ParamsFormat.common_scene_build_index)
    def scene_build_index(self, **kwargs):
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

        # params parsing
        self.parsing_params(params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(vector_type)

        # prepare data
        self.prepare_collection(vector_default_field_name, prepare, prepare_clean)
        if prepare is True:
            self.prepare_insert(data_type=self.params_obj.dataset_params[pn.dataset_name],
                                dim=self.params_obj.dataset_params[pn.dim],
                                size=self.params_obj.dataset_params[pn.dataset_size],
                                ni=self.params_obj.dataset_params[pn.ni_per])
        self.count_entities()

        # build index
        def run():
            try:
                self.prepare_index(vector_field_name=vector_default_field_name,
                                   metric_type=self.params_obj.dataset_params[pn.metric_type],
                                   clean_index_before=True)
                return self.case_report.to_dict(), True
            except Exception as e:
                log.error("[BuildIndex] Build index raise error: {}".format(e))
                return {}, False

        params_list = []
        p = CaseIterParams(callable_object=run, actual_params_used=params, case_type=self.__class__.__name__)
        params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=clean_collection)
        yield True


class Load(CommonCases):

    @check_params(ParamsFormat.common_scene_load)
    def scene_load(self, **kwargs):
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

        # params parsing
        self.parsing_params(params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(vector_type)

        # prepare data
        self.prepare_collection(vector_default_field_name, prepare, prepare_clean)
        if prepare is True:
            self.prepare_insert(data_type=self.params_obj.dataset_params[pn.dataset_name],
                                dim=self.params_obj.dataset_params[pn.dim],
                                size=self.params_obj.dataset_params[pn.dataset_size],
                                ni=self.params_obj.dataset_params[pn.ni_per])
        else:
            self.release_collection()
        self.count_entities()

        # load collection
        def run():
            try:
                self.prepare_load(**self.params_obj.load_params)
                return self.case_report.to_dict(), True
            except Exception as e:
                log.error("[Load] Load raise error: {}".format(e))
                return {}, False

        params_list = []
        p = CaseIterParams(callable_object=run, actual_params_used=params, case_type=self.__class__.__name__)
        params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=clean_collection)
        yield True


class Query(CommonCases):

    @check_params(ParamsFormat.common_scene_query_ids)
    def scene_query_ids(self, **kwargs):
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

        # params parsing
        self.parsing_params(params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(vector_type)

        # prepare data
        self.prepare_collection(vector_default_field_name, prepare, prepare_clean)
        if prepare is True:
            self.prepare_insert(data_type=self.params_obj.dataset_params[pn.dataset_name],
                                dim=self.params_obj.dataset_params[pn.dim],
                                size=self.params_obj.dataset_params[pn.dataset_size],
                                ni=self.params_obj.dataset_params[pn.ni_per])
        else:
            self.release_collection()
        self.count_entities()

        # load collection
        self.prepare_load(**self.params_obj.load_params)

        # query
        def run():
            try:
                self.prepare_query(**self.params_obj.query_params)
                return self.case_report.to_dict(), True
            except Exception as e:
                log.error("[Query] Query raise error: {}".format(e))
                return {}, False

        params_list = []
        p = CaseIterParams(callable_object=run, actual_params_used=params, case_type=self.__class__.__name__)
        params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=clean_collection)
        yield True


class Search(CommonCases):

    @check_params(ParamsFormat.common_scene_search)
    def scene_search(self, **kwargs):
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
            self.count_entities()
            self.prepare_index(vector_field_name=vector_default_field_name,
                               metric_type=self.params_obj.dataset_params[pn.metric_type])
        else:
            self.release_collection()

        # load collection
        self.prepare_load(**self.params_obj.load_params)

        # search
        s_params = self.parser_search_params()
        params_list = []
        for s_p in s_params:
            search_params, nq, top_k, expr = self.search_param_analysis(s_p, vector_default_field_name,
                                                                        self.params_obj.dataset_params[pn.metric_type])

            actual_params_used = copy.deepcopy(params)
            actual_params_used[pn.search_params] = {
                pn.nq: nq,
                "param": search_params["param"],
                pn.top_k: top_k,
                pn.expr: expr
            }
            p = CaseIterParams(callable_object=self.prepare_search,
                               object_args=[self.params_obj.dataset_params[pn.req_run_counts]],
                               object_kwargs=search_params,
                               actual_params_used=actual_params_used, case_type=self.__class__.__name__)
            params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=clean_collection)
        yield True
