import numpy as np
import copy
import time

from client.cases.base import Base
from client.cases.case_report import CasesReport
from client.common.common_func import get_source_file, read_ann_hdf5_file, normalize_data, get_acc_metric_type, \
    gen_combinations, update_dict_value, get_vector_type, get_default_field_name, get_search_ids, get_recall_value
from client.common.common_type import Precision, CaseIterParams
from client.parameters import params_name as pn
from client.parameters.params import ParamsFormat, ParamsBase
from client.util.params_check import check_params
from utils.util_log import log


class CommonCases(Base):
    """
    neighbors: used to compare with search results, topk <= columns(100), nq <= rows(10000)
    test: vector argument for search
    train: vector to insert into database
    distances: dis between neighbors and test
    """

    dataset_neighbors = []
    dataset_test = []
    dataset_train = []
    dataset_distances = []

    def __init__(self):
        super().__init__()
        self.params_obj = ParamsBase()
        self.case_report = CasesReport()

    def parsing_file(self, file_name):
        src_file = get_source_file(file_name)
        data_set = read_ann_hdf5_file(src_file)
        metric_type = get_acc_metric_type(file_name)
        vector_type = get_vector_type(file_name.split('-')[0])

        self.dataset_neighbors = np.array(data_set[pn.neighbors])
        self.dataset_test = normalize_data(metric_type, np.array(data_set[pn.test]))
        self.dataset_train = normalize_data(metric_type, np.array(data_set[pn.train]))
        if len(self.dataset_train) != data_set["train"].shape[0]:
            raise Exception("[AccCases] Row count of insert vectors: %d is not equal to dataset size: %d" % (
                len(self.dataset_train), data_set["train"].shape[0]))
        return metric_type, vector_type

    def parsing_params(self, params):
        if not isinstance(params, dict):
            log.error("[AccCases] Params({}) do not match to dict.".format(type(params)))
        self.params_obj = ParamsBase(**params)

    def prepare_collection(self, metric_type, vector_type, prepare, rebuild_index=False, prepare_clean=True):
        vector_default_field_name = get_default_field_name(vector_type,
                                                           self.params_obj.dataset_params.get(pn.vector_field_name, ""))
        self.connect()

        if prepare:
            self.clean_all_collection(clean=prepare_clean)

            # create collection
            _collection_params = update_dict_value({
                pn.vector_field_name: vector_default_field_name,
                pn.dim: self.params_obj.dataset_params[pn.dim]
            }, self.params_obj.collection_params)
            self.create_collection(**_collection_params)
            self.get_collection_schema()

            # insert vectors
            res_insert = self.ann_insert(source_vectors=self.dataset_train,
                                         ni=self.params_obj.dataset_params[pn.ni_per])
            self.case_report.add_attr(**res_insert)

            # flush collection
            self.flush_collection()

            # build index
            _index_params = update_dict_value({
                pn.field_name: vector_default_field_name,
                pn.metric_type: metric_type
            }, self.params_obj.index_params)
            self.clean_index()

            res_index = self.build_index(**_index_params)
            self.case_report.add_attr(**{"index": {"build_time": round(res_index[0][1], Precision.INDEX_PRECISION)}})

            self.show_index()

            # load collection
            self.load_collection(**self.params_obj.load_params)
        else:
            collection_names = self.utility_wrap.list_collections()[0][0] if not self.params_obj.dataset_params.get(
                pn.collection_name, None) else [self.params_obj.dataset_params[pn.collection_name]]
            if len(collection_names) == 0 or len(collection_names) > 1:
                msg = "[AccCases] There can only be one collection in the database: {}".format(collection_names)
                log.error(msg)
                raise Exception(msg)

            self.connect_collection(collection_names[0])
            self.get_collection_schema()
            self.show_index()

            if rebuild_index is True:
                _index_params = update_dict_value({
                    pn.field_name: vector_default_field_name,
                    pn.metric_type: metric_type
                }, self.params_obj.index_params)
                self.clean_index()

                res_index = self.build_index(**_index_params)
                self.case_report.add_attr(
                    **{"index": {"build_time": round(res_index[0][1], Precision.INDEX_PRECISION)}})

                self.show_index()

            self.collection_wrap.release()
            self.load_collection(**self.params_obj.load_params)

        counts = self.collection_wrap.num_entities
        log.info("[AccCases] Number of vectors in the collection({0}): {1}".format(self.collection_wrap.name, counts))

    def parser_search_params(self):
        search_params = copy.deepcopy(self.params_obj.search_params_parser(self.params_obj.search_params))
        s_p = gen_combinations({pn.top_k: search_params.pop(pn.top_k, 0),
                                pn.nq: search_params.pop(pn.nq, 0),
                                pn.search_param: search_params.pop(pn.search_param, {}),
                                pn.expr: search_params.pop(pn.expr, None)})
        search_params_list = []
        for s in s_p:
            s.update(search_params)
            search_params_list.append(s)
        return search_params_list
        # return gen_combinations(self.params_obj.search_params_parser(self.params_obj.search_params))

    def search_param_analysis(self, _search_params: dict, vector_type, metric_type: str):
        _params = copy.deepcopy(_search_params)
        nq = _params.pop(pn.nq)
        top_k = _params.pop(pn.top_k)
        search_param = _params.pop(pn.search_param)
        if nq > len(self.dataset_test):
            raise Exception("[AccCases] nq large than file support: {0}".format(len(self.dataset_test)))

        data = self.dataset_test[:nq]
        limit = top_k

        result = update_dict_value({
            "data": data,
            "anns_field": get_default_field_name(vector_type,
                                                 self.params_obj.dataset_params.get(pn.vector_field_name, "")),
            "param": update_dict_value({"params": search_param}, {"metric_type": metric_type}),
            "limit": limit,
        }, _params)
        return result, nq, top_k

    def search_recall(self, _search_params, vector_type, metric_type: str):
        _params, nq, top_k = self.search_param_analysis(_search_params, vector_type, metric_type)

        try:
            log.info("[AccCases] Params of search: {}".format(_params))
            start_time = time.time()
            end_time = start_time + 500

            cnt = 0
            search_rt = []
            while cnt < 100 and start_time < end_time:
                res_search = self.search(**_params)
                search_rt.append(round(res_search[0][1], Precision.SEARCH_PRECISION))
                cnt += 1
                start_time = time.time()

            result, check_result = self.search(**_params)
            rt = result[1]
            search_rt.append(rt)

            result_ids = get_search_ids(result[0])
            acc_value = get_recall_value(self.dataset_neighbors[:nq, :top_k].tolist(), result_ids)

            search_res = {"Recall": acc_value,
                          "RT": round(float(np.mean(search_rt)), Precision.SEARCH_PRECISION),
                          "LastRT": round(rt, Precision.SEARCH_PRECISION),
                          "MinRT": round(float(np.min(search_rt)), Precision.SEARCH_PRECISION),
                          "MaxRT": round(float(np.max(search_rt)), Precision.SEARCH_PRECISION)
                          }
            self.case_report.add_attr(**{"search": search_res})

            log.info("[AccCases] Search result:{0}".format(search_res))
            return self.case_report.to_dict(), True

        except Exception as e:
            log.error("[AccCases] Search raise error: {}".format(e))
            return {}, False


class AccCases(CommonCases):

    def __str__(self):
        return """
        1. create a collection or use an existing collection
        2. insert training dataset
        3. flush collection
        4. clean index and build new index
        5. load collection
        6. search with different parameters
        7. clean all collections or not
        """

    @check_params(ParamsFormat.acc_scene_recall)
    def scene_recall(self, **kwargs):
        """
        :param kwargs:
            params: dict
            prepare: bool
            prepare_clean: bool
            rebuild_index: bool
            clean_collection: bool
        :return:
        """
        # params prepare
        params = kwargs.get("params", None)
        prepare = kwargs.get("prepare", True)
        prepare_clean = kwargs.get("prepare_clean", True)
        rebuild_index = kwargs.get("rebuild_index", True)
        clean_collection = kwargs.get("clean_collection", True)
        log.info("[AccCases] The detailed test steps are as follows: {}".format(self))

        # file parsing
        self.parsing_params(params)
        dataset_file_name = params[pn.dataset_params][pn.dataset_name]
        metric_type, vector_type = self.parsing_file(dataset_file_name)

        # prepare collection
        self.prepare_collection(metric_type, vector_type, prepare, rebuild_index, prepare_clean)

        # set test params
        s_params = self.parser_search_params()
        params_list = []
        for s_p in s_params:
            actual_params_used = update_dict_value({pn.search_params: s_p}, params)
            p = CaseIterParams(callable_object=self.search_recall, object_args=[s_p, vector_type, metric_type],
                               actual_params_used=actual_params_used, case_type=self.__class__.__name__)
            params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=clean_collection)
        yield True
