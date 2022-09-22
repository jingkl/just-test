from client.parameters import params_name as pn
from client.common.common_func import dict_recursive_key, parser_data_size
from client.common.common_param import MetricsToIndexType
import client.parameters.input_params.define_params as dp
from utils.util_log import log


class CommonParams:

    @staticmethod
    def base(dataset_name, dim, dataset_size, ni_per, metric_type=None, req_run_counts=None,
             other_fields=None,
             replica_number=None,
             index_type=None, index_param=None,
             ids=None, query_expr=None,
             search_param=None, search_expr=None, top_k=None, nq=None, guarantee_timestamp=None):
        dataset_params = {pn.dataset_name: dataset_name,
                          pn.dim: dim,
                          pn.dataset_size: dataset_size,
                          pn.ni_per: ni_per,
                          pn.metric_type: metric_type,
                          pn.req_run_counts: req_run_counts}
        collection_params = {pn.other_fields: other_fields}
        load_params = {pn.replica_number: replica_number}
        index_params = {pn.index_type: index_type,
                        pn.index_param: index_param}
        query_params = {pn.ids: ids,
                        pn.expr: query_expr}
        search_params = {pn.top_k: top_k,
                         pn.nq: nq,
                         pn.search_param: search_param,
                         pn.expr: search_expr,
                         pn.guarantee_timestamp: guarantee_timestamp,
                         # "guarantee_timestamp": 1,
                         # "expr": ["float1 > -1 && float1 < 10", "float1 > 0 && float1 < 20"],
                         }

        return dict_recursive_key({
            pn.dataset_params: dataset_params,
            pn.collection_params: collection_params,
            pn.load_params: load_params,
            pn.index_params: index_params,
            pn.query_params: query_params,
            pn.search_params: search_params,
        })


class InsertBatchParams(CommonParams):
    def params_insert_batch(self, dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="1m", ni_per=None):
        if not isinstance(ni_per, list) or ni_per is None:
            ni_per = [500, 1000, 2000, 5000, 10000, 20000, 25000, 40000, 50000, 100000, 125000, 200000, 250000, 500000]

        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per)
        log.debug("[InsertBatchParams] Default params of params_insert_batch: {0}".format(default_params))
        return default_params


class BuildIndexParams(CommonParams):
    def params_build_index_ivf_flat(self, dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="50m", ni_per=50000,
                                    metric_type=pn.MetricsTypeName.L2, index_type=pn.IndexTypeName.IVF_FLAT,
                                    index_param={"nlist": 2048}):
        if not hasattr(pn.MetricsTypeName, metric_type) or index_type not in MetricsToIndexType[metric_type]:
            raise Exception("[BuildIndexParams] index_type:{0} not support metric_type:{1}".format(index_type,
                                                                                                   metric_type))

        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                   metric_type=metric_type, index_type=index_type, index_param=index_param)
        log.debug("[BuildIndexParams] Default params of params_build_index_ivf_flat: {0}".format(default_params))
        return default_params

    def params_build_index_hnsw(self, dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="50m", ni_per=50000,
                                metric_type=pn.MetricsTypeName.L2, index_type=pn.IndexTypeName.HNSW,
                                index_param={"M": 16, "efConstruction": 500}):
        if not hasattr(pn.MetricsTypeName, metric_type) or index_type not in MetricsToIndexType[metric_type]:
            raise Exception("[BuildIndexParams] index_type:{0} not support metric_type:{1}".format(index_type,
                                                                                                   metric_type))

        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                   metric_type=metric_type, index_type=index_type, index_param=index_param)
        log.debug("[BuildIndexParams] Default params of params_build_index_hnsw: {0}".format(default_params))
        return default_params


class LoadParams(CommonParams):
    def params_load(self, dataset_name=pn.DatasetsName.LOCAL, dim=768, dataset_size="1m", ni_per=50000):
        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per)
        log.debug("[LoadParams] Default params of params_load: {0}".format(default_params))
        return default_params


class QueryParams(CommonParams):
    def params_scene_query_ids_local(self, dataset_name=pn.DatasetsName.LOCAL, dim=512, dataset_size="50m",
                                     ni_per=50000, ids=[1, 100, 10000], req_run_counts=10):
        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                   ids=ids, req_run_counts=req_run_counts)
        log.debug("[QueryByIdsParams] Default params of params_scene_query_ids_local: {0}".format(default_params))
        return default_params

    def params_scene_query_ids_sift(self, dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="1m",
                                    ni_per=50000, ids=[1, 100, 10000], req_run_counts=10):
        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                   ids=ids, req_run_counts=req_run_counts)
        log.debug("[QueryByIdsParams] Default params of params_scene_query_ids_sift: {0}".format(default_params))
        return default_params


class SearchParams(CommonParams):
    def params_scene_search(self, dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="50m", ni_per=50000,
                            other_fields=dp.other_fields, metric_type=pn.MetricsTypeName.L2,
                            index_type=pn.IndexTypeName.IVF_FLAT, index_param={"nlist": 2048},
                            top_k=[1, 10, 100, 1000], nq=[1, 10, 100, 200, 500, 1000, 1200],
                            search_param={"nprobe": [8, 32]}, search_expr=dp.search_expr, req_run_counts=10):
        dataset_size = parser_data_size(dataset_size)

        _search_expr = []
        if isinstance(search_expr, list):
            for s in search_expr:
                _search_expr.append(eval(s))
        elif isinstance(search_expr, str):
            _search_expr.append(eval(search_expr))
        else:
            raise Exception("[SearchParams] search_expr is not: List[str], check search_expr: {0}".format(search_expr))

        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                   other_fields=other_fields, metric_type=metric_type, index_type=index_type,
                                   index_param=index_param, top_k=top_k, nq=nq, search_param=search_param,
                                   search_expr=_search_expr, req_run_counts=req_run_counts)
        log.debug("[SearchParams] Default params of params_scene_search: {0}".format(default_params))
        return default_params
