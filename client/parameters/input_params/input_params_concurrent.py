from client.parameters.input_params.input_params_common import CommonParams
from client.parameters import params_name as pn
from client.common.common_func import dict_recursive_key, parser_data_size, update_dict_value
from client.common.common_param import MetricsToIndexType
import client.parameters.input_params.define_params as dp
from utils.util_log import log


class GoBenchParams(CommonParams):
    @staticmethod
    def go_base(concurrent_number, during_time, interval):
        go_search_params = {pn.concurrent_number: concurrent_number,
                            pn.during_time: during_time,
                            pn.interval: interval
                            }

        return dict_recursive_key({
            pn.go_search_params: go_search_params,
        })

    def params_scene_go_search(self, dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="1m", ni_per=50000,
                               other_fields=[], metric_type=pn.MetricsTypeName.L2,
                               index_type=pn.IndexTypeName.HNSW, index_param={"M": 8, "efConstruction": 200},
                               top_k=[1], nq=[1], search_param={"ef": [8, 32]}, search_expr=None,
                               concurrent_number=[10, 200], during_time=120, interval=20):
        dataset_size = parser_data_size(dataset_size)

        _search_expr = []
        if isinstance(search_expr, list):
            for s in search_expr:
                _search_expr.append(eval(s))
        elif isinstance(search_expr, str):
            _search_expr.append(eval(search_expr))
        elif search_expr is None:
            _search_expr = search_expr
        else:
            raise Exception("[GoBenchParams] search_expr is not: List[str], check search_expr: {0}".format(search_expr))

        base_default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                        other_fields=other_fields, metric_type=metric_type, index_type=index_type,
                                        index_param=index_param, top_k=top_k, nq=nq, search_param=search_param,
                                        search_expr=_search_expr)
        go_default_params = self.go_base(concurrent_number=concurrent_number, during_time=during_time,
                                         interval=interval)
        default_params = update_dict_value(go_default_params, base_default_params)
        log.debug("[GoBenchParams] Default params of params_scene_go_search: {0}".format(default_params))
        return default_params
