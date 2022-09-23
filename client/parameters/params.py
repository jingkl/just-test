import copy
from dataclasses import dataclass
from typing import Optional
from client.common.common_func import gen_combinations, update_dict_value
from client.parameters.params_name import *
from utils.util_log import log


@dataclass
class ParamsBase:
    dataset_params: Optional[dict] = dict
    collection_params: Optional[dict] = dict
    load_params: Optional[dict] = dict
    index_params: Optional[dict] = dict
    search_params: Optional[dict] = dict
    query_params: Optional[dict] = dict
    go_search_params: Optional[dict] = dict
    concurrent_params: Optional[dict] = dict

    @staticmethod
    def search_params_parser(_params):
        _p = copy.deepcopy(_params)
        if search_param in _p:
            _p[search_param] = gen_combinations(_p[search_param])
        return _p


class ParamsFormat:
    base = {
        dataset_params: {dim: ([type(int())], OPTION),
                         max_length: ([type(int())], OPTION),
                         varchar_filled: ([type(bool())], OPTION),
                         scalars_index: ([type(list())], OPTION)},
        collection_params: {other_fields: ([type(list())], OPTION),
                            shards_num: ([type(int())], OPTION)},
        load_params: {replica_number: ([type(int())], OPTION)},
        search_params: {
            expr: ([type(str()), type(list()), type(None)], OPTION),
            guarantee_timestamp: ([type(int()), type(list())], OPTION),
        }
    }

    acc_scene_recall = update_dict_value({
        dataset_params: {dataset_name: ([type(str())], MUST),
                         ni_per: ([type(int()), type(str())], OPTION)},
        index_params: {index_type: ([type(str())], MUST),
                       index_param: ([type(dict())], MUST)},
        search_params: {top_k: ([type(int()), type(list())], MUST),
                        nq: ([type(int()), type(list())], MUST),
                        search_param: ([type(dict())], MUST),
                        },
    }, base)

    common_scene_insert_batch = update_dict_value({
        dataset_params: {dataset_name: ([type(str())], MUST),
                         dim: ([type(int())], MUST),
                         dataset_size: ([type(str()), type(int())], MUST),
                         ni_per: ([type(list())], MUST)},
    }, base)

    common_scene_build_index = update_dict_value({
        dataset_params: {dataset_name: ([type(str())], MUST),
                         dim: ([type(int())], MUST),
                         dataset_size: ([type(str()), type(int())], MUST),
                         ni_per: ([type(int()), type(str())], MUST),
                         metric_type: ([type(str())], MUST)},
        index_params: {index_type: ([type(str())], MUST),
                       index_param: ([type(dict())], MUST)},
    }, base)

    common_scene_load = update_dict_value({
        dataset_params: {dataset_name: ([type(str())], MUST),
                         dim: ([type(int())], MUST),
                         dataset_size: ([type(str()), type(int())], MUST),
                         ni_per: ([type(int()), type(str())], MUST),
                         metric_type: ([type(str())], MUST)},
        index_params: {index_type: ([type(str())], MUST),
                       index_param: ([type(dict())], MUST)},
    }, base)

    common_scene_query_ids = update_dict_value({
        dataset_params: {req_run_counts: ([type((int()))], MUST)},
        query_params: {ids: ([type(list())], MUST)}
    }, common_scene_load)

    common_scene_query_expr = update_dict_value({
        dataset_params: {req_run_counts: ([type((int()))], MUST)},
        query_params: {expression: ([type(str())], MUST)}
    }, common_scene_load)

    common_scene_search = update_dict_value({
        dataset_params: {req_run_counts: ([type((int()))], MUST)},
        search_params: {top_k: ([type(int()), type(list())], MUST),
                        nq: ([type(int()), type(list())], MUST),
                        search_param: ([type(dict())], MUST),
                        },
        index_params: {index_type: ([type(str())], OPTION),
                       index_param: ([type(dict())], OPTION)},
    }, common_scene_build_index)

    common_scene_go_search = update_dict_value({
        search_params: {top_k: ([type(int()), type(list())], MUST),
                        nq: ([type(int()), type(list())], MUST),
                        search_param: ([type(dict())], MUST),
                        },
        go_search_params: {concurrent_number: ([type((int())), type(list())], MUST),
                           during_time: ([type((int()))], MUST),
                           interval: ([type((int()))], MUST)}
    }, common_scene_build_index)
