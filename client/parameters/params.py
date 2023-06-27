import copy
import random
from dataclasses import dataclass, field
from typing import Optional, Union, List

from client.common.common_func import (
    gen_combinations, update_dict_value, loop_ids, gen_vectors, get_default_field_name, gen_unique_str)
from client.common.common_type import concurrent_global_params, DefaultValue
from client.parameters.params_name import *


@dataclass
class ParamsBase:
    dataset_params: Optional[dict] = field(default_factory=lambda: {})
    collection_params: Optional[dict] = field(default_factory=lambda: {})
    load_params: Optional[dict] = field(default_factory=lambda: {})
    flush_params: Optional[dict] = field(default_factory=lambda: {})
    index_params: Optional[dict] = field(default_factory=lambda: {})
    search_params: Optional[dict] = field(default_factory=lambda: {})
    query_params: Optional[dict] = field(default_factory=lambda: {})
    go_search_params: Optional[dict] = field(default_factory=lambda: {})
    concurrent_params: Optional[dict] = field(default_factory=lambda: {})
    concurrent_tasks: Optional[list] = field(default_factory=lambda: [])
    resource_groups_params: Optional[dict] = field(default_factory=lambda: {})
    database_user_params: Optional[dict] = field(default_factory=lambda: {})

    @staticmethod
    def search_params_parser(_params):
        _p = copy.deepcopy(_params)
        if search_param in _p:
            _p[search_param] = gen_combinations(_p[search_param])
        return _p


class ParamsFormat:
    base = {
        dataset_params: {
            # collection_name is replaced by collection_name in collection_params
            # collection_name: ([type(str())], OPTION),
            metric_type: ([type(str())], OPTION),
            vector_field_name: ([type(str())], OPTION),
            dim: ([type(int())], OPTION),
            max_length: ([type(int())], OPTION),
            varchar_filled: ([type(bool())], OPTION),
            scalars_index: ([type(list())], OPTION),
            scalars_params: ([type(dict())], OPTION),
            show_resource_groups: ([type(bool())], OPTION),
            show_db_user: ([type(bool())], OPTION)
        },
        collection_params: {other_fields: ([type(list())], OPTION),
                            shards_num: ([type(int())], OPTION),
                            varchar_id: ([type(bool())], OPTION),
                            collection_name: ([type(str())], OPTION)},
        load_params: {replica_number: ([type(int())], OPTION),
                      refresh: ([type(bool())], OPTION),
                      resource_groups: ([type(int()), type(list())], OPTION)},  
        flush_params: {prepare_flush: ([type(bool())], OPTION)},
        query_params: {output_fields: ([type(list()), type(None)], OPTION),
                       ignore_growing: ([type(bool())], OPTION)},
        search_params: {
            expr: ([type(str()), type(list()), type(None)], OPTION),
            guarantee_timestamp: ([type(int())], OPTION),
            output_fields: ([type(list()), type(None)], OPTION),
            ignore_growing: ([type(bool())], OPTION),
            timeout: ([type(int())], OPTION),
        },
        resource_groups_params: {groups: ([type(list()), type(dict()), type(None)], OPTION),
                                 reset: ([type(bool())], OPTION)},
        database_user_params: {reset_rbac: ([type(bool())], OPTION),
                               reset_db: ([type(bool())], OPTION)}
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
        query_params: {expr: ([type(str())], MUST)}
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

    common_scene_search_recall = update_dict_value({
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
                           during_time: ([type((int())), type((str()))], MUST),
                           interval: ([type((int()))], MUST)}
    }, common_scene_build_index)

    common_concurrent = update_dict_value({
        load_params: {prepare_load: ([type(bool())], OPTION)},
        concurrent_params: {concurrent_number: ([type((int())), type(list())], MUST),
                            during_time: ([type((int())), type((str()))], MUST),
                            interval: ([type((int()))], MUST),
                            spawn_rate: ([type((int())), type(None)], OPTION)
                            },
        concurrent_tasks: ([type(list())], MUST)
    }, common_scene_build_index)


# concurrent test parameters

class DataClassBase:
    @property
    def to_dict(self):
        return vars(self)

    @property
    def obj_params(self):
        return self.to_dict


@dataclass
class ConcurrentInputParamsDebug(DataClassBase):
    debug_params: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentTaskDebug(DataClassBase):
    debug_params: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentInputParamsSearch(DataClassBase):
    nq: int
    top_k: int
    search_param: dict
    expr: Optional[str] = None
    guarantee_timestamp: Optional[int] = None
    output_fields: Optional[list] = None
    ignore_growing: Optional[bool] = False
    timeout: Optional[int] = DefaultValue.default_timeout
    random_data: Optional[bool] = False


@dataclass
class ConcurrentTaskSearch(DataClassBase):
    data: list
    anns_field: str
    param: dict
    limit: int
    expr: Optional[str] = None
    guarantee_timestamp: Optional[int] = None
    output_fields: Optional[list] = None
    ignore_growing: Optional[bool] = False
    timeout: Optional[int] = DefaultValue.default_timeout

    # other params
    random_data: Optional[bool] = False

    @property
    def obj_params(self):
        _p = copy.deepcopy(self.to_dict)
        del _p["random_data"]
        if _p["guarantee_timestamp"] is None:
            del _p["guarantee_timestamp"]
        if _p["ignore_growing"] not in [True]:
            del _p["ignore_growing"]
        return _p


@dataclass
class ConcurrentInputParamsQuery(DataClassBase):
    ids: Optional[list] = None
    expr: Optional[str] = None
    output_fields: Optional[list] = None
    ignore_growing: Optional[bool] = False
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentTaskQuery(DataClassBase):
    expr: str
    output_fields: Optional[list] = None
    ignore_growing: Optional[bool] = False
    timeout: Optional[int] = DefaultValue.default_timeout

    @property
    def obj_params(self):
        _p = copy.deepcopy(self.to_dict)
        if _p["ignore_growing"] not in [True]:
            del _p["ignore_growing"]
        return _p


@dataclass
class ConcurrentInputParamsFlush(DataClassBase):
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentTaskFlush(DataClassBase):
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentInputParamsLoad(DataClassBase):
    replica_number: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentTaskLoad(DataClassBase):
    replica_number: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout

@dataclass
class ConcurrentInputParamsUpsert(DataClassBase):
    upsert_number: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout

    # random id or vectors
    random_id: Optional[bool] = False
    random_vector: Optional[bool] = False
    varchar_filled: Optional[bool] = False


@dataclass
class ConcurrentTaskUpsert(DataClassBase):
    dim: int
    upsert_number: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout

    # random id or vectors
    random_id: Optional[bool] = False
    random_vector: Optional[bool] = False
    varchar_filled: Optional[bool] = False

    _loop_ids = None
    fixed_ids = None
    fixed_vectors = None

    def set_params(self):
        self._loop_ids = loop_ids(step=self.upsert_number)
        self.fixed_ids = [k for k in range(self.upsert_number)]
        self.fixed_vectors = gen_vectors(self.upsert_number, self.dim)

    @property
    def get_ids(self):
        if self.random_id:
            _ids = next(self._loop_ids)
            concurrent_global_params.put_data_to_insert_queue(concurrent_global_params.concurrent_insert_ids, _ids)
            return _ids
        concurrent_global_params.put_data_to_insert_queue(
            concurrent_global_params.concurrent_insert_ids, self.fixed_ids)
        return self.fixed_ids

    @property
    def get_vectors(self):
        if self.random_vector:
            return gen_vectors(self.upsert_number, self.dim)
        return self.fixed_vectors




@dataclass
class ConcurrentInputParamsRelease(DataClassBase):
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentTaskRelease(DataClassBase):
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentInputParamsLoadRelease(DataClassBase):
    replica_number: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentTaskLoadRelease(DataClassBase):
    replica_number: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentInputParamsInsert(DataClassBase):
    nb: Optional[int] = 1  # number of batch insert
    timeout: Optional[int] = DefaultValue.default_timeout

    # random id or vectors
    random_id: Optional[bool] = False
    random_vector: Optional[bool] = False
    varchar_filled: Optional[bool] = False


@dataclass
class ConcurrentTaskInsert(DataClassBase):
    dim: int
    nb: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout

    # random id or vectors
    random_id: Optional[bool] = False
    random_vector: Optional[bool] = False
    varchar_filled: Optional[bool] = False

    _loop_ids = None
    fixed_ids = None
    fixed_vectors = None

    def set_params(self):
        self._loop_ids = loop_ids(step=self.nb)
        self.fixed_ids = [k for k in range(self.nb)]
        self.fixed_vectors = gen_vectors(self.nb, self.dim)

    @property
    def get_ids(self):
        if self.random_id:
            _ids = next(self._loop_ids)
            concurrent_global_params.put_data_to_insert_queue(concurrent_global_params.concurrent_insert_ids, _ids)
            return _ids
        concurrent_global_params.put_data_to_insert_queue(
            concurrent_global_params.concurrent_insert_ids, self.fixed_ids)
        return self.fixed_ids

    @property
    def get_vectors(self):
        if self.random_vector:
            return gen_vectors(self.nb, self.dim)
        return self.fixed_vectors

    @property
    def obj_params(self):
        return {"timeout": self.timeout}
    


@dataclass
class ConcurrentInputParamsDelete(DataClassBase):
    delete_length: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentTaskDelete(DataClassBase):
    delete_length: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout

    @property
    def get_ids(self):
        return concurrent_global_params.get_data_from_insert_queue(
            concurrent_global_params.concurrent_insert_ids, self.delete_length)

    @property
    def obj_params(self):
        return {"timeout": self.timeout}


@dataclass
class ConcurrentInputParamsSceneTest(DataClassBase):
    dim: Optional[int] = DefaultValue.default_dim
    data_size: Optional[int] = 3000
    nb: Optional[int] = 3000
    index_type: Optional[str] = IndexTypeName.IVF_SQ8
    index_param: Optional[dict] = field(default_factory=lambda: {'nlist': 2048})
    metric_type: Optional[str] = MetricsTypeName.L2


@dataclass
class ConcurrentTaskSceneTest(DataClassBase):
    dim: Optional[int] = DefaultValue.default_dim
    data_size: Optional[int] = 3000
    nb: Optional[int] = 3000
    index_type: Optional[str] = IndexTypeName.IVF_SQ8
    index_param: Optional[dict] = field(default_factory=lambda: {'nlist': 2048})
    metric_type: Optional[str] = MetricsTypeName.L2
    vector_field_name: Optional[str] = get_default_field_name()


@dataclass
class ConcurrentInputParamsSceneInsertDeleteFlush(DataClassBase):
    insert_length: Optional[int] = 1
    delete_length: Optional[int] = 1

    # random id or vectors
    random_id: Optional[bool] = False
    random_vector: Optional[bool] = False
    varchar_filled: Optional[bool] = False


@dataclass
class ConcurrentTaskSceneInsertDeleteFlush(DataClassBase):
    dim: Optional[int]
    insert_length: Optional[int] = 1
    delete_length: Optional[int] = 1

    # random id or vectors
    random_id: Optional[bool] = False
    random_vector: Optional[bool] = False
    varchar_filled: Optional[bool] = False

    _loop_ids = None
    fixed_ids = None
    fixed_vectors = None

    def set_params(self):
        self._loop_ids = loop_ids(step=self.insert_length)
        self.fixed_ids = [k for k in range(self.insert_length)]
        self.fixed_vectors = gen_vectors(self.insert_length, self.dim)

    @property
    def get_insert_ids(self):
        if self.random_id:
            _ids = next(self._loop_ids)
            concurrent_global_params.put_data_to_insert_queue(
                concurrent_global_params.concurrent_insert_delete_flush, _ids)
            return _ids
        concurrent_global_params.put_data_to_insert_queue(
            concurrent_global_params.concurrent_insert_delete_flush, self.fixed_ids)
        return self.fixed_ids

    @property
    def get_vectors(self):
        if self.random_vector:
            return gen_vectors(self.insert_length, self.dim)
        return self.fixed_vectors

    @property
    def get_delete_ids(self):
        return concurrent_global_params.get_data_from_insert_queue(
            concurrent_global_params.concurrent_insert_delete_flush, self.delete_length)


@dataclass
class ConcurrentInputParamsSceneInsertPartition(DataClassBase):
    data_size: Optional[str] = "1m"
    ni: Optional[int] = 5
    with_flush: Optional[bool] = False
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentTaskSceneInsertPartition(DataClassBase):
    data_size: Optional[str] = "1m"
    ni: Optional[int] = 5
    with_flush: Optional[bool] = False
    timeout: Optional[int] = DefaultValue.default_timeout

    @property
    def obj_params(self):
        return {"timeout": self.timeout}


@dataclass
class ConcurrentInputParamsSceneTestPartition(DataClassBase):
    # collection and insert
    data_size: Optional[int] = 3000
    ni: Optional[int] = 3000

    # search
    nq: Optional[int] = 10
    search_param: Optional[dict] = field(default_factory=lambda: {'nprobe': 16})
    limit: Optional[int] = 10
    expr: Optional[str] = None
    output_fields: Optional[list] = None
    guarantee_timestamp: Optional[int] = None
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentTaskSceneTestPartition(DataClassBase):
    # collection and insert
    data_size: Optional[int] = 3000
    ni: Optional[int] = 3000

    # search
    nq: Optional[int] = 10
    search_param: Optional[dict] = field(default_factory=lambda: {'nprobe': 16})
    limit: Optional[int] = 10
    expr: Optional[str] = None
    output_fields: Optional[list] = None
    guarantee_timestamp: Optional[int] = None
    timeout: Optional[int] = DefaultValue.default_timeout

    @property
    def search_obj_params(self):
        return {
            "expr": self.expr,
            "output_fields": self.output_fields,
            "guarantee_timestamp": self.guarantee_timestamp,
            "timeout": self.timeout
        }

    @property
    def obj_params(self):
        return {"timeout": self.timeout}


@dataclass
class ConcurrentInputParamsIterateSearch(DataClassBase):
    nq: Optional[int] = 1
    top_k: Optional[int] = 10
    search_param: Optional[dict] = field(default_factory=lambda: {})
    guarantee_timestamp: Optional[int] = None
    timeout: Optional[int] = DefaultValue.default_timeout

    # other params
    collection_names: Optional[list] = None


@dataclass
class ConcurrentTaskIterateSearch(DataClassBase):
    nq: Optional[int] = 1
    collection_names: Optional[list] = None

    limit: Optional[int] = 10

    # need to update
    param: Optional[dict] = field(default_factory=lambda: {})
    data: Optional[list] = None
    anns_field: Optional[str] = None

    guarantee_timestamp: Optional[int] = None
    timeout: Optional[int] = DefaultValue.default_timeout

    @property
    def obj_params(self):
        _p = copy.deepcopy(self.to_dict)
        del _p["nq"]
        del _p["collection_names"]
        if _p["guarantee_timestamp"] is None:
            del _p["guarantee_timestamp"]
        return _p


@dataclass
class ConcurrentInputParamsLoadSearchRelease(DataClassBase):
    nq: int
    top_k: int
    search_param: dict
    expr: Optional[str] = None
    guarantee_timestamp: Optional[int] = None
    random_data: Optional[bool] = False

    replica_number: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentTaskLoadSearchRelease(DataClassBase):
    data: list
    anns_field: str
    param: dict
    limit: int
    expr: Optional[str] = None
    guarantee_timestamp: Optional[int] = None
    timeout: Optional[int] = DefaultValue.default_timeout

    # for load
    replica_number: Optional[int] = 1

    # other params
    random_data: Optional[bool] = False

    @property
    def obj_params(self):
        _p = copy.deepcopy(self.to_dict)
        del _p["random_data"]
        del _p["replica_number"]
        if _p["guarantee_timestamp"] is None:
            del _p["guarantee_timestamp"]
        return _p


@dataclass
class ConcurrentInputParamsSceneSearchTest(DataClassBase):
    dataset: Optional[str] = DefaultValue.default_dataset
    dim: Optional[int] = DefaultValue.default_dim
    shards_num: Optional[int] = DefaultValue.default_shards_num
    data_size: Optional[int] = 3000
    nb: Optional[int] = 3000
    index_type: Optional[str] = IndexTypeName.IVF_SQ8
    index_param: Optional[dict] = field(default_factory=lambda: {'nlist': 2048})
    metric_type: Optional[str] = MetricsTypeName.L2

    # load
    replica_number: Optional[int] = 1

    # search
    nq: Optional[int] = 1
    top_k: Optional[int] = 10
    search_param: Optional[dict] = field(default_factory=lambda: {'nprobe': 16})

    # other
    prepare_before_insert: Optional[bool] = False
    search_counts: Optional[int] = 1
    new_connect: Optional[bool] = False

    # use db and user
    new_user: Optional[bool] = False


@dataclass
class ConcurrentTaskSceneSearchTest(DataClassBase):
    dataset: Optional[str] = DefaultValue.default_dataset
    dim: Optional[int] = DefaultValue.default_dim
    shards_num: Optional[int] = DefaultValue.default_shards_num
    data_size: Optional[int] = 3000
    nb: Optional[int] = 3000
    index_type: Optional[str] = IndexTypeName.IVF_SQ8
    index_param: Optional[dict] = field(default_factory=lambda: {'nlist': 2048})
    metric_type: Optional[str] = MetricsTypeName.L2
    vector_field_name: Optional[str] = get_default_field_name()

    # load
    replica_number: Optional[int] = 1

    # search
    nq: Optional[int] = 1
    top_k: Optional[int] = 10
    search_param: Optional[dict] = field(default_factory=lambda: {'nprobe': 16})

    # other
    prepare_before_insert: Optional[bool] = False
    search_counts: Optional[int] = 1
    new_connect: Optional[bool] = False

    # use user
    new_user: Optional[bool] = False


@dataclass
class ConcurrentObjParams(DataClassBase):
    type: str = ""
    weight: int = 0
    params: dict = field(default_factory=lambda: {})


@dataclass
class ConcurrentTasksParams:
    debug: Optional[ConcurrentObjParams] = ConcurrentObjParams(**{"params": DataClassBase})
    search: Optional[ConcurrentObjParams] = ConcurrentObjParams(**{"params": ConcurrentTaskSearch})
    query: Optional[ConcurrentObjParams] = ConcurrentObjParams(**{"params": ConcurrentTaskQuery})
    flush: Optional[ConcurrentObjParams] = ConcurrentObjParams(**{"params": ConcurrentTaskFlush})
    load: Optional[ConcurrentObjParams] = ConcurrentObjParams(**{"params": ConcurrentTaskLoad})
    release: Optional[ConcurrentObjParams] = ConcurrentObjParams(**{"params": ConcurrentTaskRelease})
    load_release: Optional[ConcurrentObjParams] = ConcurrentObjParams(**{"params": ConcurrentTaskLoadRelease})
    insert: Optional[ConcurrentObjParams] = ConcurrentObjParams(**{"params": ConcurrentTaskInsert})
    upsert:Optional[ConcurrentObjParams] = ConcurrentObjParams(**{"params": ConcurrentTaskUpsert})
    delete: Optional[ConcurrentObjParams] = ConcurrentObjParams(**{"params": ConcurrentTaskDelete})
    scene_test: Optional[ConcurrentObjParams] = ConcurrentObjParams(**{"params": ConcurrentTaskSceneTest})
    scene_insert_delete_flush: Optional[ConcurrentObjParams] = ConcurrentObjParams(
        **{"params": ConcurrentTaskSceneInsertDeleteFlush})
    scene_insert_partition: Optional[ConcurrentObjParams] = ConcurrentObjParams(
        **{"params": ConcurrentTaskSceneInsertPartition})
    scene_test_partition: Optional[ConcurrentObjParams] = ConcurrentObjParams(
        **{"params": ConcurrentTaskSceneTestPartition})
    iterate_search: Optional[ConcurrentObjParams] = ConcurrentObjParams(**{"params": ConcurrentTaskIterateSearch})
    load_search_release: Optional[ConcurrentObjParams] = ConcurrentObjParams(
        **{"params": ConcurrentTaskLoadSearchRelease})
    scene_search_test: Optional[ConcurrentObjParams] = ConcurrentObjParams(**{"params": ConcurrentTaskSceneSearchTest})

    @property
    def all_obj(self):
        return list(vars(self).keys())

    @property
    def to_dict(self):
        return self.deal_vars(vars(self))

    @staticmethod
    def deal_vars(input_dict: dict):
        _input_dict = copy.deepcopy(input_dict)

        def recursive_process(i_d, f_d):
            for k, v in i_d.items():
                if isinstance(v, object) and hasattr(v, "to_dict"):
                    f_d.update({k: v.to_dict})
                elif isinstance(v, dict):
                    return recursive_process(i_d[k], f_d[k])
            return f_d

        def check_object(_dict):
            if isinstance(_dict, dict):
                for k in _dict.keys():
                    if isinstance(_dict[k], dict):
                        return check_object(_dict[k])
                    elif isinstance(_dict[k], object) and hasattr(_dict[k], "to_dict"):
                        return True
            return False

        object_flag = True
        while object_flag:
            _input_dict = recursive_process(_input_dict, _input_dict)
            object_flag = check_object(_input_dict)

        return _input_dict
