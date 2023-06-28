import copy

from client.common.common_type import Precision, CaseIterParams
from client.common.common_func import (
    gen_combinations, get_vector_type, get_default_field_name, GoSearchParams, parser_time, update_dict_value,
    get_input_params)
from client.util.params_check import check_params
from client.util.api_request import info_logout
from client.cases.common_cases import CommonCases
from client.parameters import params_name as pn
from client.parameters.params import (
    ParamsFormat, ConcurrentObjParams, ConcurrentTasksParams, DataClassBase,
    ConcurrentTaskDebug, ConcurrentInputParamsDebug,
    ConcurrentTaskSearch, ConcurrentInputParamsSearch,
    ConcurrentTaskQuery, ConcurrentInputParamsQuery,
    ConcurrentTaskFlush, ConcurrentInputParamsFlush,
    ConcurrentTaskLoad, ConcurrentInputParamsLoad,
    ConcurrentTaskRelease, ConcurrentInputParamsRelease,
    ConcurrentTaskLoadRelease, ConcurrentInputParamsLoadRelease,
    ConcurrentTaskInsert, ConcurrentInputParamsInsert,
    ConcurrentTaskDelete, ConcurrentInputParamsDelete,
    ConcurrentTaskSceneTest, ConcurrentInputParamsSceneTest,
    ConcurrentTaskSceneInsertDeleteFlush, ConcurrentInputParamsSceneInsertDeleteFlush,
    ConcurrentTaskIterateSearch, ConcurrentInputParamsIterateSearch,
    ConcurrentTaskLoadSearchRelease, ConcurrentInputParamsLoadSearchRelease,
    ConcurrentTaskSceneSearchTest, ConcurrentInputParamsSceneSearchTest,
    ConcurrentTaskSceneInsertPartition, ConcurrentInputParamsSceneInsertPartition,
    ConcurrentInputParamsSceneTestPartition, ConcurrentTaskSceneTestPartition,
    ConcurrentInputParamsUpsert, ConcurrentTaskUpsert,
    ConcurrentInputParamsSceneTestGrow,ConcurrentTaskSceneTestGrow
)

from utils.util_log import log
from parameters.input_params import param_info


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
                          during_time, interval=20):
        res_go = self.go_search(index_type=index_type, go_search_params=go_search_params,
                                concurrent_number=concurrent_number, during_time=parser_time(during_time),
                                interval=interval)
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
            rebuild_index: bool
            clean_collection: bool
        :return:
        """

        # params prepare
        params, prepare, prepare_clean, rebuild_index, clean_collection = get_input_params(**kwargs)
        log.info("[GoBenchCases] The detailed test steps are as follows: {}".format(self))

        # params parsing
        self.parsing_params(params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(
            vector_type, self.params_obj.dataset_params.get(pn.vector_field_name, ""))

        # prepare data
        self.prepare_collection(vector_default_field_name, prepare, prepare_clean)
        if prepare:
            self.prepare_index(vector_field_name=vector_default_field_name,
                               metric_type=self.params_obj.dataset_params[pn.metric_type],
                               clean_index_before=True)
            self.prepare_insert(data_type=self.params_obj.dataset_params[pn.dataset_name],
                                dim=self.params_obj.dataset_params[pn.dim],
                                size=self.params_obj.dataset_params[pn.dataset_size],
                                ni=self.params_obj.dataset_params[pn.ni_per])
            self.prepare_flush()
            self.prepare_index(vector_field_name=vector_default_field_name,
                               metric_type=self.params_obj.dataset_params[pn.metric_type])
        else:
            # if pass in rebuild_index, indexes of collection will be dropped before building index
            if rebuild_index:
                self.prepare_index(vector_field_name=vector_default_field_name,
                                   metric_type=self.params_obj.dataset_params[pn.metric_type],
                                   clean_index_before=rebuild_index)

        self.count_entities()
        # load collection
        self.prepare_load(**self.params_obj.load_params)

        self.show_all_resource(shards_num=self.params_obj.collection_params.get(pn.shards_num, 2),
                               show_resource_groups=self.params_obj.dataset_params.get(pn.show_resource_groups, True),
                               show_db_user=self.params_obj.dataset_params.get(pn.show_db_user, False))

        # search
        s_params = self.parser_search_params()
        g_params = self.parser_go_search_params()
        params_list = []
        for g_p in g_params:
            for s_p in s_params:
                search_params, nq, top_k, expr, other_params = \
                    self.search_param_analysis(s_p, vector_default_field_name,
                                               self.params_obj.dataset_params[pn.metric_type])
                go_search_params = GoSearchParams(dim=self.params_obj.dataset_params[pn.dim], **search_params)

                actual_params_used = copy.deepcopy(params)
                actual_params_used[pn.search_params] = update_dict_value({
                    pn.nq: nq,
                    "param": search_params["param"],
                    pn.top_k: top_k,
                    pn.expr: expr
                }, other_params)
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


class ConcurrentClientBase(CommonCases):

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
        9. perform concurrent operations
        10. clean all collections or not
        """

    @staticmethod
    def iterate_search_param_analysis(_search_params: dict):
        _params = copy.deepcopy(_search_params)
        top_k = _params.pop(pn.top_k)
        search_param = _params.pop(pn.search_param)

        result = update_dict_value({
            "param": {"params": search_param},
            "limit": top_k,
        }, _params)
        return result

    def parser_concurrent_params(self):
        return gen_combinations(self.params_obj.concurrent_params)

    def parser_tasks_params(self, req_type, req_params, vector_field_name: str, metric_type: str):
        if req_type == pn.search:
            params = ConcurrentInputParamsSearch(**req_params)
            result, nq, top_k, expr, other_params = \
                self.search_param_analysis(_search_params=params.to_dict, default_field_name=vector_field_name,
                                           metric_type=metric_type)
            return ConcurrentTaskSearch(**result)

        elif req_type == pn.query:
            params = ConcurrentInputParamsQuery(**req_params)
            result = self.query_param_analysis(**params.to_dict)
            return ConcurrentTaskQuery(**result)

        elif req_type == pn.insert:
            params = ConcurrentInputParamsInsert(**req_params).to_dict
            params.update({"dim": self.params_obj.dataset_params[pn.dim]})
            _p = ConcurrentTaskInsert(**params)
            _p.set_params()
            return _p
        
        elif req_type == pn.upsert:
            params = ConcurrentInputParamsUpsert(**req_params).to_dict
            params.update({"dim": self.params_obj.dataset_params[pn.dim]})
            _p = ConcurrentTaskUpsert(**params)
            _p.set_params()
            return _p
        
        elif req_type == pn.scene_test:
            return ConcurrentTaskSceneTest(**ConcurrentInputParamsSceneTest(**req_params).to_dict)

        elif req_type in [pn.flush, pn.load, pn.release, pn.delete, pn.upsert, "debug"]:
            return eval(
                "ConcurrentTask{0}(**ConcurrentInputParams{0}(**req_params).to_dict)".format(req_type.capitalize()))

        elif req_type == pn.scene_insert_delete_flush:
            params = ConcurrentInputParamsSceneInsertDeleteFlush(**req_params).to_dict
            params.update({"dim": self.params_obj.dataset_params[pn.dim]})
            _p = ConcurrentTaskSceneInsertDeleteFlush(**params)
            _p.set_params()
            return _p

        elif req_type == pn.load_release:
            return ConcurrentTaskLoadRelease(**ConcurrentInputParamsLoadRelease(**req_params).to_dict)

        elif req_type == pn.scene_insert_partition:
            params = ConcurrentInputParamsSceneInsertPartition(**req_params).to_dict
            return ConcurrentTaskSceneInsertPartition(**params)

        elif req_type == pn.scene_test_partition:
            params = ConcurrentInputParamsSceneTestPartition(**req_params).to_dict
            return ConcurrentTaskSceneTestPartition(**params)

        elif req_type == pn.iterate_search:
            params = ConcurrentInputParamsIterateSearch(**req_params)
            result = self.iterate_search_param_analysis(_search_params=params.to_dict)
            return ConcurrentTaskIterateSearch(**result)

        elif req_type == pn.load_search_release:
            params = ConcurrentInputParamsLoadSearchRelease(**req_params)
            result, nq, top_k, expr, other_params = \
                self.search_param_analysis(_search_params=params.to_dict, default_field_name=vector_field_name,
                                           metric_type=metric_type)
            return ConcurrentTaskLoadSearchRelease(**result)

        elif req_type == pn.scene_search_test:
            return ConcurrentTaskSceneSearchTest(**ConcurrentInputParamsSceneSearchTest(**req_params).to_dict)
        
        elif req_type == pn.scene_test_grow:
            return ConcurrentTaskSceneTestGrow(**ConcurrentInputParamsSceneTestGrow(**req_params).to_dict)

        return DataClassBase()

    def parser_concurrent_tasks(self, tasks: list, vector_field_name: str, metric_type: str) -> ConcurrentTasksParams:
        tasks_dict = {}
        all_support_obj = ConcurrentTasksParams().all_obj
        for task in tasks:
            if task["type"] not in all_support_obj:
                raise Exception("[ConcurrentClientBase] Task type:{0} not support, please check!!!".format(task.type))
            task["params"] = self.parser_tasks_params(task["type"], task["params"], vector_field_name, metric_type)
            tasks_dict.update({task["type"]: ConcurrentObjParams(**task)})
        return ConcurrentTasksParams(**tasks_dict)

    @check_params(ParamsFormat.common_concurrent)
    def scene_concurrent_locust(self, **kwargs):
        """
        :param kwargs:
            params: dict
            prepare: bool
            prepare_clean: bool
            rebuild_index: bool
            clean_collection: bool
        :return:
        """
        from gevent import monkey
        _patch_params = {} if param_info.locust_patch_switch else {"ssl": False}
        monkey.patch_all(**_patch_params)
        # from requests.packages.urllib3.util.ssl_ import create_urllib3_context; create_urllib3_context()
        import grpc.experimental.gevent as grpc_gevent
        grpc_gevent.init_gevent()
        from client.concurrent.locust_runner import LocustRunner

        # params prepare
        params, prepare, prepare_clean, rebuild_index, clean_collection = get_input_params(**kwargs)
        log.info("[ConcurrentClientBase] The detailed test steps are as follows: {}".format(self))

        # params parsing
        self.parsing_params(params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(
            vector_type, self.params_obj.dataset_params.get(pn.vector_field_name, ""))

        obj_params = self.parser_concurrent_tasks(self.params_obj.concurrent_tasks, vector_default_field_name,
                                                  self.params_obj.dataset_params[pn.metric_type])

        # load prepare params
        _prepare_load = self.params_obj.load_params.pop("prepare_load", False)

        # prepare data
        self.prepare_collection(vector_default_field_name, prepare, prepare_clean)
        if prepare:
            self.prepare_index(vector_field_name=vector_default_field_name,
                               metric_type=self.params_obj.dataset_params[pn.metric_type],
                               clean_index_before=True)

            if _prepare_load:
                self.prepare_load(**self.params_obj.load_params)

            self.prepare_insert(data_type=self.params_obj.dataset_params[pn.dataset_name],
                                dim=self.params_obj.dataset_params[pn.dim],
                                size=self.params_obj.dataset_params[pn.dataset_size],
                                ni=self.params_obj.dataset_params[pn.ni_per])
            self.prepare_flush()
            self.prepare_index(vector_field_name=vector_default_field_name,
                               metric_type=self.params_obj.dataset_params[pn.metric_type])
        else:
            # if pass in rebuild_index, indexes of collection will be dropped before building index
            if rebuild_index:
                self.prepare_index(vector_field_name=vector_default_field_name,
                                   metric_type=self.params_obj.dataset_params[pn.metric_type],
                                   clean_index_before=rebuild_index)

        self.count_entities()
        # load collection
        self.prepare_load(**self.params_obj.load_params)

        self.show_all_resource(shards_num=self.params_obj.collection_params.get(pn.shards_num, 2),
                               show_resource_groups=self.params_obj.dataset_params.get(pn.show_resource_groups, True),
                               show_db_user=self.params_obj.dataset_params.get(pn.show_db_user, False))

        # set output log
        info_logout.reset_output()

        # concurrent test
        c_params = self.parser_concurrent_params()
        params_list = []
        for c_p in c_params:
            spawn_rate = c_p[pn.spawn_rate] if pn.spawn_rate in c_p else None
            con_client = LocustRunner(obj=self, obj_params=obj_params,
                                      interval=c_p[pn.interval], during_time=parser_time(c_p[pn.during_time]),
                                      concurrent_number=c_p[pn.concurrent_number], spawn_rate=spawn_rate)

            actual_params_used = copy.deepcopy(params)
            actual_params_used[pn.concurrent_params] = {
                pn.concurrent_number: c_p[pn.concurrent_number],
                pn.during_time: c_p[pn.during_time],
                pn.interval: c_p[pn.interval],
                pn.spawn_rate: spawn_rate
            }
            p = CaseIterParams(callable_object=con_client.start_runner, object_args=[self.case_report],
                               actual_params_used=actual_params_used, case_type=self.__class__.__name__)
            params_list.append(p)
        yield params_list

        # recover output log
        info_logout.recover_output()

        # clear env
        self.clear_collections(clean_collection=clean_collection)
        yield True
