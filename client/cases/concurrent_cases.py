import copy
import time

from client.parameters.params import ParamsFormat
from client.parameters import params_name as pn
from client.common.common_type import Precision, CaseIterParams
from client.common.common_func import gen_combinations, get_vector_type, get_default_field_name, GoSearchParams, \
    parser_time
from utils.util_log import log
from client.util.params_check import check_params
from client.cases.common_cases import CommonCases
# from client.concurrent.locust_runner import LocustRunner
from client.parameters.params import ConcurrentObjParams, ConcurrentTasksParams, ConcurrentTaskSearch, \
    ConcurrentTaskQuery, ConcurrentInputParamsQuery, ConcurrentInputParamsSearch, ConcurrentInputParamsFlush, \
    ConcurrentTaskFlush, ConcurrentInputParamsLoad, ConcurrentTaskLoad, ConcurrentInputParamsRelease, \
    ConcurrentTaskRelease, ConcurrentInputParamsInsert, ConcurrentTaskInsert, ConcurrentInputParamsDelete, \
    ConcurrentTaskDelete, ConcurrentInputParamsSceneTest, ConcurrentTaskSceneTest, DataClassBase, \
    ConcurrentInputParamsSceneInsertDeleteFlush, ConcurrentTaskSceneInsertDeleteFlush, ConcurrentInputParamsDebug, \
    ConcurrentTaskDebug
from client.util.api_request import info_logout


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

    def parser_concurrent_params(self):
        return gen_combinations(self.params_obj.concurrent_params)

    def parser_tasks_params(self, req_type, req_params, vector_field_name: str, metric_type: str):
        if req_type == pn.search:
            params = ConcurrentInputParamsSearch(**req_params)
            result, nq, top_k, expr = self.search_param_analysis(_search_params=params.to_dict,
                                                                 default_field_name=vector_field_name,
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

        elif req_type == pn.scene_test:
            return ConcurrentTaskSceneTest(**ConcurrentInputParamsSceneTest(**req_params).to_dict)

        elif req_type in [pn.flush, pn.load, pn.release, pn.delete, "debug"]:
            return eval(
                "ConcurrentTask{0}(**ConcurrentInputParams{0}(**req_params).to_dict)".format(req_type.capitalize()))

        elif req_type == pn.scene_insert_delete_flush:
            params = ConcurrentInputParamsSceneInsertDeleteFlush(**req_params).to_dict
            params.update({"dim": self.params_obj.dataset_params[pn.dim]})
            _p = ConcurrentTaskSceneInsertDeleteFlush(**params)
            _p.set_params()
            return _p

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
            clean_collection: bool
        :return:
        """

        # params prepare
        params = kwargs.get("params", None)
        prepare = kwargs.get("prepare", True)
        prepare_clean = kwargs.get("prepare_clean", True)
        clean_collection = kwargs.get("clean_collection", True)
        log.info("[ConcurrentClientBase] The detailed test steps are as follows: {}".format(self))

        # params parsing
        self.parsing_params(params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(vector_type)

        obj_params = self.parser_concurrent_tasks(self.params_obj.concurrent_tasks, vector_default_field_name,
                                                  self.params_obj.dataset_params[pn.metric_type])

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

        # set output log
        info_logout.reset_output()

        # concurrent test
        c_params = self.parser_concurrent_params()
        params_list = []
        # for c_p in c_params:
        #     spawn_rate = c_p[pn.spawn_rate] if pn.spawn_rate in c_p else None
        #     con_client = LocustRunner(obj=self, obj_params=obj_params,
        #                               interval=c_p[pn.interval], during_time=parser_time(c_p[pn.during_time]),
        #                               concurrent_number=c_p[pn.concurrent_number], spawn_rate=spawn_rate)
        #
        #     actual_params_used = copy.deepcopy(params)
        #     actual_params_used[pn.concurrent_params] = {
        #         pn.concurrent_number: c_p[pn.concurrent_number],
        #         pn.during_time: c_p[pn.during_time],
        #         pn.interval: c_p[pn.interval],
        #         pn.spawn_rate: spawn_rate
        #     }
        #     p = CaseIterParams(callable_object=con_client.start_runner,
        #                        actual_params_used=actual_params_used, case_type=self.__class__.__name__)
        #     params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=clean_collection)
        yield True
