import time
import random
from pprint import pformat

from pymilvus import DefaultConfig, DataType

from client.client_base import (
    ApiConnectionsWrapper, ApiCollectionWrapper, ApiIndexWrapper, ApiPartitionWrapper, ApiCollectionSchemaWrapper,
    ApiFieldSchemaWrapper, ApiUtilityWrapper)
from client.common.common_func import (
    gen_collection_schema, gen_unique_str, get_file_list, read_npy_file, parser_data_size, loop_files, loop_ids,
    gen_vectors, gen_entities, run_go_bench_process, go_bench, GoSearchParams, loop_gen_files, remove_list_values,
    parser_segment_info, gen_scalar_values, update_dict_value, get_default_search_params)
from client.common.common_param import TransferNodesParams, TransferReplicasParams
from client.common.common_type import Precision, CheckTasks
from client.common.common_type import DefaultValue as dv
from client.parameters.params import (
    ConcurrentTaskSearch, ConcurrentTaskQuery, ConcurrentTaskFlush, ConcurrentTaskLoad, ConcurrentTaskRelease,
    ConcurrentTaskLoadRelease, ConcurrentTaskInsert, ConcurrentTaskDelete, ConcurrentTaskSceneTest,
    ConcurrentTaskSceneInsertDeleteFlush, DataClassBase, ConcurrentTaskSceneInsertPartition,
    ConcurrentTaskSceneInsertDeleteFlush, DataClassBase, ConcurrentTaskIterateSearch, ConcurrentTaskLoadSearchRelease,
    ConcurrentTaskSceneSearchTest, ConcurrentTaskSceneInsertPartition, ConcurrentTaskSceneTestPartition)
from client.parameters.params_name import (
    reset, groups, max_length, dim, transfer_nodes, transfer_replicas, resource_groups)
from client.util.api_request import func_time_catch

# packages outside the client folder
from parameters.input_params import param_info
from commons.common_params import EnvVariable
from commons.common_type import LogLevel
from utils.util_log import log

try:
    from pymilvus import DEFAULT_RESOURCE_GROUP

    RESOURCE_GROUPS_FLAG = True
except ImportError as e:
    DEFAULT_RESOURCE_GROUP = dv.default_resource_group
    RESOURCE_GROUPS_FLAG = False


class Base:
    shards_num = 2

    def __init__(self):
        self.connection_wrap = ApiConnectionsWrapper()
        self.utility_wrap = ApiUtilityWrapper()
        self.collection_wrap = ApiCollectionWrapper()
        self.partition_wrap = ApiPartitionWrapper()
        self.index_wrap = ApiIndexWrapper()
        self.collection_schema_wrap = ApiCollectionSchemaWrapper()
        self.field_schema_wrap = ApiFieldSchemaWrapper()

        self.collection_name = ''
        self.collection_schema = None

    # def __del__(self):
    #     log.info("[Base] Start disconnect connection.")
    #     self.remove_connect()

    def connect(self, host=None, port=None, secure=False):
        """ Add a connection and create the connect """
        # remove connect before new connection
        self.remove_connect()
        host = host or param_info.param_host
        port = port or param_info.param_port
        secure = secure or param_info.param_secure

        params = {"alias": DefaultConfig.DEFAULT_USING, "host": host, "port": port}
        if secure is True:
            params.update({"user": param_info.param_user, "password": param_info.param_password, "secure": True})
        log.info("[Base] Connection params: {}".format(params))
        return self.connection_wrap.connect(**params)

    def remove_connect(self, alias=DefaultConfig.DEFAULT_USING):
        """ Disconnect and remove default connect """
        if self.connection_wrap.has_connection(alias=alias).response:
            log.info("[Base] Disconnect alias: {0}".format(alias))
            self.connection_wrap.remove_connection(alias=alias)

    def create_collection(self, collection_name="", vector_field_name="", schema=None, other_fields=[], shards_num=2,
                          collection_obj: callable = None, log_level=LogLevel.INFO, varchar_id=False, scalars_params={},
                          **kwargs):
        """ Create a collection with default schema """
        schema = gen_collection_schema(
            vector_field_name=vector_field_name, other_fields=other_fields, varchar_id=varchar_id,
            max_length=kwargs.pop(max_length, dv.default_max_length), dim=kwargs.pop(dim, dv.default_dim),
            scalars_params=scalars_params) if schema is None else schema

        collection_name = collection_name or gen_unique_str()
        self.collection_name = self.collection_name or collection_name

        log.customize(log_level)("[Base] Create collection {}".format(collection_name))
        collection_obj = collection_obj or self.collection_wrap
        return collection_obj.init_collection(collection_name, schema=schema, shards_num=shards_num, **kwargs)

    def connect_collection(self, collection_name):
        """ Connect to an exist collection """
        self.collection_name = collection_name
        log.info("[Base] Connect collection {}".format(self.collection_name))
        return self.collection_wrap.init_collection(self.collection_name)

    def clean_all_collection(self, clean=True):
        """ Drop all collections in the database """
        if not clean:
            # self.remove_connect()
            return
        collections = self.utility_wrap.list_collections().response
        log.info("[Base] Start clean all collections {}".format(collections))
        for i in collections:
            self.utility_wrap.drop_collection(i)

    def release_all_collections(self):
        collections = self.utility_wrap.list_collections().response
        log.info("[Base] Start release all collections {}".format(collections))
        for i in collections:
            c = ApiCollectionWrapper()
            c.init_collection(i)
            c.release()

    def clear_collections(self, clean_collection=True):
        if clean_collection:
            log.info("[Base] Start clear collections")
            self.collection_wrap.release()
            log.info("[Base] Release collections done")
            self.clean_all_collection()
            log.info("[Base] Clear collections Done!")
        log.info("[Base] Start disconnect connection.")
        self.remove_connect()

    def flush_collection(self, collection_obj: callable = None, log_level=LogLevel.INFO):
        collection_obj = collection_obj or self.collection_wrap
        log.customize(log_level)("[Base] Start flush collection {}".format(collection_obj.name))
        return collection_obj.flush()

    def load_collection(self, replica_number=1, log_level=LogLevel.INFO, **kwargs):
        kwargs = self.get_resource_groups(**kwargs)
        log.customize(log_level)(
            f"[Base] Start load collection {self.collection_wrap.name},replica_number:{replica_number},kwargs:{kwargs}")
        return self.collection_wrap.load(replica_number=replica_number, **kwargs)

    def release_collection(self):
        log.info("[Base] Start release collection {}".format(self.collection_wrap.name))
        return self.collection_wrap.release()

    def get_collection_schema(self):
        self.collection_schema = self.collection_wrap.schema.to_dict()
        log.info("[Base] Collection schema: {0}".format(self.collection_schema))

    def insert_batch(self, vectors, ids, data_size, varchar_filled=False, collection_obj: callable = None,
                     collection_schema=None, log_level=LogLevel.INFO, insert_scalars_params={}, **kwargs):
        if self.collection_schema is None and collection_schema is None:
            self.get_collection_schema()
            collection_schema = self.collection_schema
        else:
            collection_schema = collection_schema or self.collection_schema

        if isinstance(vectors, list):
            entities = gen_entities(collection_schema, vectors, ids, varchar_filled, insert_scalars_params)
        else:
            entities = gen_entities(collection_schema, vectors.tolist(), ids, varchar_filled, insert_scalars_params)

        log.customize(log_level)(
            "[Base] Start inserting, ids: {0} - {1}, data size: {2}".format(ids[0], ids[-1], data_size))
        collection_obj = collection_obj or self.collection_wrap
        res = collection_obj.insert(entities, **kwargs)
        self.count_entities(collection_obj=collection_obj, log_level=log_level)
        return res.rt

    def insert(self, data_type, dim, size, ni, varchar_filled=False, collection_obj: callable = None,
               collection_schema=None, collection_name="", log_level=LogLevel.INFO,
               scalars_params={}, **kwargs):
        data_size = parser_data_size(size)
        data_size_format = str(format(data_size, ',d'))
        ni_cunt = int(data_size / int(ni))
        last_insert = data_size % int(ni)

        batch_rt = 0
        last_rt = 0

        collection_name = collection_name or self.collection_name
        log.customize(log_level)(
            "[Base] Start inserting {0} vectors to collection {1}".format(data_size, collection_name))

        _loop_ids = loop_ids(int(ni))
        insert_scalars_params = gen_scalar_values(scalars_params, ni)

        if data_type == "local":

            for i in range(0, ni_cunt):
                batch_rt += self.insert_batch(gen_vectors(ni, dim), next(_loop_ids), data_size_format, varchar_filled,
                                              collection_obj, collection_schema, log_level,
                                              next(insert_scalars_params), **kwargs)

            if last_insert > 0:
                last_rt = self.insert_batch(
                    gen_vectors(last_insert, dim), next(_loop_ids)[:last_insert], data_size_format,
                    varchar_filled, collection_obj, collection_schema, log_level,
                    next(insert_scalars_params), **kwargs)

        else:
            # files = get_file_list(data_size, dim, data_type)
            # if len(files) == 0:
            #     raise Exception("[insert] Can not get files, please check.")
            #
            # _loop_file = loop_files(files)
            _loop_file = loop_gen_files(dim, data_type)
            vectors = []

            for i in range(0, ni_cunt):
                if len(vectors) < ni:
                    while True:
                        vectors.extend(read_npy_file(next(_loop_file)))
                        if len(vectors) >= ni:
                            break
                batch_rt += self.insert_batch(vectors[:ni], next(_loop_ids), data_size_format, varchar_filled,
                                              collection_obj, collection_schema, log_level,
                                              next(insert_scalars_params), **kwargs)
                vectors = vectors[ni:]

            if last_insert > 0:
                if len(vectors) < last_insert:
                    while True:
                        vectors.extend(read_npy_file(next(_loop_file)))
                        if len(vectors) >= last_insert:
                            break
                last_rt = self.insert_batch(
                    vectors[:last_insert], next(_loop_ids)[:last_insert], data_size_format, varchar_filled,
                    collection_obj, collection_schema, log_level, next(insert_scalars_params), **kwargs)

        total_time = round((batch_rt + last_rt), Precision.COMMON_PRECISION)
        ips = round(int(data_size) / total_time, Precision.INSERT_PRECISION)
        ni_time = round(batch_rt / ni_cunt, Precision.INSERT_PRECISION) if ni_cunt != 0 else 0
        msg = "[Base] Total time of insert: {0}s, average number of vector bars inserted per second: {1}," + \
              " average time to insert {2} vectors per time: {3}s"
        log.customize(log_level)(msg.format(total_time, ips, ni, ni_time))
        return {
            "insert": {
                "total_time": total_time,
                "VPS": ips,
                "batch_time": ni_time,
                "batch": ni
            }
        }

    def ann_insert(self, source_vectors, ni=100, scalars_params={}):
        size = len(source_vectors)
        data_size_format = str(format(size, ',d'))
        ni_cunt = int(size / int(ni))
        last_insert = size % int(ni)

        batch_rt = 0
        last_rt = 0

        _loop_ids = loop_ids(int(ni))
        insert_scalars_params = gen_scalar_values(scalars_params, ni)

        log.info("[Base] Start inserting {} vectors".format(size))

        for i in range(ni_cunt):
            batch_rt += self.insert_batch(source_vectors[int(i * ni):int((i + 1) * ni)], next(_loop_ids),
                                          data_size_format, insert_scalars_params=next(insert_scalars_params))

        if last_insert > 0:
            last_rt = self.insert_batch(source_vectors[-last_insert:], next(_loop_ids)[:last_insert], data_size_format,
                                        insert_scalars_params=next(insert_scalars_params))

        total_time = round(batch_rt + last_rt, Precision.INSERT_PRECISION)
        log.info("[Base] Total time of ann insert: {}s".format(total_time))
        return {
            "ann_insert": {
                "total_time": total_time
            }
        }

    def build_index(self, field_name, index_type, metric_type, index_param, collection_obj: callable = None,
                    collection_name="", log_level=LogLevel.INFO):
        """
        {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}}
        """
        index_params = {"index_type": index_type, "metric_type": metric_type, "params": index_param}
        collection_name = collection_name or self.collection_name

        log.customize(log_level)(
            "[Base] Start build index of {0} for collection {1}, params:{2}".format(index_type, collection_name,
                                                                                    index_params))
        collection_obj = collection_obj or self.collection_wrap
        return self.index_wrap.init_index(collection_obj.collection, field_name, index_params)

    def build_scalar_index(self, field_name):
        log.info("[Base] Start build scalar index of {0}".format(field_name))
        return self.index_wrap.init_index(self.collection_wrap.collection, field_name, index_params={})

    def show_index(self):
        if self.collection_wrap.has_index().response:
            index_params = self.collection_wrap.index().response.params
            log.info("[Base] Params of index: {}".format(index_params))
            return index_params
        log.info("[Base] Collection:{0} is not building index".format(self.collection_wrap.name))
        return {}

    def describe_collection_index(self):
        indexes = self.collection_wrap.indexes
        log.info("[Base] Collection:{0} indexes".format(self.collection_wrap.name, indexes))
        return indexes

    def clean_index(self):
        if self.collection_wrap.has_index().response:
            self.collection_wrap.drop_index()
            if self.collection_wrap.has_index().response:
                log.error("[Base] Index of collection {0} can not be cleaned.".format(self.collection_wrap.name))
                return False
        log.info("[Base] Clean all index done.")
        return True

    def count_entities(self, collection_obj: callable = None, log_level=LogLevel.INFO):
        collection_obj = collection_obj or self.collection_wrap
        counts = collection_obj.num_entities
        log.customize(log_level)("[Base] Number of vectors in the collection({0}): {1}".format(collection_obj.name,
                                                                                               counts))

    def query(self, ids=None, expr=None, **kwargs):
        """
        :return: (result, rt), check_result
        """
        _expr = ""
        if ids is None and expr is None:
            raise Exception("[Base] Params of query are needed.")

        elif ids is not None:
            _expr = "id in %s" % str(ids)

        elif expr is not None:
            _expr = expr

        log.info("[Base] expr of query: \"{0}\", kwargs:{1}".format(_expr, kwargs))
        return self.collection_wrap.query(expr=_expr, **kwargs)

    def search(self, data, anns_field, param, limit, expr=None, timeout=300, **kwargs):
        """
        :return: (result, rt), check_result
        """
        msg = "[Base] Params of search: nq:{0}, anns_field:{1}, param:{2}, limit:{3}, expr:\"{4}\", kwargs:{5}"
        log.info(msg.format(len(data), anns_field, param, limit, expr, kwargs))
        return self.collection_wrap.search(data, anns_field, param, limit, expr=expr, timeout=timeout, **kwargs)

    def go_search(self, index_type: str, go_search_params: GoSearchParams, concurrent_number: int,
                  during_time: int, interval: int, uri="", go_benchmark="", timeout=300, output_format="json",
                  partition_names=[], secure=False) -> dict:
        """
        :return: dict
        """
        go_benchmark = go_benchmark or EnvVariable.MILVUS_GOBENCH_PATH
        uri = uri or "{0}:{1}".format(param_info.param_host, param_info.param_port)
        secure = secure or param_info.param_secure
        user = param_info.param_user
        password = param_info.param_password

        return go_bench(go_benchmark=go_benchmark, uri=uri, collection_name=self.collection_name,
                        index_type=index_type, search_params=go_search_params.search_parameters,
                        search_timeout=timeout, search_vector=go_search_params.data,
                        concurrent_number=concurrent_number, during_time=during_time, interval=interval,
                        log_path=log.log_info, output_format=output_format, partition_names=partition_names,
                        secure=secure, user=user, password=password, json_file_path=go_search_params.json_file_path)

    def _transfer_nodes(self, source: str, target: str, num_node: int):
        if num_node > 0:
            self.utility_wrap.transfer_node(source=source, target=target, num_node=num_node)
        else:
            log.warning(f"[Base] Can not transfer node {num_node} from {source} to {target}")

    def transfer_nodes(self, node: TransferNodesParams, all_rg: list):
        if node.source not in all_rg:
            raise Exception(f"[Base] The source resource group does not exist:{node.source}")
        if node.target not in all_rg:
            log.debug(f"[Base] Create resource group {node.target} before transfer")
            self.utility_wrap.create_resource_group(name=node.target)
        self._transfer_nodes(source=node.source, target=node.target, num_node=node.num_node)

    def _transfer_replicas(self, source: str, target: str, collection_name: str, num_replica: int):
        if num_replica > 0:
            self.utility_wrap.transfer_replica(source=source, target=target, collection_name=collection_name,
                                               num_replica=num_replica)
        else:
            log.warning("[Base] Can't transfer replica %s from %s to %s, collection_name: %s" % (
                num_replica, source, target, collection_name))

    def transfer_replicas(self, replica: TransferReplicasParams, all_rg: list):
        if replica.source not in all_rg:
            raise Exception(f"[Base] The source resource group does not exist:{replica.source}")
        if replica.target not in all_rg:
            raise Exception(f"[Base] The target resource group does not exist:{replica.source}")
        self._transfer_replicas(source=replica.source, target=replica.target,
                                collection_name=replica.collection_name, num_replica=replica.num_replica)

    def drop_all_resource_groups(self):
        lrg = self.utility_wrap.list_resource_groups().response
        log.debug("[Base] All resource groups {0}".format(lrg))
        for n in remove_list_values(lrg, DEFAULT_RESOURCE_GROUP):
            res = self.utility_wrap.describe_resource_group(name=n).response
            self._transfer_nodes(source=n, target=DEFAULT_RESOURCE_GROUP, num_node=res.num_available_node)
            self.utility_wrap.drop_resource_group(name=n)
            log.debug("[Base] Dropped resource group {0}".format(n))

    def reset_resource_groups(self, reset_rg=True):
        if not reset_rg:
            return
        self.release_all_collections()
        self.drop_all_resource_groups()

        # check result
        _lrg = self.utility_wrap.list_resource_groups().response
        if len(remove_list_values(_lrg, DEFAULT_RESOURCE_GROUP)) > 0:
            raise Exception("[Base] Failed to clean resource groups:{0}, please check manually".format(_lrg))
        log.info("[Base] Dropped all resource groups except the default resource group.")

    def set_resource_groups(self, **kwargs):
        """
        {
            "groups": {
                        "transfer_nodes": [{"source": <name>, "target": <name>, "num_node": <int>}, ...],
                        "transfer_replicas": [{"source": <name>, "target": <name>, "collection_name": <collection_name>,
                         "num_replica": <int>}, ...]
                      }  # or [1, 2, 3] just for nodes
            "reset": bool
        }
        """
        _groups = kwargs.get(groups, None)
        _reset = kwargs.get(reset, False)

        # reset all resource groups to initial state and all collections will be released
        self.reset_resource_groups(_reset)

        if isinstance(_groups, list):
            # check available nodes
            res = self.utility_wrap.describe_resource_group(name=DEFAULT_RESOURCE_GROUP).response
            if sum(_groups) > res.num_available_node:
                raise Exception("[Base] Default num_available_node:%s is less than required:%s, list:%s" % (
                    res.num_available_node, sum(_groups), _groups))

            # transfer nodes
            lrg = self.utility_wrap.list_resource_groups().response
            for i in range(len(_groups)):
                self.transfer_nodes(
                    TransferNodesParams(source=DEFAULT_RESOURCE_GROUP, target=f"RG_{i}", num_node=_groups[i]), lrg)
                # todo need to check transfer result
            log.info(f"[Base] Transfer all nodes done: {_groups}")

        elif isinstance(_groups, dict):
            _transfer_nodes = _groups.get(transfer_nodes, [])
            _transfer_replicas = _groups.get(transfer_replicas, [])

            lrg = self.utility_wrap.list_resource_groups().response
            # transfer nodes
            for _node in _transfer_nodes:
                self.transfer_nodes(TransferNodesParams(**_node), lrg)
                # todo need to check transfer result
            log.info(f"[Base] Transfer all nodes done: {_transfer_nodes}")

            # transfer replicas
            for _replica in _transfer_replicas:
                self.transfer_replicas(TransferReplicasParams(**_replica), lrg)
                # todo need to check transfer result
            log.info(f"[Base] Transfer all replicas done: {_transfer_replicas}")

    def get_resource_groups(self, **kwargs):
        _rg = kwargs.pop(resource_groups, None)
        if isinstance(_rg, int):
            # get all resource groups
            lrg = self.utility_wrap.list_resource_groups().response
            if len(lrg) == _rg:
                kwargs.update({resource_groups: lrg})
            elif len(lrg) > _rg:
                rg = random.sample(remove_list_values(lrg, DEFAULT_RESOURCE_GROUP), _rg)
                kwargs.update({resource_groups: rg})
            else:
                raise Exception(f"[Base] The current resource groups{lrg}:{len(lrg)} < required:{_rg}")

        elif isinstance(_rg, list):
            kwargs.update({resource_groups: _rg})
        return kwargs

    def show_resource_groups(self, _flag=True):
        if RESOURCE_GROUPS_FLAG and _flag:
            lrg = self.utility_wrap.list_resource_groups().response
            for rg in lrg:
                res = self.utility_wrap.describe_resource_group(name=rg).response
                del_res = str(res).replace("\n", "").replace("\r", "").replace(" ", "")
                log.info(f"[Base] Describe resource group:{rg}, {del_res}")

    def show_collection_replicas(self, timeout=3600):
        res = self.collection_wrap.get_replicas(timeout=timeout).response
        log.info(f"[Base] Collection {self.collection_name} replicas info - {res}")

    def show_segment_info(self, collection_name="", shards_num=2, timeout=3600):
        collection_name = collection_name or self.collection_name
        res = self.utility_wrap.get_query_segment_info(collection_name=collection_name, timeout=timeout).response
        log.debug(f"[Base] Collection {self.collection_name} segment info: \n {res}")
        res_seg = parser_segment_info(segment_info=res, shards_num=shards_num)
        log.info(f"[Base] Parser segment info: \n{pformat(res_seg, sort_dicts=False)}")

    def show_all_resource(self, collection_name="", shards_num=2, show_resource_groups=True):
        self.show_resource_groups(_flag=show_resource_groups)
        self.show_collection_replicas()
        self.show_segment_info(collection_name=collection_name, shards_num=shards_num)

    @staticmethod
    def get_collection_params(collection_obj: ApiCollectionWrapper):
        field_name = None
        dim = None
        metric_type = None
        index_type = None

        for field in collection_obj.schema.fields:
            if field.dtype in [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR]:
                field_name = field.name
                dim = field.params.get("dim")

        if collection_obj.has_index().response:
            metric_type = collection_obj.index().response.params.get("metric_type")
            index_type = collection_obj.index().response.params.get("index_type")
            index_param = collection_obj.index().response.params.get("params")

        log.debug("[Base] Collection {0} field_name: {1}, dim:{2}, metric_type:{3}, index_type:{4}, index_param: {5}".format(
            collection_obj.name, field_name, dim, metric_type, index_type, index_param))
        return field_name, dim, metric_type, index_type, index_param

    def check_collection_load(self, collection_name: str):
        res = self.utility_wrap.loading_progress(collection_name, check_task=CheckTasks.ignore_check)
        result = res.response.get("loading_progress", "") if res.res_result else ""
        return True if result == "100%" else False

    def create_partition(self, collection, partition_name, partition_obj: callable = None, log_level=LogLevel.DEBUG):
        partition_obj = partition_obj or self.partition_wrap
        partition_obj.init_partition(collection, partition_name)
        log.customize(log_level)(
            "[Base] Create partition {0} of collection({1})".format(partition_name, collection.name))

    def drop_partition(self, partition_obj: callable = None, log_level=LogLevel.DEBUG):
        partition_obj = partition_obj or self.partition_wrap
        partition_obj.drop()
        log.customize(log_level)("[Base] Drop partition {0})".format(partition_obj.name))

    def count_partition_entities(self, partition_obj: callable = None, log_level=LogLevel.DEBUG):
        partition_obj = partition_obj or self.partition_wrap
        counts = partition_obj.num_entities
        log.customize(log_level)("[Base] Partition {0} num entities: ({1})".format(partition_obj.name, counts))

    def flush_partition(self, partition_obj: callable = None, log_level=LogLevel.DEBUG):
        partition_obj = partition_obj or self.partition_wrap
        log.customize(log_level)("[Base] Start flush partition {}".format(partition_obj.name))
        return partition_obj.flush()

    def load_partition(self, partition_obj: callable = None, replica_number=1, log_level=LogLevel.DEBUG, **kwargs):
        partition_obj = partition_obj or self.partition_wrap
        kwargs = self.get_resource_groups(**kwargs)
        log.customize(log_level)(
            f"[Base] Start load partition {partition_obj.name},replica_number:{replica_number},kwargs:{kwargs}")
        return partition_obj.load(replica_number=replica_number, **kwargs)

    def release_partition(self, partition_obj: callable = None, log_level=LogLevel.DEBUG, **kwargs):
        partition_obj = partition_obj or self.partition_wrap
        log.customize(log_level)("[Base] Start release partition {}".format([partition_obj.name]))
        return partition_obj.release(**kwargs)

    def search_partition(self, data, anns_field, param, limit, expr=None, partition_obj: callable = None, timeout=300,
                         log_level=LogLevel.DEBUG, **kwargs):
        """
        :return: (result, rt), check_result
        """
        partition_obj = partition_obj or self.partition_wrap
        msg = "[Base] Params of search: nq:{0}, anns_field:{1}, param:{2}, limit:{3}, expr:\"{4}\", kwargs:{5}"
        log.customize(log_level)(msg.format(len(data), anns_field, param, limit, expr, kwargs))
        return partition_obj.search(data, anns_field, param, limit, expr=expr, timeout=timeout, **kwargs)

    def concurrent_search(self, params: ConcurrentTaskSearch):
        if params.random_data:
            params.data = gen_vectors(nb=len(params.data), dim=len(params.data[0]))
        return self.collection_wrap.search(check_task=CheckTasks.assert_result, **params.obj_params)

    def concurrent_query(self, params: ConcurrentTaskQuery):
        return self.collection_wrap.query(check_task=CheckTasks.assert_result, **params.obj_params)

    def concurrent_flush(self, params: ConcurrentTaskFlush):
        return self.collection_wrap.flush(check_task=CheckTasks.assert_result, **params.obj_params)

    def concurrent_load(self, params: ConcurrentTaskLoad):
        return self.collection_wrap.load(check_task=CheckTasks.assert_result, **params.obj_params)

    def concurrent_release(self, params: ConcurrentTaskRelease):
        return self.collection_wrap.release(check_task=CheckTasks.assert_result, **params.obj_params)

    @func_time_catch()
    def concurrent_load_release(self, params: ConcurrentTaskLoadRelease):
        self.collection_wrap.load(check_task=CheckTasks.assert_result, replica_number=params.replica_number,
                                  timeout=params.timeout)
        self.collection_wrap.release(check_task=CheckTasks.assert_result, timeout=params.timeout)
        return "[Base] concurrent_load_release finished."

    def concurrent_insert(self, params: ConcurrentTaskInsert):
        entities = gen_entities(self.collection_schema, params.get_vectors, params.get_ids, params.varchar_filled)
        return self.collection_wrap.insert(entities, check_task=CheckTasks.assert_result, **params.obj_params)

    def concurrent_delete(self, params: ConcurrentTaskDelete):
        return self.collection_wrap.delete(expr="id in {}".format(params.get_ids), check_task=CheckTasks.assert_result,
                                           **params.obj_params)

    @func_time_catch()
    def concurrent_scene_test(self, params: ConcurrentTaskSceneTest):
        log_level = LogLevel.DEBUG
        collection_obj = ApiCollectionWrapper()
        collection_name = gen_unique_str()

        # create collection
        self.create_collection(collection_obj=collection_obj, collection_name=collection_name,
                               vector_field_name=params.vector_field_name, dim=params.dim, log_level=log_level)
        time.sleep(1)

        # insert vectors
        self.insert(data_type="local", dim=params.dim, size=params.data_size, ni=params.nb,
                    collection_obj=collection_obj, collection_name=collection_name,
                    collection_schema=collection_obj.schema.to_dict(), log_level=log_level)

        # flush collection
        self.flush_collection(collection_obj=collection_obj, log_level=log_level)

        # count vectors
        self.count_entities(collection_obj=collection_obj, log_level=log_level)

        # build index
        self.build_index(field_name=params.vector_field_name, index_type=params.index_type,
                         metric_type=params.metric_type, index_param=params.index_param,
                         collection_name=collection_name, collection_obj=collection_obj, log_level=log_level)

        time.sleep(59)
        # drop collection
        log.customize(log_level)("[Base] Drop collection {}.".format(collection_name))
        self.utility_wrap.drop_collection(collection_name)

        return "[Base] concurrent_scene_test finished."

    @func_time_catch()
    def concurrent_scene_insert_delete_flush(self, params: ConcurrentTaskSceneInsertDeleteFlush):
        log_level = LogLevel.DEBUG
        log.customize(log_level)("[Base] Start concurrent_scene_insert_delete_flush: {0}".format(self.collection_name))

        # insert vectors
        entities = gen_entities(self.collection_schema, params.get_vectors, params.get_insert_ids,
                                params.varchar_filled)
        self.collection_wrap.insert(entities)

        # delete vectors
        self.collection_wrap.delete(expr="id in {}".format(params.get_delete_ids))

        # flush collection
        self.flush_collection(log_level=log_level)
        return "[Base] concurrent_scene_insert_delete_flush finished."

    @func_time_catch()
    def concurrent_scene_insert_partition(self, params: ConcurrentTaskSceneInsertPartition):
        log_level = LogLevel.DEBUG
        _dim = self.get_collection_params(self.collection_wrap)[1]
        partition_obj = ApiPartitionWrapper()
        partition_name = gen_unique_str("p")

        # create partition
        self.create_partition(self.collection_wrap.collection, partition_name, partition_obj, log_level=log_level)

        # insert vectors
        self.insert(data_type="local", dim=_dim, size=params.data_size, ni=params.ni,
                    collection_obj=self.collection_wrap, collection_name=self.collection_wrap.name,
                    collection_schema=self.collection_schema, log_level=log_level, partition_name=partition_name, **params.obj_params)

        if params.with_flush:
            self.flush_partition(partition_obj, log_level)

        # drop partition
        self.drop_partition(partition_obj, log_level)
        # time.sleep(5)
        return "[Base] concurrent_scene_insert_partition finished."

    @func_time_catch()
    def concurrent_scene_test_partition(self, params: ConcurrentTaskSceneTestPartition):
        log_level = LogLevel.DEBUG
        _vector_field_name, _dim, _metric_type, _index_type, _index_param = self.get_collection_params(self.collection_wrap)
        partition_obj = ApiPartitionWrapper()
        partition_name = gen_unique_str("sp")

        # create partition
        self.create_partition(self.collection_wrap.collection, partition_name, partition_obj, log_level)

        # insert vectors
        self.insert(data_type="local", dim=_dim, size=params.data_size, ni=params.ni,
                    collection_obj=self.collection_wrap, collection_name=self.collection_wrap.name,
                    collection_schema=self.collection_schema, log_level=log_level, partition_name=partition_name,
                    **params.obj_params)

        # flush partition
        self.flush_partition(partition_obj, log_level)

        # count vectors
        self.count_partition_entities(partition_obj, log_level)

        # create index again
        self.build_index(field_name=_vector_field_name, index_type=_index_type,
                         metric_type=_metric_type, index_param=_index_param, log_level=log_level)

        # load and search
        self.load_partition(partition_obj, log_level=log_level)

        # search partition
        data = gen_vectors(nb=params.nq, dim=_dim)
        self.search_partition(data=data, anns_field=_vector_field_name,
                              param=params.search_param, limit=params.limit,
                              partition_obj=partition_obj,
                              check_task=CheckTasks.assert_result, log_level=log_level, **params.obj_params)

        # release partition and search failed
        self.release_partition(partition_obj, log_level=log_level, timeout=params.timeout, check_task=CheckTasks.assert_result)
        self.search_partition(data=data, anns_field=_vector_field_name,
                              param=params.search_param, limit=params.limit,
                              partition_obj=partition_obj,
                              check_task=CheckTasks.err_res, check_items={dv.err_code: 0,
                                                                          dv.err_msg: f"was not loaded into memory"},
                              log_level=log_level, **params.obj_params)

        # drop partition
        log.customize(log_level)("[Base] Drop partition {}.".format(partition_name))
        self.drop_partition(partition_obj, log_level)
        return "[Base] concurrent_scene_test_partition finished."

    @func_time_catch()
    def concurrent_debug(self, params: DataClassBase):
        # time.sleep(random.randint(1, 6))
        # time.sleep(round(random.random(), 4))
        time.sleep(float(random.randint(1, 20) / 1000.0))
        log.debug("[Base] DataClassBase.obj_params: {}".format(params.obj_params))
        return "[Base] concurrent_debug finished."

    @func_time_catch()
    def concurrent_iterate_search(self, params: ConcurrentTaskIterateSearch):
        # only for checking collections that can be searched
        log_level = LogLevel.DEBUG

        collections = self.utility_wrap.list_collections().response
        log.customize(log_level)("[Base] Start iterate search over all collections {}".format(collections))
        for i in collections:
            if self.check_collection_load(i):
                c = ApiCollectionWrapper()
                c.init_collection(i)

                # get collection params
                params.anns_field, dim, metric_type, index_type, _ = self.get_collection_params(c)

                if params.anns_field and dim and metric_type and index_type:
                    # set search vectors
                    params.data = gen_vectors(nb=params.nq, dim=dim)
                    params.param = update_dict_value(params.param,
                                                     {"metric_type": metric_type,
                                                      "params": get_default_search_params(index_type=index_type)})

                    c.search(check_task=CheckTasks.assert_result, **params.obj_params)
                else:
                    log.customize(log_level)(f"[Base] Can't get collection: {i} params.")
        return "[Base] concurrent_iterate_search finished."

    @func_time_catch()
    def concurrent_load_search_release(self, params: ConcurrentTaskLoadSearchRelease):
        self.collection_wrap.load(check_task=CheckTasks.assert_result, replica_number=params.replica_number,
                                  timeout=params.timeout)

        if params.random_data:
            params.data = gen_vectors(nb=len(params.data), dim=len(params.data[0]))
        self.collection_wrap.search(check_task=CheckTasks.assert_result, **params.obj_params)

        self.collection_wrap.release(check_task=CheckTasks.assert_result, timeout=params.timeout)
        return "[Base] concurrent_load_search_release finished."

    @func_time_catch()
    def concurrent_scene_search_test(self, params: ConcurrentTaskSceneSearchTest):
        log_level = LogLevel.DEBUG
        collection_obj = ApiCollectionWrapper()
        collection_name = gen_unique_str()

        # create collection
        self.create_collection(collection_obj=collection_obj, collection_name=collection_name,
                               vector_field_name=params.vector_field_name, dim=params.dim, log_level=log_level)
        time.sleep(1)

        # insert vectors
        self.insert(data_type="local", dim=params.dim, size=params.data_size, ni=params.nb,
                    collection_obj=collection_obj, collection_name=collection_name,
                    collection_schema=collection_obj.schema.to_dict(), log_level=log_level)

        # flush collection
        self.flush_collection(collection_obj=collection_obj, log_level=log_level)

        # count vectors
        self.count_entities(collection_obj=collection_obj, log_level=log_level)

        # build index
        self.build_index(field_name=params.vector_field_name, index_type=params.index_type,
                         metric_type=params.metric_type, index_param=params.index_param,
                         collection_name=collection_name, collection_obj=collection_obj, log_level=log_level)

        # load collection
        self.load_collection(replica_number=params.replica_number, log_level=log_level)

        # search collection
        self.collection_wrap.search(gen_vectors(nb=params.nq, dim=params.dim), anns_field=params.vector_field_name,
                                    param=update_dict_value({"metric_type": params.metric_type}, params.search_param),
                                    limit=params.top_k, check_task=CheckTasks.assert_result)

        # drop collection
        log.customize(log_level)("[Base] Drop collection {}.".format(collection_name))
        self.utility_wrap.drop_collection(collection_name)

        return "[Base] concurrent_scene_search_test finished."
