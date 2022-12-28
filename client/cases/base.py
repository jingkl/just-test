import time
import random
from pymilvus import DefaultConfig

from client.client_base import ApiConnectionsWrapper, ApiCollectionWrapper, ApiIndexWrapper, ApiPartitionWrapper, \
    ApiCollectionSchemaWrapper, ApiFieldSchemaWrapper, ApiUtilityWrapper

from parameters.input_params import param_info
from client.common.common_func import gen_collection_schema, gen_unique_str, get_file_list, read_npy_file, \
    parser_data_size, loop_files, loop_ids, gen_vectors, gen_entities, run_go_bench_process, go_bench, GoSearchParams, \
    loop_gen_files
from client.common.common_type import Precision, CheckTasks
from client.common.common_type import DefaultValue as dv
from client.parameters.params import ConcurrentTaskSearch, ConcurrentTaskQuery, ConcurrentTaskFlush, ConcurrentTaskLoad, \
    ConcurrentTaskRelease, ConcurrentTaskInsert, ConcurrentTaskDelete, ConcurrentTaskSceneTest, \
    ConcurrentTaskSceneInsertDeleteFlush, DataClassBase
from client.util.api_request import func_time_catch
from commons.common_params import EnvVariable
from commons.common_type import LogLevel
from utils.util_log import log


class Base:

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
        if self.connection_wrap.has_connection(alias=alias)[0][0]:
            log.info("[Base] Disconnect alias: {0}".format(alias))
            self.connection_wrap.remove_connection(alias=alias)

    def create_collection(self, collection_name="", vector_field_name="", schema=None, other_fields=[], shards_num=2,
                          collection_obj: callable = None, log_level=LogLevel.INFO, varchar_id=False, **kwargs):
        """ Create a collection with default schema """
        schema = gen_collection_schema(vector_field_name=vector_field_name, other_fields=other_fields,
                                       varchar_id=varchar_id,
                                       max_length=kwargs.pop("max_length", dv.default_max_length),
                                       dim=kwargs.pop("dim", dv.default_dim)) if schema is None else schema
        collection_name = collection_name or gen_unique_str()
        self.collection_name = self.collection_name or collection_name

        # self.collection_name = gen_unique_str() if collection_name == "" else collection_name
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
            self.remove_connect()
            return
        collections = self.utility_wrap.list_collections()[0][0]
        log.info("[Base] Start clean all collections {}".format(collections))
        for i in collections:
            self.utility_wrap.drop_collection(i)

    def clear_collections(self, clean_collection=True):
        if clean_collection:
            log.info("[Base] Start clear collections")
            self.collection_wrap.release()
            log.info("[Base] Release collections done")
            self.clean_all_collection()
            log.info("[Base] Clear collections Done!")
        # log.info("[Base] Start disconnect connection.")
        # self.remove_connect()

    def flush_collection(self, collection_obj: callable = None, log_level=LogLevel.INFO):
        collection_obj = collection_obj or self.collection_wrap
        log.customize(log_level)("[Base] Start flush collection {}".format(collection_obj.name))
        return collection_obj.flush()

    def load_collection(self, replica_number=1):
        log.info(
            "[Base] Start load collection {0}, replica_number:{1}".format(self.collection_wrap.name, replica_number))
        return self.collection_wrap.load(replica_number=replica_number)

    def release_collection(self):
        log.info("[Base] Start release collection {}".format(self.collection_wrap.name))
        return self.collection_wrap.release()

    def get_collection_schema(self):
        self.collection_schema = self.collection_wrap.schema.to_dict()
        log.info("[Base] Collection schema: {0}".format(self.collection_schema))

    def insert_batch(self, vectors, ids, data_size, varchar_filled=False, collection_obj: callable = None,
                     collection_schema=None, log_level=LogLevel.INFO):
        if self.collection_schema is None and collection_schema is None:
            self.get_collection_schema()
            collection_schema = self.collection_schema
        else:
            collection_schema = collection_schema or self.collection_schema

        if isinstance(vectors, list):
            entities = gen_entities(collection_schema, vectors, ids, varchar_filled)
        else:
            entities = gen_entities(collection_schema, vectors.tolist(), ids, varchar_filled)

        log.customize(log_level)(
            "[Base] Start inserting, ids: {0} - {1}, data size: {2}".format(ids[0], ids[-1], data_size))
        collection_obj = collection_obj or self.collection_wrap
        res = collection_obj.insert(entities)[0][1]
        self.count_entities(collection_obj=collection_obj, log_level=log_level)
        return res

    def insert(self, data_type, dim, size, ni, varchar_filled=False, collection_obj: callable = None,
               collection_schema=None, collection_name="", log_level=LogLevel.INFO):
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

        if data_type == "local":

            for i in range(0, ni_cunt):
                batch_rt += self.insert_batch(gen_vectors(ni, dim), next(_loop_ids), data_size_format, varchar_filled,
                                              collection_obj, collection_schema, log_level)

            if last_insert > 0:
                last_rt = self.insert_batch(gen_vectors(last_insert, dim), next(_loop_ids)[:last_insert],
                                            data_size_format, varchar_filled, collection_obj, collection_schema,
                                            log_level)

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
                                              collection_obj, collection_schema, log_level)
                vectors = vectors[ni:]

            if last_insert > 0:
                if len(vectors) < last_insert:
                    while True:
                        vectors.extend(read_npy_file(next(_loop_file)))
                        if len(vectors) >= last_insert:
                            break
                last_rt = self.insert_batch(vectors[:last_insert], next(_loop_ids)[:last_insert], data_size_format,
                                            varchar_filled, collection_obj, collection_schema, log_level)

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

    def ann_insert(self, source_vectors, ni=100):
        size = len(source_vectors)
        data_size_format = str(format(size, ',d'))
        ni_cunt = int(size / int(ni))
        last_insert = size % int(ni)

        batch_rt = 0
        last_rt = 0

        _loop_ids = loop_ids(int(ni))

        log.info("[Base] Start inserting {} vectors".format(size))

        for i in range(ni_cunt):
            batch_rt += self.insert_batch(source_vectors[int(i * ni):int((i + 1) * ni)], next(_loop_ids),
                                          data_size_format)

        if last_insert > 0:
            last_rt = self.insert_batch(source_vectors[-last_insert:], next(_loop_ids)[:last_insert], data_size_format)

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
        if self.collection_wrap.has_index()[0][0]:
            index_params = self.collection_wrap.index()[0][0].params
            log.info("[Base] Params of index: {}".format(index_params))
            return index_params
        log.info("[Base] Collection:{0} is not building index".format(self.collection_wrap.name))
        return {}

    def describe_collection_index(self):
        indexes = self.collection_wrap.indexes
        log.info("[Base] Collection:{0} indexes".format(self.collection_wrap.name, indexes))
        return indexes

    def clean_index(self):
        if self.collection_wrap.has_index()[0][0]:
            self.collection_wrap.drop_index()
            if self.collection_wrap.has_index()[0][0]:
                log.error("[Base] Index of collection {0} can not be cleaned.".format(self.collection_wrap.name))
                return False
        log.info("[Base] Clean all index done.")
        return True

    def count_entities(self, collection_obj: callable = None, log_level=LogLevel.INFO):
        collection_obj = collection_obj or self.collection_wrap
        counts = collection_obj.num_entities
        log.customize(log_level)("[Base] Number of vectors in the collection({0}): {1}".format(collection_obj.name,
                                                                                               counts))

    def query(self, ids=None, expr=None):
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

        log.info("[Base] expr of query: \"{0}\"".format(_expr))
        return self.collection_wrap.query(expr=_expr)

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
                    collection_obj=collection_obj, collection_name=collection_name, log_level=log_level)

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
    def concurrent_debug(self, params: DataClassBase):
        # time.sleep(random.randint(1, 6))
        # time.sleep(round(random.random(), 4))
        time.sleep(float(random.randint(1, 20) / 1000.0))
        log.debug("[Base] DataClassBase.obj_params: {}".format(params.obj_params))
        return "[Base] concurrent_debug finished."
