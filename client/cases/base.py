from pymilvus import DefaultConfig

from client.client_base import ApiConnectionsWrapper, ApiCollectionWrapper, ApiIndexWrapper, ApiPartitionWrapper, \
    ApiCollectionSchemaWrapper, ApiFieldSchemaWrapper, ApiUtilityWrapper

from parameters.input_params import param_info
from client.common.common_func import gen_collection_schema, gen_unique_str, get_file_list, read_npy_file, \
    parser_data_size, loop_files, loop_ids, gen_vectors, gen_entities, run_gobench_process
from client.common.common_type import Precision
from client.common.common_type import DefaultValue as dv
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

    def __del__(self):
        log.info("[Base] Start disconnect connection.")
        self.remove_connect()

    def connect(self, host=None, port=None, secure=False):
        """ Add a connection and create the connect """
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
                          **kwargs):
        """ Create a collection with default schema """
        schema = gen_collection_schema(vector_field_name=vector_field_name,
                                       other_fields=other_fields,
                                       dim=kwargs.get("dim", dv.default_dim)) if schema is None else schema
        self.collection_name = gen_unique_str() if collection_name == "" else collection_name
        log.info("[Base] Create collection {}".format(self.collection_name))
        return self.collection_wrap.init_collection(self.collection_name, schema=schema, shards_num=shards_num,
                                                    **kwargs)

    def connect_collection(self, collection_name):
        """ Connect to an exist collection """
        self.collection_name = collection_name
        log.info("[Base] Connect collection {}".format(self.collection_name))
        return self.collection_wrap.init_collection(self.collection_name)

    def clean_all_collection(self, clean=True):
        """ Drop all collections in the database """
        if not clean:
            return
        collections = self.utility_wrap.list_collections()[0][0]
        log.info("[Base] Start clean all collections {}".format(collections))
        for i in collections:
            self.utility_wrap.drop_collection(i)

    def clear_collections(self, clean_collection=True):
        if clean_collection:
            log.info("[Base] Start clear collections")
            self.collection_wrap.release()
            self.clean_all_collection()
            log.info("[Base] Clear collections Done!")
        # log.info("[Base] Start disconnect connection.")
        # self.remove_connect()

    def flush_collection(self):
        log.info("[Base] Start flush collection {}".format(self.collection_wrap.name))
        return self.collection_wrap.flush()

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

    def insert_batch(self, vectors, ids, data_size):
        if self.collection_schema is None:
            self.get_collection_schema()
        if isinstance(vectors, list):
            entities = gen_entities(self.collection_schema, vectors, ids)
        else:
            entities = gen_entities(self.collection_schema, vectors.tolist(), ids)
        log.info("[Base] Start inserting, ids: {0} - {1}, data size: {2}".format(ids[0], ids[-1], data_size))
        res = self.collection_wrap.insert(entities)[0][1]
        self.count_entities()
        return res

    def insert(self, data_type, dim, size, ni):
        data_size = parser_data_size(size)
        data_size_format = str(format(data_size, ',d'))
        ni_cunt = int(data_size / int(ni))
        last_insert = data_size % int(ni)

        batch_rt = 0
        last_rt = 0

        log.info("[Base] Start inserting {} vectors".format(data_size))

        _loop_ids = loop_ids(int(ni))

        if data_type == "local":

            for i in range(0, ni_cunt):
                batch_rt += self.insert_batch(gen_vectors(ni, dim), next(_loop_ids), data_size_format)

            if last_insert > 0:
                last_rt = self.insert_batch(gen_vectors(last_insert, dim), next(_loop_ids)[:last_insert],
                                            data_size_format)

        else:
            files = get_file_list(data_size, dim, data_type)
            if len(files) == 0:
                raise Exception("[insert] Can not get files, please check.")

            _loop_file = loop_files(files)
            vectors = []

            for i in range(0, ni_cunt):
                if len(vectors) < ni:
                    while True:
                        vectors.extend(read_npy_file(next(_loop_file)))
                        if len(vectors) >= ni:
                            break
                batch_rt += self.insert_batch(vectors[:ni], next(_loop_ids), data_size_format)
                vectors = vectors[ni:]

            if last_insert > 0:
                if len(vectors) < last_insert:
                    while True:
                        vectors.extend(read_npy_file(next(_loop_file)))
                        if len(vectors) >= last_insert:
                            break
                last_rt = self.insert_batch(vectors[:last_insert], next(_loop_ids)[:last_insert], data_size_format)

        total_time = round((batch_rt + last_rt), Precision.COMMON_PRECISION)
        ips = round(int(data_size) / total_time, Precision.INSERT_PRECISION)
        ni_time = round(batch_rt / ni_cunt, Precision.INSERT_PRECISION) if ni_cunt != 0 else 0
        msg = "[Base] Total time of insert: {0}s, average number of vector bars inserted per second: {1}," + \
              " average time to insert {2} vectors per time: {3}s"
        log.info(msg.format(total_time, ips, ni, ni_time))
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

    def build_index(self, field_name, index_type, metric_type, index_param):
        """
        {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}}
        """
        index_params = {"index_type": index_type, "metric_type": metric_type, "params": index_param}
        log.info("[Base] Start build index of {0}, params:{1}".format(index_type, index_params))
        return self.index_wrap.init_index(self.collection_wrap.collection, field_name, index_params)

    def show_index(self):
        if self.collection_wrap.has_index()[0][0]:
            index_params = self.collection_wrap.index()[0][0].params
            log.info("[Base] Params of index: {}".format(index_params))
            return index_params
        log.info("[Base] Collection:{0} is not building index".format(self.collection_wrap.name))
        return {}

    def clean_index(self):
        if self.collection_wrap.has_index()[0][0]:
            self.collection_wrap.drop_index()
            if self.collection_wrap.has_index()[0][0]:
                log.error("[Base] Index of collection {0} can not be cleaned.".format(self.collection_wrap.name))
                return False
        log.info("[Base] Clean all index done.")
        return True

    def count_entities(self):
        counts = self.collection_wrap.num_entities
        log.info("[Base] Number of vectors in the collection({0}): {1}".format(self.collection_wrap.name, counts))

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

    def search(self, data, anns_field, param, limit, expr=None, timeout=300):
        """
        :return: (result, rt), check_result
        """
        msg = "[Base] Params of search: nq:{0}, anns_field:{1}, param:{2}, limit:{3}, expr:\"{4}\""
        log.info(msg.format(len(data), anns_field, param, limit, expr))
        return self.collection_wrap.search(data, anns_field, param, limit, expr=expr, timeout=timeout)

    def go_search(self, data, anns_field, param, limit, expr=None, timeout=300):
        """
        :return: result
        """
        msg = "[Base] Params of search: nq:{0}, anns_field:{1}, param:{2}, limit:{3}, expr:\"{4}\""
        log.info(msg.format(len(data), anns_field, param, limit, expr))

        return run_gobench_process([])
