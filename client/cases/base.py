from pymilvus import DefaultConfig

from client.client_base import ApiConnectionsWrapper, ApiCollectionWrapper, ApiIndexWrapper, ApiPartitionWrapper, \
    ApiCollectionSchemaWrapper, ApiFieldSchemaWrapper, ApiUtilityWrapper

from commons.common_params import param_info
from client.common.common_func import gen_collection_schema, gen_unique_str, get_file_list, read_npy_file, \
    parser_data_size, loop_files, loop_ids, gen_vectors
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

    def connect(self, host=param_info.param_host, port=param_info.param_port):
        """ Add a connection and create the connect """
        return self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, host=host, port=port)

    def create_collection(self, collection_name="", schema=None, other_fields=[], shards_num=2, **kwargs):
        schema = gen_collection_schema(other_fields=other_fields) if schema is None else schema
        self.collection_name = gen_unique_str() if collection_name == "" else collection_name
        return self.collection_wrap.init_collection(self.collection_name, schema=schema, shards_num=shards_num,
                                                    **kwargs)

    def drop_collection(self):
        return self.collection_wrap.drop()

    def list_collection(self):
        return self.utility_wrap.list_collections()[0]

    def clean_all_collection(self):
        for i in self.list_collection():
            self.utility_wrap.drop_collection(i)

    def flush_collection(self):
        return self.collection_wrap.flush()

    def load_collection(self, replica_number=1):
        return self.collection_wrap.load(replica_number=replica_number)

    def insert_to_collection(self, data):
        return self.collection_wrap.insert(data)

    def insert_batch(self, vectors, ids):
        entities = gen_entities(self.collection_wrap.schema.to_dict(), vectors, ids)
        log.info("[insert_batch] Start insert, ids: {0} - {1}".format(ids[0], ids[-1]))
        return self.collection_wrap.insert(entities, _rt=True)[1]

    def insert(self, data_type, dim, size, ni):
        ni_cunt = int(parser_data_size(size) / int(ni))
        last_insert = parser_data_size(size) % int(ni)

        batch_rt = 0
        last_rt = 0

        _loop_ids = loop_ids(int(ni))

        if data_type == "local":

            for i in range(0, ni_cunt):
                batch_rt += self.insert_batch(gen_vectors(ni, dim), next(_loop_ids))

            if last_insert > 0:
                last_rt = self.insert_batch(gen_vectors(last_insert, dim), next(_loop_ids)[:last_insert])

        else:
            files = get_file_list(size, dim, data_type)
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
                batch_rt += self.insert_batch(vectors[:ni], next(_loop_ids))
                vectors = vectors[ni:]

            if last_insert > 0:
                if len(vectors) < last_insert:
                    while True:
                        vectors.extend(read_npy_file(next(_loop_file)))
                        if len(vectors) >= last_insert:
                            break
                last_rt = self.insert_batch(vectors[:last_insert], next(_loop_ids)[:last_insert])

        # todo: time update
        print("total time: {}".format(batch_rt + last_rt))

    def ann_insert(self, source_file):
        pass

    def build_index(self, field_name, index_type, metric_type, index_param):
        """
        {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}}
        """
        index_params = {"index_type": index_type, "metric_type": metric_type, "params": index_param}
        return self.index_wrap.init_index(self.collection_wrap.collection, field_name, index_params)

    def drop_index(self):
        return self.index_wrap.drop()


if __name__ == '__main__':
    import random
    import os
    import time
    import pandas as pd
    import numpy as np
    from client.common.common_func import gen_entities, gen_ids, gen_file_name

    base = Base()
    print(base.connect())
    base.clean_all_collection()
    print(base.create_collection())
    # print(base.build_index("float_vector", "IVF_FLAT", "L2", {"nlist": 128}))
    # print(base.collection_wrap.schema)
    # vectors = gen_vectors(nb=2, dim=128)
    # ids = gen_ids(0, 2)
    # print(base.insert_to_collection([[random.randint(1, 100) for _ in range(2)],
    #                                  [[random.random() for _ in range(128)] for _ in range(2)]]))
    # print(base.insert_to_collection(pd.DataFrame({
    #     "id": [random.randint(1, 100) for _ in range(2)],
    #     "float_vector": [[random.random() for _ in range(128)] for _ in range(2)],
    #     # "id": [random.randint(1, 100) for _ in range(2)],
    # })))
    # entities = gen_entities(base.collection_wrap.schema.to_dict(), vectors, ids)
    # print(entities)
    # print(base.insert_to_collection(entities))
    # print(base.collection_wrap.num_entities)
    data_size = "10"
    data_type = "local"
    ni_per = 300000
    dim = 128
    print(base.insert(data_type, dim, data_size, ni_per))
    # t = time.time()
    # file_list = get_file_list(data_size, dim, data_type)
    # print(file_list)
    # print(time.time() - t)
    # _f_name = gen_file_name(1, dim, datatype)
    # print(_f_name)
    # print(os.path.isfile(_f_name))

    # print(pd.DataFrame({"id": [random.randint(1, 100) for _ in range(2)],
    #                     "float_vector": [[random.random() for _ in range(128)] for _ in range(2)]}))
    # print("**")
    # _list = range(10)
    # print(np.fromiter(iter(_list), dtype=float))
    #
    # duration = "1d"
    # duration = duration.replace('d', '*3600*24+').replace('h', '*3600+').replace('m', '*60+').replace('s', '*1+') + '0'
    # print(duration)
    # print(eval(duration))
