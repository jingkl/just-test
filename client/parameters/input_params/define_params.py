from client.parameters import params_name as pn


search_expr = ["{'float_1': {'GT': -1.0, 'LT': %s * 0.1}}" % pn.dataset_size,
               "{'float_1': {'GT': -1.0, 'LT': %s * 0.5}}" % pn.dataset_size,
               "{'float_1': {'GT': -1.0, 'LT': %s * 0.9}}" % pn.dataset_size]

other_fields = ["int64_1", "int64_2", "float_1", "double_1", "varchar_1"]


class DefaultIndexParams:
    FLAT = {pn.index_type: pn.IndexTypeName.FLAT, pn.index_param: {}}
    IVF_SQ8 = {pn.index_type: pn.IndexTypeName.IVF_SQ8, pn.index_param: {pn.nlist: 1024}}
    HNSW = {pn.index_type: pn.IndexTypeName.HNSW, pn.index_param: {"M": 8, "efConstruction": 200}}
    DISKANN = {pn.index_type: pn.IndexTypeName.DISKANN, pn.index_param: {}}
