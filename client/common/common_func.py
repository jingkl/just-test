import os
import random
import json
import math
import copy
import operator
import string
import numpy as np
import pandas as pd
import h5py
import sklearn
from sklearn import preprocessing
from itertools import product
import subprocess

from pymilvus import DataType
from client.client_base.schema_wrapper import ApiCollectionSchemaWrapper, ApiFieldSchemaWrapper
from client.common.common_type import DefaultValue as dv
# from client.common.common_type import ParamsCheckType as pct
from client.parameters import params_name as pn
from client.common.common_type import NAS, SimilarityMetrics, AccMetrics
from client.common.common_param import DatasetPath
from utils.util_log import log

"""API func"""


def gen_unique_str(str_value=None):
    prefix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
    return "fouram_" + prefix if str_value is None else str_value + "_" + prefix


def field_type():
    """
    'bool', 'int8', 'int16', 'int32', 'int64', 'float', 'double',
    'string', 'varchar', 'binary_vector', 'float_vector'
    """
    data_types = dict(DataType.__members__)
    _field_types = {}
    for i in data_types.keys():
        if str(i) not in ["NONE", "UNKNOWN"]:
            _field_types.update(i=data_types[i])
    data_types = dict(sorted(data_types.items(), key=lambda item: item[0], reverse=True))
    log.debug("[field_type] Currently supported data types include: {}".format(data_types))
    return data_types


def gen_field_schema(name: str, dtype=None, description=dv.default_desc, is_primary=False, **kwargs):
    field_types = field_type()
    if dtype is None:
        for _field in field_types.keys():
            if name.startswith(_field.lower()):
                _kwargs = {}
                if _field in ["STRING", "VARCHAR"]:
                    _kwargs.update({"max_length": kwargs.get("max_length", dv.default_max_length)})
                if _field in ["BINARY_VECTOR", "FLOAT_VECTOR"]:
                    _kwargs.update({"dim": kwargs.get("dim", dv.default_dim)})
                return ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=field_types[_field],
                                                                 description=description, is_primary=is_primary,
                                                                 **_kwargs)[0][0]
    else:
        if dtype in field_types.values():
            return ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=dtype, description=description,
                                                             is_primary=is_primary, **kwargs)[0][0]
    log.error("[gen_field_schema] The field schema for generating {0} is not supported, please check.".format(name))
    return []


def gen_collection_schema(vector_field_name="", description=dv.default_desc, default_fields=True, auto_id=False,
                          other_fields=[], primary_field=None, **kwargs):
    fields = [gen_field_schema("id", dtype=DataType.INT64, is_primary=True),
              gen_field_schema(vector_field_name, dim=kwargs.get("dim", dv.default_dim))] if default_fields else []

    for _field in other_fields:
        fields.append(gen_field_schema(_field, **kwargs))
    log.debug("[gen_collection_schema] The generated field schema contains the following:{}".format(fields))
    return ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                               auto_id=auto_id, primary_field=primary_field)[0][0]


""" param handling """


def get_recall_value(true_ids, result_ids):
    """
    Use the intersection length
    """
    sum_radio = 0.0
    topk_check = True
    for index, item in enumerate(result_ids):
        # tmp = set(item).intersection(set(flat_id_list[index]))
        tmp = set(true_ids[index]).intersection(set(item))
        if len(item) != 0:
            sum_radio += len(tmp) / len(item)
        else:
            topk_check = False
            log.error("[get_recall_value] Length of returned topk is 0, please check.")
    if topk_check is False:
        raise ValueError("[get_recall_value] The result of topk is wrong, please check: {}".format(result_ids))
    return round(sum_radio / len(result_ids), 3)


def get_search_ids(result):
    ids = []
    for res in result:
        ids.append(res.ids)
    return ids


def get_default_field_name(data_type=DataType.FLOAT_VECTOR):
    if data_type == DataType.FLOAT_VECTOR:
        field_name = dv.default_float_vec_field_name
    elif data_type == DataType.BINARY_VECTOR:
        field_name = dv.default_binary_vector_name
    elif data_type == DataType.INT64:
        field_name = dv.default_int64_field_name
    elif data_type == DataType.FLOAT:
        field_name = dv.default_float_field_name
    elif data_type == DataType.VARCHAR:
        field_name = dv.default_varchar_field_name
    else:
        msg = "[get_default_field_name] Not supported data type: {}".format(data_type)
        log.error(msg)
        raise Exception(msg)
    return field_name


def get_vector_type(data_type):
    if data_type in ["random", "sift", "deep", "glove", "local"]:
        vector_type = DataType.FLOAT_VECTOR
    elif data_type in ["binary", "kosarak"]:
        vector_type = DataType.BINARY_VECTOR
    else:
        raise Exception("Data type: %s not defined" % data_type)
    return vector_type


def gen_file_name(file_id, dim, data_type):
    file_name = "%s_%sd_%05d.npy" % (dv.FILE_PREFIX, str(dim), int(file_id))
    if data_type in DatasetPath.keys():
        return DatasetPath[data_type] + file_name
    else:
        log.error("[gen_file_name] data type not supported: {}".format(data_type))
        return ""


def parser_data_size(data_size):
    return eval(str(data_size).replace("k", "*1000").replace("w", "*10000").replace("m", "*1000000").replace("b",
                                                                                                             "*1000000000"))


def get_file_list(data_size, dim, data_type):
    """
    :param data_size: end with w/m/b or number
    :param dim: int
    :param data_type: random/deep/jaccard/hamming/sift/binary/structure
    :return: list of file name
    """
    data_size = parser_data_size(data_size)
    file_names = []
    for i in range(dv.Max_file_count):
        file_name = gen_file_name(i, dim, data_type)
        file_names.append(file_name)

        file_size = len(read_npy_file(file_name))
        data_size -= file_size
        if data_size <= 0:
            break
    if data_size > 0:
        log.error("[get_file_list] The current dataset size is less than {}".format(data_size))
        return []
    return file_names


def gen_vectors(nb, dim):
    return [[random.random() for _ in range(dim)] for _ in range(nb)]


def gen_ids(start_id, end_id):
    log.debug("[gen_ids] Start id: %s, end id: %s" % (start_id, end_id))
    return [k for k in range(start_id, end_id)]


def gen_values(data_type, vectors, ids):
    values = None
    if data_type in [DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64]:
        values = ids
    elif data_type in [DataType.DOUBLE]:
        values = [(i + 0.0) for i in ids]
    elif data_type == DataType.FLOAT:
        values = pd.Series(data=[(i + 0.0) for i in ids], dtype="float32")
    elif data_type in [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR]:
        values = vectors
    elif data_type in [DataType.VARCHAR]:
        values = [str(i) for i in ids]
    return values


def gen_entities(info, vectors=None, ids=None):
    if not isinstance(info, dict):
        log.error("[gen_entities] info is not a dict, please check: {}".format(type(info)))
        return {}
    if "fields" not in info:
        log.error("[gen_entities] fields not in info, please check: {}".format(info))
        return {}

    entities = {}
    for field in info["fields"]:
        _type = field["type"]
        entities.update({field["name"]: gen_values(_type, vectors, ids)})
    return pd.DataFrame(entities)


def normalize_data(metric_type, X):
    if metric_type == SimilarityMetrics.IP:
        log.info("[normalize_data] Set normalize for metric_type: %s" % metric_type)
        X = preprocessing.normalize(X, axis=1, norm='l2')
        X = X.astype(np.float32)

    elif metric_type == SimilarityMetrics.L2:
        X = X.astype(np.float32)

    elif metric_type in [SimilarityMetrics.Jaccard, SimilarityMetrics.Hamming, SimilarityMetrics.Substructure,
                         SimilarityMetrics.Superstructure]:
        tmp = []
        for item in X:
            tmp.append(bytes(np.packbits(item, axis=-1).tolist()))
        X = tmp
    return X


def get_source_file(file_name: str):
    """ The file name consists of three parts: <dataset type>-<vector dimension>-<distance>, e.g.:sift-128-euclidean """
    file_path = "{0}{1}.hdf5".format(NAS.ANN_DATA_DIR, file_name)

    if check_file_exist(file_path):
        return file_path

    msg = "[get_source_file] Can not get source file: {}, please check".format(file_path)
    log.error(msg)
    raise Exception(msg)


def get_acc_metric_type(file_name: str):
    metric = file_name.split('-')[-1]
    if not hasattr(AccMetrics, metric):
        msg = "[get_acc_metric_type] Can not get the metric type({0}) of file:{1}, please check".format(metric,
                                                                                                        file_name)
        log.error(msg)
        raise Exception(msg)
    return eval("AccMetrics.{}".format(metric))


def gen_combinations(args):
    if isinstance(args, list):
        flat = [el if isinstance(el, list) else [el] for el in args]
        return [list(x) for x in product(*flat)]
    elif isinstance(args, dict):
        flat = []
        for k, v in args.items():
            if isinstance(v, list):
                flat.append([(k, el) for el in v])
            else:
                flat.append([(k, v)])
        return [dict(x) for x in product(*flat)]
    else:
        raise TypeError("[gen_combinations] No args handling exists for %s" % type(args).__name__)


def compare_expr(left, comp, right):
    if comp == "LT":
        return "{0} < {1}".format(left, right)
    elif comp == "LE":
        return "{0} <= {1}".format(left, right)
    elif comp == "EQ":
        return "{0} == {1}".format(left, right)
    elif comp == "NE":
        return "{0} != {1}".format(left, right)
    elif comp == "GE":
        return "{0} >= {1}".format(left, right)
    elif comp == "GT":
        return "{0} > {1}".format(left, right)
    raise Exception("[compare_expr] Not support expr: {0}".format(comp))


def parser_search_params_expr(expr):
    """
    :param expr:
        LT: less than
        LE: less than or equal to
        EQ: equal to
        NE: not equal to
        GE: greater than or equal to
        GT: greater than
    :return: expression of search
    """
    expression = ""
    if isinstance(expr, str):
        return expr
    elif isinstance(expr, dict):
        for key, value in expr.items():
            field_name = key
            if isinstance(value, dict):
                for k, v in value.items():
                    _e = compare_expr(field_name, k, v)
                    expression = _e if expression == "" else "{0} && {1}".format(expression, _e)
    else:
        raise Exception("[parser_search_params_expr] Can't parser search expression: {0}, type:{1}".format(expr,
                                                                                                           type(expr)))
    if expression == "":
        expression = None
    return expression


def get_vectors_from_binary(nq, dimension, dataset_name):
    if dataset_name in ["sift", "deep", "binary"]:
        # dataset_name: local, sift, deep, binary
        file_name = DatasetPath[dataset_name] + "query.npy"

    elif dataset_name in ["random"]:
        file_name = DatasetPath[dataset_name] + "query_%d.npy" % dimension

    elif dataset_name == "local":
        return gen_vectors(nq, dimension)

    else:
        raise Exception("[get_vectors_from_binary] Not support dataset: {0}, please check".format(dataset_name))

    data = np.load(file_name)
    if nq > len(data):
        raise Exception("[get_vectors_from_binary] nq large than file support({0})".format(len(data)))
    vectors = data[0:nq].tolist()
    return vectors


""" common func """


def dict_recursive_key(_dict, key=None):
    if isinstance(_dict, dict):
        key_list = list(_dict.keys())

        for k in key_list:
            if isinstance(_dict[k], dict):
                dict_recursive_key(_dict[k], key)

            if key is None:
                if _dict[k] is key:
                    del _dict[k]
            else:
                if _dict[k] == key:
                    del _dict[k]
    return _dict


def check_file_exist(file_dir):
    if not os.path.isfile(file_dir):
        msg = "[check_file_exist] File not exist:{}".format(file_dir)
        log.error(msg)
        return False
    return True


def modify_file(file_path_list, is_modify=False, input_content=""):
    """
    file_path_list : file list -> list[<file_path>]
    is_modify : does the file need to be reset
    input_content ：the content that need to insert to the file
    """
    if not isinstance(file_path_list, list):
        log.error("[modify_file] file is not a list.")

    for file_path in file_path_list:
        folder_path, file_name = os.path.split(file_path)
        if not os.path.isdir(folder_path):
            log.debug("[modify_file] folder(%s) is not exist." % folder_path)
            os.makedirs(folder_path)

        if not os.path.isfile(file_path):
            log.error("[modify_file] file(%s) is not exist." % file_path)
        else:
            if is_modify is True:
                log.debug("[modify_file] start modifying file(%s)..." % file_path)
                with open(file_path, "r+") as f:
                    f.seek(0)
                    f.truncate()
                    f.write(input_content)
                    f.close()
                log.info("[modify_file] file(%s) modification is complete." % file_path)


def read_json_file(file_name):
    if check_file_exist(file_name):
        with open(file_name) as f:
            file_dict = json.load(f)
            f.close()
        return file_dict
    msg = "[read_json_file] Can not read json file, please check."
    log.error(msg)
    return {}


def read_npy_file(file_name):
    if check_file_exist(file_name):
        file_list = np.load(file_name).tolist()
        return file_list
    msg = "[read_npy_file] Can not read npy file, please check."
    log.error(msg)
    return []


def read_hdf5_file(file_name):
    if check_file_exist(file_name):
        return h5py.File(file_name)
    msg = "[read_hdf5_file] Can not read hdf5 file, please check."
    log.error(msg)
    return []


def read_ann_hdf5_file(file_name):
    """
    contains 4 fields:
        neighbors: used to compare with search results, topk <= columns(100), nq <= rows(10000)
        test: vector argument for search
        train: vector to insert into database
        distances: dis between neighbors and test
    """
    file_list = read_hdf5_file(file_name)
    for i in ["neighbors", "test", "train"]:
        if i not in file_list:
            log.error("[read_ann_hdf5_file] File does not contain field:{}".format(i))
            return []
    return file_list


def read_file(file_path, block_size=1024):
    with open(file_path, 'rb') as f:
        while True:
            block = f.read(block_size)
            if block:
                yield block
            else:
                return ''


def loop_files(files):
    for file in files:
        yield file


def loop_ids(step=50000, start_id=0):
    while True:
        ids = [k for k in range(start_id, start_id + int(step))]
        start_id = start_id + int(step)
        yield ids


def dict_update(source, target):
    for key, value in source.items():
        if isinstance(value, dict) and key in target:
            dict_update(source[key], target[key])
        else:
            target[key] = value
    return target


def update_dict_value(server_resource, values_dict):
    if not isinstance(server_resource, dict) or not isinstance(values_dict, dict):
        return values_dict

    _source = copy.deepcopy(server_resource)
    _target = copy.deepcopy(values_dict)

    target = dict_update(_source, _target)

    return target


def check_key_exist(source: dict, target: dict):
    global flag
    flag = True

    def check_keys(_source, _target):
        global flag
        for key, value in _source.items():
            if key in _target and isinstance(value, dict):
                check_keys(_source[key], _target[key])
            elif key not in _target:
                log.error("[check_key_exist] Key: '{0}' must exist in target: {1}".format(key, _target))
                flag = False

    check_keys(source, target)
    return flag


def del_recursive(_dict, target):
    if isinstance(_dict, dict):
        for k in _dict.keys():
            if isinstance(_dict[k], dict) and len(_dict[k]) > 0:
                del_recursive(_dict[k], target[k])
            elif isinstance(_dict[k], dict) and len(_dict[k]) == 0:
                del target[k]
    return target


def check_exist(_dict):
    if isinstance(_dict, dict):
        for k in _dict.keys():
            if isinstance(_dict[k], dict) and len(_dict[k]) > 0:
                check_exist(_dict[k])
            elif isinstance(_dict[k], dict) and len(_dict[k]) == 0:
                return True
    return False


def params_recursive_del(_dict, target):
    if isinstance(_dict, dict):
        for k in _dict.keys():
            if isinstance(_dict[k], dict):
                params_recursive_del(_dict[k], target[k])
            elif _dict[k][1] == pn.OPTION:
                del target[k]
    return target


def max_depth(_dict):
    if not isinstance(_dict, dict):
        return 0
    if isinstance(_dict.values, dict):
        return 1
    if len(_dict.values()) == 0:
        return 1
    else:
        return 1 + max(max_depth(child) for child in _dict.values())


def get_must_params(source):
    del_option = params_recursive_del(source, copy.deepcopy(source))

    for i in range(max_depth(del_option)):
        del_option = del_recursive(del_option, copy.deepcopy(del_option))
        if not check_exist(del_option):
            break

    if check_exist(del_option):
        msg = "[get_must_params] Get must params failed, please check: {}".format(del_option)
        log.error(msg)
        raise Exception(msg)

    return del_option


def get_params(source, target, result: dict):
    for key, value in target.items():
        if isinstance(value, dict) and key in source:
            result.update({key: {}})
            get_params(source[key], target[key], result[key])
        elif key in source:
            result.update({key: source[key]})
    return result


def get_required_params(source, target):
    if not isinstance(source, dict) or not isinstance(target, dict):
        return source

    result = {}
    result = get_params(source, target, result)
    return result


def check_params_type(source: dict, target: dict):
    global flag
    flag = True

    def check_types(_s, _t):
        global flag
        for key, value in _t.items():
            if key in _s:
                if isinstance(value, dict):
                    check_types(_s[key], _t[key])
                else:
                    if not type(_s[key]) in value[0]:
                        log.error("[check_params_type] Params:{0} type:{1} not supported:{2}".format({key: _s[key]},
                                                                                                     type(_s[key]),
                                                                                                     value[0]))
                        flag = False

    check_types(source, target)
    return flag


def run_gobench_process(params: list):
    process = subprocess.Popen(params, stderr=subprocess.PIPE)
    return process.communicate()[1].decode('utf-8')
