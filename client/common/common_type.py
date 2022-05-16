from dataclasses import dataclass
from typing import Optional, Union, Callable, List, Dict, AnyStr

from commons.common_params import EnvVariable


class Error:
    """ define error class """

    def __init__(self, error):
        self.code = getattr(error, 'code', -1)
        self.message = getattr(error, 'message', str(error))


class CheckTasks:
    """ The name of the method used to check the result """
    check_nothing = "check_nothing"
    err_res = "error_response"
    ccr = "check_connection_result"


class CaseLabel:
    """ Testcase Levels """
    L0 = "L0"
    L1 = "L1"


class LogLevel:
    DEBUG = "DEBUG"
    INFO = "INFO"
    ERROR = "ERROR"


class DefaultValue:
    default_dim = 128
    default_max_length = 65535
    default_desc = ""

    default_int64_field_name = "int64"
    default_float_field_name = "float"
    default_double_field_name = "double"
    default_float_vec_field_name = "float_vector"
    default_binary_vector_name = "binary_vector"

    err_code = "err_code"
    err_msg = "err_msg"
    Not_Exist = "Not_Exist"
    list_content = "list_content"
    dict_content = "dict_content"
    value_content = "value_content"

    FILE_PREFIX = "binary"
    Max_file_count = 100000


class SimilarityMetrics:
    L2 = "L2"
    IP = "IP"
    Jaccard = "JACCARD"
    Tanimoto = "TANIMOTO"
    Hamming = "HAMMING"
    Superstructure = "SUPERSTRUCTURE"
    Substructure = "SUBSTRUCTURE"


class NAS:
    DATASET_DIR = EnvVariable.DATASET_DIR
    RAW_DATA_DIR = DATASET_DIR + "/raw_data/"
    ANN_DATA_DIR = DATASET_DIR + "/ann_hdf5/"


class AccMetrics:
    euclidean = SimilarityMetrics.L2
    angular = SimilarityMetrics.IP
    hamming = SimilarityMetrics.Hamming
    jaccard = SimilarityMetrics.Jaccard


class Precision:
    # precision of params
    LOAD_PRECISION = 4
    FLUSH_PRECISION = 4
    INDEX_PRECISION = 4
    SEARCH_PRECISION = 4
    INSERT_PRECISION = 4
    QUERY_PRECISION = 4
    COMMON_PRECISION = 4


class CaseIterParams:
    def __init__(self, callable_object: Callable, object_args: list = [], object_kwargs: dict = {},
                 actual_params_used: dict = {}, case_type: str = ""):
        self.ActualParamsUsed = actual_params_used
        self.CaseType = case_type
        self.CallableObject = callable_object
        self.ObjectArgs = object_args
        self.ObjectKwargs = object_kwargs
