import copy
from client.common.common_type import NAS
import client.parameters.params_name as pn

DatasetPath = {
    "random": NAS.RAW_DATA_DIR + 'random/',
    "deep": NAS.RAW_DATA_DIR + 'deep1b/',
    "jaccard": NAS.RAW_DATA_DIR + 'jaccard/',
    "hamming": NAS.RAW_DATA_DIR + 'hamming/',
    "sift": NAS.RAW_DATA_DIR + 'sift1b/',
    "gist": NAS.RAW_DATA_DIR + 'gist1m/',
    "sift_ground_truth": NAS.RAW_DATA_DIR + 'sift1b/gnd',
    "structure": NAS.RAW_DATA_DIR + 'structure/',
    "binary": NAS.RAW_DATA_DIR + 'binary/',
}

MetricsToIndexType = {
    "L2": [pn.IndexTypeName.FLAT, pn.IndexTypeName.IVF_FLAT, pn.IndexTypeName.IVF_SQ8, pn.IndexTypeName.IVF_PQ,
           pn.IndexTypeName.HNSW, pn.IndexTypeName.IVF_HNSW, pn.IndexTypeName.RHNSW_FLAT, pn.IndexTypeName.RHNSW_SQ,
           pn.IndexTypeName.RHNSW_PQ, pn.IndexTypeName.ANNOY],
    "IP": [pn.IndexTypeName.FLAT, pn.IndexTypeName.IVF_FLAT, pn.IndexTypeName.IVF_SQ8, pn.IndexTypeName.IVF_PQ,
           pn.IndexTypeName.HNSW, pn.IndexTypeName.IVF_HNSW, pn.IndexTypeName.RHNSW_FLAT, pn.IndexTypeName.RHNSW_SQ,
           pn.IndexTypeName.RHNSW_PQ, pn.IndexTypeName.ANNOY],
    "Jaccard": [pn.IndexTypeName.BIN_FLAT, pn.IndexTypeName.BIN_IVF_FLAT],
    "Tanimoto": [pn.IndexTypeName.BIN_FLAT, pn.IndexTypeName.BIN_IVF_FLAT],
    "Hamming": [pn.IndexTypeName.BIN_FLAT, pn.IndexTypeName.BIN_IVF_FLAT],
    "Superstructure": [pn.IndexTypeName.BIN_FLAT],
    "Substructure": [pn.IndexTypeName.BIN_FLAT],
}

GoBenchIndex = {
    "FLAT": "",
    "IVF_FLAT": "IVF_FLAT",
    "IVF_SQ8": "IVF_SQ8",
    "HNSW": "HNSW",
    "AUTOINDEX": "AUTOINDEX",
    "DISKANN": "DISKANN"
}

