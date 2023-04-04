from dataclasses import dataclass, field
from typing import Optional, List, Union

from client.common.common_type import NAS
import client.parameters.params_name as pn

DatasetPath = {
    "random": NAS.RAW_DATA_DIR + 'random/',
    "deep": NAS.RAW_DATA_DIR + 'deep1b/',
    "jaccard": NAS.RAW_DATA_DIR + 'jaccard/',
    "hamming": NAS.RAW_DATA_DIR + 'hamming/',
    "sift": NAS.RAW_DATA_DIR + 'sift1b/',
    "sift_ground_truth": NAS.RAW_DATA_DIR + 'sift1b/gnd',
    "text2img": NAS.RAW_DATA_DIR + 'text2img1b/',
    "text2img_ground_truth": NAS.RAW_DATA_DIR + 'text2img1b/gnd',
    "laion": NAS.RAW_DATA_DIR + 'laion1b/',
    "laion_ground_truth": NAS.RAW_DATA_DIR + 'laion1b/gnd',
    "gist": NAS.RAW_DATA_DIR + 'gist1m/',
    "structure": NAS.RAW_DATA_DIR + 'structure/',
    "binary": NAS.RAW_DATA_DIR + 'binary/',
}

ScalarDatasetPath = {
    "laion2b_url": NAS.SCALAR_DATA_DIR + "laion2b_url/",
    "laion2b_int64": NAS.SCALAR_DATA_DIR + "laion2b_int64/"
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


@dataclass
class InterfaceResponse:
    response: any
    rt: Union[int, float]  # response time
    res_result: bool
    check_result: bool


@dataclass
class TransferNodesParams:
    source: str
    target: str
    num_node: int


@dataclass
class TransferReplicasParams:
    source: str
    target: str
    collection_name: str
    num_replica: int


@dataclass
class SegmentsAnalysis:
    count_segment: int
    total_vectors: int
    max_segment: int
    min_segment: int
    avg_segment: float
    std_segment: float
    shards_num: int
    truncated_avg_segment: float
    truncated_std_segment: float
    top_percentile: List[dict]

    @property
    def to_dict(self):
        return vars(self)
