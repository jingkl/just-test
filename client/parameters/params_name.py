MUST = "must"
OPTION = "option"

# params types
dataset_params = "dataset_params"
collection_params = "collection_params"
load_params = "load_params"
index_params = "index_params"
search_params = "search_params"
query_params = "query_params"
concurrent_params = "concurrent_params"

# dataset
dataset_name = "dataset_name"
dim = "dim"
dataset_size = "dataset_size"

# collection
other_fields = "other_fields"

# load
replica_number = "replica_number"

# index
index_type = "index_type"
index_param = "index_param"

# query
ids = "ids"
expression = "expression"

# search
top_k = "top_k"
nq = "nq"
search_param = "search_param"
expr = "expr"
guarantee_timestamp = "guarantee_timestamp"

# common
metric_type = "metric_type"
field_name = "field_name"
vector_field_name = "vector_field_name"
shards_num = "shards_num"
ni_per = "ni_per"
req_run_counts = "req_run_counts"

# hdf5
neighbors = "neighbors"
test = "test"
train = "train"
distances = "distances"


class AccDatasetsName:
    sift_128_euclidean = "sift-128-euclidean"
    sift_256_hamming = "sift-256-hamming"
    glove_200_angular = "glove-200-angular"
    glove_100_angular = "glove-100-angular"
    glove_25_angular = "glove-25-angular"
    kosarak_27983_jaccard = "kosarak-27983-jaccard"


class DatasetsName:
    SIFT = "sift"
    RANDOM = "random"
    DEEP = "deep"
    JACCARD = "jaccard"
    HAMMING = "hamming"
    STRUCTURE = "structure"
    BINARY = "binary"
    LOCAL = "local"  # random


class MetricsTypeName:
    L2 = "L2"
    IP = "IP"
    Jaccard = "Jaccard"
    Tanimoto = "Tanimoto"
    Hamming = "Hamming"
    Superstructure = "Superstructure"
    Substructure = "Substructure"


class IndexTypeName:
    """
      1. BinaryIDMAP
      2. BinaryIVF
      3. IDMAP (GPU)
      4. IVF_FLAT (GPU)
      5. IVF_PQ (GPU)
      6. IVF_SQ (GPU)
      7. HNSW
      8. ANNOY
    """
    FLAT = "FLAT"
    IVF_FLAT = "IVF_FLAT"
    IVF_SQ8 = "IVF_SQ8"
    NSG = "NSG"
    # IVF_SQ8_HYBRID = "IVF_SQ8_HYBRID"
    IVF_PQ = "IVF_PQ"
    HNSW = "HNSW"
    ANNOY = "ANNOY"
    BIN_FLAT = "BIN_FLAT"
    BIN_IVF_FLAT = "BIN_IVF_FLAT"
    RHNSW_FLAT = "RHNSW_FLAT"
    RHNSW_PQ = "RHNSW_PQ"
    RHNSW_SQ = "RHNSW_SQ"
    IVF_HNSW = "IVF_HNSW"


