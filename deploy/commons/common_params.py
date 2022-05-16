# Milvus components
rootCoord = "rootCoord"
dataCoord = "dataCoord"
queryCoord = "queryCoord"
dataNode = "dataNode"
queryNode = "queryNode"
indexNode = "indexNode"
proxy = "proxy"
all_pods = [rootCoord, dataCoord, queryCoord, dataNode, queryNode, indexNode, proxy]

# Milvus dependencies
etcd = "etcd"
storage = "storage"
pulsar = "pulsar"
kafka = "kafka"
minio = "minio"

CLUSTER = "cluster"
STANDALONE = "standalone"

MilvusCluster = "MilvusCluster"
Milvus = "Milvus"
PersistentVolumeClaim = "PersistentVolumeClaim"
Pod = "Pod"
PodChaos = "PodChaos"
DefaultApiVersion = "DefaultApiVersion"

APIVERSION = {MilvusCluster: "milvus.io/v1alpha1",
              Milvus: "milvus.io/v1alpha1",
              PersistentVolumeClaim: "v1",
              Pod: "v1",
              PodChaos: "chaos-mesh.org/v1alpha1",
              DefaultApiVersion: "milvus.io/v1alpha1"}

# common params
IDC_NAS_URL = "//172.16.70.249/test"
DefaultRepository = "harbor.zilliz.cc/milvus/milvus"

# deploy type
Helm = "helm"
Operator = "operator"

# default params
default_namespace = "qa-milvus"


class ClassID:
    Free = "class-0000"  # 2c8g
    Small = "class-0001"  # 2c8g
    Middle = "class-0002"  # 4c16g
    Large = "class-0003"  # 8c32g
    XLarge = "class-0004"  # 16c64g cluster
    XXLarge = "class-0005"  # 32c128g cluster


class InstanceStatus:
    CREATING = 0
    RUNNING = 1
    DELETING = 2
    DELETED = 3
    RESIZING = 4
    UPGRADING = 5
    STOPPING = 6
    RESUMING = 7
    MODIFYING = 8
    STOPPED = 9
    ABNORMAL = 10
    SET_WHITELIST = 11
