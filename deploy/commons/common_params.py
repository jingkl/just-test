import enum

# Milvus components
rootCoord = "rootCoord"
dataCoord = "dataCoord"
queryCoord = "queryCoord"
dataNode = "dataNode"
queryNode = "queryNode"
indexNode = "indexNode"
standalone = "standalone"
proxy = "proxy"
all_pods = [rootCoord, dataCoord, queryCoord, dataNode, queryNode, indexNode, proxy]
mixcoord = "mixcoord"
datanode = "datanode"
querynode = "querynode"

# Milvus dependencies
etcd = "etcd"
storage = "storage"
pulsar = "pulsar"
kafka = "kafka"
minio = "minio"
rocksmq = "rocksmq"

CLUSTER = "cluster"
STANDALONE = "standalone"

Milvus = "Milvus"
PersistentVolumeClaim = "PersistentVolumeClaim"
Pod = "Pod"
PodChaos = "PodChaos"
DefaultApiVersion = "DefaultApiVersion"

APIVERSION = {Milvus: "milvus.io/v1beta1",
              PersistentVolumeClaim: "v1",
              Pod: "v1",
              PodChaos: "chaos-mesh.org/v1alpha1",
              DefaultApiVersion: "milvus.io/v1beta1"}

# common params
IDC_NAS_URL = "//172.16.70.249/test"
DefaultRepository = "harbor.milvus.io/milvus/milvus"

# deploy type
Helm = "helm"
Operator = "operator"
OP = "op"
VDC = "vdc"


# default params
default_namespace = "qa-milvus"


# default key's name
ephemeral_storage = "ephemeral-storage"


class ClassIDBase(object):
    classnone = ""  # use for not upgrade instance
    classserverless = "class-2-disk-serverless"


class ClassIDMemStandalone(ClassIDBase):
    class1cu = "class-1"  # 2c8g
    class2cu = "class-2"  # 4c16g
    class4cu = "class-4"  # 8c32g
    class8cu = "class-8"  # 16c64g
    class1pu = "class-1-pul"  # 2c9g, pulsar
    class1ro = "class-1-ro"  # 2c9g, rocketMq
    class1kfk = "class-1-kfk"  # 2c9g, kafka


class ClassIDMemCluster(ClassIDBase):
    class12cu = "class-12"  # 3 queryNode
    class16cu = "class-16"  # 4 queryNode
    class20cu = "class-20"  # 5 queryNode
    class24cu = "class-24"  # 6 queryNode
    class28cu = "class-28"  # 7 queryNode
    class32cu = "class-32"  # 8 queryNode
    class128cu = "class-128"  # 16 queryNode


class ClassIDMem(ClassIDMemStandalone, ClassIDMemCluster):
    pass


class ClassIDDiskStandalone(ClassIDBase):
    class1cudisk = "class-1-disk"
    class1diskro = "class-1-disk-ro"
    class1diskpul = "class-1-disk-pul"
    class1diskkfk = "class-1-disk-kfk"
    class2cudisk = "class-2-disk"
    class4cudisk = "class-4-disk"
    class8cudisk = "class-8-disk"

    class1cudiskcost = "class-1-disk-cost"
    class2cudiskcost = "class-2-disk-cost"
    class4cudiskcost = "class-4-disk-cost"
    class8cudiskcost = "class-8-disk-cost"


class ClassIDDiskCluster(ClassIDBase):
    class12cudisk = "class-12-disk"  # cluster
    class16cudisk = "class-16-disk"
    class20cudisk = "class-20-disk"
    class24cudisk = "class-24-disk"
    class28cudisk = "class-28-disk"
    class32cudisk = "class-32-disk"

    class12cudiskcost = "class-12-disk-cost"  # cluster
    class16cudiskcost = "class-16-disk-cost"
    class20cudiskcost = "class-20-disk-cost"
    class24cudiskcost = "class-24-disk-cost"
    class28cudiskcost = "class-28-disk-cost"
    class32cudiskcost = "class-32-disk-cost"


class ClassIDDisk(ClassIDDiskStandalone, ClassIDDiskCluster):
    pass


class ClassID(ClassIDMem, ClassIDDisk):
    pass


class RMNodeCategory(enum.Enum):
    proxy = 1
    RootCoord = 2
    QueryCoord = 3
    DataCoord = 4
    IndexCoord = 5
    dataNode = 6
    queryNode = 7
    indexNode = 8
    standalone = 9
    mixCoord = 10


class ComponentsLabel(enum.Enum):
    proxy = 1
    datanode = 6
    querynode = 7
    standalone = 9
    mixcoord = 10
