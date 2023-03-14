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


class ClassIDMemStandalone:
    # Need to remove the uppercase configuration
    Free = "class-0000"  # 2c8g
    Small = "class-0001"  # 2c8g
    Middle = "class-0002"  # 4c16g
    Large = "class-0003"  # 8c32g
    XLarge = "class-0004"  # 16c64g

    Class1CU = "class-1"
    Class2CU = "class-2"
    Class4CU = "class-4"
    Class8CU = "class-8"

    # new config
    free = "class-0000"  # 2c8g
    small = "class-0001"  # 2c8g
    middle = "class-0002"  # 4c16g
    large = "class-0003"  # 8c32g
    xlarge = "class-0004"  # 16c64g

    class1cu = "class-1"
    class2cu = "class-2"
    class4cu = "class-4"
    class8cu = "class-8"


class ClassIDMemCluster:
    # Need to remove the uppercase configuration
    XXLarge = "class-0005"  # 32c128g cluster

    Class12CU = "class-12"  # cluster
    Class16CU = "class-16"
    Class20CU = "class-20"
    Class24CU = "class-24"
    Class28CU = "class-28"
    Class32CU = "class-32"
    Class128CU = "class-128"

    # new config
    xxlarge = "class-0005"  # 32c128g cluster

    class12cu = "class-12"  # cluster
    class16cu = "class-16"
    class20cu = "class-20"
    class24cu = "class-24"
    class28cu = "class-28"
    class32cu = "class-32"
    class128cu = "class-128"


class ClassIDMem(ClassIDMemStandalone, ClassIDMemCluster):
    pass


class ClassIDDiskStandalone:
    # Need to remove the uppercase configuration
    FreeDisk = "class-0000-disk"  # 2c8g
    SmallDisk = "class-0001-disk"  # 2c8g
    MiddleDisk = "class-0002-disk"  # 4c16g
    LargeDisk = "class-0003-disk"  # 8c32g
    XLargeDisk = "class-0004-disk"  # 16c64g

    Class1CUDisk = "class-1-disk"
    Class2CUDisk = "class-2-disk"
    Class4CUDisk = "class-4-disk"
    Class8CUDisk = "class-8-disk"

    # new config
    freedisk = "class-0000-disk"  # 2c8g
    smalldisk = "class-0001-disk"  # 2c8g
    middledisk = "class-0002-disk"  # 4c16g
    largedisk = "class-0003-disk"  # 8c32g
    xlargedisk = "class-0004-disk"  # 16c64g

    class1cudisk = "class-1-disk"
    class2cudisk = "class-2-disk"
    class4cudisk = "class-4-disk"
    class8cudisk = "class-8-disk"


class ClassIDDiskCluster:
    # Need to remove the uppercase configuration
    XXLargeDisk = "class-0005-disk"  # 32c128g cluster

    Class12CUDisk = "class-12-disk"  # cluster
    Class16CUDisk = "class-16-disk"
    Class20CUDisk = "class-20-disk"
    Class24CUDisk = "class-24-disk"
    Class28CUDisk = "class-28-disk"
    Class32CUDisk = "class-32-disk"

    # new config
    xxlargedisk = "class-0005-disk"  # 32c128g cluster

    class12cudisk = "class-12-disk"  # cluster
    class16cudisk = "class-16-disk"
    class20cudisk = "class-20-disk"
    class24cudisk = "class-24-disk"
    class28cudisk = "class-28-disk"
    class32cudisk = "class-32-disk"


class ClassIDDisk(ClassIDDiskStandalone, ClassIDDiskCluster):
    pass


class ClassID(ClassIDMem, ClassIDDisk):
    pass


CLASS_RESOURCES_MAP = {
    # rootCoord.dmlChannelNum is temporarily unavailable
    # class-1, class-1-disk maybe limits.memory will eventually be updated to 8Gi
    # limits.memory of class-8, class-8-disk are temporarily uncertain

    # standalone-memory
    "class-1": {"cpu": "2", "memory": "9Gi", "replicas": 1, "component": STANDALONE},
    "class-2": {"cpu": "4", "memory": "16Gi", "replicas": 1, "component": STANDALONE},
    "class-4": {"cpu": "8", "memory": "32Gi", "replicas": 1, "component": STANDALONE},
    "class-8": {"cpu": "16", "memory": "64Gi", "replicas": 1, "component": STANDALONE},
    # cluster-memory
    "class-12": {"cpu": "8", "memory": "32Gi", "replicas": 3, "component": querynode},
    "class-16": {"cpu": "8", "memory": "32Gi", "replicas": 4, "component": querynode},
    "class-20": {"cpu": "8", "memory": "32Gi", "replicas": 5, "component": querynode},
    "class-24": {"cpu": "8", "memory": "32Gi", "replicas": 6, "component": querynode},
    "class-28": {"cpu": "8", "memory": "32Gi", "replicas": 7, "component": querynode},
    "class-32": {"cpu": "8", "memory": "32Gi", "replicas": 8, "component": querynode},
    "class-128": {"cpu": "8", "memory": "32Gi", "replicas": 32, "component": querynode},
    # standalone-disk
    "class-1-disk": {"cpu": "2", "memory": "9Gi", "replicas": 1, "component": STANDALONE},
    "class-2-disk": {"cpu": "4", "memory": "16Gi", "replicas": 1, "component": STANDALONE},
    "class-4-disk": {"cpu": "8", "memory": "32Gi", "replicas": 1, "component": STANDALONE},
    "class-8-disk": {"cpu": "16", "memory": "64Gi", "replicas": 1, "component": STANDALONE},
    # cluster-disk
    "class-12-disk": {"cpu": "8", "memory": "32Gi", "replicas": 3, "component": querynode},
    "class-16-disk": {"cpu": "8", "memory": "32Gi", "replicas": 4, "component": querynode},
    "class-20-disk": {"cpu": "8", "memory": "32Gi", "replicas": 5, "component": querynode},
    "class-24-disk": {"cpu": "8", "memory": "32Gi", "replicas": 6, "component": querynode},
    "class-28-disk": {"cpu": "8", "memory": "32Gi", "replicas": 7, "component": querynode},
    "class-32-disk": {"cpu": "8", "memory": "32Gi", "replicas": 8, "component": querynode}
}


class RMNodeCategory:
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
