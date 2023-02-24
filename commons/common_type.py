try:
    from pymilvus import DEFAULT_RESOURCE_GROUP
    RESOURCE_GROUPS_FLAG = True
except ImportError as e:
    DEFAULT_RESOURCE_GROUP = "__default_resource_group"


class LogLevel:
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class DefaultParams:
    default_replica_num = 1
    default_cpu = 8
    default_mem = 16
    default_resource_group = DEFAULT_RESOURCE_GROUP


class PRECISION:
    COMMON_PRECISION = 4
    INSERT_PRECISION = 4


class ReportMetric:
    # base slots
    Server = "server"
    Client = "client"
    Result = "result"

    # server params
    upgrade_config = "upgrade_config"

    # Client params
    run_id = "run_id"
