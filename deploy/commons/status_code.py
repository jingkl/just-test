
class CloudServiceStatus:
    META_INSTANCE_ID_NOT_EXISTS = 63017  # "InstanceId not exists."


class RMErrorCode:
    RequestBusy = 15


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


class InstanceNodeStatus:
    CREATING = 0
    RUNNING = 1
    DELETING = 2
    DELETED = 3
    RESIZING = 4
    UPGRADING = 5
    STOPPING = 6
    RESUMING = 7


class OversoldStatus:
    NOTREADY = 0
    READY = 1
