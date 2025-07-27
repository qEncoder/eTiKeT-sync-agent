import enum 

class SyncSourceStatus(str, enum.Enum):
    SYNCHRONIZING = "synchronizing"
    SYNCHRONIZED = "synchronized"
    ERROR = "error"
    PAUSED = "paused"
    
class SyncSourceTypes(str, enum.Enum):
    native = "native"
    coretools = "Core-tools"
    qcodes = "qCoDeS"
    quantify = "quantify"
    fileBase = "fileBase"
    labber = "labber"

class SyncStatus(str, enum.Enum):
    RUNNING = "running"
    ERROR = "error"
    NOT_LOGGED_IN = "not_logged_in"
    NO_CONNECTION = "no_connection"
    STOPPED = "stopped"
    