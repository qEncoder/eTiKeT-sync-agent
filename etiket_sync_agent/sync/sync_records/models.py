import uuid, datetime, enum

from datetime import timezone
from pathlib import Path

from dataclasses import dataclass, field, asdict, fields
from typing import Optional, List, Union, Dict, Callable


def utc_now():
    return datetime.datetime.now(timezone.utc)

class DataConvertorException(Exception):
    pass

class FileStatus(enum.Enum):
    OK = "OK"
    ERROR = "ERROR"
    
@dataclass
class ContentAwareChecksum:
    method : str
    value : str
    
    @classmethod
    def from_callable(cls, method : Callable, value : str):
        return cls(method = method.__module__ + "." + method.__qualname__, value = value)
    
    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)
    
@dataclass
class DataConvertor:
    method : str
    status : FileStatus
    error : Optional[str]
    
    @classmethod
    def from_callable(cls, method : Callable, status : FileStatus, error : Optional[str] = None):
        return cls(method = method.__module__ + "." + method.__qualname__, status = status, error = error)
    
    def to_dict(self):
        d = asdict(self)
        d['status'] = self.status.value
        return d

    @classmethod
    def from_dict(cls, data):
        data['status'] = FileStatus(data['status'])
        return cls(**data)

@dataclass
class FileUploadInfo:
    filename : str
    
    file_path : Optional[Path] = None
    file_m_time : Optional[datetime.datetime] = None

    file_uuid : Optional[uuid.UUID] = None
    version_id : Optional[int] = None

    sync_timestamp : datetime.datetime = field(default_factory=utc_now)
    md5 : Optional[str] = None
    content_aware_checksum : Optional[ContentAwareChecksum] = None
    size : Optional[int] = None
    status : FileStatus = FileStatus.ERROR
    converter : Optional[DataConvertor] = None
    error : Optional[str] = None
    
    def add_content_aware_checksum(self, method : Callable, value : str):
        '''
        Add a content aware checksum to the file upload info.
        '''
        self.content_aware_checksum = ContentAwareChecksum.from_callable(method, value)
    
    def to_dict(self):
        d = asdict(self)
        for key, value in d.items():
            if isinstance(value, datetime.datetime):
                d[key] = value.isoformat()
            elif isinstance(value, uuid.UUID):
                d[key] = str(value)
            elif isinstance(value, Path):
                d[key] = str(value)
            elif isinstance(value, enum.Enum):
                d[key] = value.value
        
        if self.converter:
            d['converter'] = self.converter.to_dict()
        if self.content_aware_checksum:
            d['content_aware_checksum'] = self.content_aware_checksum.to_dict()
        return d

    @classmethod
    def from_dict(cls, data):
        if data.get('file_path'):
            data['file_path'] = Path(data['file_path'])
        if data.get('file_m_time'):
            data['file_m_time'] = datetime.datetime.fromisoformat(data['file_m_time'])
        if data.get('file_uuid'):
            data['file_uuid'] = uuid.UUID(data['file_uuid'])
        if data.get('sync_timestamp'):
            data['sync_timestamp'] = datetime.datetime.fromisoformat(data['sync_timestamp'])
        if data.get('status'):
            data['status'] = FileStatus(data['status'])
        if data.get('converter'):
            data['converter'] = DataConvertor.from_dict(data['converter'])
        if data.get('content_aware_checksum'):
            data['content_aware_checksum'] = ContentAwareChecksum.from_dict(data['content_aware_checksum'])
                
        valid_keys = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in data.items() if k in valid_keys}

        return cls(**filtered_data)

@dataclass
class LogEntry:
    message: str
    date: datetime.datetime = field(default_factory=utc_now)

    def to_dict(self):
        return {"type": "log", "date": self.date.isoformat(), "message": self.message}

    @classmethod
    def from_dict(cls, data):
        return cls(message=data['message'], date=datetime.datetime.fromisoformat(data['date']))

@dataclass
class ErrorEntry:
    message: str
    stacktrace: str
    date: datetime.datetime = field(default_factory=utc_now)

    def to_dict(self):
        return {"type": "error", "date": self.date.isoformat(), "message": self.message, "stacktrace": self.stacktrace}
    
    @classmethod
    def from_dict(cls, data):
        return cls(message=data['message'], stacktrace=data['stacktrace'], date=datetime.datetime.fromisoformat(data['date']))

LogItem = Union["TaskEntry", "LogEntry", "ErrorEntry"]

@dataclass
class TaskEntry:
    name: str
    content: List[LogItem] = field(default_factory=list)
    has_errors: bool = False
    date: datetime.datetime = field(default_factory=utc_now)

    def to_dict(self):
        return {
            "type": "task",
            "name": self.name,
            "date": self.date.isoformat(),
            "content": [item.to_dict() for item in self.content],
            "has_errors": self.has_errors
        }
    
    @classmethod
    def from_dict(cls, data):
        content = []
        for item_data in data['content']:
            item_type = item_data['type']
            if item_type == 'log':
                content.append(LogEntry.from_dict(item_data))
            elif item_type == 'error':
                content.append(ErrorEntry.from_dict(item_data))
            elif item_type == 'task':
                content.append(TaskEntry.from_dict(item_data))
        
        return cls(
            name=data['name'],
            content=content,
            has_errors=data['has_errors'],
            date=datetime.datetime.fromisoformat(data['date'])
        )

@dataclass
class DatasetSyncRecordData:
    version: int = 1
    dataset_sync_path: Optional[Path] = None
    sync_time: datetime.datetime = field(default_factory=utc_now)
    files: Dict[str, List["FileUploadInfo"]] = field(default_factory=dict)
    logs: List[LogItem] = field(default_factory=list)

    def to_dict(self):
        return {
            "version": self.version,
            "dataset_sync_path": str(self.dataset_sync_path) if self.dataset_sync_path else None,
            "sync_time": self.sync_time.isoformat(),
            "files": {name: [file_info.to_dict() for file_info in file_infos] for name, file_infos in self.files.items()},
            "logs": [item.to_dict() for item in self.logs]
        }

    @classmethod
    def from_dict(cls, data):
        item_data = data.get('logs', [])
        logs = []
        for item in item_data:
            if item['type'] == 'task':
                logs.append(TaskEntry.from_dict(item))
            elif item['type'] == 'log':
                logs.append(LogEntry.from_dict(item))
            elif item['type'] == 'error':
                logs.append(ErrorEntry.from_dict(item))
        
        files = {}
        for name, f_data in data.get('files', {}).items():
            files[name] = [FileUploadInfo.from_dict(f_data) for f_data in f_data]
        
        return cls(
            version=data.get('version', 1),
            dataset_sync_path=Path(data['dataset_sync_path']) if data.get('dataset_sync_path') else None,
            sync_time=datetime.datetime.fromisoformat(data.get('sync_time', utc_now().isoformat())),
            files=files,
            logs=logs
        )

    def add_log_item(self, current_item : TaskEntry | List[LogItem], item : LogItem):
        '''
        Add a log item to the logs of the manifest.
        
        Args:
            current_item: The current item to add the log item to (e.g. a task or a list of)
            item: The log item to add.
        '''
        if isinstance(current_item, list):
            current_item.append(item)
        else:
            current_item.content.append(item)