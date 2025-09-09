import dataclasses

from abc import ABC, abstractmethod
from typing import Any, Type, List, ClassVar
from pathlib import Path

from etiket_sync_agent.models.sync_items import SyncItems
from etiket_sync_agent.sync.sync_records.manager import SyncRecordManager

class SyncSourceBase(ABC):
    SyncAgentName: ClassVar[str]
    ConfigDataClass: ClassVar[Type[Any]]
    MapToASingleScope: ClassVar[bool]

    @staticmethod
    @abstractmethod
    def checkLiveDataset(configData: Any, syncIdentifier: SyncItems, maxPriority: bool) -> bool:
        pass
    
    @staticmethod
    @abstractmethod
    def syncDatasetNormal(configData: Any, syncIdentifier: SyncItems, sync_record: SyncRecordManager):
        pass
    
    @staticmethod
    @abstractmethod
    def syncDatasetLive(configData: Any, syncIdentifier: SyncItems, sync_record: SyncRecordManager):
        pass
    
    @classmethod
    def sync_config(cls, config_data: dict) -> Any:
        return cls.ConfigDataClass(**config_data)

def _check_config_base(cls: Type[SyncSourceBase]) -> None:
    if not isinstance(getattr(cls, "SyncAgentName", None), str) or not cls.SyncAgentName:
        raise TypeError("SyncAgentName must be a non-empty str")
    if not dataclasses.is_dataclass(getattr(cls, "ConfigDataClass", None)):
        raise TypeError("ConfigDataClass must be a dataclass")
    if not isinstance(getattr(cls, "MapToASingleScope", None), bool):
        raise TypeError("MapToASingleScope must be a bool")
class SyncSourceFileBase(SyncSourceBase):
    level: ClassVar[int]
    
    @staticmethod
    @abstractmethod
    def rootPath(configData: Any) -> Path:
        raise NotImplementedError
    
    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        _check_config_base(cls)
        if not isinstance(getattr(cls, "level", None), int):
            raise TypeError("Level must be an int.")

class SyncSourceDatabaseBase(SyncSourceBase):
    @staticmethod
    @abstractmethod
    def getNewDatasets(config_data: Any, last_sync_item: SyncItems | None) -> List[SyncItems]:
        raise NotImplementedError

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        _check_config_base(cls)