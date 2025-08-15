from abc import ABC, abstractmethod
from typing import Any, Type, List
from pathlib import Path

from etiket_sync_agent.models.sync_items import SyncItems
from etiket_sync_agent.sync.sync_records.manager import SyncRecordManager

class SyncSourceBase(ABC):
    @property
    @abstractmethod
    def SyncAgentName(self) -> str:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def MapToASingleScope(self) -> bool:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def LiveSyncImplemented(self) -> bool:
        return False
    
    @classmethod
    @abstractmethod
    def config_data_class(cls) -> Type[Any]:
        raise NotImplementedError

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
        return cls.config_data_class(**config_data)

class SyncSourceFileBase(SyncSourceBase):
    @property
    @abstractmethod
    def level(self) -> int:
        raise NotImplementedError
    
    @staticmethod
    @abstractmethod
    def rootPath(configData: Any) -> Path:
        raise NotImplementedError

class SyncSourceDatabaseBase(SyncSourceBase):
    @staticmethod
    @abstractmethod
    def getNewDatasets(config_data: Any, last_sync_item: SyncItems | None) -> List[SyncItems]:
        raise NotImplementedError