from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Type, List
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
    def ConfigDataClass(self) -> Type[dataclass]:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def MapToASingleScope(self) -> bool:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def LiveSyncImplemented(self) -> bool:
        return False

    @staticmethod
    @abstractmethod
    def checkLiveDataset(configData: Type[dataclass], syncIdentifier: SyncItems, maxPriority: bool) -> bool:
        pass
    
    @staticmethod
    @abstractmethod
    def syncDatasetNormal(configData: Type[dataclass], syncIdentifier: SyncItems, sync_record: SyncRecordManager):
        pass
    
    @staticmethod
    @abstractmethod
    def syncDatasetLive(configData: Type[dataclass], syncIdentifier: SyncItems, sync_record: SyncRecordManager):
        pass

class SyncSourceFileBase(SyncSourceBase):
    @property
    @abstractmethod
    def level(self) -> int:
        raise NotImplementedError
    
    @staticmethod
    @abstractmethod
    def rootPath(configData: Type[dataclass]) -> Path:
        raise NotImplementedError

class SyncSourceDatabaseBase(SyncSourceBase):
    @staticmethod
    @abstractmethod
    def getNewDatasets(configData: Type[dataclass], lastIdentifier: str) -> List[SyncItems]:
        raise NotImplementedError