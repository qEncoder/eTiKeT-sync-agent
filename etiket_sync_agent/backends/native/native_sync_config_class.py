import dataclasses

from typing import Optional

from etiket_sync_agent.crud.sync_sources import crud_sync_sources, SyncSources
from etiket_sync_agent.models.enums import SyncSourceTypes
from etiket_sync_agent.db import get_db_session_context

@dataclasses.dataclass
class NativeConfigData:
    def validate(self, current_sync_source : Optional[SyncSources] = None):
        '''
        There can only be one native sync source, validate this, as no parameters are needed.
        '''
        with get_db_session_context() as session:
            sync_sources = crud_sync_sources.list_sync_sources(session)
            n_sources = 0
            for sync_source in sync_sources:
                if sync_source.type == SyncSourceTypes.native:
                    n_sources+=1
            if n_sources>1:
                raise ValueError("Native sync source already present. Only one may be added.")