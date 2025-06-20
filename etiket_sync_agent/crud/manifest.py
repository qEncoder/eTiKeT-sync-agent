from typing import Dict

from sqlalchemy.orm import Session
from sqlalchemy import select, update

from etiket_sync_agent.models.sync_items import SyncItems
    
class ManifestCrud:
    def read_manifest(self, session : Session, sync_source_id : int) -> 'Dict[str, float]':
        stmt = (
            select(SyncItems.dataIdentifier, SyncItems.syncPriority)
            .where(SyncItems.sync_source_id == sync_source_id)
        )
        result = session.execute(stmt).all()
        
        manifest = {}
        for item in result:
            manifest[item.dataIdentifier] = item.syncPriority
        return manifest
    
    def update_manifest(self, session: Session, sync_source_id : int, dataIdentifier : str, last_mod_time : float) -> None:
        update_stmt = (
            update(SyncItems)
            .where(SyncItems.sync_source_id == sync_source_id)
            .where(SyncItems.dataIdentifier == dataIdentifier)
            .values(syncPriority = last_mod_time)
        )
        
        session.execute(update_stmt)
        session.commit()
    
crud_manifest = ManifestCrud()