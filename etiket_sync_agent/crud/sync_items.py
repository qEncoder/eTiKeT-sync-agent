import enum, uuid

from typing import List, Optional, Tuple, Dict
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session
from sqlalchemy import select, insert, update, or_

from etiket_sync_agent.models.sync_items import SyncItems

class SyncItemSortKey(enum.StrEnum):
    id = "id"
    last_update = "last_update"
    attempts = "attempts"
    syncpriority = "syncpriority"

class OrderDirection(enum.StrEnum):
    asc = "asc"
    desc = "desc"
    
class SyncItemsCrud:
    def create_sync_items(self, session : Session, sync_source_id : int, sync_items : List[SyncItems]) -> None:
        # not exposed to the rest api
        if len(sync_items) == 0:
            return
        existing_items_result = []
        batch_size = 10000
        
        for i in range(0, len(sync_items), batch_size):
            batch = sync_items[i:i+batch_size]
            data_identifiers_batch = [item.dataIdentifier for item in batch]
            
            stmt = select(SyncItems).where(SyncItems.sync_source_id == sync_source_id)
            stmt = stmt.where(SyncItems.dataIdentifier.in_(data_identifiers_batch))

            result = session.execute(stmt).scalars().all()
            existing_items_result.extend(result)
        
        existing_data_identifiers = {item.dataIdentifier : item for item in existing_items_result}
        
        create_list = []
        update_list = []
        
        for s_item in sync_items:
            if s_item.dataIdentifier in existing_data_identifiers:
                update_list.append({"id" : existing_data_identifiers[s_item.dataIdentifier].id,
                                    "syncPriority" : s_item.syncPriority,
                                    "synchronized" : False,
                                    "attempts" : 0})
            else:
                create_list.append({"sync_source_id" : sync_source_id,
                                    "dataIdentifier" : s_item.dataIdentifier,
                                    "datasetUUID" : s_item.datasetUUID if s_item.datasetUUID is not None else uuid.uuid4(),
                                    "syncPriority" : s_item.syncPriority})
        
        if create_list:
            session.execute(insert(SyncItems), create_list)
        if update_list:
            session.execute(update(SyncItems), update_list)

        if create_list or update_list:
            session.commit()
    
    def read_next_sync_item(self, session : Session, sync_source_id : int, offset :int) -> SyncItems | None:
        select_stmt = (
            select(SyncItems)
            .where(SyncItems.sync_source_id == sync_source_id)
            .where(SyncItems.synchronized == False)
            .where(SyncItems.attempts == 0)
            .order_by(SyncItems.syncPriority.desc())
            .limit(1)
            .offset(offset)
        )
        
        result = session.execute(select_stmt).scalar_one_or_none()

        # retry rules : 1st attempt after 1 hour, 2nd attempt after 1 day, 3rd attempt after 1 week, 4th attempt after 1 month
        if result is None:
            select_stmt = (
                select(SyncItems)
                .where(SyncItems.sync_source_id == sync_source_id)
                .where(SyncItems.synchronized == False)
                .where(or_((SyncItems.attempts == 0),
                        (SyncItems.attempts == 1) & (SyncItems.last_update < datetime.now(timezone.utc) - timedelta(minutes=20)),
                        (SyncItems.attempts == 2) & (SyncItems.last_update < datetime.now(timezone.utc) - timedelta(hours=1)),
                        (SyncItems.attempts == 3) & (SyncItems.last_update < datetime.now(timezone.utc) - timedelta(hours=2)),
                        (SyncItems.attempts == 4) & (SyncItems.last_update < datetime.now(timezone.utc) - timedelta(hours=8)),
                        (SyncItems.attempts >= 5) & (SyncItems.last_update < datetime.now(timezone.utc) - timedelta(days=1))))
                .order_by(SyncItems.attempts.asc(), SyncItems.syncPriority.desc())
                .limit(1)
                .offset(offset)
            )

            result = session.execute(select_stmt).scalar_one_or_none()
        
        if result is None:
            return None 
        
        return result
            
    def list_sync_items(self, session : Session, source_id : int,
                        is_synchronized : Optional[bool] = None,
                        order_by : Optional[SyncItemSortKey] = None,
                        order_direction : Optional[OrderDirection] = None,
                        cursor : Optional[str] = None,
                        limit : int = 100,
                        ) -> Tuple[List[SyncItems], str | None]:
        # select all and return
        # implement cursor pagination
        raise NotImplementedError("Not implemented")
    
    def update_sync_item(self, session : Session,
                            sync_item_id : int,
                            dataset_uuid : Optional[uuid.UUID] = None,
                            sync_priority : Optional[float] = None,
                            attempts : Optional[int] = None,
                            sync_record : Optional[Dict] = None,
                            error : Optional[str]= None,
                            traceback : Optional[str]= None,
                            synchronized : Optional[bool] = None) -> SyncItems:
        update_stmt = update(SyncItems).where(SyncItems.id == sync_item_id)
        if dataset_uuid is not None:
            update_stmt = update_stmt.values(datasetUUID = dataset_uuid)
            
        if sync_priority is not None:
            update_stmt = update_stmt.values(syncPriority = sync_priority)
        
        if synchronized is not None:
            update_stmt = update_stmt.values(synchronized = synchronized)
        
        if attempts is not None:
            update_stmt = update_stmt.values(attempts = attempts)
        
        if sync_record is not None:
            update_stmt = update_stmt.values(sync_record = sync_record)
        
        if error is not None:
            update_stmt = update_stmt.values(error = error)
        
        if traceback is not None:
            update_stmt = update_stmt.values(traceback = traceback)
        
        session.execute(update_stmt)
        session.commit()
        
        stmt = select(SyncItems).where(SyncItems.id == sync_item_id)
        result = session.execute(stmt).scalar_one()
        return result
    
    def get_last_sync_item(self, session : Session, sync_source_id : int) -> SyncItems | None:
        stmt = (
            select(SyncItems)
            .where(SyncItems.sync_source_id == sync_source_id)
            .order_by(SyncItems.syncPriority.desc())
            .limit(1)
        )
        result = session.execute(stmt).scalar_one_or_none()
        return result
    
crud_sync_items = SyncItemsCrud()