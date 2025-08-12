import datetime, uuid

from datetime import datetime
from typing import Optional

from sqlalchemy import ForeignKey, UniqueConstraint, Index, types
from sqlalchemy.orm import Mapped, mapped_column

from etiket_sync_agent.models.base import Base
from etiket_sync_agent.models.utility.functions import utcnow
from etiket_sync_agent.models.utility.types import UtcDateTime, CompressedJSON
class SyncItems(Base):
    __tablename__  = "sync_items"
    
    __table_args__ = (UniqueConstraint('sync_source_id', 'dataIdentifier', name='sync_source_did_constraint_sync_items'),
                        Index('read_item_index', 'sync_source_id', 'synchronized'),
                        Index('failed_upload_index', 'sync_source_id', 'attempts'))
    id: Mapped[int] = mapped_column(primary_key=True)
    sync_source_id : Mapped[int] = mapped_column(ForeignKey("sync_sources.id"))
    dataIdentifier: Mapped[str] = mapped_column(index = True)
    datasetUUID  : Mapped[uuid.UUID] = mapped_column(unique=True)
    
    syncPriority : Mapped[float] = mapped_column(index = True)
    
    synchronized : Mapped[bool] = mapped_column(default = False)
    attempts : Mapped[int] = mapped_column(default = 0)
    
    sync_record : Mapped[Optional[dict]] = mapped_column(CompressedJSON)
    
    error : Mapped[Optional[str]] = mapped_column(types.String)
    traceback : Mapped[Optional[str]] = mapped_column(types.Text)
    
    last_update : Mapped[datetime] = mapped_column(UtcDateTime, server_default=utcnow(), onupdate=utcnow())