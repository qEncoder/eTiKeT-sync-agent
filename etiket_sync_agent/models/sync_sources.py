import uuid

from typing import Optional
from datetime import datetime

from sqlalchemy import ForeignKey, JSON, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from etiket_sync_agent.models.base import Base
from etiket_sync_agent.models.enums import SyncSourceStatus, SyncSourceTypes
from etiket_sync_agent.models.utility.functions import utcnow
from etiket_sync_agent.models.utility.types import UtcDateTime

class SyncSources(Base):
    __tablename__ = "sync_sources"

    id: Mapped[int] = mapped_column(primary_key=True)
    name : Mapped[str] = mapped_column(unique=True)
    type : Mapped[SyncSourceTypes]
    status : Mapped[SyncSourceStatus]
    creator : Mapped[Optional[str]]
    
    items_total : Mapped[int] = mapped_column(default=0)
    items_synchronized : Mapped[int] = mapped_column(default=0)
    items_failed : Mapped[int] = mapped_column(default=0)

    last_update : Mapped[datetime] = mapped_column(UtcDateTime, server_default=utcnow(), onupdate=utcnow())
    config_data : Mapped[dict] = mapped_column(JSON)
    
    default_scope : Mapped[Optional[uuid.UUID]]

class SyncScopeMappingsSQL(Base):
    __tablename__ = "sync_scope_mappings"
    __table_args__ = (UniqueConstraint('sync_source_id', 'scope_identifier', name='sync_source_did_constraint_scope_mapping'), )

    id: Mapped[int] = mapped_column(primary_key=True)
    sync_source_id : Mapped[int] = mapped_column(ForeignKey("sync_sources.id"))
    scope_identifier : Mapped[str]
    scope_uuid : Mapped[uuid.UUID]