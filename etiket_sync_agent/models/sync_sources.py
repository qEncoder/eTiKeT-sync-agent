import uuid, typing

from typing import Optional
from datetime import datetime

from sqlalchemy import ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from etiket_sync_agent.models.base import Base

from etiket_sync_agent.models.enums import SyncSourceStatus, SyncSourceTypes
from etiket_sync_agent.models.utility.functions import utcnow
from etiket_sync_agent.models.utility.types import UtcDateTime, CompressedStr

if typing.TYPE_CHECKING:
    from etiket_sync_agent.models.sync_items import SyncItems

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
    
    errors : Mapped[list["SyncSourceErrors"]] = relationship(cascade="all, delete-orphan")
    
    sync_items : Mapped[list["SyncItems"]] = relationship(back_populates="sync_source")
    
class SyncSourceErrors(Base):
    __tablename__ = "sync_source_errors"

    id: Mapped[int] = mapped_column(primary_key=True)
    sync_source_id : Mapped[int] = mapped_column(ForeignKey("sync_sources.id"))
    sync_iteration : Mapped[int]
    log_exception : Mapped[str]
    log_context   : Mapped[Optional[str]] = mapped_column(nullable=True)
    log_traceback : Mapped[Optional[str]] = mapped_column(CompressedStr, nullable=True)
    log_timestamp : Mapped[datetime] = mapped_column(UtcDateTime, server_default=utcnow())
    