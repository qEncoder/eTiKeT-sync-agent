from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Mapped, mapped_column

from etiket_sync_agent.models.base import Base
from etiket_sync_agent.models.enums import SyncStatus
from etiket_sync_agent.models.utility.functions import utcnow
from etiket_sync_agent.models.utility.types import UtcDateTime


class SyncStatusRecord(Base):
    __tablename__ = "sync_status"

    id: Mapped[int] = mapped_column(primary_key=True)
    sync_iteration_count : Mapped[int] = mapped_column(default=0)
    status: Mapped[SyncStatus]
    last_update: Mapped[datetime] = mapped_column(UtcDateTime, server_default=utcnow(), onupdate=utcnow())
    error_message: Mapped[Optional[str]] = mapped_column(default=None)