import datetime, uuid, json, zlib

from datetime import datetime
from typing import Optional

from sqlalchemy import ForeignKey, UniqueConstraint, Index, types
from sqlalchemy.orm import Mapped, mapped_column

from etiket_sync_agent.models.base import Base
from etiket_sync_agent.models.utility.functions import utcnow
from etiket_sync_agent.models.utility.types import UtcDateTime

class CompressedJSON(types.TypeDecorator):
    """
    This type decorator stores JSON data in a zlib-compressed binary format.
    """
    impl = types.LargeBinary
    cache_ok = True

    def process_bind_param(self, value : Optional[dict], dialect):
        """
        Serialize *value* to JSON, encode it as UTF-8 and compress using zlib
        before sending it to the database.
        """
        if value is None:
            return value
        
        json_bytes = json.dumps(value, default=str).encode("utf-8")
        return zlib.compress(json_bytes)

    def process_result_value(self, value : Optional[bytes], dialect):
        """
        Decompress *value* retrieved from the database and deserialize the JSON
        back into the corresponding Python object.
        """
        if value is None:
            return None
        try:
            decompressed = zlib.decompress(value).decode("utf-8")
            return json.loads(decompressed)
        except (zlib.error, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise ValueError("Failed to decompress or deserialize CompressedJSON payload") from exc

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