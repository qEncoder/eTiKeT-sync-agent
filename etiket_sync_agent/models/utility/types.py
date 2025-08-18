import json, zlib

from typing import Optional
from datetime import datetime, timezone
from sqlalchemy import DateTime, types, BLOB

class UtcDateTime(types.TypeDecorator):
    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value : Optional[datetime], dialect):
        if value is None:
            return None
        
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    def process_result_value(self, value : datetime, dialect):
        value = value.replace(tzinfo=timezone.utc)
        return value
class CompressedJSON(types.TypeDecorator):
    """
    This type decorator stores JSON data in a zlib-compressed binary format.
    """
    impl = BLOB
    cache_ok = True
    
    def __init__(self, compression_level: int = zlib.Z_DEFAULT_COMPRESSION, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._compression_level = compression_level

    def process_bind_param(self, value : Optional[dict], dialect):
        """
        Serialize *value* to JSON, encode it as UTF-8 and compress using zlib
        before sending it to the database.
        """
        if value is None:
            return value
        
        json_bytes = json.dumps(value, default=str).encode("utf-8")
        return zlib.compress(json_bytes, self._compression_level)

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
    
class CompressedStr(types.TypeDecorator):
    """
    Stores UTF-8 strings zlib-compressed in a BLOB/bytea column. Returns str in Python.
    """
    impl = BLOB
    cache_ok = True

    def __init__(self, compression_level: int = zlib.Z_DEFAULT_COMPRESSION, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._compression_level = compression_level

    def process_bind_param(self, value: Optional[str], dialect):
        if value is None:
            return None
        data = value.encode("utf-8")
        return zlib.compress(data, self._compression_level)

    def process_result_value(self, value: Optional[bytes], dialect):
        if value is None:
            return None
        try:
            return zlib.decompress(value).decode("utf-8")
        except zlib.error as exc:
            raise ValueError("Failed to decompress CompressedStr payload") from exc