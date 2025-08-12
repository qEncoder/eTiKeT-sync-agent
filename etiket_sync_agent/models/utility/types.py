from typing import Optional
from datetime import datetime, timezone
from sqlalchemy import DateTime, types

class UtcDateTime(types.TypeDecorator):
    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value : Optional[datetime], dialect):
        if value is None:
            return None
        
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")

    def process_result_value(self, value : datetime, dialect):
        value = value.replace(tzinfo=timezone.utc)
        return value