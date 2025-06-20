import datetime

from typing import Optional
from sqlalchemy.types import TypeDecorator, DateTime

# based on sqlalchemy_utc from pypi.

class UtcDateTime(TypeDecorator):
    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value : Optional[datetime.datetime], dialect):
        if value is not None:
            if not isinstance(value, datetime.datetime):
                raise TypeError('expecting datetime.datetime type')
            return value.astimezone(datetime.timezone.utc)
        return value

    def process_result_value(self, value : datetime.datetime, dialect):
        if value is not None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return value