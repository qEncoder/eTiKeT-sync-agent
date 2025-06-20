from sqlalchemy.sql.expression import FunctionElement
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import DateTime

class utcnow(FunctionElement):
    type = DateTime()
    inherit_cache = True

@compiles(utcnow, "sqlite")
def sqlite_utcnow(element, compiler, **kw):
    return "DATETIME('now', 'utc', 'subsec')"