import contextlib, os, platform
import platformdirs as pd

from pathlib import Path
from typing import Generator, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

def __get_base_dir():
    if platform.system() == 'Darwin':
        return os.path.expanduser("~/Library/Containers/com.qdrive.dataQruiser/Data/qdrive")
    if platform.system() == 'Linux':
        return os.path.expanduser("~/.dataQruiser/data/qdrive")
    return f"{pd.user_data_dir()}/qdrive"

DATABASE_URL = f"sqlite+pysqlite:///{Path(__get_base_dir()) / 'sql' / 'sync_agent.db'}"
ENGINE = None
SESSION_LOCAL: Optional[sessionmaker] = None

def load_engine():
    global ENGINE, SESSION_LOCAL

    db_engine = create_engine(DATABASE_URL)
    session_local = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)

    try:
        with db_engine.begin() as connection:
            import etiket_sync_agent
            
            from alembic.config import Config
            from alembic import command

            package_directory = os.path.dirname(etiket_sync_agent.__file__)
            alembic_cfg = Config()
            alembic_cfg.attributes['connection'] = connection
            alembic_cfg.set_main_option("script_location", f"{package_directory}/migrations")
            command.upgrade(alembic_cfg, "head")
    except Exception as e:
        raise ValueError(f"Error upgrading database: {e}")
    
    ENGINE = db_engine
    SESSION_LOCAL = session_local

def get_db_session() -> Generator[Session, None, None]:
    if SESSION_LOCAL is None:
        load_engine()
    assert SESSION_LOCAL is not None
    session = SESSION_LOCAL()
    try:
        yield session
    finally:
        session.close()
        
@contextlib.contextmanager
def get_db_session_context() -> Generator[Session, None, None]:
    if SESSION_LOCAL is None:
        load_engine()
    assert SESSION_LOCAL is not None
    session = SESSION_LOCAL()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
