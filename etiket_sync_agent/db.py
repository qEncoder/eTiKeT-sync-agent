import contextlib, os

from pathlib import Path
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from etiket_client.settings.folders import __get_base_dir


DATABASE_URL = f"sqlite+pysqlite:///{Path(__get_base_dir()) / 'sync_agent.db'}"
ENGINE = None
SESSION_LOCAL = None

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
    assert SESSION_LOCAL is not None
    session = SESSION_LOCAL()
    try:
        yield session
    finally:
        session.close()
        
@contextlib.contextmanager
def get_db_session_context() -> Generator[Session, None, None]:
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

load_engine()
