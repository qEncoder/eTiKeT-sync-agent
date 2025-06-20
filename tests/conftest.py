import pytest, os, tempfile, uuid, datetime
from pathlib import Path
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator, TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import sessionmaker as SessionMaker

@pytest.fixture(scope="session")
def db_engine_sessionmaker():
    """Fixture to set up a temporary SQLite database for the test session."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_sync_agent.db"
        os.environ["SYNC_AGENT_DB_PATH"] = str(db_path)
        
        # remove the db if present
        if db_path.exists():
            db_path.unlink()
            
        db_url = f"sqlite+pysqlite:///{db_path}"
        
        import etiket_sync_agent.db
        etiket_sync_agent.db.DATABASE_URL = db_url
        etiket_sync_agent.db.load_engine()
        
        yield etiket_sync_agent.db.SESSION_LOCAL

@pytest.fixture(scope="function")
def db_session(db_engine_sessionmaker: "SessionMaker[Session]") -> Generator[Session, None, None]:
    """Fixture to provide a database session for a single test function, setting the ENV VAR."""
    session = db_engine_sessionmaker()
    try:
        yield session
        session.commit() # Commit if test successful
    except Exception:
        session.rollback() # Rollback on error
        raise
    finally:
        session.close()
        
@pytest.fixture(scope="session")
def qcodes_set_up():
    from qcodes.dataset import initialise_or_create_database_at
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_qcodes.db"
        initialise_or_create_database_at(db_path)
        
        yield db_path

@pytest.fixture(scope="session")
def file_base_set_up():
    with tempfile.TemporaryDirectory() as temp_dir:
        file_base_path = Path(temp_dir) / "test_file_base"
        yield file_base_path
        
@pytest.fixture(scope="session")
def quantify_set_up():
    with tempfile.TemporaryDirectory() as temp_dir:
        quantify_path = Path(temp_dir) / "test_quantify"
        yield quantify_path
        
@pytest.fixture(scope="session")
def core_tools_set_up():
    pass

@pytest.fixture(scope="session")
def set_up_etiket_client_db():
    '''
    sets up a temp database for etiket_client.
    '''
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_etiket_client.db"
        
        # remove the db if present
        if db_path.exists():
            db_path.unlink()

        db_url = f"sqlite+pysqlite:///{db_path}"
        
        import etiket_client.settings.folders
        etiket_client.settings.folders.SQL_URL = db_url
        import etiket_client.local.database
        etiket_client.local.database.load_engine()
        etiket_client.local.database.run_alembic()
        yield
        
        if db_path.exists():
            db_path.unlink()
            
@pytest.fixture(scope="session")
def set_up_etiket_session(set_up_etiket_client_db):
    '''
    Logs in the user and returns a session.
    '''
    from etiket_client.remote.authenticate import login_legacy
    
    login_legacy(username="test_user", password="test_qdrive", institution_url="https://localhost")
    yield

@pytest.fixture(scope="session")
def get_bucket_uuid(set_up_etiket_session) -> uuid.UUID:
    from etiket_client.remote.endpoints.S3 import s3_bucket_read
    
    buckets = s3_bucket_read()
    
    if len(buckets) == 0:
        raise ValueError("No bucket found")
    
    return buckets[0].bucket_uuid

@pytest.fixture(scope="session")
def get_scope_uuid(get_bucket_uuid : uuid.UUID) -> uuid.UUID:
    bucket_uuid = get_bucket_uuid
    
    # create a new scope:
    from etiket_client.remote.endpoints.scope import scope_create, scope_read
    from etiket_client.remote.endpoints.models.scope import ScopeCreate
    
    # change name to have the current datetime
    scope_uuid = uuid.uuid4()
    sc = ScopeCreate(name=f"test_scope_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}",
                        description="test_scope", 
                        uuid=str(scope_uuid),
                        bucket_uuid=bucket_uuid)
    scope_create(sc)
    
    # sync scopes
    from etiket_client.sync.backends.native.sync_scopes import sync_scopes
    from etiket_client.local.database import Session as SessionEtiketClient, engine
    
    with SessionEtiketClient() as session:
        sync_scopes(session)

    return scope_uuid

@pytest.fixture(scope="function")
def session_etiket_client(set_up_etiket_session) -> Generator[Session, None, None]:
    from etiket_client.local.database import Session as SessionEtiketClient
    
    with SessionEtiketClient() as session:
        yield session