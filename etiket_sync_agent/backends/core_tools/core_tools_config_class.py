import dataclasses, psycopg2

from etiket_sync_agent.crud.sync_sources import crud_sync_sources
from etiket_sync_agent.models.enums import SyncSourceTypes
from etiket_sync_agent.db import get_db_session_context

@dataclasses.dataclass
class CoreToolsConfigData:
    dbname : str
    user : str
    password : str
    host : str = "localhost"
    port : int = 5432
    
    def validate(self):
        """
        Validates the connection details.

        Checks:
        1. If a sync source with the same dbname and host already exists.
        2. If a connection to the target database can be established.
        3. If the 'global_measurement_overview' table exists in the public schema.

        Raises:
            ValueError: If any validation check fails.

        Returns:
            True if all checks pass.
        """
        # check if the database is already added.
        with get_db_session_context() as session:
            sync_sources = crud_sync_sources.list_sync_sources(session)
            for sync_source in sync_sources:
                if sync_source.type == SyncSourceTypes.coretools:
                    if sync_source.config_data['dbname'] == self.dbname and sync_source.config_data['host'] == self.host:
                        raise ValueError(f"The database '{self.dbname}' on the host '{self.host}' is already added.")
        
        # check if it is possible to connect to the database and check table existence.
        try:
            with psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host, port=self.port) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'global_measurement_overview');")
                    res = cur.fetchone()[0]
                    if not res:
                        raise ValueError("The database does not contain the 'global_measurement_overview' table in the 'public' schema.")
        except psycopg2.OperationalError as e:
            raise ValueError(f"Failed to connect to the database or execute query: {e}") from e
        
        return True