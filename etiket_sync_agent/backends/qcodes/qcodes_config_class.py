import pathlib, dataclasses, sqlite3
from typing import Optional

from etiket_sync_agent.crud.sync_sources import crud_sync_sources, SyncSources
from etiket_sync_agent.models.enums import SyncSourceTypes
from etiket_sync_agent.db import get_db_session_context

@dataclasses.dataclass
class QCoDeSConfigData:
    database_path: pathlib.Path
    set_up : str
    static_attributes : Optional[dict] = dataclasses.field(default_factory=dict)
    
    def validate(self, current_sync_source : Optional[SyncSources] = None):
        """
        Validates the QCoDeS database configuration.

        Checks:
        1. If the database_path path points to an existing file.
        2. If the file has a .db extension.
        3. If the file can be opened as an SQLite database.
        4. If the database file has already been added as a QCoDeS sync source.

        Args:
            current_source (Optional[SyncSources]): in case of update, the current sync source is provided.

        Raises:
            ValueError: If any validation check fails.

        Returns:
            True if all checks pass.
        """
        try:
            abs_db_path = pathlib.Path(self.database_path).resolve(strict=True)
        except FileNotFoundError:
            raise ValueError(f"The specified database file does not exist: {self.database_path}")
        
        if not abs_db_path.is_file():
            raise ValueError(f"The specified path is not a file: {abs_db_path}")

        if abs_db_path.suffix != ".db":
            raise ValueError(f"A QCoDeS database file must have a .db extension, not '{abs_db_path.suffix}'. Provided path: {abs_db_path}")

        # test basic sqlite3 connection
        try:
            conn = sqlite3.connect(f'file:{abs_db_path}?mode=ro', uri=True)
            conn.close()
        except sqlite3.Error as e:
            raise ValueError(f"Error opening SQLite database file '{abs_db_path}': {e}") from e
        
        # check if the database file is already added.
        with get_db_session_context() as session:
            sync_sources = crud_sync_sources.list_sync_sources(session)
            for sync_source in sync_sources:
                if sync_source.type == SyncSourceTypes.qcodes:
                    try:
                        existing_path_str = sync_source.config_data['database_path']
                        existing_path = pathlib.Path(existing_path_str).resolve()
                    except Exception as e:
                        # If an existing path can't be resolved, log it but continue validation
                        print(f"Warning: Could not resolve existing database path '{existing_path_str}' for sync source '{sync_source.name}' ({sync_source.id}): {e}")
                        continue
                    
                    if abs_db_path == existing_path:
                        if current_sync_source is not None and current_sync_source.id == sync_source.id:
                            continue
                        raise ValueError(f"The database file '{abs_db_path}' is already added as sync source '{sync_source.name}'.")

        return True