import pathlib, dataclasses

from etiket_sync_agent.crud.sync_sources import crud_sync_sources
from etiket_sync_agent.models.enums import SyncSourceTypes
from etiket_sync_agent.db import get_db_session_context

@dataclasses.dataclass
class FileBaseConfigData:
    root_directory: pathlib.Path
    server_folder : bool
    
    def validate(self):
        """
        Validates the file base configuration.

        Checks:
        1. If the root_directory exists and is a directory.
        2. If the root_directory conflicts with an existing filebase sync source
            (i.e., it's identical, a subdirectory, or a parent directory).

        Raises:
            ValueError: If any validation check fails.

        Returns:
            True if all checks pass.
        """
        # Resolve to an absolute path for consistent comparisons
        try:
            abs_root_dir = self.root_directory.resolve(strict=True)
        except FileNotFoundError:
            raise ValueError(f"The specified root directory does not exist: {self.root_directory}")

        # check if the root directory exists and is a directory.
        if not abs_root_dir.is_dir():
            raise ValueError(f"The specified path is not a directory: {abs_root_dir}")

        # check if the folder is not yet added/is part of a folder that is already added.
        with get_db_session_context() as session:
            sync_sources = crud_sync_sources.list_sync_sources(session)
            for sync_source in sync_sources:
                if sync_source.type == SyncSourceTypes.fileBase:
                    # Assuming config_data stores the path as a string and is always present/valid.
                    existing_path_str = sync_source.config_data['root_directory']
                    existing_path = pathlib.Path(existing_path_str).resolve()

                    # Check for conflicts
                    if abs_root_dir == existing_path:
                        raise ValueError(f"The directory '{abs_root_dir}' is already added as sync source '{sync_source.name}'.")
                    # Check if the new path is inside an existing path
                    if abs_root_dir.is_relative_to(existing_path):
                        raise ValueError(f"The directory '{abs_root_dir}' is inside the directory '{existing_path}' added by sync source '{sync_source.name}'.")
                    # Check if an existing path is inside the new path
                    if existing_path.is_relative_to(abs_root_dir):
                        raise ValueError(f"The directory '{existing_path}' added by sync source '{sync_source.name}' is inside the specified directory '{abs_root_dir}'.")

        return True
        