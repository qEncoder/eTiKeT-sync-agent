import pathlib, dataclasses

from etiket_sync_agent.crud.sync_sources import crud_sync_sources
from etiket_sync_agent.models.enums import SyncSourceTypes
from etiket_sync_agent.db import get_db_session_context

@dataclasses.dataclass
class QuantifyConfigData:
    quantify_directory: pathlib.Path
    set_up : str
    
    def validate(self):
        """
        Validates the Quantify base directory configuration.

        Checks:
        1. If the quantify_directory exists and is a directory.
        2. If the quantify_directory conflicts with an existing Quantify sync source
            (i.e., it's identical, a subdirectory, or a parent directory).

        Raises:
            ValueError: If any validation check fails.

        Returns:
            True if all checks pass.
        """
        # Resolve to an absolute path for consistent comparisons
        try:
            abs_quantify_dir = self.quantify_directory.resolve(strict=True)
        except FileNotFoundError as e:
            raise ValueError(f"The specified Quantify directory does not exist: {self.quantify_directory}") from e

        # check if the path exists and is a directory.
        if not abs_quantify_dir.is_dir():
            raise ValueError(f"The specified path is not a directory: {abs_quantify_dir}")

        # check if the directory is not yet added/is part of a directory that is already added.
        with get_db_session_context() as session:
            sync_sources = crud_sync_sources.list_sync_sources(session)
            for sync_source in sync_sources:
                # Assuming 'quantify' is the correct enum member
                if sync_source.type == SyncSourceTypes.quantify: 
                    # Assuming config_data stores the path as a string and is always present/valid.
                    try:
                        existing_path_str = sync_source.config_data['quantify_directory']
                        existing_path = pathlib.Path(existing_path_str).resolve()
                    except Exception as e:
                        # If an existing path can't be resolved, log it but continue validation
                        print(f"Warning: Could not resolve existing quantify directory '{existing_path_str}' for sync source '{sync_source.name}' ({sync_source.id}): {e}")
                        continue

                    # Check for conflicts
                    if abs_quantify_dir == existing_path:
                        raise ValueError(f"The directory '{abs_quantify_dir}' is already added as sync source '{sync_source.name}'.")
                    # Check if the new path is inside an existing path
                    if abs_quantify_dir.is_relative_to(existing_path):
                        raise ValueError(f"The directory '{abs_quantify_dir}' is inside the directory '{existing_path}' added by sync source '{sync_source.name}'.")
                    # Check if an existing path is inside the new path
                    if existing_path.is_relative_to(abs_quantify_dir):
                        raise ValueError(f"The directory '{existing_path}' added by sync source '{sync_source.name}' is inside the specified directory '{abs_quantify_dir}'.")

        return True
        