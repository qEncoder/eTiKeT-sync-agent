from etiket_sync_agent.exceptions.sync import UpdateSyncDatasetUUIDException
from etiket_sync_agent.models.sync_items import SyncItems
from etiket_sync_agent.sync.manifest_v2 import QH_MANIFEST_FILE

from pathlib import Path
from typing import Optional

import yaml, uuid, datetime


class Manifest:
    def __init__(self, dataset_path : Path, syncIdentifier : SyncItems):
        """
        Initialize the Manifest object.

        Args:
            dataset_path (Path): The path to the dataset directory.
            syncIdentifier (SyncItems): The sync item object.
        """
        self.manifest_path = dataset_path / QH_MANIFEST_FILE
        self.manifest = generate_empty_manifest(dataset_path,
                                                syncIdentifier.datasetUUID,
                                                syncIdentifier.scopeUUID)
        if self.manifest_path.exists():
            try :
                with open(self.manifest_path, 'r', encoding="utf-8") as f:
                    manifest = yaml.safe_load(f)
                # if the scope uuid is the same as the updated scope uuid, update the manifest
                if manifest.get('scope_uuid', None) == str(syncIdentifier.scopeUUID):
                    self.manifest = manifest
                    self.manifest['errors'] = []
                    manifest_dataset_uuid = self.manifest.get('dataset_uuid', None)
                    if manifest_dataset_uuid is not None:
                        if manifest_dataset_uuid != str(syncIdentifier.datasetUUID):
                            syncIdentifier.updateDatasetUUID(uuid.UUID(manifest_dataset_uuid))
            except UpdateSyncDatasetUUIDException as e:
                self.manifest['errors'].append(str(e))
                self.write()
                raise e # the sync process should be stopped
            except (yaml.YAMLError, IOError) as e:
                self.manifest['errors'].append(f"Error loading previous manifest: {str(e)}")
    
    def add_error(self, error : Exception | str):
        '''
        Add an error to the manifest.
        
        Args:
            error: The error to add.
        '''
        self.manifest.setdefault('errors', []).append(str(error))
    
    def has_errors(self) -> bool:
        """
        Check if the manifest has any errors.

        Returns:
            True if the manifest has errors, False otherwise.
        """
        return bool(self.manifest.get('errors'))
    
    def get_errors(self) -> str:
        """
        Get the errors from the manifest.

        Returns:
            str: A string containing all errors.
        """
        errors = self.manifest.get('errors', [])
        if not errors:
            return "No errors found."

        formatted_errors = "\n\t - ".join(errors)
        return f"Errors found in the manifest:\n\t - {formatted_errors}"
    
    def is_file_uploaded(self, file_name : str, file_path : Path, converter_name : Optional[str] = None) -> bool:
        """
        Checks if the file has been uploaded by comparing the modification time and status.

        Args:
            file_name (str): The name of the file to check.
            file_path (Path): The Path object of the file.
            converter_name (Optional[str]): Optional converter name if checking a converted file.

        Returns:
            True if the file has been uploaded and is up to date, False otherwise.
        """
        file_entry = self.manifest.get('files', {}).get(file_name)
        if not file_entry:
            return False

        current_m_time = get_mtime_of_folder(file_path) if file_path.is_dir() else datetime.datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()

        if converter_name is None:
            return (file_entry.get('m_time') == current_m_time and
                    file_entry.get('status') == "OK")
        else:
            converter_entry = file_entry.get(converter_name, {})
            return ( converter_entry.get('m_time') == current_m_time and
                    converter_entry.get('status') == "OK")
    
    def add_file_upload_info(self, file_name: str, file_path: Path, error=None):
        """
        Add information about a file upload to the manifest.

        This method updates the manifest with the upload status of a file,
        including the file its modification time, status, and any error encountered during upload.

        Args:
            file_name (str): The name of the file being uploaded.
            file_path (Path): The path to the file being uploaded.
            error (Optional[Exception]): An optional exception object if an error occurred during upload.
                If None, the file upload is assumed to be successful.
        """
        file_entry = self.manifest.setdefault('files', {}).setdefault(file_name, {})
        file_entry['m_time'] = datetime.datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
        if error is None:
            file_entry['status'] = "OK"
            file_entry.pop('error', None)
        else:
            file_entry['status'] = "Error"
            file_entry['error'] = str(error)

    def add_file_converter_upload_info(self, file_name : str, file_path : Path,
                                        new_file_name : Path, converter_name : str,
                                        error = None):
        """
        Add information about a file conversion and upload to the manifest.

        This method updates the manifest with the status of a converted file,
        including its output path, modification time of the original file,
        status, and any error encountered during conversion/upload.

        Args:
            file_name (str): The name of the original file being converted.
            file_path (Path): The path to the original file.
            new_file_name (Path): The path to the converted file.
            converter_name (str): The name of the converter used for the conversion.
            error (Optional[Exception]): An optional exception object if an error occurred during conversion/upload.
                If None, the file conversion and upload is assumed to be successful.
        """
        file_entry = self.manifest.setdefault('files', {}).setdefault(file_name, {})
        converter_entry = file_entry.setdefault(converter_name, {})
        converter_entry['output'] = str(new_file_name)
        converter_entry['m_time'] = get_mtime_of_folder(file_path) if file_path.is_dir() else datetime.datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
        if error is None:
            converter_entry['status'] = "OK"
            converter_entry.pop('error', None)
        else:
            converter_entry['status'] = "Error"
            converter_entry['error'] = str(error)
    
    def write(self):
        """
        Write the manifest data to the manifest file.
        """
        with open(self.manifest_path, 'w', encoding="utf-8") as f:
            yaml.dump(self.manifest, f)
    
def get_mtime_of_folder(folder_path : Path) -> float:
    """
    Get the modification time of the folder by taking the maximum of all the files inside it.

    Args:
        folder_path (Path): The path to the folder.

    Returns:
        float: The modification time of the folder.
    """
    latest_mod_time = folder_path.stat().st_mtime
    for file_path in folder_path.rglob('*'):
        if file_path.is_file():
            mod_time = file_path.stat().st_mtime
            if mod_time > latest_mod_time:
                latest_mod_time = mod_time
    return latest_mod_time  
    
def generate_empty_manifest(root_path: Path, dataset_uuid: uuid.UUID, scope_uuid: uuid.UUID) -> dict:
    """
    Generate an empty manifest dictionary.

    This function creates a new manifest dictionary with default values, including version,
    dataset UUID, dataset synchronization path, synchronization time, and empty files and errors entries.

    Args:
        root_path (Path): The root path of the dataset.
        dataset_uuid (uuid.UUID): The unique identifier of the dataset.
        scope_uuid (uuid.UUID): The unique identifier of the scope.

    Returns:
        dict: A dictionary representing an empty manifest.
    """
    return {
        'version': 0.1,
        'dataset_uuid': str(dataset_uuid),
        'scope_uuid': str(scope_uuid),
        'dataset_sync_path': str(root_path),
        'sync_time': datetime.datetime.now().isoformat(),
        'files': {},
        'errors': []
    }