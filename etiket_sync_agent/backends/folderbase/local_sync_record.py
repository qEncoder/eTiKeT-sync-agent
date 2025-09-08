from etiket_sync_agent.exceptions.sync import UpdateSyncDatasetUUIDException
from etiket_sync_agent.models.sync_items import SyncItems
from etiket_sync_agent.sync.sync_records.manager import SyncRecordManager
from etiket_sync_agent.sync.manifests.v2.definitions import QH_MANIFEST_FILE

from pathlib import Path
from typing import Optional

import yaml, uuid, datetime


class LocalSyncRecord:
    def __init__(self, dataset_path : Path, syncIdentifier : SyncItems, sync_record: SyncRecordManager):
        """
        Small wrapper around the sync record manager to write the local record to a file (with the main function to ensure that the uuid is not swapped of the dataset in the folders.)

        Args:
            dataset_path (Path): The path to the dataset directory.
            syncIdentifier (SyncItems): The sync item object.
        """
        self.sync_record = sync_record
        self.sync_time = datetime.datetime.now()
        self.local_record_path = dataset_path / QH_MANIFEST_FILE
        self.local_record = generate_empty_manifest(dataset_path,
                                                syncIdentifier.datasetUUID,
                                                syncIdentifier.scopeUUID)
        if self.local_record_path.exists():
            try :
                with open(self.local_record_path, 'r', encoding="utf-8") as f:
                    local_record = yaml.safe_load(f)
                # if the scope uuid is the same as the updated scope uuid, update the manifest
                if local_record.get('scope_uuid', None) == str(syncIdentifier.scopeUUID):
                    self.local_record = local_record
                    manifest_dataset_uuid = self.local_record.get('dataset_uuid', None)
                    if manifest_dataset_uuid is not None:
                        if manifest_dataset_uuid != str(syncIdentifier.datasetUUID):
                            syncIdentifier.updateDatasetUUID(uuid.UUID(manifest_dataset_uuid))
            except UpdateSyncDatasetUUIDException as e:
                self.write()
                raise e # the sync process should be stopped
            except (yaml.YAMLError, IOError) as e:
                self.local_record['errors'].append(f"Error loading previous manifest: {str(e)}")

    def write(self):
        with open(self.local_record_path, 'w', encoding="utf-8") as f:
            self.local_record['files'] = self.sync_record.to_dict()['files']
            self.local_record['sync_time'] = self.sync_time.isoformat()
            yaml.dump(self.local_record, f)
    
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
        'files': {}
    }