'''
TODO :: enable usage case with local immutable files, when this is implemented!!!!


Here the upload mechanism and logic is tested for the file upload.

There are several cases that need to be considered, as the file could be stored locally already, or already be present on the server.

The logic should go as follows:
* a file is given with a certain version id :
-> a check should be performed if this version is already present on the remote server,
    -- if yes, do nothing (md5 checksum should match) --> if status == Pending --> try to re-upload the file
    -- if no, 
        -> if immutable == False --> overwrite file
        -> if immutable == True  --> create a new version id, based on the current time stamp -- check with the last version id, if the MD5 checksum matches, if this is the case, do not upload a new file.


To test this, a test should be design where a HDF5 file (content aware checksum) is uploaded and a json file (not content aware checksum)

Case 1: single version only in local and/or remote database

--> assume version local/remote has the same version id that the current version id, in this case corresponding to the version id of version 1.

Each version has the symbols
P -> present
I -> if immutable
C -> if upload compete (only applicable for remote field)
M -> if MD5 checksum should match

| id | version 1 local | version 1 remote | candidate_version_match | expected result                                                             | version_id to check (-1 to check for a new version)
|----|-----------------|------------------|-------------------------|-----------------------------------------------------------------------------| 
| 1  |                 |                  | None                    | -> upload this version                                                      | -1
| 2  | PIM             | PIMC             | 1                       | -> no upload                                                                | 1
| 3  | PIM             | PIM              | 1                       | -> upload this version                                                      | 1
| 4  | PI              | PI C             | 1                       | -> create new version_id and upload to server                               | -1
| 5  | PI              | PI               | 1                       | -> create new version_id and upload to server                               | -1
| 6  | P               | P  C             | 1                       | -> upload this version -- replace file -- also update local details         | 1
| 7  | P               | P                | 1                       | -> upload this version -- replace file -- also update local details         | 1
| 8  | PIM             |                  | 1                       | -> upload this version                                                      | 1
| 9  | PI              |                  | 1                       | -> create new version_id and upload to server                               | -1
| 10 | P               |                  | 1                       | -> upload this version -- also update local details                         | 1
| 11 |                 | PIMC             | 1                       | -> no upload                                                                | 1
| 12 |                 | PIM              | 1                       | -> upload this version                                                      | 1
| 13 |                 | PI C             | 1                       | -> create new version_id and upload to server                               | -1
| 14 |                 | PI               | 1                       | -> upload this version (no checksum known to the server)                    | 1
| 15 |                 | P  C             | 1                       | -> upload this version                                                      | 1
| 16 |                 | P                | 1                       | -> upload this version                                                      | 1

Case 2 : two versions present

Version 1 and version 2 are assumed to match on local and remote if they are present

Each version has the symbols
P -> present
I -> if immutable
C -> if upload compete (only applicable for remote field)
M -> if MD5 checksum should match

| id | version 1 local | version 2 local | version 1 remote | version 2 remote | candidate_version_match   | expected result                                                              |
|----|-----------------|-----------------|------------------|------------------|---------------------------|------------------------------------------------------------------------------|
| 1  | PIM             | PI              | PIMC             | PIC              | None                      | -> create new version_id and upload to server                                |
| 2  | PIM             | PI              | PIMC             | PIC              | 1                         | -> no upload                                                                 |
| 3  | PIM             | PI              | PIMC             | PIC              | 2                         | -> create new version_id and upload to server                                |
| 4  | PI              | PIM             | PI C             | PIMC             | None                      | -> no upload (content matches latest remote V2)                              |
| 5  | PI              | PIM             | PI C             | PIMC             | 1                         | -> no upload (content already matches last version V2)                       |
| 6  | PI              | PIM             | PI C             | PIMC             | 2                         | -> no upload                                                                 |
| 7  | PIM             | PI              |                  |                  | None                      | -> create new version_id and upload to server                                |
| 8  | PIM             | PI              |                  |                  | 1                         | -> upload this version                                                       |
| 9  | PIM             | PI              |                  |                  | 2                         | -> create new version_id and upload to server                                |
| 10 | PI              | PIM             |                  |                  | None                      | -> match with version 2, upload with version id of version 2                 |
| 11 | PI              | PIM             |                  |                  | 1                         | -> upload as version 2                                                       |
| 12 | PI              | PIM             |                  |                  | 2                         | -> upload this version                                                       |
| 13 |                 |                 | PIMC             | PIC              | None                      | -> create new version_id and upload to server                                |
| 14 |                 |                 | PIMC             | PIC              | 1                         | -> no upload                                                                 |
| 15 |                 |                 | PIMC             | PIC              | 2                         | -> create new version_id and upload to server                                |
| 16 |                 |                 | PI C             | PIMC             | None                      | -> no upload (content matches latest remote V2)                              |
| 17 |                 |                 | PI C             | PIMC             | 1                         | -> no upload (V1 content matches remote V1)                                  |
| 18 |                 |                 | PI C             | PIMC             | 2                         | -> no upload                                                                 |

'''

import uuid
import datetime
import tempfile
import os
import json
import pytest, time
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from pathlib import Path
import xarray as xr
import numpy as np

from etiket_client.local.models.file import FileCreate as FileCreateLocal, FileUpdate as FileUpdateLocal
from etiket_client.local.dao.file import dao_file
from etiket_client.local.exceptions import FileNotAvailableException

from etiket_client.remote.endpoints.models.file import FileCreate as FileCreateRemote
from etiket_client.remote.endpoints.file import file_create, file_read_by_name, file_generate_presigned_upload_link_single
from etiket_client.remote.endpoints.models.types import FileStatusRem, FileStatusLocal, FileType

from etiket_sync_agent.sync.sync_utilities import sync_utilities, file_info
from etiket_sync_agent.sync.sync_records.manager import SyncRecordManager
from etiket_sync_agent.models.sync_items import SyncItems
from etiket_sync_agent.sync.uploader.file_uploader import upload_new_file_single
from etiket_sync_agent.sync.checksums.any import md5
from etiket_sync_agent.sync.checksums.hdf5 import md5_netcdf4
from etiket_client.python_api.dataset_model.files import generate_version_id


@dataclass
class LocalFileConfig:
    """Configuration for local file state in tests"""
    present: bool = False
    version_id: int = 1
    file_path: Optional[str] = None
    immutable: bool = True
    status: FileStatusLocal = FileStatusLocal.complete
    
    def create_file(self, dataset_uuid: uuid.UUID, file_name: str, file_uuid: uuid.UUID, session, file_path: str) -> Optional[str]:
        """Create a local file based on the configuration"""
        if not self.present:
            return None
        
        # Calculate MD5 checksum from the provided file
        md5_checksum = md5(file_path)
        version_id = generate_version_id(datetime.datetime.fromtimestamp(os.path.getmtime(file_path)))
        print(file_uuid, version_id)
        
        file_type = FileType.JSON
        if file_path.endswith('.hdf5'):
            file_type = FileType.HDF5_NETCDF
        
        # Create local database entry
        file_create_local = FileCreateLocal(
            name=file_name,
            filename=os.path.basename(file_path),
            uuid=file_uuid,
            creator="test_user",

            collected=datetime.datetime.now(),
            size=os.path.getsize(file_path),
            type=file_type,
            file_generator="test",
            version_id=version_id,
            etag=None,
            status=self.status,
            local_path=file_path,
            ds_uuid=dataset_uuid,
            synchronized=False
        )
        
        dao_file.create(file_create_local, session=session)
        return file_path


@dataclass 
class RemoteFileConfig:
    """Configuration for remote file state in tests"""
    present: bool = False
    version_id: int = 1
    immutable: bool = True
    upload_complete: bool = True
    status: FileStatusRem = FileStatusRem.secured
    
    def create_file(self, dataset_uuid: uuid.UUID, file_name: str, file_uuid: uuid.UUID, file_path: str) -> Optional[str]:
        """Create a remote file based on the configuration"""
        if not self.present:
            return None
        
        # Set status based on upload_complete flag
        if not self.upload_complete:
            self.status = FileStatusRem.pending
        file_type = FileType.JSON
        if file_path.endswith('.hdf5'):
            file_type = FileType.HDF5_NETCDF
        
        version_id = generate_version_id(datetime.datetime.fromtimestamp(os.path.getmtime(file_path)))
        print("REMOTE FILE", file_name, file_uuid, version_id)
        # Create remote file entry
        file_create_remote = FileCreateRemote(
            name=file_name,
            filename=os.path.basename(file_path),
            uuid=file_uuid,
            creator="test_user",
            collected=datetime.datetime.now(),
            size=os.path.getsize(file_path),
            type=file_type,
            file_generator="test",
            version_id=version_id,
            ds_uuid=dataset_uuid,
            immutable=self.immutable
        )
        
        file_create(file_create_remote)
        
        # Upload the file if upload_complete is True
        if self.upload_complete:
            upload_info = file_generate_presigned_upload_link_single(file_create_remote.uuid, version_id)
            md5_hash = md5(file_path)
            md5_hash_netcdf4 = None
            if file_path.endswith('.hdf5'):
                md5_hash_netcdf4 = md5_netcdf4(file_path)
            upload_new_file_single(file_path, upload_info, md5_hash, md5_hash_netcdf4)
        
        return file_path

@dataclass
class DataTestCaseSingle:
    """Test case configuration for file upload scenarios"""
    case_id: int
    checksum_version_id_match: Optional[int] = None
    local_config: Optional[LocalFileConfig] = None
    remote_config: Optional[RemoteFileConfig] = None
    expected_version_id: int = -1  # -1 means check for new version, positive number means check specific version

    
# Case 1 test configurations - single version scenarios
CASE_1_TESTS = {
    1: DataTestCaseSingle(
        case_id=1,
        local_config=None,
        remote_config=None,
        checksum_version_id_match = None,
        expected_version_id=-1,  # Should create new version
    ),
    2: DataTestCaseSingle(
        case_id=2,
        local_config=LocalFileConfig(present=True, immutable=True),
        remote_config=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        checksum_version_id_match = 1,
        expected_version_id=1,  # Should find existing version 1
    ),
    3: DataTestCaseSingle(
        case_id=3,
        local_config=LocalFileConfig(present=True, immutable=True),
        remote_config=RemoteFileConfig(present=True, immutable=True, upload_complete=False),
        checksum_version_id_match = 1,
        expected_version_id=1,  # Should upload to existing version 1 (pending)
    ),
    4: DataTestCaseSingle(
        case_id=4,
        local_config=LocalFileConfig(present=True, immutable=True),
        remote_config=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        expected_version_id=-1,  # Should create new version because immutable and different checksum
    ),
    5: DataTestCaseSingle(
        case_id=5,
        local_config=LocalFileConfig(present=True, immutable=True),
        remote_config=RemoteFileConfig(present=True, immutable=True, upload_complete=False),
        checksum_version_id_match = None,
        expected_version_id=-1,  # Should create a new version, as it can compare to the local file, which will have a non-matching checksum
    ),
    # 6: TestCase(
    #     case_id=6,
    #     local_config=LocalFileConfig(present=True, immutable=False),
    #     remote_config=RemoteFileConfig(present=True, immutable=False, upload_complete=True),
    #     expected_version_id=1,  # Should replace existing version 1 because mutable, also the local file should be updated
    # ),
    # 7: TestCase(
    #     case_id=7,
    #     local_config=LocalFileConfig(present=True, immutable=False),
    #     remote_config=RemoteFileConfig(present=True, immutable=False, upload_complete=False),
    #     expected_version_id=1,  # Should replace existing version 1 because mutable
    # ),
    8: DataTestCaseSingle(
        case_id=8,
        local_config=LocalFileConfig(present=True, immutable=True),
        remote_config=None,
        checksum_version_id_match = 1,
        expected_version_id=1,  # Should upload to version 1 (file exists locally but not remotely)
    ),
    9: DataTestCaseSingle(
        case_id=9,
        local_config=LocalFileConfig(present=True, immutable=True),
        remote_config=None,
        expected_version_id=-1,  # Should create new version (immutable file exists locally)
    ),
    # 10: TestCase(
    #     case_id=10,
    #     local_config=LocalFileConfig(present=True, immutable=False),
    #     remote_config=None,
    #     expected_version_id=1,  # Should upload to version 1 (mutable file can be updated)
    # ),
    11: DataTestCaseSingle(
        case_id=11,
        local_config=None,
        remote_config=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        checksum_version_id_match = 1,
        expected_version_id=1,  # Should find existing version 1 and skip (checksum matches)
    ),
    12: DataTestCaseSingle(
        case_id=12,
        local_config=None,
        remote_config=RemoteFileConfig(present=True, immutable=True, upload_complete=False),
        expected_version_id=1,  # Should upload to existing version 1 (pending upload)
    ),
    13: DataTestCaseSingle(
        case_id=13,
        local_config=None,
        remote_config=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        expected_version_id=-1,  # Should create new version (different checksum, immutable)
    ),
    14: DataTestCaseSingle(
        case_id=14,
        local_config=None,
        remote_config=RemoteFileConfig(present=True, immutable=True, upload_complete=False),
        expected_version_id=1,  # Should upload to existing version 1 (no checksum known to the server)
    ),
    15: DataTestCaseSingle(
        case_id=15,
        local_config=None,
        remote_config=RemoteFileConfig(present=True, immutable=False, upload_complete=True),
        expected_version_id=1,  # Should upload to existing version 1 (mutable can be overwritten)
    ),
    16: DataTestCaseSingle(
        case_id=16,
        local_config=None,
        remote_config=RemoteFileConfig(present=True, immutable=False, upload_complete=False),
        expected_version_id=1,  # Should upload to existing version 1 (mutable, pending)
    ),
}

@pytest.mark.parametrize("is_hdf5", [False, True])
@pytest.mark.parametrize("case_id", list(CASE_1_TESTS.keys()))
def test_file_upload_case_1(
    case_id,
    session_etiket_client,
    get_scope_uuid,
    is_hdf5,
    db_session
):
    """Test file upload scenarios for Case 1"""
    test_case = CASE_1_TESTS[case_id]
    
    # Setup
    dataset_uuid = uuid.uuid4()
    scope_uuid = get_scope_uuid
    file_name = f"test_file_case_{case_id}_{'hdf5' if is_hdf5 else 'json'}"
    file_uuid = uuid.uuid4()
    file_type = FileType.JSON if not is_hdf5 else FileType.HDF5_NETCDF
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create dataset first (simplified)
        from etiket_client.local.models.dataset import DatasetCreate as DatasetCreateLocal
        from etiket_client.local.dao.dataset import dao_dataset
        from etiket_client.remote.endpoints.models.dataset import DatasetCreate as DatasetCreateRemote
        from etiket_client.remote.endpoints.dataset import dataset_create
        
        if is_hdf5: # use xarray file            
            file_path = os.path.join(temp_dir, f"{file_name}.hdf5")
            dataset = xr.Dataset({
                'var1': (('x', 'y'), np.random.rand(10, 10)),
                'var2': (('x', 'y'), np.random.rand(10, 10))
            })
            dataset.to_netcdf(file_path, engine='h5netcdf', invalid_netcdf=True)
        else:
            # Create file content first - this will be the base file content
            file_path = os.path.join(temp_dir, f"{file_name}.json")
            file_content = {"version": "base", "test": "base_data", "timestamp": str(datetime.datetime.now())}
            
            with open(file_path, 'w') as f:
                json.dump(file_content, f)

        # Create local dataset
        if test_case.local_config is not None:
            with session_etiket_client as session:
                local_dataset_create = DatasetCreateLocal(
                    uuid=dataset_uuid,
                    alt_uid=f"test_alt_uid_{case_id}_{'hdf5' if is_hdf5 else 'json'}",
                    name=f"Test Dataset Case {case_id} {'hdf5' if is_hdf5 else 'json'}",
                    description="Test dataset",
                    keywords=["test"],
                    attributes={},
                    ranking=0,
                    creator="test_user",
                    scope_uuid=scope_uuid,
                    collected=datetime.datetime.now()
                )
                dao_dataset.create(local_dataset_create, session=session)
                test_case.local_config.create_file(dataset_uuid, file_name, file_uuid, session, file_path)
        
        # Create remote dataset
        remote_dataset_create = DatasetCreateRemote(
            uuid=dataset_uuid,
            alt_uid=f"test_alt_uid_{case_id}_{'hdf5' if is_hdf5 else 'json'}",
            name=f"Test Dataset Case {case_id} {'hdf5' if is_hdf5 else 'json'}",
            description="Test dataset",
            keywords=["test"],
            attributes={},
            ranking=0,
            creator="test_user",
            scope_uuid=scope_uuid,
            collected=datetime.datetime.now()
        )
        dataset_create(remote_dataset_create)
        
        if test_case.remote_config is not None:
            test_case.remote_config.create_file(dataset_uuid, file_name, file_uuid, file_path)
        
        # Create sync item
        sync_item = SyncItems(
            id=case_id*2 + int(is_hdf5),
            sync_source_id=1,
            dataIdentifier=f"test_data_identifier_case_{case_id}_{'hdf5' if is_hdf5 else 'json'}",
            datasetUUID=dataset_uuid,
            syncPriority=1.0,
            synchronized=False,
            attempts=0,
            sync_record={},
            error=None,
            traceback=None
        )
        
        with db_session as session:
            session.add(sync_item)
            session.commit()
            session.refresh(sync_item)
        
        test_file_path = file_path

        if test_case.checksum_version_id_match is None:
            if is_hdf5:
                test_file_path = os.path.join(temp_dir, f"{file_name}_test.hdf5")
                dataset = xr.Dataset({
                    'var1': (('x', 'y'), np.random.rand(10, 11)),
                    'var2': (('x', 'y'), np.random.rand(10, 11))
                })
                dataset.to_netcdf(test_file_path, engine='h5netcdf', invalid_netcdf=True)
            else:
                test_file_path = os.path.join(temp_dir, f"{file_name}_test.json")
                test_content = {"version": "upload", "test": "upload_data", "timestamp": str(datetime.datetime.now())}
                with open(test_file_path, 'w') as f:
                    json.dump(test_content, f)
        
        # Create file_info for upload
        f_info = file_info(
            name=file_name,
            fileName=f"{file_name}.json",
            created=datetime.datetime.fromtimestamp(os.path.getmtime(file_path)),
            fileType=file_type,
            creator="test_user",
            file_generator="test",
            immutable_on_completion=True
        )
        
        # Create dataset manifest
        sync_record = SyncRecordManager(sync_item=sync_item, dataset_path=None)
        
        # Record initial state for comparison
        initial_remote_files = []
        try:
            initial_remote_files = file_read_by_name(dataset_uuid, file_name)
        except Exception:
            pass
        
        initial_local_files = []
        try:
            with session_etiket_client as session:
                initial_local_files = dao_file.get_file_by_name(dataset_uuid, file_name, session)
        except Exception:
            pass
        
        # Act - Call the upload_file method
        sync_utilities.upload_file(test_file_path, sync_item, f_info, sync_record)
        # Verify results based on expected_version_id
        final_remote_files = []
        try:
            final_remote_files = file_read_by_name(dataset_uuid, file_name)
        except Exception:
            pass
        
        final_local_files = []
        try:
            with session_etiket_client as session:
                final_local_files = dao_file.get_file_by_name(dataset_uuid, file_name, session)
        except Exception:
            pass
        
        upload_file_checksum = None
        if not is_hdf5:
            upload_file_checksum = str(md5(test_file_path).hexdigest())
        else:
            upload_file_checksum = str(md5_netcdf4(test_file_path).hexdigest())
        
        if test_case.expected_version_id == -1:
            # Should have created a new version - check that we have more files than initially
            if test_case.remote_config and test_case.remote_config.present:
                assert len(final_remote_files) > len(initial_remote_files), f"Case {case_id}: Expected new version to be created remotely"
            else:
                # this means a new version had been created, as such the remote file should have a different version id from the local file
                if test_case.remote_config is not None:
                    version_id = generate_version_id(datetime.datetime.fromtimestamp(os.path.getmtime(file_path)))
                    assert final_remote_files[0].version_id != version_id, f"Case {case_id}: Expected new version to be created remotely"
            
            # The latest remote file should have the checksum of the uploaded file and be complete
            if final_remote_files:
                latest_remote = max(final_remote_files, key=lambda f: f.version_id)
                assert latest_remote.status == FileStatusRem.secured, f"Case {case_id}: Remote file should be complete"
                assert latest_remote.md5_checksum == upload_file_checksum, f"Case {case_id}: Remote file checksum should match uploaded file checksum"
        
        elif test_case.expected_version_id == 1:
            # Should use existing version 1 or create version 1 if none existed

            version_id = generate_version_id(datetime.datetime.fromtimestamp(os.path.getmtime(file_path)))
            if test_case.remote_config and test_case.remote_config.present:
                # File should exist with version 1 and be complete
                version_1_files = [f for f in final_remote_files if f.version_id == version_id]
                assert len(version_1_files) == 1, f"Case {case_id}: Expected exactly one file with version 1"
                assert version_1_files[0].status == FileStatusRem.secured, f"Case {case_id}: File should have been secured"
                assert version_1_files[0].md5_checksum == upload_file_checksum, f"Case {case_id}: Remote file checksum should match uploaded file checksum"
            else:
                # Should have created version 1
                assert len(final_remote_files) == 1, f"Case {case_id}: Expected exactly one file to be created"
                assert final_remote_files[0].version_id == version_id, f"Case {case_id}: Expected version 1 to be created"
                assert final_remote_files[0].status == FileStatusRem.secured, f"Case {case_id}: File should be complete"
                assert final_remote_files[0].md5_checksum == upload_file_checksum, f"Case {case_id}: Remote file checksum should match uploaded file checksum"

        # ensure the checksum of the local version matches the checksum of the remote version
        for local_file in final_local_files:
            for remote_file in final_remote_files:
                if local_file.version_id == remote_file.version_id:
                    if not is_hdf5:
                        local_file_md5 = str(md5(local_file.local_path).hexdigest())
                    else:
                        local_file_md5 = str(md5_netcdf4(local_file.local_path).hexdigest())
                    remote_file_md5 = None
                    if remote_file.md5_checksum is not None:
                        remote_file_md5 = remote_file.md5_checksum
                        print(local_file_md5, remote_file_md5)
                        print(local_file.version_id, remote_file.version_id)
                        print(local_file.name, remote_file.name)
                        assert local_file_md5 == remote_file_md5, f"Case {case_id}: Local file checksum should match remote file checksum"
                        
@dataclass
class DataTestCaseSingleDual:
    """Test case configuration for file upload scenarios"""
    case_id: int
    local_config_1: Optional[LocalFileConfig] = None
    remote_config_1: Optional[RemoteFileConfig] = None
    local_config_2: Optional[LocalFileConfig] = None
    remote_config_2: Optional[RemoteFileConfig] = None
    file_matches_version: List[int] = field(default_factory=list)
    file_uploaded_to_version_id : int = -1
    expected_version_id: int = -1  # -1 means check for new version, positive number means check specific version

CASE_1_TESTS_DUAL = {
    1: DataTestCaseSingleDual(
        case_id=1,
        local_config_1=LocalFileConfig(present=True, immutable=True),
        remote_config_1=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        local_config_2=LocalFileConfig(present=True, immutable=True),
        remote_config_2=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        file_matches_version = [1],
        file_uploaded_to_version_id = -1, # -1 means it will have a version in the future.
        expected_version_id=-1,  # Should create new version
    ),
    2: DataTestCaseSingleDual(
        case_id=2,
        local_config_1=LocalFileConfig(present=True, immutable=True),
        remote_config_1=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        local_config_2=LocalFileConfig(present=True, immutable=True),
        remote_config_2=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        file_matches_version = [1],
        file_uploaded_to_version_id = 1, # upload to version id of 1
        expected_version_id=1,  # Should match existing version 1
    ),
    3: DataTestCaseSingleDual(
        case_id=3,
        local_config_1=LocalFileConfig(present=True, immutable=True),
        remote_config_1=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        local_config_2=LocalFileConfig(present=True, immutable=True),
        remote_config_2=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        file_matches_version = [1],
        file_uploaded_to_version_id = 2, # upload to version id of 2
        expected_version_id=-1,  # Should create new version
    ),
    4: DataTestCaseSingleDual(
        case_id=4,
        local_config_1=LocalFileConfig(present=True, immutable=True),
        remote_config_1=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        local_config_2=LocalFileConfig(present=True, immutable=True),
        remote_config_2=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        file_matches_version = [2],
        file_uploaded_to_version_id = -1, # -1 means it will have a version in the future.
        expected_version_id=2,  # Should match existing version 2
    ),
    5: DataTestCaseSingleDual(
        case_id=5,
        local_config_1=LocalFileConfig(present=True, immutable=True),
        remote_config_1=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        local_config_2=LocalFileConfig(present=True, immutable=True),
        remote_config_2=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        file_matches_version = [2],
        file_uploaded_to_version_id = 1, # upload to version id of 1
        expected_version_id=2,  # Should match existing version 2
    ),
    6: DataTestCaseSingleDual(
        case_id=6,
        local_config_1=LocalFileConfig(present=True, immutable=True),
        remote_config_1=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        local_config_2=LocalFileConfig(present=True, immutable=True),
        remote_config_2=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        file_matches_version = [2],
        file_uploaded_to_version_id = 2, # upload to version id of 1
        expected_version_id=2,  # Should match existing version 2
    ),
    7: DataTestCaseSingleDual(
        case_id=7,
        local_config_1=LocalFileConfig(present=True, immutable=True),
        remote_config_1=None,
        local_config_2=LocalFileConfig(present=True, immutable=True),
        remote_config_2=None,
        file_matches_version = [1],
        file_uploaded_to_version_id = -1, # -1 means it will have a version in the future.
        expected_version_id=-1,  # Should create new version
    ),
    8: DataTestCaseSingleDual(
        case_id=8,
        local_config_1=LocalFileConfig(present=True, immutable=True),
        remote_config_1=None,
        local_config_2=LocalFileConfig(present=True, immutable=True),
        remote_config_2=None,
        file_matches_version = [1],
        file_uploaded_to_version_id = 1, # upload to version id of 1
        expected_version_id=1,  # Should match existing version 1
    ),
    9: DataTestCaseSingleDual(
        case_id=9,
        local_config_1=LocalFileConfig(present=True, immutable=True),
        remote_config_1=None,
        local_config_2=LocalFileConfig(present=True, immutable=True),
        remote_config_2=None,
        file_matches_version = [1],
        file_uploaded_to_version_id = 2,
        expected_version_id=-1,  # Should create new version
    ),
    10: DataTestCaseSingleDual(
        case_id=10,
        local_config_1=LocalFileConfig(present=True, immutable=True),
        remote_config_1=None,
        local_config_2=LocalFileConfig(present=True, immutable=True),
        remote_config_2=None,
        file_matches_version = [2],
        file_uploaded_to_version_id = -1, # -1 means it will have a version in the future.
        expected_version_id=2,  # Should match existing version 2
    ),
    11: DataTestCaseSingleDual(
        case_id=11,
        local_config_1=LocalFileConfig(present=True, immutable=True),
        remote_config_1=None,
        local_config_2=LocalFileConfig(present=True, immutable=True),
        remote_config_2=None,
        file_matches_version = [2],
        file_uploaded_to_version_id = 1, # upload to version id of 1
        expected_version_id=2,  # Should match existing version 2
    ),
    12: DataTestCaseSingleDual(
        case_id=12,
        local_config_1=LocalFileConfig(present=True, immutable=True),
        remote_config_1=None,
        local_config_2=LocalFileConfig(present=True, immutable=True),
        remote_config_2=None,
        file_matches_version = [2],
        file_uploaded_to_version_id = 2, # upload to version id of 2
        expected_version_id=2,  # Should match existing version 2
    ),
    13: DataTestCaseSingleDual(
        case_id=13,
        local_config_1=None,
        remote_config_1=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        local_config_2=None,
        remote_config_2=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        file_matches_version = [1],
        file_uploaded_to_version_id = -1, # -1 means it will have a version in the future.
        expected_version_id=-1,  # Should create new version
    ),
    14: DataTestCaseSingleDual(
        case_id=14,
        local_config_1=None,
        remote_config_1=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        local_config_2=None,
        remote_config_2=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        file_matches_version = [1],
        file_uploaded_to_version_id = 1, # upload to version id of 1
        expected_version_id=1,  # Should match existing version 1
    ),
    15: DataTestCaseSingleDual(
        case_id=15,
        local_config_1=None,
        remote_config_1=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        local_config_2=None,
        remote_config_2=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        file_matches_version = [1],
        file_uploaded_to_version_id = 2, # upload to version id of 2
        expected_version_id=-1,  # Should create new version
    ),
    16: DataTestCaseSingleDual(
        case_id=16,
        local_config_1=None,
        remote_config_1=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        local_config_2=None,
        remote_config_2=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        file_matches_version = [2],
        file_uploaded_to_version_id = -1, # -1 means it will have a version in the future.
        expected_version_id=2,  # Should match existing version 2
    ),
    17: DataTestCaseSingleDual(
        case_id=17,
        local_config_1=None,
        remote_config_1=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        local_config_2=None,
        remote_config_2=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        file_matches_version = [2],
        file_uploaded_to_version_id = 1, # upload to version id of 1
        expected_version_id=2,  # Should match existing version 2
    ),
    18: DataTestCaseSingleDual(
        case_id=18,
        local_config_1=None,
        remote_config_1=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        local_config_2=None,
        remote_config_2=RemoteFileConfig(present=True, immutable=True, upload_complete=True),
        file_matches_version = [2],
        file_uploaded_to_version_id = 2, # upload to version id of 2
        expected_version_id=2,  # Should match existing version 2
    )
}

@pytest.mark.parametrize("is_hdf5", [False, True])
@pytest.mark.parametrize("case_id", list(CASE_1_TESTS_DUAL.keys()))
def test_file_upload_case_2_dual(
    case_id,
    session_etiket_client,
    get_scope_uuid,
    is_hdf5,
    db_session
):
    """Test file upload scenarios for Case 2 - dual version scenarios"""
    test_case = CASE_1_TESTS_DUAL[case_id]
    
    # Setup
    dataset_uuid = uuid.uuid4()
    scope_uuid = get_scope_uuid
    file_name = f"test_file_case2_{case_id}_{'hdf5' if is_hdf5 else 'json'}"
    file_uuid = uuid.uuid4()
    file_type = FileType.JSON if not is_hdf5 else FileType.HDF5_NETCDF
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create dataset first (simplified)
        from etiket_client.local.models.dataset import DatasetCreate as DatasetCreateLocal
        from etiket_client.local.dao.dataset import dao_dataset
        from etiket_client.remote.endpoints.models.dataset import DatasetCreate as DatasetCreateRemote
        from etiket_client.remote.endpoints.dataset import dataset_create
        
        # Create version 1 files
        if is_hdf5:
            file_path_v1 = os.path.join(temp_dir, f"{file_name}_v1.hdf5")
            dataset_v1 = xr.Dataset({
                'var1': (('x', 'y'), np.random.rand(10, 10)),
                'var2': (('x', 'y'), np.random.rand(10, 10))
            })
            dataset_v1.to_netcdf(file_path_v1, engine='h5netcdf', invalid_netcdf=True)
        else:
            file_path_v1 = os.path.join(temp_dir, f"{file_name}_v1.json")
            file_content_v1 = {"version": "v1", "test": "v1_data", "timestamp": str(datetime.datetime.now())}
            with open(file_path_v1, 'w') as f:
                json.dump(file_content_v1, f)
        
        time.sleep(0.001)
        # Create version 2 files
        if is_hdf5:
            file_path_v2 = os.path.join(temp_dir, f"{file_name}_v2.hdf5")
            dataset_v2 = xr.Dataset({
                'var1': (('x', 'y'), np.random.rand(12, 12)),
                'var2': (('x', 'y'), np.random.rand(12, 12))
            })
            dataset_v2.to_netcdf(file_path_v2, engine='h5netcdf', invalid_netcdf=True)
        else:
            file_path_v2 = os.path.join(temp_dir, f"{file_name}_v2.json")
            file_content_v2 = {"version": "v2", "test": "v2_data", "timestamp": str(datetime.datetime.now())}
            with open(file_path_v2, 'w') as f:
                json.dump(file_content_v2, f)

        # Create local dataset if any local configs are present
        if test_case.local_config_1 is not None or test_case.local_config_2 is not None:
            with session_etiket_client as session:
                local_dataset_create = DatasetCreateLocal(
                    uuid=dataset_uuid,
                    alt_uid=f"test_alt_uid_case2_{case_id}_{'hdf5' if is_hdf5 else 'json'}",
                    name=f"Test Dataset Case 2 {case_id} {'hdf5' if is_hdf5 else 'json'}",
                    description="Test dataset",
                    keywords=["test"],
                    attributes={},
                    ranking=0,
                    creator="test_user",
                    scope_uuid=scope_uuid,
                    collected=datetime.datetime.now()
                )
                dao_dataset.create(local_dataset_create, session=session)
                
                # Create local version 1
                if test_case.local_config_1 is not None:
                    test_case.local_config_1.create_file(dataset_uuid, file_name, file_uuid, session, file_path_v1)
                
                # Create local version 2
                if test_case.local_config_2 is not None:
                    test_case.local_config_2.create_file(dataset_uuid, file_name, file_uuid, session, file_path_v2)
        
        # Create remote dataset
        remote_dataset_create = DatasetCreateRemote(
            uuid=dataset_uuid,
            alt_uid=f"test_alt_uid_case2_{case_id}_{'hdf5' if is_hdf5 else 'json'}",
            name=f"Test Dataset Case 2 {case_id} {'hdf5' if is_hdf5 else 'json'}",
            description="Test dataset",
            keywords=["test"],
            attributes={},
            ranking=0,
            creator="test_user",
            scope_uuid=scope_uuid,
            collected=datetime.datetime.now()
        )
        dataset_create(remote_dataset_create)
        
        # Create remote version 1
        if test_case.remote_config_1 is not None:
            test_case.remote_config_1.create_file(dataset_uuid, file_name, file_uuid, file_path_v1)
        
        # Create remote version 2
        if test_case.remote_config_2 is not None:
            test_case.remote_config_2.create_file(dataset_uuid, file_name, file_uuid, file_path_v2)
        
        # Create sync item
        sync_item = SyncItems(
            id=case_id*2 + int(is_hdf5) + 1000,  # Offset to avoid conflicts with single case tests
            sync_source_id=1,
            dataIdentifier=f"test_data_identifier_case2_{case_id}_{'hdf5' if is_hdf5 else 'json'}",
            datasetUUID=dataset_uuid,
            syncPriority=1.0,
            synchronized=False,
            attempts=0,
            sync_record={},
            error=None,
            traceback=None
        )
        
        with db_session as session:
            session.add(sync_item)
            session.commit()
            session.refresh(sync_item)
        
        # Create test file that matches the specified version(s)
        if 1 in test_case.file_matches_version:
            test_file_path = file_path_v1
        elif 2 in test_case.file_matches_version:
            test_file_path = file_path_v2
        else:
            # Create a new file that doesn't match any existing versions
            if is_hdf5:
                test_file_path = os.path.join(temp_dir, f"{file_name}_test.hdf5")
                dataset_test = xr.Dataset({
                    'var1': (('x', 'y'), np.random.rand(15, 15)),
                    'var2': (('x', 'y'), np.random.rand(15, 15))
                })
                dataset_test.to_netcdf(test_file_path, engine='h5netcdf', invalid_netcdf=True)
            else:
                test_file_path = os.path.join(temp_dir, f"{file_name}_test.json")
                test_content = {"version": "test", "test": "test_data", "timestamp": str(datetime.datetime.now())}
                with open(test_file_path, 'w') as f:
                    json.dump(test_content, f)
        
        if test_case.file_uploaded_to_version_id == 1:
            created_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path_v1))
        elif test_case.file_uploaded_to_version_id == 2:
            created_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path_v2))
        else:
            created_time = datetime.datetime.now()
        
        
        # Create file_info for upload
        f_info = file_info(
            name=file_name,
            fileName=f"{file_name}.json",
            created=created_time,
            fileType=file_type,
            creator="test_user",
            file_generator="test",
            immutable_on_completion=True
        )
        
        # Create dataset manifest
        sync_record = SyncRecordManager(sync_item=sync_item, dataset_path=None)
        
        # Record initial state for comparison
        initial_remote_files = []
        try:
            initial_remote_files = file_read_by_name(dataset_uuid, file_name)
        except Exception:
            pass
        
        initial_local_files = []
        try:
            with session_etiket_client as session:
                initial_local_files = dao_file.get_file_by_name(dataset_uuid, file_name, session)
        except Exception:
            pass
        print(dataset_uuid, file_name)
        
        # Act - Call the upload_file method
        sync_utilities.upload_file(test_file_path, sync_item, f_info, sync_record)
        
        # Verify results
        final_remote_files = []
        try:
            final_remote_files = file_read_by_name(dataset_uuid, file_name)
        except Exception:
            pass
        
        final_local_files = []
        try:
            with session_etiket_client as session:
                final_local_files = dao_file.get_file_by_name(dataset_uuid, file_name, session)
        except Exception:
            pass
        
        upload_file_checksum = None
        if not is_hdf5:
            upload_file_checksum = str(md5(test_file_path).hexdigest())
        else:
            upload_file_checksum = str(md5_netcdf4(test_file_path).hexdigest())
        
        # Get version IDs for comparison
        version_id_v1 = generate_version_id(datetime.datetime.fromtimestamp(os.path.getmtime(file_path_v1)))
        version_id_v2 = generate_version_id(datetime.datetime.fromtimestamp(os.path.getmtime(file_path_v2)))
        
        if test_case.expected_version_id == -1:
            # Should have created a new version
            if initial_remote_files:
                assert len(final_remote_files) > len(initial_remote_files), f"Case 2-{case_id}: Expected new version to be created remotely"
            else:
                assert len(final_remote_files) >= 1, f"Case 2-{case_id}: Expected at least one file to be created"
            
            # The latest remote file should have the checksum of the uploaded file and be complete
            if final_remote_files:
                latest_remote = max(final_remote_files, key=lambda f: f.version_id)
                assert latest_remote.status == FileStatusRem.secured, f"Case 2-{case_id}: Remote file should be complete"
                assert latest_remote.md5_checksum == upload_file_checksum, f"Case 2-{case_id}: Remote file checksum should match uploaded file checksum"
                
                # Ensure the new version is not version 1 or 2
                assert latest_remote.version_id != version_id_v1, f"Case 2-{case_id}: New version should not be version 1"
                assert latest_remote.version_id != version_id_v2, f"Case 2-{case_id}: New version should not be version 2"
        
        elif test_case.expected_version_id == 1:
            # Should use/create version 1
            version_1_files = [f for f in final_remote_files if f.version_id == version_id_v1]
            assert len(version_1_files) == 1, f"Case 2-{case_id}: Expected exactly one file with version 1"
            assert version_1_files[0].status == FileStatusRem.secured, f"Case 2-{case_id}: Version 1 file should be complete"
            assert version_1_files[0].md5_checksum == upload_file_checksum, f"Case 2-{case_id}: Version 1 file checksum should match uploaded file checksum"
        
        elif test_case.expected_version_id == 2:
            # Should use/create version 2
            version_2_files = [f for f in final_remote_files if f.version_id == version_id_v2]
            assert len(version_2_files) == 1, f"Case 2-{case_id}: Expected exactly one file with version 2"
            assert version_2_files[0].status == FileStatusRem.secured, f"Case 2-{case_id}: Version 2 file should be complete"
            assert version_2_files[0].md5_checksum == upload_file_checksum, f"Case 2-{case_id}: Version 2 file checksum should match uploaded file checksum"

        # Ensure the checksum of the local version matches the checksum of the remote version
        for local_file in final_local_files:
            for remote_file in final_remote_files:
                if local_file.version_id == remote_file.version_id:
                    if not is_hdf5:
                        local_file_md5 = str(md5(local_file.local_path).hexdigest())
                    else:
                        local_file_md5 = str(md5_netcdf4(local_file.local_path).hexdigest())
                    remote_file_md5 = None
                    if remote_file.md5_checksum is not None:
                        remote_file_md5 = remote_file.md5_checksum
                        print(f"Case 2-{case_id}: Local MD5: {local_file_md5}, Remote MD5: {remote_file_md5}")
                        print(f"Case 2-{case_id}: Local version: {local_file.version_id}, Remote version: {remote_file.version_id}")
                        print(f"Case 2-{case_id}: Local name: {local_file.name}, Remote name: {remote_file.name}")
                        assert local_file_md5 == remote_file_md5, f"Case 2-{case_id}: Local file checksum should match remote file checksum"
