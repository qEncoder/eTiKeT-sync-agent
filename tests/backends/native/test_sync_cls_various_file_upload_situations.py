"""
Here the idea is to test if the NativeSync class handles the upload of files correctly in various situations.

- scenario 1:
    - local dataset is created (status complete, synchronized=False, type=text)
    - remote dataset is created on the server, no presigned link requested
    -> expected action (syncDatasetNormal): upload the file to the server, status should be secured

- scenario 2:
    - local dataset is created (status complete, synchronized=False, type=text)
    - remote dataset is created on the server, presigned link requested (and remote file record pre-created)
    -> expected action (syncDatasetNormal): upload the file to the server, status should be secured

- scenario 3:
    - local dataset is created (status complete, synchronized=False, type=HDF5_CACHE)
    - no file on the server
    -> expected action (syncDatasetNormal): file is not uploaded or created on the server

- scenario 4 (this should not happen):
    - local dataset is created (status complete, synchronized=True, type=HDF5)
    - file is not present on the server
    -> expected action (syncDatasetNormal): file is not uploaded or created on the server

- scenario 5:
    - local dataset is created (status writing, synchronized=False, type=HDF5)
    - no file on server
    -> expected action (syncDatasetNormal): file is not uploaded or created on the server
"""

import os
import tempfile
import uuid
from datetime import datetime

import pytest
from sqlalchemy.orm import Session

from etiket_client.local.dao.dataset import dao_dataset
from etiket_client.local.dao.file import dao_file
from etiket_client.local.models.dataset import DatasetCreate
from etiket_client.local.models.file import FileCreate

from etiket_client.remote.endpoints.dataset import dataset_create, dataset_read
from etiket_client.remote.endpoints.file import (
    file_create as file_create_remote,
    file_generate_presigned_upload_link_single,
)
from etiket_client.remote.endpoints.models.dataset import (
    DatasetCreate as DatasetCreateRemote,
)
from etiket_client.remote.endpoints.models.file import (
    FileCreate as FileCreateRemote,
    FileRead,
    FileStatusRem,
    FileType,
)
from etiket_client.remote.endpoints.models.types import FileStatusLocal

from etiket_sync_agent.backends.native.native_sync_class import NativeSync
from etiket_sync_agent.backends.native.native_sync_config_class import NativeConfigData
from etiket_sync_agent.sync.sync_records.manager import SyncRecordManager
from etiket_sync_agent.models.sync_items import SyncItems


def _create_file(temp_dir: str, filename: str) -> tuple[int, str]:
    path = os.path.join(temp_dir, filename)
    with open(path, "w") as f:
        f.write("test")
    return os.path.getsize(path), path


def _find_file(files: list[FileRead], file_uuid: uuid.UUID, version_id: int) -> FileRead:
    for f in files:
        if f.uuid == file_uuid and f.version_id == version_id:
            return f
    raise ValueError(f"File {file_uuid} with version {version_id} not found")

def test_s1_local_complete_text_remote_dataset_only_uploads_and_secures(
    session_etiket_client: Session, get_scope_uuid: uuid.UUID):
    scope_uuid = get_scope_uuid
    min_last_sync_item = SyncItems(
        datasetUUID=uuid.uuid4(),
        dataIdentifier="initial",
        syncPriority=datetime.now().timestamp(),
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        config = NativeConfigData()

        # Create local dataset and file (complete, unsynchronized, TEXT)
        ds_uuid = uuid.uuid4()
        collected_time = datetime.now()
        ds_local = DatasetCreate(
            uuid=ds_uuid,
            scope_uuid=scope_uuid,
            collected=collected_time,
            name="s1_local",
            creator="",
            ranking=0,
            keywords=[],
            synchronized=False,
        )
        dao_dataset.create(ds_local, session_etiket_client)

        size, path = _create_file(temp_dir, "file_s1.txt")
        file_uuid = uuid.uuid4()
        f_local = FileCreate(
            name="file_s1",
            filename="file_s1.txt",
            uuid=file_uuid,
            version_id=1,
            creator="",
            collected=datetime.now(),
            size=size,
            local_path=path,
            type=FileType.TEXT,
            file_generator="test",
            status=FileStatusLocal.complete,
            ds_uuid=ds_uuid,
            synchronized=False,
        )
        dao_file.create(f_local, session_etiket_client)

        # Create remote dataset (no file records yet)
        ds_remote_create = DatasetCreateRemote(
            uuid=ds_uuid,
            scope_uuid=scope_uuid,
            collected=collected_time,
            name="s1_local",
            creator="",
            ranking=0,
            keywords=[],
        )
        dataset_create(ds_remote_create)

        # Detect and sync
        sync_items = NativeSync.getNewDatasets(config, min_last_sync_item)
        assert len(sync_items) == 1
        sr = SyncRecordManager(sync_items[0])
        NativeSync.syncDatasetNormal(config, sync_items[0], sr)

        # Validate
        ds_local_after = dao_dataset.read(ds_uuid, session_etiket_client)
        ds_remote_after = dataset_read(ds_uuid)
        rf = _find_file(ds_remote_after.files, file_uuid, 1)
        assert rf.status == FileStatusRem.secured
        # local file should be marked synchronized
        assert ds_local_after.files is not None
        lf = next(f for f in ds_local_after.files if f.uuid == file_uuid and f.version_id == 1)
        assert lf.synchronized is True


def test_s2_local_complete_text_remote_file_record_exists_uploads_and_secures(
    session_etiket_client: Session, get_scope_uuid: uuid.UUID
):
    scope_uuid = get_scope_uuid
    min_last_sync_item = SyncItems(
        datasetUUID=uuid.uuid4(),
        dataIdentifier="initial",
        syncPriority=datetime.now().timestamp(),
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        config = NativeConfigData()

        # Create local dataset and file (complete, unsynchronized, TEXT)
        ds_uuid = uuid.uuid4()
        collected_time = datetime.now()
        ds_local = DatasetCreate(
            uuid=ds_uuid,
            scope_uuid=scope_uuid,
            collected=collected_time,
            name="s2_local",
            creator="",
            ranking=0,
            keywords=[],
            synchronized=False,
        )
        dao_dataset.create(ds_local, session_etiket_client)

        size, path = _create_file(temp_dir, "file_s2.txt")
        file_uuid = uuid.uuid4()
        f_local = FileCreate(
            name="file_s2",
            filename="file_s2.txt",
            uuid=file_uuid,
            version_id=1,
            creator="",
            collected=datetime.now(),
            size=size,
            local_path=path,
            type=FileType.TEXT,
            file_generator="test",
            status=FileStatusLocal.complete,
            ds_uuid=ds_uuid,
            synchronized=False,
        )
        dao_file.create(f_local, session_etiket_client)

        # Create remote dataset and pre-create remote file record (simulating presigned link requested earlier)
        ds_remote_create = DatasetCreateRemote(
            uuid=ds_uuid,
            scope_uuid=scope_uuid,
            collected=collected_time,
            name="s2_local",
            creator="",
            ranking=0,
            keywords=[],
        )
        dataset_create(ds_remote_create)

        fr_remote = FileCreateRemote(
            name="file_s2",
            filename="file_s2.txt",
            uuid=file_uuid,
            creator="",
            collected=datetime.now(),
            size=size,
            type=FileType.TEXT,
            file_generator="test",
            version_id=1,
            ds_uuid=ds_uuid,
            immutable=True,
        )
        file_create_remote(fr_remote)
        _ = file_generate_presigned_upload_link_single(file_uuid, 1)

        # Detect and sync
        sync_items = NativeSync.getNewDatasets(config, min_last_sync_item)
        assert len(sync_items) == 1
        sr = SyncRecordManager(sync_items[0])
        NativeSync.syncDatasetNormal(config, sync_items[0], sr)

        # Validate
        ds_remote_after = dataset_read(ds_uuid)
        rf = _find_file(ds_remote_after.files, file_uuid, 1)
        assert rf.status == FileStatusRem.secured


def test_s3_local_hdf5_cache_not_uploaded(
    session_etiket_client: Session, get_scope_uuid: uuid.UUID
):
    scope_uuid = get_scope_uuid
    min_last_sync_item = SyncItems(
        datasetUUID=uuid.uuid4(),
        dataIdentifier="initial",
        syncPriority=datetime.now().timestamp(),
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        config = NativeConfigData()

        ds_uuid = uuid.uuid4()
        collected_time = datetime.now()
        ds_local = DatasetCreate(
            uuid=ds_uuid,
            scope_uuid=scope_uuid,
            collected=collected_time,
            name="s3_local",
            creator="",
            ranking=0,
            keywords=[],
            synchronized=False,
        )
        dao_dataset.create(ds_local, session_etiket_client)

        size, path = _create_file(temp_dir, "file_s3.h5")
        file_uuid = uuid.uuid4()
        f_local = FileCreate(
            name="file_s3",
            filename="file_s3.h5",
            uuid=file_uuid,
            version_id=1,
            creator="",
            collected=datetime.now(),
            size=size,
            local_path=path,
            type=FileType.HDF5_CACHE,
            file_generator="test",
            status=FileStatusLocal.complete,
            ds_uuid=ds_uuid,
            synchronized=False,
        )
        dao_file.create(f_local, session_etiket_client)

        # Detect and sync (should skip cache file)
        sync_items = NativeSync.getNewDatasets(config, min_last_sync_item)
        assert len(sync_items) == 1
        sr = SyncRecordManager(sync_items[0])
        NativeSync.syncDatasetNormal(config, sync_items[0], sr)

        # Validate remote dataset exists but no such file present
        ds_remote_after = dataset_read(ds_uuid)
        assert all(not (f.uuid == file_uuid and f.version_id == 1) for f in ds_remote_after.files)


def test_s4_local_complete_synced_true_hdf5_not_uploaded(
    session_etiket_client: Session, get_scope_uuid: uuid.UUID
):
    scope_uuid = get_scope_uuid
    min_last_sync_item = SyncItems(
        datasetUUID=uuid.uuid4(),
        dataIdentifier="initial",
        syncPriority=datetime.now().timestamp(),
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        config = NativeConfigData()

        ds_uuid = uuid.uuid4()
        collected_time = datetime.now()
        ds_local = DatasetCreate(
            uuid=ds_uuid,
            scope_uuid=scope_uuid,
            collected=collected_time,
            name="s4_local",
            creator="",
            ranking=0,
            keywords=[],
            synchronized=False,
        )
        dao_dataset.create(ds_local, session_etiket_client)

        size, path = _create_file(temp_dir, "file_s4.h5")
        file_uuid = uuid.uuid4()
        f_local = FileCreate(
            name="file_s4",
            filename="file_s4.h5",
            uuid=file_uuid,
            version_id=1,
            creator="",
            collected=datetime.now(),
            size=size,
            local_path=path,
            type=FileType.HDF5,
            file_generator="test",
            status=FileStatusLocal.complete,
            ds_uuid=ds_uuid,
            synchronized=True,  # already marked synced locally (should be skipped)
        )
        dao_file.create(f_local, session_etiket_client)

        # Detect and sync (should skip already-synchronized file)
        sync_items = NativeSync.getNewDatasets(config, min_last_sync_item)
        assert len(sync_items) == 1
        sr = SyncRecordManager(sync_items[0])
        NativeSync.syncDatasetNormal(config, sync_items[0], sr)

        # Validate remote dataset exists but no such file present
        ds_remote_after = dataset_read(ds_uuid)
        assert all(not (f.uuid == file_uuid and f.version_id == 1) for f in ds_remote_after.files)


@pytest.mark.usefixtures()
def test_s5_local_writing_hdf5_not_uploaded(
    session_etiket_client: Session, get_scope_uuid: uuid.UUID
):
    scope_uuid = get_scope_uuid
    min_last_sync_item = SyncItems(
        datasetUUID=uuid.uuid4(),
        dataIdentifier="initial",
        syncPriority=datetime.now().timestamp(),
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        config = NativeConfigData()

        ds_uuid = uuid.uuid4()
        collected_time = datetime.now()
        ds_local = DatasetCreate(
            uuid=ds_uuid,
            scope_uuid=scope_uuid,
            collected=collected_time,
            name="s5_local",
            creator="",
            ranking=0,
            keywords=[],
            synchronized=False,
        )
        dao_dataset.create(ds_local, session_etiket_client)

        size, path = _create_file(temp_dir, "file_s5.h5")
        file_uuid = uuid.uuid4()
        f_local = FileCreate(
            name="file_s5",
            filename="file_s5.h5",
            uuid=file_uuid,
            version_id=1,
            creator="",
            collected=datetime.now(),
            size=size,
            local_path=path,
            type=FileType.HDF5,
            file_generator="test",
            status=FileStatusLocal.writing,  # not complete
            ds_uuid=ds_uuid,
            synchronized=False,
        )
        dao_file.create(f_local, session_etiket_client)

        # Detect and sync (should skip writing status)
        sync_items = NativeSync.getNewDatasets(config, min_last_sync_item)
        assert len(sync_items) == 1
        sr = SyncRecordManager(sync_items[0])
        NativeSync.syncDatasetNormal(config, sync_items[0], sr)

        # Validate remote dataset exists but no such file present
        ds_remote_after = dataset_read(ds_uuid)
        assert all(not (f.uuid == file_uuid and f.version_id == 1) for f in ds_remote_after.files)
