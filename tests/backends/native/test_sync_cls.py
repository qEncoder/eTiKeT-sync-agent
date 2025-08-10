import uuid, tempfile, os, pytest

from datetime import datetime
from typing import List, Tuple

from sqlalchemy.orm import Session

from etiket_client.local.dao.dataset import dao_dataset
from etiket_client.local.models.dataset import DatasetCreate, DatasetUpdate, DatasetRead
from etiket_client.local.dao.file import dao_file
from etiket_client.local.models.file import FileCreate

from etiket_client.remote.endpoints.dataset import dataset_read, dataset_create
from etiket_client.remote.endpoints.models.dataset import DatasetRead as RemoteDatasetRead
from etiket_client.remote.endpoints.file import file_create, file_generate_presigned_upload_link_single
from etiket_client.remote.endpoints.models.file import FileRead, FileStatusRem, FileType, FileCreate as FileCreateRemote

from etiket_client.remote.endpoints.models.types import FileStatusRem, FileStatusLocal
from etiket_client.remote.endpoints.models.dataset import DatasetCreate as DatasetCreateRemote
from etiket_sync_agent.sync.sync_utilities import md5
from etiket_sync_agent.sync.uploader.file_uploader import upload_new_file_single
from etiket_sync_agent.backends.native.native_sync_class import NativeSync
from etiket_sync_agent.backends.native.native_sync_config_class import NativeConfigData

from etiket_sync_agent.sync.sync_records.manager import SyncRecordManager

MIN_LAST_IDENTIFIER = datetime.now().timestamp()

@pytest.fixture()
def min_last_identifier():
    return MIN_LAST_IDENTIFIER


def test_getNewDatasets(session_etiket_client: Session, get_scope_uuid: uuid.UUID, min_last_identifier: float):
    with tempfile.TemporaryDirectory() as temp_dir:
        configData = NativeConfigData()
        
        # check if it returns nothing
        sync_items = NativeSync.getNewDatasets(configData, str(min_last_identifier))
        assert len(sync_items) == 0

        # Create dataset
        scope_uuid = get_scope_uuid
        ds = DatasetCreate(
            uuid=uuid.uuid4(),
            scope_uuid=scope_uuid,
            collected=datetime.now(),
            name="ds_priority_test",
            creator="",
            ranking=0,
            keywords=[],
            synchronized=False,
        )
        dao_dataset.create(ds, session_etiket_client)

        # 1) Detect new dataset
        sync_items = NativeSync.getNewDatasets(configData, str(min_last_identifier))
        assert len(sync_items) == 1
        assert sync_items[0].datasetUUID == ds.uuid
        last_identifier = sync_items[0].syncPriority

        # 2) Add a file -> should update priority
        size1, path1 = create_file(temp_dir, "file1.txt")
        f1 = FileCreate(
            name="file1",
            filename="file1.txt",
            uuid=uuid.uuid4(),
            version_id=1,
            creator="",
            collected=datetime.now(),
            size=size1,
            local_path=path1,
            type=FileType.TEXT,
            file_generator="test",
            status=FileStatusLocal.complete,
            ds_uuid=ds.uuid,
            synchronized=False,
        )
        dao_file.create(f1, session_etiket_client)

        sync_items = NativeSync.getNewDatasets(configData, str(last_identifier))
        assert len(sync_items) == 1
        assert sync_items[0].datasetUUID == ds.uuid
        assert sync_items[0].syncPriority > last_identifier
        last_identifier = sync_items[0].syncPriority

        # 3) Update dataset attribute (e.g., name) -> should update priority again
        du = DatasetUpdate(name="ds_priority_test_updated")
        dao_dataset.update(ds.uuid, du, session_etiket_client)

        sync_items = NativeSync.getNewDatasets(configData, str(last_identifier))
        assert len(sync_items) == 1
        assert sync_items[0].datasetUUID == ds.uuid
        assert sync_items[0].syncPriority > last_identifier
        last_identifier = sync_items[0].syncPriority

        # 4) Add another file -> should update priority again
        size2, path2 = create_file(temp_dir, "file2.txt")
        f2 = FileCreate(
            name="file2",
            filename="file2.txt",
            uuid=uuid.uuid4(),
            version_id=1,
            creator="",
            collected=datetime.now(),
            size=size2,
            local_path=path2,
            type=FileType.TEXT,
            file_generator="test",
            status=FileStatusLocal.complete,
            ds_uuid=ds.uuid,
            synchronized=False,
        )
        dao_file.create(f2, session_etiket_client)

        sync_items = NativeSync.getNewDatasets(configData, str(last_identifier))
        assert len(sync_items) == 1
        assert sync_items[0].datasetUUID == ds.uuid
        assert sync_items[0].syncPriority > last_identifier
        
        global MIN_LAST_IDENTIFIER
        MIN_LAST_IDENTIFIER = sync_items[0].syncPriority

def test_CreateDatasetWithSingleFile_SyncsSuccessfully(
    session_etiket_client: Session, get_scope_uuid: uuid.UUID, 
    min_last_identifier: float
):
    scope_uuid = get_scope_uuid
    with tempfile.TemporaryDirectory() as temp_dir:
        configData = NativeConfigData()

        # Create dataset
        ds = DatasetCreate(
            uuid=uuid.uuid4(),
            scope_uuid=scope_uuid,
            collected=datetime.now(),
            name="simple_ds",
            creator="",
            ranking=0,
            keywords=[],
            synchronized=False,
        )
        dao_dataset.create(ds, session_etiket_client)

        # Add single file
        size1, path1 = create_file(temp_dir, "simple.txt")
        f_uuid = uuid.uuid4()
        f1 = FileCreate(
            name="simple",
            filename="simple.txt",
            uuid=f_uuid,
            version_id=1,
            creator="",
            collected=datetime.now(),
            size=size1,
            local_path=path1,
            type=FileType.TEXT,
            file_generator="test",
            status=FileStatusLocal.complete,
            ds_uuid=ds.uuid,
            synchronized=False,
        )
        dao_file.create(f1, session_etiket_client)

        # Detect and sync
        sync_items = NativeSync.getNewDatasets(configData, str(min_last_identifier))
        assert len(sync_items) == 1
        assert sync_items[0].datasetUUID == ds.uuid
        sr = SyncRecordManager(sync_items[0])
        NativeSync.syncDatasetNormal(configData, sync_items[0], sr)

        # Validate
        ds_local = dao_dataset.read(ds.uuid, session_etiket_client)
        ds_remote = dataset_read(ds.uuid)
        check_dataset_in_sync(ds_local, ds_remote)
        
        #  create a new local dataset
        ds = DatasetCreate(uuid = uuid.uuid4(),
                            scope_uuid=scope_uuid,
                            collected=datetime.now(),
                            name='test',
                            creator = '',
                            ranking=0, keywords=[],
                            synchronized=False)
        dao_dataset.create(ds, session_etiket_client)
        
        filesize, path = create_file(temp_dir, 'test.txt')
        fc = FileCreate(name='test',
                        filename='test.txt',
                        uuid = uuid.uuid4(),
                        version_id=1,
                        creator = '',
                        collected=datetime.now(),
                        size=filesize,
                        local_path=path,
                        type=FileType.TEXT,
                        file_generator='test',
                        status=FileStatusLocal.complete,
                        ds_uuid=ds.uuid,
                        synchronized=False)
        dao_file.create(fc, session_etiket_client)
        
        # use NativeSync.getNewDatasets()
        sync_items = NativeSync.getNewDatasets(configData, str(min_last_identifier))
        assert len(sync_items) == 1
        assert sync_items[0].datasetUUID == ds.uuid
        last_identifier = sync_items[0].syncPriority
        
        # check if it finds the dataset ( look at the sync items and see that it has the uuid of the dataset)
        ds_local = dao_dataset.read(ds.uuid, session_etiket_client)
        assert ds_local.synchronized == False
        
        # run NativeSync.syncDatasetNormal()
        sync_record = SyncRecordManager(sync_items[0])
        NativeSync.syncDatasetNormal(configData, sync_items[0], sync_record)
        
        #  read the local dataset, sync status should be True
        ds_local = dao_dataset.read(ds.uuid, session_etiket_client)
        assert ds_local.synchronized == True
        
        #  read the remote dataset and check if is is in sync
        ds_remote = dataset_read(ds.uuid)
        check_dataset_in_sync(ds_local, ds_remote)
        
        # check for new sync items again, there should be nothing
        sync_items = NativeSync.getNewDatasets(configData, str(last_identifier))
        assert len(sync_items) == 0

def test_DatasetAttributeModification(session_etiket_client: Session, get_scope_uuid: uuid.UUID, min_last_identifier: float):
    scope_uuid = get_scope_uuid
    with tempfile.TemporaryDirectory() as temp_dir:
        configData = NativeConfigData()

        # Create initial dataset and one file
        ds = DatasetCreate(
            uuid=uuid.uuid4(),
            scope_uuid=scope_uuid,
            collected=datetime.now(),
            name="test_ds_attr",
            creator="",
            ranking=0,
            keywords=[],
            synchronized=False,
        )
        dao_dataset.create(ds, session_etiket_client)

        size1, path1 = create_file(temp_dir, "file1.txt")
        f1_uuid = uuid.uuid4()
        f1 = FileCreate(
            name="file1",
            filename="file1.txt",
            uuid=f1_uuid,
            version_id=1,
            creator="",
            collected=datetime.now(),
            size=size1,
            local_path=path1,
            type=FileType.TEXT,
            file_generator="test",
            status=FileStatusLocal.complete,
            ds_uuid=ds.uuid,
            synchronized=False,
        )
        dao_file.create(f1, session_etiket_client)

        # Initial detection and sync
        sync_items = NativeSync.getNewDatasets(configData, str(min_last_identifier))
        assert len(sync_items) == 1
        last_identifier = sync_items[0].syncPriority
        sr = SyncRecordManager(sync_items[0])
        NativeSync.syncDatasetNormal(configData, sync_items[0], sr)
        
        sync_items = NativeSync.getNewDatasets(configData, str(last_identifier))
        assert len(sync_items) == 0
        
        # Modify dataset attribute(s)
        du = DatasetUpdate(attributes={"test_attr": "test_value"}, name="test_ds_attr_updated")
        dao_dataset.update(ds.uuid, du, session_etiket_client)
    
        # Should be detected as new due to modified timestamp
        sync_items = NativeSync.getNewDatasets(configData, str(last_identifier))
        assert len(sync_items) == 1
        last_identifier = sync_items[0].syncPriority
        
        # Sync again
        sr2 = SyncRecordManager(sync_items[0])
        NativeSync.syncDatasetNormal(configData, sync_items[0], sr2)

        # Validate remote matches local
        ds_local = dao_dataset.read(ds.uuid, session_etiket_client)
        ds_remote = dataset_read(ds.uuid)
        
        check_dataset_in_sync(ds_local, ds_remote)
        
        sync_items = NativeSync.getNewDatasets(configData, str(last_identifier))
        assert len(sync_items) == 0

def test_DatasetFileAddition(session_etiket_client: Session, get_scope_uuid: uuid.UUID, min_last_identifier: float):
    scope_uuid = get_scope_uuid
    with tempfile.TemporaryDirectory() as temp_dir:
        configData = NativeConfigData()

        # Create initial dataset and one file
        ds = DatasetCreate(
            uuid=uuid.uuid4(),
            scope_uuid=scope_uuid,
            collected=datetime.now(),
            name="test_ds_file_add",
            creator="",
            ranking=0,
            keywords=[],
            synchronized=False,
        )
        dao_dataset.create(ds, session_etiket_client)

        size1, path1 = create_file(temp_dir, "file1.txt")
        f1_uuid = uuid.uuid4()
        f1 = FileCreate(
            name="file1",
            filename="file1.txt",
            uuid=f1_uuid,
            version_id=1,
            creator="",
            collected=datetime.now(),
            size=size1,
            local_path=path1,
            type=FileType.TEXT,
            file_generator="test",
            status=FileStatusLocal.complete,
            ds_uuid=ds.uuid,
            synchronized=False,
        )
        dao_file.create(f1, session_etiket_client)

        # Initial detection and sync
        sync_items = NativeSync.getNewDatasets(configData, str(min_last_identifier))
        assert len(sync_items) == 1
        last_identifier = sync_items[0].syncPriority
        sr = SyncRecordManager(sync_items[0])
        NativeSync.syncDatasetNormal(configData, sync_items[0], sr)

        sync_items = NativeSync.getNewDatasets(configData, str(last_identifier))
        assert len(sync_items) == 0

        # Add a new file to the dataset
        size2, path2 = create_file(temp_dir, "file2.txt")
        f2_uuid = uuid.uuid4()
        f2 = FileCreate(
            name="file2",
            filename="file2.txt",
            uuid=f2_uuid,
            version_id=1,
            creator="",
            collected=datetime.now(),
            size=size2,
            local_path=path2,
            type=FileType.TEXT,
            file_generator="test",
            status=FileStatusLocal.complete,
            ds_uuid=ds.uuid,
            synchronized=False,
        )
        dao_file.create(f2, session_etiket_client)

        # Should be detected as new due to dataset modification
        sync_items = NativeSync.getNewDatasets(configData, str(last_identifier))
        assert len(sync_items) == 1
        last_identifier = sync_items[0].syncPriority

        # Sync again
        sr2 = SyncRecordManager(sync_items[0])
        NativeSync.syncDatasetNormal(configData, sync_items[0], sr2)

        # Validate both files are present remotely and synchronized locally
        ds_local = dao_dataset.read(ds.uuid, session_etiket_client)
        ds_remote = dataset_read(ds.uuid)
        check_dataset_in_sync(ds_local, ds_remote)
        # Ensure specific files exist remotely
        _ = find_file(ds_remote.files, f1_uuid, 1)
        _ = find_file(ds_remote.files, f2_uuid, 1)

        sync_items = NativeSync.getNewDatasets(configData, str(last_identifier))
        assert len(sync_items) == 0

def test_DatasetFileModification(session_etiket_client: Session, get_scope_uuid: uuid.UUID, min_last_identifier: float):
    scope_uuid = get_scope_uuid
    with tempfile.TemporaryDirectory() as temp_dir:
        configData = NativeConfigData()

        # Create initial dataset and one file
        ds = DatasetCreate(
            uuid=uuid.uuid4(),
            scope_uuid=scope_uuid,
            collected=datetime.now(),
            name="test_ds_file_mod",
            creator="",
            ranking=0,
            keywords=[],
            synchronized=False,
        )
        dao_dataset.create(ds, session_etiket_client)

        size1, path1 = create_file(temp_dir, "file1.txt")
        f_uuid = uuid.uuid4()
        f1 = FileCreate(
            name="file1",
            filename="file1.txt",
            uuid=f_uuid,
            version_id=1,
            creator="",
            collected=datetime.now(),
            size=size1,
            local_path=path1,
            type=FileType.TEXT,
            file_generator="test",
            status=FileStatusLocal.complete,
            ds_uuid=ds.uuid,
            synchronized=False,
        )
        dao_file.create(f1, session_etiket_client)

        # Initial detection and sync
        sync_items = NativeSync.getNewDatasets(configData, str(min_last_identifier))
        assert len(sync_items) == 1
        last_identifier = sync_items[0].syncPriority
        sr = SyncRecordManager(sync_items[0])
        NativeSync.syncDatasetNormal(configData, sync_items[0], sr)

        # Modify file by adding a new version (version_id=2)
        size2, path2 = create_file(temp_dir, "file1_v2.txt")
        f2 = FileCreate(
            name="file1",
            filename="file1_v2.txt",
            uuid=f_uuid,  # same file UUID, new version
            version_id=2,
            creator="",
            collected=datetime.now(),
            size=size2,
            local_path=path2,
            type=FileType.TEXT,
            file_generator="test",
            status=FileStatusLocal.complete,
            ds_uuid=ds.uuid,
            synchronized=False,
        )
        dao_file.create(f2, session_etiket_client)

        # Should be detected as new due to dataset modification
        sync_items = NativeSync.getNewDatasets(configData, str(last_identifier))
        assert len(sync_items) == 1

        # Sync again
        sr2 = SyncRecordManager(sync_items[0])
        NativeSync.syncDatasetNormal(configData, sync_items[0], sr2)
        
        sync_items = NativeSync.getNewDatasets(configData, str(last_identifier))
        assert len(sync_items) == 0

        # Validate remote has both versions and latest secured
        ds_local = dao_dataset.read(ds.uuid, session_etiket_client)
        ds_remote = dataset_read(ds.uuid)
        check_dataset_in_sync(ds_local, ds_remote)
        _ = find_file(ds_remote.files, f_uuid, 1)
        _ = find_file(ds_remote.files, f_uuid, 2)
        
        sync_items = NativeSync.getNewDatasets(configData, str(last_identifier))
        assert len(sync_items) == 0

def test_RemoteDatasetExists_LocalNameChangeAndNewFileVersion_SyncsCorrectly(
    session_etiket_client: Session, get_scope_uuid: uuid.UUID, min_last_identifier: float
):
    scope_uuid = get_scope_uuid
    with tempfile.TemporaryDirectory() as temp_dir:
        configData = NativeConfigData()
        ds_uuid = uuid.uuid4()
        collected_time = datetime.now()
        # Create remote dataset initially
        remote_ds = DatasetCreateRemote(
            uuid=ds_uuid,
            scope_uuid=scope_uuid,
            collected=collected_time,
            name="remote_original",
            creator="",
            ranking=0,
            keywords=[],
        )
        dataset_create(remote_ds)

        # Add file v1 to remote so we can add v2 locally
        size1, path1 = create_file(temp_dir, "fileX_v1.txt")
        file_uuid = uuid.uuid4()
        file_create_remote = FileCreateRemote(
            name="fileX",
            filename="fileX_v1.txt",
            uuid=file_uuid,
            creator="",
            collected=datetime.now(),
            size=size1,
            type=FileType.TEXT,
            file_generator="test",
            version_id=1,
            ds_uuid=ds_uuid,
            immutable=True,
        )
        file_create(file_create_remote)
        upload_info_v1 = file_generate_presigned_upload_link_single(file_uuid, 1)
        upload_new_file_single(path1, upload_info_v1, md5(path1))

        # Create local dataset with different name
        local_ds = DatasetCreate(
            uuid=ds_uuid,
            scope_uuid=scope_uuid,
            collected=collected_time,
            name="local_updated",
            creator="",
            ranking=0,
            keywords=[],
            synchronized=False,
        )
        dao_dataset.create(local_ds, session_etiket_client)
        
        # Locally add version 2
        size2, path2 = create_file(temp_dir, "fileX_v2.txt")
        f2 = FileCreate(
            name="fileX",
            filename="fileX_v2.txt",
            uuid=file_uuid,
            version_id=2,
            creator="",
            collected=datetime.now(),
            size=size2,
            local_path=path2,
            type=FileType.TEXT,
            file_generator="test",
            status=FileStatusLocal.complete,
            ds_uuid=ds_uuid,
            synchronized=False,
        )
        dao_file.create(f2, session_etiket_client)

        # Detect and sync
        sync_items = NativeSync.getNewDatasets(configData, str(min_last_identifier))
        assert len(sync_items) == 1
        sr = SyncRecordManager(sync_items[0])
        NativeSync.syncDatasetNormal(configData, sync_items[0], sr)

        # Assert: remote name should be updated from local, and v2 uploaded
        ds_remote = dataset_read(ds_uuid)
        assert ds_remote.name == "local_updated"
        _ = find_file(ds_remote.files, file_uuid, 1)
        _ = find_file(ds_remote.files, file_uuid, 2)

        ds_local = dao_dataset.read(ds_uuid, session_etiket_client)
        check_dataset_in_sync(ds_local, ds_remote)

# Local created first, then remote created with different name, plus new local version

def test_LocalFirst_ThenRemoteDifferentName_NewLocalVersion_UploadsAndKeepsRemote(
    session_etiket_client: Session, get_scope_uuid: uuid.UUID, min_last_identifier: float
):
    scope_uuid = get_scope_uuid
    with tempfile.TemporaryDirectory() as temp_dir:
        configData = NativeConfigData()
        ds_uuid = uuid.uuid4()
        collected_time = datetime.now()

        # Local dataset first
        local_ds = DatasetCreate(
            uuid=ds_uuid,
            scope_uuid=scope_uuid,
            collected=collected_time,
            name="local_name",
            creator="",
            ranking=0,
            keywords=[],
            synchronized=False,
        )
        dao_dataset.create(local_ds, session_etiket_client)

        # Initial file version 1 locally (to then add v2 later)
        size1, path1 = create_file(temp_dir, "myfile_v1.txt")
        file_uuid = uuid.uuid4()
        f1 = FileCreate(
            name="myfile",
            filename="myfile_v1.txt",
            uuid=file_uuid,
            version_id=1,
            creator="",
            collected=datetime.now(),
            size=size1,
            local_path=path1,
            type=FileType.TEXT,
            file_generator="test",
            status=FileStatusLocal.complete,
            ds_uuid=ds_uuid,
            synchronized=False,
        )
        dao_file.create(f1, session_etiket_client)

        # Add v2 locally
        size2, path2 = create_file(temp_dir, "myfile_v2.txt")
        f2 = FileCreate(
            name="myfile",
            filename="myfile_v2.txt",
            uuid=file_uuid,
            version_id=2,
            creator="",
            collected=datetime.now(),
            size=size2,
            local_path=path2,
            type=FileType.TEXT,
            file_generator="test",
            status=FileStatusLocal.complete,
            ds_uuid=ds_uuid,
            synchronized=False,
        )
        dao_file.create(f2, session_etiket_client)

        # Remote dataset gets created later with different name
        remote_ds = DatasetCreateRemote(
            uuid=ds_uuid,
            scope_uuid=scope_uuid,
            collected=collected_time,
            name="remote_name",
            creator="",
            ranking=0,
            keywords=[],
        )
        dataset_create(remote_ds)
        
        # Detect and sync
        sync_items = NativeSync.getNewDatasets(configData, str(min_last_identifier))
        assert len(sync_items) == 1
        sr = SyncRecordManager(sync_items[0])
        NativeSync.syncDatasetNormal(configData, sync_items[0], sr)

        # After local change, remote metadata should match local and both versions present
        ds_local = dao_dataset.read(ds_uuid, session_etiket_client)
        assert ds_local.name == "remote_name"
        
        ds_remote = dataset_read(ds_uuid)
        _ = find_file(ds_remote.files, file_uuid, 1)
        _ = find_file(ds_remote.files, file_uuid, 2)

        ds_local = dao_dataset.read(ds_uuid, session_etiket_client)
        check_dataset_in_sync(ds_local, ds_remote)

def check_dataset_in_sync(ds_local: DatasetRead, ds_remote: RemoteDatasetRead):
    assert ds_local.uuid == ds_remote.uuid
    assert ds_local.alt_uid == ds_remote.alt_uid
    assert ds_local.collected == ds_remote.collected
    assert ds_local.name == ds_remote.name
    assert ds_local.description == ds_remote.description
    assert ds_local.creator == ds_remote.creator
    assert ds_local.ranking == ds_remote.ranking
    assert ds_local.keywords == ds_remote.keywords
    assert ds_local.attributes == ds_remote.attributes
    
    assert ds_local.synchronized == True
    
    if ds_local.files is not None:
        for file_local in ds_local.files:
            if file_local.status == FileStatusLocal.complete and file_local.type != FileType.HDF5_CACHE:
                file_remote = find_file(ds_remote.files, file_local.uuid, file_local.version_id)
                assert file_remote.status == FileStatusRem.secured
                assert file_local.name == file_remote.name
                assert file_local.filename == file_remote.filename
                assert file_local.size == file_remote.size
                assert file_local.type == file_remote.type
                assert file_local.file_generator == file_remote.file_generator
                assert file_local.collected == file_remote.collected
                assert file_local.ranking == file_remote.ranking
                assert file_local.creator == file_remote.creator
                assert file_local.synchronized == True

def find_file(files: List[FileRead], file_uuid: uuid.UUID, version_id: int) -> FileRead:
    for file in files:
        if file.uuid == file_uuid and file.version_id == version_id:
            return file
    raise ValueError(f"File {file_uuid} with version {version_id} not found")

def create_file(temp_dir: str, filename: str) -> Tuple[int, str]:
    path = os.path.join(temp_dir, filename)
    with open(path, 'w') as f:
        f.write('test')
    return os.path.getsize(path), path