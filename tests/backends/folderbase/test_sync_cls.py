'''
Tests for folderbase synchronization.

- test FolderBaseSync.syncDatasetNormal

# test sync of simple folder
/root/file1.text
/root/dir1/file.hdf5 (xarray converted to hdf5)
/root/_QH_dataset_info.yaml --> from qdrive.dataset import generate_dataset_info
            --> generate example with dataset_name, collected, description, attributes, tags

--> check if the .QH_manifest.yaml when the sync in done
--> use the remote api to read the dataset (in.QH_manifest.yaml, the dataset_uuid can be loaded)
--> compare the name, collected, description, attributes, tags
------> names should be file1.txt, dir1/file.hdf5
------> status should be secured.

# test skip
/root/file1.text
/root/dir0/file2.text
/root/dir1/file.hdf5
/root/dir2/file.hdf5 (xarray converted to hdf5)
/root/_QH_dataset_info.yaml --> from qdrive.dataset import generate_dataset_info
            --> generate example with dataset_name, skip = ["dir1/*", "*.text"]

--> check the files and that only dir2/file.hdf5 is present

# test file converters
make a csv vile with two columns (e.g. with pandas). x can be a linspace, and y random numbers.
--> provide the convertor :: etiket_sync_agent.backends.folderbase.converters.csv_to_hdf5.CSVToHDF5Converter
--> check that both the file files/data.csv and files/data.hdf5 are present in the dataset.
--> run again and check that not a new version is created.
--> modify the csv file and run again --> check that a new version is created.

# test folder converters
make a zarr file with two variables (e.g. with xarray) (e.g. files/data.zarr).
--> provide the convertor :: etiket_sync_agent.backends.folderbase.converters.zarr_to_netcdf4.ZarrToNETCDF4Converter
--> check that data.hdf5 is present in the dataset --> check that the zarr folder is not present.
--> run again and check that not a new version is   created.
--> modify the zarr file and run again --> check that a new version is created.

# TODO 
# test _QH_dataset_info.yaml changes.
# --> create folder structure, with the yaml.
# --> upload the folder.
# --> then overwrite the yaml file with different tags, attributes, description, ds_name 
# --> check that these are updated.

# dataset_uuid resolution test, in same scope.
# create a sync source and create a related sync_identifier.
# create dummy folder with stuff and upload it.
# then remove the sync source.
# create a new sync source, and create a new sync_identifier with a random dataset_uuid.
# perform upload again of the same folder.
# re-read the sync_identifier from db, and check that the dataset_uuid is the same as the one in the sync_identifier.

# dataset_uuid resolution test, in different scope.
# create a sync source and create a related sync_identifier.
# create dummy folder with stuff and upload it.
# then remove the sync source.
# create a new sync source, and create a new sync_identifier with a random dataset_uuid and using a different scope.
# perform upload again on the same folder.
# re-read the sync_identifier from db, and check that the dataset_uuid is not the same as the one in the initial scope. Also check if it is correct in the .QH_manifest.yaml file.

# legacy test, check if created and keywords is recognized (replace in the yaml file the keys collected and tags), e.g. load the yaml, then change them and save again.
--> check if the collected and tags are are in the dataset as expected when reading it.

# test uploading the dataset and then copying the folder.
--> upload the folder first, then move it (e.g. /root/folder1 to /root/folder2)
--> the sync agent should upload the folder again, the uuid of the dataset should change (and also the dataset_sync_path), and this should be reflected in the .QH_manifest.yaml file.

'''

import os, uuid, tempfile, yaml, datetime, shutil, pytest
import xarray
import numpy as np

from pathlib import Path
from typing import List

from etiket_client.remote.endpoints.dataset import dataset_read
from etiket_client.remote.endpoints.models.file import FileRead
from etiket_client.remote.endpoints.models.types import FileStatusRem

from etiket_sync_agent.backends.folderbase.folderbase_sync_class import FolderBaseSync
from etiket_sync_agent.backends.folderbase.folderbase_config_class import FolderBaseConfigData
from etiket_sync_agent.sync.manifests.v2.definitions import QH_MANIFEST_FILE, QH_DATASET_INFO_FILE
from etiket_sync_agent.sync.sync_records.manager import SyncRecordManager
from etiket_sync_agent.crud.sync_items import crud_sync_items
from etiket_sync_agent.crud.sync_sources import crud_sync_sources
from etiket_sync_agent.models.sync_sources import SyncSources
from etiket_sync_agent.models.enums import SyncSourceTypes, SyncSourceStatus
from etiket_sync_agent.models.sync_items import SyncItems


class DummySyncItem:
    def __init__(self, dataset_uuid: uuid.UUID, data_identifier: str, scope_uuid: uuid.UUID):
        self.datasetUUID = dataset_uuid
        self.dataIdentifier = data_identifier
        self.scopeUUID = scope_uuid
        self.syncPriority = datetime.datetime.now().timestamp()
        self.sync_record = None
        self.id = None

    def updateDatasetUUID(self, new_uuid: uuid.UUID):
        self.datasetUUID = new_uuid


def write_dataset_info(dataset_dir: Path,
                        dataset_name: str,
                        collected: datetime.datetime,
                        description: str,
                        attributes: dict | None = None,
                        tags: list[str] | None = None,
                        skip: list[str] | None = None,
                        converters: dict | None = None) -> None:
    info = {
        'dataset_name': dataset_name,
        'collected': collected.strftime('%Y-%m-%dT%H:%M:%S'),
        'description': description,
        'attributes': attributes or {},
        'tags': tags or [],
    }
    if skip is not None:
        info['skip'] = skip
    if converters is not None:
        info['converters'] = converters
    with open(dataset_dir / QH_DATASET_INFO_FILE, 'w', encoding='utf-8') as f:
        yaml.safe_dump(info, f)


def create_file(root: Path, rel_path: str, content: str = 'test') -> Path:
    file_path = root / rel_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    return file_path


def get_remote_files_by_name(files: List[FileRead], name: str) -> List[FileRead]:
    return [f for f in files if f.name == name]


@pytest.mark.parametrize("server_folder", [False])
def test_simple_folder_sync(get_scope_uuid: uuid.UUID, server_folder: bool):
    with tempfile.TemporaryDirectory() as temp_root:
        root = Path(temp_root)
        ds_name = 'simple_ds'
        ds_uuid = uuid.uuid4()
        scope_uuid = get_scope_uuid

        dataset_dir = root / ds_name
        dataset_dir.mkdir(parents=True, exist_ok=True)

        # Files
        create_file(dataset_dir, 'file1.txt', 'hello')
        create_file(dataset_dir, 'dir1/file.hdf5', 'binary-ish')

        # Dataset info
        collected = datetime.datetime.now()
        description = 'A simple dataset'
        attributes = {'a': 1, 'b': 'two'}
        tags = ['t1', 't2']
        write_dataset_info(dataset_dir, ds_name, collected, description, attributes, tags)

        # Run sync
        config = FolderBaseConfigData(root_directory=root, server_folder=server_folder)
        s_item = DummySyncItem(ds_uuid, ds_name, scope_uuid)
        sync_record = SyncRecordManager(s_item)
        FolderBaseSync.syncDatasetNormal(config, s_item, sync_record)

        # Manifest written
        manifest_path = dataset_dir / QH_MANIFEST_FILE
        assert manifest_path.exists()

        # Remote dataset and files
        ds_remote = dataset_read(ds_uuid)
        assert ds_remote.name == ds_name
        assert abs((ds_remote.collected - collected).total_seconds()) <= 1
        assert description in ds_remote.description
        assert set(ds_remote.keywords) == set(tags)
        assert ds_remote.attributes is not None and all(k in ds_remote.attributes for k in attributes.keys())

        names = sorted([f.name for f in ds_remote.files])
        assert 'file1.txt' in names
        assert 'dir1/file.hdf5' in names
        for f in ds_remote.files:
            if f.name in ('file1.txt', 'dir1/file.hdf5'):
                assert f.status == FileStatusRem.secured


@pytest.mark.parametrize("server_folder", [False])
def test_skip_pattern_1(get_scope_uuid: uuid.UUID, server_folder: bool):
    with tempfile.TemporaryDirectory() as temp_root:
        root = Path(temp_root)
        ds_name = 'skip_test'
        ds_uuid = uuid.uuid4()
        scope_uuid = get_scope_uuid

        dataset_dir = root / ds_name
        dataset_dir.mkdir(parents=True, exist_ok=True)

        # Files
        create_file(dataset_dir, 'file1.text')
        create_file(dataset_dir, 'dir0/file2.text')
        create_file(dataset_dir, 'dir1/file.hdf5')
        create_file(dataset_dir, 'dir2/file.hdf5')

        # Skip text files only (directory-based skips are not supported by implementation)
        skip = ['*.text']
        write_dataset_info(dataset_dir, ds_name, datetime.datetime.now(), 'skip case', skip=skip)

        config = FolderBaseConfigData(root_directory=root, server_folder=server_folder)
        s_item = DummySyncItem(ds_uuid, ds_name, scope_uuid)
        sync_record = SyncRecordManager(s_item)
        FolderBaseSync.syncDatasetNormal(config, s_item, sync_record)

        ds_remote = dataset_read(ds_uuid)
        names = sorted([f.name for f in ds_remote.files])
        assert 'file1.text' not in names
        assert 'dir0/file2.text' not in names
        # Both hdf5 present since only filename-based skips are supported
        assert 'dir1/file.hdf5' in names
        assert 'dir2/file.hdf5' in names

@pytest.mark.parametrize("server_folder", [False])
def test_skip_pattern_2(get_scope_uuid: uuid.UUID, server_folder: bool):
    with tempfile.TemporaryDirectory() as temp_root:
        root = Path(temp_root)
        ds_name = 'skip_test'
        ds_uuid = uuid.uuid4()
        scope_uuid = get_scope_uuid

        dataset_dir = root / ds_name
        dataset_dir.mkdir(parents=True, exist_ok=True)

        # Files
        create_file(dataset_dir, 'file1.text')
        create_file(dataset_dir, 'dir0/file2.text')
        create_file(dataset_dir, 'dir1/file.hdf5')
        create_file(dataset_dir, 'dir2/file.hdf5')

        # Skip text files only (directory-based skips are not supported by implementation)
        skip = ['dir1/*']
        write_dataset_info(dataset_dir, ds_name, datetime.datetime.now(), 'skip case', skip=skip)

        config = FolderBaseConfigData(root_directory=root, server_folder=server_folder)
        s_item = DummySyncItem(ds_uuid, ds_name, scope_uuid)
        sync_record = SyncRecordManager(s_item)
        FolderBaseSync.syncDatasetNormal(config, s_item, sync_record)

        ds_remote = dataset_read(ds_uuid)
        names = sorted([f.name for f in ds_remote.files])
        assert 'file1.text' in names
        assert 'dir0/file2.text' in names
        # Both hdf5 present since only filename-based skips are supported
        assert 'dir1/file.hdf5' not in names
        assert 'dir2/file.hdf5' in names
        
@pytest.mark.parametrize("server_folder", [False])
def test_csv_converter_idempotency_and_versioning(get_scope_uuid: uuid.UUID, server_folder: bool):
    with tempfile.TemporaryDirectory() as temp_root:
        root = Path(temp_root)
        ds_name = 'csv_conv'
        ds_uuid = uuid.uuid4()
        scope_uuid = get_scope_uuid
        dataset_dir = root / ds_name
        dataset_dir.mkdir(parents=True, exist_ok=True)

        # CSV content
        files_dir = dataset_dir / 'files'
        files_dir.mkdir(parents=True, exist_ok=True)
        csv_path = create_file(files_dir, 'data.csv', 'x,y\n0,0\n1,1\n')

        converters = {
            'csv_to_hdf5_converter': {
                'module': 'etiket_sync_agent.backends.folderbase.converters.csv_to_hdf5',
                'class': 'CSVToHDF5Converter',
            }
        }
        write_dataset_info(dataset_dir, ds_name, datetime.datetime.now(), 'csv converter', converters=converters)

        config = FolderBaseConfigData(root_directory=root, server_folder=server_folder)
        s_item = DummySyncItem(ds_uuid, ds_name, scope_uuid)
        sync_record = SyncRecordManager(s_item)
        FolderBaseSync.syncDatasetNormal(config, s_item, sync_record)

        ds_remote = dataset_read(ds_uuid)
        names = sorted([f.name for f in ds_remote.files])
        assert 'files/data.csv' in names
        assert 'files/data.hdf5' in names

        # Run again without changes -> no new versions should appear (counts remain)
        before_counts = {name: len(get_remote_files_by_name(ds_remote.files, name)) for name in ['files/data.csv', 'files/data.hdf5']}
        sync_record2 = SyncRecordManager(s_item)
        FolderBaseSync.syncDatasetNormal(config, s_item, sync_record2)
        ds_remote_2 = dataset_read(ds_uuid)
        after_counts = {name: len(get_remote_files_by_name(ds_remote_2.files, name)) for name in ['files/data.csv', 'files/data.hdf5']}
        assert after_counts == before_counts

        # Modify CSV and sync again -> converted file should get a new version
        with open(csv_path, 'a', encoding='utf-8') as f:
            f.write('2,4\n')
        os.utime(csv_path, None)

        sync_record3 = SyncRecordManager(s_item)
        FolderBaseSync.syncDatasetNormal(config, s_item, sync_record3)
        ds_remote_3 = dataset_read(ds_uuid)
        counts_3 = {name: len(get_remote_files_by_name(ds_remote_3.files, name)) for name in ['files/data.csv', 'files/data.hdf5']}
        assert counts_3['files/data.hdf5'] == after_counts['files/data.hdf5'] + 1
        assert counts_3['files/data.csv'] == after_counts['files/data.csv'] + 1

@pytest.mark.parametrize("server_folder", [False])
def test_zarr_converter_only_output_uploaded(get_scope_uuid: uuid.UUID, server_folder: bool):
    with tempfile.TemporaryDirectory() as temp_root:
        root = Path(temp_root)
        ds_name = 'zarr_conv'
        ds_uuid = uuid.uuid4()
        scope_uuid = get_scope_uuid
        dataset_dir = root / ds_name
        dataset_dir.mkdir(parents=True, exist_ok=True)

        # Create a small zarr dataset under files/data.zarr
        zarr_dir = dataset_dir / 'files' / 'data.zarr'
        zarr_dir.parent.mkdir(parents=True, exist_ok=True)
        ds = xarray.Dataset({
            'a': (('x',), np.arange(10)),
            'b': (('x',), np.linspace(0, 1, 10)),
        })
        ds.to_zarr(zarr_dir)

        converters = {
            'zarr_to_netcdf4_converter': {
                'module': 'etiket_sync_agent.backends.folderbase.converters.zarr_to_netcdf4',
                'class': 'ZarrToNETCDF4Converter',
            }
        }
        write_dataset_info(dataset_dir, ds_name, datetime.datetime.now(), 'zarr converter', converters=converters)

        config = FolderBaseConfigData(root_directory=root, server_folder=server_folder)
        s_item = DummySyncItem(ds_uuid, ds_name, scope_uuid)
        sync_record = SyncRecordManager(s_item)
        FolderBaseSync.syncDatasetNormal(config, s_item, sync_record)

        ds_remote = dataset_read(ds_uuid)
        names = sorted([f.name for f in ds_remote.files])
        assert 'files/data.hdf5' in names
        # Ensure the zarr folder itself is not uploaded
        assert 'files/data.zarr' not in names
        assert len(ds_remote.files) == 1

        # Sync again unchanged -> counts stable
        before_count = len(get_remote_files_by_name(ds_remote.files, 'files/data.hdf5'))
        sync_record2 = SyncRecordManager(s_item)
        FolderBaseSync.syncDatasetNormal(config, s_item, sync_record2)
        ds_remote_2 = dataset_read(ds_uuid)
        after_count = len(get_remote_files_by_name(ds_remote_2.files, 'files/data.hdf5'))
        assert after_count == before_count

        # Modify zarr content and sync -> new version for output
        ds_mod = xarray.Dataset({'a': (('x',), np.arange(11))})
        # Clean and rewrite the zarr to update content
        shutil.rmtree(zarr_dir)
        ds_mod.to_zarr(zarr_dir)

        sync_record3 = SyncRecordManager(s_item)
        FolderBaseSync.syncDatasetNormal(config, s_item, sync_record3)
        ds_remote_3 = dataset_read(ds_uuid)
        count_3 = len(get_remote_files_by_name(ds_remote_3.files, 'files/data.hdf5'))
        assert count_3 == after_count + 1


@pytest.mark.parametrize("server_folder", [False])
def test_dataset_info_yaml_updates_remote_dataset(get_scope_uuid: uuid.UUID, server_folder: bool):
    with tempfile.TemporaryDirectory() as temp_root:
        root = Path(temp_root)
        ds_name = 'yaml_update_ds'
        ds_uuid = uuid.uuid4()
        scope_uuid = get_scope_uuid

        dataset_dir = root / ds_name
        dataset_dir.mkdir(parents=True, exist_ok=True)

        # Initial files
        create_file(dataset_dir, 'file1.txt', 'hello')

        # Initial dataset info
        collected_1 = datetime.datetime.now()
        description_1 = 'Initial description'
        attributes_1 = {'a': 1}
        tags_1 = ['t1']
        write_dataset_info(dataset_dir, ds_name, collected_1, description_1, attributes_1, tags_1)

        # Initial sync
        config = FolderBaseConfigData(root_directory=root, server_folder=server_folder)
        s_item = DummySyncItem(ds_uuid, ds_name, scope_uuid)
        sync_record = SyncRecordManager(s_item)
        FolderBaseSync.syncDatasetNormal(config, s_item, sync_record)

        ds_remote_1 = dataset_read(ds_uuid)
        assert ds_remote_1.name == ds_name
        assert set(ds_remote_1.keywords) == set(tags_1)
        assert ds_remote_1.attributes is not None and ds_remote_1.attributes.get('a') == '1'
        assert description_1 in ds_remote_1.description

        # Overwrite dataset info with different values
        new_name = 'yaml_update_ds_newname'
        collected_2 = datetime.datetime.now()
        description_2 = 'Updated description'
        attributes_2 = {'a': 2, 'b': 'two'}
        tags_2 = ['t2', 't3']
        write_dataset_info(dataset_dir, new_name, collected_2, description_2, attributes_2, tags_2)

        # Re-sync to apply updates
        sync_record2 = SyncRecordManager(s_item)
        FolderBaseSync.syncDatasetNormal(config, s_item, sync_record2)

        ds_remote_2 = dataset_read(ds_uuid)
        assert ds_remote_2.name == new_name
        assert set(ds_remote_2.keywords) == set(tags_2)
        # Attributes should contain updated values
        assert ds_remote_2.attributes is not None
        assert ds_remote_2.attributes.get('a') == '2'
        assert ds_remote_2.attributes.get('b') == 'two'
        assert description_2 in ds_remote_2.description


@pytest.mark.parametrize("server_folder", [False])
def test_dataset_uuid_resolution_same_scope_manifest(db_session, get_scope_uuid: uuid.UUID, server_folder: bool):
    sync_source_id_1 = None
    try:
        with tempfile.TemporaryDirectory() as temp_root:
            root = Path(temp_root)
            ds_name = 'uuid_same_scope'
            first_uuid = uuid.uuid4()
            scope_uuid = get_scope_uuid

            # Create a sync source and initial DB-backed sync item
            source = SyncSources(
                name=f"src_{uuid.uuid4()}",
                type=SyncSourceTypes.fileBase,
                status=SyncSourceStatus.SYNCHRONIZING,
                creator="test",
                config_data={"root_directory": str(root)},
                default_scope=get_scope_uuid,
            )
            db_session.add(source)
            db_session.commit()
            db_session.refresh(source)
            sync_source_id_1 = source.id

            s_item_db = SyncItems(
                dataIdentifier=ds_name,
                datasetUUID=first_uuid,
                syncPriority=datetime.datetime.now().timestamp(),
                sync_source_id=source.id,
            )
            crud_sync_items.create_sync_items(db_session, source.id, [s_item_db])
            # Fetch inserted item to get ID
            s_item_db = db_session.query(SyncItems).filter(
                SyncItems.sync_source_id == source.id,
                SyncItems.dataIdentifier == ds_name
            ).first()
            assert s_item_db is not None

            dataset_dir = root / ds_name
            dataset_dir.mkdir(parents=True, exist_ok=True)
            create_file(dataset_dir, 'file.txt', 'content')
            write_dataset_info(dataset_dir, ds_name, datetime.datetime.now(), 'desc')

            # First sync with first UUID (Dummy carries DB id and scope)
            config = FolderBaseConfigData(root_directory=root, server_folder=server_folder)
            sync_record_1 = SyncRecordManager(s_item_db)
            FolderBaseSync.syncDatasetNormal(config, s_item_db, sync_record_1)

            # give it a fake uuid --> check if it updates it
            second_uuid = uuid.uuid4()
            s_item_db = crud_sync_items.update_sync_item(db_session, s_item_db.id, dataset_uuid=second_uuid)
            sync_record_2 = SyncRecordManager(s_item_db)
            FolderBaseSync.syncDatasetNormal(config, s_item_db, sync_record_2)

            # Verify database sync item UUID is reset to manifest UUID (first_uuid)
            db_session.refresh(s_item_db)
            assert s_item_db.datasetUUID == first_uuid
            # Manifest should still reflect first_uuid as it was originally written
            manifest_path = dataset_dir / QH_MANIFEST_FILE
            assert manifest_path.exists()
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = yaml.safe_load(f)
            assert manifest.get('dataset_uuid') == str(first_uuid)
            assert manifest.get('scope_uuid') == str(scope_uuid)
    finally:
        if sync_source_id_1 is not None:
            crud_sync_sources.delete_sync_source(db_session, sync_source_id_1)
        db_session.commit()


@pytest.mark.parametrize("server_folder", [False])
def test_dataset_uuid_resolution_different_scope_manifest(db_session, get_scope_uuid: uuid.UUID, get_other_scope_uuid: uuid.UUID, server_folder: bool):
    sync_source_id_1 = None
    sync_source_id_2 = None
    try:
        with tempfile.TemporaryDirectory() as temp_root:
            root = Path(temp_root)
            ds_name = 'uuid_diff_scope'
            first_uuid = uuid.uuid4()
            scope_uuid_1 = get_scope_uuid

            # Create a sync source and initial DB-backed sync item
            source = SyncSources(
                name=f"src_{uuid.uuid4()}",
                type=SyncSourceTypes.fileBase,
                status=SyncSourceStatus.SYNCHRONIZING,
                creator="test",
                config_data={"root_directory": str(root)},
                default_scope=scope_uuid_1,
            )
            db_session.add(source)
            db_session.commit()
            db_session.refresh(source)
            sync_source_id_1 = source.id
            s_item_db = SyncItems(
                dataIdentifier=ds_name,
                datasetUUID=first_uuid,
                syncPriority=datetime.datetime.now().timestamp(),
                sync_source_id=source.id,
            )
            crud_sync_items.create_sync_items(db_session, source.id, [s_item_db])
            s_item_db = db_session.query(SyncItems).filter(
                SyncItems.sync_source_id == source.id,
                SyncItems.dataIdentifier == ds_name
            ).first()
            assert s_item_db is not None

            dataset_dir = root / ds_name
            dataset_dir.mkdir(parents=True, exist_ok=True)
            create_file(dataset_dir, 'file.txt', 'content')
            write_dataset_info(dataset_dir, ds_name, datetime.datetime.now(), 'desc')

            # First sync under scope 1
            config = FolderBaseConfigData(root_directory=root, server_folder=server_folder)
            sync_record_1 = SyncRecordManager(s_item_db)
            FolderBaseSync.syncDatasetNormal(config, s_item_db, sync_record_1)

            # Second sync under a different scope; DB should NOT be overridden by manifest
            second_uuid = uuid.uuid4()
            
            source_2 = SyncSources(
                name=f"src_{uuid.uuid4()}",
                type=SyncSourceTypes.fileBase,
                status=SyncSourceStatus.SYNCHRONIZING,
                creator="test",
                config_data={"root_directory": str(root)},
                default_scope=get_other_scope_uuid,
            )
            db_session.add(source_2)
            db_session.commit()
            db_session.refresh(source_2)
            
            s_item_2 = SyncItems(
                dataIdentifier=ds_name,
                datasetUUID=second_uuid,
                syncPriority=datetime.datetime.now().timestamp(),
                sync_source_id=source_2.id,
            )
            db_session.add(s_item_2)
            db_session.commit()
            db_session.refresh(s_item_2)
            crud_sync_items.create_sync_items(db_session, source_2.id, [s_item_2])
            
            sync_record_2 = SyncRecordManager(s_item_2)
            FolderBaseSync.syncDatasetNormal(config, s_item_2, sync_record_2)

            # UUID should remain the second one in the DB since scope differs
            db_session.refresh(s_item_2)
            assert s_item_2.datasetUUID == second_uuid

            # The manifest should reflect the second scope and UUID now
            manifest_path = dataset_dir / QH_MANIFEST_FILE
            assert manifest_path.exists()
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = yaml.safe_load(f)
            assert manifest.get('scope_uuid') == str(get_other_scope_uuid)
            assert manifest.get('dataset_uuid') == str(second_uuid)
    finally:
        if sync_source_id_1 is not None:
            crud_sync_sources.delete_sync_source(db_session, sync_source_id_1)
        if sync_source_id_2 is not None:
            crud_sync_sources.delete_sync_source(db_session, sync_source_id_2)
        db_session.commit()

@pytest.mark.parametrize("server_folder", [False])
def test_legacy_created_and_keywords_keys(get_scope_uuid: uuid.UUID, server_folder: bool):
    with tempfile.TemporaryDirectory() as temp_root:
        root = Path(temp_root)
        ds_name = 'legacy_keys_ds'
        ds_uuid = uuid.uuid4()
        scope_uuid = get_scope_uuid

        dataset_dir = root / ds_name
        dataset_dir.mkdir(parents=True, exist_ok=True)
        create_file(dataset_dir, 'file.txt', 'content')

        # Use legacy keys: 'created' and 'keywords'
        info = {
            'dataset_name': ds_name,
            'created': datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            'description': 'legacy keys test',
            'attributes': {'x': 1},
            'keywords': ['k1', 'k2']
        }
        with open(dataset_dir / QH_DATASET_INFO_FILE, 'w', encoding='utf-8') as f:
            yaml.safe_dump(info, f)

        config = FolderBaseConfigData(root_directory=root, server_folder=server_folder)
        s_item = DummySyncItem(ds_uuid, ds_name, scope_uuid)
        sync_record = SyncRecordManager(s_item)
        FolderBaseSync.syncDatasetNormal(config, s_item, sync_record)

        ds_remote = dataset_read(ds_uuid)
        assert set(ds_remote.keywords) == set(['k1', 'k2'])
        assert ds_remote.attributes.get('x') == '1'


@pytest.mark.parametrize("server_folder", [False])
def test_move_or_copy_folder_creates_new_dataset_uuid(db_session, get_scope_uuid: uuid.UUID, server_folder: bool):
    sync_source_id = None
    try:
        with tempfile.TemporaryDirectory() as temp_root:
            root = Path(temp_root)
            scope_uuid = get_scope_uuid

            # Create a real sync source
            source = SyncSources(
                name=f"src_{uuid.uuid4()}",
                type=SyncSourceTypes.fileBase,
                status=SyncSourceStatus.SYNCHRONIZING,
                creator="test",
                config_data={"root_directory": str(root)},
                default_scope=scope_uuid,
            )
            db_session.add(source)
            db_session.commit()
            db_session.refresh(source)
            sync_source_id = source.id

            # Initial dataset folder and sync using DB-backed sync item
            ds_name_1 = 'folder1'
            ds_uuid_1 = uuid.uuid4()
            dataset_dir_1 = root / ds_name_1
            dataset_dir_1.mkdir(parents=True, exist_ok=True)
            create_file(dataset_dir_1, 'file.txt', 'content')
            write_dataset_info(dataset_dir_1, ds_name_1, datetime.datetime.now(), 'initial desc')

            s_item_db1 = SyncItems(
                dataIdentifier=ds_name_1,
                datasetUUID=ds_uuid_1,
                syncPriority=datetime.datetime.now().timestamp(),
                sync_source_id=source.id,
            )
            crud_sync_items.create_sync_items(db_session, source.id, [s_item_db1])
            s_item_db1 = db_session.query(SyncItems).filter(
                SyncItems.sync_source_id == source.id,
                SyncItems.dataIdentifier == ds_name_1
            ).first()
            assert s_item_db1 is not None

            config = FolderBaseConfigData(root_directory=root, server_folder=server_folder)
            sync_record_1 = SyncRecordManager(s_item_db1)
            FolderBaseSync.syncDatasetNormal(config, s_item_db1, sync_record_1)

            # Ensure first manifest exists and points to folder1
            manifest_path_1 = dataset_dir_1 / QH_MANIFEST_FILE
            assert manifest_path_1.exists()
            with open(manifest_path_1, 'r', encoding='utf-8') as f:
                manifest_1 = yaml.safe_load(f)
            assert manifest_1.get('dataset_uuid') == str(ds_uuid_1)
            assert manifest_1.get('dataset_sync_path') == str(dataset_dir_1)

            # Copy/move dataset to folder2, EXCLUDING the manifest to force a new dataset
            ds_name_2 = 'folder2'
            dataset_dir_2 = root / ds_name_2
            dataset_dir_2.mkdir(parents=True, exist_ok=True)
            for dirpath, dirnames, filenames in os.walk(dataset_dir_1):
                rel_dir = Path(dirpath).relative_to(dataset_dir_1)
                target_dir = dataset_dir_2 / rel_dir
                target_dir.mkdir(parents=True, exist_ok=True)
                for filename in filenames:
                    src = Path(dirpath) / filename
                    dst = target_dir / filename
                    shutil.copy2(src, dst)

            # Re-sync as a new dataset with a new UUID using a new DB-backed sync item
            ds_uuid_2 = uuid.uuid4()
            s_item_db2 = SyncItems(
                dataIdentifier=ds_name_2,
                datasetUUID=ds_uuid_2,
                syncPriority=datetime.datetime.now().timestamp(),
                sync_source_id=source.id,
            )
            crud_sync_items.create_sync_items(db_session, source.id, [s_item_db2])
            s_item_db2 = db_session.query(SyncItems).filter(
                SyncItems.sync_source_id == source.id,
                SyncItems.dataIdentifier == ds_name_2
            ).first()
            assert s_item_db2 is not None
            sync_record_2 = SyncRecordManager(s_item_db2)
            FolderBaseSync.syncDatasetNormal(config, s_item_db2, sync_record_2)

            # Assert a new manifest exists for folder2 with new UUID and updated path         
            manifest_path_2 = dataset_dir_2 / QH_MANIFEST_FILE
            with open(manifest_path_2, 'r', encoding='utf-8') as f:
                manifest_2 = yaml.safe_load(f)
            assert manifest_path_2.exists()
            assert manifest_2.get('dataset_uuid') == str(ds_uuid_2)
            assert manifest_2.get('dataset_sync_path') == str(dataset_dir_2)

            # Remote dataset for the new UUID should exist
            ds_remote_2 = dataset_read(ds_uuid_2)
            assert ds_remote_2 is not None
    finally:
        if sync_source_id is not None:
            crud_sync_sources.delete_sync_source(db_session, sync_source_id)
        db_session.commit()
