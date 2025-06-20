'''
Tests when creating datasets.

Dataset can either be already present locally or on the remote server, or not at both places.

Case 1:
Dataset is not present locally, nor on the remote server.
--> run the function and check if the dataset is created on the remote server, though not locally.

Case 2:
Dataset is present locally, but not on the remote server.
--> run the function and check if the dataset is created on the remote server, and locally updated.
--> the dataset to be uploaded to the server should contain an additional attribute and keyword.

Case 3:
Dataset is present on the remote server, but not locally.
--> run the function and check if the dataset is created locally, and the remote dataset is updated correctly.
--> the dataset to be uploaded has an additional attribute and keyword.

Case 4:
Dataset is present locally and on the remote server.
--> run the function and check if the dataset is updated on the remote server.
--> the dataset to be uploaded to the server should contain an additional attribute and keyword.

Case 5:
A dataset locally is present with the same alt_uid as the syncItem.
--> check that the uuid of the syncItem is updated and the remote dataset with the same alt_uid present.

Case 6:
A dataset remote is present with the same alt_uid as the syncItem.
--> check that the uuid of the syncItem is updated to the one of the remote dataset.

'''

import pytest, uuid, datetime

from etiket_client.local.models.dataset import DatasetCreate as DatasetCreateLocal
from etiket_client.local.dao.dataset import dao_dataset
from etiket_client.local.exceptions import DatasetNotFoundException

from etiket_client.remote.endpoints.models.dataset import DatasetCreate as DatasetCreateRemote
from etiket_client.remote.endpoints.dataset import dataset_create, dataset_read

from etiket_sync_agent.sync.sync_utilities import sync_utilities, dataset_info
from etiket_sync_agent.models.sync_items import SyncItems
from etiket_sync_agent.backends.dataset_manifest import DatasetManifest

def test_case_1_dataset_not_present_locally_nor_remotely(
    session_etiket_client, 
    get_scope_uuid
):
    """
    Case 1: Dataset is not present locally, nor on the remote server.
    --> run the function and check if the dataset is created on the remote server, though not locally.
    """
    # Arrange
    dataset_uuid = uuid.uuid4()
    scope_uuid = get_scope_uuid
    alt_uid = f"test_alt_uid_{uuid.uuid4()}"
    
    # Create a sync item
    sync_item = SyncItems(
        id=1,
        sync_source_id=1,
        dataIdentifier="test_data_identifier",
        datasetUUID=dataset_uuid,
        syncPriority=1.0,
        synchronized=False,
        attempts=0,
        manifest={},
        error=None,
        traceback=None
    )
    
    # Create dataset info
    ds_info = dataset_info(
        name="Test Dataset Case 1",
        datasetUUID=dataset_uuid,
        scopeUUID=scope_uuid,
        created=datetime.datetime.now(),
        alt_uid=alt_uid,
        description="Test dataset for case 1",
        keywords=["test", "case1"],
        attributes={"test_attr": "test_value"},
        ranking=1,
        creator="test_user"
    )
    
    dataset_manifest = DatasetManifest(sync_item=sync_item, dataset_path=None)
    
    sync_utilities.create_or_update_dataset(
        live_mode=False,
        s_item=sync_item,
        ds_info=ds_info,
        dataset_manifest=dataset_manifest
    )
    
    # Assert
    remote_dataset = dataset_read(dataset_uuid)
    assert remote_dataset is not None
    assert remote_dataset.uuid == dataset_uuid
    assert remote_dataset.name == ds_info.name
    assert remote_dataset.alt_uid == ds_info.alt_uid
    assert remote_dataset.description == ds_info.description
    assert set(remote_dataset.keywords) == set(ds_info.keywords)
    assert remote_dataset.attributes == ds_info.attributes
    assert remote_dataset.ranking == ds_info.ranking
    assert remote_dataset.creator == ds_info.creator
    assert remote_dataset.scope.uuid == scope_uuid
    
    # Check that dataset was NOT created locally (live_mode=False)
    with pytest.raises(DatasetNotFoundException):
        dao_dataset.read(dataset_uuid, session=session_etiket_client)
    
    # veryify that entries have been added to the log of the dataset_manifest
    logs = dataset_manifest.get_logs()
    assert len(logs) == 2
    assert "Creating or updating dataset" in logs[0]
    assert "Dataset record created on remote server" in logs[1]

def test_case_2_dataset_present_locally_not_remotely(
    session_etiket_client, 
    get_scope_uuid,
    db_session
):
    """
    Case 2: Dataset is present locally, but not on the remote server.
    --> run the function and check if the dataset is created on the remote server, and locally updated.
    --> the dataset to be uploaded to the server should contain an additional attribute and keyword.
    """
    # Arrange
    dataset_uuid = uuid.uuid4()
    scope_uuid = get_scope_uuid
    alt_uid = f"test_alt_uid_{uuid.uuid4()}"
    
    # Create dataset locally first
    local_dataset_create = DatasetCreateLocal(
        uuid=dataset_uuid,
        alt_uid=alt_uid,
        name="Test Dataset Case 2 Local",
        description="Test dataset for case 2 - local version",
        keywords=["test", "case2", "local"],
        attributes={"local_attr": "local_value"},
        ranking=1,
        creator="test_user",
        scope_uuid=scope_uuid,
        collected=datetime.datetime.now()
    )
    
    with session_etiket_client as session:
        dao_dataset.create(local_dataset_create, session=session)
    
    # Create a sync item
    sync_item = SyncItems(
        id=98,
        sync_source_id=1,
        dataIdentifier="test_data_identifier_case2",
        datasetUUID=dataset_uuid,
        syncPriority=1.0,
        synchronized=False,
        attempts=0,
        manifest={},
        error=None,
        traceback=None
    )
    with db_session as session:
        session.add(sync_item)
        session.commit()
        session.refresh(sync_item)
    
    # Create dataset info with additional attribute and keyword
    ds_info = dataset_info(
        name="Test Dataset Case 2 Updated",
        datasetUUID=dataset_uuid,
        scopeUUID=scope_uuid,
        created=datetime.datetime.now(),
        alt_uid=alt_uid,
        description="Test dataset for case 2 - updated version",
        keywords=["test", "case2", "updated"],  # additional keyword
        attributes={"local_attr": "local_value", "additional_attr": "additional_value"},  # additional attribute
        ranking=2,
        creator="test_user"
    )
    
    dataset_manifest = DatasetManifest(sync_item=sync_item, dataset_path=None)
    
    # Act
    sync_utilities.create_or_update_dataset(
        live_mode=True,
        s_item=sync_item,
        ds_info=ds_info,
        dataset_manifest=dataset_manifest
    )
    
    # Assert
    # Check remote dataset was created
    remote_dataset = dataset_read(dataset_uuid)
    assert remote_dataset is not None
    assert remote_dataset.uuid == dataset_uuid
    assert remote_dataset.name == ds_info.name
    assert remote_dataset.alt_uid == ds_info.alt_uid
    assert remote_dataset.description == ds_info.description
    assert set(remote_dataset.keywords) == set(ds_info.keywords)
    assert remote_dataset.attributes == ds_info.attributes
    assert remote_dataset.ranking == ds_info.ranking
    assert remote_dataset.creator == ds_info.creator
    assert remote_dataset.scope.uuid == scope_uuid
    
    # Check local dataset was updated
    with session_etiket_client as session:
        local_dataset = dao_dataset.read(dataset_uuid, session=session)
        assert local_dataset is not None
        assert local_dataset.uuid == dataset_uuid
        assert local_dataset.name == ds_info.name
        assert local_dataset.alt_uid == ds_info.alt_uid
        assert local_dataset.description == ds_info.description
        assert set(local_dataset.keywords) == set(ds_info.keywords)
        assert local_dataset.attributes == ds_info.attributes
        assert local_dataset.ranking == ds_info.ranking
        assert local_dataset.creator == ds_info.creator
    
    # Verify that entries have been added to the log of the dataset_manifest
    logs = dataset_manifest.get_logs()
    assert len(logs) >= 3
    assert "Creating or updating dataset" in logs[0]
    assert "Dataset record created on remote server" in logs[1]
    assert "Dataset record found on local server" in logs[2] or "Dataset record updated on local server" in logs[2]


def test_case_3_dataset_present_remotely_not_locally(
    session_etiket_client, 
    get_scope_uuid
):
    """
    Case 3: Dataset is present on the remote server, but not locally.
    --> run the function and check if the dataset is created locally, and the remote dataset is updated correctly.
    --> the dataset to be uploaded has an additional attribute and keyword.
    """
    # Arrange
    dataset_uuid = uuid.uuid4()
    scope_uuid = get_scope_uuid
    alt_uid = f"test_alt_uid_{uuid.uuid4()}"
    
    # Create dataset remotely first
    remote_dataset_create = DatasetCreateRemote(
        uuid=dataset_uuid,
        alt_uid=alt_uid,
        name="Test Dataset Case 3 Remote",
        description="Test dataset for case 3 - remote version",
        keywords=["test", "case3", "remote"],
        attributes={"remote_attr": "remote_value"},
        ranking=1,
        creator="test_user",
        scope_uuid=scope_uuid,
        collected=datetime.datetime.now()
    )
    
    dataset_create(remote_dataset_create)
    
    # Create a sync item
    sync_item = SyncItems(
        id=3,
        sync_source_id=1,
        dataIdentifier="test_data_identifier_case3",
        datasetUUID=dataset_uuid,
        syncPriority=1.0,
        synchronized=False,
        attempts=0,
        manifest={},
        error=None,
        traceback=None
    )
    
    # Create dataset info with additional attribute and keyword
    ds_info = dataset_info(
        name="Test Dataset Case 3 Updated",
        datasetUUID=dataset_uuid,
        scopeUUID=scope_uuid,
        created=datetime.datetime.now(),
        alt_uid=alt_uid,
        description="Test dataset for case 3 - updated version",
        keywords=["test", "case3", "updated"],  # additional keyword
        attributes={"remote_attr": "remote_value", "additional_attr": "additional_value"},  # additional attribute
        ranking=2,
        creator="test_user"
    )
    
    dataset_manifest = DatasetManifest(sync_item=sync_item, dataset_path=None)
    
    # Act
    sync_utilities.create_or_update_dataset(
        live_mode=True,
        s_item=sync_item,
        ds_info=ds_info,
        dataset_manifest=dataset_manifest
    )
    
    # Assert
    # Check remote dataset was updated
    remote_dataset = dataset_read(dataset_uuid)
    assert remote_dataset is not None
    assert remote_dataset.uuid == dataset_uuid
    assert remote_dataset.name == ds_info.name
    assert remote_dataset.alt_uid == ds_info.alt_uid
    assert remote_dataset.description == ds_info.description
    assert set(remote_dataset.keywords) == set(ds_info.keywords)
    # Note: attributes should be merged, so both original and additional should be present
    expected_attributes = {"remote_attr": "remote_value", "additional_attr": "additional_value"}
    assert remote_dataset.attributes == expected_attributes
    assert remote_dataset.ranking == ds_info.ranking
    assert remote_dataset.creator == ds_info.creator
    assert remote_dataset.scope.uuid == scope_uuid
    
    # Check local dataset was created
    with session_etiket_client as session:
        local_dataset = dao_dataset.read(dataset_uuid, session=session)
        assert local_dataset is not None
        assert local_dataset.uuid == dataset_uuid
        assert local_dataset.name == ds_info.name
        assert local_dataset.alt_uid == ds_info.alt_uid
        assert local_dataset.description == ds_info.description
        assert set(local_dataset.keywords) == set(ds_info.keywords)
        assert local_dataset.attributes == ds_info.attributes
        assert local_dataset.ranking == ds_info.ranking
        assert local_dataset.creator == ds_info.creator
    
    # Verify that entries have been added to the log of the dataset_manifest
    logs = dataset_manifest.get_logs()
    print(logs)
    assert len(logs) >= 4
    assert "Creating or updating dataset" in logs[0]
    assert "Dataset record found on remote server" in logs[1]
    assert "Dataset record updated on remote server" in logs[2]
    assert "Dataset record created on local server" in logs[3]

def test_case_4_dataset_present_locally_and_remotely(
    session_etiket_client, 
    get_scope_uuid
):
    """
    Case 4: Dataset is present locally and on the remote server.
    --> run the function and check if the dataset is updated on the remote server.
    --> the dataset to be uploaded to the server should contain an additional attribute and keyword.
    """
    # Arrange
    dataset_uuid = uuid.uuid4()
    scope_uuid = get_scope_uuid
    alt_uid = f"test_alt_uid_{uuid.uuid4()}"
    
    # Create dataset locally first
    local_dataset_create = DatasetCreateLocal(
        uuid=dataset_uuid,
        alt_uid=alt_uid,
        name="Test Dataset Case 4 Local",
        description="Test dataset for case 4 - local version",
        keywords=["test", "case4", "local"],
        attributes={"local_attr": "local_value"},
        ranking=1,
        creator="test_user",
        scope_uuid=scope_uuid,
        collected=datetime.datetime.now()
    )
    
    with session_etiket_client as session:
        dao_dataset.create(local_dataset_create, session=session)
    
    # Create dataset remotely first
    remote_dataset_create = DatasetCreateRemote(
        uuid=dataset_uuid,
        alt_uid=alt_uid,
        name="Test Dataset Case 4 Remote",
        description="Test dataset for case 4 - remote version",
        keywords=["test", "case4", "remote"],
        attributes={"remote_attr": "remote_value"},
        ranking=1,
        creator="test_user",
        scope_uuid=scope_uuid,
        collected=datetime.datetime.now()
    )
    
    dataset_create(remote_dataset_create)
    
    # Create a sync item
    sync_item = SyncItems(
        id=4,
        sync_source_id=1,
        dataIdentifier="test_data_identifier_case4",
        datasetUUID=dataset_uuid,
        syncPriority=1.0,
        synchronized=False,
        attempts=0,
        manifest={},
        error=None,
        traceback=None
    )
    
    # Create dataset info with additional attribute and keyword
    ds_info = dataset_info(
        name="Test Dataset Case 4 Updated",
        datasetUUID=dataset_uuid,
        scopeUUID=scope_uuid,
        created=datetime.datetime.now(),
        alt_uid=alt_uid,
        description="Test dataset for case 4 - updated version",
        keywords=["test", "case4", "updated"],  # additional keyword
        attributes={"local_attr": "local_value", "remote_attr": "remote_value", "additional_attr": "additional_value"},  # additional attribute
        ranking=2,
        creator="test_user"
    )
    
    dataset_manifest = DatasetManifest(sync_item=sync_item, dataset_path=None)
    
    # Act
    sync_utilities.create_or_update_dataset(
        live_mode=True,
        s_item=sync_item,
        ds_info=ds_info,
        dataset_manifest=dataset_manifest
    )
    
    # Assert
    # Check remote dataset was updated
    remote_dataset = dataset_read(dataset_uuid)
    assert remote_dataset is not None
    assert remote_dataset.uuid == dataset_uuid
    assert remote_dataset.name == ds_info.name
    assert remote_dataset.alt_uid == ds_info.alt_uid
    assert remote_dataset.description == ds_info.description
    assert set(remote_dataset.keywords) == set(ds_info.keywords)
    assert remote_dataset.attributes == ds_info.attributes
    assert remote_dataset.ranking == ds_info.ranking
    assert remote_dataset.creator == ds_info.creator
    assert remote_dataset.scope.uuid == scope_uuid
    
    # Check local dataset was updated
    with session_etiket_client as session:
        local_dataset = dao_dataset.read(dataset_uuid, session=session)
        assert local_dataset is not None
        assert local_dataset.uuid == dataset_uuid
        assert local_dataset.name == ds_info.name
        assert local_dataset.alt_uid == ds_info.alt_uid
        assert local_dataset.description == ds_info.description
        assert set(local_dataset.keywords) == set(ds_info.keywords)
        assert local_dataset.attributes == ds_info.attributes
        assert local_dataset.ranking == ds_info.ranking
        assert local_dataset.creator == ds_info.creator
    
    # Verify that entries have been added to the log of the dataset_manifest
    logs = dataset_manifest.get_logs()
    assert len(logs) >= 5
    assert "Creating or updating dataset" in logs[0]
    assert "Dataset record found on remote server" in logs[1]
    assert "Dataset record updated on remote server" in logs[2]
    assert "Dataset record found on local server" in logs[3] 
    assert "Dataset record updated on local server" in logs[4]

def test_case_5_local_dataset_with_same_alt_uid(
    session_etiket_client,
    db_session,
    get_scope_uuid
):
    """
    Case 5: A dataset locally is present with the same alt_uid as the syncItem.
    --> check that the uuid of the syncItem is updated and the remote dataset with the same alt_uid present.
    """
    # Arrange
    existing_dataset_uuid = uuid.uuid4()
    sync_item_uuid = uuid.uuid4()  # Different UUID for sync item
    scope_uuid = get_scope_uuid
    alt_uid = f"test_alt_uid_{uuid.uuid4()}"
    
    # Create dataset locally first with existing_dataset_uuid
    local_dataset_create = DatasetCreateLocal(
        uuid=existing_dataset_uuid,
        alt_uid=alt_uid,
        name="Test Dataset Case 5 Local",
        description="Test dataset for case 5 - local version",
        keywords=["test", "case5", "local"],
        attributes={"local_attr": "local_value"},
        ranking=1,
        creator="test_user",
        scope_uuid=scope_uuid,
        collected=datetime.datetime.now()
    )
    
    with session_etiket_client as session:
        dao_dataset.create(local_dataset_create, session=session)
    
    # Create a sync item with different UUID but same alt_uid
    with db_session as session:
        sync_item = SyncItems(
            id=100,
            sync_source_id=1,
            dataIdentifier="test_data_identifier_case5",
            datasetUUID=sync_item_uuid,  # Different UUID
            syncPriority=1.0,
            synchronized=False,
            attempts=0,
            manifest={},
            error=None,
            traceback=None
        )
        session.add(sync_item)
        session.commit()
        session.refresh(sync_item)
    
    # Create dataset info with sync_item_uuid but same alt_uid
    ds_info = dataset_info(
        name="Test Dataset Case 5 Updated",
        datasetUUID=sync_item_uuid,  # This should get updated to existing_dataset_uuid
        scopeUUID=scope_uuid,
        created=datetime.datetime.now(),
        alt_uid=alt_uid,  # Same alt_uid as existing local dataset
        description="Test dataset for case 5 - updated version",
        keywords=["test", "case5", "updated"],  # additional keyword
        attributes={"local_attr": "local_value", "additional_attr": "additional_value"},  # additional attribute
        ranking=2,
        creator="test_user"
    )
    
    dataset_manifest = DatasetManifest(sync_item=sync_item, dataset_path=None)
    
    # Act
    sync_utilities.create_or_update_dataset(
        live_mode=True,
        s_item=sync_item,
        ds_info=ds_info,
        dataset_manifest=dataset_manifest
    )
    
    # Assert
    # Check remote dataset was created with existing_dataset_uuid (not sync_item_uuid)
    remote_dataset = dataset_read(existing_dataset_uuid)
    assert remote_dataset is not None
    assert remote_dataset.uuid == existing_dataset_uuid
    assert remote_dataset.name == ds_info.name
    assert remote_dataset.alt_uid == ds_info.alt_uid
    assert remote_dataset.description == ds_info.description
    assert set(remote_dataset.keywords) == set(ds_info.keywords)
    assert remote_dataset.attributes == ds_info.attributes
    assert remote_dataset.ranking == ds_info.ranking
    assert remote_dataset.creator == ds_info.creator
    assert remote_dataset.scope.uuid == scope_uuid
    
    # Check local dataset with existing_dataset_uuid still exists and was updated
    with session_etiket_client as session:
        local_dataset = dao_dataset.read(existing_dataset_uuid, session=session)
        assert local_dataset is not None
        assert local_dataset.uuid == existing_dataset_uuid
        assert local_dataset.name == ds_info.name
        assert local_dataset.alt_uid == ds_info.alt_uid
    
    # Verify that entries have been added to the log of the dataset_manifest
    logs = dataset_manifest.get_logs()
    assert len(logs) >= 4
    assert "Creating or updating dataset" in logs[0]
    assert "Found alt_uid in a dataset on the local server, updating uuid ..." in logs[1]
    assert "Dataset record created on remote server" in logs[2]
    assert "Dataset record found on local server" in logs[3]
    assert "Dataset record updated on local server" in logs[4]

def test_case_6_remote_dataset_with_same_alt_uid(
    session_etiket_client, 
    get_scope_uuid,
    db_session
):
    """
    Case 6: A dataset remote is present with the same alt_uid as the syncItem.
    --> check that the uuid of the syncItem is updated to the one of the remote dataset.
    """
    # Arrange
    existing_dataset_uuid = uuid.uuid4()
    sync_item_uuid = uuid.uuid4()  # Different UUID for sync item
    scope_uuid = get_scope_uuid
    alt_uid = f"test_alt_uid_{uuid.uuid4()}"
    
    # Create dataset remotely first with existing_dataset_uuid
    remote_dataset_create = DatasetCreateRemote(
        uuid=existing_dataset_uuid,
        alt_uid=alt_uid,
        name="Test Dataset Case 6 Remote",
        description="Test dataset for case 6 - remote version",
        keywords=["test", "case6", "remote"],
        attributes={"remote_attr": "remote_value"},
        ranking=1,
        creator="test_user",
        scope_uuid=scope_uuid,
        collected=datetime.datetime.now()
    )
    
    dataset_create(remote_dataset_create)
    
    # Create a sync item with different UUID but same alt_uid
    sync_item = SyncItems(
        id=101,
        sync_source_id=1,
        dataIdentifier="test_data_identifier_case6",
        datasetUUID=sync_item_uuid,  # Different UUID
        syncPriority=1.0,
        synchronized=False,
        attempts=0,
        manifest={},
        error=None,
        traceback=None
    )
    with db_session as session:
        session.add(sync_item)
        session.commit()
        session.refresh(sync_item)
    
    # Create dataset info with sync_item_uuid but same alt_uid
    ds_info = dataset_info(
        name="Test Dataset Case 6 Updated",
        datasetUUID=sync_item_uuid,  # This should get updated to existing_dataset_uuid
        scopeUUID=scope_uuid,
        created=datetime.datetime.now(),
        alt_uid=alt_uid,  # Same alt_uid as existing remote dataset
        description="Test dataset for case 6 - updated version",
        keywords=["test", "case6", "updated"],  # additional keyword
        attributes={"remote_attr": "remote_value", "additional_attr": "additional_value"},  # additional attribute
        ranking=2,
        creator="test_user"
    )
    
    dataset_manifest = DatasetManifest(sync_item=sync_item, dataset_path=None)
    
    # Act
    sync_utilities.create_or_update_dataset(
        live_mode=True,
        s_item=sync_item,
        ds_info=ds_info,
        dataset_manifest=dataset_manifest
    )
    
    # Assert
    # The sync_utilities should have found the existing remote dataset by alt_uid
    # and updated the sync item's UUID to match the existing dataset
    # So we should check the existing_dataset_uuid, not sync_item_uuid
    remote_dataset = dataset_read(existing_dataset_uuid)
    assert remote_dataset is not None
    assert remote_dataset.uuid == existing_dataset_uuid
    assert remote_dataset.name == ds_info.name
    assert remote_dataset.alt_uid == ds_info.alt_uid
    assert remote_dataset.description == ds_info.description
    assert set(remote_dataset.keywords) == set(ds_info.keywords)
    # Attributes should be merged
    expected_attributes = {"remote_attr": "remote_value", "additional_attr": "additional_value"}
    assert remote_dataset.attributes == expected_attributes
    assert remote_dataset.ranking == ds_info.ranking
    assert remote_dataset.creator == ds_info.creator
    assert remote_dataset.scope.uuid == scope_uuid
    
    # Check local dataset was created with the existing_dataset_uuid
    with session_etiket_client as session:
        local_dataset = dao_dataset.read(existing_dataset_uuid, session=session)
        assert local_dataset is not None
        assert local_dataset.uuid == existing_dataset_uuid
        assert local_dataset.name == ds_info.name
        assert local_dataset.alt_uid == ds_info.alt_uid
    
    # Check that sync_item_uuid dataset was NOT created locally
    with pytest.raises(DatasetNotFoundException):
        with session_etiket_client as session:
            dao_dataset.read(sync_item_uuid, session=session)
    
    # Verify that entries have been added to the log of the dataset_manifest
    logs = dataset_manifest.get_logs()
    assert len(logs) >= 5
    assert "Creating or updating dataset" in logs[0]
    assert "Dataset record found on remote server (by alt_uid), updating uuid to match the one on the remote server." in logs[1]
    assert "Dataset record updated on remote server" in logs[2]
    assert "Dataset record created on local server" in logs[3]
    assert "Dataset record found on local server, no update needed." in logs[4]