import pytest, uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session
from typing import Generator, List, Dict, Any

from etiket_sync_agent.crud.sync_items import crud_sync_items

from etiket_sync_agent.models.enums import SyncSourceTypes, SyncSourceStatus
from etiket_sync_agent.models.sync_items import SyncItems
from etiket_sync_agent.models.sync_sources import SyncSources

# Define an example sync item
example_sync_item: Dict[str, Any] = {
    "dataIdentifier": f"item_{uuid.uuid4()}",
    "priority": 1.0,
}

# Regarding missing things: The tests cover the scenarios described in the comments quite well. Possible additions could include:
# - Testing edge cases like providing empty lists to create_sync_items.
# - Testing error handling (e.g., what happens if invalid data is provided, although type hinting helps prevent this).
# - More thoroughly testing the retry logic in read_next_sync_item by manipulating last_update times to cover all attempt intervals.


# test sync items:
# test 1
# -- provide 1 new items -- check with sql if it is indeed added
# -- provide 5 new items -- check with sql if they are added
# in sql set attempts to 1 and set synchronized to True
# -- provide 5 new items with same dataIdentifier, but different priority, check synchrized False and attempts 0
# clear all items -- use source id

# test 2
# test bulk insert, add 20k items, count the number of items in the table -- use source id
# clear all items -- use source id

# test 3 : check read_next_sync_item function
# create a list of sync items, in sql, such that the id is known:
# - priority 10 - attemps 0 - synchronized True - last_update 1 day ago
# - priority 2 - attemps 0 - synchronized False - last_update None
# - priority 1 - attemps 5 - synchronized False - last_update 1 hour ago
# - priority 0.1 - attemps 1 - synchronized False - last_update 21 minutes ago
# - priority 0.01 - attemps 2 - synchronized False - last_update 1 and 1 minute ago
# - priority 0.001 - attemps 0 - synchronized False - last_update None

# simulate the requests, mark just with sqlalchemy
# 1. read next sync item, check if it has priority 2
# 2. read next sync item, with offset 1, check if it has priority 0.001
# 3. mark the item with priority 2 as synchronized
# 4. read next sync item, check if it has priority 0.001
# 5. read next sync item, with offset 1, check if it has priority 1
# 6. mark the item with priority 0.001 as synchronized
# 7. read next sync item, check if it has priority 1
# 8. read next sync item, with offset 1, check if it has priority 0.1
# 9. read next sync item, with offset 10, check if None is returned
# 10. mark all as synchronized
# 11. read next sync item, check if None is returned
# 12. read next sync item, with offset 1,  check if None is returned
# --clear all items

# test 4 : update_sync_item
# create sync item, try to do a partial update, and a full update, check if the update matched
# --clear all items

# test 5 : get_last_sync_item
# create a list of sync items, in sql, such that the id is known:
# - priority 10 synchronized True
# - priority 2 synchronized False
# - priority 1 synchronized False
# should return the item with priority 10.
# clean up all items


@pytest.fixture(scope="function")
def sync_source_id(db_session : Session) -> Generator[int, None, None]:
    """Creates a SyncSource for the test function and deletes it afterwards."""
    
    sync_source_data = None
    try:
        sync_source_data = SyncSources(
            name = f"test_source_{uuid.uuid4()}",
            type = SyncSourceTypes.qcodes,
            status = SyncSourceStatus.PAUSED,
            creator = "test_user",
            config_data = {},
            default_scope = None
        )
        db_session.add(sync_source_data)
        db_session.commit()
        db_session.refresh(sync_source_data)
        yield sync_source_data.id
    finally:
        if sync_source_data:
            db_session.delete(sync_source_data)
            db_session.commit()

def test_create_sync_item(db_session: Session, sync_source_id: int):
    """Test creating a single sync item."""
    # Create a SyncItems model instance
    item_to_create = SyncItems(
        dataIdentifier=example_sync_item["dataIdentifier"],
        syncPriority=example_sync_item["priority"], # Ensure field names match the model
        sync_source_id=sync_source_id # Associate with the source
    )

    crud_sync_items.create_sync_items(
        session=db_session,
        sync_source_id=sync_source_id,
        sync_items=[item_to_create] # Pass the model instance
    )
    # Add verification
    created_item = db_session.query(SyncItems).filter(
        SyncItems.sync_source_id == sync_source_id,
        SyncItems.dataIdentifier == item_to_create.dataIdentifier # Use instance attribute
    ).first()
    assert created_item is not None
    assert created_item.syncPriority == item_to_create.syncPriority # Check correct attribute
    assert created_item.synchronized is False
    assert created_item.attempts == 0

    # Clean up
    db_session.delete(created_item)
    db_session.commit()

# Helper function to clear items for a source
def _clear_sync_items(session: Session, sync_source_id: int):
    session.query(SyncItems).filter(SyncItems.sync_source_id == sync_source_id).delete()
    session.commit()

def test_sync_item_creation_and_update(db_session: Session, sync_source_id: int):
    """Test 1: Create, verify, update on conflict, and clear."""
    try:
        # -- provide 1 new item -- check with sql if it is indeed added
        item1_data = SyncItems(
            dataIdentifier=f"item_test1_1_{uuid.uuid4()}",
            syncPriority=1.0,
            sync_source_id=sync_source_id
        )
        crud_sync_items.create_sync_items(db_session, sync_source_id, [item1_data])
        db_item1 = db_session.query(SyncItems).filter(
            SyncItems.dataIdentifier == item1_data.dataIdentifier
        ).first()
        assert db_item1 is not None
        assert db_item1.syncPriority == 1.0

        # -- provide 5 new items -- check with sql if they are added
        items_data = []
        for i in range(5):
            items_data.append(SyncItems(
                dataIdentifier=f"item_test1_multi_{i}_{uuid.uuid4()}",
                syncPriority=float(i),
                sync_source_id=sync_source_id
            ))
        crud_sync_items.create_sync_items(db_session, sync_source_id, items_data)
        count = db_session.query(SyncItems).filter(
            SyncItems.sync_source_id == sync_source_id,
            SyncItems.dataIdentifier.like("item_test1_multi_%")
        ).count()
        assert count == 5

        # in sql set attempts to 1 and set synchronized to True for item1
        db_item1.attempts = 1
        db_item1.synchronized = True
        db_session.add(db_item1)
        db_session.commit()
        db_session.refresh(db_item1)
        assert db_item1.attempts == 1
        assert db_item1.synchronized is True

        # -- provide 5 new items with same dataIdentifier as item1, but different priority,
        # check synchronized=False and attempts=0
        conflict_items = []
        for i in range(5):
            conflict_items.append(SyncItems(
                dataIdentifier=item1_data.dataIdentifier, # Same identifier
                syncPriority=10.0 + i,
                sync_source_id=sync_source_id
            ))
        crud_sync_items.create_sync_items(db_session, sync_source_id, conflict_items)

        db_session.refresh(db_item1)
        assert db_item1.synchronized is False
        assert db_item1.attempts == 0
        # Priority should be updated to the last one provided (14.0)
        assert db_item1.syncPriority == 14.0

    finally:
        # clear all items -- use source id
        _clear_sync_items(db_session, sync_source_id)


def test_bulk_insert_sync_items(db_session: Session, sync_source_id: int):
    """Test 2: Test bulk insert (using 1k items instead of 20k for speed)."""
    try:
        item_count = 20000
        bulk_items = []
        for i in range(item_count):
            bulk_items.append(SyncItems(
                dataIdentifier=f"bulk_item_{i}_{uuid.uuid4()}",
                syncPriority=float(i),
                sync_source_id=sync_source_id
            ))
        
        crud_sync_items.create_sync_items(db_session, sync_source_id, bulk_items)
        
        count = db_session.query(SyncItems).filter(SyncItems.sync_source_id == sync_source_id).count()
        assert count == item_count

    finally:
        # clear all items -- use source id
        _clear_sync_items(db_session, sync_source_id)


def test_read_next_sync_item(db_session: Session, sync_source_id: int):
    """Test 3: Test read_next_sync_item logic with various item states."""
    try:
        now = datetime.now(timezone.utc)
        # Manually create items with specific states
        items_to_create = [
            # - priority 10 - attemps 0 - synchronized True - last_update 1 day ago
            SyncItems(datasetUUID=uuid.uuid4(), sync_source_id=sync_source_id, dataIdentifier="p10_syncT", 
                        syncPriority=10.0, attempts=0, synchronized=True, last_update=now - timedelta(days=1)),
            # - priority 2 - attemps 0 - synchronized False - last_update None
            SyncItems(datasetUUID=uuid.uuid4(), sync_source_id=sync_source_id, dataIdentifier="p2_syncF_att0",
                        syncPriority=2.0, attempts=0, synchronized=False, last_update=None),
            # - priority 1 - attemps 5 - synchronized False - last_update 1 hour ago (should not be picked by retry logic yet)
            SyncItems(datasetUUID=uuid.uuid4(), sync_source_id=sync_source_id, dataIdentifier="p1_syncF_att5",
                        syncPriority=1.0, attempts=5, synchronized=False, last_update=now - timedelta(hours=1)),
            # - priority 0.1 - attemps 1 - synchronized False - last_update 21 minutes ago (ready for retry)
            SyncItems(datasetUUID=uuid.uuid4(), sync_source_id=sync_source_id, dataIdentifier="p0.1_syncF_att1_retry",
                        syncPriority=0.1, attempts=1, synchronized=False, last_update=now - timedelta(minutes=21)),
            # - priority 0.01 - attemps 2 - synchronized False - last_update 1 hour and 1 minute ago (ready for retry)
            SyncItems(datasetUUID=uuid.uuid4(), sync_source_id=sync_source_id, dataIdentifier="p0.01_syncF_att2_retry",
                        syncPriority=0.01, attempts=2, synchronized=False, last_update=now - timedelta(hours=1, minutes=1)),
            # - priority 0.001 - attemps 0 - synchronized False - last_update None
            SyncItems(datasetUUID=uuid.uuid4(), sync_source_id=sync_source_id, dataIdentifier="p0.001_syncF_att0",
                        syncPriority=0.001, attempts=0, synchronized=False, last_update=None)
        ]
        db_session.add_all(items_to_create)
        db_session.commit()

        # Refresh items to get their IDs
        item_map = {item.dataIdentifier: item for item in db_session.query(SyncItems).filter(SyncItems.sync_source_id == sync_source_id).all()}

        # 1. read next sync item, check if it has priority 2 (highest priority, sync=F, att=0)
        next_item = crud_sync_items.read_next_sync_item(db_session, sync_source_id, offset=0)
        assert next_item is not None
        assert next_item.dataIdentifier == "p2_syncF_att0"

        # 2. read next sync item, with offset 1, check if it has priority 0.001 (next highest P, sync=F, att=0)
        next_item = crud_sync_items.read_next_sync_item(db_session, sync_source_id, offset=1)
        assert next_item is not None
        assert next_item.dataIdentifier == "p0.001_syncF_att0"
        
        # --- No more sync=F, att=0 items. Now check retry logic --- 
        # It should prioritize lower attempts first, then higher priority.
        # Ready for retry: p0.1_syncF_att1_retry, p0.01_syncF_att2_retry
        # Expect: p0.1_syncF_att1_retry (att=1) before p0.01_syncF_att2_retry (att=2)
        
        # 3. read next sync item (offset 0 from retry), check p0.1_syncF_att1_retry
        next_item = crud_sync_items.read_next_sync_item(db_session, sync_source_id, offset=2) # Offset 2 because 2 items have attempts=0 & synchronized=False
        assert next_item is not None
        assert next_item.dataIdentifier == "p0.1_syncF_att1_retry"
        
        # 4. read next sync item (offset 1 from retry), check p0.01_syncF_att2_retry
        next_item = crud_sync_items.read_next_sync_item(db_session, sync_source_id, offset=3)
        assert next_item is not None
        assert next_item.dataIdentifier == "p0.01_syncF_att2_retry"

        # 5. Mark the item with priority 2 as synchronized
        item_p2 = item_map["p2_syncF_att0"]
        crud_sync_items.update_sync_item(db_session, sync_item_id=item_p2.id, synchronized=True)
        db_session.refresh(item_p2)
        assert item_p2.synchronized is True

        # 6. read next sync item, check if it has priority 0.001 (highest P, sync=F, att=0)
        next_item = crud_sync_items.read_next_sync_item(db_session, sync_source_id, offset=0)
        assert next_item is not None
        assert next_item.dataIdentifier == "p0.001_syncF_att0"

        # 7. Mark the item with priority 0.001 as synchronized
        item_p0001 = item_map["p0.001_syncF_att0"]
        crud_sync_items.update_sync_item(db_session, sync_item_id=item_p0001.id, synchronized=True)
        db_session.refresh(item_p0001)
        assert item_p0001.synchronized is True
        
        # --- No more sync=F, att=0 items. Check retry again --- 
        # Remaining eligible for retry: p0.1_syncF_att1_retry, p0.01_syncF_att2_retry
        # Expect: p0.1_syncF_att1_retry (att=1) first.

        # 8. read next sync item (offset 0 for retry), check p0.1_syncF_att1_retry
        next_item = crud_sync_items.read_next_sync_item(db_session, sync_source_id, offset=0) # Offset resets as no sync=F, att=0 left
        assert next_item is not None
        assert next_item.dataIdentifier == "p0.1_syncF_att1_retry"
        
        # 9. read next sync item (offset 1 for retry), check p0.01_syncF_att2_retry
        next_item = crud_sync_items.read_next_sync_item(db_session, sync_source_id, offset=1)
        assert next_item is not None
        assert next_item.dataIdentifier == "p0.01_syncF_att2_retry"

        # 10. read next sync item, with offset 10, check if None is returned
        next_item = crud_sync_items.read_next_sync_item(db_session, sync_source_id, offset=10)
        assert next_item is None

        # 11. mark all remaining as synchronized
        items_to_update = db_session.query(SyncItems).filter(
            SyncItems.sync_source_id == sync_source_id,
            SyncItems.synchronized == False
        ).all()
        for item in items_to_update:
            crud_sync_items.update_sync_item(db_session, sync_item_id=item.id, synchronized=True)
        db_session.commit()

        # 12. read next sync item, check if None is returned
        next_item = crud_sync_items.read_next_sync_item(db_session, sync_source_id, offset=0)
        assert next_item is None

    finally:
        # clear all items
        _clear_sync_items(db_session, sync_source_id)


def test_update_sync_item(db_session: Session, sync_source_id: int):
    """Test 4: Update sync item (partial and full)."""
    try:
        # Create sync item
        item_data = SyncItems(
            dataIdentifier=f"update_item_{uuid.uuid4()}",
            datasetUUID=uuid.uuid4(),
            syncPriority=5.0,
            attempts=0,
            synchronized=False,
            sync_source_id=sync_source_id
        )
        db_session.add(item_data)
        db_session.commit()
        db_session.refresh(item_data)
        item_id = item_data.id

        # Partial update (priority and attempts)
        crud_sync_items.update_sync_item(
            session=db_session,
            sync_item_id=item_id,
            sync_priority=6.0,
            attempts=1
        )
        db_session.refresh(item_data)
        assert item_data.syncPriority == 6.0
        assert item_data.attempts == 1
        assert item_data.synchronized is False # Should not change

        # Full update (using available fields in update_sync_item)
        new_uuid = uuid.uuid4()
        new_manifest = {"file1": "hash1"}
        crud_sync_items.update_sync_item(
            session=db_session,
            sync_item_id=item_id,
            dataset_uuid=new_uuid,
            sync_priority=7.0,
            attempts=0,
            sync_record=new_manifest,
            error="Test Error",
            traceback="Test Traceback",
            synchronized=True
        )
        db_session.refresh(item_data)
        assert item_data.datasetUUID == new_uuid
        assert item_data.syncPriority == 7.0
        assert item_data.attempts == 0
        assert item_data.sync_record == new_manifest
        assert item_data.error == "Test Error"
        assert item_data.traceback == "Test Traceback"
        assert item_data.synchronized is True

    finally:
        # clear all items
        _clear_sync_items(db_session, sync_source_id)


def test_get_last_sync_item(db_session: Session, sync_source_id: int):
    """Test 5: Get the last sync item based on highest priority."""
    try:
        # Create a list of sync items
        items_to_create = [
            SyncItems(datasetUUID=uuid.uuid4(), sync_source_id=sync_source_id, dataIdentifier="p10_syncT", syncPriority=10.0, synchronized=True),
            SyncItems(datasetUUID=uuid.uuid4(), sync_source_id=sync_source_id, dataIdentifier="p2_syncF", syncPriority=2.0, synchronized=False),
            SyncItems(datasetUUID=uuid.uuid4(), sync_source_id=sync_source_id, dataIdentifier="p1_syncF", syncPriority=1.0, synchronized=False)
        ]
        db_session.add_all(items_to_create)
        db_session.commit()

        # Get last sync item (should return the one with priority 10)
        last_item = crud_sync_items.get_last_sync_item(db_session, sync_source_id)
        assert last_item is not None
        assert last_item.dataIdentifier == "p10_syncT"
        assert last_item.syncPriority == 10.0

    finally:
        # clean up all items
        _clear_sync_items(db_session, sync_source_id)


