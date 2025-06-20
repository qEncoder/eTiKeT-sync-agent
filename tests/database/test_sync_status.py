'''
Unit tests for testing the sync status class.

# test 1 ::
-- get model, check if the status is running
-- get model again, check in the database, if only one record exists (id should match the prev one)

# test 2 ::
-- update the status to error and give an example error message
-- get status and check if this is correct
-- update the status to running
-- get status and check if this is correct + check if the last updated time is updated compared to the previous one.
'''
import time

from sqlalchemy.orm import Session

from etiket_sync_agent.crud.sync_status import crud_sync_status
from etiket_sync_agent.models.enums import SyncStatus
from etiket_sync_agent.models.sync_status import SyncStatusRecord


def test_get_or_create_status_creates_new_record(db_session: Session):
    """Test 1: Get model, check if the status is running, verify only one record exists."""
    # First call should create a new record with RUNNING status
    status_record = crud_sync_status.get_or_create_status(db_session)
    
    assert status_record is not None
    assert status_record.id is not None
    assert status_record.status == SyncStatus.RUNNING
    assert status_record.last_update is not None
    assert status_record.error_message is None
    
    # Store the initial record details
    first_record_id = status_record.id
    first_last_update = status_record.last_update
    
    # Second call should return the same record (not create a new one)
    status_record_2 = crud_sync_status.get_or_create_status(db_session)
    
    assert status_record_2.id == first_record_id
    assert status_record_2.status == SyncStatus.RUNNING
    assert status_record_2.last_update == first_last_update
    
    # Verify only one record exists in the database
    all_records = db_session.query(SyncStatusRecord).all()
    assert len(all_records) == 1
    assert all_records[0].id == first_record_id


def test_update_status_and_verify_changes(db_session: Session):
    """Test 2: Update status to error with message, then back to running, verify updates."""
    # Create initial record
    initial_record = crud_sync_status.get_or_create_status(db_session)
    initial_last_update = initial_record.last_update
    
    # Update status to ERROR with error message
    error_message = "Test error occurred during sync"
    updated_record = crud_sync_status.update_status(
        session=db_session,
        status=SyncStatus.ERROR,
        error_message=error_message
    )
    
    # Verify the update
    assert updated_record.id == initial_record.id  # Same record
    assert updated_record.status == SyncStatus.ERROR
    assert updated_record.error_message == error_message
    assert updated_record.last_update > initial_last_update  # Should be updated
    
    # Get status and verify it matches
    current_status = crud_sync_status.get_status(db_session)
    assert current_status.id == initial_record.id
    assert current_status.status == SyncStatus.ERROR
    assert current_status.error_message == error_message
    
    # Store the error state timestamp
    error_last_update = updated_record.last_update
    
    # Update status back to RUNNING
    running_record = crud_sync_status.update_status(
        session=db_session,
        status=SyncStatus.RUNNING
    )
    
    # Verify the update to RUNNING
    assert running_record.id == initial_record.id  # Same record
    assert running_record.status == SyncStatus.RUNNING
    assert running_record.error_message is None  # Should be cleared when not ERROR
    assert running_record.last_update > error_last_update  # Should be updated again
    
    # Final verification with get_status
    final_status = crud_sync_status.get_status(db_session)
    assert final_status.id == initial_record.id
    assert final_status.status == SyncStatus.RUNNING
    assert final_status.error_message is None
    assert final_status.last_update == running_record.last_update


def test_update_status_preserves_error_message_for_error_status(db_session: Session):
    """Test updating to ERROR status preserves existing error message if none provided."""
    # Create initial record and set to ERROR with message
    crud_sync_status.get_or_create_status(db_session)
    
    initial_error_msg = "Initial error message"
    crud_sync_status.update_status(
        session=db_session,
        status=SyncStatus.ERROR,
        error_message=initial_error_msg
    )
    
    # Update to ERROR again without providing error_message
    updated_record = crud_sync_status.update_status(
        session=db_session,
        status=SyncStatus.ERROR
    )
    
    # Should preserve the existing error message
    assert updated_record.status == SyncStatus.ERROR
    assert updated_record.error_message == initial_error_msg


def test_update_status_clears_error_message_for_non_error_status(db_session: Session):
    """Test that error message is cleared when status is not ERROR."""
    # Create initial record and set to ERROR with message
    crud_sync_status.get_or_create_status(db_session)
    
    crud_sync_status.update_status(
        session=db_session,
        status=SyncStatus.ERROR,
        error_message="Some error"
    )
    
    # Update to different statuses and verify error message is cleared
    for status in [SyncStatus.RUNNING, SyncStatus.STOPPED, SyncStatus.NO_CONNECTION, SyncStatus.NOT_LOGGED_IN]:
        updated_record = crud_sync_status.update_status(
            session=db_session,
            status=status
        )
        
        assert updated_record.status == status
        assert updated_record.error_message is None


def test_multiple_status_transitions(db_session: Session):
    """Test multiple status transitions to ensure consistency."""
    # Create initial record
    record = crud_sync_status.get_or_create_status(db_session)
    initial_id = record.id
    
    # Test various status transitions
    transitions = [
        (SyncStatus.RUNNING, None),
        (SyncStatus.ERROR, "Connection failed"),
        (SyncStatus.STOPPED, None),
        (SyncStatus.NO_CONNECTION, None),
        (SyncStatus.NOT_LOGGED_IN, None),
        (SyncStatus.RUNNING, None),
    ]
    
    previous_update_time = record.last_update
    for status, error_msg in transitions:
        # sleep 3 ms to ensure that the last update time is updated (granularity of 1ms)
        time.sleep(0.003)
        updated_record = crud_sync_status.update_status(
            session=db_session,
            status=status,
            error_message=error_msg
        )
        # Verify the record is always the same one
        assert updated_record.id == initial_id
        assert updated_record.status == status
        
        # Verify error message handling
        if status == SyncStatus.ERROR and error_msg:
            assert updated_record.error_message == error_msg
        elif status != SyncStatus.ERROR:
            assert updated_record.error_message is None
        
        # Verify timestamp is updated
        assert updated_record.last_update > previous_update_time
        previous_update_time = updated_record.last_update
        
        # Verify get_status returns the same state
        current_status = crud_sync_status.get_status(db_session)
        assert current_status.id == initial_id
        assert current_status.status == status
        assert current_status.error_message == updated_record.error_message


def test_only_one_status_record_exists(db_session: Session):
    """Test that only one status record ever exists in the database."""
    # Create multiple records through different methods
    record1 = crud_sync_status.get_or_create_status(db_session)
    record2 = crud_sync_status.get_status(db_session)
    
    crud_sync_status.update_status(db_session, SyncStatus.ERROR, "Test error")
    record3 = crud_sync_status.get_status(db_session)
    
    crud_sync_status.update_status(db_session, SyncStatus.RUNNING)
    record4 = crud_sync_status.get_or_create_status(db_session)
    
    # All should be the same record
    assert record1.id == record2.id == record3.id == record4.id
    
    # Verify only one record exists in database
    all_records = db_session.query(SyncStatusRecord).all()
    assert len(all_records) == 1
    assert all_records[0].id == record1.id