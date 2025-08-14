import time
import threading
from typing import Any
from unittest.mock import patch, MagicMock, call
from sqlalchemy.orm import Session

from etiket_sync_agent.crud.sync_status import crud_sync_status
from etiket_sync_agent.models.enums import SyncStatus
from etiket_sync_agent.run import sync_loop


'''
TEST 1: checking sync status reporting

1.1: set the sync status to stopped, check if the sync is indeed not doing anything when running the loop.
1.2: set the sync status to running, check if the sync is indeed doing something when running the loop.
1.3: simulate a connection error (e.g using the sync_scopes function), check if this is correctly reported in the status
1.4: ensure that the user is logged out, check that this error get caught. When logging the user in again, check that the sync is resumed.

'''


def test_sync_loop_stopped_status_does_nothing(db_session: Session):
    """
    Test 1.1: Set the sync status to stopped, check if the sync is indeed not doing anything when running the loop.
    """
    # Set sync status to STOPPED
    crud_sync_status.update_status(db_session, SyncStatus.STOPPED)
    
    # Verify status is set to STOPPED
    status = crud_sync_status.get_status(db_session)
    assert status.status == SyncStatus.STOPPED
    
    # Mock all the sync functions that should not be called when status is STOPPED
    with patch('etiket_sync_agent.run.api_token_session') as mock_api_token, \
            patch('etiket_sync_agent.run.client.check_user_session') as mock_check_user, \
            patch('etiket_sync_agent.run.sync_scopes') as mock_sync_scopes, \
            patch('etiket_sync_agent.run.dao_file_delete_queue.clean_files') as mock_clean_files, \
            patch('etiket_sync_agent.run.run_sync_iter') as mock_run_sync_iter, \
            patch('etiket_sync_agent.run.Session') as mock_session_cls:
        
        # Mock the Session context manager to return our test session
        mock_session_cls.return_value.__enter__.return_value = db_session
        mock_session_cls.return_value.__exit__.return_value = None
        
        # Set up a flag to stop the loop after a few iterations
        loop_iterations = 0
        original_sleep = time.sleep
        
        def mock_sleep(duration):
            nonlocal loop_iterations
            loop_iterations += 1
            # Stop after 3 iterations to prevent infinite loop
            if loop_iterations >= 3:
                raise KeyboardInterrupt("Test completed")
            original_sleep(0.01)  # Small sleep to prevent busy waiting
        
        with patch('time.sleep', side_effect=mock_sleep):
            try:
                sync_loop()
            except KeyboardInterrupt:
                pass  # Expected way to exit the loop for testing
        
        # Verify that sync functions were NOT called
        mock_api_token.assert_not_called()
        mock_check_user.assert_not_called()
        mock_sync_scopes.assert_not_called()
        mock_clean_files.assert_not_called()
        mock_run_sync_iter.assert_not_called()
        
        # Verify we had at least a few iterations (confirming the loop was running)
        assert loop_iterations >= 3
        
        # Verify the status remains STOPPED (wasn't changed to RUNNING)
        final_status = crud_sync_status.get_status(db_session)
        assert final_status.status == SyncStatus.STOPPED


def test_sync_loop_running_status_does_something(db_session: Session, session_etiket_client: Session):
    """
    Test 1.2: Set the sync status to running, check if the sync is indeed doing something when running the loop.
    """
    # Set sync status to RUNNING
    crud_sync_status.update_status(db_session, SyncStatus.RUNNING)
    
    # Verify status is set to RUNNING
    status = crud_sync_status.get_status(db_session)
    assert status.status == SyncStatus.RUNNING
    
    # Mock all the sync functions that should be called when status is RUNNING
    with patch('etiket_sync_agent.run.api_token_session') as mock_api_token, \
            patch('etiket_sync_agent.run.client.check_user_session') as mock_check_user, \
            patch('etiket_sync_agent.run.sync_scopes') as mock_sync_scopes, \
            patch('etiket_sync_agent.run.dao_file_delete_queue.clean_files') as mock_clean_files, \
            patch('etiket_sync_agent.run.run_sync_iter') as mock_run_sync_iter, \
            patch('etiket_sync_agent.run.get_db_session_context') as mock_session_cls, \
            patch('etiket_sync_agent.run.Session') as mock_session_etiket_client, \
            patch('etiket_sync_agent.run.user_settings') as mock_user_settings, \
            patch('etiket_sync_agent.run.SyncConf') as mock_sync_conf:
        
        # Set faster timing intervals for testing
        mock_sync_conf.SCOPE_SYNC_INTERVAL = 0.1  # 100ms instead of 60 seconds
        mock_sync_conf.DELETE_HDF5_CACHE_INTERVAL = 0.1  # 100ms instead of 60 seconds
        mock_sync_conf.USER_CHECK_INTERVAL = 0.05  # 50ms instead of 5 seconds
        mock_sync_conf.CONNECTION_ERROR_DELAY = 0.1  # 100ms instead of 60 seconds
        mock_sync_conf.IDLE_DELAY = 0.01  # 10ms instead of 1 second
        
        # Mock the Session context manager to return our test session
        mock_session_cls.return_value.__enter__.return_value = db_session
        mock_session_cls.return_value.__exit__.return_value = None
        
        mock_session_etiket_client.return_value.__enter__.return_value = session_etiket_client
        mock_session_etiket_client.return_value.__exit__.return_value = None
        
        # Mock user_settings.user_sub
        mock_user_settings.user_sub = "test_user"
        
        # Mock api_token_session context manager
        mock_api_token.return_value.__enter__.return_value = None
        mock_api_token.return_value.__exit__.return_value = None
        
        # Mock time.time() to control the timing intervals
        mock_time = 0
        def mock_time_func():
            nonlocal mock_time
            mock_time += 0.2  # Increment by 200ms to trigger all intervals
            return mock_time
        
        # Stop the loop by raising KeyboardInterrupt after two sync iterations
        call_count = 0
        def run_iter_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise KeyboardInterrupt("Test completed")

        mock_run_sync_iter.side_effect = run_iter_side_effect

        with patch('time.time', side_effect=mock_time_func):
            try:
                sync_loop()
            except KeyboardInterrupt:
                pass  # Expected way to exit the loop for testing
        
        # check that the sync_iteration is 5
        sync_iteration = crud_sync_status.get_status(db_session).sync_iteration_count
        
        # Verify that sync functions WERE called
        mock_api_token.assert_called()
        mock_check_user.assert_called()
        mock_sync_scopes.assert_called_with(session_etiket_client)
        mock_clean_files.assert_called_with(session_etiket_client)
        mock_run_sync_iter.assert_called_with(db_session, session_etiket_client, sync_iteration)
        
        # Verify we had at least a couple iterations (confirming the loop was running)
        assert mock_run_sync_iter.call_count >= 2
        
        # Verify the status was updated to RUNNING during the sync process
        # (The sync_loop should call crud_sync_status.update_status with RUNNING)
        final_status = crud_sync_status.get_status(db_session)
        assert final_status.status == SyncStatus.RUNNING