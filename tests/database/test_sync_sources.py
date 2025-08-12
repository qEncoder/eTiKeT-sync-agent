# test 1: create_sync_source
# use qcodes_set_up fixture to test source of type qcodes
# - QCoDeSConfigData -> define -> and convert to dict,
# - Default scope should be defined in this case, get first scope with 
# -- from etiket_client.python_api.scopes import get_scopes

# To test::
# - create source, and check if the returned object matches the input parameters + default values
# - create source again, with same name, should fail
# - create source with default scope set to None, should fail
# - remove one of the fields from the config data, should fail
# - in the config data, put a qcodes path that does not exist, should fail

# -- cleanup : delete any sources created

# test 2 : list_sync_sources and read_sync_source
# - create 3 sources with the qcodes type, different names, check if they are all returned
# - read one of the sources, check if the returned object matches the input parameters + default values
# - clear all sources

# test 3 : update_sync_source::
# - create a qcodes source + add 10 sync items, half synchronized, half not.
# -- update the name, check if the returned object matches the updated name
# -- update the default scope to the same value as the original --> check if all sync items are not reset.
# -- update the default scope (use random uuid), should fail
# -- update the default scope (get_scopes-> second scope), check if the returned ob
        # + check if all sync items are reset (synchronized = False, attempts = 0, skipped = 0)
# -- update the config data, add a qcodes path that does not exist, should fail
# -- add valid update of the config data, check if the returned object matches the updated config data
# -- remove all sync items
# -- add new sync items
#    + 3 with synchronized = True, attempts = 0
#    + 5 with synchronized = False, attempts = 1
#    + 7 with synchronized = False, attempts = 0
# -- call update_statistics, total should be 15, synchronized should be 3, failed should be 5
#    + check if last_update is updated

# test 4 : delete_sync_source
# - create a qcodes source + add 10 sync items, half synchronized, half not.
# -- delete the source, check if the source is deleted and the associated sync items are deleted
# -- delete the source again, should fail
# -- cleanup : delete any sources created

import pytest, uuid, dataclasses, time, traceback

from pathlib import Path
from typing import Optional
from unittest.mock import patch, MagicMock

from sqlalchemy.orm import Session
from sqlalchemy import delete

from etiket_sync_agent.crud.sync_sources import crud_sync_sources
from etiket_sync_agent.crud.sync_items import crud_sync_items
from etiket_sync_agent.models.enums import SyncSourceTypes, SyncSourceStatus
from etiket_sync_agent.models.sync_sources import SyncSources, SyncSourceErrors
from etiket_sync_agent.models.sync_items import SyncItems
from etiket_sync_agent.backends.qcodes.qcodes_config_class import QCoDeSConfigData
from etiket_sync_agent.exceptions.CRUD.sync_sources import SyncSourceDefaultScopeRequiredError, SyncSourceNameAlreadyExistsError,\
    SyncSourceInvalidDefaultScopeError, SyncSourceConfigDataValidationError, SyncSourceNotFoundError

# Mock scopes
MOCK_SCOPE_1_UUID = uuid.uuid4()
MOCK_SCOPE_2_UUID = uuid.uuid4()
MOCK_SCOPES = [
    MagicMock(uuid=MOCK_SCOPE_1_UUID, name="Scope 1"),
    MagicMock(uuid=MOCK_SCOPE_2_UUID, name="Scope 2"),
]

def mock_get_scopes():
    return MOCK_SCOPES

def mock_get_scope_by_uuid(scope_uuid):
    for scope in MOCK_SCOPES:
        if scope.uuid == scope_uuid:
            return scope
    return None

# Helper to create valid QCoDeS config data for tests
def create_valid_qcodes_config(db_path: Path, setup: str = "TestSetup", extra: Optional[dict] = None) -> dict:
    config = QCoDeSConfigData(
        database_path=db_path,
        set_up=setup,
        extra_attributes=extra or {}
    )
    # Bypass validation during test setup as it checks for existing sources in the *real* DB
    return dataclasses.asdict(config)

# Helper to clean up sources by name
def clear_all_sources(session: Session):
    stmt = delete(SyncItems)
    session.execute(stmt)
    session.commit()
    
    stmt = delete(SyncSourceErrors)
    session.execute(stmt)
    session.commit()
    
    stmt = delete(SyncSources)
    session.execute(stmt)
    session.commit()


# Patch the external scope functions for all tests in this module
@pytest.fixture(autouse=True)
def mock_scope_calls():
    with patch('etiket_sync_agent.crud.sync_sources.get_scope_by_uuid', side_effect=mock_get_scope_by_uuid) as mock_get_by_uuid, \
        patch('etiket_client.python_api.scopes.get_scopes', side_effect=mock_get_scopes) as mock_get_all: # Assuming get_scopes is needed elsewhere or for completeness
        yield mock_get_by_uuid, mock_get_all
class TestSyncSourcesCRUD:
    # --- Test 1: create_sync_source ---
    def test_create_qcodes_source_success(self, db_session: Session, qcodes_set_up: Path):
        source_name = f"test_qcodes_create_success_{uuid.uuid4()}"
        config_data = create_valid_qcodes_config(qcodes_set_up, setup="Setup1", extra={"key": "value"})

        try:
            source = crud_sync_sources.create_sync_source(
                session=db_session,
                name=source_name,
                sync_source_type=SyncSourceTypes.qcodes,
                config_data=config_data,
                default_scope=MOCK_SCOPE_1_UUID
            )
            db_session.refresh(source)

            assert source is not None
            assert source.id is not None
            assert source.name == source_name
            assert source.type == SyncSourceTypes.qcodes
            assert source.status == SyncSourceStatus.SYNCHRONIZING # Default status
            assert source.default_scope == MOCK_SCOPE_1_UUID
            assert source.config_data["database_path"] == str(qcodes_set_up.resolve())
            assert source.config_data["set_up"] == "Setup1"
            assert source.config_data["extra_attributes"] == {"key": "value"}
            assert source.items_total == 0 # Defaults
            assert source.items_synchronized == 0
            assert source.items_failed == 0
            assert source.last_update is not None

        finally:
            clear_all_sources(db_session)

    def test_create_qcodes_source_no_default_scope_fails(self, db_session: Session, qcodes_set_up: Path):
        source_name = f"test_no_scope_fails_{uuid.uuid4()}"
        config_data = create_valid_qcodes_config(qcodes_set_up)
        try:
            with pytest.raises(SyncSourceDefaultScopeRequiredError):
                crud_sync_sources.create_sync_source(
                    session=db_session,
                    name=source_name,
                    sync_source_type=SyncSourceTypes.qcodes,
                    config_data=config_data,
                    default_scope=None # Explicitly None
                )
            count = db_session.query(SyncSources).filter(SyncSources.name == source_name).count()
            assert count == 0
        finally:
            clear_all_sources(db_session)

    @patch('etiket_sync_agent.backends.qcodes.qcodes_config_class.QCoDeSConfigData.validate', return_value=True)
    def test_create_source_duplicate_name_fails(self, mock_validate, db_session: Session, qcodes_set_up: Path):
        source_name = f"test_duplicate_{uuid.uuid4()}"
        config_data = create_valid_qcodes_config(qcodes_set_up)
        try:
            # Create first source
            crud_sync_sources.create_sync_source(session=db_session, name=source_name, sync_source_type=SyncSourceTypes.qcodes,
                                                    config_data=config_data, default_scope=MOCK_SCOPE_1_UUID)
            # Attempt to create second with same name
            with pytest.raises(SyncSourceNameAlreadyExistsError):
                crud_sync_sources.create_sync_source(session=db_session, name=source_name, sync_source_type=SyncSourceTypes.qcodes,
                                                        config_data=config_data, default_scope=MOCK_SCOPE_1_UUID)
        finally:
            # The mock_validate object should not affect cleanup
            clear_all_sources(db_session)

    def test_create_source_invalid_scope_fails(self, db_session: Session, qcodes_set_up: Path):
        source_name = f"test_invalid_scope_{uuid.uuid4()}"
        config_data = create_valid_qcodes_config(qcodes_set_up)
        invalid_scope_uuid = uuid.uuid4()
        with pytest.raises(SyncSourceInvalidDefaultScopeError):
            crud_sync_sources.create_sync_source(db_session, source_name, SyncSourceTypes.qcodes, config_data, invalid_scope_uuid)
        # Ensure no source was actually created
        assert db_session.query(SyncSources).filter(SyncSources.name == source_name).count() == 0


    def test_create_source_invalid_config_fails(self, db_session: Session, qcodes_set_up: Path):
        source_name = f"test_invalid_config_{uuid.uuid4()}"
        config_data = create_valid_qcodes_config(qcodes_set_up)
        config_data["database_path"] = Path("/invalid/path/test.db")
        with pytest.raises(SyncSourceConfigDataValidationError):
            crud_sync_sources.create_sync_source(db_session, source_name, SyncSourceTypes.qcodes, config_data, MOCK_SCOPE_1_UUID)
        assert db_session.query(SyncSources).filter(SyncSources.name == source_name).count() == 0


    # --- Test 2: list_sync_sources and read_sync_source ---
    @patch('etiket_sync_agent.backends.qcodes.qcodes_config_class.QCoDeSConfigData.validate', return_value=True)
    def test_list_and_read_sync_sources(self, mock_validate, db_session: Session, qcodes_set_up: Path):
        source_names = [f"test_list_{i}_{uuid.uuid4()}" for i in range(3)]
        source_ids = []
        config_data = create_valid_qcodes_config(qcodes_set_up)
        try:
            # Create 3 sources
            for i, name in enumerate(source_names):
                scope = MOCK_SCOPE_1_UUID if i % 2 == 0 else MOCK_SCOPE_2_UUID
                source = crud_sync_sources.create_sync_source(db_session, name, SyncSourceTypes.qcodes, config_data, scope)
                source_ids.append(source.id)

            # List sources
            all_sources = crud_sync_sources.list_sync_sources(db_session)
            listed_names = {s.name for s in all_sources if s.name in source_names}
            assert len(listed_names) == 3

            # Read one source
            read_source = crud_sync_sources.read_sync_source(db_session, source_ids[1])
            assert read_source is not None
            assert read_source.id == source_ids[1]
            assert read_source.name == source_names[1]
            assert read_source.type == SyncSourceTypes.qcodes
            assert read_source.default_scope == MOCK_SCOPE_2_UUID # Should be the second scope

        finally:
            # Cleanup
            for name in source_names:
                clear_all_sources(db_session)

    # --- Test 3: update_sync_source ---
    def test_update_sync_source(self, db_session: Session, qcodes_set_up: Path):
        source_name = f"test_update_{uuid.uuid4()}"
        initial_config = create_valid_qcodes_config(qcodes_set_up, setup="InitialSetup")
        source = None
        try:
            # Create initial source
            source = crud_sync_sources.create_sync_source(db_session, source_name, SyncSourceTypes.qcodes, initial_config, MOCK_SCOPE_1_UUID)
            source_id = source.id
            initial_last_update = source.last_update

            # Add some sync items
            items_to_add = []
            for i in range(10):
                items_to_add.append(SyncItems(
                    sync_source_id=source_id,
                    dataIdentifier=f"item_update_{i}",
                    datasetUUID=uuid.uuid4(),
                    syncPriority=float(i),
                    synchronized=(i < 5), # 5 True, 5 False
                    attempts= 1 if 5 <= i < 8 else 0 # 3 have attempts=1
                ))
            db_session.add_all(items_to_add)
            db_session.commit()

            # -- Update name
            updated_name = f"test_updated_name_{uuid.uuid4()}"
            source = crud_sync_sources.update_sync_source(db_session, source_id, name=updated_name)
            assert source.name == updated_name
            assert source.last_update > initial_last_update
            last_update_after_name = source.last_update

            # -- Update default scope to the same value -> items should NOT be reset
            source = crud_sync_sources.update_sync_source(db_session, source_id, default_scope=MOCK_SCOPE_1_UUID)
            assert source.default_scope == MOCK_SCOPE_1_UUID
            items_after_same_scope = db_session.query(SyncItems).filter(SyncItems.sync_source_id == source_id).all()
            assert sum(1 for item in items_after_same_scope if item.synchronized) == 5 # Still 5 synchronized
            assert sum(1 for item in items_after_same_scope if item.attempts > 0) == 3 # Still 3 with attempts > 0


            # -- Update default scope to invalid -> should fail
            invalid_scope_uuid = uuid.uuid4()
            with pytest.raises(SyncSourceInvalidDefaultScopeError):
                crud_sync_sources.update_sync_source(db_session, source_id, default_scope=invalid_scope_uuid)
            db_session.rollback() # Rollback failed transaction
            source = crud_sync_sources.read_sync_source(db_session, source_id) # Re-read source state
            assert source.default_scope == MOCK_SCOPE_1_UUID # Should be unchanged

            # -- Update default scope to different valid scope -> items SHOULD be reset
            source = crud_sync_sources.update_sync_source(db_session, source_id, default_scope=MOCK_SCOPE_2_UUID)
            assert source.default_scope == MOCK_SCOPE_2_UUID
            assert source.last_update > last_update_after_name
            last_update_after_scope = source.last_update
            items_after_new_scope = db_session.query(SyncItems).filter(SyncItems.sync_source_id == source_id).all()
            assert all(not item.synchronized for item in items_after_new_scope) # All False
            assert all(item.attempts == 0 for item in items_after_new_scope) # All 0 attempts


            # -- Update config data (valid)
            updated_config = create_valid_qcodes_config(qcodes_set_up, setup="UpdatedSetup", extra={"new_key": "new_value"})
            source = crud_sync_sources.update_sync_source(db_session, source_id, config_data=updated_config)
            assert source.config_data["set_up"] == "UpdatedSetup"
            assert source.config_data["extra_attributes"] == {"new_key": "new_value"}
            assert source.last_update > last_update_after_scope
            last_update_after_config = source.last_update
            time.sleep(0.01)


            # -- Test update_statistics
            # Clear existing items and add new ones for statistics test
            db_session.query(SyncItems).filter(SyncItems.sync_source_id == source_id).delete()
            db_session.commit() # Commit deletion before adding new items
            db_session.expunge_all() # Clear the session's identity map
            
            items_for_stats = []
            # + 3 with synchronized = True, attempts = 0
            for i in range(3): items_for_stats.append(SyncItems(sync_source_id=source_id, dataIdentifier=f"stat_syncT_{i}", datasetUUID=uuid.uuid4(),
                                                                syncPriority=1.0, synchronized=True, attempts=0))
            # + 5 with synchronized = False, attempts = 1
            for i in range(5): items_for_stats.append(SyncItems(sync_source_id=source_id, dataIdentifier=f"stat_syncF_att1_{i}",
                                                                datasetUUID=uuid.uuid4(), syncPriority=1.0, synchronized=False, attempts=1))
            # + 7 with synchronized = False, attempts = 0
            for i in range(7): items_for_stats.append(SyncItems(sync_source_id=source_id, dataIdentifier=f"stat_syncF_att0_{i}",
                                                                datasetUUID=uuid.uuid4(), syncPriority=1.0, synchronized=False, attempts=0))
            
            db_session.add_all(items_for_stats)
            db_session.commit()

            # Call update with statistics flag
            source = crud_sync_sources.update_sync_source(db_session, source_id, update_statistics=True)
            assert source.items_total == 15
            assert source.items_synchronized == 3
            assert source.items_failed == 5 # attempts > 0 and synchronized = False
            assert source.last_update > last_update_after_config

        finally:
            clear_all_sources(db_session)


    def test_update_source_invalid_config_fails(self, db_session: Session, qcodes_set_up: Path):
        source_name = f"test_update_invalid_cfg_{uuid.uuid4()}"
        source = None
        try:
            # Create source first (without patch, so it succeeds)
            source = crud_sync_sources.create_sync_source(db_session, source_name, SyncSourceTypes.qcodes, create_valid_qcodes_config(qcodes_set_up), MOCK_SCOPE_1_UUID)
            source_id = source.id
            initial_config_dict = source.config_data

            invalid_config_update = create_valid_qcodes_config(qcodes_set_up, setup="InvalidUpdate") # Data ok, validation mocked to fail

            # Apply patch only for the update operation
            with patch('etiket_sync_agent.backends.qcodes.qcodes_config_class.QCoDeSConfigData.validate', side_effect=ValueError("Mock config validation error")):
                with pytest.raises(SyncSourceConfigDataValidationError):
                    crud_sync_sources.update_sync_source(db_session, source_id, config_data=invalid_config_update)

            db_session.rollback() # Rollback failed transaction
            source = crud_sync_sources.read_sync_source(db_session, source_id) # Re-read source state
            assert source.config_data == initial_config_dict # Config should be unchanged

        finally:
            if source:
                clear_all_sources(db_session)


    # --- Test 4: delete_sync_source ---
    def test_delete_sync_source(self, db_session: Session, qcodes_set_up: Path):
        source_name = f"test_delete_{uuid.uuid4()}"
        source = None
        try:
            # Create source and items
            source = crud_sync_sources.create_sync_source(db_session, source_name, SyncSourceTypes.qcodes, create_valid_qcodes_config(qcodes_set_up), MOCK_SCOPE_1_UUID)
            source_id = source.id
            items_to_add = [SyncItems(sync_source_id=source_id, dataIdentifier=f"item_del_{i}", datasetUUID=uuid.uuid4(), syncPriority=1.0) for i in range(5)]
            crud_sync_items.create_sync_items(db_session, source_id, items_to_add)
            
            assert db_session.query(SyncItems).filter(SyncItems.sync_source_id == source_id).count() == 5

            crud_sync_sources.delete_sync_source(db_session, source_id)
            
            # Verify source is deleted
            deleted_source = db_session.query(SyncSources).filter(SyncSources.id == source_id).first()
            assert deleted_source is None
            
            # Verify that associated sync items are deleted
            assert db_session.query(SyncItems).filter(SyncItems.sync_source_id == source_id).count() == 0

            with pytest.raises(SyncSourceNotFoundError):
                crud_sync_sources.delete_sync_source(db_session, source_id)

        finally:
            clear_all_sources(db_session)


class TestSyncSourceErrors:
    def setup_method(self):
        self.source_name_1 = f"test_logs_source_1_{uuid.uuid4()}"
        self.source_name_2 = f"test_logs_source_2_{uuid.uuid4()}"      

    def test_add_and_read_single_log(self, db_session: Session, qcodes_set_up: Path):
        """
        Test adding a single log to a sync source and reading it back.
        """
        try:
            # Create a source to associate logs with
            source_1 = crud_sync_sources.create_sync_source(db_session, self.source_name_1, SyncSourceTypes.qcodes, create_valid_qcodes_config(qcodes_set_up), MOCK_SCOPE_1_UUID)
            source_id = source_1.id
            
            error = ValueError("The value was too large")
            crud_sync_sources.add_sync_source_error(db_session, source_id, 1, error)
            
            logs = crud_sync_sources.read_sync_source_errors(db_session, source_id)
            
            assert len(logs) == 1
            assert logs[0].log_exception == repr(error)
            assert logs[0].log_context is None
            assert logs[0].log_traceback is None
            assert logs[0].sync_source_id == source_id
        finally:
            clear_all_sources(db_session)

    def test_add_log_with_stacktrace(self, db_session: Session, qcodes_set_up: Path):
        """
        Test adding a log with a stacktrace and verifying it is stored correctly.
        """
        try:
            source_1 = crud_sync_sources.create_sync_source(db_session, self.source_name_1, SyncSourceTypes.qcodes, create_valid_qcodes_config(qcodes_set_up), MOCK_SCOPE_1_UUID)
            source_id = source_1.id
            
            traceback_str = None
            context = "This is a test context"
            error = None
            try:
                8/0
            except Exception as e:
                error = e
                traceback_str = traceback.format_exc()
            
            if error is None:
                raise ValueError()
                        
            crud_sync_sources.add_sync_source_error(db_session, source_id, 1, error,
                                                    log_context=context,
                                                    log_traceback=traceback_str)
            
            # ensure the log is on the top.
            time.sleep(0.001)
            # Add another simple log to ensure we get the correct one
            crud_sync_sources.add_sync_source_error(db_session, source_id, 2, error,
                                                    log_context=context,
                                                    log_traceback=traceback_str)
            
            
            logs = crud_sync_sources.read_sync_source_errors(db_session, source_id, limit=2)
            
            # Logs are ordered by timestamp descending, so the last one added is first
            assert len(logs) == 2
            assert logs[0].sync_iteration == 2
            assert logs[1].sync_iteration == 1
            assert logs[1].log_exception == repr(error)
            assert logs[1].log_context == context
            assert logs[1].log_traceback == traceback_str
            
        finally:
            clear_all_sources(db_session)

    def test_log_pagination_limit_and_offset(self, db_session: Session, qcodes_set_up: Path):
        """
        Test the limit and offset functionality for reading logs from multiple sources.
        """
        try:
            # Create two sources
            source_1 = crud_sync_sources.create_sync_source(db_session, self.source_name_1, SyncSourceTypes.qcodes, create_valid_qcodes_config(qcodes_set_up, setup="Setup1"), MOCK_SCOPE_1_UUID)
            source_1_id = source_1.id

            # Add 5 logs to source 1
            errors_s1 = [ValueError(f"S1-Log-{i}") for i in range(5)]
            i = 0
            for error in errors_s1:
                crud_sync_sources.add_sync_source_error(db_session, source_1_id, i, error)
                i += 1
                time.sleep(0.001) # Ensure distinct timestamps

            # Test Source 1 logs
            # Test limit: get first 2 logs for source 1 (most recent)
            logs_s1_limit2 = crud_sync_sources.read_sync_source_errors(db_session, source_1_id, limit=2)
            assert len(logs_s1_limit2) == 2
            assert logs_s1_limit2[0].log_exception == "ValueError('S1-Log-4')" # Most recent
            assert logs_s1_limit2[1].log_exception == "ValueError('S1-Log-3')"

            # Test offset: get logs starting from the 3rd one (index 2)
            logs_s1_offset2 = crud_sync_sources.read_sync_source_errors(db_session, source_1_id, skip=2, limit=2)
            assert len(logs_s1_offset2) == 2
            assert logs_s1_offset2[0].log_exception == "ValueError('S1-Log-2')"
            assert logs_s1_offset2[1].log_exception == "ValueError('S1-Log-1')"
            
            # Test offset reaching the end
            logs_s1_offset4 = crud_sync_sources.read_sync_source_errors(db_session, source_1_id, skip=4, limit=2)
            assert len(logs_s1_offset4) == 1
            assert logs_s1_offset4[0].log_exception == "ValueError('S1-Log-0')"

            # Test that logs from source 2 are not mixed in
            all_logs_s1 = crud_sync_sources.read_sync_source_errors(db_session, source_1_id, limit=10)
            assert len(all_logs_s1) == 5
            assert all(log.log_exception.startswith("ValueError('S1-Log-") for log in all_logs_s1)
            
            
            crud_sync_sources.delete_sync_source(db_session, source_1_id)
            all_logs_s1 = crud_sync_sources.read_sync_source_errors(db_session, source_1_id, limit=10)
            assert len(all_logs_s1) == 0

        finally:
            clear_all_sources(db_session)