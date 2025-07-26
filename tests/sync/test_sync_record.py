import pytest
import uuid

from pathlib import Path
from datetime import datetime, timedelta, timezone

from etiket_sync_agent.models.sync_items import SyncItems
from etiket_sync_agent.sync.sync_records.manager import SyncRecordManager
import etiket_sync_agent.sync.sync_records.models as dsrc

@pytest.fixture
def sync_item():
    """Provides a basic SyncItems instance for tests."""
    return SyncItems(
        id=1,
        sync_source_id=1,
        dataIdentifier="test_identifier",
        datasetUUID=uuid.uuid4(),
        syncPriority=1.0,
        synchronized=False,
        attempts=0,
        sync_record=None,
        error=None,
        traceback=None
    )

def test_add_logs(sync_item):
    """Test adding multiple logs to the record."""
    record = SyncRecordManager(sync_item)
    record.add_log("First log")
    record.add_log("Second log")
    record.add_log("Third log")

    assert len(record.record.logs) == 3
    assert all(isinstance(item, dsrc.LogEntry) for item in record.record.logs)

def test_add_log_and_error(sync_item):
    """Test adding both a log and an error to the record."""
    record = SyncRecordManager(sync_item)
    record.add_log("An informational log.")
    record.add_error("A test error.", "Traceback here")

    assert len(record.record.logs) == 2
    assert isinstance(record.record.logs[0], dsrc.LogEntry)
    assert isinstance(record.record.logs[1], dsrc.ErrorEntry)

def test_simple_task(sync_item):
    """Test logging within a single task."""
    record = SyncRecordManager(sync_item)
    with record.task("simple_task"):
        record.add_log("Log inside task")
        record.add_log("Another log inside task")

    assert len(record.record.logs) == 1
    task = record.record.logs[0]
    assert isinstance(task, dsrc.TaskEntry)
    assert task.name == "simple_task"
    assert len(task.content) == 2
    assert all(isinstance(item, dsrc.LogEntry) for item in task.content)
    assert task.has_errors is False

def test_task_with_error(sync_item):
    """Test that an error raised within a task is caught and logged."""
    record = SyncRecordManager(sync_item)
    with pytest.raises(ValueError, match="test error"):
        with record.task("task_with_error"):
            record.add_log("Log before error")
            raise ValueError("test error")

    task = record.record.logs[0]
    assert isinstance(task, dsrc.TaskEntry)
    assert task.name == "task_with_error"
    assert task.has_errors is True
    assert len(task.content) == 2 # LogEntry and ErrorEntry
    assert isinstance(task.content[0], dsrc.LogEntry)
    assert isinstance(task.content[1], dsrc.ErrorEntry)
    assert task.content[1].message == "test error"

def test_sequential_tasks(sync_item):
    """Test that sequential tasks are logged correctly at the same level."""
    record = SyncRecordManager(sync_item)
    with record.task("task1"):
        record.add_log("Log for task 1")
    with record.task("task2"):
        record.add_log("Log for task 2")

    assert len(record.record.logs) == 2
    task1, task2 = record.record.logs
    assert isinstance(task1, dsrc.TaskEntry)
    assert task1.name == "task1"
    assert len(task1.content) == 1
    assert isinstance(task2, dsrc.TaskEntry)
    assert task2.name == "task2"
    assert len(task2.content) == 1

def test_nested_tasks(sync_item):
    """Test that nested tasks are logged correctly."""
    record = SyncRecordManager(sync_item)
    with record.task("parent_task"):
        record.add_log("Parent log before")
        with record.task("child_task"):
            record.add_log("Child log 1")
            record.add_log("Child log 2")
        record.add_log("Parent log after")

    assert len(record.record.logs) == 1
    parent_task = record.record.logs[0]
    assert isinstance(parent_task, dsrc.TaskEntry)
    assert parent_task.name == "parent_task"
    assert len(parent_task.content) == 3
    assert isinstance(parent_task.content[0], dsrc.LogEntry) # parent log before
    assert isinstance(parent_task.content[1], dsrc.TaskEntry) # child task
    assert isinstance(parent_task.content[2], dsrc.LogEntry) # parent log after

    child_task = parent_task.content[1]
    assert child_task.name == "child_task"
    assert len(child_task.content) == 2
    assert not child_task.has_errors

def test_nested_task_with_error(sync_item):
    """Test that an error in a nested task propagates the error status upwards."""
    record = SyncRecordManager(sync_item)
    with pytest.raises(ValueError, match="nested error"):
        with record.task("parent_task"):
            with record.task("child_task"):
                raise ValueError("nested error")
            record.add_log("This should not be logged")

    parent_task = record.record.logs[0]
    assert isinstance(parent_task, dsrc.TaskEntry)
    assert parent_task.name == "parent_task"
    assert parent_task.has_errors is True
    assert len(parent_task.content) == 1 
    
    child_task = parent_task.content[0]
    assert isinstance(child_task, dsrc.TaskEntry)
    assert child_task.name == "child_task"
    assert child_task.has_errors is True
    assert len(child_task.content) == 1
    assert isinstance(child_task.content[0], dsrc.ErrorEntry)

def test_json_serialization_deserialization(sync_item):
    """Test that a complex record can be serialized to JSON and back."""
    record = SyncRecordManager(sync_item, dataset_path=Path("/tmp/test"))
    
    # Build a complex record
    record.add_log("A simple log")
    with record.task("main_task"):
        record.add_upload_task("file1.txt")
        with record.task("sub_task"):
            record.add_log("sub_task log")
            record.add_error("sub_task_error", "trace")
    
    # Serialize
    record_dict = record.to_dict()
    assert isinstance(record_dict, dict)
    
    # Deserialize
    new_sync_item = SyncItems(
        id=sync_item.id,
        sync_source_id=sync_item.sync_source_id,
        dataIdentifier=sync_item.dataIdentifier,
        datasetUUID=sync_item.datasetUUID,
        sync_record=record_dict
    )
    new_record = SyncRecordManager(new_sync_item, dataset_path=Path("/tmp/test"))

    # Compare
    assert record.record == new_record.record

def test_from_json_empty_record(sync_item):
    """Test that deserializing an empty record works."""
    # Create a record, convert to json, then back
    record = SyncRecordManager(sync_item)
    record_dict = record.to_dict()
    
    assert record_dict['files'] == {}
    assert record_dict['logs'] == []
    
    sync_item.sync_record = record_dict

    new_record = SyncRecordManager(sync_item)
    
    assert len(new_record.record.logs) == 0
    
def test_file_upload_info_and_logs(sync_item):
    """
    Test:
    - make a file for a file upload --> add the fil version/uuid/other info, check if it is present in the record.
    - also check if in the logs, the task is registered correctly (with errors).
    """
    record = SyncRecordManager(sync_item)
    file_name = "test_file.txt"
    with record.add_upload_task(file_name) as file_upload_info:
        file_upload_info.file_uuid = uuid.uuid4()
        file_upload_info.version_id = 1
        file_upload_info.status = dsrc.FileStatus.OK

    assert file_name in record.record.files
    assert len(record.record.files[file_name]) == 1
    uploaded_file_info = record.record.files[file_name][0]
    assert uploaded_file_info.file_uuid == file_upload_info.file_uuid
    assert uploaded_file_info.version_id == 1
    assert uploaded_file_info.status == dsrc.FileStatus.OK

    assert len(record.record.logs) == 1
    task = record.record.logs[0]
    assert isinstance(task, dsrc.TaskEntry)
    assert task.name == f"upload {file_name}"
    assert not task.has_errors

def test_file_upload_with_error(sync_item):
    """
    Test:
    - make a file for a file upload --> fill information partially and call an error --> check if the error is present in the record.
    - also check if in the logs, the task is registered correctly (with errors).
    """
    record = SyncRecordManager(sync_item)
    file_name = "error_file.txt"
    with pytest.raises(ValueError, match="upload failed"):
        with record.add_upload_task(file_name) as file_upload_info:
            file_upload_info.file_uuid = uuid.uuid4()
            raise ValueError("upload failed")

    assert file_name in record.record.files
    assert len(record.record.files[file_name]) == 1
    uploaded_file_info = record.record.files[file_name][0]
    assert uploaded_file_info.status == dsrc.FileStatus.ERROR
    assert uploaded_file_info.error == "upload failed"

    assert len(record.record.logs) == 1
    task = record.record.logs[0]
    assert isinstance(task, dsrc.TaskEntry)
    assert task.name == f"upload {file_name}"
    assert task.has_errors
    assert len(task.content) == 1
    error_entry = task.content[0]
    assert isinstance(error_entry, dsrc.ErrorEntry)
    assert "upload failed" in error_entry.message


def test_multiple_file_uploads(sync_item):
    """
    Test:
    - make a file upload twice, check that there are two entries in the file record --> check if their content is as expected.
    """
    record = SyncRecordManager(sync_item)
    file_name = "multiple_uploads.txt"

    # First upload
    with record.add_upload_task(file_name) as file_upload_info_1:
        file_upload_info_1.file_uuid = uuid.uuid4()
        file_upload_info_1.version_id = 1
        file_upload_info_1.status = dsrc.FileStatus.OK

    # Second upload
    with record.add_upload_task(file_name) as file_upload_info_2:
        file_upload_info_2.file_uuid = uuid.uuid4()
        file_upload_info_2.version_id = 2
        file_upload_info_2.status = dsrc.FileStatus.OK
    
    assert file_name in record.record.files
    assert len(record.record.files[file_name]) == 2
    
    first_record, second_record = record.record.files[file_name]
    assert first_record.version_id == 1
    assert second_record.version_id == 2
    assert first_record.file_uuid != second_record.file_uuid

def dummy_converter(a, b):
    pass

def test_file_upload_with_converter(sync_item):
    """
    Test:
    - make a file for a file upload --> add a converter
    - afterwards also the file info.
    """
    record = SyncRecordManager(sync_item)
    file_name = "converted_file.zarr"
    
    with record.add_upload_task(file_name) as file_upload_info:
        file_upload_info.status = dsrc.FileStatus.OK
        with record.define_converter(file_upload_info, dummy_converter) as converter_info:
            converter_info.status = dsrc.FileStatus.OK

    assert file_name in record.record.files
    uploaded_file_info = record.record.files[file_name][0]
    assert uploaded_file_info.converter is not None
    converter = uploaded_file_info.converter
    assert converter.method == dummy_converter.__module__ + "." + dummy_converter.__qualname__
    assert converter.status == dsrc.FileStatus.OK
    assert converter.error is None
    
    assert len(record.record.logs) == 1
    upload_task = record.record.logs[0]
    assert isinstance(upload_task, dsrc.TaskEntry)
    assert upload_task.name == f"upload {file_name}"
    assert not upload_task.has_errors
    assert len(upload_task.content) == 1 # converter task
    converter_task = upload_task.content[0]
    assert isinstance(converter_task, dsrc.TaskEntry)
    assert converter_task.name == f"convert {dummy_converter.__module__}.{dummy_converter.__qualname__} called"
    assert not converter_task.has_errors

def test_file_upload_with_converter_error(sync_item):
    """
    Test:
    - make a file for a file upload --> add a converter
    - error happens in the convertion step --> check if the error is present in the file record.
    """
    record = SyncRecordManager(sync_item)
    file_name = "converter_error.zarr"
    
    with pytest.raises(dsrc.DataConvertorException):
        with record.add_upload_task(file_name) as file_upload_info:
            file_upload_info.status = dsrc.FileStatus.OK
            with record.define_converter(file_upload_info, dummy_converter) as converter_info:
                raise ValueError("Conversion failed")

    uploaded_file_info = record.record.files[file_name][0]
    assert uploaded_file_info.converter is not None
    converter = uploaded_file_info.converter
    assert converter.status == dsrc.FileStatus.ERROR
    assert "Conversion failed" in str(converter.error)

    upload_task = record.record.logs[0]
    assert isinstance(upload_task, dsrc.TaskEntry)
    assert upload_task.has_errors
    assert len(upload_task.content) == 1 # converter task
    converter_task = upload_task.content[0]
    assert isinstance(converter_task, dsrc.TaskEntry)
    assert converter_task.has_errors
    error_entry = converter_task.content[0]
    assert isinstance(error_entry, dsrc.ErrorEntry)
    assert "Conversion failed" in error_entry.message

def test_file_upload_error_after_converter(sync_item):
    """
    Test:
    - make a file for a file upload --> add a converter
    - error happens in the upload step (after the converter) --> check if the error is present in the file record.
    - check also if the logs are registered correctly
    """
    record = SyncRecordManager(sync_item)
    file_name = "upload_error_after_converter.zarr"

    with pytest.raises(ValueError, match="Upload failed after conversion"):
        with record.add_upload_task(file_name) as file_upload_info:
            with record.define_converter(file_upload_info, dummy_converter) as converter_info:
                converter_info.status = dsrc.FileStatus.OK
            record.add_log("Converter finished, starting upload.")
            raise ValueError("Upload failed after conversion")

    uploaded_file_info = record.record.files[file_name][0]
    assert uploaded_file_info.status == dsrc.FileStatus.ERROR
    assert "Upload failed after conversion" in str(uploaded_file_info.error)

    upload_task = record.record.logs[0]
    assert isinstance(upload_task, dsrc.TaskEntry)
    assert upload_task.has_errors
    
    assert isinstance(upload_task.content[0], dsrc.TaskEntry) # converter task
    assert not upload_task.content[0].has_errors
    assert isinstance(upload_task.content[1], dsrc.LogEntry)
    assert isinstance(upload_task.content[2], dsrc.ErrorEntry)

'''
Tests for the file upload info.

- make a file for a file upload --> add the fil version/uuid/other info, check if it is present in the record.
                                --> also check if in the logs, the task is registered correctly (with errors).
- make a file for a file upload --> fill information partially and call an error --> check if the error is present in the record.
                                --> also check if in the logs, the task is registered correctly (with errors).
- make a file upload twice, check that there are two entries in the file record --> check if their content is as expected.
- make a file for a file upload --> add a converter (e.g. etiket_sync_agent.backends.filebase.converters.zarr_to_netcdf.ZarrToNetcdfConverter)
                                --> afterwards also the file info.
- make a file for a file upload --> add a converter (e.g. etiket_sync_agent.backends.filebase.converters.zarr_to_netcdf.ZarrToNetcdfConverter)
                                --> error happens in the convertion step --> check if the error is present in the file record.
- make a file for a file upload --> add a converter (e.g. etiket_sync_agent.backends.filebase.converters.zarr_to_netcdf.ZarrToNetcdfConverter)
                                --> error happens in the upload step (after the converter) --> check if the error is present in the file record.
                                --> check also if the logs are registered correctly (add a few logs in btween, a task should be present for upload and converter)
'''

def netcdf4_md5_checksum(file_path : Path) -> None:
    """
    Calculate the MD5 checksum of a NetCDF4 file.
    """
    pass
    

def test_file_upload_and_record_generation(sync_item):
    """
    Test:
    - make a file for a file upload --> add a converter
    - add a few logs in between, check if the logs are registered correctly
    """
    record = SyncRecordManager(sync_item)
    file_name = "converter_with_logs.zarr"
    
    file_uuid = uuid.uuid4()
    file_m_time = datetime.now(timezone.utc)
    
    with record.add_upload_task(file_name) as file_upload_info:
        file_upload_info.file_path = Path("/tmp/test_file.zarr")
        file_upload_info.file_uuid = file_uuid
        file_upload_info.file_m_time = file_m_time
        file_upload_info.version_id = 1
        file_upload_info.md5 = "1234567890"
        file_upload_info.size = 1000
        file_upload_info.add_content_aware_checksum(netcdf4_md5_checksum, "1234567890")
        
        record.add_log("Upload finished")
        record.add_log("Record generation started")
    

    assert file_name in record.record.files
    uploaded_file_info = record.record.files[file_name][0]
    assert uploaded_file_info.filename == file_name
    
    assert uploaded_file_info.file_path == Path("/tmp/test_file.zarr")
    assert uploaded_file_info.file_m_time == file_m_time
    
    assert uploaded_file_info.file_uuid == file_uuid
    assert uploaded_file_info.version_id == 1
    
    # current with 5 sec of current timestamp
    assert abs(uploaded_file_info.sync_timestamp - datetime.now(timezone.utc)) < timedelta(seconds=5)
    assert uploaded_file_info.md5 == "1234567890"
    assert uploaded_file_info.size == 1000
    assert uploaded_file_info.content_aware_checksum is not None
    assert uploaded_file_info.content_aware_checksum.method == netcdf4_md5_checksum.__module__ + "." + netcdf4_md5_checksum.__qualname__
    assert uploaded_file_info.content_aware_checksum.value == "1234567890"
    assert uploaded_file_info.status == dsrc.FileStatus.OK
    assert uploaded_file_info.converter is None
    assert uploaded_file_info.error is None
    
    upload_task = record.record.logs[0]
    assert isinstance(upload_task, dsrc.TaskEntry)
    assert upload_task.name == f"upload {file_name}"
    assert not upload_task.has_errors
    assert len(upload_task.content) == 2
    assert isinstance(upload_task.content[0], dsrc.LogEntry)
    assert isinstance(upload_task.content[1], dsrc.LogEntry)
    assert upload_task.content[0].message == "Upload finished"
    assert upload_task.content[1].message == "Record generation started"
    
def test_file_upload_and_record_generation_failed(sync_item):
    """
    Test:
    - make a file for a file upload --> add a converter
    - add a few logs in between, check if the logs are registered correctly
    """
    record = SyncRecordManager(sync_item)
    file_name = "converter_with_logs.zarr"
    
    file_uuid = uuid.uuid4()
    file_m_time = datetime.now(timezone.utc)
    
    with pytest.raises(ValueError, match="Upload failed"):
        with record.add_upload_task(file_name) as file_upload_info:
            file_upload_info.file_path = Path("/tmp/test_file.zarr")
            file_upload_info.file_uuid = file_uuid
            file_upload_info.file_m_time = file_m_time
            file_upload_info.version_id = 1
            file_upload_info.md5 = "1234567890"
            file_upload_info.size = 1000
            file_upload_info.add_content_aware_checksum(netcdf4_md5_checksum, "1234567890")
            raise ValueError("Upload failed")
    
    uploaded_file_info = record.record.files[file_name][0]
    assert uploaded_file_info.filename == file_name
    assert uploaded_file_info.file_path == Path("/tmp/test_file.zarr")
    assert uploaded_file_info.file_m_time == file_m_time
    assert uploaded_file_info.file_uuid == file_uuid
    assert abs(uploaded_file_info.sync_timestamp - datetime.now(timezone.utc)) < timedelta(seconds=5)
    assert uploaded_file_info.version_id == 1
    assert uploaded_file_info.md5 == "1234567890"
    assert uploaded_file_info.size == 1000
    assert uploaded_file_info.status == dsrc.FileStatus.ERROR
    assert uploaded_file_info.error is not None
    assert "Upload failed" in str(uploaded_file_info.error)

def test_file_upload_first_failed_then_ok(sync_item):
    """
    Test:
    - make a file for a file upload --> add a converter
    - add a few logs in between, check if the logs are registered correctly
    """
    record = SyncRecordManager(sync_item)
    file_name = "converter_with_logs.zarr"
    
    file_uuid = uuid.uuid4()
    file_m_time = datetime.now(timezone.utc)
    
    with pytest.raises(ValueError, match="Upload failed"):
        with record.add_upload_task(file_name) as file_upload_info:
            file_upload_info.file_path = Path("/tmp/test_file.zarr")
            file_upload_info.file_uuid = file_uuid
            file_upload_info.file_m_time = file_m_time
            file_upload_info.version_id = 1
            file_upload_info.md5 = "1234567890"
            file_upload_info.size = 1000
            raise ValueError("Upload failed")
    
    with record.add_upload_task(file_name) as file_upload_info:
        file_upload_info.file_path = Path("/tmp/test_file.zarr")
        file_upload_info.file_uuid = file_uuid
        file_upload_info.file_m_time = file_m_time
        file_upload_info.version_id = 1
        file_upload_info.md5 = "1234567890"
        file_upload_info.size = 1000
        file_upload_info.add_content_aware_checksum(netcdf4_md5_checksum, "1234567890")
    
    assert file_name in record.record.files
    assert len(record.record.files[file_name]) == 2
    uploaded_file_info_1 = record.record.files[file_name][0]
    assert uploaded_file_info_1.filename == file_name
    assert uploaded_file_info_1.file_path == Path("/tmp/test_file.zarr")
    assert uploaded_file_info_1.file_m_time == file_m_time
    assert uploaded_file_info_1.file_uuid == file_uuid
    assert abs(uploaded_file_info_1.sync_timestamp - datetime.now(timezone.utc)) < timedelta(seconds=5)
    assert uploaded_file_info_1.version_id == 1
    assert uploaded_file_info_1.status == dsrc.FileStatus.ERROR
    assert uploaded_file_info_1.converter is None
    assert uploaded_file_info_1.error is not None
    assert "Upload failed" in str(uploaded_file_info_1.error)
    
    uploaded_file_info_2 = record.record.files[file_name][1]
    assert uploaded_file_info_2.filename == file_name
    assert uploaded_file_info_2.file_path == Path("/tmp/test_file.zarr")
    assert uploaded_file_info_2.file_m_time == file_m_time
    assert uploaded_file_info_2.file_uuid == file_uuid
    assert abs(uploaded_file_info_2.sync_timestamp - datetime.now(timezone.utc)) < timedelta(seconds=5)
    assert uploaded_file_info_2.sync_timestamp > uploaded_file_info_1.sync_timestamp
    assert uploaded_file_info_2.version_id == 1
    assert uploaded_file_info_2.status == dsrc.FileStatus.OK
    assert uploaded_file_info_2.converter is None
    assert uploaded_file_info_2.error is None
    
def test_file_upload_and_record_generation_with_converter(sync_item):
    """
    Test:
    - make a file for a file upload --> add a converter
    - add a few logs in between, check if the logs are registered correctly
    """
    record = SyncRecordManager(sync_item)
    file_name = "converter_with_logs.zarr"
    
    file_uuid = uuid.uuid4()
    file_m_time = datetime.now(timezone.utc)
    
    with record.add_upload_task(file_name) as file_upload_info:
        file_upload_info.file_path = Path("/tmp/test_file.zarr")
        file_upload_info.file_uuid = file_uuid
        file_upload_info.file_m_time = file_m_time
        file_upload_info.version_id = 1
        file_upload_info.md5 = "1234567890"
        with record.define_converter(file_upload_info, dummy_converter) as converter_info:
            converter_info.status = dsrc.FileStatus.OK
            record.add_log("Converter finished")
            record.add_log("Record generation started")
    
    assert file_name in record.record.files
    uploaded_file_info = record.record.files[file_name][0]
    assert uploaded_file_info.filename == file_name
    assert uploaded_file_info.file_path == Path("/tmp/test_file.zarr")
    assert uploaded_file_info.file_m_time == file_m_time
    assert uploaded_file_info.file_uuid == file_uuid
    assert abs(uploaded_file_info.sync_timestamp - datetime.now(timezone.utc)) < timedelta(seconds=5)
    assert uploaded_file_info.status == dsrc.FileStatus.OK
    
    assert uploaded_file_info.converter is not None
    converter = uploaded_file_info.converter
    assert converter.method == dummy_converter.__module__ + "." + dummy_converter.__qualname__
    assert converter.status == dsrc.FileStatus.OK
    assert converter.error is None
    
def test_file_upload_and_record_generation_with_converter_error(sync_item):
    """
    Test:
    - make a file for a file upload --> add a converter
    - add a few logs in between, check if the logs are registered correctly
    """
    record = SyncRecordManager(sync_item)
    file_name = "converter_with_logs.zarr"
    
    file_uuid = uuid.uuid4()
    file_m_time = datetime.now(timezone.utc)
    
    with pytest.raises(dsrc.DataConvertorException):
        with record.add_upload_task(file_name) as file_upload_info:
            file_upload_info.file_path = Path("/tmp/test_file.zarr")
            file_upload_info.file_uuid = file_uuid
            file_upload_info.file_m_time = file_m_time
            file_upload_info.version_id = 1
            with record.define_converter(file_upload_info, dummy_converter) as converter_info:
                raise ValueError("Conversion failed")
    
    uploaded_file_info = record.record.files[file_name][0]
    assert uploaded_file_info.filename == file_name
    assert uploaded_file_info.file_path == Path("/tmp/test_file.zarr")
    assert uploaded_file_info.file_m_time == file_m_time
    assert uploaded_file_info.file_uuid == file_uuid
    assert abs(uploaded_file_info.sync_timestamp - datetime.now(timezone.utc)) < timedelta(seconds=5)
    assert uploaded_file_info.version_id == 1
    assert uploaded_file_info.status == dsrc.FileStatus.ERROR
    assert uploaded_file_info.converter is not None
    assert uploaded_file_info.converter.status == dsrc.FileStatus.ERROR
    assert uploaded_file_info.converter.error is not None
    assert "Conversion failed" in str(uploaded_file_info.converter.error)
    
    upload_task = record.record.logs[0]
    assert isinstance(upload_task, dsrc.TaskEntry)
    assert upload_task.has_errors
