import datetime

from typing import Optional, Callable
from datetime import timezone
from contextlib import contextmanager
from pathlib import Path

from etiket_sync_agent.sync.sync_records.models import FileUploadInfo, DataConvertor, FileStatus,\
    DataConvertorException, TaskEntry, LogEntry, ErrorEntry, DatasetSyncRecordData
from etiket_sync_agent.models.sync_items import SyncItems

def utc_now_iso():
    return datetime.datetime.now(timezone.utc).isoformat()

class SyncRecordManager:
    def __init__(self, sync_item : SyncItems, dataset_path : Optional[Path] = None):
        """
        Initialize the SyncRecordManager object.

        Args:
            sync_item (SyncItems): The sync item object.
            dataset_path (Optional[Path]): The path of the dataset (only needed for file base sync).
        """
        self.dataset_path = dataset_path

        if sync_item.sync_record is not None:
            self.record = DatasetSyncRecordData.from_dict(sync_item.sync_record)
        else:
            self.record = DatasetSyncRecordData(dataset_sync_path=dataset_path)
        self.__current_log = self.record.logs

    def to_dict(self):
        return self.record.to_dict()
    
    @contextmanager
    def task(self, task_name : str):
        '''
        Add a task to the sync record.
        
        Args:
            task_name (str): The name of the task.
        
        To be called like:
        with sync_record.task("task_name"):
            # do something
            # if an error is raised, it will be added to the manifest automatically.
        '''
        task = TaskEntry(name=task_name)
        old_item = self.__current_log
        self.record.add_log_item(old_item, task)
        self.__current_log = task
        
        try:
            yield
        except Exception as e:
            if not error_in_children(e, task):
                # do not propagate the stacktrace to all levels.
                self.add_error(str(e), e)
            else:
                # if child task fails, parent also shows the error.
                task.has_errors = True
            raise e

        finally:
            self.__current_log = old_item
    
    def has_errors(self) -> bool:
        '''
        Check if the sync record has errors.
        '''
        for item in self.record.logs:
            if isinstance(item, TaskEntry):
                if item.has_errors:
                    return True
        return False
    
    def add_error(self, message : str, error : Exception | str):
        '''
        Add an error to the logs of the sync record.
        
        Args:
            error: The error to add.
        '''
        error_entry = ErrorEntry(message=message, stacktrace=str(error))
        self.record.add_log_item(self.__current_log, error_entry)
        if isinstance(self.__current_log, TaskEntry):
            self.__current_log.has_errors = True
    
    def add_log(self, log : str):
        '''
        Add a log to the logs of the sync record.
        '''
        log_entry = LogEntry(message=log)
        self.record.add_log_item(self.__current_log, log_entry)
        
    @contextmanager
    def add_upload_task(self, file_name : str):
        '''
        Declare a file upload.
        
        Args:
            file_name (str): The name of the file.
        
        Example usage:
        with sync_record.add_upload_task("file_name") as file_upload_info:
            set vars :
                file_upload_info.file_path = Path("path/to/file")
                ...
            # if an error is raised, it will be added to the sync record automatically.
        
        '''
        file_upload_info = FileUploadInfo(filename = file_name)
        self.record.files[file_name] = self.record.files.get(file_name, [])
        self.record.files[file_name].append(file_upload_info)
        
        with self.task(f"upload {file_name}"):
            try:
                yield file_upload_info
            except DataConvertorException as e:
                file_upload_info.status = FileStatus.ERROR
                raise e
            except Exception as e:
                file_upload_info.status = FileStatus.ERROR
                file_upload_info.error = str(e)
                raise e

        file_upload_info.status = FileStatus.OK
    
    @contextmanager
    def define_converter(self, file_upload_info : FileUploadInfo, converter_method : Callable):
        '''
        Declare a converter.
        
        Args:
            file_upload_info (FileUploadInfo): The file upload info.
            converter_name (str): The name of the converter.
        
        Example usage:
        with sync_record.define_converter(file_upload_info, ) as data_convertor:
        '''
        # get fqn of the converter
        data_convertor = DataConvertor.from_callable(converter_method, FileStatus.OK)
        file_upload_info.converter = data_convertor
        
        with self.task(f"convert {data_convertor.method} called"):
            try:
                yield data_convertor
            except Exception as e:
                data_convertor.status = FileStatus.ERROR
                data_convertor.error = str(e)
                raise DataConvertorException(e) from e

def error_in_children(e : Exception, task : TaskEntry) -> bool:
    '''
    Check if the error is in the children of the task.
    '''
    for item in task.content:
        if isinstance(item, ErrorEntry):
            if item.message == str(e):
                return True
        elif isinstance(item, TaskEntry):
            if error_in_children(e, item):
                return True
    return False