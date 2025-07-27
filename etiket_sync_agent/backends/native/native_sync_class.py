import typing, uuid

from etiket_client.local.dao.dataset import dao_dataset, DatasetUpdate as DatasetUpdateLocal
from etiket_client.local.dao.file import dao_file, FileUpdate as FileUpdateLocal
from etiket_client.local.database import Session 
from etiket_client.local.models.file import FileSelect, FileStatusLocal, FileType

from etiket_client.local.exceptions import DatasetNotFoundException

from etiket_client.remote.api_tokens import api_token_session
from etiket_client.remote.endpoints.dataset import dataset_read, dataset_create, dataset_update
from etiket_client.remote.endpoints.file import file_create, file_generate_presigned_upload_link_single

from etiket_client.remote.endpoints.models.dataset import DatasetCreate, DatasetUpdate
from etiket_client.remote.endpoints.models.file import FileCreate, FileRead
from etiket_client.remote.endpoints.models.types import FileStatusRem

from etiket_sync_agent.sync.sync_source_abstract import SyncSourceDatabaseBase
from etiket_sync_agent.sync.sync_utilities import SyncItems
from etiket_sync_agent.sync.sync_records.manager import SyncRecordManager

from etiket_sync_agent.sync.sync_utilities import md5
from etiket_sync_agent.sync.uploader.file_uploader import upload_new_file_single

from etiket_sync_agent.backends.native.native_sync_config_class import NativeConfigData

# TODO tests :: add sync loop item, that first creates a qcodes dataset and then changes that dataset
# -- this is important to see that the restrictions in the database are set correctly.

class NativeSync(SyncSourceDatabaseBase):
    SyncAgentName: typing.ClassVar[str] = "native"
    ConfigDataClass: typing.ClassVar[typing.Type[NativeConfigData]] = NativeConfigData
    MapToASingleScope: typing.ClassVar[bool] = False
    LiveSyncImplemented: typing.ClassVar[bool] = False

    @staticmethod
    def getNewDatasets(configData: NativeConfigData, lastIdentifier: str | None) -> typing.List[SyncItems]:
        with Session() as session:
            datasets = dao_dataset.get_sync_items(lastIdentifier, session)
            sync_items = []
            for dataset in datasets:
                sync_item = SyncItems(datasetUUID = dataset.uuid,
                                        dataIdentifier = dataset.uuid,
                                        sync_priority = dataset.modified)
                sync_items.append(sync_item)
        return sync_items

    @staticmethod
    def checkLiveDataset(configData: NativeConfigData, syncIdentifier: SyncItems, maxPriority: bool) -> bool:
        return False
    
    @staticmethod
    def syncDatasetNormal(configData: NativeConfigData, syncIdentifier: SyncItems, sync_record: SyncRecordManager):
        # Here manual functions are used.
        with sync_record.task("Start synchronization of the dataset"):
            with Session() as session:
                with sync_record.task("Read local dataset"):
                    dataset_local = dao_dataset.read(syncIdentifier.datasetUUID, session)
                    sync_record.add_log("Success!")
                
                dataset_remote = None 
                with sync_record.task("synchronize metadata to server"):
                    with api_token_session(dataset_local.creator):
                        try :
                            dataset_remote = dataset_read(dataset_local.uuid)
                            sync_record.add_log("A remote dataset is already present.")
                        except DatasetNotFoundException:
                            sync_record.add_log("No remote dataset found.")

                        if not dataset_remote:
                            sync_record.add_log("Attempt to create remote dataset.")
                            dc = DatasetCreate(**dataset_local.model_dump(), scope_uuid=dataset_local.scope.uuid)
                            dataset_create(dc)
                            dataset_remote = dataset_read(dataset_local.uuid)
                            sync_record.add_log("Remote dataset created.")
                        else:
                            if dataset_local.modified > dataset_remote.modified:
                                sync_record.add_log("Updating metadata ...")
                                du = DatasetUpdate(**dataset_local.model_dump())
                                dataset_update(dataset_local.uuid, du)
                                sync_record.add_log("Metadata updated.")
                            else:
                                sync_record.add_log("Metadata up to date, no updates needed.")

                with sync_record.task("Synchronizing files to server"):
                    files =  dataset_local.files if dataset_local.files is not None else []
                    for file in files:
                        if file.status == FileStatusLocal.complete and file.synchronized is False and file.type != FileType.HDF5_CACHE:
                            sync_record.add_log(f"Synchronizing file with name {file.name} and version_id {file.version_id}")
                            fs = FileSelect(uuid=file.uuid, version_id=file.version_id)

                            file_remote = get_remote_file(dataset_remote.files, file.uuid, file.version_id)
                            if file_remote:
                                sync_record.add_log("File record already present on the remote server, updating details.")
                                # TODO (later) update details --> this is currently done auto in dataqruiser.
                                if file_remote.status == FileStatusRem.secured:
                                    fu = FileUpdateLocal(synchronized=True)
                                    dao_file.update(fs, fu, session)
                                    sync_record.add_log("File already secured, done.")
                                    continue
                                else:
                                    sync_record.add_log("File record already present on the remote server, but not yet secured, proceeding to upload.")
                            else:
                                sync_record.add_log("Creating file record on the remote server.")
                                fc = FileCreate(**file.model_dump(), ds_uuid=dataset_local.uuid)
                                file_create(fc)
                                sync_record.add_log("File record created.")
                            
                            
                            sync_record.add_log("Starting upload of the file.")
                            upload_info = file_generate_presigned_upload_link_single(file.uuid, file.version_id)
                            md5_checksum = md5(file.local_path)
                            upload_new_file_single(file.local_path, upload_info, md5_checksum)
                            sync_record.add_log("Upload finished.")

                            fu = FileUpdateLocal(synchronized=True)
                            dao_file.update(fs, fu, session)
                        else:
                            if file.status != FileStatusLocal.complete:
                                sync_record.add_log(f"skipping {file.name} (version :: {file.version_id}) as file status is not yet completed (status = {file.status}).")
                            elif file.type == FileType.HDF5_CACHE:
                                sync_record.add_log(f"skipping {file.name} (version :: {file.version_id}) as file is marked as cache.")
                            else:
                                raise ValueError("Error the code should not reach here. -- name : {file.name}, file_uuid : {file.uuid}, version_id : {file.version_id}, status : {file.status}, path : {file.local_path}.")
                    
                    # check if last modified is the same as before starting!!
                    dataset_local_updated = dao_dataset.read(syncIdentifier.datasetUUID, session)
                    
                    if dataset_local_updated.modified == dataset_local_updated.modified:
                        du = DatasetUpdateLocal(synchronized=True)
                        dao_dataset.update(dataset_local.uuid, du,session)
                    
            sync_record.add_log("Dataset sync is successful!")


    @staticmethod
    def syncDatasetLive(configData: NativeConfigData, syncIdentifier: SyncItems, sync_record: SyncRecordManager):
        raise NotImplementedError
    
def get_remote_file(files : typing.List[FileRead], file_uuid : uuid.UUID, version_id : int):
    for file in files:
        if file.uuid == file_uuid and file.version_id == version_id:
            return file
    return None