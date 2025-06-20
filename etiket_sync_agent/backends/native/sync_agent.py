from etiket_client.local.dao.dataset import dao_dataset, DatasetUpdate as DatasetUpdateLocal
from etiket_client.local.dao.file import dao_file, FileUpdate as FileUpdateLocal
from etiket_client.local.models.file import FileSelect, FileStatusLocal, FileType

from etiket_client.remote.api_tokens import api_token_session
from etiket_client.remote.endpoints.dataset import dataset_read, dataset_create, dataset_update
from etiket_client.remote.endpoints.file import file_create, file_generate_presigned_upload_link_single

from etiket_client.remote.endpoints.models.dataset import DatasetCreate, DatasetUpdate
from etiket_client.remote.endpoints.models.file import FileCreate, FileRead

from etiket_client.remote.endpoints.models.types import FileStatusRem
from etiket_client.remote.errors import CONNECTION_ERRORS

from etiket_sync_agent.sync.sync_utilities import md5
from etiket_sync_agent.sync.uploader.file_uploader import upload_new_file_single

from sqlalchemy.orm import Session

import logging, typing, uuid

logger = logging.getLogger(__name__)

# TODO refactor to work in the new base class model

def run_native_sync(session : Session):
    logger.info("start sync of local datasets")
    unsynced_uuids = dao_dataset.get_unsynced_datasets(session)

    n_syncs = 0
    
    logger.info("synchronizing %s datasets", len(unsynced_uuids))

    for unsynced_uuid in unsynced_uuids:
        try:
            dataset_local = dao_dataset.read(unsynced_uuid, session)
            with api_token_session(dataset_local.creator):
                dataset_remote = None
                logger.info("synchronizing dataset with uuid : %s.", dataset_local.uuid)
                
                try :
                    dataset_remote = dataset_read(dataset_local.uuid)
                    logger.info("A remote dataset is already present.")
                except Exception:
                    logger.info("No remote dataset found.")
                    # request fails if there is no dataset
                    # TODO make error code for this, such that other errors do not break this
                
                if not dataset_remote:
                    logger.info("Trying to create remote dataset.")
                    dc = DatasetCreate(**dataset_local.model_dump(), scope_uuid=dataset_local.scope.uuid)
                    dataset_create(dc)
                    logger.info("Remote dataset created.")
                    dataset_remote = dataset_read(dataset_local.uuid)
                    logger.info("dataset read")
                else:
                    if dataset_local.modified > dataset_remote.modified:
                        logger.info("Trying to update field of the remote dataset.")
                        du = DatasetUpdate(**dataset_local.model_dump())
                        dataset_update(dataset_local.uuid, du)
                        logger.info("Fields updated.")
                
                for file in dataset_local.files:
                    logger.info("Starting file upload to the remote server.")
                    if file.status == FileStatusLocal.complete and file.synchronized is False and file.type != FileType.HDF5_CACHE:
                        logger.info("Synchronizing file with name %s, uuid %s and version_id %s", file.name, file.uuid, file.version_id)
                        fs = FileSelect(uuid=file.uuid, version_id=file.version_id)

                        file_remote = get_remote_file(dataset_remote.files, file.uuid, file.version_id)
                        if file_remote:
                            logger.info("File record already present on the remote server, updating details.")
                            # TODO update details.
                            if file_remote.status == FileStatusRem.secured:
                                fu = FileUpdateLocal(synchronized=True, ranking=file.ranking)
                                dao_file.update(fs, fu, session)
                                continue
                        else:
                            logger.info("Creating file record on the remote server.")
                            fc = FileCreate(**file.model_dump(), ds_uuid=dataset_local.uuid)
                            file_create(fc)
                            logger.info("File record created.")
                        
                        
                        logger.info("Starting upload of the file.")
                        upload_info = file_generate_presigned_upload_link_single(file.uuid, file.version_id)
                        md5_checksum = md5(file.local_path)
                        upload_new_file_single(file.local_path, upload_info, md5_checksum)
                        logger.info("Upload finished.")
                    
                        fu = FileUpdateLocal(synchronized=True)
                        dao_file.update(fs, fu, session)
                
                du = DatasetUpdateLocal(synchronized=True)
                dao_dataset.update(dataset_local.uuid, du,session)
            
                logger.info("Dataset Synchronization is successful!")
                n_syncs += 1
        except CONNECTION_ERRORS as e:
            raise e
        except Exception:
            logger.exception("Error occurred during the sync of the dataset.")
    return n_syncs

def get_remote_file(files : typing.List[FileRead], file_uuid : uuid.UUID, version_id : int):
    for file in files:
        if file.uuid == file_uuid and file.version_id == version_id:
            return file
    return None