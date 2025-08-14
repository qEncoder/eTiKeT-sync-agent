import os, json, tempfile, logging, uuid, dataclasses, datetime, xarray, enum, shutil, traceback

from typing import List, Dict, Optional, Set, Tuple, Any, Type

from etiket_client.local.exceptions import DatasetNotFoundException
from etiket_client.local.models.file import FileRead as FileReadLocal, FileSelect as FileSelectLocal, FileUpdate as FileUpdateLocal, FileStatusLocal
from etiket_client.remote.endpoints.dataset import dataset_create, dataset_read, dataset_read_by_alt_uid, dataset_update
from etiket_client.remote.endpoints.file import file_create, file_generate_presigned_upload_link_single, file_read_by_name, file_read, FileSelect
from etiket_client.remote.endpoints.models.dataset import DatasetCreate, DatasetUpdate as DatasetUpdateRem, DatasetRead as DatasetReadRem
from etiket_client.remote.endpoints.models.types import FileStatusRem, FileType
from etiket_client.remote.endpoints.models.file import FileCreate, FileRead as FileReadRem

from etiket_client.local.database import Session
from etiket_client.local.dao.dataset import dao_dataset
from etiket_client.local.dao.file import dao_file
from etiket_client.local.models.dataset import  DatasetCreate as DatasetCreateLocal, DatasetRead as DatasetReadLocal, DatasetUpdate as DatasetUpdateLocal
from etiket_client.settings.folders import create_file_dir
from etiket_client.remote.errors import CONNECTION_ERRORS

from etiket_sync_agent.sync.checksums.hdf5 import md5_netcdf4
from etiket_sync_agent.sync.checksums.any import md5

from etiket_sync_agent.sync.uploader.file_uploader import upload_new_file_single
from etiket_sync_agent.sync.sync_records.manager import SyncRecordManager

from etiket_sync_agent.models.sync_items import SyncItems
from etiket_sync_agent.db import get_db_session_context
from etiket_sync_agent.crud.sync_items import crud_sync_items
from etiket_client.python_api.dataset_model.files import generate_version_id

logger = logging.getLogger(__name__)

class FileCompatibility(enum.Enum):
    MATCH = 0
    MISMATCH = 1
    EMPTY = 2
    
@dataclasses.dataclass
class dataset_info:
    name: str
    datasetUUID: uuid.UUID
    scopeUUID: uuid.UUID
    created: datetime.datetime
    alt_uid: Optional[str] = None
    description: Optional[str] = None
    keywords: List[str] = dataclasses.field(default_factory=list)
    attributes: Dict[str, str] = dataclasses.field(default_factory=dict)
    ranking: int = 0
    creator: str = dataclasses.field(default="")
    
    def __post_init__(self):
        # Convert None to empty string for creator field
        if self.creator is None:
            self.creator = ""

@dataclasses.dataclass
class file_info:
    name : str
    fileName: str

    created: datetime.datetime
    
    fileType : Optional[FileType]
    creator: str = ''
    # TODO standardize this
    file_generator : Optional[str] = None
    
    immutable_on_completion : bool = True # only set to false if you know what you are doing

# TODO proper implementation of immutabilitly of files
class sync_utilities:
    @staticmethod
    def create_or_update_dataset(live_mode : bool, s_item : SyncItems, ds_info : dataset_info, sync_record: SyncRecordManager):
        '''
        Create or update a dataset on the remote server and the local server if live_mode is True.

        This method performs the following steps:
        1. Attempts to read the dataset from the remote server using the datasetUUID.
        2. If the dataset is not found, it attempts to read the dataset using the alt_uid and scopeUUID.
            - If the dataset is still not found, it creates a new dataset on the remote server.
            - If the dataset is found, it updates the datasetUUID in the sync item and the sync_item table
                in the local database.
        3. If live_mode is set to True, the dataset is created on the local server.

        Args:
            live_mode (bool): If True, the dataset is also created or updated on the local server.
            s_item (SyncItems): The sync item that contains the datasetUUID.
            ds_info (dataset_info): The dataset information to create or update.
        '''        
        with sync_record.task("Creating or updating dataset on remote server"):
            with get_db_session_context() as session_sync:
                with Session() as session_etiket:
                    try :
                        ds = dataset_read(ds_info.datasetUUID)
                        sync_record.add_log("Dataset record found on remote server (by uuid).")
                    except DatasetNotFoundException:
                        try:
                            if ds_info.alt_uid is None:
                                raise DatasetNotFoundException
                            ds = dataset_read_by_alt_uid(ds_info.alt_uid, ds_info.scopeUUID)
                            s_item = crud_sync_items.update_sync_item(session_sync, s_item.id, dataset_uuid=ds.uuid)
                            sync_record.add_log("Dataset record found on remote server (by alt_uid), updating uuid to match the one on the remote server.")
                        except DatasetNotFoundException:
                            # check locally if the alt_uid is already present
                            if ds_info.alt_uid is not None:
                                try:
                                    ds_local = dao_dataset.read_by_uuid_and_alt_uid(ds_info.alt_uid, ds_info.scopeUUID, session_etiket)
                                    if ds_local.uuid != s_item.datasetUUID:
                                        s_item = crud_sync_items.update_sync_item(session_sync, s_item.id, dataset_uuid=ds_local.uuid)
                                        ds_info.datasetUUID = ds_local.uuid
                                    
                                        logger.info("Dataset record found on local server (by alt_uid), updating uuid to match the one on the local server.")
                                        sync_record.add_log("Found alt_uid in a dataset on the local server, updating uuid ...")
                                except DatasetNotFoundException:
                                    pass
                            
                            dc = DatasetCreate(uuid = ds_info.datasetUUID, alt_uid= ds_info.alt_uid,
                                    collected=ds_info.created,  name = ds_info.name, creator = ds_info.creator,
                                    description= ds_info.description, keywords = ds_info.keywords,
                                    ranking= ds_info.ranking, scope_uuid = ds_info.scopeUUID, 
                                    attributes = ds_info.attributes)
                            dataset_create(dc)
                            ds = None 
                            logger.info("Dataset record created on remote server.")  
                            sync_record.add_log("Dataset record created on remote server.")
                    
                    if ds is not None:
                        needs_update, du = compare_and_prepare_update(ds_info, ds, updateSchema = DatasetUpdateRem)
                        if needs_update:
                            if isinstance(du, DatasetUpdateRem):
                                dataset_update(s_item.datasetUUID, du)
                            else:
                                raise ValueError(f"Dataset update object is not a remote dataset update object: {du}")
                            logger.info("Dataset record updated on remote server.")
                            sync_record.add_log("Dataset record updated on remote server.")
                        else:
                            logger.info("Dataset record found on remote server, no update needed.")
                            sync_record.add_log("Dataset record found on remote server, already up to date.")
                    if live_mode:
                        try:
                            ds = dao_dataset.read(s_item.datasetUUID, session=session_etiket)
                            sync_record.add_log("Dataset record found on local server (Live Dataset).")
                        except DatasetNotFoundException:
                            dc = DatasetCreateLocal(uuid = s_item.datasetUUID, 
                                alt_uid= ds_info.alt_uid, collected=ds_info.created,
                                name = ds_info.name, creator = ds_info.creator,
                                description= ds_info.description, keywords = ds_info.keywords,
                                ranking= ds_info.ranking, scope_uuid = ds_info.scopeUUID,
                                attributes = ds_info.attributes)
                            ds = dao_dataset.create(dc, session=session_etiket)
                            sync_record.add_log("Dataset record created on local server.")
                            
                        needs_update, du = compare_and_prepare_update(ds_info, ds, updateSchema = DatasetUpdateLocal)
                        if needs_update:
                            if isinstance(du, DatasetUpdateLocal):
                                dao_dataset.update(s_item.datasetUUID, du, session=session_etiket)
                            else:
                                raise ValueError(f"Dataset update object is not a local dataset update object: {du}")
                            sync_record.add_log("Dataset record updated on local server.")
                            logger.info("Dataset record updated on local server.")
                        else:
                            sync_record.add_log("Dataset record found on local server, no update needed.")
                            logger.info("Dataset record found on local server, no update needed.")
                                
    @staticmethod
    def upload_xarray(xarray_object : xarray.Dataset, s_item : SyncItems,  f_info : file_info, sync_record: SyncRecordManager):
        with sync_record.task("Converting xarray object to netcdf file and uploading to server"):
            sync_record.add_log("Converting xarray object to netcdf file")
            try:
                with tempfile.TemporaryDirectory() as tmpdirname:
                    file_path = f'{tmpdirname}/{f_info.name}.h5'
                    comp = {"zlib": True, "complevel": 3}
                    encoding = {var: comp for var in list(xarray_object.data_vars)+list(xarray_object.coords)}
                    xarray_object.to_netcdf(file_path, engine='h5netcdf', encoding=encoding, invalid_netcdf=True)
                    sync_record.add_log("Conversion successfull")
                    f_info.fileType = FileType.HDF5_NETCDF
                    sync_utilities.upload_file(file_path, s_item, f_info, sync_record)
            except Exception as e:
                traceback_str = traceback.format_exc()
                sync_record.add_error("Error converting xarray object to netcdf file", e, traceback_str)
                logger.exception("Failed to convert xarray object to netcdf file %s", f_info.name)
                # fail silently -- errors noted in the manifest.
    
    @staticmethod
    def upload_JSON(content : 'Dict | List | Set | str | int | float', s_item : SyncItems, f_info : file_info, sync_record: SyncRecordManager):
        with sync_record.task("Converting JSON object to file and uploading to server"):
            try:
                with tempfile.TemporaryDirectory() as tmpdirname:
                    file_path = f'{tmpdirname}/{f_info.name}.json'
                    content = json.dumps(content)
                    
                    f_info.fileType = FileType.JSON
                    with open(file_path, 'wb') as file_raw:
                        file_raw.write(content.encode())
                        file_raw.flush()
                    sync_record.add_log("Conversion successfull")
                    sync_utilities.upload_file(file_path, s_item, f_info, sync_record)
            except Exception as e:
                traceback_str = traceback.format_exc()
                sync_record.add_error("Error converting JSON object to file", e, traceback_str)
                logger.exception("Failed to convert JSON object to file %s", f_info.name)
                # fail silently -- errors noted in the manifest.
    
    @staticmethod
    def upload_file(file_path, s_item : SyncItems, f_info : file_info, sync_record: SyncRecordManager):
        with sync_record.add_upload_task(f_info.name) as file_upload_info:
            try:
                sync_record.add_log(f"Starting upload process for file {f_info.name}")
                if os.stat(file_path).st_size == 0:
                    logger.warning("File %s is empty, skipping.", file_path)
                    return
                
                sync_record.add_log(f"Reading file versions for file {f_info.name}")
                r_files, l_files = read_files(s_item.datasetUUID, f_info.name)
                # get checksums
                md5_checksum = md5(file_path)
                md5_checksum_netcdf4 = None
                if f_info.fileType is FileType.HDF5_NETCDF:
                    try:
                        md5_checksum_netcdf4 = md5_netcdf4(file_path)
                    except Exception:
                        logger.warning("Could not calculate md5 checksum for file %s, of dataset with uuid : %s. This file will be considered as a normal H5 file.", f_info.name, s_item.datasetUUID)
                        sync_record.add_log(f"Could not calculate md5 (NETCDF4) checksum for file {f_info.name}. This file will be considered as a normal H5 file.")
                        f_info.fileType = FileType.HDF5
                        md5_checksum_netcdf4 = md5(file_path)
                
                file_version_id = generate_version_id(f_info.created)

                # get relevant file versions:
                local_version = r_files.get(file_version_id, None)
                remote_version = l_files.get(file_version_id, None)
                            
                max_version_id = -1 # cannot be negative.
                if len(l_files) > 0 or len(r_files) > 0:
                    max_version_id = max(list(l_files.keys()) + list(r_files.keys()))     

                local_last_version = r_files.get(max_version_id, None)
                remote_last_version = l_files.get(max_version_id, None)
                
                # check if file is already uploaded, or is compatible for upload
                local_version_compatibility, local_version_replace = check_file_status_and_replacement_needed(local_version, md5_checksum, md5_checksum_netcdf4)
                remote_version_compatibility, remote_version_replace = check_file_status_and_replacement_needed(remote_version, md5_checksum, md5_checksum_netcdf4)
                
                last_local_version_compatibility, last_local_version_replace = check_file_status_and_replacement_needed(local_last_version, md5_checksum, md5_checksum_netcdf4)
                last_remote_version_compatibility, last_remote_version_replace = check_file_status_and_replacement_needed(remote_last_version, md5_checksum, md5_checksum_netcdf4)
                
                file_create_data = FileCreate(name = f_info.name, filename=f_info.fileName,
                                creator=f_info.creator, uuid =uuid.uuid4(), collected = f_info.created,
                                size = os.stat(file_path).st_size, type = f_info.fileType if f_info.fileType is not None else FileType.UNKNOWN,
                                file_generator = f_info.file_generator, version_id = file_version_id,
                                ds_uuid = s_item.datasetUUID, immutable=f_info.immutable_on_completion)
                
                if len(r_files) > 0:
                    file_create_data.uuid = r_files[list(r_files.keys())[0]].uuid
                elif len(l_files) > 0:
                    file_create_data.uuid = l_files[list(l_files.keys())[0]].uuid
                
                # decide where the file should be uploaded:
                if (not (local_version_compatibility is FileCompatibility.MISMATCH or remote_version_compatibility is FileCompatibility.MISMATCH) and not 
                    (local_version_compatibility is FileCompatibility.EMPTY and remote_version_compatibility is FileCompatibility.EMPTY)):
                    if remote_version is None:
                        sync_record.add_log("No remote version found, creating new file on server")
                        file_create(file_create_data)
                    else:
                        sync_record.add_log("Remote version found, no need to create new file on server")
                    
                    upload_file_to_server(file_path, file_create_data.uuid, file_create_data.version_id, md5_checksum, md5_checksum_netcdf4, remote_version_replace, sync_record)
                    
                    if local_version is not None and local_version_replace is True:
                        sync_record.add_log("Local version found, replacing local file, as it is not immutable and the content is different.")
                        replace_local_file(s_item.datasetUUID, file_path,local_version, sync_record)
                elif ((last_local_version_compatibility is FileCompatibility.MATCH or last_remote_version_compatibility is FileCompatibility.MATCH) and 
                    (last_local_version_compatibility is not FileCompatibility.MISMATCH and last_remote_version_compatibility is not FileCompatibility.MISMATCH)):
                        file_create_data.version_id = max_version_id
                        if remote_last_version is None:
                            sync_record.add_log("No remote version found, creating new file on server (last version present of the local file).")
                            file_create(file_create_data)
                        else:
                            sync_record.add_log("Remote version found, no need to create new file on server (last version present of the local file).")
                        
                        upload_file_to_server(file_path, file_create_data.uuid, file_create_data.version_id, md5_checksum, md5_checksum_netcdf4, last_remote_version_replace, sync_record)
                        
                        if local_last_version is not None and last_local_version_replace is True:
                            sync_record.add_log("Local version found, replacing local file, as it is not immutable and the content is different.")
                            replace_local_file(s_item.datasetUUID, file_path, local_last_version, sync_record)
                else:
                    sync_record.add_log("File is not compatible with the existing file versions, creating new file on server.")
                    file_create_data.version_id = generate_version_id(datetime.datetime.now()) # TO Discuss, should the be the last modified time?
                    file_create(file_create_data)
                    upload_file_to_server(file_path, file_create_data.uuid, file_create_data.version_id, md5_checksum, md5_checksum_netcdf4, False, sync_record)
            except CONNECTION_ERRORS as e:
                traceback_str = traceback.format_exc()
                sync_record.add_error("Connection error", e, traceback_str)
                raise e
            except Exception as e:
                traceback_str = traceback.format_exc()
                sync_record.add_error("Error uploading file to server, will try again later.", e, traceback_str)
                logger.exception("Failed to upload file :/")
                # fail silently -- errors noted in the manifest.
    
def read_files(dataset_uuid : uuid.UUID, file_name : str) -> Tuple[Dict[int, FileReadLocal], Dict[int, FileReadRem]]:
    '''
    Read all file versions for a given dataset and file name from both local and remote sources.
    
    Args:
        dataset_uuid: UUID of the dataset containing the files
        file_name: Name of the file to retrieve versions for
    
    Returns:
        Tuple containing:
        - Dict[int, FileReadLocal]: Local files indexed by version_id
        - Dict[int, FileReadRem]: Remote files indexed by version_id
    
    Raises:
        ValueError: If dataset_uuid is invalid or file_name is empty
        Exception: If database or API operations fail
    '''
    l_files = {}
    r_files = {}
    
    with Session() as session:
        try:
            print(dataset_uuid, file_name)
            l_files_list = dao_file.get_file_by_name(dataset_uuid, file_name, session=session)
            l_files = {file.version_id : FileReadLocal.model_validate(file) for file in l_files_list}
            print(l_files)
        except DatasetNotFoundException:
            pass
    
    # remote dataset should always be present (created in prev step).
    r_files_list = file_read_by_name(dataset_uuid, file_name)
    r_files = {file.version_id : file for file in r_files_list}

    return l_files, r_files

def check_file_status_and_replacement_needed(file : Optional[FileReadLocal | FileReadRem], md5_checksum : Any, md5_checksum_netcdf4 : Optional[Any]) -> Tuple[FileCompatibility, bool]:
    '''
    Check if the uploaded file is compatible with the existing file version and whether replacement is needed.
    
    Cases:
    - File is None : return empty as the file needs to be created.
    - File is local and local_path is None : match, as the file is not downloaded yet from the server.
    - File is local and local_path is not None and md5_checksum matches : file is an exact copy of the existing file -> match
    - File is local and local_path is not None and md5_checksum does not match -- immutable : mismatch as it cannot be replaced.
    - File is local and local_path is not None and md5_checksum does not match -- mutable : match as it can be replaced.
    - File is remote and status is not secured : no files yet uploaded to the server, can be replaced.
    - File is remote and status is secured and md5_checksum matches : match as it is an exact copy of the existing file.
    - File is remote and status is secured and md5_checksum does not match -- immutable : mismatch as it cannot be replaced.
    - File is remote and status is secured and md5_checksum does not match -- mutable : match as it can be replaced.
    
    Returns:
        Tuple[FileCompatibility, bool]: (compatibility_status, needs_replacement)
    '''
    if file is None:
        return FileCompatibility.EMPTY, False
        
    if isinstance(file, FileReadLocal):
        if file.local_path is not None:
            if has_MD5_match(file, md5_checksum, md5_checksum_netcdf4):
                return FileCompatibility.MATCH, False
            # TODO :: add this as a property to the file object (currently not implemented)
            # if not file.immutable:
            #     return FileCompatibility.MATCH, True
            return FileCompatibility.MISMATCH, False
        # File exists but not downloaded locally yet
        return FileCompatibility.MATCH, False
    elif isinstance(file, FileReadRem):
        if file.status != FileStatusRem.secured:
            return FileCompatibility.MATCH, False
        if file.md5_checksum is not None:
            if has_MD5_match(file, md5_checksum, md5_checksum_netcdf4):
                return FileCompatibility.MATCH, False
            if not file.immutable:
                return FileCompatibility.MATCH, True
            return FileCompatibility.MISMATCH, False
        # Secured file without checksum - legacy ..
        return FileCompatibility.MISMATCH, False
    
    raise ValueError(f"File {file} is not a valid file object")

def has_MD5_match(file : FileReadLocal | FileReadRem, md5_checksum : Any, md5_checksum_netcdf4 : Optional[Any]) -> bool:
    '''
    Check if the MD5 checksum matches of a file. A implementation for both local and remote files is made.
    '''
    if isinstance(file, FileReadLocal):
        if file.local_path is not None:
            md5_checksum_local = md5(file.local_path)
            if md5_checksum_local.hexdigest() == md5_checksum.hexdigest():
                return True
            if md5_checksum_netcdf4 is not None:
                try:
                    md5_checksum_netcdf4_local = md5_netcdf4(file.local_path)
                    if md5_checksum_netcdf4_local.hexdigest() == md5_checksum_netcdf4.hexdigest():
                        return True
                except Exception:
                    return False
            return False
    elif isinstance(file, FileReadRem):
        if file.md5_checksum is not None:
            if file.md5_checksum == md5_checksum.hexdigest():
                return True
            if (md5_checksum_netcdf4 is not None and file.md5_checksum == md5_checksum_netcdf4.hexdigest()):
                return True
            return False
    else:
        raise ValueError(f"File {file} is not a valid file object")
    return False
    
def upload_file_to_server(file_path : str, file_uuid : uuid.UUID, file_version_id : int, md5_checksum : Any, md5_checksum_netcdf4 : Optional[Any], replace_file : bool, sync_record: SyncRecordManager):
    """
    Upload a file to the server if needed.
    
    Args:
        file_path: Local path to the file to upload
        file_uuid: UUID of the file on the server
        file_version_id: Version ID of the file
        md5_checksum: MD5 checksum of the file
        md5_checksum_netcdf4: Optional NetCDF4-specific MD5 checksum
        replace_file: Whether to force replacement of existing file
        dataset_manifest: Manifest for logging upload status
    """
    sync_record.add_log(f"Uploading file to server with version id {file_version_id}")
    try:
        file_select = FileSelect(uuid=file_uuid, version_id=file_version_id)
        files = file_read(file_select)
        
        if len(files) != 1:
            raise ValueError(f"Expected exactly 1 file, found {len(files)} for {file_select}")
        
        file = files[0]
        
        should_upload = (file.status != FileStatusRem.secured) or replace_file
        if file.status == FileStatusRem.secured and replace_file:
            sync_record.add_log("File is secured on server, but replace is requested, overwriting file on server.")
        
        if should_upload:
            sync_record.add_log(f"Uploading file to server (status: {file.status}, replace: {replace_file})")
            upload_info = file_generate_presigned_upload_link_single(file_uuid, file_version_id)
            upload_new_file_single(file_path, upload_info, md5_checksum, md5_checksum_netcdf4)
            sync_record.add_log("File upload completed successfully")
        else:
            sync_record.add_log("File already secured on server, skipping upload")
            
    except Exception as e:
        sync_record.add_log(f"Error uploading file to server: {str(e)}")
        logger.error("Failed to upload file %s version %s: %s", file_uuid, file_version_id, str(e))
        raise e

def replace_local_file(dataset_uuid : uuid.UUID, file_path : str, file : FileReadLocal, sync_record: SyncRecordManager):
    """
    Replace a local file with a new version.
    
    Args:
        dataset_uuid: UUID of the dataset
        file_path: Path to the new file content
        file: Local file record to update
        dataset_manifest: Manifest for logging operations
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source file not found: {file_path}")
        
    try:
        with Session() as session:
            dataset = dao_dataset.read(dataset_uuid, session) 
            
            if file.local_path is not None and os.path.exists(file.local_path):
                try:
                    os.remove(file.local_path)
                    sync_record.add_log(f"Removed old local file: {file.local_path}")
                except OSError as e:
                    sync_record.add_log(f"Warning: Could not remove old file {file.local_path}: {str(e)}")
                    logger.warning("Could not remove old file %s: %s", file.local_path, str(e))
            
            # Create new file location
            file_dir = create_file_dir(dataset.scope.uuid, dataset_uuid, file.uuid, file.version_id)
            local_path = os.path.join(file_dir, file.filename)
            
            # Ensure directory exists
            os.makedirs(file_dir, exist_ok=True)
            
            # Copy new file
            shutil.copy2(file_path, local_path)
            sync_record.add_log(f"Copied new file to: {local_path}")
            
            # Update database record
            file_update = FileUpdateLocal(
                status=FileStatusLocal.complete, 
                local_path=local_path, 
                synchronized=True
            )
            file_select = FileSelectLocal(uuid=file.uuid, version_id=file.version_id)
            dao_file.update(file_select, file_update, session)
            
            sync_record.add_log("Local file replacement completed successfully")
            
    except Exception as e:
        error_msg = f"Failed to replace local file for {file.uuid}: {str(e)}"
        sync_record.add_log(f"Error: {error_msg}")
        logger.error(error_msg)
        raise

def compare_and_prepare_update(ds_info: dataset_info,
                                existing_dataset: DatasetReadLocal | DatasetReadRem,
                                updateSchema : Type[DatasetUpdateRem] | Type[DatasetUpdateLocal]) -> Tuple[bool, DatasetUpdateRem | DatasetUpdateLocal]:
    """
    Compare ds_info with an existing DatasetUpdate and determine if an update is needed.

    Args:
        ds_info (dataset_info): The new dataset information to compare.
        existing_dataset (DatasetUpdate): The existing dataset information.

    Returns:
        Tuple[bool, DatasetUpdate]: A tuple containing a boolean indicating if an update is needed,
                                    and a DatasetUpdate object with the fields that need to be updated.
    """
    if existing_dataset is None: # None is returned when it is created -- TODO update this to return the dataset.
        return False, updateSchema()
    
    needs_update = False
    update_fields = {}

    if ds_info.alt_uid != existing_dataset.alt_uid:
        update_fields['alt_uid'] = ds_info.alt_uid
        needs_update = True

    if ds_info.name != existing_dataset.name:
        update_fields['name'] = ds_info.name
        needs_update = True

    if ds_info.description != existing_dataset.description:
        update_fields['description'] = ds_info.description
        needs_update = True

    # convert keywords to a set to ignore order
    if set(ds_info.keywords) != set(existing_dataset.keywords):
        update_fields['keywords'] = ds_info.keywords
        needs_update = True

    if ds_info.ranking != existing_dataset.ranking:
        update_fields['ranking'] = ds_info.ranking
        needs_update = True

    # update attributes, to not overwrite extras added by the user
    if ds_info.attributes != existing_dataset.attributes:
        attributes = existing_dataset.attributes.copy() if existing_dataset.attributes is not None else {}
        attributes.update(ds_info.attributes)
        update_fields['attributes'] = attributes
        needs_update = True

    if needs_update:
        du = updateSchema(**update_fields)
    else:
        du = updateSchema()

    return needs_update, du

