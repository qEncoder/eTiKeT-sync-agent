import logging, time, traceback

from typing import Any, Type
from pathlib import Path

from sqlalchemy.orm import Session

from etiket_client.exceptions import NoLoginInfoFoundException

from etiket_client.local.database import get_db_session_context as get_db_session_etiket
from etiket_client.local.models.file import FileSelect
from etiket_client.local.dao.file import dao_file, dao_file_delete_queue
from etiket_client.local.dao.dataset import dao_dataset
from etiket_client.local.exceptions import DatasetNotFoundException
from etiket_client.remote.client import client
from etiket_client.settings.user_settings import get_user_settings
from etiket_client.remote.api_tokens import api_token_session
from etiket_client.remote.endpoints.models.types import FileType

from etiket_sync_agent.sync.manifest_mgr import manifest_manager
from etiket_sync_agent.sync.sync_records.manager import SyncRecordManager
from etiket_sync_agent.sync.sync_source_abstract import SyncSourceDatabaseBase, SyncSourceFileBase
from etiket_sync_agent.crud.sync_items import crud_sync_items
from etiket_sync_agent.crud.sync_sources import crud_sync_sources, init_sync_sources
from etiket_sync_agent.crud.sync_status import crud_sync_status
from etiket_sync_agent.crud.manifest import crud_manifest

from etiket_sync_agent.models.enums import SyncSourceStatus, SyncStatus
from etiket_sync_agent.models.sync_items import SyncItems
from etiket_sync_agent.models.sync_sources import SyncSources, SyncSourceTypes
from etiket_sync_agent.db import get_db_session_context

from etiket_sync_agent.backends.sources import get_source_sync_class
from etiket_sync_agent.backends.native.sync_scopes import sync_scopes

from etiket_client.remote.errors import CONNECTION_ERRORS

logger = logging.getLogger(__name__)

'''
DEV notes : currently there is some confusing naming going on:
* datasetManifest is the manifest that contains all the logs and errors that happen during the sync of a single dataset.
* manifest_mgr : is a global manager for the files being synchronized, it keeps track of file changes of the files system,
                and generates the necessary information to create sync items.
The latter should probably change name.
'''

class SyncConf:
    SCOPE_SYNC_INTERVAL = 60  # sync the scopes every 60 seconds
    DELETE_HDF5_CACHE_INTERVAL = 60  # check and delete old HDF5_CACHE files every 60 seconds
    USER_CHECK_INTERVAL = 5  # check if a user is logged in every 5 seconds
    CONNECTION_ERROR_DELAY = 60  # delay of 60 seconds when there is a connection error
    IDLE_DELAY = 1  # delay of 1 second when the sync agent is idling (all statuses are set to SYNCHRONIZED)
    N_RETRIES_BEFORE_ERROR = 5  # number of retries before a sync source is set to ERROR

def sync_loop(n_cycles : int = 0):
    '''
    Loop to continuously sync the sync sources.
    
    Performs:
    * sync the scopes every SCOPE_SYNC_INTERVAL (60 seconds)
    * clean the file delete queue every DELETE_HDF5_CACHE_INTERVAL (60 seconds)
    * checks if a user is logged in every USER_CHECK_INTERVAL (5 seconds)
    * syncs the sync sources continuously.
        A delay of 1 second is added, when the sync agent is idling (all statuses are set to SYNCHRONIZED)
    
    On error:
    * The sync process is paused for 60 seconds if there is no internet connection.
    '''
    last_sync_time = 0
    n_cycles_completed = 0
    
    init_sync_sources()
    
    while (n_cycles <= 0 or n_cycles_completed < n_cycles):
        n_cycles_completed += 1
        user_settings = get_user_settings()
        user_settings.load()
        with get_db_session_context() as session_sync:
            with get_db_session_etiket() as session_etiket:
                status = crud_sync_status.get_or_create_status(session_sync)
                sync_iteration = crud_sync_status.increment_sync_iteration(session_sync)

                if status.status == SyncStatus.STOPPED:
                    time.sleep(1) #check again in 1 second
                    continue
                
                try:
                    current_time = time.time()
                    # try to use API token for this (more robust)
                    with api_token_session(user_settings.user_sub):
                        if current_time - last_sync_time >= SyncConf.USER_CHECK_INTERVAL:
                            client.check_user_session()
                        if current_time - last_sync_time >= SyncConf.SCOPE_SYNC_INTERVAL:
                            sync_scopes(session_etiket)
                        if current_time - last_sync_time >= SyncConf.DELETE_HDF5_CACHE_INTERVAL:
                            dao_file_delete_queue.clean_files(session_etiket)
                        last_sync_time = current_time
                        # assuming when getting here, the sync is running.
                        crud_sync_status.update_status(session_sync, SyncStatus.RUNNING)
                        run_sync_iter(session_sync, session_etiket, sync_iteration)
                except NoLoginInfoFoundException:
                    logger.info("No access token found, waiting for 10 seconds.")
                    time.sleep(10)
                    crud_sync_status.update_status(session_sync, SyncStatus.NOT_LOGGED_IN)
                except CONNECTION_ERRORS:
                    crud_sync_status.update_status(session_sync, SyncStatus.NO_CONNECTION)
                    logger.warning("No connection, waiting for 60 seconds.")
                    time.sleep(SyncConf.CONNECTION_ERROR_DELAY)
                except Exception as e:
                    crud_sync_status.update_status(session_sync, SyncStatus.ERROR, repr(e))
                    logger.exception('Sync loop broken!')
                    time.sleep(SyncConf.IDLE_DELAY)

def run_sync_iter(session_sync : Session, session_etiket : Session, sync_iteration: int):
    n_syncs = 0
    sync_sources = crud_sync_sources.list_sync_sources(session_sync)
    for sync_source in sync_sources:
        print(f"Syncing {sync_source.name}")
        if sync_source.status in (SyncSourceStatus.PAUSED, SyncSourceStatus.ERROR):
            continue
        
        logger.info("Syncing %s, of type %s", sync_source.name, sync_source.type)
        
        try:
            sync_cls = get_source_sync_class(sync_source.type)
            sync_config = sync_cls.sync_config(sync_source.config_data)
                    
            get_new_sync_items(sync_source, sync_cls, sync_config, session_sync)
            s_item, live_dataset = get_next_sync_item(sync_source, sync_cls, sync_config, session_sync, session_etiket)
            
            # if nothing to do, we say it is synchronized (even if there were errors)
            if s_item is None:
                if sync_source.status != SyncSourceStatus.SYNCHRONIZING:
                    crud_sync_sources.update_sync_source(session_sync, sync_source.id, status=SyncSourceStatus.SYNCHRONIZED)
                logger.info("No new items to sync from %s.", sync_source.name)
                continue
            elif sync_source.status != SyncSourceStatus.SYNCHRONIZING:
                crud_sync_sources.update_sync_source(session_sync, sync_source.id, status=SyncSourceStatus.SYNCHRONIZING)
            
            # prepare the sync record
            if sync_source.type == SyncSourceTypes.fileBase:
                sync_record = SyncRecordManager(s_item, Path(s_item.dataIdentifier))
            else:
                sync_record = SyncRecordManager(s_item)
            
            # TODO: if native dataset, it should be the creator of the dataset --> how to handle this?
            with api_token_session(sync_source.creator):
                if live_dataset is True:
                    success = sync_dataset(sync_source, s_item, live_dataset=True, sync_record=sync_record)
                success = sync_dataset(sync_source, s_item, live_dataset=False, sync_record=sync_record)
                
            remove_dataset_caches(s_item, session_etiket, sync_record)
            
            if success:
                n_syncs += 1
                logger.info("Synced %s from %s.", s_item.dataIdentifier, sync_source.name)

                crud_sync_items.update_sync_item(session_sync, s_item.id, synchronized=True, sync_record=sync_record.to_dict())
                crud_sync_sources.update_sync_source(session_sync, sync_source.id, status=SyncSourceStatus.SYNCHRONIZING)
            else:
                logger.exception("Failed to synchronize %s from %s.", s_item.dataIdentifier, sync_source.name)
                crud_sync_items.update_sync_item(session_sync, s_item.id, attempts=s_item.attempts+1, sync_record=sync_record.to_dict())
                crud_sync_sources.update_sync_source(session_sync, sync_source.id,
                                                    status=SyncSourceStatus.SYNCHRONIZING,
                                                    update_statistics=True)
        except CONNECTION_ERRORS as e:
            raise e
        except Exception as e:
            logger.exception("Failed to sync %s", sync_source.name)
            traceback_str = traceback.format_exc()
            crud_sync_sources.add_sync_source_error(session_sync, sync_source.id, sync_iteration, log_exception=e, log_traceback=traceback_str)
            
            # update status to ERROR if 5 sequential errors are found.
            errors = crud_sync_sources.read_sync_source_errors(session_sync, sync_source.id, limit=SyncConf.N_RETRIES_BEFORE_ERROR)
            if (len(errors) >= SyncConf.N_RETRIES_BEFORE_ERROR 
                and all(error.sync_iteration == errors[0].sync_iteration - i
                        for i, error in enumerate(errors))):
                crud_sync_sources.update_sync_source(session_sync, sync_source.id, status=SyncSourceStatus.ERROR)

    if n_syncs == 0:
        time.sleep(1)

def get_new_sync_items(sync_source : SyncSources, sync_cls : Type[SyncSourceDatabaseBase] | Type[SyncSourceFileBase], sync_config : Any, session_sync : Session):
    '''
    Check for new sync items, add them to the sync items table.
    Determine the next item to sync and if it is a live dataset.
    
    Args:
        sync_source (SyncSources): The sync source object.
        sync_cls (Type[SyncSourceDatabaseBase] | Type[SyncSourceFileBase]): The sync class object.
        sync_config (Any): The sync config object (dataclass).
        session_sync (Session): The database session object to the sync database
        session_etiket (Session): The database session object to the etiket database
        
    Returns:
        None
    '''
    if issubclass(sync_cls, SyncSourceDatabaseBase):
        last_s_item = crud_sync_items.get_last_sync_item(session_sync, sync_source.id)
        new_items = sync_cls.getNewDatasets(sync_config, last_s_item)
        logger.info("Found %s new items in remote location, to add to the list of things to synchronize.", len(new_items))
        crud_sync_items.create_sync_items(session_sync, sync_source.id, new_items)
    else:
        current_manifest = crud_manifest.read_manifest(session_sync, sync_source.id)

        is_NFS = sync_config.server_folder if hasattr(sync_config, 'server_folder') else False
        manifest_mgr = manifest_manager(sync_source.name, sync_source.sync_class.rootPath(sync_config),
                                            current_manifest, level = sync_source.sync_class.level,
                                            is_NFS=is_NFS)
        
        new_manifests = manifest_mgr.get_updates()
        
        logger.info("Found %s new datasets (file base), will add these in the sync queue.", len(new_manifests))
        new_sync_items = []
        for identifier, priority in new_manifests.items():
            new_sync_items.append(SyncItems(dataIdentifier=identifier, syncPriority=priority))
        crud_sync_items.create_sync_items(session_sync, sync_source.id, new_sync_items)

def get_next_sync_item(sync_source : SyncSources, sync_cls : Type[SyncSourceDatabaseBase] | Type[SyncSourceFileBase], sync_config : Any,
                        session_sync : Session, session_etiket : Session):
    '''
    Get the next sync item from the sync items table. Only return a live dataset if it is implemented and not already tried.
    
    Args:
        sync_source (SyncSources): The sync source object.
        sync_cls (Type[SyncSourceDatabaseBase] | Type[SyncSourceFileBase]): The sync class object.
        sync_config (Any): The sync config object (dataclass).
        session_sync (Session): The database session object to the sync database
        session_etiket (Session): The database session object to the etiket database
    
    Returns:
        s_item (SyncItems): The sync item object.
        live_ds (bool): True if the dataset is a live dataset, False otherwise.
    '''
    s_item = crud_sync_items.read_next_sync_item(session_sync, sync_source.id, offset = 0)
    live_ds = False
    
    if s_item is not None:
        last_s_item = crud_sync_items.get_last_sync_item(session_sync, s_item.sync_source_id)
        if last_s_item is None:
            raise ValueError("Last sync item is None, this should not happen! Please report this as a bug.")
            
        live_ds = sync_cls.checkLiveDataset(sync_config, s_item, last_s_item.id==s_item.id)
        
        # this is needed to not get stuck in a loop of live datasets (though not the cleanest way)
        liveDS_already_tried = check_live_DS_already_tried(s_item, session_etiket)
        if (live_ds is True) and (sync_source.sync_class.LiveSyncImplemented is False or liveDS_already_tried):
            # assume only one live dataset at a time.
            s_item = crud_sync_items.read_next_sync_item(session_sync, sync_source.id, offset = 1)
            live_ds = False
    
    return s_item, live_ds

def sync_dataset(sync_source : SyncSources, s_item : SyncItems, live_dataset : bool, sync_record : SyncRecordManager) -> bool:
    '''
    Sync a dataset normally (not live).
    
    Args:
        sync_source (SyncSources): The sync source object.
        s_item (SyncItems): The sync item object.
        live_dataset (bool): True if the dataset is a live dataset, False otherwise.
    Returns:
        bool: True if the sync was successful, False otherwise.
    '''    
    logger.info("Syncing %s from %s.", s_item.dataIdentifier, sync_source.name)
    
    if live_dataset is True:
        sync_record.add_log("Start of sync (live)")
    else:
        sync_record.add_log("Start of sync (normal)")
    
    sync_class = get_source_sync_class(sync_source.type)
    sync_config = sync_class.sync_config(sync_source.config_data)
    try:
        if live_dataset:
            sync_class.syncDatasetLive(sync_config, s_item, sync_record)
        elif issubclass(sync_class, SyncSourceDatabaseBase):
            sync_class.syncDatasetNormal(sync_config, s_item, sync_record)
        else:
            manifest_mgr = manifest_manager(sync_source.name)
            manifest_before =  manifest_mgr.get_last_change(s_item.dataIdentifier)
            sync_class.syncDatasetNormal(sync_config, s_item, sync_record)
            manifest_after = manifest_mgr.get_last_change(s_item.dataIdentifier)
            if manifest_before != manifest_after:
                manifest_mgr.push_update(s_item.dataIdentifier, manifest_after)
    except CONNECTION_ERRORS as e:
        traceback_str = traceback.format_exc()
        sync_record.add_error("Failed to synchronize dataset because of a connection error", e, traceback_str)
        raise e
    except Exception as e:
        traceback_str = traceback.format_exc()
        sync_record.add_error("failed to synchronize dataset with error", e, traceback_str)

    if sync_record.has_errors():
        sync_record.add_log(f"Sync finished (live={live_dataset}) with errors")
    else:
        sync_record.add_log(f"Sync finished (live={live_dataset}) successfully")

    logger.info("Synced %s from %s. %s", s_item.dataIdentifier, sync_source.name, "with errors" if sync_record.has_errors() else "successfully")

    if sync_record.has_errors():
        return False
    return True

def remove_dataset_caches(s_item : SyncItems, session_etiket : Session, sync_record : SyncRecordManager):
    '''
    Remove any files from that dataset with the type HDF5_CACHE (used for live datasets of non-native types).
    
    Args:
        s_item (SyncItems): The sync item object.
        session_etiket (Session): The database session object to the etiket database
        sync_record (SyncRecordManager): The sync record object.
    
    Returns:
        None
    '''
    try: # remove any dataset caches:
        dataset_local = dao_dataset.read(s_item.datasetUUID, session_etiket)
        for file in dataset_local.files or []:
            if file.type == FileType.HDF5_CACHE:
                fs = FileSelect(uuid=file.uuid, version_id=file.version_id)
                dao_file.delete(fs, session_etiket)
                sync_record.add_log(f"Deleted HDF5_CACHE file {file.uuid} from dataset {s_item.datasetUUID}")
    except DatasetNotFoundException:
        pass

def check_live_DS_already_tried(s_item : SyncItems, session_etiket):
    try:
        dataset = dao_dataset.read(s_item.datasetUUID, session_etiket)
        for file in dataset.files or []:
            if file.type == FileType.HDF5_CACHE:
                return True
    except DatasetNotFoundException:
        return False
    return False