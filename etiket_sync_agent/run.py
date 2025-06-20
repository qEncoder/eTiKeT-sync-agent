import logging, time

from typing import Type
from pathlib import Path

from etiket_client.exceptions import NoLoginInfoFoundException
from etiket_client.local.models.file import FileSelect

from etiket_client.local.dao.file import dao_file, dao_file_delete_queue
from etiket_client.local.dao.dataset import dao_dataset
from etiket_client.local.exceptions import DatasetNotFoundException
from etiket_client.remote.client import client, user_settings
from etiket_client.remote.api_tokens import api_token_session
from etiket_client.remote.endpoints.models.types import FileType
from etiket_sync_agent.backends.native.sync_agent import run_native_sync
from etiket_sync_agent.sync.manifest_mgr import manifest_manager
from etiket_sync_agent.sync.sync_source_abstract import SyncSourceDatabaseBase, SyncSourceFileBase
from etiket_sync_agent.crud.sync_items import crud_sync_items
from etiket_sync_agent.crud.sync_sources import crud_sync_sources
from etiket_sync_agent.crud.sync_status import crud_sync_status
from etiket_sync_agent.crud.manifest import crud_manifest

from etiket_sync_agent.models.enums import SyncSourceStatus, SyncStatus
from etiket_sync_agent.models.sync_items import SyncItems
from etiket_sync_agent.models.sync_sources import SyncSources, SyncSourceTypes
from etiket_sync_agent.backends.sources import get_source_sync_class
from etiket_sync_agent.backends.native.sync_scopes import sync_scopes
from etiket_sync_agent.backends.dataset_manifest import DatasetManifest

from etiket_client.remote.utility import check_internet_connection
from etiket_client.local.database import Session 
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
    SYNC_SOURCE_ERROR_DELAY = 60  # delay of 60 seconds when a sync source is in error

def sync_loop():
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
    running = True
    
    while running == True:
        with Session() as session:
            status = crud_sync_status.get_or_create_status(session)
            if status.status == SyncStatus.STOPPED:
                print("is stopped")
                time.sleep(1) #check again in 1 second
                continue
            
            try:
                current_time = time.time()
                # try to use API token for this (more robust)
                with api_token_session(user_settings.user_sub):
                    if current_time - last_sync_time >= SyncConf.USER_CHECK_INTERVAL:
                        client.check_user_session()
                    if current_time - last_sync_time >= SyncConf.SCOPE_SYNC_INTERVAL:
                        sync_scopes(session)
                    if current_time - last_sync_time >= SyncConf.DELETE_HDF5_CACHE_INTERVAL:
                        dao_file_delete_queue.clean_files(session)
                    last_sync_time = current_time
                    # assuming when getting here, the sync is running.
                    crud_sync_status.update_status(session, SyncStatus.RUNNING)
                    run_sync_iter(session)
            except NoLoginInfoFoundException:
                logger.info("No access token found, waiting for 10 seconds.")
                time.sleep(10)
                crud_sync_status.update_status(session, SyncStatus.NOT_LOGGED_IN)
            except Exception as e:
                if check_internet_connection() == True:
                    crud_sync_status.update_status(session, SyncStatus.ERROR, str(e))
                    logger.exception('Sync loop broken!')
                    time.sleep(SyncConf.IDLE_DELAY)
                else:
                    crud_sync_status.update_status(session, SyncStatus.NO_CONNECTION)
                    logger.warning("No connection, waiting for 60 seconds.")
                time.sleep(SyncConf.CONNECTION_ERROR_DELAY)

def run_sync_iter(session):
    n_syncs = 0
    sync_sources = crud_sync_sources.list_sync_sources(session)
    
    try:
        logger.info("Syncing native datasets")
        items_synced = run_native_sync(session)
        n_syncs += items_synced
    except CONNECTION_ERRORS as e:
        raise e
    except Exception:
        logger.exception("Failed to sync native datasets")

    for sync_source in sync_sources:
        if sync_source.status == SyncSourceStatus.PAUSED:
            continue
        
        if (sync_source.status == SyncSourceStatus.ERROR and
            sync_source.last_update.timestamp()+ SyncConf.SYNC_SOURCE_ERROR_DELAY) < time.time():
            # if the sync source is in error and has been updated in the last minute, skip it.
            continue
        
        sync_cls = get_source_sync_class(sync_source.type)
        
        with api_token_session(sync_source.creator):
            try:
                logger.info("Syncing %s, of type %s", sync_source.name, sync_source.type)
                
                # check if new sync items are available (i.e. new datasets)
                try:
                    get_new_sync_items(sync_source, sync_cls, session)
                except Exception as e :
                    # TODO write error to sync source.
                    crud_sync_sources.update_sync_source(session, sync_source.id, status=SyncSourceStatus.ERROR)
                    raise e
                
                # check if there is a new item to sync, determine if it is a live dataset.
                s_item = crud_sync_items.read_next_sync_item(session, sync_source.id, offset = 0)
                
                if s_item is not None:
                    last_s_item = crud_sync_items.get_last_sync_item(session, s_item.sync_source_id)
                    liveDS = sync_cls.checkLiveDataset(sync_source.sync_config, s_item, last_s_item.id==s_item.id)
                    
                    # this is needed to not get stuck in a loop of live datasets (though not the cleanest way)
                    liveDS_already_tried = check_live_DS_already_tried(s_item, session)
                    if (liveDS is True) and (sync_source.sync_class.LiveSyncImplemented is False or liveDS_already_tried):
                        # assume only one live dataset at a time.
                        s_item = crud_sync_items.read_next_sync_item(session, sync_source.id, offset = 1)
                        liveDS = False
                
                if s_item is None:
                    crud_sync_sources.update_sync_source(session, sync_source.id,
                                                            status=SyncSourceStatus.SYNCHRONIZED)
                    logger.info("No new items to sync from %s.", sync_source.name)
                    continue
                
                crud_sync_sources.update_sync_source(session, sync_source.id,
                                                            status=SyncSourceStatus.SYNCHRONIZING)
                
                if liveDS is True:
                    logger.info("Syncing live dataset, %s from %s.", s_item.dataIdentifier, sync_source.name)
                    success = sync_dataset(sync_source, s_item, liveDS)
                    logger.info("Synced live dataset, %s from %s.", s_item.dataIdentifier, sync_source.name)

                                            
                logger.info("Syncing %s from %s.", s_item.dataIdentifier, sync_source.name)                        
                success = sync_dataset(sync_source, s_item, False)
                
                try: # remove any dataset caches:
                    dataset_local = dao_dataset.read(s_item.datasetUUID, session)
                    for file in dataset_local.files or []:
                        if file.type == FileType.HDF5_CACHE:
                            fs = FileSelect(uuid=file.uuid, version_id=file.version_id)
                            dao_file.delete(fs, session)
                except DatasetNotFoundException:
                    pass
                
                if success:
                    n_syncs += 1
                    logger.info("Synced %s from %s.", s_item.dataIdentifier, sync_source.name)

                    crud_sync_items.update_sync_item(session, s_item.id, synchronized=True)
                    crud_sync_sources.update_sync_source(session, sync_source.id,
                                                        status=SyncSourceStatus.SYNCHRONIZING)
                else:
                    logger.exception("Failed to synchronize %s from %s.", s_item.dataIdentifier, sync_source.name)
                    crud_sync_items.update_sync_item(session, s_item.id, attempts=s_item.attempts+1)
                    crud_sync_sources.update_sync_source(session, sync_source.id,
                                                        status=SyncSourceStatus.SYNCHRONIZING,
                                                        update_statistics=True)
            except CONNECTION_ERRORS as e:
                crud_sync_sources.update_sync_source(session, sync_source.id,
                                                        status=SyncSourceStatus.ERROR)
                raise e
            except Exception:
                logger.exception("Failed to sync %s", sync_source.name)
    if n_syncs == 0:
        time.sleep(1)

def get_new_sync_items(sync_source : SyncSources, sync_cls : Type[SyncSourceDatabaseBase] | Type[SyncSourceFileBase], session):
    '''
    Check for new sync items in the sync source.
    
    Args:
        sync_source (SyncSources): The sync source object.
        sync_cls (Type[SyncSourceDatabaseBase] | Type[SyncSourceFileBase]): The sync class object.
        session (Session): The database session object.
        
    Returns:
        None
    '''
    if issubclass(sync_cls, SyncSourceDatabaseBase):
        last_s_item = crud_sync_items.get_last_sync_item(session, sync_source.id)
        new_items = sync_cls.getNewDatasets(sync_source.sync_config, last_s_item)
        logger.info("Found %s new items in remote location, to add to the list of things to synchronize.", len(new_items))
        crud_sync_items.create_sync_items(session, sync_source.id, new_items)
    else:
        current_manifest = crud_manifest.read_manifest(session, sync_source.id)

        is_NFS = sync_source.sync_config.server_folder if hasattr(sync_source.sync_config, 'server_folder') else False
        manifest_mgr = manifest_manager(sync_source.name, sync_source.sync_class.rootPath(sync_source.sync_config),
                                            current_manifest, level = sync_source.sync_class.level,
                                            is_NFS=is_NFS)
        
        new_manifests = manifest_mgr.get_updates()
        
        logger.info("Found %s new datasets (file base), will add these in the sync queue.", len(new_manifests))
        new_sync_items = []
        for identifier, priority in new_manifests.items():
            new_sync_items.append(SyncItems(dataIdentifier=identifier, syncPriority=priority))
        crud_sync_items.create_sync_items(session, sync_source.id, new_sync_items)

def sync_dataset(sync_source : SyncSources, s_item : SyncItems, liveDS : bool) -> bool:
    '''
    Sync a dataset normally (not live).
    
    Args:
        sync_source (SyncSources): The sync source object.
        s_item (SyncItems): The sync item object.
        liveDS (bool): True if the dataset is a live dataset, False otherwise.
    Returns:
        bool: True if the sync was successful, False otherwise.
    '''
    if sync_source.sync_class.type == SyncSourceTypes.fileBase:
        dataset_manifest = DatasetManifest(s_item, Path(s_item.dataIdentifier))
    else:
        dataset_manifest = DatasetManifest(s_item)
    
    if liveDS:
        dataset_manifest.add_log("Start of sync (normal)")
    else:
        dataset_manifest.add_log("Start of sync (live)")
    
    try:
        if liveDS:
            sync_source.sync_class.syncDatasetLive(sync_source.sync_config, s_item, dataset_manifest)
        elif issubclass(sync_source.sync_class, SyncSourceDatabaseBase):
            sync_source.sync_class.syncDatasetNormal(sync_source.sync_config, s_item, dataset_manifest)
        else:
            manifest_mgr = manifest_manager(sync_source.name)
            manifest_before =  manifest_mgr.get_last_change(s_item.dataIdentifier)
            sync_source.sync_class.syncDatasetNormal(sync_source.sync_config, s_item, dataset_manifest)
            manifest_after = manifest_mgr.get_last_change(s_item.dataIdentifier)
            if manifest_before != manifest_after:
                manifest_mgr.push_update(s_item.dataIdentifier, manifest_after)
    except CONNECTION_ERRORS as e:
        raise e
    except Exception as e:
        # any error should be reported in the manifest.
        dataset_manifest.add_error(e)

    if dataset_manifest.has_errors():
        dataset_manifest.add_log(f"Sync finished (live={liveDS}) with errors")
    else:
        dataset_manifest.add_log(f"Sync finished (live={liveDS}) successfully")
    dataset_manifest.write()

    if dataset_manifest.has_errors():
        return False
    return True

def check_live_DS_already_tried(s_item : SyncItems, session):
    try:
        dataset = dao_dataset.read(s_item.datasetUUID, session)
        for file in dataset.files or []:
            if file.type == FileType.HDF5_CACHE:
                return True
    except DatasetNotFoundException:
        return False
    return False