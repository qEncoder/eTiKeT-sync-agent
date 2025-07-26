import typing, sqlite3, qcodes as qc, os, logging

from datetime import datetime

from etiket_sync_agent.sync.sync_source_abstract import SyncSourceDatabaseBase
from etiket_sync_agent.sync.sync_records.manager import SyncRecordManager
from etiket_sync_agent.sync.sync_utilities import file_info, sync_utilities,\
    dataset_info, SyncItems, FileType

from etiket_sync_agent.backends.qcodes.real_time_sync import QCoDeS_live_sync
from etiket_sync_agent.backends.qcodes.qcodes_config_class import QCoDeSConfigData
from etiket_sync_agent.backends.utility.extract_metadata_from_QCoDeS import extract_labels_and_attributes_from_snapshot, MetaDataExtractionError

from qcodes.dataset import load_by_id
from qcodes.dataset.data_set import DataSet

logger = logging.getLogger(__name__)

class QCoDeSSync(SyncSourceDatabaseBase):
    SyncAgentName: typing.ClassVar[str] = "QCoDeS"
    ConfigDataClass: typing.ClassVar[typing.Type[QCoDeSConfigData]] = QCoDeSConfigData
    MapToASingleScope: typing.ClassVar[bool] = True
    LiveSyncImplemented: typing.ClassVar[bool] = True
    
    @staticmethod
    def getNewDatasets(configData: QCoDeSConfigData, lastIdentifier: str) -> typing.List[SyncItems] | None:
        if not os.path.exists(configData.database_path):
            raise FileNotFoundError(f"Database file not found at {configData.database_path}")
        
        qc.config.core.db_location = str(configData.database_path)
        
        newSyncIdentifiers = []
        
        with sqlite3.connect(configData.database_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            if not lastIdentifier:
                lastIdentifier = 0         
            
            get_newer_guid_query = """SELECT run_id FROM runs WHERE run_id > ? ORDER BY run_id ASC"""
            
            cursor = conn.cursor()
            cursor.execute(get_newer_guid_query, (int(lastIdentifier),))
            rows = cursor.fetchall()
            
            newSyncIdentifiers += [SyncItems(dataIdentifier=str(row[0])) for row in rows]
        return newSyncIdentifiers
    
    @staticmethod
    def checkLiveDataset(configData: QCoDeSConfigData, syncIdentifier: SyncItems, maxPriority: bool) -> bool:
        if maxPriority is False:
            return False
            
        ds_qc = load_by_id(int(syncIdentifier.dataIdentifier))
        return not ds_qc.completed
    
    @staticmethod
    def syncDatasetNormal(configData: QCoDeSConfigData, syncIdentifier: SyncItems, sync_record: SyncRecordManager):
        with sync_record.task("Loading dataset from QCoDeS"):
            ds_qc = create_ds_from_qcodes(configData, syncIdentifier, False, sync_record)
            sync_record.add_log("Converting dataset to xarray format")
            ds_xr = ds_qc.to_xarray_dataset()
        
        if ds_qc.run_timestamp_raw is not None:
            created_time = datetime.fromtimestamp(ds_qc.run_timestamp_raw)
        else:
            raise ValueError("Run timestamp is None")
        
        f_info = file_info(name = 'measurement', fileName = 'measured_data.hdf5',
                            fileType= FileType.HDF5_NETCDF,
                            created = created_time, file_generator = "QCoDeS")
        sync_utilities.upload_xarray(ds_xr, syncIdentifier, f_info, sync_record)

    @staticmethod
    def syncDatasetLive(configData: QCoDeSConfigData, syncIdentifier: SyncItems, sync_record: SyncRecordManager):
        create_ds_from_qcodes(configData, syncIdentifier, True, sync_record)
        sync_record.add_log("Starting live feed to the remote server.")
        QCoDeS_live_sync(int(syncIdentifier.dataIdentifier), str(configData.database_path), syncIdentifier.datasetUUID)
        sync_record.add_log("Live feed to the remote server completed.")

def create_ds_from_qcodes(configData: QCoDeSConfigData, syncIdentifier: SyncItems, live : bool, sync_record: SyncRecordManager) -> DataSet:
    sync_record.add_log("Loading dataset and extracting metadata from QCoDeS with ID: " + syncIdentifier.dataIdentifier)
    ds_qc = load_by_id(int(syncIdentifier.dataIdentifier))
    sync_record.add_log("Dataset loaded from QCoDeS, will start extracting metadata")

    if ds_qc.run_timestamp_raw is not None:
        collected_time = datetime.fromtimestamp(ds_qc.run_timestamp_raw)
    else:
        raise ValueError("Run timestamp is None")
    
    ranking = 0
    if 'inspectr_tag' in ds_qc.metadata.keys():
        if ds_qc.metadata['inspectr_tag']=='star': ranking=1
        if ds_qc.metadata['inspectr_tag']=='cross': ranking=-1
        
    # get variable names in the dataset, this is handy for searching!
    ds_xr = ds_qc.to_xarray_dataset()
    keywords = set()
    try:
        for key in ds_xr.keys():
            if 'long_name' in ds_xr[key].attrs.keys():
                keywords.add(ds_xr[key].attrs['long_name'])
                continue
            if 'name' in ds_xr[key].attrs.keys():
                keywords.add(ds_xr[key].attrs['name'])

        for key in ds_xr.coords:
            if 'long_name' in ds_xr[key].attrs.keys():
                keywords.add(ds_xr[key].attrs['long_name'])
                continue
            if 'name' in ds_xr[key].attrs.keys():
                keywords.add(ds_xr[key].attrs['name'])
    except Exception:
        pass
    
    name_lines = ds_qc.name.splitlines()
    name = name_lines[0] if name_lines else ""

    additional_description = "\n".join(name_lines[1:]) if len(name_lines) > 1 else ""
    description = f"database : {os.path.basename(configData.database_path)} | run ID : {ds_qc.run_id} | GUID : {ds_qc.guid} | exp name : {ds_qc.exp_name}"
    
    if additional_description:
        description += f"\n\n{additional_description}"
    
    attributes = {"sample" : ds_qc.sample_name,
                    "set-up" : configData.set_up}
    if configData.extra_attributes is not None:
        attributes.update(configData.extra_attributes) 
    
    try: # experimental
        if ds_qc.snapshot is not None:
            extra_labels, extra_attributes = extract_labels_and_attributes_from_snapshot(ds_qc.snapshot)
        else:
            extra_labels = []
            extra_attributes = {}
    except MetaDataExtractionError:
        logger.exception("Could not extract labels and attributes from snapshot.")
        extra_labels = []
        extra_attributes = {}
    
    keywords.update(extra_labels)
    attributes.update(extra_attributes)   
    
    sync_record.add_log("Creating dataset info")
    ds_info = dataset_info(name = name, datasetUUID = syncIdentifier.datasetUUID,
                alt_uid = ds_qc.guid, scopeUUID = syncIdentifier.scopeIdentifier,
                created = collected_time, keywords = list(keywords),  description = description,
                ranking=ranking, creator=syncIdentifier.creator,
                # exp_name not added, since some people use it to name their experiments ...
                attributes = attributes)
    sync_utilities.create_or_update_dataset(live, syncIdentifier, ds_info, sync_record)
    return ds_qc