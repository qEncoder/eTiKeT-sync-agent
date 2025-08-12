import os, pathlib, dataclasses, json

from datetime import datetime

from etiket_client.sync.base.sync_source_abstract import SyncSourceFileBase
from etiket_client.sync.base.sync_utilities import file_info, sync_utilities,\
    dataset_info, sync_item
from etiket_client.remote.endpoints.models.types import FileType
from etiket_client.sync.backends.labber.labber_config_class import LabberConfigData
from etiket_client.sync.backends.labber.labber_ds.to_xarray import to_xarray
from etiket_client.sync.backends.labber.labber_ds.dataset import read_labber_file
from etiket_client.sync.backends.labber.labber_ds.model import LabberDataset, LabberDataContent

class LabberSync(SyncSourceFileBase):
    SyncAgentName = "Labber"
    ConfigDataClass = LabberConfigData
    MapToASingleScope = True
    LiveSyncImplemented = True
    level = 4
    is_single_file = True
    
    @staticmethod
    def rootPath(configData: LabberConfigData) -> pathlib.Path:
        return pathlib.Path(configData.labber_directory)

    @staticmethod
    def checkLiveDataset(configData: LabberConfigData, syncIdentifier: sync_item, maxPriority: bool) -> bool:
        if not maxPriority:
            return False

        file_path = os.path.join(configData.labber_directory, syncIdentifier.dataIdentifier)
        if not os.path.exists(file_path) or os.path.getsize(file_path) < 100:
            return True
        return False
        
    @staticmethod
    def syncDatasetNormal(configData: LabberConfigData, syncIdentifier: sync_item):
        labber_file_path = pathlib.Path(configData.labber_directory) / syncIdentifier.dataIdentifier
        if not labber_file_path.suffix == '.hdf5':
            return
        
        if not labber_file_path.exists():
            raise FileNotFoundError(f"File {labber_file_path} not found")
        
        labber_ds = read_labber_file(labber_file_path)
                
        create_ds_from_labber(labber_ds, labber_file_path, syncIdentifier, configData, False)

        f_info = file_info(name = 'original', fileName = 'original.hdf5',
                            created = datetime.fromtimestamp(pathlib.Path(labber_file_path).stat().st_mtime),
                            fileType = FileType.HDF5, file_generator = "Labber")
        sync_utilities.upload_file(labber_file_path, syncIdentifier, f_info)
        
        # upload json contents:
        settings = labber_ds.settings

        f_info = file_info(name = 'settings', fileName = 'settings.json',
                            created = datetime.fromtimestamp(pathlib.Path(labber_file_path).stat().st_mtime),
                            fileType = FileType.JSON, file_generator = "Labber")
        sync_utilities.upload_JSON(settings, syncIdentifier, f_info)
        
        if len(labber_ds.dataset_content) >= 1:
            content = labber_ds.dataset_content[0]
            instruments = content.instruments
            
            f_info = file_info(name = 'instruments', fileName = 'instruments.json',
                            created = datetime.fromtimestamp(pathlib.Path(labber_file_path).stat().st_mtime),
                            fileType = FileType.JSON, file_generator = "Labber")
            sync_utilities.upload_JSON(instruments, syncIdentifier, f_info)
        
        if len(labber_ds.dataset_content) == 1:
            f_info = file_info(name = 'ds_converted', fileName = 'ds_converted.hdf5',
                            created = datetime.fromtimestamp(pathlib.Path(labber_file_path).stat().st_mtime),
                            fileType = FileType.HDF5_NETCDF, file_generator = "Labber")
            xr_ds = convert_to_xarray(labber_ds.dataset_content[0])
            sync_utilities.upload_xarray(xr_ds, syncIdentifier, f_info)
        else:
            for i in range(len(labber_ds.dataset_content)):
                f_info = file_info(name = f'ds_converted_{i}', fileName = f'ds_converted_{i}.hdf5',
                                created = datetime.fromtimestamp(pathlib.Path(labber_file_path).stat().st_mtime),
                                fileType = FileType.HDF5_NETCDF, file_generator = "Labber")
                xr_ds = convert_to_xarray(labber_ds.dataset_content[i])
                sync_utilities.upload_xarray(xr_ds, syncIdentifier, f_info)

    @staticmethod
    def syncDatasetLive(configData: LabberConfigData, syncIdentifier: sync_item):
        raise NotImplementedError


def create_ds_from_labber(labber_ds : LabberDataset, dataset_path : pathlib.Path, syncIdentifier: sync_item,
                            configData : LabberConfigData, live : bool = False):
    keywords = set()
    
    for _, values in labber_ds.tags.items():
        for v in values:
            keywords.add(v)
    
    if len(labber_ds.dataset_content) > 0:
        content = labber_ds.dataset_content[0]
        for data_item in content.data:
            keywords.add(data_item.name)
            keywords.add(data_item.unit)
        for name, trace in content.traces.items():
            keywords.add(trace.name)
            keywords.add(trace.unit)
            keywords.add(trace.setpoint_name)
            keywords.add(trace.setpoint_unit)
            
    keywords = {k for k in keywords if k}  # Remove empty strings
    
    is_starred = labber_ds.is_starred
    
    alt_uid = labber_ds.dataset_name + "_" + str(labber_ds.creation_time.timestamp())
    
    ds_info = dataset_info(name = labber_ds.dataset_name,
                alt_uid = alt_uid,
                datasetUUID = syncIdentifier.datasetUUID,
                scopeUUID = syncIdentifier.scopeUUID,
                created = labber_ds.creation_time, keywords = list(keywords), 
                ranking=is_starred,
                attributes = {"set-up" : configData.set_up}, creator=syncIdentifier.creator)
    sync_utilities.create_or_update_dataset(live, syncIdentifier, ds_info)

def convert_to_xarray(labber_ds_content : LabberDataContent):
    attributes = {}
    attributes['instrument_config'] = labber_ds_content.instrument_config
    attributes['log_list'] = labber_ds_content.log_list
    step_config = labber_ds_content.step_config
    attributes['step_config'] = {}
    for step_name in step_config:
        attributes['step_config'][step_name] = dataclasses.asdict(step_config[step_name])
    attributes['step_list'] = labber_ds_content.step_list
    
    xr_ds = to_xarray(labber_ds_content)
    for key, value in attributes.items():
        xr_ds.attrs[key] = json.dumps(value)
    return xr_ds