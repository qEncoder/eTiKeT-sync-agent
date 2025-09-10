import os, pathlib, xarray, re, typing

from datetime import datetime
from pathlib import Path

from etiket_sync_agent.backends.quantify.live_sync import XArrayReplicator, is_dataset_live
from etiket_sync_agent.backends.quantify.quantify_config_class import QuantifyConfigData
from etiket_sync_agent.sync.sync_source_abstract import SyncSourceFileBase
from etiket_sync_agent.sync.sync_records.manager import SyncRecordManager
from etiket_sync_agent.sync.sync_utilities import dataset_info, file_info, FileType, SyncItems, sync_utilities


class QuantifySync(SyncSourceFileBase):
    SyncAgentName: typing.ClassVar[str] = "Quantify"
    ConfigDataClass: typing.ClassVar[typing.Type[QuantifyConfigData]] = QuantifyConfigData
    MapToASingleScope: typing.ClassVar[bool] = True
    LiveSyncImplemented: typing.ClassVar[bool] = True
    level: typing.ClassVar[int] = 2
    
    @staticmethod
    def rootPath(config_data: QuantifyConfigData) -> pathlib.Path:
        return pathlib.Path(config_data.quantify_directory)

    @staticmethod
    def checkLiveDataset(config_data: QuantifyConfigData, syncIdentifier: SyncItems, maxPriority: bool) -> bool:
        if not maxPriority:
            return False
        
        dataset_dir = Path(os.path.join(config_data.quantify_directory, syncIdentifier.dataIdentifier))

        # Check if any new directories in parent directory have newer modification time
        try:
            parent_dir = dataset_dir.parent
            current_dataset_mtime = dataset_dir.stat().st_mtime
            
            # Check siblings in the parent directory
            for item in parent_dir.parent.iterdir():
                if item.is_dir() and item.name != parent_dir.name:
                    dir_mtime = item.stat().st_mtime
                    if dir_mtime > current_dataset_mtime:
                        return False
            
            # Check siblings in the grandparent directory
            if parent_dir.parent.parent.exists():
                for item in parent_dir.parent.parent.iterdir():
                    if item.is_dir() and item.name != parent_dir.parent.name:
                        dir_mtime = item.stat().st_mtime
                        if dir_mtime > current_dataset_mtime:
                            return False
        except (PermissionError, FileNotFoundError):
            return False
        
        # Also check for any HDF5 files in the dataset directory that might be live
        for root, _, files in os.walk(dataset_dir):
            for file in files:
                if file.endswith(".hdf5") or file.endswith(".h5"):
                    file_path = Path(os.path.join(root, file))
                    if is_dataset_live(file_path):
                        return True
        
        return False
        
    @staticmethod
    def syncDatasetNormal(configData: QuantifyConfigData, syncIdentifier: SyncItems, sync_record: SyncRecordManager):
        with sync_record.task("Creating dataset from Quantify dataset (not live)"):
            create_ds_from_quantify(configData, syncIdentifier, False, sync_record)

        dataset_path = pathlib.Path(os.path.join(configData.quantify_directory, syncIdentifier.dataIdentifier))
        with sync_record.task("Uploading auxiliary files to the server"):
            for root, dirs, files in os.walk(dataset_path):
                for file in files:
                    if not (file.endswith(".hdf5") or file.endswith(".h5")):
                        name, file_path = process_file_name(root, file, dataset_path)
                        if name is None:
                            continue

                        f_type = FileType.UNKNOWN
                        if file.endswith(".json"):
                            f_type = FileType.JSON
                        if file.endswith(".txt"):
                            f_type = FileType.TEXT
                            
                        f_info = file_info(name = name, fileName = file,
                            created = datetime.fromtimestamp(pathlib.Path(os.path.join(root, file)).stat().st_mtime),
                            fileType = f_type, file_generator = "Quantify")
                        
                        sync_utilities.upload_file(file_path, syncIdentifier, f_info, sync_record)

        with sync_record.task("Uploading HDF5 datasets to the server"):
            for root, dirs, files in os.walk(dataset_path):
                for file in files:
                    if file.endswith(".hdf5") or file.endswith(".h5"):
                        name, file_path = process_file_name(root, file, dataset_path)
                        if name is None:
                            continue
                    
                        if is_dataset_live(file_path) is True:
                            replicator = XArrayReplicator(name, file_path, syncIdentifier.datasetUUID)
                            replicator.sync()

                        # upload only if the dataset is not live anymore, assuming the exit of the replicator is triggered on finish
                        if is_dataset_live(file_path) is False:
                            f_info = file_info(name = name, fileName = file,
                                                created = datetime.fromtimestamp(pathlib.Path(os.path.join(root, file)).stat().st_mtime),
                                                fileType = FileType.HDF5_NETCDF, file_generator = "Quantify")
                            ds = xarray.load_dataset(file_path, engine='h5netcdf')
                            
                            # check if fields in the datasets are standard deviations and mark them as such -- this is useful for plotting
                            data_vars = list(ds)
                            for var_name in data_vars:
                                if var_name.endswith("_u") and var_name[:-2] in data_vars:
                                    ds[var_name[:-2]].attrs['__std'] = var_name
                                    ds[var_name].attrs['__is_std'] = 1
                                                    
                            sync_utilities.upload_xarray(ds, syncIdentifier, f_info, sync_record)
                        else:
                            raise Exception("Live dataset is still live after replicator exit")
    
    @staticmethod
    def syncDatasetLive(configData: QuantifyConfigData, syncIdentifier: SyncItems, sync_record: SyncRecordManager):
        with sync_record.task("Creating dataset from Quantify dataset (live)"):
            create_ds_from_quantify(configData, syncIdentifier, True, sync_record)
        dataset_path = pathlib.Path(os.path.join(configData.quantify_directory, syncIdentifier.dataIdentifier))

        with sync_record.task("Starting live replication from Quantify datasets"):
            for root, dirs, files in os.walk(dataset_path):
                for file in files:
                    if file.endswith(".hdf5") or file.endswith(".h5"):
                        name, file_path = process_file_name(root, file, dataset_path)
                        if name is None:
                            continue
                        
                        if is_dataset_live(file_path) is True:
                            replicator = XArrayReplicator(name, file_path, syncIdentifier.datasetUUID)
                            replicator.sync()

def process_file_name(file_dir : str, file_name : str, dataset_path : str) -> typing.Tuple[str, pathlib.Path]:
    if file_name.startswith("."):
        return None, None

    relative_path = os.path.relpath(os.path.join(file_dir, file_name), start=dataset_path)
    name_parts = [re.sub(r"\d{8}-\d{6}-\d{3}-[a-z0-9]{6}-", "", part)
                    for part in pathlib.Path(relative_path).parts]
    reformatted_file_name = ".".join(name_parts)
    file_path = pathlib.Path(os.path.join(file_dir, file_name))
    return reformatted_file_name, file_path

def create_ds_from_quantify(configData: QuantifyConfigData, syncIdentifier: SyncItems, live : bool, sync_record: SyncRecordManager):
    sync_record.add_log("Extracting metadata from Quantify dataset: " + syncIdentifier.dataIdentifier)
    tuid = syncIdentifier.dataIdentifier.split('/')[1][:26]
    name = syncIdentifier.dataIdentifier.split('/')[1][27:]
    created = datetime.strptime(tuid[:18], "%Y%m%d-%H%M%S-%f")
    
    # get variable names in the dataset, this is handy for searching!
    keywords = set()
    
    # loop through all datasets in the folder os.path.join(configData.quantify_directory, syncIdentifier.dataIdentifier) (not recursive) and get the keywords
    for file in os.listdir(os.path.join(configData.quantify_directory, syncIdentifier.dataIdentifier)):
        try:
            if file.endswith(".hdf5") or file.endswith(".h5"):
                with xarray.load_dataset(os.path.join(configData.quantify_directory, syncIdentifier.dataIdentifier, file), engine='h5netcdf') as xr_ds:
                    for key in xr_ds.keys():
                        if 'long_name' in xr_ds[key].attrs.keys():
                            keywords.add(xr_ds[key].attrs['long_name'])
                            continue
                        if 'name' in xr_ds[key].attrs.keys():
                            keywords.add(xr_ds[key].attrs['name'])

                    for key in xr_ds.coords:
                        if 'long_name' in xr_ds[key].attrs.keys():
                            keywords.add(xr_ds[key].attrs['long_name'])
                            continue
                        if 'name' in xr_ds[key].attrs.keys():
                            keywords.add(xr_ds[key].attrs['name'])  
        except Exception as e:
            print(f"Error loading dataset: {e}")
    
    sync_record.add_log("Creating dataset info")
    ds_info = dataset_info(name = name, datasetUUID = syncIdentifier.datasetUUID,
                alt_uid = tuid, scopeUUID = syncIdentifier.scopeUUID,
                created = created, keywords = list(keywords), 
                attributes = {"set-up" : configData.set_up}, creator=syncIdentifier.creator)
    sync_utilities.create_or_update_dataset(live, syncIdentifier, ds_info, sync_record)