import fnmatch, os, pathlib, re, logging, yaml, xarray, traceback

from datetime import datetime
from pathlib import Path
from typing import Dict, List

from etiket_sync_agent.backends.folderbase.converters.base import FileConverterHelper
from etiket_sync_agent.backends.folderbase.folderbase_config_class import FolderBaseConfigData
from etiket_sync_agent.backends.folderbase.local_sync_record import LocalSyncRecord
from etiket_sync_agent.exceptions.sync import NoConvertorException
from etiket_sync_agent.sync.manifests.v2.definitions import QH_DATASET_INFO_FILE, QH_MANIFEST_FILE
from etiket_sync_agent.sync.sync_source_abstract import SyncSourceFileBase
from etiket_sync_agent.sync.sync_records.manager import SyncRecordManager
from etiket_sync_agent.sync.sync_utilities import dataset_info, file_info, FileType, SyncItems, sync_utilities

logger = logging.getLogger(__name__)

# in the config file, the converter should be named as ExtensionA_to_ExtensionB_converter
converter_naming_scheme = re.compile(r'^(\w+)_to_(\w+)_converter$')

class FolderBaseSync(SyncSourceFileBase):
    SyncAgentName = "FolderBaseGeneric"
    ConfigDataClass = FolderBaseConfigData
    MapToASingleScope = True
    LiveSyncImplemented = False
    level = -1
    
    @staticmethod
    def rootPath(configData: FolderBaseConfigData) -> Path:
        return Path(configData.root_directory)

    @staticmethod
    def checkLiveDataset(configData: FolderBaseConfigData, syncIdentifier: SyncItems, maxPriority: bool) -> bool:
        return False
    
    @staticmethod
    def syncDatasetNormal(configData: FolderBaseConfigData, syncIdentifier: SyncItems, sync_record: SyncRecordManager):
        dataset_path = Path(configData.root_directory) / syncIdentifier.dataIdentifier

        with sync_record.task("Read dataset info file and manifest file."):
            if not (dataset_path / QH_DATASET_INFO_FILE).exists():
                raise FileNotFoundError(f"Dataset info file not found for dataset: {syncIdentifier.dataIdentifier}")
            with open(dataset_path / QH_DATASET_INFO_FILE, 'r', encoding="utf-8") as f:
                sync_info = yaml.safe_load(f)
            
            local_sync_record = LocalSyncRecord(dataset_path, syncIdentifier, sync_record)
        
        # TODO check traversal --> unnecessary if skip is used
        try:
            with sync_record.task("Create or update dataset."):
                create_or_update_dataset(configData, syncIdentifier, sync_info, sync_record)
                
            with sync_record.task("Check files."):
                file_converters = get_file_converters(sync_info, sync_record)
                skip = sync_info.get('skip', [])
                skip_folders = ["*.zarr"]
                
                for root, _, files in os.walk(dataset_path):
                    # Do not check files in the skip_folders
                    if not any(fnmatch.fnmatch(root, pattern) or fnmatch.fnmatch(root, f"{pattern}/*") for pattern in skip_folders):
                        for file in files:
                            if not any(fnmatch.fnmatch(file, pattern) for pattern in skip):
                                upload_file(root, file, dataset_path, syncIdentifier, sync_record, file_converters)
                            else:
                                logger.info(f"File {file} is skipped, as per the skip list.")
                    # do allow folder uploads if a converter is present
                    if any(fnmatch.fnmatch(root, pattern) for pattern in skip_folders):
                        folder_name =  os.path.basename(root)
                        upload_folder(root, dataset_path, syncIdentifier, sync_record, file_converters)
        finally:
            local_sync_record.write()
    @staticmethod
    def syncDatasetLive(configData: FolderBaseConfigData, syncIdentifier: SyncItems, sync_record: SyncRecordManager):
        raise NotImplementedError("Live sync is not implemented for FolderBaseSync")

def create_or_update_dataset(configData : FolderBaseConfigData, syncIdentifier : SyncItems, sync_info : dict, sync_record: SyncRecordManager):
    """
    Create or update the dataset based on synchronization information.

    This function gathers the dataset name, attributes, creation time, keywords, and identifiers
    and creates/updates the dataset as needed in the remote server.
    
    Args:
        configData (FileBaseData): Configuration data containing the root directory.
        syncIdentifier (sync_item): Synchronization identifier with dataset UUIDs and identifiers.
        sync_info (dict): Dictionary containing synchronization information from the dataset info file.
    """
    dataset_path = pathlib.Path(configData.root_directory) / syncIdentifier.dataIdentifier

    name = sync_info.get("dataset_name", dataset_path.name)
    attributes = sync_info.get('attributes', {})
    description = sync_info.get('description', None)
    # get creation time of first file in the folder
    created = get_created_time(dataset_path)
    
    if description is None:
        description = f"Dataset {name} from FileBaseGeneric"
    else:
        description += f"\n\nDataset source path: {dataset_path}"
    
    if 'created' in sync_info:
        try :
            created_time_str = sync_info['created']
            created = datetime.strptime(created_time_str, '%Y-%m-%dT%H:%M:%S')
        except Exception:
            pass
        
    keywords = sync_info.get('keywords', [])
    ds_info =  dataset_info(name = name, datasetUUID = syncIdentifier.datasetUUID,
                alt_uid = None, scopeUUID = syncIdentifier.scopeUUID,
                description=description,
                created = created, keywords = list(keywords), 
                attributes = attributes)
    
    sync_utilities.create_or_update_dataset(False, syncIdentifier, ds_info, sync_record)
    
def get_created_time(dataset_path : pathlib.Path) -> datetime:
    """
    Get the earliest modification time among all files in the dataset.

    This function traverses all files within the dataset directory (excluding specific manifest files and hidden files (UNIX))
    to find the earliest modification time, which is considered the creation time of the dataset.

    Args:
        dataset_path (Path): Path to the dataset directory.

    Returns:
        datetime: The earliest modification time of the dataset files.
    """
    min_time = datetime.timestamp(datetime.now())

    for root, _, files in os.walk(dataset_path):
        for file in files:
            file_path = pathlib.Path(root) / file
            if file_path.name in [QH_DATASET_INFO_FILE, QH_MANIFEST_FILE] or file_path.name.startswith('.'):
                continue
            if file_path.stat().st_mtime < min_time:
                min_time = file_path.stat().st_mtime
    return datetime.fromtimestamp(min_time)

def process_name(dataset_path : Path, current_path : Path) -> str:
    # if root is /A/B and current is /A/B/C/D.txt then return C/D.txt
    return str(current_path.relative_to(dataset_path))

def generate_converted_file_name(dataset_path : Path, current_path : Path, new_extension : str) -> str:
    # get processed name
    relative_path = current_path.relative_to(dataset_path)
    if relative_path.suffix:
        new_name = relative_path.with_suffix(f'.{new_extension}')
    else:
        new_name = relative_path.parent / f"{relative_path.name}.{new_extension}"
    return str(new_name)

def upload_file(root : str, file : str, dataset_path : pathlib.Path,
                syncIdentifier : SyncItems, sync_record: SyncRecordManager,
                file_converters : Dict[str, List[FileConverterHelper]]):
    file_path = pathlib.Path(root) / file
    name = process_name(dataset_path, file_path)
    if file in [QH_DATASET_INFO_FILE, QH_MANIFEST_FILE] or file.startswith('.'):
        return

    f_type = get_file_type(file_path)
    
    f_info = file_info(name = name, fileName = file,
        created = datetime.fromtimestamp(pathlib.Path(os.path.join(root, file)).stat().st_mtime),
        fileType = f_type, file_generator = "")
    sync_utilities.upload_file(file_path, syncIdentifier, f_info, sync_record)

    # check if there is a converter for the file
    extension = file_path.suffix.lstrip('.')
    if extension in file_converters:
        for converterHelper in file_converters[extension]:
            converted_file_name = generate_converted_file_name(dataset_path, file_path, converterHelper.converter.output_type)
            with sync_record.add_upload_task(converted_file_name) as file_upload_info:
                with sync_record.define_converter(file_upload_info, converterHelper.converter, file_path) as converted_file_path:
                    f_type = get_file_type(converted_file_path)
                    f_info = file_info(name = converted_file_name, fileName = converted_file_path.name,
                                created = datetime.now(),
                                fileType = f_type, file_generator = "")
                
                    sync_utilities.upload_file(converted_file_path, syncIdentifier, f_info, sync_record)

def upload_folder(root : str, dataset_path : pathlib.Path, syncIdentifier : SyncItems, sync_record: SyncRecordManager,
                file_converters : Dict[str, List[FileConverterHelper]]):
    '''
    Function that uploads the contents of a folder as a file. Currently, this is only needed for zarr files.
    '''
    folder_path = pathlib.Path(root)    
    extension = folder_path.suffix[1:]

    if extension in file_converters:
        for converterHelper in file_converters[extension]:
            converted_file_name = generate_converted_file_name(dataset_path, folder_path, converterHelper.converter.output_type)
            with sync_record.add_upload_task(converted_file_name) as file_upload_info:
                with sync_record.define_converter(file_upload_info, converterHelper.converter, folder_path) as converted_file_path:
                    f_type = get_file_type(converted_file_path)
                    f_info = file_info(name = converted_file_name, fileName = converted_file_path.name,
                                created = datetime.now(),
                                fileType = f_type, file_generator = "")
                    
                    sync_utilities.upload_file(converted_file_path, syncIdentifier, f_info, sync_record)

def get_file_type(file_path : Path) -> FileType:
    f_type = FileType.UNKNOWN
    if file_path.name.endswith(".json"):
        f_type = FileType.JSON
    if file_path.name.endswith(".txt"):
        f_type = FileType.TEXT
    if file_path.name.endswith(".hdf5") or file_path.name.endswith(".h5") or file_path.name.endswith(".nc"):
        try:
            xr_ds = xarray.open_dataset(file_path, engine='h5netcdf', invalid_netcdf=True)
            xr_ds.close()
            f_type = FileType.HDF5_NETCDF
        except Exception:
            f_type = FileType.HDF5
    return f_type

def get_file_converters(sync_info : dict, sync_record: SyncRecordManager) -> Dict[str, List[FileConverterHelper]]:
    converters_to_load = sync_info.get('converters', {})
    if len(converters_to_load) == 0:
        sync_record.add_log("No file converters found in the dataset info file.")
        return {}
    with sync_record.task("Loading file converters."):
        converters = {}

        for key in converters_to_load.keys():
            if converter_naming_scheme.match(key):
                try:
                    module_name = converters_to_load[key]['module']
                    class_name = converters_to_load[key]['class']
                    converter_helper = FileConverterHelper(key, module_name, class_name)
                    extension = converter_helper.converter.input_type.lower()
                    converters.setdefault(extension, []).append(converter_helper)
                    sync_record.add_log(f"Loaded converter {key}")
                except KeyError as e:
                    logger.exception("Converter %s is missing module or class information.", key)
                    stacktrace = traceback.format_exc()
                    sync_record.add_error(f"Converter {key} is missing module or class information", e, stacktrace)
                except NoConvertorException as e:
                    logger.exception("Failed to load converter %s: %s", key, str(e))
                    stacktrace = traceback.format_exc()
                    sync_record.add_error(f"Failed to load converter {key}: {str(e)}", e, stacktrace)
        return converters