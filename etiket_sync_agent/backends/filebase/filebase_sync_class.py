import fnmatch, os, pathlib, re, logging, typing, yaml, xarray

from datetime import datetime
from pathlib import Path

from etiket_sync_agent.backends.filebase.converters.base import FileConverterHelper
from etiket_sync_agent.backends.filebase.filebase_config_class import FileBaseConfigData
from etiket_sync_agent.backends.filebase.manifest import Manifest, get_mtime_of_folder
from etiket_sync_agent.exceptions.sync import NoConvertorException, SynchronizationErrorException
from etiket_sync_agent.sync.manifest_v2 import QH_DATASET_INFO_FILE, QH_MANIFEST_FILE
from etiket_sync_agent.sync.sync_source_abstract import SyncSourceFileBase
from etiket_sync_agent.sync.sync_utilities import dataset_info, file_info, FileType, SyncItems, sync_utilities

logger = logging.getLogger(__name__)

# in the config file, the converter should be named as ExtensionA_to_ExtensionB_converter
converter_naming_scheme = re.compile(r'^(\w+)_to_(\w+)_converter$')

class FileBaseSync(SyncSourceFileBase):
    SyncAgentName: typing.ClassVar[str] = "FileBaseGeneric"
    ConfigDataClass: typing.ClassVar[typing.Type[FileBaseConfigData]] = FileBaseConfigData
    MapToASingleScope: typing.ClassVar[bool] = True
    LiveSyncImplemented: typing.ClassVar[bool] = False
    level: typing.ClassVar[int] = -1
    
    @staticmethod
    def rootPath(configData: FileBaseConfigData) -> pathlib.Path:
        return pathlib.Path(configData.root_directory)

    @staticmethod
    def checkLiveDataset(configData: FileBaseConfigData, syncIdentifier: SyncItems, maxPriority: bool) -> bool:
        return False
    
    @staticmethod
    def syncDatasetNormal(configData: FileBaseConfigData, syncIdentifier: SyncItems):
        dataset_path = pathlib.Path(configData.root_directory) / syncIdentifier.dataIdentifier
        manifest = Manifest(dataset_path, syncIdentifier)

        try :
            if not (dataset_path / QH_DATASET_INFO_FILE).exists():
                raise FileNotFoundError(f"Dataset info file not found for dataset: {syncIdentifier.dataIdentifier}")
            with open(dataset_path / QH_DATASET_INFO_FILE, 'r', encoding="utf-8") as f:
                sync_info = yaml.safe_load(f)
            create_or_update_dataset(configData, syncIdentifier, sync_info)
        except Exception as e:
            manifest.add_error(e)
            manifest.write()
            raise e
        
        try:
            file_converters = get_file_converters(sync_info, manifest)
            skip = sync_info.get('skip', [])
            skip_folders = ["*.zarr"]
            
            for root, _, files in os.walk(dataset_path):
                # Do not check files in the skip_folders
                if not any(fnmatch.fnmatch(root, pattern) or fnmatch.fnmatch(root, f"{pattern}/*") for pattern in skip_folders):
                    for file in files:
                        if not any(fnmatch.fnmatch(file, pattern) for pattern in skip):
                            upload_file(root, file, dataset_path, syncIdentifier, manifest, file_converters)
                        else:
                            logger.info(f"File {file} is skipped, as per the skip list.")
                # do allow folder uploads if a converter is present
                if any(fnmatch.fnmatch(root, pattern) for pattern in skip_folders):
                    folder_name =  os.path.basename(root)
                    upload_folder(root, folder_name, dataset_path, syncIdentifier, manifest, file_converters)
            manifest.write()
        except Exception as e:
            manifest.add_error(e)
            manifest.write()
            raise e
        
        if manifest.has_errors():
            raise SynchronizationErrorException(f"Errors occurred during dataset synchronization. {manifest.get_errors()}")
        
    @staticmethod
    def syncDatasetLive(configData: FileBaseConfigData, syncIdentifier: SyncItems):
        raise NotImplementedError("Live sync is not implemented for FileBaseSync")

def create_or_update_dataset(configData : FileBaseConfigData, syncIdentifier : SyncItems, sync_info : dict):
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
    
    sync_utilities.create_or_update_dataset(False, syncIdentifier, ds_info)
    
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

def upload_file(root : str, file : str, dataset_path : pathlib.Path,
                syncIdentifier : SyncItems, manifest : Manifest,
                file_converters : typing.Dict[str, typing.List[FileConverterHelper]]):
    file_path = pathlib.Path(root) / file
    name = process_name(dataset_path, file_path)
    if file in [QH_DATASET_INFO_FILE, QH_MANIFEST_FILE] or file.startswith('.'):
        return

    if not manifest.is_file_uploaded(file, file_path):
        try:
            f_type = get_file_type(file_path)
            
            f_info = file_info(name = name, fileName = file,
                created = datetime.fromtimestamp(pathlib.Path(os.path.join(root, file)).stat().st_mtime),
                fileType = f_type, file_generator = "")
            sync_utilities.upload_file(file_path, syncIdentifier, f_info)
            manifest.add_file_upload_info(name, file_path)
        except Exception as e:
            manifest.add_file_upload_info(name, file_path, e)
    else:
        logger.info(f"File {file} already uploaded, skipping...")

    # check if there is a converter for the file
    extension = file_path.suffix
    if extension in file_converters:
        for converterHelper in file_converters[extension]:
            try:
                if not manifest.is_file_uploaded(file, file_path, converterHelper.converter_name):
                    with converterHelper.converter(file_path) as converted_file:
                        new_file_path = converted_file.convert()
                        f_type = get_file_type(new_file_path)
                        new_name = name.replace(file_path.name, new_file_path.name)
                        
                        f_info = file_info(name = new_name, fileName = new_file_path.name,
                            created = datetime.fromtimestamp(pathlib.Path(os.path.join(root, file)).stat().st_mtime),
                            fileType = f_type, file_generator = "")
                        sync_utilities.upload_file(converted_file, syncIdentifier, f_info)
                        manifest.add_file_converter_upload_info(file, file_path, new_name,
                                                                converterHelper.converter_name)
                else:
                    logger.info(f"File {file} already uploaded with converter {converterHelper.converter_name}, skipping...")
            except Exception as e:
                manifest.add_file_converter_upload_info(file, file_path, new_file_path,
                                                        converterHelper.converter_name, e)

def upload_folder(root : str, folder_name : str, dataset_path : pathlib.Path,
                syncIdentifier : SyncItems, manifest : Manifest,
                file_converters : typing.Dict[str, typing.List[FileConverterHelper]]):
    '''
    Function that uploads the contents of a folder as a file. Currently, this is only needed for zarr files.
    '''
    folder_path = pathlib.Path(root)
    name = process_name(dataset_path, folder_path)
    
    extension = folder_path.suffix[1:]

    if extension in file_converters:
        for converterHelper in file_converters[extension]:
            try:
                if not manifest.is_file_uploaded(folder_name, folder_path, converterHelper.converter_name):
                    with converterHelper.converter(folder_path) as converter:
                        new_file_path = converter.convert()
                        f_type = get_file_type(new_file_path)
                        new_name = name.replace(folder_path.name, new_file_path.name)
                        
                        
                        f_info = file_info(name = new_name, fileName = new_file_path.name,
                            created = datetime.fromtimestamp(get_mtime_of_folder(folder_path)),
                            fileType = f_type, file_generator = "")
                        sync_utilities.upload_file(new_file_path, syncIdentifier, f_info)
                        manifest.add_file_converter_upload_info(folder_name, folder_path, new_name,
                                                                converterHelper.converter_name)
                        logger.info(f"Uploaded folder {folder_name} with converter {converterHelper.converter_name}")
            except Exception as e:
                logger.exception(f"Failed to upload folder {folder_name} with converter {converterHelper.converter_name}, skipping...")
                manifest.add_file_converter_upload_info(folder_name, folder_path, None,
                                                        converterHelper.converter_name, e)
                
def get_file_type(file_path : Path) -> FileType:
    f_type = FileType.UNKNOWN
    if file_path.name.endswith(".json"):
        f_type = FileType.JSON
    if file_path.name.endswith(".txt"):
        f_type = FileType.TEXT
    if file_path.name.endswith(".hdf5") or file_path.name.endswith(".h5") or file_path.name.endswith(".nc"):
        try:
            xarray.open_dataset(file_path, engine='h5netcdf', invalid_netcdf=True)
            f_type = FileType.HDF5_NETCDF
        except Exception:
            f_type = FileType.HDF5
    return f_type

def get_file_converters(sync_info : dict, manifest : Manifest) -> typing.Dict[str, typing.List[FileConverterHelper]]:
    converters = {}

    for key in sync_info.get('converters', {}).keys():
        if converter_naming_scheme.match(key):
            try:
                module_name = sync_info['converters'][key]['module']
                class_name = sync_info['converters'][key]['class']
                converter_helper = FileConverterHelper(key, module_name, class_name)
                extension = converter_helper.converter.input_type.lower()
                converters.setdefault(extension, []).append(converter_helper)
            except KeyError:
                logger.exception("Converter %s is missing module or class information.", key)
                manifest.add_error(f"Converter {key} is missing module or class information")
            except NoConvertorException as e:
                logger.exception("Failed to load converter %s: %s", key, str(e))
                manifest.add_error(f"Failed to load converter {key}: {str(e)}")
    
    return converters