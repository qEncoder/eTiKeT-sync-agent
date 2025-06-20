from contextlib import contextmanager
from pathlib import Path
import shutil

from qdrive.dataset.dataset import dataset
from etiket_client.remote.endpoints.models.types import FileStatusLocal, FileType

import time, logging, tempfile, h5py, uuid

import xarray as xr
import numpy as np

logger = logging.getLogger(__name__)

@contextmanager
def with_dataset_snapshot(file_location: Path) -> Path:
    """
    Creates a safe, temporary copy of an HDF5/netCDF dataset file to prevent conflicts
    when the original is being actively written to by another process.
    
    Args:
        file_location: Path to the original dataset file
        
    Yields:
        Path to the temporary copy that can be safely read
        
    Raises:
        Various exceptions if file operations fail
        
    Example:
        with with_dataset_snapshot(data_file) as safe_file:
            dataset = xr.open_dataset(safe_file, engine='h5netcdf')
            # Process dataset without worrying about concurrent writes
    """
    with tempfile.NamedTemporaryFile(suffix=".hdf5", delete=True) as temp_file:
        try:
            shutil.copy2(file_location, temp_file.name)
            yield Path(temp_file.name)
        except (IOError, OSError) as e:
            logger.exception(f"Error creating dataset snapshot for {file_location}: {e}")
            raise
        except Exception as e:
            logger.exception(f"Unexpected error handling dataset {file_location}: {e}")
            raise

def is_dataset_live(file_location: Path, perform_NAN_check = True) -> bool:
    """
    Returns False if one of the following conditions are met:
    - If the dataset has not been modified in the last 2 minutes
    - If a new directory is created in the same parent directory with a newer modification time
    - If a new directory is created in the grandparent directory with a newer modification time
    - If the dataset does not contain any NaN values (if perform_NAN_check is True)
    """
    # Check modification time
    last_modified = file_location.stat().st_mtime
    if (time.time() - last_modified) > 120:
        return False

    current_dataset_mtime = file_location.stat().st_mtime
    parent_dir = file_location.parent
    
    # Check if any new directories in the same parent directory have newer modification time
    try:
        for item in parent_dir.parent.iterdir():
            if item.is_dir() and item.name != parent_dir.name:
                dir_mtime = item.stat().st_mtime
                if dir_mtime > current_dataset_mtime:
                    logger.debug(f"Found newer directory {item} in parent directory")
                    return False
                    
        if parent_dir.parent.parent.exists():
            for item in parent_dir.parent.parent.iterdir():
                if item.is_dir() and item.name != parent_dir.parent.name:  # Check siblings of grandparent
                    dir_mtime = item.stat().st_mtime
                    if dir_mtime > current_dataset_mtime:
                        logger.debug(f"Found newer directory {item} in grandparent directory")
                        return False
    except (PermissionError, FileNotFoundError) as e:
        logger.warning(f"Error checking for new directories: {e}")
    
    
    if perform_NAN_check is True:
        try:
            with with_dataset_snapshot(file_location) as safe_file:
                with xr.open_dataset(safe_file, engine='h5netcdf') as dataset:
                    return has_nan_values(dataset)
        except Exception:
            logger.exception("Error checking for NaN values in dataset")
            return False
                
    return True

class XArrayReplicator:
    '''
    Replicates the xarray dataset into a new HDF5 file. This file is launched in SWMR mode, and gets the expected attributes present in the qdrive dataset.
    The sync process works by following the state of the NAN values in the dataset.
    '''
    def __init__(self, ds_name : str, dataset_location: Path, dataset_uuid: uuid.UUID):
        self.dataset_location = dataset_location
        self.qdrive_dataset = dataset(dataset_uuid)

        self.dataset_followers = {}
        
        self.last_mod_sync = dataset_location.stat().st_mtime

        with with_dataset_snapshot(dataset_location) as safe_file:
            with xr.open_dataset(safe_file, engine='h5netcdf') as xr_dataset:
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp = Path(temp_dir) / "temp.hdf5"
                    xr_dataset.to_netcdf(temp, engine='h5netcdf', invalid_netcdf=True)
                    m_file = Path(temp_dir) / "measurement.hdf5"
                    # kinda have to do some hacky stuff to get the superblock to work ... (standard superblock is v2, but we need at least v3 for the qdrive dataset)
                    convert_to_superblock_v3(m_file, h5py.File(temp, 'r'))
                    if ds_name not in self.qdrive_dataset.files.keys():
                        self.qdrive_dataset.add_new_file(ds_name, destination=m_file,
                                        file_type=FileType.HDF5_CACHE, generator="quantify_sync_module ", status = FileStatusLocal.writing)
                    
                self.hdf5_file = h5py.File(self.qdrive_dataset[ds_name].path, 'a', locking=False, libver='v112')

                for name in xr_dataset.variables :
                    self.dataset_followers[name] = DatasetFollower(self.hdf5_file[name], xr_dataset[name])

        self.hdf5_file.swmr_mode = True
        
    def sync(self):
        keep_syncing = True
        
        while keep_syncing:
            keep_syncing = not self.__check_done()
            if self.__has_update():
                try:
                    with with_dataset_snapshot(self.dataset_location) as safe_file:
                        with xr.open_dataset(safe_file, engine='h5netcdf') as xr_dataset:
                            for name in xr_dataset.variables:
                                self.dataset_followers[name].update(xr_dataset[name])
                            self.hdf5_file.flush()
                except Exception:
                    time.sleep(0.5)
                    logger.exception("Error reading dataset")
            else:
                time.sleep(0.5) # default write interval in quantify is 0.5s
        
        for follower in self.dataset_followers.values():
            follower.complete()

    def __has_update(self) -> bool:
        last_mod = self.dataset_location.stat().st_mtime
        if last_mod > self.last_mod_sync:
            self.last_mod_sync = last_mod
            return True
        return False
    
    def __check_done(self):
        done = True
        for follower in self.dataset_followers.values():
            if follower.noNanValues == False:
                done = False
                break
        if done:
            return True
        
        return not is_dataset_live(self.dataset_location, perform_NAN_check=False)
        
class DatasetFollower:
    '''
    Object used cache the state of a datasets in the netcdf4 file. If the file has new values, they will be written to the live HDF5 file.
    '''
    def __init__(self, h5_dataset: h5py.Dataset, initial_state: xr.DataArray):
        self.dataset = h5_dataset
        self.noNanValues = False
        
        raw_data = np.asarray(initial_state.data)
        cursor = self.__get_cursor(raw_data)
        cursor_shape = (1,) if raw_data.ndim == 0 else (raw_data.ndim,)
        
        h5_dataset.attrs.create('__cursor', cursor, dtype=np.int32, shape=cursor_shape)
        h5_dataset.attrs['completed'] = False
        
    def update(self, data_array: xr.DataArray):
        data = data_array.values
        old_cursor = self.dataset.attrs['__cursor']
        new_cursor = self.__get_cursor(data)
        if not np.array_equal(old_cursor, new_cursor):
            try:
                if self.dataset.shape != data.shape:
                    self.dataset.resize(data.shape)
                
                slices = []
                for i in range(len(data.shape)):
                    if old_cursor[i] == new_cursor[i]:
                        slices.append(slice(new_cursor[i], new_cursor[i]+1))
                    else:
                        if i == data.ndim-1:
                            slices.append(slice(old_cursor[i], new_cursor[i]))
                        else:
                            slices.append(slice(old_cursor[i], new_cursor[i]+1))
                        break
                self.dataset.write_direct(data, np.s_[tuple(slices)], np.s_[tuple(slices)])
                self.dataset.attrs['__cursor'] = new_cursor
                self.dataset.attrs['completed'] = False
            except Exception as e:
                self.dataset.attrs['__cursor'] = old_cursor
                self.dataset.attrs['completed'] = False
                logger.exception("Error updating dataset")
    
    def complete(self):
        self.dataset.attrs['completed'] = True
    
    def __get_cursor(self, raw_data: np.ndarray):
        '''
        Finds the position of the last value that is not NaN in the data array.
        This helps track how much of the dataset has already been written.
        '''
        non_nan_mask = ~np.isnan(raw_data)
        
        if np.all(non_nan_mask):
            self.noNanValues = True
            return np.unravel_index(raw_data.size - 1, raw_data.shape)
        
        # If all values are NaN, return zeros
        if not np.any(non_nan_mask):
            return tuple([0] * len(raw_data.shape))
        
        # Find the last non-NaN value
        flat_indices = np.flatnonzero(non_nan_mask)
        if len(flat_indices) > 0:
            last_non_nan_idx = flat_indices[-1]
            # Convert flat index to dimensional indices
            return np.unravel_index(last_non_nan_idx, raw_data.shape)
    
        # Fallback to all zeros (shouldn't reach here given the checks above)
        return tuple([0] * len(raw_data.shape))
    
def convert_to_superblock_v3(new_file : Path, h5_old_file : h5py.File):
    with h5py.File(new_file, 'w', locking=False, libver='v112') as h5_new_file:
        # create all groups and dataset of the original file (normally not nested)
        for h5_name, h5_object in h5_old_file.items():
            if isinstance(h5_object, h5py.Group):
                h5_new_file.create_group(h5_name)
            elif isinstance(h5_object, h5py.Dataset):
                h5_new_file.create_dataset(h5_name, data=h5_object[()])
            else:
                raise ValueError("Unknown type in HDF5 file")
        
        # Copy file attributes
        for h5_name, h5_object in h5_old_file.attrs.items():
            h5_new_file.attrs[h5_name] = h5_object
        
        # Copy object attributes and handle special cases
        for h5_name, h5_object in h5_old_file.items():
            for attr_name, attr_value in h5_object.attrs.items():          
                if attr_name == 'DIMENSION_LIST':
                    dimension_scale = [np.array([h5_new_file[h5py.h5r.get_name(ds_ref, h5_old_file.id)].ref 
                                            for ds_ref in reference_list], dtype=np.object_)
                                            for reference_list in attr_value]
                    
                    create_dimension_list_attr(h5_new_file, h5_name, dimension_scale)
                elif attr_name == 'REFERENCE_LIST':
                    # extract from compound datatype
                    reference_list = [(h5_new_file[h5py.h5r.get_name(ref_compound['dataset'], h5_old_file.id)].ref, ref_compound['dimension']) for ref_compound in attr_value]
                    create_reference_list_attr(h5_new_file, h5_name, reference_list)
                elif attr_name == 'CLASS':
                    create_str_attr(h5_new_file[h5_name], 'CLASS', str(attr_value.decode('utf-8')))
                elif attr_name == 'NAME':
                    create_str_attr(h5_new_file[h5_name], 'NAME', str(attr_value.decode('utf-8')))
                else :
                    h5_new_file[h5_name].attrs[attr_name] = attr_value


def create_dimension_list_attr(h5_new_file, h5_name, dimension_scale):
    type_id = h5py.h5t.vlen_create(h5py.h5t.STD_REF_OBJ)
    space_id = h5py.h5s.create_simple((len(dimension_scale),), (len(dimension_scale),))
    attr = h5py.h5a.create(h5_new_file[h5_name].id, 'DIMENSION_LIST'.encode('utf-8'), type_id, space_id)
    arr = np.array(dimension_scale + [''], dtype=object)[:-1]  # Append and remove an empty string to ensure correct type
    attr.write(arr)

def create_reference_list_attr(h5_new_file, h5_name, reference_list):
    type_id = h5py.h5t.create(h5py.h5t.COMPOUND, h5py.h5t.STD_REF_OBJ.get_size() + h5py.h5t.NATIVE_UINT32.get_size())
    type_id.insert('dataset'.encode('utf-8'), 0, h5py.h5t.STD_REF_OBJ)
    type_id.insert('dimension'.encode('utf-8'), h5py.h5t.STD_REF_OBJ.get_size(), h5py.h5t.NATIVE_UINT32)
    space_id = h5py.h5s.create_simple((len(reference_list),), (len(reference_list),))
    attr = h5py.h5a.create(h5_new_file[h5_name].id, 'REFERENCE_LIST'.encode('utf-8'), type_id, space_id)
    attr.write(np.array(reference_list, dtype=[('dataset', 'O'), ('dimension', np.uint32)]))


def create_str_attr(dataset : h5py.Dataset, attr_name : str, string_value: str):
    if h5py.h5a.exists(dataset.id, attr_name.encode('utf-8')):
            h5py.h5a.delete(dataset.id,name = attr_name.encode('utf-8'))   

    type_id = h5py.h5t.TypeID.copy(h5py.h5t.C_S1)
    type_id.set_size(len(string_value)+1)
    type_id.set_strpad(h5py.h5t.STR_NULLTERM)
    space = h5py.h5s.create(h5py.h5s.SCALAR)
    
    attr = h5py.h5a.create(dataset.id, attr_name.encode('utf-8'), type_id, space)
    string = np.array(string_value.encode('ascii'), dtype=h5py.string_dtype('ascii', len(string_value)+1))
    attr.write(string)
    
def has_nan_values(dataset: xr.Dataset) -> bool:
    """
    Check if an xarray Dataset contains any NaN values in data variables or coordinates.
    
    Args:
        dataset: The xarray Dataset to check
    
    Returns:
        True if any NaN values are found, False otherwise
    """
    for var_name, da in dataset.data_vars.items():
        try:
            if np.isnan(da.values).any():
                logger.debug(f"Found NaN values in data variable: {var_name}")
                return True
        except TypeError: # Skip non-numeric arrays (e.g., strings)
            continue
    
    for coord_name, coord in dataset.coords.items():
        try:
            if np.isnan(coord.values).any():
                logger.debug(f"Found NaN values in coordinate: {coord_name}")
                return True
        except TypeError:
            continue

    return False