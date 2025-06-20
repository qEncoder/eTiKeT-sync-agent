# adapted from https://github.com/willirath/netcdf-hash/blob/master/demo_001.ipynb
# using h5netcdf instead of netCDF4, sometimes compatibility issues between h5py and netCDF4 can arise.
from h5netcdf.legacyapi import Dataset, Variable
import numpy as np
import hashlib
from typing import Any

def attribute_filter(attributes):
    "Filter for elements which do not start with '__NCH'."
    return filter(lambda a: not a.startswith("__NCH"), attributes)

def update_hash_attr(name, value, hash_obj : Any):
    """Update `hash_obj` with the UTF8 encoded string version of first `name` and then `value`."""
    hash_obj.update(str(name).encode('utf8'))
    
    if isinstance(value, np.ndarray):
        hash_obj.update(value.tobytes())
    else:
        hash_obj.update(str(value).encode('utf8'))
    return hash_obj

def update_hash_var(var_obj : Variable, hash_obj : Any):
    """Update `hash_obj` from a variable."""
    hash_obj.update(str(var_obj.name).encode('utf8'))
   
    if var_obj.shape != ():
        if isinstance(var_obj[:], np.ndarray) and var_obj[:].dtype is not np.dtype('O'):
            hash_obj.update(var_obj[:].tobytes())
        elif isinstance(var_obj[:], np.ndarray):
            hash_obj.update(str(var_obj[:].tolist()).encode('utf8'))
            
    for attr in attribute_filter(sorted(var_obj.ncattrs())):
        update_hash_attr(attr, var_obj.getncattr(attr), hash_obj)
    return hash_obj

def md5_netcdf4(file_name) -> Any:
    """Calculate hash for a given netCDF file."""
    with Dataset(str(file_name)) as data_set:
        hash_obj = hashlib.md5()
        for key, var in sorted(data_set.variables.items()):
            hash_obj = update_hash_var(var, hash_obj)
        for name in attribute_filter(data_set.ncattrs()):
            value = data_set.getncattr(name)
            hash_obj = update_hash_attr(name, value, hash_obj)
        return hash_obj