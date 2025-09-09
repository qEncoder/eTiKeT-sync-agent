from etiket_sync_agent.backends.folderbase.converters.base import FileConverter

import xarray, pathlib

class ZarrToNETCDF4Converter(FileConverter):
    input_type = 'zarr'
    output_type = 'hdf5'
    
    def convert(self, input_path: pathlib.Path) -> pathlib.Path:
        folder_name = input_path.stem
        ds = xarray.open_zarr(input_path)
        # TODO: add to convert list and dicts to JSON
        path = pathlib.Path(self.temp_dir) / f"{folder_name}.hdf5"
        ds.to_netcdf(str(path), engine='h5netcdf', invalid_netcdf=True)
        return path