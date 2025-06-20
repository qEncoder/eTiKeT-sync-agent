from etiket_sync_agent.backends.filebase.converters.base import FileConverter

import xarray, pathlib

class ZarrToNETCDF4Converter(FileConverter):
    input_type = 'zarr'
    output_type = 'netcdf4'
    
    def convert(self) -> pathlib.Path:
        folder_name = self.file_path.name
        ds = xarray.open_zarr(self.file_path)
        # TODO: add to convert list and dicts to JSON
        path = pathlib.Path(self.temp_dir.name) / f"{folder_name}.hdf5"
        ds.to_netcdf(str(path), engine='h5netcdf', invalid_netcdf=True)
        return path