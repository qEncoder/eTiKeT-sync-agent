from etiket_sync_agent.backends.folderbase.converters.base import FileConverter

import pathlib
import pandas as pd


class CSVToHDF5Converter(FileConverter):
    input_type = 'csv'
    output_type = 'hdf5'

    def convert(self, input_path: pathlib.Path) -> pathlib.Path:
        df = pd.read_csv(input_path)
        ds = df.to_xarray()
        
        path = pathlib.Path(self.temp_dir) / f"{input_path.stem}.hdf5"
        ds.to_netcdf(str(path), engine='h5netcdf', invalid_netcdf=True)
        return path