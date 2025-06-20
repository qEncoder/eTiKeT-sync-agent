from etiket_sync_agent.backends.filebase.converters.base import FileConverter

import shutil, pathlib

class ZarrToZipConverter(FileConverter):
    input_type = 'zarr'
    output_type = 'zip'
    
    def convert(self) -> pathlib.Path:
        folder_name = self.file_path.name
        shutil.make_archive(
            base_name=str(pathlib.Path(self.temp_dir.name) / folder_name),
            format='zip',
            root_dir=str(self.file_path)
        )
        return pathlib.Path(self.temp_dir.name) / f"{folder_name}.zip"
    