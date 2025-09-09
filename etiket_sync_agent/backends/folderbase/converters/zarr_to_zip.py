from etiket_sync_agent.backends.folderbase.converters.base import FileConverter

import shutil, pathlib

class ZarrToZipConverter(FileConverter):
    input_type = 'zarr'
    output_type = 'zip'
    
    def convert(self, input_path: pathlib.Path) -> pathlib.Path:
        folder_name = input_path.name
        shutil.make_archive(
            base_name=str(pathlib.Path(self.temp_dir) / folder_name),
            format='zip',
            root_dir=str(input_path)
        )
        return pathlib.Path(self.temp_dir.name) / f"{folder_name}.zip"
    