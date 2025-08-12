import pathlib, dataclasses

@dataclasses.dataclass
class LabberConfigData:
    labber_directory: pathlib.Path
    set_up : str