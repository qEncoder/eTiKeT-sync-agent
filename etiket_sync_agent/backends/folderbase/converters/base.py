import pathlib, tempfile, importlib

from contextlib import contextmanager
from abc import ABC, abstractmethod

from etiket_sync_agent.exceptions.sync import NoConvertorException

class FileConverter(ABC):
    """
    Abstract base class for file converters.
    Subclasses must define `input_type` and `output_type` class attributes.
    """
    input_type: str
    output_type: str
    
    def __init__(self, temp_dir: pathlib.Path):
        """
        Initialize the file converter.
        """
        self.temp_dir : pathlib.Path = temp_dir

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        if not hasattr(cls, 'input_type') or not isinstance(cls.input_type, str):
            raise NotImplementedError(f"{cls.__name__} must define a class attribute `input_type` of type `str`.")
        if not hasattr(cls, 'output_type') or not isinstance(cls.output_type, str):
            raise NotImplementedError(f"{cls.__name__} must define a class attribute `output_type` of type `str`.")

    @abstractmethod
    def convert(self, input_path: pathlib.Path) -> pathlib.Path:
        """
        Convert the file and return the path to the converted file.
        
        Args:
            input_path (pathlib.Path): The path to the file to convert.

        Returns:
            pathlib.Path: The path to the converted file.
        """
        pass
    
class FileConverterHelper:
    """
    Helper class to dynamically load and validate file converter classes.
    """
    def __init__(self, converter_name : str,  module_name : str, class_name : str):
        """
        Initialize the helper by importing the converter class.

        Args:
            converter_name (str): name for the converter (should be A_to_B_converter), where A and B are the file extensions.
            module_name (str): name of the module to import.
            class_name (str): name of the converter class to load.
        """
        self.converter = None
        self.converter_name = converter_name
        
        self.error = None
        
        try:
            imported_module = importlib.import_module(module_name)
            converter_class = getattr(imported_module, class_name)
            if not issubclass(converter_class, FileConverter):
                raise TypeError(
                    f"Converter '{self.converter_name}' is not a subclass of FileConverter "
                    f"(module: '{module_name}', class: '{class_name}')"
                )
            self.converter = converter_class
        except Exception as e: #catch all exceptions
            self.error = e
        
    @contextmanager
    def convert(self, input_path: pathlib.Path) -> pathlib.Path:
        """
        Access the converter class.

        Returns:
            Type[FileConverter]: The type that does the conversion.

        Raises:
            Exception: If there was an error when loading the converter.
        """
        if self.error is not None:
            raise NoConvertorException(str(self.error)) from self.error
        
        with tempfile.TemporaryDirectory() as temp_dir:
            converter = self.converter(temp_dir)
            yield converter.convert(input_path)