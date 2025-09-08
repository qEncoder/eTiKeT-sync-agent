from abc import ABC, abstractmethod
import pathlib, tempfile, importlib, typing

from etiket_sync_agent.exceptions.sync import NoConvertorException

class FileConverter(ABC):
    """
    Abstract base class for file converters.
    Subclasses must define `input_type` and `output_type` class attributes.
    """
    input_type: str
    output_type: str
    
    def __init__(self, file_path: pathlib.Path):
        """
        Initialize the file converter.

        Args:
            file_path (pathlib.Path): The path to the file to convert.
        """
        self.file_path = file_path
        self.temp_dir = tempfile.TemporaryDirectory()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        if not hasattr(cls, 'input_type') or not isinstance(cls.input_type, str):
            raise NotImplementedError(f"{cls.__name__} must define a class attribute `input_type` of type `str`.")
        if not hasattr(cls, 'output_type') or not isinstance(cls.output_type, str):
            raise NotImplementedError(f"{cls.__name__} must define a class attribute `output_type` of type `str`.")
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.temp_dir.cleanup()

    @abstractmethod
    def convert(self) -> pathlib.Path:
        """
        Convert the file and return the path to the converted file.

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
        self.__converter = None
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
            self.__converter = converter_class
        except Exception as e: #catch all exceptions
            self.error = e
        
    @property
    def converter(self) -> typing.Type[FileConverter]:
        """
        Access the converter class.

        Returns:
            Type[FileConverter]: The type that does the conversion.

        Raises:
            Exception: If there was an error when loading the converter.
        """
        if self.error is not None:
            raise NoConvertorException(str(self.error)) from self.error
        return self.__converter