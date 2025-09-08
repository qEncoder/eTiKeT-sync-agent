from etiket_sync_agent.backends.folderbase.converters.base import FileConverter
from etiket_sync_agent.backends.folderbase.manifest_v2 import QH_DATASET_INFO_FILE

from typing import Union, Optional, List, Dict, Type

import yaml, datetime, pathlib

def generate_dataset_info(
    path: Union[str, pathlib.Path],
    dataset_name: Optional[str] = None,
    creation: Optional[datetime.datetime] = None,
    description: Optional[str] = None,
    attributes: Optional[Dict[str, Union[str, int, float]]] = None,
    keywords: Optional[List[str]] = None,
    converters: Optional[List[Type[FileConverter]]] = None,
    skip: Optional[List[str]] = None) -> None:
    """
    Generate a dataset info YAML file for a specified path.

    This function creates a `dataset_info.yml` file containing metadata about the dataset,
    including its name, creation time, description, attributes, keywords, converters, and
    patterns of files or folders to skip during synchronization.

    Args:
        path (Union[str, Path]): The path to the dataset where the `dataset_info.yml` will be created.
            If the path does not yet exists, it will be created.
        dataset_name (Optional[str], optional): The name of the dataset. Defaults to `None`, which uses the directory name.
        creation (Optional[datetime.datetime], optional): The creation time of the dataset. 
            If provided, this should be a `datetime` object.
        description (Optional[str], optional): A brief description of the dataset.
        attributes (Optional[Dict[str, Union[str, int, float]]], optional): 
            A dictionary of attributes to be added to the dataset.
        keywords (Optional[List[str]], optional): 
            A list of keywords associated with the dataset.
        converters (Optional[List[Type[FileConverter]]], optional): 
            A list of converter classes used for processing files within the dataset. Each class should be a subclass of `FileConverter`.
        skip (Optional[List[str]], optional): 
            A list of file or folder patterns to skip during synchronization (e.g., `["*.json", "text.txt"]`).
    """
    
    dataset_info_dict = {}
    dataset_info_dict['version'] = "0.1"
    
    if dataset_name:
        dataset_info_dict['dataset_name'] = dataset_name
    if creation: # format in ISO format
        dataset_info_dict['creation'] = creation.strftime('%Y-%m-%dT%H:%M:%S')
    if description:
        dataset_info_dict['description'] = description
    if attributes:
        # check if the depth of the attributes is 1
        if all(isinstance(v, (str, int, float)) for v in attributes.values()):
            dataset_info_dict['attributes'] = attributes
        else:
            raise ValueError(
                'Attributes must be a dictionary with a depth of 1, '
                'and values of type str, int, or float.')
    if keywords:
        dataset_info_dict['keywords'] = keywords
    if converters:
        if all(issubclass(c, FileConverter) for c in converters):
            dataset_info_dict['converters'] = { f"{c.input_type}_to_{c.output_type}_converter" : 
                                                    {'module': c.__module__, 'class': c.__name__}
                                                    for c in converters}
        else:
            raise ValueError('Converters must be a list of subclasses of FileConverter.')
    if skip:
        dataset_info_dict['skip'] = skip
    
    dataset_path = pathlib.Path(path) if isinstance(path, str) else path

    if not dataset_path.exists():
        dataset_path.mkdir(parents=True)
    if not dataset_path.is_dir():
        raise NotADirectoryError(f"The specified path is not a directory: {dataset_path}")

    output_file = dataset_path / QH_DATASET_INFO_FILE
    try:
        with output_file.open('w', encoding="utf-8") as f:
            yaml.dump(dataset_info_dict, f, sort_keys=False)
    except IOError as e:
        raise IOError("Failed to write dataset info file.") from e