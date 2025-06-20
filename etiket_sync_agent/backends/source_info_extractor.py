import dataclasses
import typing
import pathlib
from typing import Any, Dict, List, Type, TypeVar, get_origin, get_args

T = TypeVar("T")

def get_dataclass_field_info(dc: Type[T]) -> List[Dict[str, Any]]:
    """
    Inspects a dataclass and returns a list of dictionaries describing its fields.

    Args:
        dc: The dataclass type to inspect.

    Returns:
        A list of dictionaries, where each dictionary represents a field
        and contains 'name', 'type', 'optional', and 'default' keys.
    """
    if not dataclasses.is_dataclass(dc):
        raise TypeError("Input must be a dataclass type")

    field_info_list = []
    fields = dataclasses.fields(dc)

    for field in fields:
        field_name = field.name
        field_type = field.type
        origin_type = get_origin(field_type)
        type_args = get_args(field_type)

        is_optional = False
        actual_type = field_type
        default_value = None

        # Check for Optional[X]
        if origin_type is typing.Union and type(None) in type_args:
            is_optional = True
            # Get the type other than None
            actual_type = next(t for t in type_args if t is not type(None))
        elif origin_type is typing.Optional: # Handles Optional[X] syntax if used directly
            is_optional = True
            actual_type = type_args[0]


        # Check for default values or factories
        if field.default is not dataclasses.MISSING:
            is_optional = True
            default_value = field.default
        elif field.default_factory is not dataclasses.MISSING:
            is_optional = True
            # We represent the default factory simply as None for the output,
            # as the factory function itself isn't easily serializable.
            # The fact that it's optional is the key info.
            default_value = None # Or maybe represent as "<factory>"? Sticking to None.

        # Determine type string
        type_name = getattr(actual_type, "__name__", str(actual_type))

        # Special handling for pathlib.Path based on field name convention
        if actual_type is pathlib.Path:
            if field_name.endswith("_directory"):
                type_name = "dir_path"
            elif field_name.endswith("_path"): # Assuming _path often means file path
                type_name = "file_path"
            else:
                type_name = "path" # Generic path if convention unclear

        field_info = {
            "name": field_name,
            "type": type_name.lower(),
            "optional": is_optional,
            "default": default_value,
        }
        field_info_list.append(field_info)

    return field_info_list