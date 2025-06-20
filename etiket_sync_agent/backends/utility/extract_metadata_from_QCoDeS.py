from typing import List, Dict, Union, Tuple

class MetaDataExtractionError(Exception):
	pass

def extract_labels_and_attributes_from_snapshot(snapshot : dict) -> Tuple[List[str], Dict[str, Union[Dict, str]]]:
    """
    Extract labels and attributes from a snapshot dictionary from the QH_MetadataManager.

    Args:
        snapshot (dict): The snapshot dictionary to extract metadata from.

    Returns:
        List[str]: A list of labels extracted from the snapshot.
        Dict[str, Union[float, int, str]]: A dictionary of attributes extracted from the snapshot.
    """
    try:
        station : Dict = snapshot.get("station", {})
        instruments : Dict = station.get("instruments", {})
        metadata : Dict = instruments.get("qh_meta", {})
        params : Dict = metadata.get("parameters", {})
        labels : List[str] = params.get("labels", {}).get("value", [])
        attributes : Dict[str, Union[Dict, str]] = params.get("attributes", {}).get("value", {})
        
        # fix attr (currently no good support for parameter on the server side)
        attr = {}
        for key, value in attributes.items():
            if isinstance(value, dict):
                if "value" in value:
                    attr[key] = str(value.get("value"))
            else:
                attr[key] = value
        
        return labels, attr
    except Exception as e:
        raise MetaDataExtractionError("Could not extract labels and attributes from snapshot.") from e
