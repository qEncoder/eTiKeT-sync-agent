import os

from typing import Dict
from queue import Queue
from pathlib import Path

def dataset_get_mod_time(folder_path : str | Path , max_mod_time: float = 0):
    '''
    Get the modification time of the dataset present in the folder_path.
    
    Args:
        folder_path (str) : the path to the folder
        max_mod_time (float) : the maximum modification time (used for recursion)
        
    Returns:
        float : the modification time of the dataset
    '''
    try:
        for entry in os.scandir(folder_path):
            # Skip hidden and manifest files quickly
            if entry.name.startswith('.'):
                continue
            if entry.is_file(follow_symlinks=False):
                max_mod_time = max(max_mod_time, entry.stat(follow_symlinks=False).st_ctime)
            elif entry.is_dir(follow_symlinks=False):
                max_mod_time = max(max_mod_time, dataset_get_mod_time(entry.path, max_mod_time))
    except (FileNotFoundError, PermissionError):
        pass
    return max_mod_time

def enqueue_updates(queue: Queue, manifest: Dict[str, float], new_items: Dict[str, float]) -> bool:
    '''
    Update the queue with the new items. This functions updates the current manifest to the mod_time of the new items.
    
    Returns:
        bool : True if there was an update, False otherwise
    '''
    updated = False
    for path, mod_time in new_items.items():
        if mod_time == 0:
            continue
        old = manifest.get(path)
        if old is None or mod_time > old:
            queue.put((path, mod_time))
            manifest[path] = mod_time
            updated = True
    return updated