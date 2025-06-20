'''
Manifest V2

The ManifestV2 class manages the manifests of datasets by tracking dataset information files (e.g., 'QH_dataset_info.yaml') within a specified directory. It efficiently monitors changes to these files and updates the manifests accordingly, ensuring synchronization between local files and a database or remote storage.

Functionality:

1. Initialization:
   - Initializes a structure to hold a collection of manifests related to a given root path.
   - Determines the storage type ('local' or 'remote') and sets up appropriate monitoring mechanisms.

2. Loading Manifests:
   - When invoked, the manifest manager loads the manifests for a given sync source name:
     a. Database Retrieval: Requests from the database the last modified times of datasets that already exist in the manifest.
     b. Filesystem Scan: Searches the filesystem starting from the root path for dataset info files ('*dataset_info.yaml') and records their last modified times.
     c. Comparison and Update: Compares the timestamps from the database and the filesystem to identify new or updated datasets. Updates the manifest and stores new or updated dataset items in the database.


Manifest Manager V2

This class manages the manifests of the datasets. 
It works in the following way:

1. It initializes and holds a structure that can hold a collection of manifests related to a path.
2. When called for, the manifest manager load the manifests for a given sync source name:
    a. It request from the database the last modified time of the datasets that already exist in the manifest.
    b. It searches the hard drive for the "QH_dataset_info.yaml" files and calculates the last modified time of the datasets.
    c. It compares the two and returns the datasets that have been modified and stores the new dataset items in the database.
3. When the manifest is again called, it is already loaded, in this case, it :
    a. in case of local harddrive:
        - it uses watchdog to monitor the changes in the datasets
        - when it detects a change, it updates the manifest and the database.
    b. in case of remote storage:
        - it does not check for changes as long not all datasets are synced.
        - when it does check, it only does so every minute
        - implemented using the PollingObserver class from watchdog.
'''

import typing, logging

from pathlib import Path
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler

from dataclasses import dataclass, field
from typing import Dict, Optional

if typing.TYPE_CHECKING:
    from watchdog.observers.api import BaseObserverSubclassCallable

QH_DATASET_INFO_FILE = '_QH_dataset_info.yaml'
QH_MANIFEST_FILE = '.QH_manifest.yaml'

logger = logging.getLogger(__name__)

@dataclass
class manifest_state_V2:
    root_path: Path
    current_manifest: Dict[str, float]
    update_queue: Dict[str, float] = field(default_factory=lambda: {})
    file_watcher: Optional['BaseObserverSubclassCallable'] = None
    is_NFS: bool = False
    
    def __post_init__(self):
        self.file_watcher = get_observer(self.root_path, self.update_queue, self.is_NFS)
        new_manifest = get_dataset_mod_times(self.root_path)
        self.update_queue.update(get_mod_time_updates(self.current_manifest, new_manifest))

    def get_last_change(self, identifier : str) -> float:
        manifest_path = Path.joinpath(self.root_path, *identifier.split('/'))
        return list(get_dataset_mod_times(manifest_path).values())[0]

def get_observer(root_path : Path, update_queue : Dict[str, float], is_NFS : bool) -> 'BaseObserverSubclassCallable':
    if is_NFS is True:
        observer = PollingObserver()
    else:
        observer = Observer()
    observer.schedule(manifest_v2_watcher(root_path, update_queue), root_path, recursive=True)
    observer.start()
    return observer

class manifest_v2_watcher(FileSystemEventHandler):
    def __init__(self, root_path : Path, update_queue : Dict[str, float]):
        super().__init__()
        self.root_path = root_path
        self.update_queue = update_queue
    
    def on_created(self, event):
        self.add_update(Path(event.src_path))
    
    def on_modified(self, event):
        self.add_update(Path(event.src_path))
        
    def on_moved(self, event):
        self.add_update(Path(event.dest_path))
        
    def add_update(self, path : Path):
        # check if the QH_DATASET_INFO_FILE file is present in the current/parent directories
        # if yes, then update the manifest!
        if neglect_file(path):
            return

        while path != self.root_path:
            if (path / QH_DATASET_INFO_FILE).exists():
                relative_path = str(path.relative_to(self.root_path))
                mod_times = list(get_dataset_mod_times(path).values())
                if len(mod_times) > 0:
                    self.update_queue[relative_path] = mod_times[0]
                return
            path = path.parent

def get_dataset_mod_times(root_path : Path) -> Dict[str, float]:
    dataset_info_file_locations = root_path.rglob(f'*{QH_DATASET_INFO_FILE}')
    dataset_mod_times = {}
    
    for info_file in dataset_info_file_locations:
        dataset_dir = info_file.parent
        
        latest_mod_time = 0
        n_files = 0 # little hack to ensure that the mod time goes up if an extra file is added that is "older" than the others.
        for file_path in dataset_dir.rglob('*'):
            if file_path.is_file() and not neglect_file(file_path):
                mod_time = file_path.stat().st_mtime
                if mod_time > latest_mod_time:
                    latest_mod_time = mod_time
                n_files += 1
        
        if n_files > 1: # the QH_DATASET_INFO_FILE and at least one other file
            # get relative path
            rel_path = dataset_dir.relative_to(root_path)
            dataset_mod_times[str(rel_path)] = latest_mod_time + n_files * 1e-3
    
    return dataset_mod_times

def get_mod_time_updates(current_manifest : Dict[str, float], new_manifest : Dict[str, float]) -> Dict[str, float]:
    updates = {}
    
    for path, new_mod_time in new_manifest.items():
        if path in current_manifest:
            if new_mod_time > current_manifest[path]:
                updates[path] = new_mod_time
        else:
            updates[path] = new_mod_time
    
    return updates

def neglect_file(file_path: str) -> bool:
    file_path = Path(file_path)
    if file_path.name.startswith('.'):
        return True
    if file_path.name == QH_MANIFEST_FILE:
        return True