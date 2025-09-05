import os

from pathlib import Path
from typing import Dict, Optional
from queue import Queue

from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver
from watchdog.events import FileSystemEventHandler, FileSystemEvent

from etiket_sync_agent.sync.manifests.utility import enqueue_updates, dataset_get_mod_time
from etiket_sync_agent.sync.manifests.v1.dataset_poller_NFS import dataset_explorer_full

def dataset_poller_local(root_path : Path, current_manifest: Dict[str, float], 
                            update_queue : Queue, level : int,
                            is_single_file : bool = False, enable_watcher : bool = True) -> Optional[BaseObserver]:
    '''
    Poll function to find all new datasets that are not yet present in the current manifest.
    
    Args:
        root_path (Path) : the root path to poll
        current_manifest (Dict[str, float]) : the current manifest
        update_queue (Queue) : the update queue, updates will be enqueued as tuples of (relative_path, mod_time).
        level (int) : the level (folder depth), at which the datasets can be found
        is_single_file (bool) : whether a dataset is a single file.
        enable_watcher (bool) : whether to enable the watcher
    
    Returns:
        Optional[BaseObserver] : the observer if enabled, None otherwise. Don't forget to stop the observer after use!
    '''
    for key_name, ds_mod_time in dataset_explorer_full(root_path, level, is_single_file):
        enqueue_updates(update_queue, current_manifest, {key_name: ds_mod_time})
    
    observer = None
    if enable_watcher:
        observer = Observer()
        observer.schedule(ManifestV1Watcher(root_path, update_queue, current_manifest, level, is_single_file), str(root_path), recursive=True)
        observer.start()
    return observer

class ManifestV1Watcher(FileSystemEventHandler):
    def __init__(self, root_path : Path, update_queue : Queue, current_manifest : Dict[str, float],
                    level : int, is_single_file : bool):
        super().__init__()
        self.root_path = root_path
        self.update_queue = update_queue
        self.current_manifest = current_manifest
        self.level = level
        self.is_single_file = is_single_file
    
    def on_created(self, event : FileSystemEvent):
        self.add_update(Path(str(event.src_path)))
    
    def on_modified(self, event : FileSystemEvent):
        self.add_update(Path(str(event.src_path)))
        
    def on_moved(self, event : FileSystemEvent):
        self.add_update(Path(str(event.dest_path)))
        
    def add_update(self, path : Path):
        if path.name.startswith('.'):
            return

        if self.is_single_file is False:
            path = path if os.path.isdir(path) else Path(os.path.dirname(path))
            path_parts = path.relative_to(self.root_path).parts[:self.level]
            if len(path_parts) >= self.level:
                key_name = "/".join(path_parts)
                dataset_dir = self.root_path.joinpath(*path_parts)
                try:
                    mod_time = dataset_get_mod_time(str(dataset_dir))
                    enqueue_updates(self.update_queue, self.current_manifest, {key_name: mod_time})
                except FileNotFoundError:
                    return
        else:
            key_name = path.relative_to(self.root_path)
            if len(key_name.parts) == self.level:
                enqueue_updates(self.update_queue, self.current_manifest, {str(key_name): os.stat(path).st_ctime})