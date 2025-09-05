from pathlib import Path
from typing import Dict, Optional
from queue import Queue

from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver
from watchdog.events import FileSystemEventHandler, FileSystemEvent

from etiket_sync_agent.sync.manifests.v2.definitions import QH_DATASET_INFO_FILE
from etiket_sync_agent.sync.manifests.utility import enqueue_updates, dataset_get_mod_time

def dataset_poller_local(root_path : Path, current_manifest : Dict[str, float],
                            update_queue : Queue, enable_watcher : bool = True) -> Optional[BaseObserver]:
    '''
    Poll function to find all new datasets that are not yet present in the current manifest.
    
    Args:
        root_path (Path) : the root path to poll
        current_manifest (Dict[str, float]) : the current manifest
        update_queue (Queue) : the update queue. If a thread-safe queue is provided,
            updates will be enqueued as tuples of (relative_path, mod_time).
        enable_watcher (bool) : whether to enable the watcher
        
    Returns:
        Optional[BaseObserver] : the observer if enabled, None otherwise. Don't forget to stop the observer after use!
    '''
    observer = None
    if enable_watcher:
        observer = Observer()
        observer.schedule(ManifestV2Watcher(root_path, update_queue, current_manifest), str(root_path), recursive=True)
        observer.start()
        
    dataset_info_file_locations = root_path.rglob(QH_DATASET_INFO_FILE)
    for dataset_info_file in dataset_info_file_locations:
        ds_mod_time = dataset_get_mod_time(dataset_info_file.parent)
        rel_path = dataset_info_file.parent.relative_to(root_path)
        enqueue_updates(update_queue, current_manifest, {str(rel_path): ds_mod_time})
    return observer

class ManifestV2Watcher(FileSystemEventHandler):
    def __init__(self, root_path : Path, update_queue : Queue, current_manifest : Dict[str, float]):
        super().__init__()
        self.root_path = root_path
        self.update_queue = update_queue
        self.current_manifest = current_manifest
    
    def on_created(self, event : FileSystemEvent):
        self.add_update(Path(str(event.src_path)))
    
    def on_modified(self, event : FileSystemEvent):
        self.add_update(Path(str(event.src_path)))
        
    def on_moved(self, event : FileSystemEvent):
        self.add_update(Path(str(event.dest_path)))
        
    def add_update(self, path : Path):
        # check if the QH_DATASET_INFO_FILE file is present in the current/parent directories
        # if yes, then update the manifest!
        if path.name.startswith('.'):
            return

        while path != self.root_path:
            if (path / QH_DATASET_INFO_FILE).exists():
                relative_path = str(path.relative_to(self.root_path))
                mod_time = dataset_get_mod_time(path)
                enqueue_updates(self.update_queue, self.current_manifest, {relative_path: mod_time})
                return
            
            path = path.parent
            
            # break if we have reached filesystem root to avoid infinite loop (should not happen, but just in case)
            if path.parent == path:
                return