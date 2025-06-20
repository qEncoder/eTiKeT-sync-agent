from dataclasses import dataclass, field
from typing import Dict, Optional
from pathlib import Path

from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler

import os, logging, typing

if typing.TYPE_CHECKING:
    from watchdog.observers.api import BaseObserverSubclassCallable

logger = logging.getLogger(__name__)


@dataclass
class manifest_state_V1:
    root_path: Path
    current_manifest: dict
    level : int
    is_NFS : bool
    update_queue: Dict[str, float] = field(default_factory=dict)
    file_watcher: Optional['BaseObserverSubclassCallable'] = None
    
    def __post_init__(self):
        self.file_watcher = get_observer(self.root_path, self.level, self.update_queue, self.is_NFS)
        new_manifest = manifest_get(self.root_path, self.level)
        diff_manifest = manifest_diff(self.current_manifest, new_manifest)
        diff_manifest.update(self.update_queue)
        self.update_queue.update(diff_manifest)

    def get_last_change(self, identifier : str) -> float:
        manifest_path = Path.joinpath(self.root_path, *identifier.split('/'))
        try:
            return manifest_get(manifest_path, 0).values()[0]
        except Exception:
            return 0

def get_observer(root_path : Path, level : int, update_queue : Dict[str, float], is_NFS : bool) -> 'BaseObserverSubclassCallable':
    if is_NFS:
        observer = PollingObserver()
    else:
        observer = Observer()
    
    observer.schedule(manifest_v1_watcher(root_path, level, update_queue), root_path, recursive=True)
    observer.start()
    return observer

class manifest_v1_watcher(FileSystemEventHandler):
    def __init__(self, root_path : Path, level : int, update_queue : Dict[str, float]):
        super().__init__()
        self.root_path = root_path
        self.level = level
        self.update_queue = update_queue
    
    def on_created(self, event):
        if event.src_path.endswith('.DS_Store'):
            return
        self.add_update(Path(event.src_path))
    
    def on_modified(self, event):
        if event.src_path.endswith('.DS_Store'):
            return
        self.add_update(Path(event.src_path))
        
    def on_moved(self, event):
        if event.src_path.endswith('.DS_Store'):
            return
        self.add_update(Path(event.dest_path))
        
    def add_update(self, path : Path):
        path = path if os.path.isdir(path) else Path(os.path.dirname(path))
        path_parts = path.relative_to(self.root_path).parts[:self.level]
        if len(path_parts) >= self.level:
            key_name = "/".join(path_parts[:self.level])
            self.update_queue[key_name] = os.stat(path).st_mtime

def manifest_get(base_directory : Path, level : int) -> Dict[str, float]:
    manifest  = {}
    base_depth = len(base_directory.parts)

    for root, _, files in os.walk(base_directory):
        current_depth = len(Path(root).parts) - base_depth
        if current_depth >= level:
            for file in files:
                mod_time = os.stat(f"{root}/{file}").st_mtime
                key_name = "/".join(Path(root).relative_to(base_directory).parts[:level])
                manifest[key_name] = max(manifest.get(key_name, 0), mod_time)

    return manifest

def manifest_diff(old_manigest : Dict[str, float], new_manifest : Dict[str, float]) -> Dict[str, float]:
    differences = {}
    for key in new_manifest.keys():
        if key in old_manigest and old_manigest[key] == new_manifest[key]:
            continue
        differences[key] = new_manifest[key]
    return differences