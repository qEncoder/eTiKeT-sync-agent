import os, time

from pathlib import Path
from typing import Dict, Optional
from queue import Queue
from threading import Event

from etiket_sync_agent.sync.manifests.v2.definitions import QH_DATASET_INFO_FILE
from etiket_sync_agent.sync.manifests.utility import enqueue_updates, dataset_get_mod_time

POLLER_RATE_INTERVAL = 0.1

def dataset_poller_NFS(root_path: Path, current_manifest : Dict[str, float], update_queue : Queue, stop_event: Optional[Event] = None):
    """
    Poll function to find all new datasets that are not yet present in the current manifest, on a network file system.
    
    Since for the network file system we cannot make use of events, we need to manually find new datasets.
    In the background, this function will scan continuously scan for new datasets.
    As the scanning can take a while, it regularly does a check, based on the location of the most recent dataset, if a new one appeared in a nearby location.
    """
    # rate limit to ~10 datasets/sec across full + quick scans
    next_allowed = time.perf_counter() + POLLER_RATE_INTERVAL
    while stop_event is None or not stop_event.is_set():
        iterations_without_updates = 0
        
        for dataset_path, mod_time in dataset_explorer_full(root_path):
            has_update = enqueue_updates(update_queue, current_manifest, {str(dataset_path): mod_time})
            iterations_without_updates = 0 if has_update else iterations_without_updates + 1
            # rate limiting
            now = time.perf_counter()
            if now < next_allowed:
                time.sleep(next_allowed - now)
            next_allowed = time.perf_counter() + POLLER_RATE_INTERVAL

            if stop_event and stop_event.is_set():
                return
            if iterations_without_updates > 5:
                for dataset_path, mod_time in dataset_explorer_quick(root_path, current_manifest):
                    enqueue_updates(update_queue, current_manifest, {str(dataset_path): mod_time})
                    # rate limiting
                    now = time.perf_counter()
                    if now < next_allowed:
                        time.sleep(next_allowed - now)
                    next_allowed = time.perf_counter() + POLLER_RATE_INTERVAL
                    if stop_event and stop_event.is_set():
                        return
                iterations_without_updates = 0

def dataset_explorer_full(root_path : Path):
    '''
    Find all datasets by trying to find the QH_DATASET_INFO_FILE. The walk function in used to traverse the filesystem.
    
    Args:
        root_path (Path) : the root path where to look for datasets
    '''
    def walk(current: Path):
        try:
            dirs = []
            for entry in os.scandir(current):
                if entry.name.startswith('.'):
                    continue
                if entry.name == QH_DATASET_INFO_FILE:
                    parent_path = Path(entry.path).parent
                    rel_path = str(parent_path.relative_to(root_path))
                    yield rel_path, dataset_get_mod_time(parent_path)
                    return
                if entry.is_dir(follow_symlinks=False):
                    dirs.append(entry.path)
            for d in dirs:
                yield from walk(Path(d))
        except (FileNotFoundError, PermissionError):
            return
    yield from walk(root_path)

def dataset_explorer_quick(root_path : Path, manifest_dict : Dict[str, float]):
    '''
    Find a dataset (by trying to find the QH_DATASET_INFO_FILE)
    '''
    if not manifest_dict:
        return
    last_ds_rel_path, _ = max(manifest_dict.items(), key=lambda item: item[1])
    last_ds_path = root_path / last_ds_rel_path

    try:
        for entry in os.scandir(last_ds_path.parent):
            if entry.name.startswith('.'):
                continue
            rel_path = str(Path(entry.path).relative_to(root_path))
            if rel_path in manifest_dict and rel_path != last_ds_rel_path:
                continue
            if entry.is_dir(follow_symlinks=False):
                info_path = os.path.join(entry.path, QH_DATASET_INFO_FILE)
                try:
                    os.stat(info_path, follow_symlinks=False)
                    ds_path = Path(entry.path)
                    yield str(ds_path.relative_to(root_path)), dataset_get_mod_time(ds_path)
                except (FileNotFoundError, PermissionError):
                    pass
    except (FileNotFoundError, PermissionError):
        pass