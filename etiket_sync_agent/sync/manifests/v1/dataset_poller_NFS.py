import os, time

from pathlib import Path
from typing import Dict, Iterator, Tuple, Optional
from queue import Queue
from threading import Event

from etiket_sync_agent.sync.manifests.utility import dataset_get_mod_time, enqueue_updates

POLLER_RATE_INTERVAL = 0.1

def dataset_poller_NFS(root_path: Path, current_manifest : Dict[str, float], update_queue : Queue, level: int, is_single_file: bool = False, stop_event: Optional[Event] = None):
    """
    Poll function to find all new datasets that are not yet present in the current manifest, on a network file system.
    
    Since for the network file system we cannot make use of events, we need to manually find new datasets.
    In the background, this function will scan continuously scan for new datasets.
    As the scanning can take a while, it regularly does a check, based on the location of the most recent dataset, if a new one appeared in a nearby location.
    """
    # rate limit to ~10 datasets/sec across full + quick scans
    next_allowed = time.perf_counter() + POLLER_RATE_INTERVAL
    iterations_without_updates = 0
    while stop_event is None or not stop_event.is_set():
        for dataset_path, mod_time in dataset_explorer_full(root_path, level, is_single_file):
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
                for q_dataset_path, q_mod_time in dataset_explorer_quick(root_path, current_manifest, is_single_file):
                    enqueue_updates(update_queue, current_manifest, {str(q_dataset_path): q_mod_time})
                    # rate limiting
                    now = time.perf_counter()
                    if now < next_allowed:
                        time.sleep(next_allowed - now)
                    next_allowed = time.perf_counter() + POLLER_RATE_INTERVAL
                    if stop_event and stop_event.is_set():
                        return
                iterations_without_updates = 0

def dataset_explorer_full(root_path: Path, level: int, is_single_file: bool) -> Iterator[Tuple[str, float]]:
    """
    Yield all datasets found at a specific directory depth.

    Yields:
        (relative_path: str, modified_time: float)
    """

    def walk(current: Path, current_level: int) -> Iterator[Tuple[str, float]]:
        if current_level == level-1:
            try:
                with os.scandir(current) as scanner:
                    for entry in scanner:
                        if entry.name.startswith('.'):
                            continue
                        if is_single_file:
                            if entry.is_file(follow_symlinks=False):
                                try:
                                    mod_time = entry.stat(follow_symlinks=False).st_mtime
                                except (FileNotFoundError, PermissionError):
                                    continue
                                rel_path = str(Path(entry.path).relative_to(root_path))
                                yield rel_path, mod_time
                        else:
                            if entry.is_dir(follow_symlinks=False):
                                try:
                                    mod_time = dataset_get_mod_time(entry.path)
                                except (FileNotFoundError, PermissionError):
                                    continue
                                rel_path = str(Path(entry.path).relative_to(root_path))
                                yield rel_path, mod_time
            except (FileNotFoundError, PermissionError):
                return
        else:
            try:
                with os.scandir(current) as scanner:
                    for entry in scanner:
                        if entry.name.startswith('.'):
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            yield from walk(Path(entry.path), current_level + 1)
            except (FileNotFoundError, PermissionError):
                return

    yield from walk(root_path, 0)

def dataset_explorer_quick(root_path: Path, manifest_dict: Dict[str, float], is_single_file: bool) -> Iterator[Tuple[str, float]]:
    """
    Quickly rescan the parent directory of the most recent dataset and yield candidates.

    Yields:
        (relative_path: str, modified_time: float)
    """
    if not manifest_dict:
        return

    last_rel_path, _ = max(manifest_dict.items(), key=lambda item: item[1])
    parent = (root_path / last_rel_path).parent

    try:
        with os.scandir(parent) as scanner:
            for entry in scanner:
                if entry.name.startswith('.'):
                    continue

                if is_single_file:
                    if not entry.is_file(follow_symlinks=False):
                        continue
                    rel_path = str(Path(entry.path).relative_to(root_path))
                    if rel_path in manifest_dict and rel_path != last_rel_path:
                        continue
                    try:
                        mod_time = entry.stat(follow_symlinks=False).st_ctime
                    except (FileNotFoundError, PermissionError):
                        continue
                    yield rel_path, mod_time
                else:
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    rel_path = str(Path(entry.path).relative_to(root_path))
                    if rel_path in manifest_dict and rel_path != last_rel_path:
                        continue
                    try:
                        mod_time = dataset_get_mod_time(entry.path)
                    except (FileNotFoundError, PermissionError):
                        continue
                    yield rel_path, mod_time
    except (FileNotFoundError, PermissionError):
        return