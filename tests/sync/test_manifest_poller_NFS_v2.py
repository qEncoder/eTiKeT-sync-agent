''' 
- create a structure looking like (in a temp folder):
    /root/ds1/_QH_dataset_info.yaml
    /root/ds1/file1.txt
    /root/ds2/_QH_dataset_info.yaml
    /root/level1/ds3/_QH_dataset_info.yaml
    /root/level1/ds3/file1.txt
    /root/level1/level2/ds4/_QH_dataset_info.yaml
    /root/level1/level2/ds4/file1.txt
    
    # run the dataset_poller_NFS in a separate process. 
    --> use the function dataset_poller_NFS --> start with an empty manifest, and queue + wait 1 second 
        --> check if the `current_manifest` is updated correctly (should match the new manifest)
        --> check if the `update_queue` is updated correctly (should match the new manifest)
    --> run the function again, with current manifest and empty queue + wait 1 second
        --> current manifest should not be changed and queue should be empty
- update tests:
    --> create the structure again, run the dataset_poller_NFS function
    --> start doing some updates ( wait 1 second before queue is flushed before)
    --> add a file to ds1 --> wait --> queue should be updated
    --> add a file to level1/ds3 --> wait --> queue should be updated
    --> add a hidden file to level1/ds3 --> wait --> queue should not be updated
    --> modify a file in ds1 --> wait --> queue should be updated
    --> modify the _QH_dataset_info.yaml file in ds2 --> wait --> queue should be updated
- test handling of multiple pollers:
    --> create the structure in two different folders,
    --> run the poller in two separate processes
    --> start in both with empty manifest, both should return the correct queues
    --> flush both queues
    --> update a file in the first folder --> check if dataset is added in the queue (wait 400ms to ensure the process has time to process the update).
    --> do the same for the second folder
    --> check if the queue is correct in both folders
- test lazy poller --> create a simple structure with 1000 datasets, and create a filled manifest at start.
    --> folder structure is root/ds_all/ds1/, root/ds_all/ds2/, ..., root/ds_all/ds1000/
    --> wait 1 second. check if queue is empty
    --> create a new dataset by making a folder (e.g. root/ds_all/ds1001/) with a _QH_dataset_info.yaml file and a data file.
    --> wait 1 second. check if queue is updated with the new dataset.
    --> repeat this 2 times, clearing to queue every time.
    --> Now try to add a dataset to the root/ds_other/ds001 + qharbor file --> wait 1 second. --> queue should not be updated (as he is not looking in this folder)
'''

import os, time, multiprocessing as mp
from queue import Empty

from pathlib import Path
from typing import Dict

from etiket_sync_agent.sync.manifests.v2.definitions import QH_DATASET_INFO_FILE
from etiket_sync_agent.sync.manifests.v2.dataset_poller_NFS import dataset_poller_NFS


# --------------------
# Helper utilities
# --------------------

def _touch(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a"):
        os.utime(p, None)

def _write(p: Path, content: str = "x") -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)

def _drain_queue(q) -> Dict[str, float]:
    out: Dict[str, float] = {}
    while True:
        try:
            k, v = q.get_nowait()
            out[k] = v
        except Empty:
            break
    return out

def _start_poller(root: Path, manifest, q, stop_event=None):
    evt = stop_event or mp.Event()
    p = mp.Process(target=dataset_poller_NFS, args=(root, manifest, q, evt), daemon=True)
    p.start()
    return p, evt

def _manifest_to_plain_dict(manifest) -> Dict[str, float]:
    try:
        # Support manager dict proxies
        return {k: manifest[k] for k in list(manifest.keys())}
    except (AttributeError, TypeError):
        return dict(manifest)

def _wait_for_manifest(manifest, expected: Dict[str, float], timeout: float = 2.0) -> bool:
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if _manifest_to_plain_dict(manifest) == expected:
            return True
        time.sleep(0.02)
    return _manifest_to_plain_dict(manifest) == expected

def _wait_for_queue_key(q, key: str, expected_value: float | None = None, timeout: float = 1.2) -> bool:
    start = time.monotonic()
    seen: Dict[str, float] = {}
    while time.monotonic() - start < timeout:
        try:
            k, v = q.get(timeout=0.02)
            seen[k] = v
            if k == key and (expected_value is None or v == expected_value):
                return True
        except Empty:
            pass
    return key in seen and (expected_value is None or seen.get(key) == expected_value)

def _wait_for_queue_empty(q, timeout: float = 0.8) -> bool:
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if q.empty():
            return True
        try:
            q.get(timeout=0.02)
        except Empty:
            pass
    return q.empty()

def _build_example_tree(root: Path) -> Dict[str, float]:
    ds1 = root / "ds1"
    ds2 = root / "ds2"
    ds3 = root / "level1" / "ds3"
    ds4 = root / "level1" / "level2" / "ds4"

    # Create dataset info files
    for ds in (ds1, ds2, ds3, ds4):
        _write(ds / QH_DATASET_INFO_FILE)

    # Populate files
    _write(ds1 / "file1.txt", "a")
    _write(ds3 / "file1.txt", "c")
    _write(ds4 / "file1.txt", "d")

    expected = {
        str(ds1.relative_to(root)): os.stat(ds1 / "file1.txt").st_ctime,
        str(ds2.relative_to(root)): os.stat(ds2 / QH_DATASET_INFO_FILE).st_ctime,
        str(ds3.relative_to(root)): os.stat(ds3 / "file1.txt").st_ctime,
        str(ds4.relative_to(root)): os.stat(ds4 / "file1.txt").st_ctime,
    }
    return expected


# ----- #
# Tests #
# ----- #

def test_initial_scan_populates_manifest_and_queue(tmp_path: Path) -> None:
    expected = _build_example_tree(tmp_path)

    manager = mp.Manager()
    current_manifest = manager.dict()
    update_queue = mp.Queue()

    p, evt = _start_poller(tmp_path, current_manifest, update_queue)

    assert _wait_for_manifest(current_manifest, expected)
    assert _drain_queue(update_queue) == expected
    evt.set()
    p.join(timeout=2)
    if p.is_alive():
        p.terminate()


def test_no_duplicate_updates_without_changes(tmp_path: Path) -> None:
    expected = _build_example_tree(tmp_path)

    manager = mp.Manager()
    current_manifest = manager.dict()
    update_queue = mp.Queue()

    p, evt = _start_poller(tmp_path, current_manifest, update_queue)
    assert _wait_for_manifest(current_manifest, expected)
    _drain_queue(update_queue)

    # Without any changes, the queue should remain empty
    assert _wait_for_queue_empty(update_queue)
    evt.set()
    p.join(timeout=2)
    if p.is_alive():
        p.terminate()


def test_updates_and_hidden_ignored(tmp_path: Path) -> None:
    expected = _build_example_tree(tmp_path)

    manager = mp.Manager()
    current_manifest = manager.dict()
    update_queue = mp.Queue()

    p, evt = _start_poller(tmp_path, current_manifest, update_queue)
    assert _wait_for_manifest(current_manifest, expected)
    _drain_queue(update_queue)

    # Add a file to ds1
    ds1 = tmp_path / "ds1"
    _write(ds1 / "new.txt", "z")
    expected_ctime = os.stat(ds1 / "new.txt").st_ctime
    assert _wait_for_queue_key(update_queue, "ds1", expected_ctime)
    assert current_manifest["ds1"] == expected_ctime
    _drain_queue(update_queue)

    # Add a file to level1/ds3
    ds3 = tmp_path / "level1" / "ds3"
    _write(ds3 / "new.txt", "y")
    expected_ctime = os.stat(ds3 / "new.txt").st_ctime
    assert _wait_for_queue_key(update_queue, "level1/ds3", expected_ctime)
    assert current_manifest["level1/ds3"] == expected_ctime
    _drain_queue(update_queue)

    # Add a hidden file to ds3 (should be ignored)
    _write(ds3 / ".hidden.txt", "ignored")
    assert _wait_for_queue_empty(update_queue)

    # Modify a file in ds1
    _write(ds1 / "file1.txt", "modified")
    expected_ctime = os.stat(ds1 / "file1.txt").st_ctime
    assert _wait_for_queue_key(update_queue, "ds1", expected_ctime)
    assert current_manifest["ds1"] == expected_ctime
    _drain_queue(update_queue)

    # Modify the _QH_dataset_info.yaml file in ds2
    ds2 = tmp_path / "ds2"
    _touch(ds2 / QH_DATASET_INFO_FILE)
    expected_ctime = os.stat(ds2 / QH_DATASET_INFO_FILE).st_ctime
    assert _wait_for_queue_key(update_queue, "ds2", expected_ctime)
    assert current_manifest["ds2"] == expected_ctime
    _drain_queue(update_queue)

    evt.set()
    p.join(timeout=2)
    if p.is_alive():
        p.terminate()


def test_multiple_pollers_independent_queues(tmp_path: Path) -> None:
    root1 = tmp_path / "root1"
    root2 = tmp_path / "root2"
    root1.mkdir()
    root2.mkdir()

    expected1 = _build_example_tree(root1)
    expected2 = _build_example_tree(root2)

    manager = mp.Manager()
    manifest1 = manager.dict()
    queue1 = mp.Queue()
    manifest2 = manager.dict()
    queue2 = mp.Queue()

    p1, e1 = _start_poller(root1, manifest1, queue1)
    p2, e2 = _start_poller(root2, manifest2, queue2)

    assert _wait_for_manifest(manifest1, expected1)
    assert _wait_for_manifest(manifest2, expected2)
    _drain_queue(queue1)
    _drain_queue(queue2)

    # Update in root1: ds1
    _write(root1 / "ds1" / "added.txt", "r1")
    expected_ctime = os.stat(root1 / "ds1" / "added.txt").st_ctime
    assert _wait_for_queue_key(queue1, "ds1", expected_ctime)
    assert manifest1["ds1"] == expected_ctime
    _drain_queue(queue1)

    # Update in root2: level1/ds3
    _write(root2 / "level1" / "ds3" / "added.txt", "r2")
    expected_ctime = os.stat(root2 / "level1" / "ds3" / "added.txt").st_ctime
    assert _wait_for_queue_key(queue2, "level1/ds3", expected_ctime)
    assert manifest2["level1/ds3"] == expected_ctime
    _drain_queue(queue2)
    
    e1.set(); e2.set()
    p1.join(timeout=2); p2.join(timeout=2)
    if p1.is_alive():
        p1.terminate()
    if p2.is_alive():
        p2.terminate()


def test_lazy_poller_with_prepopulated_manifest(tmp_path: Path) -> None:
    # Build many datasets under ds_all
    ds_all = tmp_path / "ds_all"
    num_datasets = 100
    manifest = {}
    q = mp.Queue()

    for i in range(1, num_datasets + 1):
        ds = ds_all / f"ds{i}"
        _write(ds / QH_DATASET_INFO_FILE)
        _write(ds / "file1.txt", f"{i}")
        rel = str(ds.relative_to(tmp_path))
        manifest[rel] = os.stat(ds / "file1.txt").st_ctime

    # Start poller with pre-filled manifest; it should not enqueue anything initially
    p, evt = _start_poller(tmp_path, manifest, q)
    assert _wait_for_queue_empty(q)

    # Create a new dataset next to the most recent one => detected quickly
    ds_new = ds_all / f"ds{num_datasets + 1}"
    _write(ds_new / QH_DATASET_INFO_FILE)
    _write(ds_new / "file1.txt", "new")
    rel_new = str(ds_new.relative_to(tmp_path))
    expected_ctime = os.stat(ds_new / "file1.txt").st_ctime
    assert _wait_for_queue_key(q, rel_new, expected_ctime)
    _drain_queue(q)

    # Create a dataset in a different subtree; the scan should not get there yet
    ds_other = tmp_path / "ds_other" / "ds001"
    _write(ds_other / QH_DATASET_INFO_FILE)
    _write(ds_other / "file1.txt", "o1")
    assert _wait_for_queue_empty(q, 1)
    
    evt.set()
    p.join(timeout=2)
    if p.is_alive():
        p.terminate()