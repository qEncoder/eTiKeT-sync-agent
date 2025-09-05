"""
Unit tests for dataset manifest polling and watching (v2).

- create a structure looking like (in a temp folder):
    /root/ds1/_QH_dataset_info.yaml
    /root/ds1/file1.txt
    /root/ds2/_QH_dataset_info.yaml
    /root/level1/ds3/_QH_dataset_info.yaml
    /root/level1/ds3/file1.txt
    /root/level1/level2/ds4/_QH_dataset_info.yaml
    /root/level1/level2/ds4/file1.txt
    
    # test this for watcher enabled and disabled. stop watcher (observer) after test
    for each folder that is created in this way --> load the last modified time of the lastly created file in the folder.
    --> get a dict with the manifest (e.g ds1: some_timestamp, ds2: some_timestamp, level1/ds3: some_timestamp, level1/level2/ds4: some_timestamp)
    --> use the function dataset_poller_local --> start with an empty manifest, and queue
        --> check if the `current_manifest` is updated correctly (should match the new manifest)
        --> check if the `update_queue` is updated correctly (should match the new manifest)
    --> run the function again, with current manifest and empty manifest
        --> current manifest should not be changed and queue should be empty
- update tests:
    --> create the structure again, run the dataset_poller_local function
    --> start doing some updates (queue is flushed before)
    --> add a file to ds1 --> queue should be updated
    --> add a file to level1/ds3 --> queue should be updated
    --> add a hidden file to level1/ds3 --> queue should not be updated
    --> modify a file in ds1 --> queue should be updated
    --> modify the _QH_dataset_info.yaml file in ds2 --> queue should be updated
- test handling of mulitple pollers:
    --> create the structure in two different folders,
    --> run the poller in two seperate threads
    --> start in both with empty manifest, both should return the correct queues
    --> flush both queues
    --> update a file in the first folder --> check if dataset is added in the queue (wait 50ms to ensure the tread has time to process the update)
    --> do the same for the second folder
    --> check if the queue is correct in both folders

"""
import os, time
from queue import Queue, Empty

from pathlib import Path
from typing import Dict

from etiket_sync_agent.sync.manifests.v2.definitions import QH_DATASET_INFO_FILE
from etiket_sync_agent.sync.manifests.v2.dataset_puller_local import dataset_poller_local


def _touch(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a"):
        os.utime(p, None)

def _write(p: Path, content: str = "x") -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)

def _drain_queue(q: Queue) -> Dict[str, float]:
    out: Dict[str, float] = {}
    while True:
        try:
            k, v = q.get_nowait()
            out[k] = v
        except Empty:
            break
    return out

def _wait_for_queue_key(q: Queue, key: str, expected_value: float | None = None, timeout: float = 0.6) -> bool:
    start = time.monotonic()
    seen: Dict[str, float] = {}
    while time.monotonic() - start < timeout:
        try:
            k, v = q.get(timeout=0.01)
            seen[k] = v
            if k == key and (expected_value is None or v == expected_value):
                return True
        except Empty:
            pass
    return key in seen and (expected_value is None or seen.get(key) == expected_value)

def _wait_for_queue_empty(q: Queue, timeout: float = 0.5) -> bool:
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if q.empty():
            return True
        try:
            q.get(timeout=0.01)
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


def test_initial_scan_without_watcher(tmp_path: Path) -> None:
    expected = _build_example_tree(tmp_path)

    current_manifest: Dict[str, float] = {}
    update_queue: Queue = Queue()

    observer = dataset_poller_local(tmp_path, current_manifest, update_queue, enable_watcher=False)
    assert observer is None

    assert current_manifest == expected
    assert _drain_queue(update_queue) == expected


def test_idempotent_rescan(tmp_path: Path) -> None:
    expected = _build_example_tree(tmp_path)

    current_manifest: Dict[str, float] = {}
    update_queue: Queue = Queue()

    dataset_poller_local(tmp_path, current_manifest, update_queue, enable_watcher=False)
    # Flush queue and rescan
    _drain_queue(update_queue)
    observer = dataset_poller_local(tmp_path, current_manifest, update_queue, enable_watcher=False)
    assert observer is None

    assert current_manifest == expected
    assert _drain_queue(update_queue) == {}

def test_watcher_updates_and_hidden_ignored(tmp_path: Path) -> None:
    expected = _build_example_tree(tmp_path)

    current_manifest: Dict[str, float] = {}
    update_queue: Queue = Queue()

    observer = dataset_poller_local(tmp_path, current_manifest, update_queue, enable_watcher=True)
    try:
        # Initial scan results are already present
        assert current_manifest == expected
        assert _drain_queue(update_queue) == expected

        # Flush queue before making changes
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
    finally:
        if observer is not None:
            observer.stop()
            observer.join(timeout=2)


def test_multiple_pollers_independent_queues(tmp_path: Path) -> None:
    root1 = tmp_path / "root1"
    root2 = tmp_path / "root2"
    root1.mkdir()
    root2.mkdir()

    expected1 = _build_example_tree(root1)
    expected2 = _build_example_tree(root2)

    manifest1: Dict[str, float] = {}
    queue1: Queue = Queue()
    manifest2: Dict[str, float] = {}
    queue2: Queue = Queue()

    obs1 = dataset_poller_local(root1, manifest1, queue1, enable_watcher=True)
    obs2 = dataset_poller_local(root2, manifest2, queue2, enable_watcher=True)
    try:
        assert manifest1 == expected1
        assert manifest2 == expected2
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
    finally:
        if obs1 is not None:
            obs1.stop()
            obs1.join(timeout=2)
        if obs2 is not None:
            obs2.stop()
            obs2.join(timeout=2)


def test_watcher_move_event_updates_manifest(tmp_path: Path) -> None:
    expected = _build_example_tree(tmp_path)

    current_manifest: Dict[str, float] = {}
    update_queue: Queue = Queue()

    observer = dataset_poller_local(tmp_path, current_manifest, update_queue, enable_watcher=True)
    try:
        assert current_manifest == expected
        _drain_queue(update_queue)

        # Rename a file inside ds2 to trigger on_moved
        ds1 = tmp_path / "ds1"
        src = ds1 / "file1.txt"
        dst = ds1 / "file1_renamed.txt"
        src.rename(dst)

        expected_ctime = os.stat(ds1 / "file1_renamed.txt").st_ctime
        assert _wait_for_queue_key(update_queue, "ds1", expected_ctime)
        assert current_manifest["ds1"] == expected_ctime
        _drain_queue(update_queue)
        
    finally:
        if observer is not None:
            observer.stop()
            observer.join(timeout=2)


def test_new_dataset_creation_detected_by_watcher(tmp_path: Path) -> None:
    expected = _build_example_tree(tmp_path)

    current_manifest: Dict[str, float] = {}
    update_queue: Queue = Queue()

    observer = dataset_poller_local(tmp_path, current_manifest, update_queue, enable_watcher=True)
    try:
        assert current_manifest == expected
        _drain_queue(update_queue)

        # Create new dataset ds5 with info file and a data file
        ds5 = tmp_path / "level1" / "level2" / "ds5"
        _write(ds5 / QH_DATASET_INFO_FILE)
        _write(ds5 / "file1.txt", "n1")

        rel = str(ds5.relative_to(tmp_path))
        expected_ctime = os.stat(ds5 / "file1.txt").st_ctime
        assert _wait_for_queue_key(update_queue, rel, expected_ctime)
        assert current_manifest[rel] == expected_ctime
    finally:
        if observer is not None:
            observer.stop()
            observer.join(timeout=2)


def test_new_dataset_creation_detected_on_rescan(tmp_path: Path) -> None:
    expected = _build_example_tree(tmp_path)

    current_manifest: Dict[str, float] = {}
    update_queue: Queue = Queue()

    # Initial scan without watcher
    dataset_poller_local(tmp_path, current_manifest, update_queue, enable_watcher=False)
    assert current_manifest == expected
    _drain_queue(update_queue)

    # Create new dataset ds5
    ds5 = tmp_path / "ds5"
    _write(ds5 / QH_DATASET_INFO_FILE)
    _write(ds5 / "file1.txt", "n1")

    # Rescan to pick up ds5
    dataset_poller_local(tmp_path, current_manifest, update_queue, enable_watcher=False)

    rel = str(ds5.relative_to(tmp_path))
    drained = _drain_queue(update_queue)
    assert rel in drained
    assert drained[rel] == os.stat(ds5 / "file1.txt").st_ctime
    assert current_manifest[rel] == os.stat(ds5 / "file1.txt").st_ctime
