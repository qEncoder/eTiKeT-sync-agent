'''
create a folder structure for level 1:
- root/ds1/file1.txt
- root/ds1/sub_folder/file2.txt
- root/ds1/sub_folder/sub_folder/file3.txt
- root/ds2/file1.txt
- root/ds3/file1.txt
- root/ds4/file1.txt

# get manifest (e.g. ds1, ds2, ds3, ds4 with ctime of the last file).

create a folder structure for level 3:
- root/folder_1/folder_1/ds1/file1.txt
- root/folder_1/folder_1/ds1/sub_folder/file2.txt
- root/folder_1/folder_1/ds1/sub_folder/sub_folder/file3.txt
- root/folder_2/folder_1/ds2/file1.txt
- root/folder_2/folder_2/ds3/file1.txt
- root/folder_3/folder_0/ds4/file1.txt

# get manifest (e.g. folder_1/folder_1/ds1, folder_1/folder_1/ds2, folder_2/folder_1/ds3, folder_3/folder_0/ds4 with ctime of the last file).

# test local poller:
    #  --> check if it finds the correct datasets, and can detect updates within subfolders.
    # use structure 1 :: provide empty manifest and queue
    --> check if the the update queue matches the expected manifest
    --> flush queue
    --> add a file to ds1 :: check if the the update queue matches the expected manifest
    --> flush queue
    --> wait 400ms --> check empty queue
    --> modify to ds1/sub_folder/file2.txt :: check if the the update queue matches the expected manifest
    --> flush queue
    --> add file to ds1/sub_folder/ :: check if the the update queue matches the expected manifest
    # check update detection in a high level folder
    # use structure 2 :: provide empty manifest and queue
    --> check if the the update queue matches the expected manifest
    --> flush queue
    --> add a file to folder_1/folder_1/ds1 :: check if the the update queue matches the expected manifest
    --> flush queue
    --> modify to folder_1/folder_1/ds1/sub_folder/file2.txt :: check if the the update queue matches the expected manifest
    --> flush queue
    --> add a new folder with file --> root/folder_1/folder_1/ds5/file2.txt :: check if the the update queue matches the expected manifest
    --> flush queue
    --> add a new folder with file --> root/folder_3/folder_1/ds6/file2.txt :: check if the the update queue matches the expected manifest

# test NFS poller:
    --> all the tests of the local poller
    --> large folder check (100 datasets)
    --> level2 folders check, intial datasets can be like root/A/ds1/file1.txt, root/A/ds2/file1.txt, ...
    --> load with the expected manifest the poller, and check if the queue is empty (1 sec)
    --> add a new dataset in (root/a/ds101/file1.txt)
    --> check if the queue is updated (max 1 second)
    --> add a new dataset in (root/B/ds1/file1.txt)
    --> check that the queue is not updated (1 second)

Please implement for this case with the v1 manifest manager.
'''
import os, time

from pathlib import Path
from typing import Dict

from etiket_sync_agent.sync.manifests.manifest_mgr import manifest_manager
from etiket_sync_agent.sync.manifests.utility import dataset_get_mod_time


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

def _wait_for_manifest(mm: manifest_manager, expected: Dict[str, float], timeout: float = 2.0) -> bool:
    start = time.monotonic()
    acc: Dict[str, float] = {}
    while time.monotonic() - start < timeout:
        updates = mm.get_updates()
        print(f"got updates: {updates}")
        if updates:
            acc.update(updates)
        if acc == expected:
            return True
        time.sleep(0.02)
        print("end while")
    # final check
    acc.update(mm.get_updates())
    print(acc, expected)
    return acc == expected

def _wait_for_update_key(mm: manifest_manager, key: str, expected_value: float | None = None, timeout: float = 1.5) -> bool:
    start = time.monotonic()
    seen: Dict[str, float] = {}
    while time.monotonic() - start < timeout:
        updates = mm.get_updates()
        if updates:
            seen.update(updates)
            val = updates.get(key)
            if val is not None and (expected_value is None or val == expected_value):
                return True
        time.sleep(0.02)
    val = seen.get(key)
    return val is not None and (expected_value is None or val == expected_value)

def _wait_for_no_updates(mm: manifest_manager, timeout: float = 0.8) -> bool:
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if not mm.get_updates():
            time.sleep(0.05)
            # double-check still empty on the next tick
            if not mm.get_updates():
                return True
        else:
            # drain anything that may have appeared
            time.sleep(0.02)
    return not bool(mm.get_updates())


# --------------------
# Builders for test trees
# --------------------

def _build_level1_tree(root: Path) -> Dict[str, float]:
    ds1 = root / "ds1"
    ds2 = root / "ds2"
    ds3 = root / "ds3"
    ds4 = root / "ds4"

    # Populate nested structure in ds1
    _write(ds1 / "file1.txt", "a")
    _write(ds1 / "sub_folder" / "file2.txt", "b")
    _write(ds1 / "sub_folder" / "sub_folder" / "file3.txt", "c")

    # Single file in other datasets
    _write(ds2 / "file1.txt", "d")
    _write(ds3 / "file1.txt", "e")
    _write(ds4 / "file1.txt", "f")

    expected = {
        "ds1": os.stat(ds1 / "sub_folder" / "sub_folder" / "file3.txt").st_ctime,
        "ds2": os.stat(ds2 / "file1.txt").st_ctime,
        "ds3": os.stat(ds3 / "file1.txt").st_ctime,
        "ds4": os.stat(ds4 / "file1.txt").st_ctime,
    }
    return expected

def _build_level3_tree(root: Path) -> Dict[str, float]:
    d1 = root / "folder_1" / "folder_1" / "ds1"
    d2 = root / "folder_2" / "folder_1" / "ds2"
    d3 = root / "folder_2" / "folder_2" / "ds3"
    d4 = root / "folder_3" / "folder_0" / "ds4"

    _write(d1 / "file1.txt", "1")
    _write(d1 / "sub_folder" / "file2.txt", "2")
    _write(d1 / "sub_folder" / "sub_folder" / "file3.txt", "3")

    _write(d2 / "file1.txt", "4")
    _write(d3 / "file1.txt", "5")
    _write(d4 / "file1.txt", "6")

    expected = {
        "folder_1/folder_1/ds1": os.stat(d1 / "sub_folder" / "sub_folder" / "file3.txt").st_ctime,
        "folder_2/folder_1/ds2": os.stat(d2 / "file1.txt").st_ctime,
        "folder_2/folder_2/ds3": os.stat(d3 / "file1.txt").st_ctime,
        "folder_3/folder_0/ds4": os.stat(d4 / "file1.txt").st_ctime,
    }
    return expected


# ----- #
# Local watcher tests (V1, level-based)
# ----- #

def test_local_level1_initial_and_updates(tmp_path: Path) -> None:
    expected = _build_level1_tree(tmp_path)
    name = "v1-local-l1"
    try:
        print("building manifest manager")
        mm = manifest_manager(name, tmp_path, {}, level=1, is_NFS=False)
        print("waiting for manifest")
        assert _wait_for_manifest(mm, expected)

        # Add a file to ds1 => ds1 updated
        ds1 = tmp_path / "ds1"
        _write(ds1 / "new.txt", "z")
        exp = dataset_get_mod_time(ds1)
        assert _wait_for_update_key(mm, "ds1", exp)


        # Wait 400ms => should remain empty
        time.sleep(0.4)
        assert _wait_for_no_updates(mm)

        # Modify existing file in subfolder
        _write(ds1 / "sub_folder" / "file2.txt", "modified")
        exp = dataset_get_mod_time(ds1)
        assert _wait_for_update_key(mm, "ds1", exp)

        # Add file in sub_folder
        _write(ds1 / "sub_folder" / "added.txt", "x")
        exp = dataset_get_mod_time(ds1)
        assert _wait_for_update_key(mm, "ds1", exp)
    finally:
        manifest_manager.delete_manifest(name)


def test_local_level3_initial_and_updates(tmp_path: Path) -> None:
    expected = _build_level3_tree(tmp_path)
    name = "v1-local-l3"
    try:
        mm = manifest_manager(name, tmp_path, {}, level=3, is_NFS=False)
        assert _wait_for_manifest(mm, expected)

        d1 = tmp_path / "folder_1" / "folder_1" / "ds1"
        # Add a file to ds1
        _write(d1 / "added.txt", "y")
        exp = dataset_get_mod_time(d1)
        assert _wait_for_update_key(mm, "folder_1/folder_1/ds1", exp)

        # Modify a file in subfolder
        _write(d1 / "sub_folder" / "file2.txt", "mod")
        exp = dataset_get_mod_time(d1)
        assert _wait_for_update_key(mm, "folder_1/folder_1/ds1", exp)

        # Add new dataset ds5 in same subtree
        d5 = tmp_path / "folder_1" / "folder_1" / "ds5"
        _write(d5 / "file2.txt", "n")
        exp = dataset_get_mod_time(d5)
        assert _wait_for_update_key(mm, "folder_1/folder_1/ds5", exp)

        # Add new dataset ds6 in different subtree
        d6 = tmp_path / "folder_3" / "folder_1" / "ds6"
        _write(d6 / "file2.txt", "n6")
        exp = dataset_get_mod_time(d6)
        assert _wait_for_update_key(mm, "folder_3/folder_1/ds6", exp)
    finally:
        manifest_manager.delete_manifest(name)


# ----- #
# NFS poller tests (V1, process-based, level-based)
# ----- #

def test_nfs_initial_and_no_duplicates(tmp_path: Path) -> None:
    expected = _build_level1_tree(tmp_path)
    name = "v1-nfs-initial"
    try:
        mm = manifest_manager(name, tmp_path, {}, level=1, is_NFS=True)
        assert _wait_for_manifest(mm, expected)
        # Drain anything left
        mm.get_updates()
        # Without changes, no new updates
        assert _wait_for_no_updates(mm)
    finally:
        manifest_manager.delete_manifest(name)


def test_nfs_updates_and_hidden_ignored(tmp_path: Path) -> None:
    expected = _build_level1_tree(tmp_path)
    name = "v1-nfs-updates"
    try:
        mm = manifest_manager(name, tmp_path, {}, level=1, is_NFS=True)
        assert _wait_for_manifest(mm, expected)
        mm.get_updates()

        # Add a file to ds1
        ds1 = tmp_path / "ds1"
        _write(ds1 / "extra.txt", "z")
        mod_time =  os.stat(ds1 / "extra.txt").st_ctime
        assert _wait_for_update_key(mm, "ds1", mod_time)
        mm.get_updates()

        # Hidden file should be ignored
        _write(ds1 / ".hidden.txt", "ignored")
        assert _wait_for_no_updates(mm)

        # Modify an existing file
        _write(ds1 / "file1.txt", "modified")
        exp = os.stat(ds1 / "file1.txt").st_ctime
        assert _wait_for_update_key(mm, "ds1", exp)
        mm.get_updates()
    finally:
        manifest_manager.delete_manifest(name)


def test_nfs_multiple_pollers_independent(tmp_path: Path) -> None:
    root1 = tmp_path / "root1"
    root2 = tmp_path / "root2"
    root1.mkdir()
    root2.mkdir()

    expected1 = _build_level1_tree(root1)
    expected2 = _build_level1_tree(root2)

    name1 = "v1-nfs-multi-1"
    name2 = "v1-nfs-multi-2"
    try:
        mm1 = manifest_manager(name1, root1, {}, level=1, is_NFS=True)
        mm2 = manifest_manager(name2, root2, {}, level=1, is_NFS=True)

        assert _wait_for_manifest(mm1, expected1)
        assert _wait_for_manifest(mm2, expected2)
        mm1.get_updates(); mm2.get_updates()

        # Update in root1: ds1
        _write(root1 / "ds1" / "added.txt", "r1")
        exp = dataset_get_mod_time(root1 / "ds1")
        assert _wait_for_update_key(mm1, "ds1", exp)
        mm1.get_updates()

        # Update in root2: ds3
        _write(root2 / "ds3" / "added.txt", "r2")
        exp = dataset_get_mod_time(root2 / "ds3")
        assert _wait_for_update_key(mm2, "ds3", exp)
        mm2.get_updates()
    finally:
        manifest_manager.delete_manifest(name1)
        manifest_manager.delete_manifest(name2)


def test_nfs_lazy_poller_with_prepopulated_manifest(tmp_path: Path) -> None:
    # Build many datasets under ds_all at level 2
    ds_all = tmp_path / "ds_all"
    num_datasets = 100
    manifest: Dict[str, float] = {}
    for i in range(1, num_datasets + 1):
        ds = ds_all / f"ds{i}"
        _write(ds / "file1.txt", f"{i}")
        rel = str(ds.relative_to(tmp_path))
        manifest[rel] = dataset_get_mod_time(ds)

    name = "v1-nfs-lazy"
    try:
        mm = manifest_manager(name, tmp_path, manifest.copy(), level=2, is_NFS=True)
        # Should not enqueue anything initially
        assert _wait_for_no_updates(mm)

        # Create a new dataset next to the most recent one => detected quickly
        ds_new = ds_all / f"ds{num_datasets + 1}"
        _write(ds_new / "file1.txt", "new")
        rel_new = str(ds_new.relative_to(tmp_path))
        exp = dataset_get_mod_time(ds_new)
        assert _wait_for_update_key(mm, rel_new, exp)
        mm.get_updates()

        # Create a dataset in a different subtree; the scan should not get there yet
        ds_other = tmp_path / "ds_other" / "ds001"
        _write(ds_other / "file1.txt", "o1")
        assert _wait_for_no_updates(mm, 1.0)
    finally:
        manifest_manager.delete_manifest(name)