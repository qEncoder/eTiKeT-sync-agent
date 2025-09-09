'''
Tests for folderbase configuration.

- test FolderBaseConfigData.validate
* prodide a folder with a path that does not exist

* creat a sync source with an existing folder, then check that when adding again, the validate function fails.

* add a subfolder of an existing folder --> check if it fails.

* add a parent folder of an existing folder --> check if it fails.

'''

import tempfile
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from etiket_sync_agent.backends.folderbase.folderbase_config_class import FolderBaseConfigData
from etiket_sync_agent.models.enums import SyncSourceTypes, SyncSourceStatus
from etiket_sync_agent.models.sync_sources import SyncSources


def _clear_all_sources(session: Session):
    session.query(SyncSources).delete()
    session.commit()


def _add_filebase_source(session: Session, name: str, root_dir: Path) -> SyncSources:
    source = SyncSources(
        name=name,
        type=SyncSourceTypes.fileBase,
        status=SyncSourceStatus.SYNCHRONIZING,
        config_data={"root_directory": str(root_dir.resolve())},
        default_scope=None,
    )
    session.add(source)
    session.commit()
    session.refresh(source)
    return source


def test_validate_nonexistent_root_directory_raises(db_session: Session):
    with tempfile.TemporaryDirectory() as temp_dir:
        non_existing = Path(temp_dir) / "does_not_exist"
        cfg = FolderBaseConfigData(root_directory=non_existing, server_folder=False)
        with pytest.raises(ValueError):
            cfg.validate()


def test_validate_conflict_same_directory_fails(db_session: Session):
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            root.mkdir(parents=True, exist_ok=True)

            existing = _add_filebase_source(db_session, "existing_root", root)

            cfg = FolderBaseConfigData(root_directory=root, server_folder=False)
            with pytest.raises(ValueError) as exc:
                cfg.validate()

            # Sanity check message mentions already added
            assert "already added" in str(exc.value)
            assert existing.name in str(exc.value)
    finally:
        _clear_all_sources(db_session)


def test_validate_conflict_subdirectory_fails(db_session: Session):
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            parent = Path(temp_dir) / "parent"
            sub = parent / "sub"
            parent.mkdir(parents=True, exist_ok=True)
            sub.mkdir(parents=True, exist_ok=True)

            existing = _add_filebase_source(db_session, "parent_source", parent)

            cfg = FolderBaseConfigData(root_directory=sub, server_folder=False)
            with pytest.raises(ValueError) as exc:
                cfg.validate()

            msg = str(exc.value)
            assert "inside the directory" in msg
            assert str(parent.resolve()) in msg
            assert existing.name in msg
    finally:
        _clear_all_sources(db_session)


def test_validate_conflict_parent_directory_fails(db_session: Session):
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            parent = Path(temp_dir) / "parent"
            sub = parent / "sub"
            parent.mkdir(parents=True, exist_ok=True)
            sub.mkdir(parents=True, exist_ok=True)

            existing = _add_filebase_source(db_session, "sub_source", sub)

            cfg = FolderBaseConfigData(root_directory=parent, server_folder=False)
            with pytest.raises(ValueError) as exc:
                cfg.validate()

            msg = str(exc.value)
            assert "inside the specified directory" in msg
            assert str(parent.resolve()) in msg
            assert existing.name in msg
    finally:
        _clear_all_sources(db_session)
