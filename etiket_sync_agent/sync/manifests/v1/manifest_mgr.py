import threading
from multiprocessing import Process, Event, Queue as MPQueue
from queue import Queue, Empty

from dataclasses import dataclass, field
from typing import Dict, Optional, Any, Union
from pathlib import Path
import os

from etiket_sync_agent.sync.manifests.v1.dataset_poller_NFS import dataset_poller_NFS
from etiket_sync_agent.sync.manifests.v1.dataset_poller_local import dataset_poller_local
from etiket_sync_agent.sync.manifests.utility import dataset_get_mod_time

@dataclass
class ManifestStateV1:
    root_path: Path
    current_manifest: Dict[str, float]
    level: int
    is_NFS: bool = False
    is_single_file: bool = False
    update_queue: Any = field(default_factory=Queue)
    worker: Optional[Union[threading.Thread, Process]] = None
    stop_event: Optional[Any] = None
    local_observer: Optional[Any] = None
    
    def __post_init__(self):
        if self.is_NFS:
            self._start_nfs_worker(self.current_manifest)
        else:
            self.local_observer = dataset_poller_local(self.root_path, self.current_manifest, self.update_queue,
                                                        self.level, self.is_single_file)

    def _start_nfs_worker(self, current_manifest: Dict[str, float]) -> None:
        self.update_queue = MPQueue()
        self.stop_event = Event()
        proc = Process(
            target=dataset_poller_NFS,
            args=(self.root_path, current_manifest, self.update_queue, self.level, self.is_single_file, self.stop_event),
            daemon=True,
        )
        proc.start()
        self.worker = proc

    def _ensure_nfs_worker_alive(self) -> None:
        if not self.is_NFS:
            return
        if self.worker is None or not isinstance(self.worker, Process) or not self.worker.is_alive():
            self._start_nfs_worker(self.current_manifest)

    def get_last_change(self, identifier: str) -> float:
        manifest_path = Path.joinpath(self.root_path, *identifier.split('/'))
        if self.is_single_file:
            try:
                return os.stat(manifest_path).st_mtime
            except FileNotFoundError:
                return 0.0
        return dataset_get_mod_time(manifest_path)

    def push_update(self, identifier: str, priority: float) -> None:
        self.update_queue.put((identifier, priority))

    def get_updates(self) -> Dict[str, float]:
        # For NFS, ensure the background process is alive; restart if needed
        self._ensure_nfs_worker_alive()
        updates: Dict[str, float] = {}
        while True:
            try:
                key, val = self.update_queue.get_nowait()
                # ensure current_manifest is updated
                self.current_manifest[key] = val
                updates[key] = val
            except Empty:
                break
        return updates

    def shutdown(self, timeout: float = 2.0) -> None:
        # Stop NFS process if running
        if self.is_NFS and isinstance(self.worker, Process):
            if self.stop_event is not None:
                try:
                    self.stop_event.set()
                except (AttributeError, RuntimeError):
                    pass
            self.worker.join(timeout=timeout)
            if self.worker.is_alive():
                self.worker.terminate()
            self.worker = None
            return

        # Stop local observer (watchdog) if present
        if self.local_observer is not None:
            try:
                self.local_observer.stop()
                self.local_observer.join(timeout=timeout)
            except (AttributeError, RuntimeError):
                pass
            self.local_observer = None

        if isinstance(self.worker, threading.Thread) and self.worker.is_alive():
            self.worker.join(timeout=timeout)
        self.worker = None
