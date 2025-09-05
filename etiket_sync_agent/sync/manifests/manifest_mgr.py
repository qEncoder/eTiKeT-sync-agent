from etiket_sync_agent.sync.manifests.v1.manifest_mgr import ManifestStateV1
from etiket_sync_agent.sync.manifests.v2.manifest_mgr import ManifestStateV2

from typing import Dict, Optional
from pathlib import Path

import logging

logger = logging.getLogger(__name__)

class manifest_manager:
    __manifest_contents = {}
    
    def __init__(self, name : str, root_path : Optional[Path] = None, current_manifest : Optional[Dict[str, float]] = None,  level : int = -1, is_NFS : bool = False, is_single_file : bool = False):
        '''
        initialise the manifest manager with the name of the dataset and the root path of the dataset.
        
        Args:
            name (str) : the name of the dataset
            root_path (Path) : the root path of the dataset
            current_manifest (Dict[str, float]) : the current manifest that contains the keys of the datasets and the last modified time of the dataset.
            level (int) : the depth of folders at which the datasets are stored. Default is 1.
            is_single_file (bool) : whether the dataset is a single file or a directory with files. Default is False.
        '''
        self.name = name
        if name in self.__manifest_contents:
            self.state = self.__manifest_contents[name]
        else:
            logger.debug("Initializing manifest manager for %s", name)
            if current_manifest is None:
                current_manifest = {}
            if root_path is None:
                raise ValueError("Root path is required for V2 manifest manager")
            
            if level > 0:
                self.state = ManifestStateV1(root_path, current_manifest, level, is_NFS=is_NFS, is_single_file=is_single_file)
            else:
                self.state = ManifestStateV2(root_path, current_manifest, is_NFS=is_NFS)
            logger.debug("Initialized manifest manager for %s", name)
            self.__manifest_contents[name] = self.state
    
    def get_last_change(self, data_identifier : str) -> float:
        '''
        Get the current manifest of the dataset. This can be used to compare changes.
        '''
        return self.state.get_last_change(data_identifier)
    
    def push_update(self, identifier : str, priority : float):
        '''
        Push manually an update to the manifest manager.
        '''
        if hasattr(self.state, 'push_update'):
            self.state.push_update(identifier, priority)
        else:
            self.state.update_queue[identifier] = priority
        
    def get_updates(self) -> Dict[str, float]:
        '''
        Get changes in the manifest since last update call.
        '''
        return self.state.get_updates()
    
    @staticmethod
    def delete_manifest(name : str):
        '''
        Delete the manifest manager and remove the manifest from the manifest contents.
        '''
        if name in manifest_manager.__manifest_contents:
            manifest_manager.__manifest_contents[name].shutdown()
            del manifest_manager.__manifest_contents[name]
            