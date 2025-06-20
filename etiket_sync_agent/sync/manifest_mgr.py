from etiket_sync_agent.sync.manifest_v1 import manifest_state_V1
from etiket_sync_agent.sync.manifest_v2 import manifest_state_V2

from typing import Dict
from pathlib import Path

import logging

logger = logging.getLogger(__name__)

class manifest_manager:
    __manifest_contents = {}
    
    def __init__(self, name : str, root_path : Path = None, current_manifest : Dict[str, float] = None,  level : int = -1, is_NFS : bool = False):
        '''
        initialise the manifest manager with the name of the dataset and the root path of the dataset.
        
        Args:
            name (str) : the name of the dataset
            root_path (Path) : the root path of the dataset
            current_manifest (Dict[str, float]) : the current manifest that contains the keys of the datasets and the last modified time of the dataset.
            level (int) : the depth of folders at which the datasets are stored. Default is 1.
        '''
        self.name = name
        if name in self.__manifest_contents:
            self.state = self.__manifest_contents[name]
        else:
            logger.debug("Initializing manifest manager for %s", name)
            if level > 0:
                self.state = manifest_state_V1(root_path, current_manifest, level, is_NFS=is_NFS)
            else:
                self.state = manifest_state_V2(root_path, current_manifest, is_NFS=is_NFS)
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
        self.state.update_queue[identifier] = priority
        
    def get_updates(self) -> Dict[str, int]:
        '''
        Get changes in the manifest since last update call.
        '''
        updates = {**self.state.update_queue}
        self.state.update_queue.clear()
        return updates
    
    @staticmethod
    def delete_manifest(name : str):
        '''
        Delete the manifest manager and remove the manifest from the manifest contents.
        '''
        if name in manifest_manager.__manifest_contents:
            del manifest_manager.__manifest_contents[name]