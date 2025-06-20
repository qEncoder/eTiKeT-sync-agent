from typing import Type

from etiket_sync_agent.models.sync_sources import  SyncSourceTypes
from etiket_sync_agent.sync.sync_source_abstract import SyncSourceFileBase, SyncSourceDatabaseBase

def get_mapping() -> tuple[dict[SyncSourceTypes, Type[SyncSourceDatabaseBase]|Type[SyncSourceFileBase]], dict[SyncSourceTypes, Type]]:
    # TODO : This is a temporary solution.
    from etiket_sync_agent.backends.quantify.quantify_sync_class import QuantifySync, QuantifyConfigData
    from etiket_sync_agent.backends.qcodes.qcodes_sync_class import QCoDeSSync, QCoDeSConfigData
    from etiket_sync_agent.backends.core_tools.core_tools_sync_class import CoreToolsSync, CoreToolsConfigData
    from etiket_sync_agent.backends.filebase.filebase_sync_class import FileBaseSync, FileBaseConfigData

    type_mapping = {SyncSourceTypes.quantify : QuantifySync,
                    SyncSourceTypes.qcodes : QCoDeSSync,
                    SyncSourceTypes.coretools : CoreToolsSync,
                    SyncSourceTypes.fileBase : FileBaseSync}

    config_mapping = {SyncSourceTypes.quantify : QuantifyConfigData,
                        SyncSourceTypes.qcodes : QCoDeSConfigData,
                        SyncSourceTypes.coretools : CoreToolsConfigData,
                        SyncSourceTypes.fileBase : FileBaseConfigData}

    return type_mapping, config_mapping

def detect_type(sync_class, sync_config) -> SyncSourceTypes:
    # TODO : This is a temporary solution.
    from etiket_sync_agent.backends.quantify.quantify_sync_class import QuantifySync, QuantifyConfigData
    from etiket_sync_agent.backends.qcodes.qcodes_sync_class import QCoDeSSync, QCoDeSConfigData
    from etiket_sync_agent.backends.core_tools.core_tools_sync_class import CoreToolsSync, CoreToolsConfigData
    from etiket_sync_agent.backends.filebase.filebase_sync_class import FileBaseSync, FileBaseConfigData
    
    
    if sync_class == QuantifySync and isinstance(sync_config, QuantifyConfigData):
        return SyncSourceTypes.quantify
    elif sync_class == QCoDeSSync and isinstance(sync_config, QCoDeSConfigData):
        return SyncSourceTypes.qcodes
    elif sync_class == CoreToolsSync and isinstance(sync_config, CoreToolsConfigData):
        return SyncSourceTypes.coretools
    elif sync_class == FileBaseSync and isinstance(sync_config, FileBaseConfigData):
        return SyncSourceTypes.fileBase
    
    raise ValueError(f"Unknown sync source type: {sync_class} with config {sync_config}")

def get_source_config_class(sync_source_type : SyncSourceTypes) -> Type:
    _, config_mapping = get_mapping()
    return config_mapping[sync_source_type]

def get_source_sync_class(sync_source_type : SyncSourceTypes) -> Type[SyncSourceDatabaseBase]|Type[SyncSourceFileBase]:
    type_mapping, _ = get_mapping()
    return type_mapping[sync_source_type]
