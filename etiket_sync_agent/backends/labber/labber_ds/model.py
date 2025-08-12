import numpy as np

from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Any, List

@dataclass
class StepConfig:
    optimizer: Dict[str, Any]
    relation_parameters: List[Dict[str, Any]]
    step_items: List[Dict[str, Any]]

@dataclass
class TraceData:
    name : str
    unit : str
    values : np.ndarray

    irregular_setpoints : bool
    setpoint_values : np.ndarray
    setpoint_unit : str
    setpoint_name : str

@dataclass
class ChannelData:
    name: str
    unit : str
    values: np.ndarray
    is_setpoint: bool

@dataclass
class RawTraceData:
    name: str
    data : np.ndarray
    attributes : Dict[str, Any]

@dataclass
class LabberDataContent:
    channels: Dict[str, Any]
    data: List[ChannelData]
    instrument_config: Dict[str, Any]
    instruments: List[Dict[str, Any]]
    log_list: List[str]
    step_config: Dict[str, StepConfig]
    step_list: List[Dict[str, Any]]
    traces: Dict[str, TraceData]

@dataclass
class LabberDataset:
    dataset_name: str
    dataset_content: List[LabberDataContent]
    settings: Dict[str, Any]
    tags: Dict[str, Any]
    is_starred: bool
    creation_time: datetime