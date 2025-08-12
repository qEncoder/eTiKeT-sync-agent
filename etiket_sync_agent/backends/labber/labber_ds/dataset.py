import h5py, re, pathlib
import numpy as np

from datetime import datetime
from typing import Dict, Any, List, Tuple, Union

from etiket_client.sync.backends.labber.labber_ds.model import LabberDataset, LabberDataContent,\
    StepConfig, TraceData, ChannelData, RawTraceData

def read_compound_dataset(dataset: h5py.Dataset) -> List[Dict[str, Any]]:
    """Convert a compound dataset to a list of dictionaries."""
    if not isinstance(dataset, h5py.Dataset) or dataset.dtype.kind != 'V':
        raise ValueError("Dataset must be a compound type")
    result = []
    for item in dataset:
        result.append({name: _convert_to_python_type(item[name]) for name in dataset.dtype.names})
    return result

def _convert_to_python_type(value: Any) -> Any:
    """Converts a value to its Python native type if it's a NumPy type or bytes, handling complex numbers for JSON serialization."""
    # 1. Handle NumPy scalars first, converting them to Python native types
    if isinstance(value, np.generic):
        value = value.item() # value is now a Python native type (e.g., int, float, complex, bool)

    # 2. Now handle Python native types that need special conversion for JSON or further processing
    if isinstance(value, complex):
        return {'real': value.real, 'imag': value.imag}
    # 3. Handle NumPy arrays (which might contain complex numbers or other types)
    elif isinstance(value, np.ndarray):
        # .tolist() converts array elements to Python types;
        # then recursively apply this function to handle complex numbers or nested structures within the list.
        return [_convert_to_python_type(v) for v in value.tolist()]
    # 4. Handle generic Python lists and dicts for recursion
    elif isinstance(value, list):
        return [_convert_to_python_type(v) for v in value]
    elif isinstance(value, dict):
        return {k: _convert_to_python_type(v_item) for k, v_item in value.items()}
    # 5. Handle bytes
    elif isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            return value # Return as bytes if decoding fails
    
    # 6. If none of the above, assume it's already a JSON-serializable Python native type
    return value

def read_attributes(obj: Union[h5py.Group, h5py.Dataset]) -> Dict[str, Any]:
    """Read all attributes from a group or dataset, converting NumPy types to Python native types."""
    attrs = {}
    for name, value in obj.attrs.items():
        attrs[name] = _convert_to_python_type(value)
    return attrs

def read_numpy_array(dataset: h5py.Dataset) -> np.ndarray:
    """Read a dataset as a numpy array."""
    if not isinstance(dataset, h5py.Dataset):
        raise ValueError("Input must be a dataset")
    return np.asarray(dataset[:])

def _get_single_step_config(group: h5py.Group, prefix: str = "") -> Tuple[str, StepConfig]:
    group_name_part = ""
    if group.name and isinstance(group.name, str):
        name_parts = group.name.rsplit('/')
        if name_parts:
            group_name_part = name_parts[-1]
    
    if "Optimizer" in group:
        optimizer_obj = group.get('Optimizer')
        optimizer = {}
        if isinstance(optimizer_obj, (h5py.Group, h5py.Dataset)):
            optimizer = read_attributes(optimizer_obj)
        # else: Decide how to handle if optimizer_obj is a Datatype or other

        relation_parameters_ds = group['Relation parameters']
        relation_parameters = [] # Default to empty list
        if isinstance(relation_parameters_ds, h5py.Dataset):
            relation_parameters = read_compound_dataset(relation_parameters_ds)
        # else: Decide how to handle if not a Dataset

        step_items_ds = group['Step items']
        step_items = [] # Default to empty list
        if isinstance(step_items_ds, h5py.Dataset):
            step_items = read_compound_dataset(step_items_ds)
        # else: Decide how to handle if not a Dataset

        return prefix + group_name_part, StepConfig(optimizer=optimizer, relation_parameters=relation_parameters, step_items=step_items)
    else:
        # get group that is there (should only by one)
        if len(group.keys()) == 1:
            # Ensure the item is a group before recursive call
            item = list(group.values())[0]
            if isinstance(item, h5py.Group):
                return _get_single_step_config(item, prefix + group_name_part + "/")
            else:
                raise ValueError("Expected item in group to be an h5py.Group")
        else:
            raise ValueError("Expected exactly one group in the current group")

def get_step_config(main_group: h5py.Group) -> Dict[str, StepConfig]:
    step_config = {}
    for name, group in main_group.items():
        name, step_config_data = _get_single_step_config(group)
        step_config[name] = step_config_data    
    return step_config

def _add_data_to_channel_data(channel_data: ChannelData, values: np.ndarray, channel_name: dict) -> ChannelData:
    info = channel_name.get('info', '')

    if info == "Imaginary":
        channel_data.values = values*1j + channel_data.values 
    else:
        channel_data.values += values
    return channel_data

def read_channel_data(main_group: h5py.Group, channel_info: Dict[str, Any], log_list: List[str]) -> List[ChannelData]:
    channel_names_ds = main_group.get('Channel names')
    channel_names = []
    if isinstance(channel_names_ds, h5py.Dataset):
        channel_names = read_compound_dataset(channel_names_ds)
    
    raw_data_ds = main_group.get('Data')
    # Assuming raw_data_ds must be a Dataset, otherwise behavior is undefined.
    # If it can be something else, more robust error handling or default for raw_data is needed.
    if not isinstance(raw_data_ds, h5py.Dataset):
        # Or handle this case by returning empty data or raising a more specific error
        raise ValueError("Expected 'Data' to be a Dataset in read_channel_data")
    raw_data = read_numpy_array(raw_data_ds)

    data = []
    for i in range(len(channel_names)):
        name = channel_names[i]['name']
        channel_data = ChannelData(name=name, values=np.zeros((raw_data[:, i, :].size,)),
                                    unit=channel_info[name].get('unitPhys', 'a.u.'),
                                    is_setpoint=name not in log_list)
        # in case of complex data, the name is repeated twice --> complex data needs to be added twice.
        data_names = [data.name for data in data]
        if name in data_names:
            _add_data_to_channel_data(data[data_names.index(name)], raw_data[:, i, :].T.flatten(), channel_names[i])
        else:
            _add_data_to_channel_data(channel_data, raw_data[:, i, :].T.flatten(), channel_names[i])
            data.append(channel_data)

    # reorder the data (such that the the fast changing variable is the last one)
    return data[::-1]

def get_traces(raw_traces: Dict[str, RawTraceData], channel_info: Dict[str, Any]) -> Dict[str, TraceData]:
    """Get traces from a LabberData object."""
    traces = {}
    
    trace_names = list(raw_traces.keys())
    # remove any trace that equals timestamps, or ends with _N , _t0dt
    trace_names = [i for i in trace_names if i != "Time stamp" and not i.endswith("_N") and not i.endswith("_t0dt")]

    for trace_name in trace_names:
        data = raw_traces[trace_name].data
        setpoint_name = raw_traces[trace_name].attributes.get("x, name", "unknown")
        setpoint_unit = raw_traces[trace_name].attributes.get("x, unit", "a.u.")   
        is_complex = raw_traces[trace_name].attributes.get("complex", False)
        if is_complex:
            data = data[:, 0, :] + 1j*data[:, 1, :]
        else:
            if data.ndim != 1:
                data = data[:, 0, :]
        setpoints_t0dt = raw_traces[trace_name + "_t0dt"].data
        if setpoints_t0dt.shape[0] == 1:
            irregular_setpoints = False
            setpoints = setpoints_t0dt[0][0] + np.arange(data.shape[0])*setpoints_t0dt[0][1]
        else:
            irregular_setpoints = True
            setpoints = np.zeros(data.shape)

            for i in range(data.shape[1]):
                setpoints[:, i] = setpoints_t0dt[0][0] + np.arange(data.shape[0])*setpoints_t0dt[0][1]

        var_name = channel_info[trace_name].get("name", "unknown")
        var_unit = channel_info[trace_name].get("unitPhys", "a.u.")

        traces[trace_name] = TraceData(var_name, var_unit, data.T, irregular_setpoints, 
                                        setpoint_values=setpoints, setpoint_unit=setpoint_unit, setpoint_name=setpoint_name)

    return traces


def read_dataset_content(hdf5_log_group: h5py.Group) -> LabberDataContent:
    log_list_raw_ds = hdf5_log_group.get('Log list')
    log_list_raw = []
    if isinstance(log_list_raw_ds, h5py.Dataset):
        log_list_raw = read_compound_dataset(log_list_raw_ds)
    
    log_list = []
    for i in log_list_raw:
        channel_name = i['channel_name']
        log_list.append(channel_name)
    
    # Read channels
    channels_raw_ds = hdf5_log_group.get('Channels')
    channels_raw = []
    if isinstance(channels_raw_ds, h5py.Dataset):
        channels_raw = read_compound_dataset(channels_raw_ds)

    channels = {}
    for i in channels_raw:
        channels[i['name']] = i

    # Read Data group
    data_obj = hdf5_log_group.get('Data')
    data = [] # Default to empty list
    if isinstance(data_obj, h5py.Group):
        data = read_channel_data(data_obj, channels, log_list)
    # else: Decide how to handle if data_obj is not a Group (e.g. log warning, use default)

    # Read Instrument config
    instrument_config = {}
    instrument_config_group = hdf5_log_group.get('Instrument config')
    if isinstance(instrument_config_group, h5py.Group):
        for name, group_item in instrument_config_group.items():
            if isinstance(group_item, h5py.Group):
                instrument_config[name] = read_attributes(group_item)
            # else: handle if it's a Dataset or Datatype if necessary

    # Read instruments dataset
    instruments_ds = hdf5_log_group.get('Instruments')
    instruments = [] # Default to empty list
    if isinstance(instruments_ds, h5py.Dataset):
        instruments = read_compound_dataset(instruments_ds)
    # else: Decide how to handle if not a Dataset

    # Read Step config
    step_config_group = hdf5_log_group.get('Step config')
    step_config = {} # Default to empty dict
    if isinstance(step_config_group, h5py.Group):
        step_config = get_step_config(step_config_group)
    # else: Decide how to handle if not a Group

    # Read Step list
    step_list_ds = hdf5_log_group.get('Step list')
    step_list = [] # Default to empty list
    if isinstance(step_list_ds, h5py.Dataset):
        step_list = read_compound_dataset(step_list_ds)
    # else: Decide how to handle if not a Dataset

    # Read Traces
    raw_traces = {}
    traces_group = hdf5_log_group.get('Traces')
    if isinstance(traces_group, h5py.Group):
        for name, dataset_item in traces_group.items():
            if isinstance(dataset_item, h5py.Dataset):
                trace_data = read_numpy_array(dataset_item)
                trace_attributes = read_attributes(dataset_item)
                raw_traces[name] = RawTraceData(name, trace_data, trace_attributes)
            # else: handle if it's a Group or Datatype if necessary

    traces = get_traces(raw_traces, channels)
        
    return LabberDataContent(
        channels=channels, # unit of log list here.
        data=data,
        instrument_config=instrument_config,
        instruments=instruments,
        log_list=log_list, # names of the measurement setpoints
        step_config=step_config,
        step_list=step_list, # names of the variables
        traces=traces # values of the setpoints
    )

def read_labber_file(file_path: pathlib.Path) -> LabberDataset:
    """Read a Labber HDF5 file and return structured data."""
    with h5py.File(file_path, 'r') as hdf5_file:
        # Read dataset name
        dataset_name = _convert_to_python_type(hdf5_file.attrs.get('log_name', ''))
        is_starred = hdf5_file.attrs.get('star', False)
        creation_time_timestamp = hdf5_file.attrs.get('creation_time', None)
        if creation_time_timestamp is not None:
            creation_time = datetime.fromtimestamp(creation_time_timestamp)
        else:
            # file creation time
            creation_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        # Read Settings
        settings_obj = hdf5_file.get('/Settings')
        settings = {} # Default to empty dict
        if isinstance(settings_obj, (h5py.Group, h5py.Dataset)):
            settings = read_attributes(settings_obj)
        # else: Decide how to handle if not Group or Dataset

        # Read Tags
        tags_obj = hdf5_file.get('/Tags')
        tags = {} # Default to empty dict
        if isinstance(tags_obj, (h5py.Group, h5py.Dataset)):
            tags = read_attributes(tags_obj)
        
        dataset_content = []
        dataset_content.append(read_dataset_content(hdf5_file))
        
        for name, group_item in hdf5_file.items():
            if re.match('^Log_\\d+$', name) and isinstance(group_item, h5py.Group):
                dataset_content.append(read_dataset_content(group_item))
        
        return LabberDataset(
            dataset_name=dataset_name,
            settings=settings,  
            dataset_content=dataset_content,
            tags=tags,
            is_starred=is_starred,
            creation_time=creation_time
        )
