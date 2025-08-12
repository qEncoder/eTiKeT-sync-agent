import numpy as np
import xarray as xr
import re

from typing import List, Optional, Tuple
from dataclasses import dataclass

from etiket_client.sync.backends.labber.labber_ds.model import ChannelData, TraceData, LabberDataContent

class IsIrregularSetpointsError(Exception):
    pass

@dataclass
class Dimension:
    name : str
    unit : str
    values : np.ndarray

@dataclass
class DimSpec:
    dimensions : List[Dimension]
    is_grid : bool

    @property
    def shape(self) -> Tuple[int, ...]:
        if len(self.dimensions) == 0:
            return (0,)
        if self.is_grid:
            return tuple([d.values.size for d in self.dimensions])
        else:
            return (self.dimensions[0].values.size,)

def _netcdf_compatible_name(name: str) -> str:
    """Clear the name to be NetCDF-compatible by replacing problematic characters.
    
    Args:
        name: The original name that may contain spaces or other problematic characters
        
    Returns:
        A sanitized name safe for use as NetCDF coordinate/variable names
    """
    # Replace spaces and other problematic characters with underscores
    # Keep alphanumeric characters, underscores, and hyphens
    sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', name)
    
    # Ensure it doesn't start with a number (NetCDF requirement)
    if sanitized and sanitized[0].isdigit():
        sanitized = f"var_{sanitized}"
    
    # Remove multiple consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')
    
    return sanitized

def get_dimspec(channel_data : List[ChannelData], trace_data : Optional[TraceData] = None) -> DimSpec:
    try:
        if trace_data is not None:
            if trace_data.irregular_setpoints is True:
                raise IsIrregularSetpointsError
        
        if len(channel_data) > 0:
            data_size = channel_data[0].values.size
            dim_cache = np.zeros([len(channel_data), data_size])

            index = [0] * len(channel_data)
            max_size = [1] * len(channel_data)

            # set initial values:
            for ch_idx in range(len(channel_data)):
                dim_cache[ch_idx][0] = channel_data[ch_idx].values[0]

            # set init values:
            for idx in range(1, data_size):
                for ch_idx in range(len(channel_data)):
                    # if data has the same compared to the previous index, do nothing
                    if channel_data[ch_idx].values[idx] == channel_data[ch_idx].values[idx-1]:
                        continue

                    # reset all other channels
                    for idx_reset in range(ch_idx+1, len(channel_data)):
                        index[idx_reset] = -1

                    index[ch_idx] += 1

                    if index[ch_idx] == max_size[ch_idx]:
                        dim_cache[ch_idx][index[ch_idx]] = channel_data[ch_idx].values[idx]
                        max_size[ch_idx] += 1
                    else:
                        # check if the cache matches the value
                        if dim_cache[ch_idx][index[ch_idx]] != channel_data[ch_idx].values[idx]:
                            raise IsIrregularSetpointsError

        dimensions = []
        for ch_idx in range(len(channel_data)):
            data = dim_cache[ch_idx][:max_size[ch_idx]]
            dimensions.append(Dimension(channel_data[ch_idx].name,
                                        channel_data[ch_idx].unit,
                                        data))

        if trace_data is not None:
            dimension = Dimension(trace_data.setpoint_name,
                                    trace_data.setpoint_unit,
                                    trace_data.setpoint_values)
            dimensions.append(dimension)

        return DimSpec(dimensions, is_grid=True)
    except IsIrregularSetpointsError:
        dimensions = []
        if trace_data is None:
            for ch_idx in range(len(channel_data)):
                dimensions.append(Dimension(channel_data[ch_idx].name,
                                            channel_data[ch_idx].unit,
                                            channel_data[ch_idx].values))
            return DimSpec(dimensions, is_grid=False)
        else:
            # the trace data has now a second dimension, corresponding to the trace setpoint
            tace_data_size = trace_data.values.shape[1]
            for ch_idx in range(len(channel_data)):
                data = np.tile(channel_data[ch_idx].values, tace_data_size)
                dimensions.append(Dimension(channel_data[ch_idx].name,
                                            channel_data[ch_idx].unit,
                                            data.flatten()))
            
            if trace_data.irregular_setpoints is True:
                trace_data_dim = Dimension(trace_data.setpoint_name,
                                            trace_data.setpoint_unit,
                                            trace_data.setpoint_values.flatten())
            else:
                dimensions_size = 1
                if len(channel_data) > 1:
                    dimensions_size = channel_data[0].values.size
                values  = np.tile(trace_data.setpoint_values, dimensions_size)
                trace_data_dim = Dimension(trace_data.setpoint_name,
                                            trace_data.setpoint_unit,
                                            values.flatten())
            dimensions.append(trace_data_dim)

            return DimSpec(dimensions, is_grid=False)
    except Exception as e:
        raise e

def _get_unique_irreg_name(dim: Dimension, existing_coords: dict) -> str:
    """Generate a unique name for irregular coordinates by appending a number if needed.
    
    Args:
        dim: The dimension to use
        existing_coords: Dictionary of existing coordinates
        
    Returns:
        A unique name that doesn't conflict with existing coordinates
    """
    base_name = _netcdf_compatible_name(dim.name) + "_irreg"
    if base_name not in existing_coords:
        return base_name
    
    if np.array_equal(dim.values, existing_coords[f"{base_name}"][1]):
        return f"{base_name}"

    counter = 1
    while f"{base_name}_{counter}" in existing_coords:
        if np.array_equal(dim.values, existing_coords[f"{base_name}_{counter}"][1]):
            return f"{base_name}_{counter}"
        counter += 1
    return f"{base_name}_{counter}"

def to_xarray(dataset: LabberDataContent) -> xr.Dataset:
    data_meas_channels = [data_item for data_item in dataset.data if not data_item.is_setpoint]
    data_spt_channels = [data_item for data_item in dataset.data if data_item.is_setpoint]

    traces = dataset.traces

    reg_coords = {}
    irreg_coords = {}
    data_vars = {}
    
    for data_meas_channel in (data_meas_channels + list(traces.values())):
        if isinstance(data_meas_channel, TraceData):
            raw_data = data_meas_channel.values
            units = data_meas_channel.unit
            channel_name = data_meas_channel.name
            safe_channel_name = _netcdf_compatible_name(channel_name)
            dim_spec = get_dimspec(data_spt_channels, data_meas_channel)
        else:
            raw_data = data_meas_channel.values
            units = data_meas_channel.unit
            channel_name = data_meas_channel.name
            safe_channel_name = _netcdf_compatible_name(channel_name)
            dim_spec = get_dimspec(data_spt_channels)

        if dim_spec.is_grid:
            data_meas_channel_data = np.full(dim_spec.shape, np.nan, dtype=raw_data.dtype).flatten()
            data_meas_channel_data[:raw_data.size] = raw_data.flatten()
            data_meas_channel_data = data_meas_channel_data.reshape(dim_spec.shape)

            reg_coords = {}
            coords_name = []
            for dim in dim_spec.dimensions:
                safe_dim_name = _netcdf_compatible_name(dim.name)
                reg_coords[safe_dim_name] = ((safe_dim_name,), dim.values, {'units': dim.unit, 'long_name': dim.name})
                coords_name.append(safe_dim_name)

            data_vars[safe_channel_name] = (tuple(coords_name), data_meas_channel_data, {'units': units, 'long_name': channel_name})
        else:
            channel_coords_name = f"{safe_channel_name}_coords"
            for dim in dim_spec.dimensions:
                coord_name = _get_unique_irreg_name(dim, irreg_coords)
                if coord_name not in irreg_coords:
                    irreg_coords[coord_name] = ((channel_coords_name,), dim.values, {'units': dim.unit, 'long_name': dim.name})

            data_vars[safe_channel_name] = ((channel_coords_name,), raw_data.flatten(), {'units': units, 'long_name': channel_name})

    return xr.Dataset(data_vars, coords={**reg_coords, **irreg_coords})
