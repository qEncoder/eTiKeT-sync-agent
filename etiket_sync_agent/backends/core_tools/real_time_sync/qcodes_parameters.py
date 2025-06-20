from etiket_sync_agent.backends.core_tools.real_time_sync.core_tools_m_param import core_tools_m_param
from qcodes.parameters import Parameter, MultiParameter

try: 
    from core_tools.data.SQL.buffer_writer import buffer_reader
except ImportError:
    pass

from typing import List
import numpy as np

class qcodes_m_param_emulator:
    def __init__(self, SQL_conn, param_id : int, param_info : List[core_tools_m_param]) -> None:
        self.param_info = param_info
        self.buffers = []
        self.setpoint_buffers = []
        self.__setpoint_local_sizes = []
        self.__setpoint_local_shapes = []
        self.param = None
        self.m_var = True
        self.m_var_ready = True
        self.expected_size = 0
        self.__current_cursor = -1
        self.__npt_per_meas = []
        
        m_param = get_param_by_id(param_id, param_info)
        self.param_id = m_param.param_id
        self.dependencies = []
        
        for dep in m_param.depencies:
            param = get_param_by_id(dep, param_info)
            if param.setpoint == True:
                self.dependencies.append(dep)
        
        if m_param.setpoint == True:
            self.m_var = False
            self.param = Parameter(m_param.name, label=m_param.label, unit=m_param.unit)
            self.nsets = 1
            
            buffer = buffer_reader(SQL_conn, m_param.oid, [np.prod(m_param.shape)])
            self.buffers += [buffer]
            
            self.shape = m_param.shape
            self.__npt_per_meas = [1]
        else:
            if not is_multiparameter(param_id, param_info):
                self.param = Parameter(m_param.name, label=m_param.label, unit=m_param.unit)
                self.nsets = 1
                self.buffers += [buffer_reader(SQL_conn, m_param.oid, [np.prod(m_param.shape)])]
                self.shape = m_param.shape
                self.__npt_per_meas = [1]
            else:
                self.m_var_ready = False
                m_params = get_multi_parameters(param_id, param_info)
                
                names = []
                labels = []
                units = []
                shapes = []

                setpoint_names = []
                setpoint_units = []
                setpoint_labels = []
                                
                for i, m_param in enumerate(m_params):
                    names += [m_param.name]
                    labels += [m_param.label]
                    units += [m_param.unit]
                                        
                    
                    self.setpoint_buffers.append([])
                    self.__setpoint_local_sizes.append([])
                    self.__setpoint_local_shapes.append([])

                    shape = []
                    setpoint_name_single = []
                    setpoint_units_single = []
                    setpoint_labels_single = []
                    
                    for setpt in get_setpoints_local(m_param.dependencies, param_info):
                        setpoint_name_single += [setpt.name]
                        setpoint_labels_single += [setpt.label]
                        setpoint_units_single += [setpt.unit]
                        
                        shape.append((setpt.shape[0]), )
                        
                        self.setpoint_buffers[i].append(buffer_reader(SQL_conn, setpt.oid, setpt.shape))
                        self.__setpoint_local_sizes[i].append(np.prod(setpt.shape))
                        self.__setpoint_local_shapes[i].append(tuple(setpt.shape))
                    
                    if len(shape) == 0:
                        buffer_shape = [np.prod(m_param.shape)]
                    else:
                        buffer_shape = [int(np.prod(m_param.shape)/np.prod(shape))] + shape
                    
                    self.buffers.append(buffer_reader(SQL_conn, m_param.oid, buffer_shape))
                    self.__npt_per_meas += [np.prod(shape)]
                    
                    setpoint_names.append(tuple(setpoint_name_single))
                    setpoint_units.append(tuple(setpoint_units_single))
                    setpoint_labels.append(tuple(setpoint_labels_single))
                    
                    if not shape:
                        shape = ()

                    shapes.append(tuple(shape))
                
                self.param = GenericMuliparameter(name = m_params[0].name_gobal, names=names,
                                            shapes=tuple(shapes), labels=labels,units=units,
                                            setpoint_names=setpoint_names,
                                            setpoint_units=setpoint_units,
                                            setpoint_labels=setpoint_labels)
    @property
    def size(self):
        return self.buffers[0].data.shape[0]
    
    def get(self, index : int):
        out = [i.data[index] for i in self.buffers]
        if isinstance(self.param, Parameter):
            return out[0]
        return out
    
    def sync(self):
        current_cursors = []
        for i, buffer in enumerate(self.buffers):
            buffer.sync()
            current_cursors.append(int(buffer.cursor / self.__npt_per_meas[i]))
        self.__current_cursor = min(current_cursors)
            
        if self.m_var_ready is False:
            for buffers_setpt in self.setpoint_buffers:
                for buffer_setpt in buffers_setpt:
                    buffer_setpt.sync()
            setpt_local_cursors = [[int(buffer.cursor) for buffer in buffer_list]
                                        for buffer_list in self.setpoint_buffers]
            
            if setpt_local_cursors == self.__setpoint_local_sizes:
                self.param.setpoints = tuple(tuple(buffer.buffer for buffer in buffer_list)
                                                    for buffer_list in self.setpoint_buffers)
                self.m_var_ready= True
        
        if self.m_var_ready is False:
            return -1
        return self.__current_cursor
    
def is_multiparameter(param_id : int, parameter_list : List[core_tools_m_param]):
    n_sets = 0
    local_param = False
    
    for param in parameter_list:
        if param.param_id == param_id:
            n_sets += 1
        if param.setpoint_local is True:
            local_param = True
    
    if n_sets>1 or local_param is True:
        return True
    return False

def get_multi_parameters(param_id : int, parameter_list : List[core_tools_m_param]) -> List[core_tools_m_param]:
    params = []
    for param in parameter_list:
        if param.param_id == param_id:
            params += [param]
    
    params.sort(key=lambda x : x.nth_set)
    return params

def get_setpoints_local(setpoints : List[int], parameter_list : List[core_tools_m_param]):
    params = []
    for param in parameter_list:
        if param.param_id in setpoints and param.setpoint_local is True:
            params += [param]
    params.sort(key=lambda x : x.nth_dim)
    return params

def get_param_by_id(param_id : int, parameter_list : List[core_tools_m_param]) -> core_tools_m_param:
    for param in parameter_list:
        if param.param_id == param_id:
            return param
    raise ValueError("Expected parameter does not exist.")


class GenericMuliparameter(MultiParameter):
    def __init__(self, name, names,shapes, labels,units,setpoint_names, setpoint_units, setpoint_labels):
        if not setpoint_names:
            setpoint_names = None
            setpoint_units = None
            setpoint_labels = None
        else:
            setpoint_names = tuple(setpoint_names)
            setpoint_units = tuple(setpoint_units)
            setpoint_labels = tuple(setpoint_labels)

        super().__init__(name=name, names=names,shapes=shapes, labels=labels,units=units,
                         setpoint_names=setpoint_names, setpoint_units=setpoint_units,setpoint_labels=setpoint_labels)

    def get_raw(self):
        return None
    
    def set_raw(self, value):
        raise NotImplementedError