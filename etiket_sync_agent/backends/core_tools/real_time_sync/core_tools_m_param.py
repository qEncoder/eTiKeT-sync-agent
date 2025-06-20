import pydantic, typing

class core_tools_m_param(pydantic.BaseModel):
    param_index :int 
    param_id : int
    nth_set : int
    nth_dim : int
    param_id_m_param : int
    setpoint : bool
    setpoint_local : bool
    name_gobal : str 
    name : str
    label : str
    unit : str
    depencies : typing.List[int]
    shape : typing.List[int]
    write_cursor : int
    total_size : int
    oid : int
    
    @property
    def dependencies(self):
        return self.depencies