from etiket_client.remote.endpoints.models.types import FileType
from etiket_sync_agent.backends.core_tools.real_time_sync.core_tools_m_param import core_tools_m_param
from etiket_sync_agent.backends.core_tools.real_time_sync.qcodes_parameters import qcodes_m_param_emulator
from etiket_sync_agent.backends.core_tools.data_getters.get_gates import get_gates_formatted

from qdrive.measurement.data_collector import data_collector, from_QCoDeS_parameter
from qdrive.dataset.dataset import dataset

from qcodes.utils.helpers import NumpyJSONEncoder

try:
    from core_tools.data.SQL.SQL_connection_mgr import SQL_database_manager
    from core_tools.data.ds.data_set import load_by_id
except ImportError:
    pass

import logging, time, json, os

logger = logging.getLogger(__name__)

class live_measurement_synchronizer:
    def __init__(self, core_tools_id, datasetUUID) -> None:
        self.core_tools_ds = load_by_id(core_tools_id)
        self.native_ds = dataset(datasetUUID)
        
        self.conn = SQL_database_manager().conn_local
        cursor = self.conn.cursor()
        
        stmt = "SELECT * from measurement_parameters WHERE exp_uuid = %s ;"
        cursor.execute(stmt, vars=[self.core_tools_ds.exp_uuid])
        
        results = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        cursor.close()
        
        m_params = []
        for res in results:
            res_dict = {k:v for k,v in zip(col_names, res)}
            m_params += [core_tools_m_param(**res_dict)]
        
        generated_qcodes_parameters = []
        for m_param in m_params:
            if m_param.setpoint_local != True and m_param.nth_set == 0:
                p = qcodes_m_param_emulator(self.conn, m_param.param_id, m_params)
                generated_qcodes_parameters.append(p)
        
        self.sync_objects = []
        for param in generated_qcodes_parameters:
            if param.m_var == True:
                it = m_object_iterator(param, generated_qcodes_parameters)
                self.sync_objects.append(it)
        
        start_time = time.time()
        while not self.__ready():
            if time.time() - start_time > 3:  # 3 seconds have passed
                raise ValueError("Not all parameters are ready after 3 seconds")
            time.sleep(0.1)  # wait for 100 milliseconds before the next check
        self.dc = data_collector(self.native_ds, dtype=FileType.HDF5_CACHE)
        try:
            if self.core_tools_ds.snapshot:
                self.dc.set_attr('snapshot', json.dumps(self.core_tools_ds.snapshot, cls=NumpyJSONEncoder))
                gates = get_gates_formatted(self.core_tools_ds.snapshot)
                if gates:
                    self.dc.set_attr('gates', json.dumps(gates, cls=NumpyJSONEncoder))
            if self.core_tools_ds.metadata is not None:
                self.dc.set_attr('metadata', json.dumps(self.core_tools_ds.metadata, cls=NumpyJSONEncoder))
            
            for sync_obj in self.sync_objects:
                dep = [i.param for i in sync_obj.dependencies]
                self.dc += from_QCoDeS_parameter(sync_obj.m_param.param, dep[::-1] , self.dc)
            self.dc._enable_swmr()
        except Exception as e:
            if hasattr(self.dc, 'lock_file'):
                if self.dc.lock_file is not None:
                    if os.path.isfile(self.dc.lock_file):
                        os.remove(self.dc.lock_file)
            self.dc.complete()
            raise e
        self._last_write = time.time()
        self.__marked_complete = False
        
    def __ready(self):
        for sync_obj in self.sync_objects:
            if sync_obj.ready() == False:
                return False
        return True
        
    def sync(self):
        for it in self.sync_objects:
            write_happened = it.sync(self.dc)
            if write_happened:
                self._last_write = time.time()
    
    def complete(self):
        self.dc.complete()
        
    def is_complete(self):
        if time.time() - self._last_write > 30*60:
            logger.warning("Closing due to inactive measurement (hanging for 30 minutes).")
            raise ValueError("Timeout detected while reading data from the core-tools database.")
        
        stmt = "SELECT completed from global_measurement_overview WHERE uuid = %s ;"
        cursor = self.conn.cursor()
        cursor.execute(stmt, vars=[self.core_tools_ds.exp_uuid])
        ds_complete = cursor.fetchone()[0]
        cursor.close()
            
        if ds_complete:
            if not self.__marked_complete:
                self.__marked_complete = True
                return False
            return True
        return False
    
class m_object_iterator:
    def __init__(self, m_param, other_params):
        self.index = 0
        self.m_param = m_param
        self.dependencies = []
        for dependency in m_param.dependencies[::-1]:
            for param in other_params:
                if dependency == param.param_id:
                    self.dependencies.append(param)
        
    def sync(self, data_coll):
        iterations_available = []
        for p in [self.m_param] + self.dependencies:
            iterations_available.append(p.sync())

        current_index = min(iterations_available)
        if current_index > self.index:
            for i in range(self.index, current_index):
                input_data = {self.m_param.param : self.m_param.get(i)}
                for dep in self.dependencies:
                    input_data[dep.param] = dep.get(i)
                data_coll.add_data(input_data)
            self.index = current_index
            return True
        return False
    
    def ready(self):
        for p in [self.m_param] + self.dependencies:
            p.sync()
            if p.m_var_ready == False :
                return False
        return True
    
    def completed(self):
        if self.index == self.m_param.size:
            return True
        return False