import sqlite3, logging, time, io, os
import numpy as np

from qcodes import Parameter
from uuid import UUID

from etiket_client.remote.endpoints.models.types import FileType
from qdrive.measurement.data_collector import data_collector, from_QCoDeS_parameter
from qdrive.dataset.dataset import dataset

logger = logging.getLogger(__name__)

def QCoDeS_live_sync(run_id : int, database : str, datasetUUID : UUID):
    print("Starting real time sync")
    conn = sqlite3.connect(f'{database}')
    conn.execute('pragma journal_mode=wal')

    c = conn.cursor()
    c.execute("SELECT * FROM layouts where run_id = ?", (run_id,))
    layout = c.fetchall()
    
    parameters = {}
    parameters_by_name = {}
    for l in layout: 
        parameters[l[0]] = Parameter(l[2], label=l[3], unit=l[4])
        parameters_by_name[l[2]] = parameters[l[0]]
    
    dependency_ids= list(parameters.keys())
    c.execute(f"Select * from dependencies where dependent in ({','.join(['?']*len(dependency_ids))})",
                dependency_ids)
    dependencies = c.fetchall()
    
    relationships = {}
    for dep in dependencies:
        if dep[0] not in relationships.keys():
            relationships[dep[0]] = []
        relationships[dep[0]].append(dep[1])
    
    native_ds = dataset(datasetUUID)
    dc = data_collector(native_ds, FileType.HDF5_CACHE)
    
    try :
        for k, v in relationships.items():
            dep = [parameters[i] for i in v]
            dc += from_QCoDeS_parameter(parameters[k], dep, dc)

        sync_idx = 0
        res = c.execute("Select result_table_name, snapshot from runs where run_id = ?", (run_id,)).fetchone()
        table_name = res[0]
        snapshot_json = res[1]
        
        dc.set_attr('snapshot', snapshot_json)
        dc._enable_swmr() # manually set this, sometimes it takes long for qCoDeS to start writing data ... .
    except Exception as e:
        if (hasattr(dc, 'lock_file')):
            if (dc.lock_file is not None):
                if os.path.isfile(dc.lock_file):
                    os.remove(dc.lock_file)
        dc.complete()
        raise e
    
    t_last_fetch = time.time()
    
    try: 
        while not (check_complete(c, run_id) and sync_idx == getMaxId(c, table_name)):            
            c.execute(f"SELECT * FROM '{table_name}' WHERE id > ?", (sync_idx,))
            c_names = [description[0] for description in c.description]
            
            data = c.fetchall()
            if len(data) == 0:
                if t_last_fetch + 10*60 < time.time():
                    logger.warning("No new data found in the last ten minutes. Exiting real time sync.")
                    break
                time.sleep(0.1)
                continue
            else:
                t_last_fetch = time.time()
            
            for d in data:
                array_size = None
                data_to_write = {}
                for i, name in enumerate(c_names[1:]):
                    value = d[i+1]
                    if value is not None:
                        try: 
                            value = float(value)
                        except Exception as e:
                            # array parameters are stored as a blob :/
                            value = np.load(io.BytesIO(value))
                            if array_size is not None and array_size != value.size:
                                raise ValueError("Array size mismatch in the data") from e
                            array_size = value.size
                        data_to_write[parameters_by_name[name]] = value
                
                if array_size is None:
                    dc.add_data(data_to_write)
                else:
                    for i in range(array_size):
                        data_item_to_write = {}
                        for k, v in data_to_write.items():
                            if isinstance(v, np.ndarray):
                                data_item_to_write[k] = v[i]
                            else:
                                data_item_to_write[k] = v
                        dc.add_data(data_item_to_write)
            
            sync_idx = data[-1][0]
    except Exception as e:
        logger.exception("Real time sync failed with error :: %s", e)
    finally:
        dc.complete()
    
def check_complete(cursor : sqlite3.Cursor, run_id : int):
    cursor.execute("Select is_completed from runs where run_id = ?", (run_id,))
    return cursor.fetchone()[0]

def getMaxId(cursor : sqlite3.Cursor, table_name : str):
    cursor.execute(f"SELECT MAX(id) FROM '{table_name}'")
    return cursor.fetchone()[0]