# adapted from qt_dataviewer/plot_ui/pulses_widget.py
import json, logging

import xarray as xr
import numpy as np

from typing import Dict, List
from qcodes.utils.helpers import NumpyJSONEncoder

logger = logging.getLogger(__name__)

def get_AWG_pulses(snapshot : Dict) -> xr.Dataset:
    try:
        pulses = snapshot['measurement']['sequence']
    except KeyError:
        return None
    
    pulse_data = _get_pulse_data(pulses)
    
    if pulse_data:
        data_vars = {data['name']: ([f'time_{data["name"]}'], data['y'], {'units': 'mV'})
                        for data in pulse_data}
        coords = {f'time_{data["name"]}': data['x'] for data in pulse_data}

        ds = xr.Dataset(data_vars, coords)
        ds.attrs = {'pulses': json.dumps(snapshot['measurement'], cls=NumpyJSONEncoder)}
        
        for data in pulse_data:
            ds.coords[f'time_{data["name"]}'].attrs = {'units' : "ns", 'long_name' : "Time"}

        return ds
    
    return None

def _get_pulse_data(pulses : Dict) -> 'List[Dict] | None':
    pulse_data_plot = []
    
    try:
        pc_keys = [k for k, v in pulses.items() if k.startswith('pc') and v is not None]
        gate_keys = sorted(set([key for pc in pc_keys for key in pulses[pc] if not key.startswith('_')]))
        
        end_times = {}
        for pc in pc_keys:
            if 'branch_0' in pulses[pc]:
                seg = pulses[pc]['branch_0']
            else:
                seg = pulses[pc]
            try:
                end_time = seg['_total_time']
                while isinstance(end_time, list):
                    end_time = end_time[-1]
            except Exception:
                end_time = max([x['stop'] for y in seg.values() for x in y.values()])
            end_times[pc] = end_time
        
        try:
            lo_freqs = pulses['LOs']
        except KeyError:
            pass
        
        # TODO handle acquisition channels
        for (j, name) in enumerate(gate_keys):
            if name.endswith('_baseband'):
                old_format = False
                for pc in pc_keys:
                    if name in pc:
                        old_format = 'index_start' in pulses[pc][name]['p0']
                        break
                if old_format:
                    pulse_data_plot.append(_get_baseband_old(pulses, pc_keys, end_times, name,))
                else:
                    pulse_data_plot.append(_get_baseband(pulses, pc_keys, end_times, name,))
            elif name.endswith('_pulses'):
                try:
                    lo_frequency = lo_freqs[name[:-7]]
                except Exception:
                    logger.warning('No baseband frequency found, assuming 0')
                    lo_frequency = 0
                pulse_data_plot.append(_get_mw_pulses(pulses, pc_keys, end_times, name, lo_frequency))
        
    except Exception:
        logger.exception("Error in generating core-tools pulses")
        
    return pulse_data_plot
        
def _get_baseband_old(pulses, pc_keys, end_times, name):
    t0 = 0
    x_plot = list()
    y_plot = list()
    for pc in pc_keys:
        end_time = end_times[pc]

        try:
            seg_pulses = pulses[pc][name]
        except Exception:
            t0 += end_time
            continue

        timepoints = set([x[key] for x in seg_pulses.values() for key in ['start','stop']])
        timepoints.add(end_time)
        for tp in sorted(timepoints):
            point1 = 0
            point2 = 0
            for seg_name, seg_dict in seg_pulses.items():
                if seg_dict['start'] < tp and seg_dict['stop'] > tp: # active segement
                    offset = tp/(seg_dict['stop'] - seg_dict['start']) * (seg_dict['v_stop'] - seg_dict['v_start']) + seg_dict['v_start']
                    point1 += offset
                    point2 += offset
                elif seg_dict['start'] == tp:
                    point2 += seg_dict['v_start']
                elif seg_dict['stop'] == tp:
                    point1 += seg_dict['v_stop']
            x_plot += [tp + t0, tp + t0]
            y_plot += [point1, point2]
        t0 += end_time

    legend_name = name[:-9]
    return {'x': x_plot, 'y': y_plot, 'name': legend_name}

def _get_baseband(pulses, pc_keys, end_times, name):
    t0 = 0
    x_plot = [0.0]
    y_plot = [0.0]
    t = 0.0
    v = 0.0
    for pc in pc_keys:
        end_time = end_times[pc]

        if 'branch_0' in pulses[pc]:
            seg = pulses[pc]['branch_0']
        else:
            seg = pulses[pc]

        try:
            seg_pulses = seg[name]
        except Exception:
            t0 += end_time
            continue

        for pulse in seg_pulses.values():
            start = pulse['start'] + t0
            stop = pulse['stop'] + t0
            v_start = pulse['v_start']
            v_stop = pulse['v_stop']
            if start != t:
                # there is a gap. Add point at end of last pulse
                x_plot.append(t)
                y_plot.append(0.0)
                v = 0.0
                if v_start != v:
                    # there is a step
                    x_plot.append(start)
                    y_plot.append(v)
                x_plot.append(start)
                y_plot.append(v_start)
            elif v_start != v:
                # there is a step
                x_plot.append(start)
                y_plot.append(v_start)
            x_plot.append(stop)
            y_plot.append(v_stop)
            t = stop
            v = v_stop
        t0 += end_time

    if t != t0:
        # there is a gap. Add line till end.
        x_plot.append(t)
        y_plot.append(0.0)
        x_plot.append(t0)
        y_plot.append(0.0)

    legend_name = name[:-9]
    return {'x': x_plot, 'y': y_plot, 'name': legend_name}

def _get_mw_pulses(pulses, pc_keys, end_times, name, lo_frequency):
    t0 = 0
    x_plot = list()
    y_plot = list()
    for pc in pc_keys:
        end_time = end_times[pc]

        if 'branch_0' in pulses[pc]:
            seg = pulses[pc]['branch_0']
        else:
            seg = pulses[pc]

        try:
            seg_pulses = seg[name]
        except Exception:
            t0 += end_time
            continue

        x = []
        y = []
        for seg_name, seg_dict in seg_pulses.items():
            x_ar = np.arange(seg_dict['start'], seg_dict['stop'])
            xx_ar = x_ar-seg_dict['start']
            f_rl = (seg_dict['frequency'] - lo_frequency)/1e9
            y_ar = np.sin(2*np.pi*f_rl*xx_ar+seg_dict['start_phase'])*seg_dict['amplitude']
            x = x + [seg_dict['start']] + list(x_ar) + [seg_dict['stop']]
            y = y + [0] + list(y_ar) + [0]
            x_plot = x
            y_plot = y
        t0 += end_time

    legend_name = name[:-7]
    return {'x': x_plot, 'y': y_plot, 'name': legend_name}