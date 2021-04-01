import logging
import json
from datetime import datetime, timedelta
import pickle
import os
import gzip
import math
from PIL import Image
import numpy as np
from lab_api_client.config import JsonConfig
import matplotlib
client_config = JsonConfig()
#from matplotlib.cbook import get_sample_data
#from matplotlib.offsetbox import (TextArea, DrawingArea, OffsetImage, AnnotationBbox)
# important for all servers
#if 'earconnect.de' in client_config.server:
#    matplotlib.use('Agg')
# for interactive plotting mode del the line
#matplotlib.use('Agg')
import matplotlib.pyplot as plt
import copy
import sys
from scipy import signal

# Cosinuss modules
from data_container.rawdata.rawdata_dict import rawdata_dict, identifier_sorted, id2identifier

sys.path.insert(0, '../algorithm_prototyping')
try:
    from algorithm import calc_rm_spikes, calc_spo2
except ImportError:
    print('no algorithm imported')
    pass

# rawdata_version
#   0 -> old
#   1 -> new rawdata
#   2 -> add_events
#   3 -> ???
#   4 -> update all rd, add changelog of rd_version, add available cols, ...
#   5 -> implemented handling for af e.g. add_all methods, file_type variable
current_rawdata_version = 5

def color_inverted(color_str):
    
    r = hex(255 - int(color_str[1:3], 16))[2:4].zfill(2)
    g = hex(255 - int(color_str[3:5], 16))[2:4].zfill(2)
    b = hex(255 - int(color_str[5:7], 16))[2:4].zfill(2)
    
    color = '#' + r + g + b
    
    return color

def color_change(color_str, scale=0.75):
    
    r = int(scale * int(color_str[1:3], 16))
    g = int(scale * int(color_str[3:5], 16))
    b = int(scale * int(color_str[5:7], 16))
    
    color = '#'
    for c in [r, g, b]:
        color += hex(c)[2:4].zfill(2)
    
#    color = '#'
#    for c in [r, g, b]:
#        if c + diff > 255 or c + diff < 0:
#            c = c - diff
#        else:
#            c = c + diff
#        color += hex(c)[2:4].zfill(2)
    
    return color

# for some old pickle data, to be able to load them
class RawDataEvent():
    pass

class PathError(Exception):
    pass

class AnalyseError(Exception):
    pass

def calc_quality_level(quality_list, mode='finished'):
    
    quality_red = 0
    quality_yellow = 0
    quality_green = 0
    
    for i in range(len(quality_list)):
        if i < 5:
            continue
        max_q = max(quality_list[i-5:i])
        if max_q < 25:
            quality_red = quality_red + 1
        elif max_q < 45:
            quality_yellow = quality_yellow + 1
        else:
            quality_green = quality_green + 1
    try:
        q_green = float(quality_green) / float(quality_red + quality_yellow + quality_green)
        q_yellow = float(quality_yellow) / float(quality_red + quality_yellow + quality_green)
        q_red = float(quality_red) / float(quality_red + quality_yellow + quality_green)
    except ZeroDivisionError:
        q_green = None
        q_yellow = None
        q_red = None
    
    if mode == 'live':
        if q_green and q_green > 0.5:
            q_green = 1
            q_yellow = 0
            q_red = 0
        elif q_red and q_red > 0.5:
            q_green = 0
            q_yellow = 0
            q_red = 1
        else:
            q_green = 0
            q_yellow = 1
            q_red = 0
    
    return (q_green, q_yellow, q_red)

# ToDo: rawdata
#   - reserved key for col_names!!!

class RawDataCol():
    
    def __init__(self, identifier, path, source='received', check_sum=False):
        
        # WARNING:
        #       because of __getattr__() you have to take care about name conflicts
        #       e.g. do not use self.meta['values'], it will not be acessible with self.values because this attribute already exists
        #       ToDo: check this
        # see: http://stackoverflow.com/questions/16237659/python-how-to-implement-getattr
        
        if not path:
            raise PathError('please define a valid path')
        
        try:
            source = rawdata_dict[identifier]['source']
        except KeyError:
            pass
        
        if rawdata_dict[identifier]['color']:
            color_set = rawdata_dict[identifier]['color']
        else:
            color_set = '#AAAAAA'
        
        try:
            volatile_flag = rawdata_dict[identifier]['volatile']
        except KeyError:
            volatile_flag = False
        
        # data
        super().__setattr__('_data', {
                'data': {
                            'time_calc':  [], 
                            'time_rec':  [], 
                            'values':  [], 
                            'meta': {
                                            'identifier': identifier, 
                                            'source': source, 
                                            'min': None, 
                                            'max': None, 
                                            'sampling_rate_custom': None
                                            }
                                }
                })

        # for orm.DevicePcb() we have the check sums from the serial connection
        if check_sum:
            self._data['data']['check_sum'] = []

        # volatile data
        super().__setattr__('_volatile', {
                'status': None, 
                'path': str(path) + self.identifier + '.pkl.gz',
                'logger': logging.getLogger(__name__), 
                'id': rawdata_dict[identifier]['id'], 
                'name': rawdata_dict[identifier]['name'], 
                'name_short': identifier.replace('_', ' '), 
                'cast': rawdata_dict[identifier]['cast'], 
                'sampling_rate': rawdata_dict[identifier]['sampling_rate'], 
                'unit': rawdata_dict[identifier]['unit'], 
                'ble_service': rawdata_dict[identifier]['ble_service'], 
                'desc': rawdata_dict[identifier]['desc'], 
                'order': rawdata_dict[identifier]['order'], 
                'color': color_set, 
                'volatile': volatile_flag, 
                'data_volatile': {}
                })
    
    def __getattr__(self, name):
        
        if name == 'data':
            return self._data['data']
        elif name in list(self._data['data']):
            if self.status == None and not self.volatile:
                self.load()
            return self._data['data'][name]
        elif name in list(self._data['data']['meta']):
            return self._data['data']['meta'][name]
        elif name in list(self._volatile):
            return self._volatile[name]
        elif name in list(self._volatile['data_volatile']):
            return self._volatile['data_volatile'][name]
        else:
            self.logger.warning('object has no attribute ' + str(name))
            raise AttributeError('object has no attribute ' + str(name))
    
    def __setattr__(self, name, value):
        
        if name == 'data':
            self._data['data'] = value
        elif name in list(self._data['data']):
            self._data['data'][name] = value
        elif name in list(self._data['data']['meta']):
            self._data['data']['meta'][name] = value
        elif name in list(self._volatile):
            self._volatile[name] = value
        else:
            self.logger.warning('object has no attribute ' + str(name))
            raise AttributeError('object has no attribute ' + str(name))
    
    def __iter__(self):
        
        for i in range(len(self.time_calc)):
            x = self.time_calc[i]
            y = self.values[i]
            yield x, y
    
    def __repr__(self):
        
        if len(self.time_calc) < 15:
            time_calc_str = str(self.time_calc)
        else:
            time_calc_str = str(self.time_calc[:5])[:-1] + ', ..., ' + str(self.time_calc[-5:])[1:]
        if len(self.time_rec) < 15:
            time_rec_str = str(self.time_rec)
        else:
            time_rec_str = str(self.time_rec[:5])[:-1] + ', ..., ' + str(self.time_rec[-5:])[1:]
        if len(self.values) < 15:
            values_str = str(self.values)
        else:
            values_str = str(self.values[:5])[:-1] + ', ..., ' + str(self.values[-5:])[1:]
        
        repr_infos = {}
        repr_infos['meta'] = self._data['data']['meta']
        repr_infos[self.identifier] = {
                                                'time_calc[' + str(len(self.time_calc)) + ']': time_calc_str, 
                                                'time_rec[' + str(len(self.time_rec)) + ']': time_rec_str, 
                                                'values[' + str(len(self.values)) + ']': values_str, 
                                                }
        
        return_json = json.dumps(repr_infos)
        return(return_json)
    
    def dumps(self):
        
        if len(self.time_calc) < 15:
            time_calc_str = str(self.time_calc)
        else:
            time_calc_str = str(self.time_calc[:5])[:-1] + ', ..., ' + str(self.time_calc[-5:])[1:]
        if len(self.time_rec) < 15:
            time_rec_str = str(self.time_rec)
        else:
            time_rec_str = str(self.time_rec[:5])[:-1] + ', ..., ' + str(self.time_rec[-5:])[1:]
        if len(self.values) < 15:
            values_str = str(self.values)
        else:
            values_str = str(self.values[:5])[:-1] + ', ..., ' + str(self.values[-5:])[1:]
        if 'check_sum' in self.data and self.check_sum:
            len_check_sum = len(self.check_sum)
            if len(self.check_sum) < 15:
                check_sum_str = str(self.check_sum)
            else:
                check_sum_str = str(self.check_sum[:5])[:-1] + ', ..., ' + str(self.check_sum[-5:])[1:]
        else:
            check_sum_str = 'None'
            len_check_sum = 0
        try:
            sampling_rate_custom = self.sampling_rate_custom
        except AttributeError:
            sampling_rate_custom = None
        try:
            source = self.source
        except AttributeError:
            source = None
        
        repr_infos = {
                            'time_calc[' + str(len(self.time_calc)) + ']': time_calc_str, 
                            'time_rec[' + str(len(self.time_rec)) + ']': time_rec_str, 
                            'values[' + str(len(self.values)) + ']': values_str,
                            'min': self.min, 
                            'max': self.max, 
                            'source': source, 
                            'sampling_rate_custom': sampling_rate_custom, 
                            'volatile': self.volatile, 
                            }

        if 'check_sum' in self.data and self.check_sum:

            repr_infos['check_sum[' + str(len_check_sum) + ']'] = check_sum_str

        return(repr_infos)
    
#    def __repr__(self):
#        
#        return self.identifier
    
    @property
    def x(self):
        
        if len(self.time_calc) > len(self.values):
            
            self.logger.error('x and y have different length: ' + str(len(self.time_calc)) + ' != ' + str(len(self.values)))
            return self.time_calc[:len(self.values)]
        
        else:
            
            return self.time_calc
    
    @property
    def rec(self):
        
        return self.time_rec
    
    @property
    def y(self):
        
        if len(self.values) > len(self.time_calc):
            
            self.logger.error('x and y have different length: ' + str(len(self.time_calc)) + ' != ' + str(len(self.values)))
            return self.values[:len(self.time_calc)]
        
        else:
            
            return self.values
    
    def __moving_average(self, data):
        
        average_count = 15
        data_return = []
        for i in range(len(data)):
            
            if average_count > i:
                av = i
            else:
                av = average_count
            data_return.append(sum(data[i-av:i+1])/float(av+1))
        
        return data_return
    
    def __moving_max(self, data):
        
        max_count = 15
        data_return = []
        for i in range(len(data)):
            
            if max_count > i:
                mc = i
            else:
                mc = max_count
            data_return.append(max(data[i-mc:i+1]))
        
        return data_return
    
    @property
    def y_smoothed(self):
        
        if not '_y_smoothed' in self.data_volatile:
            #self.logger.debug('_y_smoothed not in data_volatile, new calculation')
            self.data_volatile['_y_smoothed'] = self.__moving_average(self.y)
        
        return self._y_smoothed
    
    def y_smoothed_del(self):
        
        del(self._volatile['data_volatile']['_y_smoothed'])
    
    @property
    def y_moving_max(self):
        
        if not '_y_moving_max' in self.data_volatile:
            #self.logger.debug('_y_moving_max not in data_volatile, new calculation')
            self.data_volatile['_y_moving_max'] = self.__moving_max(self.y)
        
        return self._y_moving_max
    
    def y_moving_max_del(self):
        
        del(self._volatile['data_volatile']['_y_moving_max'])
    
    @property
    def rm_spikes_filled(self):
        
        return self._calc_rm_spikes(filled=True)
    
    def x_timestamps(self, timestamp_start):
        
        if not '_x_timestamps' in self.data_volatile:
            #self.logger.debug('_x_timestamps not in data_volatile, new calculation')
            self.data_volatile['_x_timestamps'] = []
            for x in self.x:
                self.data_volatile['_x_timestamps'].append(timestamp_start + timedelta(seconds=x))
        
        return self._x_timestamps
    
    def x_timestamps_del(self):
        
        del(self._volatile['data_volatile']['_x_timestamps'])
    
    @property
    def x_minutes(self):
        
        if not '_x_minutes' in self.data_volatile:
            #self.logger.debug('_x_minutes not in data_volatile, new calculation')
            self.data_volatile['_x_minutes'] = []
            for x in self.x:
                self.data_volatile['_x_minutes'].append(x/60.0)
        
        return self._x_minutes
    
    # does not cache the calc x_minutes list like x_minutes()
    def x_minutes_onthefly(self):
        
        x_minutes = []
        for x in self.x:
            x_minutes.append(x/60.0)
        
        return x_minutes
    
    # does not cache the calc x_minutes list like x_minutes()
    def time_rec_minutes_onthefly(self):
        
        time_rec_minutes = []
        for time_rec in self.time_rec:
            time_rec_minutes.append(time_rec/60.0)
        
        return time_rec_minutes
    
    def x_minutes_del(self):
        
        del(self._volatile['data_volatile']['_x_minutes'])
    
    @property
    def x_hours(self):
        
        if not '_x_hours' in self.data_volatile:
            #self.logger.debug('_x_hours not in data_volatile, new calculation')
            self.data_volatile['_x_hours'] = []
            for x in self.x:
                self.data_volatile['_x_hours'].append(x/3600.0)
        
        return self._x_hours
    
    def x_hours_del(self):
        
        del(self._volatile['data_volatile']['_x_hours'])
    
    @property
    def rm_spikes_del(self):
        
        del(self._volatile['data_volatile']['_rm_spikes'])
    
    @property
    def rm_spikes(self):
        
        return self._calc_rm_spikes()
    
    def _calc_rm_spikes(self, filled=False, algo='rm_spikes_2'):
        
        if not self.identifier.startswith('ppg'):
            self.logger.warning('for this function you need ppg data')
            return False
        
        if len(self.values) != len(self.time_calc):
            self.logger.warning('len(self.values) != len(self.time_calc): ' + str(len(self.values)) + ' != ' + str(len(self.time_calc)))
            self.data_volatile['_rm_spikes'] = {'y': [], 'x': []}
        
        if not '_rm_spikes' in self.data_volatile:
            self.logger.debug('_rm_spikes not in data_volatile, new calculation')
            try:
                if algo == 'rm_spikes_3':
                    v = calc_rm_spikes.rm_spikes_3(self.values, self.time_calc, self.time_rec, filled=filled)
                else:
                    v = calc_rm_spikes.rm_spikes_2(self.values, self.time_calc, self.time_rec, filled=filled)
                self.logger.info('detected spikes per minute: ' + str(v.spikes_per_minute))
                self.data_volatile['_rm_spikes'] = {'y': v.pulse_raw_rm_spikes, 'x': v.pulse_raw_rm_spikes_time_calc}
            except NameError:
                self.logger.error('no algorithm imported: undefined calc_rm_spikes()')
                return False
        
        return self._rm_spikes
    
    @property
    def filter_reset(self):
        
        self.data_volatile['_filtered'] = copy.copy(self.values)
    
    @property
    def filter_h(self):
        
        return self._filter('h')
    
    @property
    def filter_l(self):
        
        return self._filter('l')
    
    @property
    def filter_l2(self):
        
        return self._filter('l2')
    
    @property
    def filter_hhl(self):
        
        self._filter('h')
        self._filter('h')
        return self._filter('l')
    
    def _filter(self, type):
        
        # h_b, h_a = signal.iirfilter(order, wp, rp=apass, rs=astop, btype=filtertype, analog=False, ftype=design_method)
        param = {
                        'h': signal.iirfilter(5.0, 0.004, rp=1.0, rs=80.0, btype='highpass', analog=False, ftype='cheby2'),                                  # highpass filter
                        'l': signal.iirfilter(12.0, 0.19920000000000002, rp=1.0, rs=40.0, btype='lowpass', analog=False, ftype='cheby2'),      # lowpass
                        'l2': signal.iirfilter(18.0, 0.19920000000000002, rp=1.0, rs=80.0, btype='lowpass', analog=False, ftype='cheby2')     # lowpass version 2 (steiler), not yet ready
                        }
        
        if not '_filtered' in self.data_volatile:
            if '_rm_spikes' in self.data_volatile:
                self.logger.info('filter values, after rm_spikes')
                self.data_volatile['_filtered'] = copy.copy(self.data_volatile['_rm_spikes'])
            else:
                self.logger.info('filter raw values')
                self.data_volatile['_filtered'] = copy.copy(self.values)
        self.data_volatile['_filtered'] = signal.lfilter(param[type][0], param[type][1], self.data_volatile['_filtered'])
        
        return self.data_volatile['_filtered']
    
    def cast_value(self, value):
        
        if self.cast == 'int':
            return_value = int(value)
        elif self.cast == 'float':
            return_value = float(value)
        elif self.cast == 'timestamp':
            return_value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
        
        if return_value == value or self.cast == 'timestamp':
            return return_value
        else:
            self.logger.error('cast error: The type of the col ' + self.identifier + ' is: ' + str(self.cast) + ', value: ' + str(value))
            raise ValueError()
    
    def add_value(self, time_calc, time_rec, value, check_sum):
        
        try:
            self.values.append(self.cast_value(value))
            self.time_calc.append(time_calc)
            self.time_rec.append(time_rec)
            # ToDo: optimize, just store check_sum errors and leave out the 99% OK sums...
            if not check_sum is None:
                self.check_sum.append(check_sum)
            self.status = 'added'
        except ValueError:
            return False
        
    def add_all(self, time_calc, time_rec, value_list, check_sum, sort=False):
        try:
            if self.values:
                self.time_calc = self.time_calc + time_calc
                self.time_rec = self.time_rec + time_rec
                self.values = self.values + value_list
            else:
                self.values = value_list
                self.time_calc = time_calc
                self.time_rec = time_rec
                
            if sort == True:
                self.values = [values for _,values in sorted(zip(self.time_calc, self.values))]
                self.time_calc = list(np.sort(self.time_calc))
                self.time_rec = list(np.sort(self.time_rec))
            # ToDo: optimize, just store check_sum errors and leave out the 99% OK sums...
            if not check_sum is None:
                self.check_sum.append(check_sum)
            self.status = 'added'
        except ValueError:
            return False
    
    def save(self):
        
        f = gzip.open(self.path,'wb')
        pickle.dump(self.data, f, protocol=3)
        f.close()
    
    def load(self):
        
        if self.volatile:
            
            return False
        
        pickle_data = False
        count_atemps = 10
        
        # load full raw data
        try:
            f = gzip.open(self.path, 'rb')
            while(pickle_data is False and count_atemps > 0):
                try:
                    pickle_data = pickle.load(f)
                except:
                    pickle_data = False
                    count_atemps = count_atemps - 1
            f.close()
        except FileNotFoundError:
            self.logger.debug('lazy load ' + self.identifier + ' -> no data file')
            self.status = 'nofile'
            return False
        
        if pickle_data is False:
            self.logger.error('not able to load pickle data (zlib.error?), col_name: ' + self.identifier)
            return False
        
        # START: load old rawdata rd_version == 0
        try:
            test = pickle_data['source']
            self.load_v00(pickle_data)
            return True
        except KeyError:
            pass
        # END: load old rawdata rd_version == 0
        
        self.data = pickle_data

        # status
        self.status = 'loaded'
        self.logger.debug('lazy load ' + self.identifier)
    
    def load_v00(self, pickle_data):
        
        if pickle_data is False:
            self.logger.error('not able to load pickle data')
            return False
        
        self.source = pickle_data['source']
        
        # data
        self.time_calc = pickle_data['time']
        self.time_rec = pickle_data['time_rec']
        self.values = pickle_data['values']
        
        # status
        self.status = 'loaded'
        self.logger.debug('lazy load_v00 ' + self.identifier)

class RawData():
    
    def __init__(self, check_sum=False):
        
            
        # WARNING:
        #       because of __getattr__() you have to take care about name conflicts
        #       e.g. do not use self._meta['current_time_calc'], it will not be acessible with self.current_time_calc because this attribute already exists
        #       same issue for col names: see rawdata_dict.py!
        #       ToDo: check this -> write a function to check name conflicts?
        # see: http://stackoverflow.com/questions/16237659/python-how-to-implement-getattr
        
        # data
        super().__setattr__('_data', {
                'data': {
                            'meta': {
                                            'rd_version': current_rawdata_version, 
                                            'db': {
                                                        'server': None, 
                                                        'server_version': None, 
                                                        'ds_id': None, 
                                                        'df_id': None, 
                                                        'id_padded': None, 
                                                        'df_comment': None, 
                                                        'date_time_start': None, 
                                                        'date_time_end': None, 
                                                        'datetime_import': None, 
                                                        'phone_id': None, 
                                                        'phone_imei': None, 
                                                        'device_id': None, 
                                                        'device_address': None,
                                                        'device_identifier': None,
                                                        'device_serial_number': None,
                                                        'device_type': None,
                                                        'device_pcb_type': None,
                                                        'device_label': None, 
                                                        'device_model_number': None, 
                                                        'device_software_revision': None, 
                                                        'device_system_id': None, 
                                                        'person_id': None, 
                                                        'person_label': None, 
                                                        'project_id': None, 
                                                        'project_identifier': None, 
                                                        'project_name': None
                                                        }, 
                                            'rd': {
                                                        'duration': None, 
                                                        'duration_str': None, 
                                                        'transfer_rate_all': None, 
                                                        'quality_red': None, 
                                                        'quality_yellow': None, 
                                                        'quality_green': None, 
                                                        'battery_start': None, 
                                                        'battery_end': None, 
                                                        'sampling_rate_real': None, 
                                                        'linear_fit_m': None, 
                                                        'linear_fit_b': None
                                                        }, 
                                            'events': [], 
                                            'rd_version_updates': [], 
                                            'last_update': None, 
                                            'last_update_username': None,
                                            'check_sum': check_sum,
                                            'available_cols': [],
                                            'source': None 
                                            }
                                    }
                            }
                )
        
        # volatile data
        super().__setattr__('_volatile', {
                'cols': {}, 
                'path': None, 
                'logger': logging.getLogger(__name__), 
                'analyse': False, 
                'current_time_calc': None, 
                'current_time_rec': None, 
                'status_cols': None
                })
    
    def __getattr__(self, name):
        if name == 'time_calc':
            return self.counter.time_calc
        elif name == 'time_rec':
            return self.counter.time_rec
        elif name in list(self._volatile):
            if name == 'cols':
                # load all cols
                if self._volatile['status_cols'] == None:
                    if self.path == None:
                        self.logger.error('you have to set self.path')
                        raise PathError('self.path is undefined')
                    try:
                        dir_list = os.listdir(self.path)
                    except FileNotFoundError:
                        self.logger.error('rawdata directory does not exist: ' + str(self.path))
                        self._volatile['status_cols'] = 'nofile'
                        dir_list = []
                    for identifier in dir_list:
                        identifier = identifier.replace('.pkl.gz',  '')
                        if (identifier == 'rawdata' or
                                identifier == 'data_lines.raw' or
                                identifier == 'data_blocks.raw' or
                                identifier == 'old_versions' or
                                identifier == 'seizures' or
                                identifier == '.DS_Store' or
                                identifier == 'rawdata_json.txt'):
                            continue
                        self._volatile['cols'][identifier] = RawDataCol(identifier, self.path, check_sum=self.check_sum)
                    self._volatile['status_cols'] = 'loaded'
            return self._volatile[name]
        elif name == 'data':
            return self._data['data']
        elif name in list(self._data['data']['meta']):
            return self._data['data']['meta'][name]
        elif name in list(self._data['data']['meta']['db']):
            return self._data['data']['meta']['db'][name]
        elif name in list(self._data['data']['meta']['rd']):
            return self._data['data']['meta']['rd'][name]
        elif name == 'path':
            return self._volatile['path']
        elif name == 'ppg':
            if 'ppg_green' in self.available_cols:
                return self.ppg_green
            elif 'ppg_ir' in self.available_cols:
                return self.ppg_ir
            elif 'ppg_red' in self.available_cols:
                return self.ppg_red
            else:
                self.logger.warning('object has no ppg data')
                return False
        else:
            try:
                # load col if not done yet
                if not name in rawdata_dict:
                    raise KeyError
                try:
                    status = self._volatile['cols'][name].status
                    source = self._volatile['cols'][name].source
                except KeyError:
                    status = None
                    source = None
                #if status == None and source != 'calculated' and self.status != 'receiving':
                if status == None:
                    if self.path:
                        self._volatile['cols'][name] = RawDataCol(name, self.path, check_sum=self.check_sum)
                    else:
                        self.logger.error('you have to set self.path')
                        raise PathError('self.path is undefined')
                return self._volatile['cols'][name]
            except KeyError:
                self.logger.warning('object has no attribute ' + str(name))
                raise AttributeError('object has no attribute ' + str(name))
    
    def __setattr__(self, name, value):
        if name == 'time_calc' or name == 'time_rec':
            # ToDo: implement and set it for all cols?
            self.logger.warning('this attribute is not yet implemented in __setattr__() ' + str(name))
            raise AttributeError('this attribute is not yet implemented in __setattr__() ' + str(name))
        elif name in list(self._volatile):
            self._volatile[name] = value
            if name == 'path':
                for identifier, col in self.cols.items():
                    col.path = str(self.path) + identifier + '.pkl.gz'
        elif name == 'data':
            self._data['data'] = value
        elif name in list(self._data['data']['meta']):
            self._data['data']['meta'][name] = value
        elif name in list(self._data['data']['meta']['db']):
            self._data['data']['meta']['db'][name] = value
        elif name in list(self._data['data']['meta']['rd']):
            self._data['data']['meta']['rd'][name] = value
        else:
            try:
                if not name in rawdata_dict:
                    raise KeyError
                self._volatile['cols'][name] = value
            except KeyError:
                self.logger.warning('object has no attribute ' + str(name))
                raise AttributeError('object has no attribute ' + str(name))
    
    def __repr__(self):
        repr_infos = {}
        repr_infos['meta'] = self._data['data']['meta']
        #repr_infos['available_cols'] = self._available_cols
        
        return_json = json.dumps(repr_infos)
        return(return_json)
    
    # free used memory -> it's a test
    def free(self):
        
        self._volatile['cols'] = {}
    
    def dumps(self, indent=4, silent=False, cols_dic=False):
        repr_infos = {}
        repr_infos['meta'] = self._data['data']['meta']
        # avoid lazy load of the cols by using cols_dic
        if cols_dic:
            repr_infos['~cols'] = cols_dic
        else:
            repr_infos['~cols'] = {}
            for col in self._available_cols:
                repr_infos['~cols'][col] = self.cols[col].dumps()
        
        return_json = json.dumps(repr_infos, indent=indent, sort_keys=True)
        
        if not silent:
            print(return_json)
        
        return(return_json)
    
    @property
    def _available_cols(self):
        
        col_list = []
        
        for col in self.cols:
            
            col_list.append(col)
        
        col_list = sorted(col_list)
        
        return col_list
    
    def heart_rate_lasertag(self, game_start, game_end):
        
        quality_lasertag = {'x': [], 'x_min': [], 'y': []}
        
        try:
            self.quality.x_timestamps_del()
        except KeyError:
            pass
        try:
            self.quality.y_moving_max_del()
        except KeyError:
            pass
        try:
            self.heart_rate.x_minutes_del()
        except KeyError:
            pass
        
        for i, timestamp in enumerate(self.quality.x_timestamps(datetime.strptime(self.date_time_start, '%Y-%m-%d %H:%M:%S'))):
            
            if timestamp < game_start or timestamp > game_end:
                
                #self.logger.debug('skip ' + str(timestamp))
                continue
            
            elif self.quality.y_moving_max[i] < 30:
                
                    quality_lasertag['x'].append(self.heart_rate.x[i])
                    quality_lasertag['x_min'].append(self.heart_rate.x_minutes[i])
                    quality_lasertag['y'].append(0)
            
            else:
                
                try:
                    
                    quality_lasertag['x'].append(self.heart_rate.x[i])
                    quality_lasertag['x_min'].append(self.heart_rate.x_minutes[i])
                    quality_lasertag['y'].append(self.heart_rate.y[i])
                
                except IndexError:
                    
                    pass
        
        return quality_lasertag
    
    def heart_rate_lasertag_time_range(self, game_start, game_end):
        
#        if 'game_start_x' in self._volatile and not self.game_start_x is None:
#            return (self._volatile['game_start_x'], self._volatile['game_end_x'])
        
        if game_start is None or game_end is None:
            return (1e12, -1e12)
        
        found_start = False
        found_end = False
        
        for i, timestamp in enumerate(self.heart_rate.x_timestamps(datetime.strptime(self.date_time_start, '%Y-%m-%d %H:%M:%S'))):
            
            # set game_start_x once
            if found_start is False and timestamp > game_start and timestamp < game_end:
                
                self._volatile['game_start_x'] = self.heart_rate.x[i]
                found_start = True
            
            # set game_end_x once
            if found_start and timestamp < game_end:
                
                self._volatile['game_end_x'] = self.heart_rate.x[i]
                found_end = True
        
        if found_start is False:
            self._volatile['game_start_x'] = 1e12
        if found_end is False:
            self._volatile['game_end_x'] = -1e12
        
        return (self._volatile['game_start_x'], self._volatile['game_end_x'])
    
    def heart_rate_lasertag_time_range_del(self):
        
        if 'game_start_x' in self._volatile:
            self._volatile['game_start_x'] = None
            self._volatile['game_end_x'] = None
    
    def list_meta(self):
        
        for key in sorted(self.data['meta']):
            
            if str(type(self.data['meta'][key])) == "<class 'dict'>":
                print('\t' + key + ': ')
                for key2 in sorted(self.data['meta'][key]):
                    print('\t\t' + key2 + ': ' + str(self.data['meta'][key][key2]))
            else:
                print('\t' + key + ': ' + str(self.data['meta'][key]))
        
        print('cols')
        for identifier, col in self.cols.items():
            print('\t' + identifier + ' > min:' + str(col.min) + ', max:' + str(col.max) + ', source:' + str(col.source))
    
    def identifier_sorted(self):
        
        return identifier_sorted
    
    def new_line(self, time_calc, time_rec):
        
        self.current_time_calc = time_calc
        self.current_time_rec = time_rec
        self.analyse = False
    
    def add(self, identifier, value, source='received', check_sum=None):
        
        if self.current_time_calc is None or self.current_time_rec is None:
            self.logger.warning('set current_time_calc and current_time_rec before adding new values!')
            return False
        
        try:
            col = self._volatile['cols'][identifier]
        except KeyError:
            self._volatile['cols'][identifier] = RawDataCol(identifier, self.path, source=source, check_sum=self.check_sum)
            col = self._volatile['cols'][identifier]
        
        if not value is False:
            col.add_value(self.current_time_calc, self.current_time_rec, value, check_sum=check_sum)
        
        self.analyse = False
    
    # Important: take care of flag self._volatile['status_cols'] = 'loaded'!!!
    def add_all(self, identifier, time_rec_list, time_calc_list, value_list, source='received', sort=False, check_sum=None):

        try:
            col = self._volatile['cols'][identifier]
        except KeyError:
            self._volatile['cols'][identifier] = RawDataCol(identifier, self.path, source=source, check_sum=self.check_sum)
            col = self._volatile['cols'][identifier]
        
        if time_rec_list and time_calc_list and value_list and len(time_rec_list) == len(time_calc_list) and len(time_rec_list) == len(value_list):
            col.add_all(time_rec_list, time_calc_list, value_list, check_sum=check_sum, sort=sort)
        
        self.analyse = False
        self._volatile['status_cols'] = 'loaded'
    
    def analyse(self):
        
        # ToDo: inspect code performance -> time for analyse()
        
        self.logger.debug('analyse rawdata')
        
        # #######################
        # reset calculated cols and load others
        for identifier in list(self.cols):
            if self.cols[identifier].source == 'calculated':
                os.remove(self.cols[identifier].path)
                del self.cols[identifier]
            else:
                if self.cols[identifier].status == None:
                    self.cols[identifier].load()
        
        # #########################
        # check if rawdata are loaded
        this_are_rawdata = True
        try:
            rd_len = len(self.time_calc)
        except AttributeError:
            self.logger.warning('no rawdata')
            this_are_rawdata = False
            #return False
        
        if rd_len == 0:
            self.logger.warning('no rawdata')
            this_are_rawdata = False
            #return False
        
        # #########################
        # remove time gaps and detect time_rec_leaps
        
        check_index = [0]
        time_calc_new_l = []
        time_calc_new_l_100 = []
        counter_overflows_sum = 0
        counter_overflow_delta = 0
        time_calc_new = 0
        diff1 = []
        diff2 = []
        
        # ###################################
        # START do this only for real rawdata with time_calc, counter, ...
        if this_are_rawdata:
            for i in range(rd_len):
                
                # #########################
                # remove all time gaps with counter_overflows
                if self.rd_version == 0:
                    if i > 0:
                        delta = self.time_calc[i] - self.time_calc[i-1]
                        counter_overflows = int(delta / (256*0.02))
                        counter_overflows_sum = counter_overflows_sum + counter_overflows
                        if counter_overflows > 0:
                            counter_overflow_delta = counter_overflows_sum * 256*0.02
                    time_calc_new = self.time_calc[i] - counter_overflow_delta
                    time_calc_new_l.append(time_calc_new)
                    time_calc_new_l_100.append(time_calc_new)
                    time_calc_new_l_100.append(time_calc_new+0.01)
                else:
                    time_calc_new_l.append(self.time_calc[i])
                    time_calc_new_l_100.append(self.time_calc[i])
                    time_calc_new_l_100.append(self.time_calc[i]+0.01)
                
                # #########################
                # detect time_rec_leaps
                if i > 0:
                    time_rec_leap = self.time_rec[i] - self.time_rec[i-1]
                    if i > 1:
                        time_rec_leap = time_rec_leap + self.time_rec[i-1] - self.time_rec[i-2]
                    if i > 2:
                        time_rec_leap = time_rec_leap + self.time_rec[i-2] - self.time_rec[i-3]
                    # ToDo: test different params: time_rec_leap > 4.0 or time_rec_leap > 2.5, etc.
                    if time_rec_leap > 4.0 and i - check_index[-1] > 3:
                        check_index.append(i)
                        #self.logger.debug('time_rec: ' + str(self.time_rec[i]) + ', time_rec_leap: ' + str(time_rec_leap))
                else:
                    time_rec_leap = 0
                diff1.append(time_rec_leap)
                # diff between time_rec and time_calc (without counter_overflows)
                diff2.append(self.time_rec[i] - time_calc_new_l[i])
            check_index.append(rd_len)
            
            # #########################
            # analyse all indices in check_index
            insert_time_calc = {}
            min_last = False
            
            for i in range(len(check_index)):
                
                # ToDo: does this avoid the chek of index 0 and rd_len?
                if check_index[i] == 0 or check_index[i] == rd_len:
                    continue
                
                MAX_LEN = 15*50
                
                # #######################
                # find min diff between time_calc and time_rec before current i
                # ToDo: if len_int_1 << this part is highly vulnerable
                len_int_1 = check_index[i] - check_index[i-1]
                if len_int_1 > MAX_LEN:
                    len_int_1 = MAX_LEN
                
                i_1 = check_index[i]-len_int_1
                i_2 = check_index[i]
                if min_last:
                    min_1 = min_last
                else:
                    min_1 = min(diff2[i_1:i_2])
                
                #self.logger.debug('min_1: ' + str(min_1) + ', i_1: ' + str(i_1) + ', i_2: ' + str(i_2))
                
                # #######################
                # find min diff between time_calc and time_rec after current i
                len_int_2 = check_index[i+1] - check_index[i]
                
                if len_int_2 > MAX_LEN:
                    len_int_2 = MAX_LEN
                
                i_1 = check_index[i]
                i_2 = check_index[i]+len_int_2
                min_2 = min(diff2[i_1:i_2])
                
                #self.logger.debug('min_2: ' + str(min_2) + ', i_1: ' + str(i_1) + ', i_2: ' + str(i_2))
                
                # define a limit for the detected time_gap
                time_gap_limit = math.sqrt(MAX_LEN / len_int_1) * 0.5 + math.sqrt(MAX_LEN / len_int_2) * 0.5
                
                time_gap = min_2 - min_1
                insert_counter_overflows = time_gap/(256.0*0.02)
                
                # #######################
                # if the time_gap > time_gap_limit: record time_gap to insert_time_calc
                if time_gap > time_gap_limit:
                    self.logger.debug('time_gap: ' + str(time_gap) + ' @ ' + str(time_calc_new_l[check_index[i]]) + ' insert_counter_overflows: ' + str(insert_counter_overflows) + ', time_gap_limit: ' + str(time_gap_limit))
                    insert_time_calc[check_index[i]] = time_gap
                
                    if check_index[i+1] - check_index[i] < 2*MAX_LEN:
                        min_last = min_2
                    else:
                        min_last = False
                    
                else:
                    #self.logger.debug('\t' + 'NO: time_gap: ' + str(time_gap) + ' @ ' + str(time_calc_new_l[check_index[i]]) + ' insert_counter_overflows: ' + str(insert_counter_overflows) + ', time_gap_limit: ' + str(time_gap_limit))
                    
                    # ToDo: make this more general: last min before last gap
                    if check_index[i+1] - check_index[i] < 2*MAX_LEN:
                        min_last = min_1
            
            # #######################
            # add insert_time_calc records to time_calc_new_l
            diff3 = []
            time_calc_add = 0
            for i in range(rd_len):
                if i in insert_time_calc:
                    time_calc_add = time_calc_add + insert_time_calc[i]
                time_calc_new_l[i] = time_calc_new_l[i] + time_calc_add
                time_calc_new_l_100[2*i] = time_calc_new_l_100[2*i] + time_calc_add
                time_calc_new_l_100[2*i+1] = time_calc_new_l_100[2*i+1] + time_calc_add
                diff3.append(self.time_rec[i] - time_calc_new_l[i])
            
            # #######################
            # filter maxima and spikes out of diff3 -> best fit to linear function
            diff4 = []
            diff4_x = []
            limit = False
            # analyse only with 1 Hz
            diff3_sliced = diff3[::50]
            if len(diff3_sliced) > 60:
                for i in range(len(diff3_sliced)):
                    if i > 20:
                        limit = min(diff3_sliced[i-20:i]) + 0.1
                    if not limit is False and diff3_sliced[i] < limit:
                        diff4_x.append(time_calc_new_l[i*50])
                        diff4.append(diff3_sliced[i])
            
            # #######################
            # fit diff4 with linear function -> inclination: m and offset: b
            # subtract diff4 from time_calc_new_l
            diff5 = []
            diff5_x = []
            diff6 = []
            if diff4:
                m, b = np.polyfit(diff4_x, diff4, 1)
                #self.logger.debug('m: ' + str(m) + ', b: ' + str(b))
                for i in range(len(diff3_sliced)):
                        diff5_x.append(time_calc_new_l[i*50])
                        diff5.append(m*time_calc_new_l[i*50]+b)
            else:
                m = False
            if m:
                for i in range(rd_len):
                    time_calc_new_l[i] = time_calc_new_l[i] + m*time_calc_new_l[i]+b
                    time_calc_new_l_100[2*i] = time_calc_new_l_100[2*i] + m*time_calc_new_l_100[2*i]+b
                    time_calc_new_l_100[2*i+1] = time_calc_new_l_100[2*i+1] + m*time_calc_new_l_100[2*i+1]+b
                    diff6.append(self.time_rec[i] - time_calc_new_l[i])
                self.linear_fit_m = m
                self.linear_fit_b = b
            try:
                col = self.cols['diff_time_rec_calc']
            except KeyError:
                self.diff_time_rec_calc = RawDataCol('diff_time_rec_calc', self.path)
            self.diff_time_rec_calc.status = 'calculated'
            if diff6:
                self.diff_time_rec_calc.values = diff6
                self.diff_time_rec_calc.time_calc = copy.deepcopy(time_calc_new_l)
            elif diff3:
                self.diff_time_rec_calc.values = diff3
                self.diff_time_rec_calc.time_calc = copy.deepcopy(time_calc_new_l)
            else:
                self.diff_time_rec_calc.values = []
                self.diff_time_rec_calc.time_calc = []
            try:
                #sampling_rate_real = 1.0/(time_calc_new_l[1]-time_calc_new_l[0])
                test = time_calc_new_l[100]
                hist_diff = np.histogram(np.diff(time_calc_new_l[:100]), 10)
                t_diff = hist_diff[1][np.argmax(hist_diff[0])]
                if t_diff > 0:
                    sampling_rate_real = 1.0 / t_diff
                else:
                    sampling_rate_real = None
                #self.logger.debug('real sampling_rate: ' + str(round(sampling_rate_real, 4)))
                self.sampling_rate_real = sampling_rate_real
            except (IndexError, ZeroDivisionError):
                #self.logger.debug('real sampling_rate: NA')
                t_diff = 0.02
                pass
            
            # #######################
            # analyse missing lines in ble_service debug_data, transfer_rate and duration
            len_time_calc_new_l = len(time_calc_new_l)
                
            count_inserts_sum = 0
            count_data = 0
            second = 0
            
            if len_time_calc_new_l > 1:
                
                #delta = time_calc_new_l[1]-time_calc_new_l[0]
                delta = t_diff
                
                self.new_line(0, 0)
                self.add('transfer_rate', 0)
                
                for i in range(len_time_calc_new_l):
                    
                    self.new_line(time_calc_new_l[i], time_calc_new_l[i])
                    self.add('ble_service_debug_data_inserts', 0)
                    count_data = count_data + 1
                    
                    # calc transfer_rate
                    if int(time_calc_new_l[i]) > second:
                        
                        second = int(time_calc_new_l[i])
                        
                        self.new_line(second, second)
                        transfer_rate = float(5*50 - sum(self.ble_service_debug_data_inserts.y[-5*50:])) / float(5*50)
                        self.add('transfer_rate', transfer_rate)
                    
                    if i < len_time_calc_new_l - 1:
                        
                        missing_lines = int(round((time_calc_new_l[i+1] - time_calc_new_l[i]) / delta, 0)) - 1
                        count_inserts = 1
                        while missing_lines > 0:
                            
                            self.new_line(time_calc_new_l[i] + count_inserts*delta, time_calc_new_l[i] + count_inserts*delta)
                            self.add('ble_service_debug_data_inserts', 1)
                            missing_lines = missing_lines - 1
                            count_inserts = count_inserts + 1
                            count_inserts_sum = count_inserts_sum + 1
            
            try:
                self.transfer_rate_all = float(count_data) / float(count_data + count_inserts_sum)
            except ZeroDivisionError:
                self.transfer_rate_all = 0
            if time_calc_new_l:
                self.duration = time_calc_new_l[-1]-time_calc_new_l[0]
            else:
                self.duration = None
            
            # #######################
            # change time_calc to time_calc_new_l for all debug data
            for identifier, col in self.cols.items():
                if col.ble_service == 'debug data' and col.sampling_rate == 50 and identifier != 'ble_service_debug_data_inserts':
                    if col.sampling_rate_custom == 100:
                        col.time_calc = copy.deepcopy(time_calc_new_l_100)
                    else:
                        col.time_calc = copy.deepcopy(time_calc_new_l)
                # ToDo: implement 100 Hz, etc.
            
            for identifier, col in self.cols.items():
                if col.sampling_rate != 50:
                    col.time_calc = copy.deepcopy(col.time_rec)
        # END do this only for real rawdata with time_calc, counter, ...
        # ###################################
        else:
            duration_max = 0
            length_max = 0
            # for non real raw data set time_calc to time_rec
            for identifier, col in self.cols.items():
                if col.sampling_rate != 50:
                    col.time_calc = copy.deepcopy(col.time_rec)
                    if len(col.time_rec) > length_max:
                        length_max = len(col.time_rec)
                    try:
                        if col.time_rec[-1] > duration_max:
                            duration_max = col.time_rec[-1]
                    except IndexError:
                        pass
            self.duration = duration_max
            try:
                transfer_rate_all = float(duration_max) / float(length_max)
            except ZeroDivisionError:
                transfer_rate_all = 0
            if transfer_rate_all > 1.0:
                transfer_rate_all = 1.0
            self.transfer_rate_all = transfer_rate_all
            
            #try:
            #    self.duration = self.heart_rate.time_rec[-1]
            #except AttributeError:
            #    pass
        
        # #########################
        # quality green, yellow, red
        try:
            q_green, q_yellow, q_red = calc_quality_level(self.quality.y)
            self.quality_green = q_green
            self.quality_yellow = q_yellow
            self.quality_red = q_red
        except AttributeError:
            pass
        
        # #########################
        # battery_start, battery_end
        try:
            self.battery_start = self.battery_percentage.y[0]
            self.battery_end = self.battery_percentage.y[-1]
        except (AttributeError, IndexError):
            pass
        
        # #########################
        # min and max
        for identifier, col in self.cols.items():
            
            max_value = -1e20
            min_value = 1e20
            for val in col.y:
                if val > max_value:
                    max_value = val
                if val < min_value:
                    min_value = val
            if min_value < 1e20:
                col.min = min_value
            if max_value > -1e20:
                col.max = max_value
        
        # ToDo: this could be implemented better:
        # #######################
        # derive time_calc for 1 Hz data and sampling_rate == None
#        grid_1hz = {}
#        for identifier, col in self.cols.items():
#            if col.sampling_rate is None:
#                col.time_calc = col.time_rec
#            elif col.sampling_rate != 1:
#                continue
#            grid_1hz[identifier] = {}
#            for i in range(-100, int(self.time_rec[-1]) + 100):
#                grid_1hz[identifier][i] = i
#            
#            for i in range(len(col.y))[::-1]:
#                
#                time_grid = int(col.time_rec[i])
#                
#                insert = False
#                try_count = 0
#                while not insert:
#                    if time_grid in grid_1hz[identifier] or try_count > 100:
#                        col.time_calc[i] = time_grid
#                        del grid_1hz[identifier][time_grid]
#                        insert = True
#                        if try_count > 100:
#                            self.logger.debug('time_grid: try_count > 100')
#                            break
#                    else:
#                        time_grid = time_grid - 1
#                        try_count = try_count + 1
        
        self.analyse = True
        self.rd_version = current_rawdata_version
        
        try:
            test = diff6
            return (diff1, diff2, diff3, diff4, diff5, diff5_x, diff6)
        except UnboundLocalError:
            return ([], [], [], [], [], [], [])
    
    def save(self, meta_data_only=False):
        
        if self.analyse is False:
            
            raise AnalyseError('please perform analyse() before saving')
        
        if not self.path:
            
            raise PathError('self.path is undefined')
        
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        
        if self.source == 'received':
            # try to load cols_dic from json file
            try:
                fp = open(self.path + 'rawdata_json.txt', 'r')
                lines = fp.readlines()
                line_str = ''
                for line in lines:
                    line_str += line
                if len(line_str) > 10:
                    line_str_json = json.loads(line_str)
                else:
                    line_str_json = {}
                if '~cols' in line_str_json:
                    cols_dic = line_str_json['~cols']
                elif '_cols' in line_str_json:
                    cols_dic = line_str_json['_cols']
                elif 'Cols' in line_str_json:
                    cols_dic = line_str_json['Cols']
                elif 'cols' in line_str_json:
                    cols_dic = line_str_json['cols']
                else:
                    cols_dic = False
                fp.close()
            except FileNotFoundError:
                cols_dic = False
        
            fp = open(self.path + 'rawdata_json.txt', 'w')
            # don't update cols_dic just take the existing one to avoid lazy load of the cols
            if cols_dic:
                fp.write(self.dumps(indent=4, silent=True, cols_dic=cols_dic))
            else:
                fp.write(self.dumps(indent=4, silent=True))
            fp.close()
            
        elif self.source == 'analyzed':    
            fp = open(self.path + 'rawdata_json.txt', 'w')
            fp.write(self.dumps(indent=4, silent=True))
            fp.close()
            
        f = gzip.open(self.path + 'rawdata.pkl.gz','wb')
        pickle.dump(self.data, f, protocol=3)
        f.close()
        
        if not meta_data_only:
            
            for identifier, col in self.cols.items():
                
                # if col is volatile or empty do not save it
                if not col.volatile and not col.y == []:
                    
                    col.save()
    
    def add_event(self, event):
        
        if not self.path:
            
            raise PathError('self.path is undefined')
        
        try:
            x_start = int((event.event_start - datetime.strptime(self.date_time_start, '%Y-%m-%d %H:%M:%S')).total_seconds())
            x_end = int((event.event_end - datetime.strptime(self.date_time_start, '%Y-%m-%d %H:%M:%S')).total_seconds())
        except TypeError:
            return False
        
        if (str(self.date_time_start) == 'None' or str(self.date_time_end) == 'None') and self.duration is None:
            return False
        elif self.duration is None:
            duration = (datetime.strptime(self.date_time_end, '%Y-%m-%d %H:%M:%S') - datetime.strptime(self.date_time_start, '%Y-%m-%d %H:%M:%S')).total_seconds()
        else:
            duration = self.duration
        
        # check if event already exists
        for e in self.events:
            if e['event_start'] == str(event.event_start):
                self.logger.debug('Event ' + str(e['id']) + ' already exists in rawdata. Skip...')
                return False
            
        #if(x_start < 0 or x_end > self.duration + 300):
        if(x_start < 0 or x_start > duration):
            #self.logger.debug('seems not to be an event of this data file: ' + str(event))
            return False
        
#        if self.rd_version < 2:
#            self.data['meta']['events'] = []
#            self.rd_version = current_rawdata_version
#        
#        self.events.append({
#                                        'id': event.id, 
#                                        'event_start': str(event.event_start), 
#                                        'event_end': str(event.event_end), 
#                                        'category': event.category, 
#                                        'description': event.description, 
#                                        'x_start': x_start, 
#                                        'x_end': x_end
#                                        })
#        
#        self.save(meta_data_only=True)
        
        return (x_start, x_end)
    
    def delete_event(self, event_id):
        
        if not self.path:
            
            raise PathError('self.path is undefined')
        
        for i, event in enumerate(self.events):
            
            if event['id'] == event_id:
                self.events.remove(event)
        
        self.save(meta_data_only=True)
    
    def id(self, id, file_type='df'):
        
        if not self.path:
            
            raise PathError('self.path is undefined')
        
        id = int(id)
        id_padded = '{:08}'.format(id)
        #if file_type == 'df':
        #    self.path = config.path_rawdata + id_padded + '/'
        #elif file_type == 'af':
        #    self.path = config.path_rawdata_af + id_padded + '/'
        # if there are already any loaded data cols free the space in memory self.free() -> self._volatile['cols'] = {}
        self.free()
        self.load()
    
    def load(self):
        
        if not self.path:
            
            raise PathError('self.path is undefined')
        
        # ToDo: reset all volatile data
        self.current_time_calc = None
        self.current_time_rec = None
        self.cols = {}
        self.status_cols = None
        
        pickle_data = False
        count_atemps = 10
        
        # load full raw data
        try:
            f = gzip.open(str(self.path) + 'rawdata.pkl.gz', 'rb')
        except FileNotFoundError:
            self.logger.error('file rawdata.pkl.gz not found')
            return False

        while(pickle_data is False and count_atemps > 0):
            try:
                pickle_data = pickle.load(f)
            except:
                pickle_data = False
                count_atemps = count_atemps - 1
        f.close()
        
        if pickle_data is False:
            self.logger.error('not able to load pickle data (zlib.error?)')
            return False
        
        # related meta data
        try:
            test = pickle_data['meta']['rd']['transfer_rate_all']
            test = pickle_data['meta']['rd']['battery_start']
        except KeyError:
            # initial rawdata version
            #self.load_v00(pickle_data)
            #self.rd_version = 0
            #return True
            return False
        
        self.data = pickle_data

        # fix for rawdata without check_sum specified
        if not 'check_sum' in self.data['meta']:
            self.data['meta']['check_sum'] = False
        
#        for identifier in os.listdir(self.path):
#            
#            identifier = identifier.replace('.pkl.gz',  '')
#            
#            if identifier == 'rawdata' or identifier == 'data_lines.raw' or identifier == 'data_blocks.raw':
#                continue
#            
#            self.cols[identifier] = RawDataCol(identifier, self.path, check_sum=self.check_sum)
#            self.cols[identifier].load()
        
        if self.rd_version and self.rd_version > 0:
            self.analyse = True
        
        return True
    
    def plot_diff_rec_calc(self):
        
        return self.plot([['transfer_rate'], ['diff_time_rec_calc']])
    
    def plot_hr(self):
        
        return self.plot('heart_rate')
    
    def plot_rr_int(self):
        
        return self.plot('rr_int', linewidth=0.0)
    
    def plot_hr2(self):
        
        return self.plot([['heart_rate'], ['quality'], ['acc_x', 'acc_y', 'acc_z']])
    
    def plot_temp(self):
        
        return self.plot('temperature')
    
    def plot_temp_accz(self):
        
        return self.plot([['temperature'], ['acc_z']])
    
    def plot_batt(self):
        
        return self.plot('battery_percentage')
    
    def plot_watermark(self):
        
        fig = plt.figure()
        col = '#2196f3'
        #specify color of plot when showing in program. 
        #fig.set_facecolor(col) also works
        fig.patch.set_facecolor(col)
        #specify color of background in saved figure
        matplotlib.rcParams['savefig.facecolor'] = col
    
    def plot(self, col_names, start=False, end=False, y_start=False, y_end=False, dots=False, linewidth=1.0, watermark=False, y_tick_sci=False):
        
        # ToDos:
        #   - xlabel only for the bottom subplot 
        #   - evaluate the max x value for all data and do: xlim=[0, max-x-value]
        #   - do xlim=[start, end] and ylim=[min, max] for zooms
        #   - change time depending of the duration: seconds, hours, days... maybe custom, maybe timestamps?
        #   - add plot of timestamps, quality_red/green/yellow, time_rec
        #   - define height of subplots in plot_format
        #   - some default plots: acc_x, acc_y...
        #   - use scatter not plot:
        #       - exception for large files: OverflowError: Allocated too many blocks
        #       - src: http://stackoverflow.com/questions/23870186/plotting-many-datapoints-with-matplotlib-in-python
        #       - -> did not work
        #       - work around for now: turn of plot of ble_service_debug_data_inserts
        
        try:
            plt.close()
        except:
            pass
        
        if watermark:
            self.plot_watermark()
        
#        rd_len = len(self.time_calc)
#        if rd_len > 60000:
#            slicing = 20
#        else:
#            slicing = 1
        
        if dots:
            marker_style = 'o'
            marker_size = 5
        else:
            marker_style = '.'
            marker_size = 5
        
        # ToDo: do not use this try block for all the lines!
        try:
        
            # ###################
            # plotting one single col in one single chart
            if str(type(col_names)) == "<class 'str'>":
                
                if col_names == 'time_rec':
                    #plt.plot(self.time_calc, self.time_rec, color='#AAAAAA', label='time_rec', marker=marker_style, markersize=marker_size, linewidth=linewidth)
                    plt.plot(self.time_calc, self.time_rec, color='#AAAAAA', label='time_rec', marker=marker_style, markersize=marker_size, linewidth=linewidth)
                elif col_names == 'time_calc':
                    plt.plot(self.time_calc, self.time_calc, color='#AAAAAA', label='time_calc', marker=marker_style, markersize=marker_size, linewidth=linewidth)
                else:
                    try:
                        try:
                            col_name, calc_function = col_names.split('.')
                            calc_function_str = ' . ' + calc_function
                        except ValueError:
                            col_name = col_names
                            calc_function = ''
                            calc_function_str = ''
                        col = self.cols[col_name]
                        if calc_function:
                            calc_values = getattr(col, calc_function)
                            y_values = calc_values['y']
                            x_values = calc_values['x']
                        else:
                            y_values = col.y
                            x_values = col.x
                            color_set = col.color
                        plt.plot(x_values, y_values, color=color_set, label=col.name_short + calc_function_str, marker=marker_style, markersize=marker_size, linewidth=linewidth)
                    except KeyError:
                        self.logger.warning('unknown col_name: ' + str(col_names))
                        return plt
                self.plot_format(plt, [col_name], start=start, end=end, y_start=y_start, y_end=y_end, y_tick_sci=y_tick_sci)
            
            elif str(type(col_names)) == "<class 'list'>":
                
                # ###################
                # plotting some cols in one single chart
                if str(type(col_names[0])) == "<class 'str'>":
                    
                    col_names2 = []
                    for col_name in col_names:
                        
                        try:
                            if col_name == 'time_rec':
                                plt.plot(self.time_calc, self.time_rec, color='#AAAAAA', label='time_rec', marker=marker_style, markersize=marker_size, linewidth=linewidth)
                            elif col_name == 'time_calc':
                                plt.plot(self.time_calc, self.time_calc, color='#AAAAAA', label='time_calc', marker=marker_style, markersize=marker_size, linewidth=linewidth)
                            else:
                                try:
                                    col_name, calc_function = col_name.split('.')
                                    calc_function_str = ' . ' + calc_function
                                except ValueError:
                                    calc_function = ''
                                    calc_function_str = ''
                                col_names2.append(col_name)
                                col = self.cols[col_name]
                                if calc_function:
                                    calc_values = getattr(col, calc_function)
                                    y_values = calc_values['y']
                                    x_values = calc_values['x']
                                else:
                                    y_values = col.y
                                    x_values = col.x
                                    color_set = col.color
                                col = self.cols[col_name]
                                plt.plot(x_values, y_values, color=color_set, label=col.name_short + calc_function_str, marker=marker_style, markersize=marker_size, linewidth=linewidth)
                        except KeyError:
                            self.logger.warning('unknown col_name: ' + str(col_name))
                            return plt
                    self.plot_format(plt, col_names2, start=start, end=end, y_start=y_start, y_end=y_end, y_tick_sci=y_tick_sci)
                
                # ###################
                # plotting some cols in some subplots
                elif str(type(col_names[0])) == "<class 'list'>":

                    for i, subplot in enumerate(col_names):

                        col_names2 = []
                        
                        plt.subplot(len(col_names), 1, i+1)
                        
                        for col_name in subplot:
                            
                            try:
                                if col_name == 'time_rec':
                                    plt.plot(self.time_calc, self.time_rec, color='#AAAAAA', label='time_rec', marker=marker_style, markersize=marker_size, linewidth=linewidth)
                                elif col_name == 'time_calc':
                                    plt.plot(self.time_calc, self.time_calc, color='#AAAAAA', label='time_calc', marker=marker_style, markersize=marker_size, linewidth=linewidth)
                                else:
                                    try:
                                        col_name, calc_function = col_name.split('.')
                                        calc_function_str = ' . ' + calc_function
                                    except ValueError:
                                        calc_function = ''
                                        calc_function_str = ''
                                    col_names2.append(col_name)
                                    col = self.cols[col_name]
                                    if calc_function:
                                        calc_values = getattr(col, calc_function)
                                        try:
                                            y_values = calc_values['y']
                                            x_values = calc_values['x']
                                        except TypeError:
                                            y_values = calc_values
                                            x_values = col.x
                                        color_set = color_change(col.color)
                                        # color_inverted
                                        color_set = color_inverted(col.color)
                                        color_set = col.color
                                    else:
                                        y_values = col.y
                                        x_values = col.x
                                        color_set = col.color
                                    col = self.cols[col_name]
                                    calc_function_str = ''
                                    plt.plot(x_values, y_values, color=color_set, label=col.name_short + calc_function_str, marker=marker_style, markersize=marker_size, linewidth=linewidth)
                            except KeyError:
                                self.logger.warning('unknown col_name: ' + str(col_name))
                                return plt
                        if i < len(col_names) - 1:
                            self.plot_format(plt, col_names2, sub=True, start=start, end=end, y_start=y_start, y_end=y_end, y_tick_sci=y_tick_sci)
                        else:
                            self.plot_format(plt, col_names2, sub=False, start=start, end=end, y_start=y_start, y_end=y_end, y_tick_sci=y_tick_sci)
        except ValueError:
            self.logger.error('x and y dimensions may be different. col: ' + col.identifier + ', x: ' + str(len(x_values)) + ', y: ' + str(len(y_values)))
        
        if watermark:
            self.logger.error('feature not available anymore!!!')
            '''
            #plt.savefig(config.path_tmp + 'cosinuss_plot.png', facecolor=fig.get_facecolor(), transparent=True)
            plt.savefig(config.path_tmp + 'cosinuss_plot.png', figsize=(8, 6), dpi=100)
            img_plot = Image.open(config.path_tmp + 'cosinuss_plot.png')
            img_logo = Image.open(config.root_path + 'webapp/static/img/cosinuss_logo.png')
            img_plot.paste(img_logo, (660, 560))
            img_plot.save(config.path_tmp + 'cosinuss_plot.png')
            self.logger.info('plot with watermark saved to: ' + '\n' + '\t' + config.path_tmp + 'cosinuss_plot.png')
            '''
        
        return plt
    
    def plot_format(self, plt, col_names, sub=False, start=False, end=False, y_start=False, y_end=False, y_tick_sci=False):
        
#        DURATION_LIMIT = 120
#        duration = col.x[-1]
#        
#        if duration > DURATION_LIMIT:
#            plt.xlabel('time (minutes)')
#        else:
#            plt.xlabel('time (seconds)')
        
        # #####################
        # USE: http://matplotlib.org/users/gridspec.html
        
        if len(self.time_calc) < 60000:
            min_val = 1000000000000
            max_val = -1000000000000
            for col_name in col_names:
                
                if col_name == 'time_rec':
                    col_min = self.time_rec[0]
                    col_max = self.time_rec[-1]
                elif col_name == 'time_calc':
                    col_min = self.time_calc[0]
                    col_max = self.time_calc[-1]
                else:
                    col_min = self.cols[col_name].min
                    col_max = self.cols[col_name].max
                
                if not col_min is None and not col_max is None:
                    
                    if col_min < min_val:
                        
                        min_val = col_min
                    
                    if col_max > max_val:
                        
                        max_val = col_max
            
            if min_val < 1000000000000 and max_val > -1000000000000:
                y_range = max_val - min_val
                max_val = max_val + y_range * 0.05
                if min_val != 0:
                    min_val = min_val - y_range * 0.05
                min_list = []
                max_list = []
                x_list = []
                for i in range(len(self.ble_service_debug_data_inserts.y)):
                    if self.ble_service_debug_data_inserts.y[i] == 1:
                        min_list.append(min_val)
                        max_list.append(max_val)
                    else:
                        min_list.append(0)
                        max_list.append(0)
                    x_list.append(self.ble_service_debug_data_inserts.x[i])
                plt.fill_between(x_list, min_list, max_list, alpha=0.25,  linewidth=0.0, facecolor='#ff0000')
                plt.ylim([min_val, max_val])
            # ToDo: min < max       see UserWarning: Attempting to set identical bottom==top results
        
        if not sub:
            plt.xlabel('time (seconds)')
        if col_names[0] == 'time_calc':
            plt.ylabel('time calc (seconds)')
        elif col_names[0] == 'time_rec':
            plt.ylabel('time rec (seconds)')
        elif self.cols[col_names[0]].unit:
            plt.ylabel(self.cols[col_names[0]].name_short + ' (' + self.cols[col_names[0]].unit + ')')
        else:
            plt.ylabel(self.cols[col_names[0]].name_short)
        if start:
            plt.xlim([start, end])
        if y_start:
            plt.ylim([y_start, y_end])
        
        if y_tick_sci:
            plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
        plt.legend(bbox_to_anchor=(1.01, 1), loc=2, borderaxespad=0., prop={'size':10})
        plt.grid(b=True, which='both', color='0.65',linestyle='-')
        plt.gca().xaxis.set_tick_params(width=1, length=7)
        plt.gca().yaxis.set_tick_params(width=1, length=7)
        plt.tight_layout(pad=0.5)
        # ###########################
        # ToDo: bug:
#                can't invoke "event" command: application has been destroyed
#            while executing
#        "event generate $w <<ThemeChanged>>"
#            (procedure "ttk::ThemeChanged" line 6)
#            invoked from within
#        "ttk::ThemeChanged"
#
#---------------------------------------------------
#        solved by: plt.close('all')





