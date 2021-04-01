import logging
import sys

# DO NOT CHANGE: the ids of the rawdata_dict -> its the identifier in the sensor firmware and the lab app
# You are allowed to chnage the order -> will chnage the order of arrangments in the lab server

rawdata_dict = {
                        'heart_rate': 
                                {
                                'id': 1, 
                                'name': 'heart rate', 
                                'cast': 'float', 
                                'sampling_rate': 1, 
                                'unit': 'bpm', 
                                'ble_service': 'heart rate', 
                                'desc': 'Heart rate is the speed of the heartbeat measured by the number of contractions of the heart per minute (bpm).', 
                                'color': '#FF0000',
                                'order': 1
                                }, 
                        'rr_int': 
                                {
                                'id': 2, 
                                'name': 'R-R intervals', 
                                'cast': 'int', 
                                'sampling_rate': None, 
                                'unit': 'ms', 
                                'ble_service': 'heart rate', 
                                'desc': 'The time intervals elapsing between two consecutive R waves in the electrocardiogram.', 
                                'color': '#FF0000',
                                'order': 2
                                }, 
                        'quality': 
                                {
                                'id': 3, 
                                'name': 'signal quality', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': None, 
                                'ble_service': 'quality', 
                                'desc': 'Quality of the spectrum of the pulse wave. This is a special feature of the cosinuss One°.', 
                                'color': '#00FF00',
                                'order': 3
                                }, 
                        'temperature': 
                                {
                                'id': 4, 
                                'name': 'temperature', 
                                'cast': 'float', 
                                'sampling_rate': 1, 
                                'unit': '°C', 
                                'ble_service': 'temperature', 
                                'desc': 'Body temperature measured by the device.', 
                                'color': '#0000A0',
                                'order': 4
                                }, 
                        'temperature_chip': 
                                {
                                'id': 5, 
                                'name': 'temperature chip', 
                                'cast': 'float', 
                                'sampling_rate': 1, 
                                'unit': '°C', 
                                'ble_service': 'temperature', 
                                'desc': 'Temperature of the chip of the device.', 
                                'color': '#6E00A1',
                                'order': 5
                                }, 
                        'battery_voltage': 
                                {
                                'id': 6, 
                                'name': 'battery status', 
                                'cast': 'float', 
                                'sampling_rate': None, 
                                'unit': 'V', 
                                'ble_service': 'battery', 
                                'desc': 'The battery status of the device in voltage.', 
                                'color': '#0000FF',
                                'order': 6
                                }, 
                        'battery_percentage': 
                                {
                                'id': 7, 
                                'name': 'battery status', 
                                'cast': 'float', 
                                'sampling_rate': None, 
                                'unit': '%', 
                                'ble_service': 'battery', 
                                'desc': 'The battery status of the device in percent.', 
                                'color': '#FF0000',
                                'order': 7
                                },
                        'step_frequency': 
                                {
                                'id': 8, 
                                'name': 'step frequency', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': 'spm', 
                                'ble_service': 'step frequency',        # still part of debug data
                                'desc': 'Calculated steps per minute.', 
                                'color': '#FFA500',
                                'order': 8
                                }, 
                        'acc_x': 
                                {
                                'id': 9, 
                                'name': 'acceleration x-axis', 
                                'cast': 'float', 
                                'sampling_rate': 50, 
                                'unit': 'g', 
                                'ble_service': 'debug data', 
                                'desc': 'Acceleration: x-axis. In units of the earth gravity.', 
                                'color': '#A52A2A',
                                'order': 9
                                }, 
                        'acc_y': 
                                {
                                'id': 10, 
                                'name': 'acceleration y-axis', 
                                'cast': 'float', 
                                'sampling_rate': 50, 
                                'unit': 'g', 
                                'ble_service': 'debug data', 
                                'desc': 'Acceleration: y-axis. In units of the earth gravity.', 
                                'color': '#800000',
                                'order': 10
                                }, 
                        'acc_z': 
                                {
                                'id': 11, 
                                'name': 'acceleration z-axis', 
                                'cast': 'float', 
                                'sampling_rate': 50, 
                                'unit': 'g', 
                                'ble_service': 'debug data', 
                                'desc': 'Acceleration: z-axis. In units of the earth gravity.', 
                                'color': '#808000',
                                'order': 11
                                }, 
                        'acc_sum_filtered': 
                                {
                                'id': 12, 
                                'name': 'acceleration filtered sum', 
                                'cast': 'float', 
                                'sampling_rate': 50, 
                                'unit': 'g', 
                                'ble_service': 'debug data', 
                                'desc': 'Acceleration: filtered sum of x-, y- and z-axis. In units of the earth gravity.', 
                                'color': '#FFA500',
                                'order': 12
                                }, 
                        'ppg_green': 
                                {
                                'id': 13, 
                                'name': 'PPG green', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with the green LED.', 
                                'color': '#00cc00',
                                'order': 13
                                }, 
                        'ppg_green_filtered': 
                                {
                                'id': 14, 
                                'name': 'PPG green filtered', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Filtered photoplethysmogram with the green LED.', 
                                'color': '#00cc00',
                                'order': 14
                                }, 
                        'ppg_red': 
                                {
                                'id': 15, 
                                'name': 'PPG red', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with the red LED.', 
                                'color': '#ff3300',
                                'order': 15
                                }, 
                        'ppg_red_filtered': 
                                {
                                'id': 16, 
                                'name': 'PPG red filtered', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Filtered photoplethysmogram with the red LED.', 
                                'color': '#ff3300',
                                'order': 16
                                }, 
                        'ppg_ir': 
                                {
                                'id': 17, 
                                'name': 'PPG ir', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with the ir LED.', 
                                'color': '#661400',
                                'order': 17
                                }, 
                        'ppg_ir_filtered': 
                                {
                                'id': 18, 
                                'name': 'PPG ir filtered', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Filtered photoplethysmogram with the ir LED.', 
                                'color': '#661400',
                                'order': 18
                                },
                        'ppg_ambient': 
                                {
                                'id': 19, 
                                'name': 'PPG ambient', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with all LEDs turned off.', 
                                'color': '#661400',
                                'order': 19
                                }, 
                        'led_current': 
                                {
                                'id': 20, 
                                'name': 'led current', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': 'mA', 
                                'ble_service': 'debug data', 
                                'desc': 'Current of the main LED (green/ir).', 
                                'color': '#00FF00',
                                'order': 20
                                }, 
                        'amp_1': 
                                {
                                'id': 21, 
                                'name': 'amp 1', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'amplification: param 1', 
                                'color': '#FF00FF',
                                'order': 21
                                }, 
                        'amp_2': 
                                {
                                'id': 22, 
                                'name': 'amp 2', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'amplification: param 2', 
                                'color': '#800080',
                                'order': 22
                                }, 
                        'amp_current_offset': 
                                {
                                'id': 23, 
                                'name': 'amp current offset', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'amplification: offset current', 
                                'color': '#FFFF00',
                                'order': 23
                                }, 
                        'timestamp': 
                                {
                                'id': 24, 
                                'name': 'timestamp', 
                                'cast': 'timestamp', 
                                'sampling_rate': None, 
                                'unit': None, 
                                'ble_service': 'lab app', 
                                'desc': 'Timestamps sent by the lab app.', 
                                'color': None,
                                'order': 24
                                }, 
                        'counter': 
                                {
                                'id': 25, 
                                'name': 'counter', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Counter generated by the device.', 
                                'color': None,
                                'order': 25
                                }, 
                        'ble_service_debug_data_inserts': 
                                {
                                'id': 26, 
                                'name': 'ble_service debug data inserted lines', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'inserted/missing lines of the ble_service debug data.', 
                                'color': None,
                                'order': 26, 
                                'source': 'calculated'
                                }, 
                        'transfer_rate': 
                                {
                                'id': 27, 
                                'name': 'transfer rate', 
                                'cast': 'float', 
                                'sampling_rate': 1, 
                                'unit': None, 
                                'ble_service': None, 
                                'desc': 'transfer rate of the debug data', 
                                'color': '#FF0000',
                                'order': 27, 
                                'source': 'calculated'
                                }, 
                        'diff_time_rec_calc': 
                                {
                                'id': 28, 
                                'name': 'time_rec - time_calc', 
                                'cast': 'float', 
                                'sampling_rate': 50, 
                                'unit': 'seconds', 
                                'ble_service': None, 
                                'desc': 'differnece between time_rec and time_calc', 
                                'color': '#0000FF',
                                'order': 28, 
                                'source': 'calculated', 
                                'volatile': True
                                }, 
                        'spike_count_red': 
                                {
                                'id': 29, 
                                'name': 'spikes led red', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'counts the spikes of the red ppg signal', 
                                'color': '#ff3300',
                                'order': 29
                                }, 
                        'spike_count_ir': 
                                {
                                'id': 30, 
                                'name': 'spikes led ir', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'counts the spikes of the ir ppg signal', 
                                'color': '#661400',
                                'order': 30
                                }, 
                        'spike_count_green': 
                                {
                                'id': 31, 
                                'name': 'spikes led green', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'counts the spikes of the green ppg signal', 
                                'color': '#00cc00',
                                'order': 31
                                }, 
                        'calc_time_hr_algo': 
                                {
                                'id': 32, 
                                'name': 'calculation time hr algo', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': 'ms', 
                                'ble_service': 'debug data', 
                                'desc': 'time in ms for the heart rate algorithm within a one second interval', 
                                'color': '#0000FF',
                                'order': 32
                                }, 
                        'ppg_green_led': 
                                {
                                'id': 33, 
                                'name': 'PPG green led-signal', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with the green LED. The raw led signal without subtraction of the ambient signal.', 
                                'color': '#00cc00',
                                'order': 33
                                }, 
                        'ppg_green_ambient': 
                                {
                                'id': 34, 
                                'name': 'PPG green ambient-signal', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with the green LED. The raw ambient signal with led turned off.', 
                                'color': '#00cc00',
                                'order': 34
                                }, 
                        'ppg_red_led': 
                                {
                                'id': 35, 
                                'name': 'PPG red led-signal', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with the red LED. The raw led signal without subtraction of the ambient signal.', 
                                'color': '#ff3300',
                                'order': 35
                                }, 
                        'ppg_red_ambient': 
                                {
                                'id': 36, 
                                'name': 'PPG red ambient-signal', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with the red LED. The raw ambient signal with led turned off.', 
                                'color': '#ff3300',
                                'order': 36
                                }, 
                        'ppg_ir_led': 
                                {
                                'id': 37, 
                                'name': 'PPG ir led-signal', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with the ir LED. The raw led signal without subtraction of the ambient signal.', 
                                'color': '#661400',
                                'order': 37
                                }, 
                        'ppg_ir_ambient': 
                                {
                                'id': 38, 
                                'name': 'PPG ir ambient-signal', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with the ir LED. The raw ambient signal with led turned off.', 
                                'color': '#661400',
                                'order': 38
                                }, 
                        'ppg_green_led_dc': 
                                {
                                'id': 39, 
                                'name': 'PPG green led-signal DC', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with the green LED. The raw DC led signal without subtraction of the ambient signal.', 
                                'color': '#00cc00',
                                'order': 39
                                }, 
                        'ppg_green_ambient_dc': 
                                {
                                'id': 40, 
                                'name': 'PPG green ambient-signal DC', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with the green LED. The raw DC ambient signal with led turned off.', 
                                'color': '#00cc00',
                                'order': 40
                                }, 
                        'ppg_red_led_dc': 
                                {
                                'id': 41, 
                                'name': 'PPG red led-signal DC', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with the red LED. The raw DC led signal without subtraction of the ambient signal.', 
                                'color': '#ff3300',
                                'order': 41
                                }, 
                        'ppg_red_ambient_dc': 
                                {
                                'id': 42, 
                                'name': 'PPG red ambient-signal DC', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with the red LED. The raw DC ambient signal with led turned off.', 
                                'color': '#ff3300',
                                'order': 42
                                }, 
                        'ppg_ir_led_dc': 
                                {
                                'id': 43, 
                                'name': 'PPG ir led-signal DC', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with the ir LED. The raw DC led signal without subtraction of the ambient signal.', 
                                'color': '#661400',
                                'order': 43
                                }, 
                        'ppg_ir_ambient_dc': 
                                {
                                'id': 44, 
                                'name': 'PPG ir ambient-signal DC', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Photoplethysmogram with the ir LED. The raw DC ambient signal with led turned off.', 
                                'color': '#661400',
                                'order': 44
                                }, 
                        'temperature_ir_object': 
                                {
                                'id': 45, 
                                'name': 'temperature ir sensor object', 
                                'cast': 'float', 
                                'sampling_rate': 50, 
                                'unit': '°C', 
                                'ble_service': 'debug data', 
                                'desc': 'Body temperature measured by the ir sensor (object).', 
                                'color': '#0000A0',
                                'order': 45
                                },
                        'temperature_ir_ambient': 
                                {
                                'id': 46, 
                                'name': 'temperature ir sensor ambient', 
                                'cast': 'float', 
                                'sampling_rate': 50, 
                                'unit': '°C', 
                                'ble_service': 'debug data', 
                                'desc': 'Body temperature measured by the ir sensor (ambient).', 
                                'color': '#0000A0',
                                'order': 46
                                }, 
                        'temperature_ir_object_counts': 
                                {
                                'id': 47, 
                                'name': 'temperature ir sensor object counts', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Body temperature measured by the ir sensor (object counts).', 
                                'color': '#0000A0',
                                'order': 47
                                },
                        'temperature_ir_ambient_counts': 
                                {
                                'id': 48, 
                                'name': 'temperature ir sensor ambient counts', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Body temperature measured by the ir sensor (ambient counts).', 
                                'color': '#0000A0',
                                'order': 48
                                }, 
                        'ppg_green_filtered_h': 
                                {
                                'id': 49, 
                                'name': 'PPG green filtered (high pass)', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Filtered (high pass) photoplethysmogram with the green LED.', 
                                'color': '#00cc00',
                                'order': 49
                                }, 
                        'ppg_red_filtered_h': 
                                {
                                'id': 50, 
                                'name': 'PPG red filtered (high pass)', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Filtered (high pass) photoplethysmogram with the red LED.', 
                                'color': '#ff3300',
                                'order': 50
                                }, 
                        'ppg_ir_filtered_h': 
                                {
                                'id': 51, 
                                'name': 'PPG ir filtered (high pass)', 
                                'cast': 'int', 
                                'sampling_rate': 50, 
                                'unit': None, 
                                'ble_service': 'debug data', 
                                'desc': 'Filtered (high pass) photoplethysmogram with the ir LED.', 
                                'color': '#661400',
                                'order': 51
                                }, 
                        'rr_int_2': 
                                {
                                'id': 52, 
                                'name': 'R-R intervals (alternative 2)', 
                                'cast': 'int', 
                                'sampling_rate': None, 
                                'unit': 'ms', 
                                'ble_service': 'heart rate', 
                                'desc': 'The time intervals elapsing between two consecutive R waves in the electrocardiogram.', 
                                'color': '#e29702',
                                'order': 52
                                }, 
                        'rr_int_3': 
                                {
                                'id': 53, 
                                'name': 'R-R intervals (alternative 3)', 
                                'cast': 'int', 
                                'sampling_rate': None, 
                                'unit': 'ms', 
                                'ble_service': 'heart rate', 
                                'desc': 'The time intervals elapsing between two consecutive R waves in the electrocardiogram.', 
                                'color': '#e2026a',
                                'order': 53
                                }, 
                        'magnet_x': 
                                {
                                'id': 54, 
                                'name': 'magnetic field x', 
                                'cast': 'float', 
                                'sampling_rate': 1, 
                                'unit': 'Gauss', 
                                'ble_service': 'debug data', 
                                'desc': 'magnetic field x.', 
                                'color': '#A52A2A',
                                'order': 54
                                }, 
                        'magnet_y': 
                                {
                                'id': 55, 
                                'name': 'magnetic field y', 
                                'cast': 'float', 
                                'sampling_rate': 1, 
                                'unit': 'Gauss', 
                                'ble_service': 'debug data', 
                                'desc': 'magnetic field y.', 
                                'color': '#800000',
                                'order': 55
                                }, 
                        'magnet_z': 
                                {
                                'id': 56, 
                                'name': 'magnetic field z', 
                                'cast': 'float', 
                                'sampling_rate': 1, 
                                'unit': 'Gauss', 
                                'ble_service': 'debug data', 
                                'desc': 'magnetic field z.', 
                                'color': '#808000',
                                'order': 56
                                }, 
                        'heart_rate_2': 
                                {
                                'id': 57, 
                                'name': 'heart rate 2', 
                                'cast': 'float', 
                                'sampling_rate': 1, 
                                'unit': 'bpm', 
                                'ble_service': '-', 
                                'desc': 'Alternative calculated heart rate (bpm).', 
                                'color': '#FF0000',
                                'order': 57
                                }, 
                        'spo2': 
                                {
                                'id': 58, 
                                'name': 'SpO2', 
                                'cast': 'float', 
                                'sampling_rate': 1, 
                                'unit': '%', 
                                'ble_service': '-', 
                                'desc': 'Blood oxygene satturation in %.', 
                                'color': '#FF0000',
                                'order': 58
                                }, 
                        'ecg': 
                                {
                                'id': 59, 
                                'name': 'ecg', 
                                'cast': 'float', 
                                'sampling_rate': 256, 
                                'unit': None, 
                                'ble_service': '-', 
                                'desc': 'ecg.', 
                                'color': '#0000FF',
                                'order': 59
                                }, 
                        'eeg_channel_1': 
                                {
                                'id': 60, 
                                'name': 'eeg_channel_1', 
                                'cast': 'float', 
                                'sampling_rate': 125, 
                                'unit': 'V', 
                                'ble_service': 'debug data', 
                                'desc': 'eeg_channel_1.', 
                                'color': '#FF0000',
                                'order': 60
                                }, 
                        'eeg_channel_2': 
                                {
                                'id': 61, 
                                'name': 'eeg_channel_2', 
                                'cast': 'float', 
                                'sampling_rate': 125, 
                                'unit': 'V', 
                                'ble_service': 'debug data', 
                                'desc': 'eeg_channel_2.', 
                                'color': '#0000FF',
                                'order': 61
                                },
                        'device_reconnect':
                                {
                                'id': 62,
                                'name': 'device_reconnect',
                                'cast': 'int',
                                'sampling_rate': None,
                                'unit': None,
                                'ble_service': None,
                                'desc': 'reconnects of the sensor/device.',
                                'color': '#FF0000',
                                'order': 62
                                },
                        'temperature_reference':
                                {
                                'id': 63,
                                'name': 'temperature',
                                'cast': 'float',
                                'sampling_rate': 1,
                                'unit': '°C',
                                'ble_service': 'temperature',
                                'desc': 'Body temperature measured by the device.',
                                'color': '#0000A0',
                                'order': 63
                                },
                        'fatigue':
                                {
                                'id': 64,
                                'name': 'fatigue_score',
                                'cast': 'int',
                                'sampling_rate': 1,
                                'unit': None,
                                'ble_service': None,
                                'desc': 'Fatigue score from Cardiolyse ',
                                'color': '#0000A0',
                                'order': 64
                                },
                        'hrv_quality':
                                {
                                'id': 65,
                                'name': 'hrv_quality',
                                'cast': 'float',
                                'sampling_rate': 1,
                                'unit': None,
                                'ble_service': None,
                                'desc': 'Quality for RRI from Cardiolyse ',
                                'color': '#0000A0',
                                'order': 65
                                },
                        'days_left':
                                {
                                'id': 66,
                                'name': 'days_left',
                                'cast': 'int',
                                'sampling_rate': 1,
                                'unit': None,
                                'ble_service': None,
                                'desc': 'Days left until fatigue score personalization (Cardiolyse) ',
                                'color': '#0000A0',
                                'order': 66
                                },
                        'adc_range':
                                {
                                'id': 67,
                                'name': 'adc range',
                                'cast': 'int',
                                'sampling_rate': None,
                                'unit': 'µA',
                                'ble_service': None,
                                'desc': 'param from afe max86141',
                                'color': '#0000AA',
                                'order': 67
                                },
                        'pulse_width':
                                {
                                'id': 68,
                                'name': 'pulse width',
                                'cast': 'int',
                                'sampling_rate': None,
                                'unit': 'µs',
                                'ble_service': None,
                                'desc': 'param from afe max86141',
                                'color': '#00AA00',
                                'order': 68
                                },
                        'sampling_rate':
                                {
                                'id': 69,
                                'name': 'sampling rate',
                                'cast': 'int',
                                'sampling_rate': None,
                                'unit': 'Hz',
                                'ble_service': None,
                                'desc': 'sampling rate of the incoming raw data',
                                'color': '#AA0000',
                                'order': 69
                                },
                        'led_current_ir': 
                                {
                                'id': 70, 
                                'name': 'led current ir', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': 'mA', 
                                'ble_service': 'debug data', 
                                'desc': 'Current of the ir LED.', 
                                'color': '#00FF00',
                                'order': 70
                                }, 
                        'led_current_red': 
                                {
                                'id': 71, 
                                'name': 'led current red', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': 'mA', 
                                'ble_service': 'debug data', 
                                'desc': 'Current of the red LED.', 
                                'color': '#00FF00',
                                'order': 71
                                }, 
                        'led_current_green': 
                                {
                                'id': 72, 
                                'name': 'led current green', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': 'mA', 
                                'ble_service': 'debug data', 
                                'desc': 'Current of the green LED.', 
                                'color': '#00FF00',
                                'order': 72
                                },
                        'breathing_rate': 
                                {
                                'id': 73, 
                                'name': 'breathing rate', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': '1/min', 
                                'ble_service': None, 
                                'desc': 'Breathing rate in 1/min.', 
                                'color': '#0000A0',
                                'order': 73
                                }, 
                        'ppg_quality': 
                                {
                                'id': 74, 
                                'name': 'ppg quality', 
                                'cast': 'int', 
                                'sampling_rate': 1, 
                                'unit': False, 
                                'ble_service': None, 
                                'desc': 'ppg quality.', 
                                'color': '#00FF00',
                                'order': 74
                                }, 
                        'perfusion_ir': 
                                {
                                'id': 75, 
                                'name': 'perfusion ir', 
                                'cast': 'float', 
                                'sampling_rate': 1, 
                                'unit': '%', 
                                'ble_service': None, 
                                'desc': 'perfusion of ppg_ir.', 
                                'color': '#00FF00',
                                'order': 75
                                },
                        'heart_rate_quality_filt': 
                                {
                                'id': 76, 
                                'name': 'hr_filt', 
                                'cast': 'float', 
                                'sampling_rate': 1, 
                                'unit': 'bpm', 
                                'ble_service': None, 
                                'desc': 'filtered heart rate by quality', 
                                'color': '#00FF00',
                                'order': 76
                                },
                        'acc_vector': 
                                {
                                'id': 77, 
                                'name': 'acc_vector', 
                                'cast': 'float', 
                                'sampling_rate': 50, 
                                'unit': 'g', 
                                'ble_service': None, 
                                'desc': 'acceleration vector compute from all 3 axis', 
                                'color': '#A52A2A',
                                'order': 77
                                },
                        'rr_int_offline': 
                                {
                                'id': 78, 
                                'name': 'rr_int_offline', 
                                'cast': 'float', 
                                'sampling_rate': 1, 
                                'unit': 'ms', 
                                'ble_service': None, 
                                'desc': 'R-R intervals computed using sigproclib implementation', 
                                'color': '#FF0000',
                                'order': 78
                                }, 
                        'ppg_average_deviation': 
                                {
                                'id': 79, 
                                'name': 'ppg_average_deviation', 
                                'cast': 'float', 
                                'sampling_rate': None, 
                                'unit': None, 
                                'ble_service': None, 
                                'desc': 'average of first deviation of ppg data', 
                                'color': '#00cc00',
                                'order': 79
                                },
                        'electrodermal_activity': 
                                {
                                'id': 80, 
                                'name': 'electrodermal_activity', 
                                'cast': 'float', 
                                'sampling_rate': None, 
                                'unit': 'µS', 
                                'ble_service': None, 
                                'desc': 'electrodermal_activity from empatica', 
                                'color': '#00cc00',
                                'order': 80
                                }
                        }

identifier_sorted = []
id2identifier = {}

def check_ids():
    
    # check order -> no duplicates!!!
    logger = logging.getLogger(__name__)
    
    error = False
    ids = []
    order = {}
    
    for identifier in rawdata_dict:
        ids.append(rawdata_dict[identifier]['id'])
        order[rawdata_dict[identifier]['order']] = identifier
        id2identifier[rawdata_dict[identifier]['id']] = identifier
    
    identifier_sorted.append('time')
    identifier_sorted.append('time_rec')
    for i in range(500):
        try:
            identifier = order[i]
        except KeyError:
            continue
        identifier_sorted.append(identifier)
    
    ids.sort()
    counter = 1
    for id in ids:
        if id != counter:
            error = True
        counter = counter + 1
    
    if error:
        logger.error('The ids of the rawdata_dict are not well defined')
        sys.exit()

check_ids()


