import random
import string
import os
from pathlib import Path
import json
import numpy as np

from mongoengine import EmbeddedDocumentListField, EmbeddedDocument, StringField, FloatField, IntField
import mongoengine

# import package modules
from .data_slice import DataSlice
from .dc_helper import DcHelper, AttributesContainer, InstancesContainer
from .odm import User, Project, Person, Receiver, Device, EventLog, Comment

from . import config

class DataColumn(EmbeddedDocument):
    # id
    # _hash_id = StringField(primary_key=True)

    # relationships
    data_type = StringField(db_field='d_ty', choices=config.data_types)
    dtype = StringField(db_field='dty')
    time_slices_ref = StringField(db_field='tsr', null=True)
    # for algo -> algo version hash
    source = StringField(db_field='src', null=True)
    source_producer_hash = StringField(db_field='src_ph', null=True)
    # uuid from the ble charqacteristic
    ble_chr_uuid = StringField(db_field='bleUUI', null=True)
    min = FloatField(db_field='min', null=True)
    max = FloatField(db_field='max', null=True)
    mean = FloatField(db_field='avg', null=True)
    median = FloatField(db_field='med', null=True)
    upper_quartile = FloatField(db_field='uqrt', null=True)
    lower_quartile = FloatField(db_field='lqrt', null=True)
    samples_meta = IntField(default=0, db_field='s')
    compressed_size_meta = IntField(default=0, db_field='cs')
    compression_ratio_meta = FloatField(default=0, db_field='cr')
    sampling_rate = FloatField(db_field='srt', null=True)
    linear_fit_m = FloatField(db_field='lft_m', null=True)
    linear_fit_b = FloatField(db_field='lft_b', null=True)
    duration = FloatField(default=0, db_field='dur')
    transfer_rate_all = FloatField(db_field='tr_a', null=True)

    # embedded docs: slices
    _slices_y = EmbeddedDocumentListField('DataSlice', db_field='s_y')
    _slices_time_rec = EmbeddedDocumentListField('DataSlice', db_field='s_tr')

    # Todo: old todos:
    # ToDo: define a setter/getter in df -> global default for all dc
    #       define setter/getter for dc -> for single dc

    # def __init__(self, data_type, df, time_slices_ref=None):
    def __init__(self, *args, **kwargs):

        super(DataColumn, self).__init__(*args, **kwargs)

        # self._slices = {}
        # self._slices['y'] = self._slices_y
        # self._slices['time_rec'] = self._slices_time_rec
        self.df = None
        # todo: mongo needed in database? how to handle this when reopening the file?
        self._current_time = 0
        self.logger = config.logger
        # todo: mongodb what about here?
        # set attr from data_types_dict.json
        #for attr in config.data_types_dict[self.data_type]:
        #    setattr(self, attr, config.data_types_dict[data_type][attr])

    def _init(self, df):

        self.df = df

    # ~ def __str__(self):

        # ~ # ToDos: ideas: file size, device, person, ...
        # ~ format_str = 'DataColumn(hash_long={}, slices={}, samples={}, file_size={})'
        # ~ return format_str.format(
                                # ~ self.hash_long,
                                # ~ self.slices,
                                # ~ self.samples,
                                # ~ self.file_compressed_size
                                # ~ )

    def __str__(self):

        # ToDos: ideas: file size, device, person, ...
        format_str = 'DataColumn(hash_long={})'
        return format_str.format(
                                self.hash_long
                                )

    @property
    def hash_long(self):

        return self.df.hash_id + '/' + self.data_type

    @property
    def slices(self):

        return len(self._slices_y + self._slices_time_rec)

    @property
    def all_slices(self):
        return self._slices_y + self._slices_time_rec

    @property
    def samples(self):

        samples = 0

        # loop only slices_time_rec and take the bin_size to calc the amount of samples
        for sl in self._slices_y:
            #print(sl.bin_size)
            samples += sl.samples

        return samples

    @property
    def bin_size(self):

        size_bytes = 0
        all_slices = self._slices_y + self._slices_time_rec

        # loop all slices of any kind
        for sl in all_slices:
            #print(sl.bin_size)
            size_bytes += sl.bin_size

        return size_bytes

    @property
    def file_size(self):
        return DcHelper.file_size_str(self.bin_size)

    @property
    def compressed_size(self):

        compressed_size = 0
        all_slices = self._slices_y + self._slices_time_rec

        # loop all slices of any kind
        for sl in all_slices:
            #print(sl.bin_size)
            compressed_size += sl.compressed_size

        return compressed_size

    @property
    def file_compressed_size(self):
        return DcHelper.file_size_str(self.compressed_size)

    @property
    def compression_ratio(self):
        try:
            return round(self.bin_size / self.compressed_size, 1)
        except ZeroDivisionError:
            return 1

    @property
    def dtype_time(self):
        return config.data_types_dict[self.data_type]['dtype_time']

    @property
    def dtype_size(self):
        return DcHelper.helper_dtype_size(self.dtype)

    @property
    def dtype_time_size(self):
        return DcHelper.helper_dtype_size(self.dtype_time)

    def _slice_append(self, slice_type):
        '''
        appends a new slice DataFileColumnSlice
        generate new slice_hash with length 8
        slice_type could be: y, time_rec
        adds new entry in
            self.slices_y[slice_time_offset] = slice_hash
            self.slices_time_rec[slice_time_offset] = slice_hash
        '''
        pass

    @property
    def chunks_x(self):

        chunks_x = []

        for chunk in self.df.chunks:
            if chunk.date_time_end:
                chunks_x.append(chunk.date_time_end)

        return chunks_x

    @property
    def chunks_y(self):

        chunks_y = []

        for chunk in self.df.chunks:
            if chunk.date_time_end:

                try:
                    if self.data_type == 'spo2':
                        chunks_y.append(chunk.cols[self.data_type].max)
                    elif self.data_type == 'perfusion_index_ir':
                        chunks_y.append(chunk.cols[self.data_type].min)
                    else:
                        chunks_y.append(chunk.cols[self.data_type].median)
                # chunk col does not exist in this chunk
                except KeyError:
                    chunks_y.append(None)

        return chunks_y

    @property
    def x(self):

        time_rec = []

        if self.time_slices_ref is None:

            for sl in self._slices_time_rec:
                time_rec += sl.values
        else:

            col = self.df.cols[self.time_slices_ref]

            for sl in col._slices_time_rec:
                time_rec += sl.values

        if config.operating_system == 'Windows' or config.numpy_size == 'maximize':
            return np.asarray(time_rec, dtype='float64')
        else:
            return np.asarray(time_rec, dtype=self.dtype_time)

    @property
    def y(self):

        y = []

        for sl in self._slices_y:
            y += sl.values

        if config.operating_system == 'Windows' or config.numpy_size == 'maximize':
            # this will return in all data types being 64 bit float, but it works on windows
            # https://stackoverflow.com/questions/38314118/overflowerror-python-int-too-large-to-convert-to-c-long-on-windows-but-not-ma
            if 'uint' in self.dtype:
                dtype = 'uint64'
            elif 'int' in self.dtype:
                dtype = 'int64'
            else:
                dtype = 'float64'

            return np.asarray(y, dtype=dtype)

        else:
            if self.dtype == 'uint24':
                dtype = 'uint32'
            elif self.dtype == 'int24':
                dtype = 'int32'
            else:
                dtype = self.dtype

            return np.asarray(y, dtype=dtype)

    @property
    def time_rec(self, start=None, end=None, duration=None):
        # returns slices_time_rec
        pass

    @property
    def x_min(self, start=None, end=None, duration=None):
        # returns time_rec in units min
        pass

    @property
    def x_hours(self, start=None, end=None, duration=None):
        # returns time_rec in units hours
        pass

    def __slicing(self, start=None, end=None, duration=None):
        '''
        for all of the data returs (x, y, time_rec, ...) you could provide params: start, end, duration
        in this method it's handled and the needed slices gets selected and loaded to finally return the data
        '''
        pass

    def csv_export(self):
        # interpolation
        pass

    def csv_import(self):
        # interpolation
        pass

    def append_value_list(self, value_list, time_rec_list):

        # todo: check this in df?
        if len(value_list) != len(time_rec_list):
            self.logger.error('Length of value_list is not the same as length of time_rec_list')
            return

        for value, time_rec_value in zip(value_list, time_rec_list):
            self.append_value(value, time_rec_value)

    def append_value(self, value, time_rec):
        # cast values

        if 'int' in self.dtype:
            value = round(value, 0)

        self._current_time = time_rec

        if self._slices_y == [] or self._slices_y[-1].bin_size >= self.df.slice_max_size:
            self._initiate_new_slice('y')
        self._slices_y[-1].append(value)

        # add time slices only if there is no time_slices reference (combined_columns)
        if self.time_slices_ref is None:
            if self._slices_time_rec == [] or self._slices_time_rec[-1].bin_size >= self.df.slice_max_size:
                self._initiate_new_slice('time_rec')
            self._slices_time_rec[-1].append(time_rec)

    def append_binary(self, byte_values, time_rec):

        # set the current time to None then the times can be determined later in final_analyze
        self._current_time = time_rec[0]

        # deal with y bytes first
        if not self._slices_y:
            self._initiate_new_slice('y')

        # y data first...

        buffer_size = len(self._slices_y[-1]._values_bin) if self._slices_y[-1]._values_bin else 0
        current_slice_length_y = self._slices_y[-1].values_write_pointer * self.dtype_size + buffer_size

        # bytes still fit into this slice
        if len(byte_values) + current_slice_length_y <= self.df.slice_max_size:
            self._slices_y[-1].append_binary(byte_values)

        # bytes have to be split
        else:
            # fill the first slice until it's full
            remaining_sl_bytes = self.df.slice_max_size - current_slice_length_y
            self._slices_y[-1].append_binary(byte_values[:remaining_sl_bytes])

            # use remaining values to fill up more slices
            current_time_samples = int(remaining_sl_bytes / self.dtype_size)
            iterations = DcHelper.get_number_of_iterations(byte_values[remaining_sl_bytes:], self.df.slice_max_size)
            for i in range(iterations):
                self._current_time = time_rec[current_time_samples]
                self._initiate_new_slice('y')
                bytes_in_this_iteration = byte_values[remaining_sl_bytes + i*self.df.slice_max_size : remaining_sl_bytes + (i + 1) * self.df.slice_max_size]
                self._slices_y[-1].append_binary(bytes_in_this_iteration)
                current_time_samples += int(len(bytes_in_this_iteration) / self.dtype_size)

        # now deal with time_rec if this is no combined col...

        if self.time_slices_ref is None:
            # time comes as float values, not as bytes!

            self._current_time = time_rec[0]
            if not self._slices_time_rec:
                self._initiate_new_slice('time_rec')

            current_slice_length_time_rec = len(self._slices_time_rec[-1].values)
            slice_max_samples = int(self.df.slice_max_size / self.dtype_time_size)

            # bytes still fit into this slice
            if len(time_rec) + current_slice_length_time_rec <= slice_max_samples:
                self._slices_time_rec[-1].extend(time_rec)

            # bytes have to be split
            else:
                # fill the first slice until it's full
                remaining_sl_samples = slice_max_samples - current_slice_length_time_rec
                self._slices_time_rec[-1].extend(time_rec[:remaining_sl_samples])

                # use remaining values to fill up more slices
                iterations = DcHelper.get_number_of_iterations(time_rec[remaining_sl_samples:], slice_max_samples)
                for i in range(iterations):
                    samples_in_this_iteration = time_rec[remaining_sl_samples + i * slice_max_samples : remaining_sl_samples + (i + 1) * slice_max_samples]
                    self._current_time = samples_in_this_iteration[0]
                    self._initiate_new_slice('time_rec')
                    self._slices_time_rec[-1].extend(samples_in_this_iteration)

    def write_appended_binaries(self, skip_time=False):

        for sl in self._slices_y:
            if not sl.status_slice_full and sl.binaries_appended:
                sl.write_appended_binaries()
        if not skip_time:
            # time slices don't get appended as binaries but as normal sl.values...
            for sl in self._slices_time_rec:
                if not sl.check_and_set_status_slice_full:
                    sl.write_bin()

    def _initiate_new_slice(self, slice_type):

        if slice_type == 'y':
            dtype = self.dtype
        else:
            dtype = config.data_types_dict[self.data_type]['dtype_time']

        sl = DataSlice()
        sl.data_type = self.data_type
        sl.dtype = dtype
        sl.slice_type = slice_type
        sl.slice_time_offset = self._current_time
        sl._init(df=self.df)

        if slice_type == 'y':
            self._slices_y.append(sl)
        elif slice_type == 'time_rec':
            self._slices_time_rec.append(sl)

        # self._slices[slice_type].append(sl)

    def set_values(self, value_list, time_rec_list, store=True, compression=False, finalize=True):
        # just set the whole list of values
        # cast values
        # define when to start a new slice: file_size vs amount of files

        # todo: mongodb adjustments
        # ToDo: all validation (length, type...) here or in df?

        if len(value_list) != len(time_rec_list):
            self.logger.error('value_list and time_rec_list not of same length. Aborting set_values()...')
            return

        # round data_types with integers correctly => otherwise 1.9 will become 1 during integer conversion to binary
        if 'int' in self.dtype:
            value_list = [round(value, 0) for value in value_list]


        value_samples_per_slice = int(self.df.slice_max_size / self.dtype_size)
        time_dtype_size = DcHelper.helper_dtype_size(config.data_types_dict[self.data_type]['dtype_time'])
        time_samples_per_slice = int(self.df.slice_max_size / time_dtype_size)

        # this will append the values to the last slice instead of generating a new slice
        if not finalize and self._slices_y != []:

            # Todo: check consistency and speed (currently almost 10x slower)
            #      - if set_values(... finalize=True) and then set_values(... finalize=False) => sample in last
            #      time_rec is too big

            # y-slice

            samples_left_in_last_slice = int((self.df.slice_max_size - self._slices_y[-1].bin_size) / self.dtype_size)

            # the value_list does not completely fill the last slice
            if len(value_list) <= samples_left_in_last_slice:
                self._slices_y[-1].extend(value_list)

            # new slices need to be created
            else:

                # fill up last slice
                self._slices_y[-1].extend(value_list[0:samples_left_in_last_slice])

                # put remaining samples in new slice(s)
                value_list = value_list[samples_left_in_last_slice:]

                iterations = DcHelper.get_number_of_iterations(value_list, value_samples_per_slice)
                for i in range(iterations):
                    self._current_time = time_rec_list[i * value_samples_per_slice]
                    self._initiate_new_slice('y')
                    self._slices_y[-1].extend(value_list[i * value_samples_per_slice:(i + 1) * value_samples_per_slice])
                    self._slices_y[-1].check_and_set_status_slice_full()

            # time_rec - assumtion: y and time_rec have same length

            samples_left_in_last_slice = int((self.df.slice_max_size - self._slices_time_rec[-1].bin_size) / time_dtype_size)

            if len(time_rec_list) <= samples_left_in_last_slice:
                self._slices_time_rec[-1].extend(time_rec_list)

            else:
                self._slices_time_rec[-1].extend(time_rec_list[0:samples_left_in_last_slice])
                time_rec_list = time_rec_list[samples_left_in_last_slice:]

                iterations = DcHelper.get_number_of_iterations(time_rec_list, time_samples_per_slice)
                for i in range(iterations):
                    self._current_time = time_rec_list[i * time_samples_per_slice]
                    self._initiate_new_slice('time_rec')
                    self._slices_time_rec[-1].extend(time_rec_list[i * time_samples_per_slice:(i + 1) * time_samples_per_slice])
                    self._slices_time_rec[-1].check_and_set_status_slice_full()

        else:
            # slices_y
            iterations = DcHelper.get_number_of_iterations(value_list, value_samples_per_slice)
            for i in range(iterations):
                # ToDo check that slice_time_offset works for a dtype like unit16, which has only 2 bytes vs 4 bytes of flaot32 time_rec
                self._current_time = time_rec_list[i*value_samples_per_slice]
                self._initiate_new_slice('y')
                self._slices_y[-1].values_write_bin(value_list[i * value_samples_per_slice:(i + 1) * value_samples_per_slice], store=store, compression=compression)
                self._slices_y[-1].check_and_set_status_slice_full()

            # slices_time_rec
            iterations = DcHelper.get_number_of_iterations(time_rec_list, time_samples_per_slice)
            for i in range(iterations):
                self._current_time = time_rec_list[i*time_samples_per_slice]
                self._initiate_new_slice('time_rec')
                self._slices_time_rec[-1].values_write_bin(time_rec_list[i * time_samples_per_slice:(i + 1) * time_samples_per_slice], store=store, compression=compression)
                self._slices_time_rec[-1].check_and_set_status_slice_full()

    def write_bin(self):

        # loop all slices of any kind and write binary data
        for sl in self._slices_y:
            # Slices can only have the attribute finally_analyzed once they are full
            # and all their data was already stored. With this check an unnecessary
            # lazy_load can be avoided. Other wise even fully written slices would be lazy_loaded
            if not sl.status_finally_analzyed and not sl.binaries_appended:
                sl.write_bin()
                # if binaries were appended, the status gets set once they are full, so there is no need to provoke a lazy_load
                if not sl.status_slice_full:
                    sl.check_and_set_status_slice_full()
        for sl in self._slices_time_rec:
            if not sl.status_finally_analzyed:
                sl.write_bin()
                # there are no binaries to append for time values, so here it does not matter
                if not sl.status_slice_full:
                    sl.check_and_set_status_slice_full()

    def compress(self, algorithm='zstd', level=2):

        # loop all slices of any kind and compress and write data
        for sl in self._slices_y:
            sl.compress(algorithm=algorithm, level=level)
        for sl in self._slices_time_rec:
            sl.compress(algorithm=algorithm, level=level)

    def send_old(self, server, session, partially=False):

        # loop all slices of any kind and send binary data

        for sl in self._slices_y:
            if partially:
                sl.send_partially_old(server, session)
            else:
                sl.send_old(server, session)

        for sl in self._slices_time_rec:
            if partially:
                sl.send_partially_old(server, session)
            else:
                sl.send_old(server, session)

    def send(self, session, partially=False):

        # loop all slices of any kind and send binary data
        for sl in self._slices_y + self._slices_time_rec:
            sl.send(session, partially)

    # ToDo implement lazy load
    #  purpose of this? fill sl._vlaues? return some values?
    # def load(self, slice_type):
    #
    #    for sl in self._slices[slice_type]:
    #        sl.lazy_load()

    def dump(self):

        self.final_analyse()

        #todo remove / change this for mongo
        dump_dic = {
                    'm': dict(self.meta)
                    }

        for slice_type in self._slices:
            dump_dic[slice_type] = []
            for sl in self._slices[slice_type]:
                dump_dic[slice_type].append(sl.dump())
                # remove some redundant info
                del dump_dic[slice_type][-1]['df.hash_id']
                del dump_dic[slice_type][-1]['data_type']
                del dump_dic[slice_type][-1]['dtype']
                del dump_dic[slice_type][-1]['slice_type']

        return dump_dic

    def final_analyse(self):

        min_list = []
        max_list = []
        values_list = []
        mean_sum = 0
        samples_sum = 0

        self.logger.debug(f'final_analyse {self}')

        if self._slices_y:

            if config.data_types_dict[self.data_type]['box_plot']:

                # except for acc this is probably only one slice
                for sl in self._slices_y:

                    sl.final_analyse()

                    values_list.extend(sl.values)
                    if sl.min:
                        min_list.append(sl.min)
                    if sl.max:
                        max_list.append(sl.max)
                    if sl.mean:
                        mean_sum += sl.mean*sl.samples_meta
                    samples_sum += sl.samples_meta

                length = len(values_list)

                if length != 0:
                    values_list.sort()
                    half = int(length / 2)
                    first_quarter = int(length / 4)
                    third_quarter = int(length * 3 / 4)

                    # cast to normal Python int/float to avoid incompatibilities with mongoengine floatfileds...
                    self.median = round(float(values_list[half]), 2)
                    self.upper_quartile = round(float(values_list[third_quarter]), 2)
                    self.lower_quartile = round(float(values_list[first_quarter]), 2)
                    if min_list:
                        self.min = round(float(min(min_list)), 2)
                    if max_list:
                        self.max = round(float(max(max_list)), 2)
                    self.samples_meta = samples_sum
                    try:
                        self.mean = round(float(mean_sum / samples_sum), 2)  # weighted average
                    except ZeroDivisionError:
                        self.mean = None
                else:
                    self.median = None
                    self.upper_quartile = None
                    self.lower_quartile = None
                    self.min = None
                    self.max = None
                    self.samples_meta = 0
                    self.mean = None

            # big data like ppg with many slices
            else:

                for sl in self._slices_y:

                    sl.final_analyse()
                    samples_sum += sl.samples_meta

                self.samples_meta = samples_sum

        # time_rec: find the duration + samples
        if self._slices_time_rec:

            if self._slices_time_rec[-1].values:
                self.duration = float(max(self._slices_time_rec[-1].values))
            else:
                self.duration = 0

            for sl in self._slices_time_rec:

                sl.final_analyse()
                samples_sum += sl.samples_meta

        # compressed size:
        self.compressed_size_meta = self.compressed_size
        self.compression_ratio_meta = self.compression_ratio

        # self.logger.debug(
        #     f'dc.final_analyse(): '
        #     f'data_type: {self.data_type}, med {self.median}, up_qt {self.upper_quartile}, l_qt {self.lower_quartile}, min {self.min}, max {self.max}, samples_meta {self.samples_meta}, '
        #     f'mean {self.mean}, dur: {self.duration}, compressed_s_meta {self.compressed_size_meta} , compr_ratio_meta {self.compression_ratio_meta}'
        # )

