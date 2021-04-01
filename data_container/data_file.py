import random
import string
import os
from pathlib import Path
from terminaltables import AsciiTable
import json
import hashlib
import zstd
import gzip
import requests
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import time
import concurrent.futures
import threading
import psutil
import traceback


from mongoengine import *
import mongoengine
from mongoengine.queryset import OperationError

# import package modules
from .data_chunk import DataChunk, ChunkTimeError, ChunkNoValuesError
from .data_column import DataColumn
from .document_tweak import DocumentTweak
from .dc_helper import DcHelper, InstancesContainer

utc_to_local = DcHelper.utc_to_local
from . import config


class DataFile(DocumentTweak):
    # Sphinx Docstring format
    """[Summary]

    :param [ParamName]: [ParamDescription], defaults to [DefaultParamVal]
    :type [ParamName]: [ParamType](, optional)
    ...
    :raises [ErrorType]: [ErrorDescription]
    ...
    :return: [ReturnDescription]
    :rtype: [ReturnType]
    """

    # id
    # Note: according to this post, the parent class DocumentTweak needs to have
    #  the same primary key. Therefore it is defined in DocumentTweak
    #  https://stackoverflow.com/questions/59699716/valueerror-cannot-override-primary-key-field
    # _hash_id = StringField(primary_key=True)

    # relationships
    scope = ReferenceField('Scope')
    project = ReferenceField('Project', required=True)
    person = ReferenceField('Person', required=True)
    owner = ReferenceField('User')
    receiver = ReferenceField('Receiver')
    device = ReferenceField('Device')
    df_progressed = ReferenceField('DataFile')

    # time fields
    _time_c = DateTimeField(db_field='t_c')
    _time_m = DateTimeField(db_field='t_m')

    _date_time_start = DateTimeField(db_field='dts')
    _date_time_end = DateTimeField(db_field='dte')
    _date_time_upload = DateTimeField(db_field='dtu')

    # status fields at df creation
    dc_version = IntField(default=config.DC_VERSION, required=True)
    __slice_max_size = IntField(required=True, db_field='sms')
    receiver_release = StringField(max_length=10, db_field='rr')
    device_release = StringField(max_length=10, db_field='dr')
    device_model = StringField(max_length=40, db_field='dm')
    source = StringField(db_field='src')
    import_md5 = StringField(db_field='md5')
    import_file_name = StringField(db_field='ifn')

    # status fields at last store()
    status_closed = BooleanField(default=False, db_field='stc')
    consistent = BooleanField(default=True, db_field='cons')
    duration = FloatField(db_field='dur', default=0)
    duration_netto_meta = FloatField(db_field='durm', default=0)
    samples_meta = IntField(default=0, db_field='s')
    bin_size_meta = IntField(default=0, db_field='bs')
    compressed_size_meta = IntField(default=0, db_field='cs')
    compression_ratio_meta = FloatField(default=0, db_field='cr')

    # embedded docs: cols, chunks
    combined_columns = MapField(ListField(StringField(max_length=50)), db_field='coc')
    cols = MapField(EmbeddedDocumentField('DataColumn'))
    chunks = EmbeddedDocumentListField('DataChunk')
    chunks_labelled = EmbeddedDocumentListField('DataChunk', db_field='ch_l')
    markers = EmbeddedDocumentListField('DataChunk', db_field='mrkr')

    # other
    comments = ListField(EmbeddedDocumentField('Comment'), db_field='com')
    _afe_config = ListField(db_field='afe_c')

    # ToDos: coming later...
    # ble_disconnects = ???
    # events = ??? -> markers during recording
    # dc_version_log
    # afe_config
    # acc_config

    def __init__(self, *args, **kwargs):

        super(DataFile, self).__init__(*args, **kwargs)
        self.logger = config.logger
        # super(DataFile, self).__init__()

        self.__set_slice_max_size()
        self._init()

    def __str__(self):

        # return 'banana'
        # ToDos: ideas: file size, device, person, ...
        format_str = 'DataFile(_hash_id={}, date_time_start={}, duration={}, columns={}, slices={}, samples={}, file_size={})'
        return format_str.format(
            self._hash_id,
            self.date_time_start,
            self.duration,
            self.columns,
            self.slices,
            self.samples_meta,
            DcHelper.file_size_str(self.compressed_size_meta)
        )

    def __set_slice_max_size(self):
        # this should never be changed once it is set ...

        if not self.__slice_max_size:
            self.__slice_max_size = config.SLICE_MAX_SIZE

    @property
    def slice_max_size(self):
        return self.__slice_max_size

    @property
    def dev_model(self):
        if self.device_model:
            return self.device_model
        elif self.device:
            return self.device.device_model
        else:
            return None

    def _init(self):

        # todo we never use that _status...
        self._status = 'init'
        # live_date: This influences the date_time_xxx behavior for the df and the chunks.
        # A developer with a PC uses False, but a Gateway must set live_data to True
        self.live_data = False
        # free_slices_after_finally_analysed: If True, once a slice is analyzed sl.final_analyze and the sl.finally_analyzed = True is set,
        # then the sl._values are reset to free the memory. Systems with less memory are then less likely to crash.
        self.free_slices_when_finally_analysed = False
        self._continue_recording = False
        # while _saving_db is true, writing to the database (df.write_appended_binaries and df.store())
        # are not allowed. The intention is to avoid data being stored during the send process which might create a
        # conflict in the database.
        # todo: didn't proof useful for the intended problem... maybe remove this again...
        self._saving_db = False
        # avoid loading y slices (only used when appending binaries)
        self._avoid_lazy_load = False
        self.server = None
        self.api_client = None
        self.c = None

        # initiate slices after loading from database
        for data_type in self.cols:

            self.cols[data_type]._init(df=self)

            for sl in self.cols[data_type]._slices_y:
                sl._init(df=self)
            for sl in self.cols[data_type]._slices_time_rec:
                sl._init(df=self)

        # initiate data chunks
        for chunk in self.chunks:
            chunk._init(df=self)

        # initiate labelled chunks
        for chunk in self.chunks_labelled:
            chunk._init(df=self)

        # initiate markers
        for chunk in self.markers:
            chunk._init(df=self)

        # make df.c.heart_rate.x etc. work
        self.update_attributes()

    def all_slices(self, y_only=False, time_only=False):
        # return all slices to make it easier to iterate

        sl_list = []
        for col in self.cols.values():
            if y_only:
                slices = col._slices_y
            elif time_only:
                slices = col._slices_time_rec
            else:
                slices = col._slices_y + col._slices_time_rec

            for sl in slices:
                sl_list.append(sl)

        return sl_list

    @property
    def producer_hash(self):
        return self.hash_id.split('.')[0]

    @property
    def time_c(self):
        return utc_to_local(self._time_c, self.project.timezone)

    @property
    def time_c_date(self):
        return str(self.time_c).split(' ')[0]

    @property
    def time_m(self):
        return utc_to_local(self._time_m, self.project.timezone)

    @property
    def date_time_start(self):
        if self.project and self.project.timezone:
            tz = self.project.timezone
        else:
            tz = None

        return utc_to_local(self._date_time_start, tz)

    @property
    def date_time_start_date(self):
        return str(self.date_time_start).split(' ')[0]

    @date_time_start.setter
    def date_time_start(self, datetime_new):
        if self.project and self.project.timezone:
            tz = self.project.timezone
        else:
            tz = 'UTC'
        self._date_time_start = DcHelper.datetime_validation(datetime_new, tz)

    @property
    def date_time_end(self):
        return utc_to_local(self._date_time_end, self.project.timezone)

    @date_time_end.setter
    def date_time_end(self, datetime_new):
        if self.project and self.project.timezone:
            tz = self.project.timezone
        else:
            tz = 'UTC'
        self._date_time_end = DcHelper.datetime_validation(datetime_new, tz)

    @property
    def date_time_upload(self):
        return utc_to_local(self._date_time_upload, self.project.timezone)

    @date_time_upload.setter
    def date_time_upload(self, datetime_new):
        # todo: make sure datetime_new is UTC?
        self._date_time_upload = datetime_new

    @property
    def samples_str(self):

        if self.samples_meta is None:
            return 'na'

        elif self.samples_meta >= 1000000:
            return str(round(self.samples_meta / 1000000, 1)) + ' M'

        elif self.samples_meta >= 1000:
            return str(round(self.samples_meta / 1000, 1)) + ' K'

        else:
            return str(self.samples_meta)

    def store(self, final_analyse=True):

        if self._hash_id:

            # don't store while saving db to avoid db conflicts
            while self._saving_db:
                self.logger.debug(f'{self.hash_id}: while loop in store: waiting for db save to finish')
                time.sleep(0.1)

            # this must happen before save_changes() otherwise the new pointers will get lost
            for data_type in self.cols:
                self.cols[data_type].write_bin()
                # only write y since time_rec was just stored in the line above
                self.cols[data_type].write_appended_binaries(skip_time=True)

            self.save_changes(final_analyse=final_analyse)

        else:

            self.save(final_analyse=final_analyse)

            # todo: nicer way for this?
            #  check if values need to be written
            #  if yes => write and save_changes to avoid loss of pointers

            for data_type in self.cols:
                self.cols[data_type].write_bin()
            if self.cols:
                self.save_changes(final_analyse=final_analyse)

    def save(self, final_analyse=True, *args, **kwargs):

        if final_analyse:
            self.final_analyse()

        # avoid that somebody calls df.save over and over again which would create lots of new folders...
        if self._hash_id:
            self.logger.warning(
                f'df.save() called on {self.hash_id}. '
                f'Do not call df.save on a data file with an existing hash_id. '
                f'Use df.store() or df.save_changes() in that case.')
            self.save_changes(final_analyse=final_analyse)
            return	

        first_try = True
        while True:

            try:

                # set unique _hash_id
                if config.producer_hash:
                    self._hash_id = config.producer_hash + '.' + config.generate_hash(6)
                else:
                    self._hash_id = config.generate_hash(6)

                # set time_c
                self._time_c = datetime.now(timezone.utc)
                self._time_m = datetime.now(timezone.utc)
                # call save() of super; ensure uniqueness with force_insert
                super(DataFile, self).save(force_insert=True, *args, **kwargs)
                # break if there is no NotUniqueError
                self._status = 'stored'
                self.logger.info(f'created new DataFile with hash {self._hash_id}')
                break

            except mongoengine.errors.NotUniqueError as e:
                self.logger.debug(e)

            except OperationError as e:

                if first_try:
                    super(DataFile, self).save(force_insert=True, remove_keys=True, *args, **kwargs)
                    first_try = False
                else:
                    # up to this point there will be no single datapoint in this file
                    # => nothing to save. Just raise the exception.
                    self.logger.error(f'OperationError for df with no hash was not fixed the first time. Raising error.')
                    raise e

        # create directory
        if not self.path.exists():
            self.path.mkdir()

    def save_changes(self, final_analyse=True, *args, **kwargs):

        # this is not needed for example when only pointers change
        if final_analyse:
            self.final_analyse()

        # set time_m
        self._time_m = datetime.now(timezone.utc)
        self._status = 'stored'

        self._saving_db = True
        try:

            try:
                super(DataFile, self).save(*args, **kwargs)
            except OperationError:
                self.logger.warning(f'save_changes OperationError: trying to remove keys...')
                super(DataFile, self).save(remove_keys=True, *args, **kwargs)

        except Exception as e:
            # something goes wrong
            self.logger.error(f'df.save_changes error: {e}')
            self.logger.error(str(traceback.format_exc()))
            df_json = self.to_json()
            file_name = f'error_df_{datetime.now(tz=timezone.utc)}.json'
            storage_path = str(config.df_path / str(self.hash_id) / file_name)
            with open(storage_path, 'w') as fp:
                fp.write(df_json)
                fp.flush()
                os.fsync(fp)

            raise e

        self._saving_db = False

    def update_attributes(self):

        # self.meta = AttributesContainer(self._meta)
        self.c = InstancesContainer(self.cols)

    @property
    def avoid_lazy_load(self):
        return self._avoid_lazy_load

    @avoid_lazy_load.setter
    def avoid_lazy_load(self, mode):
        # this is typically used when the Gateway is appending binaries, therefore y slices are treated as binares
        if mode:
            self._avoid_lazy_load = True
            # todo
            for data_type in self.cols:
                for sl in self.cols[data_type]._slices_y:
                    sl._values_bin = bytearray()
        else:
            self._avoid_lazy_load = False

    @property
    def path(self):

        if self._hash_id:
            return config.df_path / Path(self._hash_id)
        else:
            self.logger.warning('df.path does not yet exist. call df.save() first')
            return None

    @property
    def hash_short(self):

        if self._hash_id:
            return self._hash_id.split('.')[-1]

        return None

    @property
    def hash_id(self):

        return self._hash_id

    @property
    def columns(self):

        return len(self.cols)

    @property
    def duration_netto(self):
        # summs up all time values of each data chunk to exclude the recording gaps
        duration = 0
        for chunk in self.chunks:
            if chunk.finalized:
                duration += chunk.duration

        return round(duration, 1)

    @property
    def duration_str(self):

        return DcHelper.seconds_to_time_str(self.duration)

    @property
    def duration_netto_str(self):

        return DcHelper.seconds_to_time_str(self.duration_netto)

    @property
    def duration_netto_meta_str(self):

        return DcHelper.seconds_to_time_str(self.duration_netto_meta)

    @property
    def last_time_val(self):
        # return the highes time value of all data points

        last_val = 0

        for col in self.cols.values():
            if col._slices_time_rec:
                if col._slices_time_rec[-1].values:
                    if col._slices_time_rec[-1].values[-1] > last_val:
                        last_val = col._slices_time_rec[-1].values[-1]

        # convert to python float (for example timedelta does not accept numpy float)
        return float(last_val)

    @property
    def stats_json(self):

        samples_real_x_total = 0
        samples_real_y_total = 0
        values_write_pointer_total = 0

        columns = {}
        for data_type in self.cols:
            col = self.cols[data_type]
            data_type = col.data_type

            samples_real_x = 0
            samples_real_y = 0
            values_write_pointer = 0

            # first should be checked if slice is combined. for that we check if there is a col.time_slices_ref
            if col.time_slices_ref:
                for sl_x in self.cols[col.time_slices_ref]._slices_time_rec:
                    samples_real_x += sl_x.samples
                    values_write_pointer += sl_x.values_write_pointer
            else:
                for sl_x in self.cols[data_type]._slices_time_rec:
                    samples_real_x += sl_x.samples
                    values_write_pointer += sl_x.values_write_pointer
            samples_real_x_total += samples_real_x

            for sl_y in self.cols[data_type]._slices_y:
                samples_real_y += sl_y.samples
                values_write_pointer += sl_y.values_write_pointer
            samples_real_y_total += samples_real_y
            values_write_pointer_total += values_write_pointer

            try:
                percentage_upload = round(((samples_real_y + samples_real_x) / values_write_pointer) * 100, 1)
            except ZeroDivisionError:
                percentage_upload = 0.0

            samples_real_x_str = 'ok' if samples_real_x == col.samples_meta else str(samples_real_x)
            samples_real_y_str = 'ok' if samples_real_y == col.samples_meta else str(samples_real_y)

            columns[data_type] = {}
            if col.time_slices_ref:
                columns[data_type]['data_type_str'] = str(data_type) + ' (comb)'
                columns[data_type]['slices'] = col.slices
                columns[data_type]['samples_real_x'] = samples_real_x_str + '⁺'
                columns[data_type]['samples_real_y'] = samples_real_y_str
                columns[data_type]['samples_meta'] = col.samples_meta
                columns[data_type]['dtype'] = col.dtype
                columns[data_type]['file_size'] = col.file_compressed_size
                columns[data_type]['compression_ratio'] = col.compression_ratio
                columns[data_type]['percentage_upload'] = str(percentage_upload) + '⁺'
            else:
                columns[data_type]['data_type_str'] = str(data_type)
                columns[data_type]['slices'] = col.slices
                columns[data_type]['samples_real_x'] = samples_real_x_str
                columns[data_type]['samples_real_y'] = samples_real_y_str
                columns[data_type]['samples_meta'] = col.samples_meta
                columns[data_type]['dtype'] = col.dtype
                columns[data_type]['file_size'] = col.file_compressed_size
                columns[data_type]['compression_ratio'] = col.compression_ratio
                columns[data_type]['percentage_upload'] = str(percentage_upload)
        try:
            percentage_upload_total = round(((samples_real_y_total + samples_real_x_total) / values_write_pointer_total) * 100, 1)
        except ZeroDivisionError:
            percentage_upload_total = 0.0

        samples_real_x_total_str = 'ok' if samples_real_x_total == self.samples_meta else str(samples_real_x_total)
        samples_real_y_total_str = 'ok' if samples_real_y_total == self.samples_meta else str(samples_real_y_total)

        data_file = {
            'hash': self.hash_id,
            'slices': self.slices,
            'samples_meta': self.samples_meta,
            'samples_real_x_total': samples_real_x_total_str,
            'samples_real_y_total': samples_real_y_total_str,
            'compression_ratio': self.compression_ratio,
            'file_size': self.file_compressed_size,
            'columns': columns,
            'percentage_upload_total': percentage_upload_total
        }

        return json.dumps(data_file)

    @property
    def stats(self):

        data_file = json.loads(self.stats_json)

        table_data = []
        space = '--------'
        header = ['data type', 'slices', 'samples meta', 'samples real x', 'samples real y', 'dtype', 'file_size',
                  'comp. ratio', '%upload']

        row = []
        for item in header:
            row.append(item)
        table_data.append(row)

        for data_type in data_file['columns']:
            col = data_file['columns'][data_type]

            row = []

            row.append(col['data_type_str'])
            row.append(col['slices'])
            row.append(col['samples_meta'])
            row.append(col['samples_real_x'])
            row.append(col['samples_real_y'])
            row.append(col['dtype'])
            row.append(col['file_size'])
            row.append(col['compression_ratio'])
            row.append(col['percentage_upload'])
            table_data.append(row)

        row = []
        for _ in header:
            row.append(space)
        table_data.append(row)

        row = []
        row.append('data_file sum')
        row.append(data_file['slices'])
        row.append(data_file['samples_meta'])
        row.append(data_file['samples_real_x_total'])
        row.append(data_file['samples_real_y_total'])
        row.append('')
        row.append(data_file['file_size'])
        row.append(data_file['compression_ratio'])
        row.append(data_file['percentage_upload_total'])
        table_data.append(row)

        return AsciiTable(table_data).table

    @property
    def stats_slices_json(self):

        data_file = {
            'hash': self.hash_id,
            'slices': self.slices,
            'samples_meta': self.samples_meta,
            'samples': '',
            'compression_ratio': self.compression_ratio,
            'file_size': self.file_compressed_size,
            'percentage_upload_total': None,
            'columns': {}
        }

        values_write_pointer_total = 0
        samples_total = 0
        for data_type in self.cols:

            col = self.cols[data_type]
            data_file['columns'][data_type] = {}
            data_file['columns'][data_type]['slices'] = col.slices
            data_file['columns'][data_type]['samples'] = col.samples
            data_file['columns'][data_type]['dtype'] = col.dtype
            data_file['columns'][data_type]['file_size'] = col.file_compressed_size
            data_file['columns'][data_type]['compression_ratio'] = col.compression_ratio

            data_file['columns'][data_type]['slices'] = []
            all_slices = self.cols[data_type]._slices_y + self.cols[data_type]._slices_time_rec

            for sl in all_slices:

                values_write_pointer_total += sl.values_write_pointer
                samples_total += sl.samples

                try:
                    percentage_upload = round((sl.samples / sl.values_write_pointer) * 100, 1)
                except ZeroDivisionError:
                    percentage_upload = 0.0

                if sl.values_send_pointer > sl.samples * DcHelper.helper_dtype_size(sl.dtype):
                    self.logger.error(f'slice {sl._hash}: values_send_pointer {sl.values_send_pointer} > number of real sample bytes {sl.samples * DcHelper.helper_dtype_size(sl.dtype)}')

                samples_str = 'ok' if sl.samples == sl.samples_meta else str(sl.samples)

                slice_dic = {}
                slice_dic['hash'] = sl._hash
                slice_dic['ext'] = str(sl._path.name).split('.')[-1]
                slice_dic['data_type'] = sl.data_type
                slice_dic['slice_type'] = sl.slice_type
                slice_dic['samples_meta'] = sl.samples_meta
                slice_dic['samples'] = samples_str
                slice_dic['slice_time_offset'] = round(sl.slice_time_offset, 2) if sl.slice_time_offset else sl.slice_time_offset
                slice_dic['dtype'] = sl.dtype
                slice_dic['file_size'] = sl.file_compressed_size
                slice_dic['compression_ratio'] = sl.compression_ratio
                slice_dic['file_exists'] = sl.file_exists
                slice_dic['percentage_upload'] = percentage_upload

                data_file['columns'][data_type]['slices'].append(slice_dic)

            samples_str = 'ok' if self.samples == self.samples_meta else str(self.samples)

            try:
                percentage_upload_total = round((samples_total / values_write_pointer_total) * 100, 1)
            except ZeroDivisionError:
                percentage_upload_total = 0.0

            data_file['samples'] = samples_str
            data_file['percentage_upload_total'] = percentage_upload_total

        return json.dumps(data_file)

    @property
    def stats_slices(self):

        data_file = json.loads(self.stats_slices_json)

        table_data = []
        space = '--------'
        header = ['hash', 'data type', 'slice_type', 'slice_time_offset', 'samples meta', 'samples real', 'dtype',
                  'file_size', 'comp. ratio', 'file exists', '%upload']
        row = []
        for item in header:
            row.append(item)
        table_data.append(row)

        for data_type in data_file['columns']:

            for sl in data_file['columns'][data_type]['slices']:

                row = []
                row.append((str(sl['hash'])) + '.' + sl['ext'])
                row.append(sl['data_type'])
                row.append(sl['slice_type'])
                row.append(sl['slice_time_offset'])
                row.append(sl['samples_meta'])
                row.append(sl['samples'])
                row.append(sl['dtype'])
                row.append(sl['file_size'])
                row.append(sl['compression_ratio'])
                row.append(sl['file_exists'])
                row.append(sl['percentage_upload'])
                table_data.append(row)

        row = []
        for _ in header:
            row.append(space)
        table_data.append(row)

        row = []
        row.append(data_file['hash'])
        row.append('data_file sum')
        row.append(data_file['slices'])
        row.append('')
        row.append(data_file['samples_meta'])
        row.append(data_file['samples'])
        row.append('')
        row.append((data_file['file_size']))
        row.append(data_file['compression_ratio'])
        row.append('')
        row.append(data_file['percentage_upload_total'])
        table_data.append(row)

        return AsciiTable(table_data).table

    @property
    def stats_chunks_json(self):

        data_file = {
            'hash': self.hash_id,
            'samples': self.samples,
            'samples_meta': self.samples_meta,
            'chunks': {},
            'chunks_labelled': {},
            'markers': {},
        }

        # data chunks
        for index, chunk in enumerate(self.chunks):

            samples_real_y_total = 0
            samples_real_x_total = 0
            samples_meta_total = 0

            cols = 0
            for data_type in chunk.cols:

                col = chunk.cols[data_type]
                samples_meta_total += col.samples

                if col.samples != 0:
                    cols += 1

                samples_real_y_total += len(col.y)
                samples_real_x_total += len(col.x)

            try:
                percentage_upload_total = round(
                    ((samples_real_y_total + samples_real_x_total) / (2 * samples_meta_total)) * 100, 1)
            except ZeroDivisionError:
                percentage_upload_total = 0.0

            samples_real_x_str = 'ok' if samples_real_x_total == samples_meta_total else str(samples_real_x_total)
            samples_real_y_str = 'ok' if samples_real_y_total == samples_meta_total else str(samples_real_y_total)

            data_file['chunks'][index] = {}
            data_file['chunks'][index]['index'] = chunk.index
            data_file['chunks'][index]['samples_meta_total'] = samples_meta_total
            data_file['chunks'][index]['samples_real_x'] = samples_real_x_str
            data_file['chunks'][index]['samples_real_y'] = samples_real_y_str
            data_file['chunks'][index]['columns'] = str(cols)
            time_offset = round(chunk.time_offset, 3) if chunk.time_offset else chunk.time_offset
            data_file['chunks'][index]['time_offset'] = str(time_offset)
            duration = round(chunk.duration, 3) if chunk.duration else chunk.duration
            data_file['chunks'][index]['duration'] = str(duration)
            data_file['chunks'][index]['finalized'] = chunk.finalized
            data_file['chunks'][index]['percentage_upload_total'] = percentage_upload_total

            index += 1

        # labelled chunks
        for index, chunk in enumerate(self.chunks_labelled):

            samples_meta_total = 0
            samples_real_x_total = 0
            samples_real_y_total = 0

            cols = 0
            for data_type in chunk.cols:

                col = chunk.cols[data_type]
                samples_meta_total += col.samples

                if col.samples != 0:
                    cols += 1

                samples_real_y_total += len(col.y)
                samples_real_x_total += len(col.x)

            try:
                percentage_upload_total = round(
                    ((samples_real_y_total + samples_real_x_total) / (2 * samples_meta_total)) * 100, 1)
            except ZeroDivisionError:
                percentage_upload_total = 0.0

            samples_real_x_str = 'ok' if samples_real_x_total == samples_meta_total else str(samples_real_x_total)
            samples_real_y_str = 'ok' if samples_real_y_total == samples_meta_total else str(samples_real_y_total)

            data_file['chunks_labelled'][index] = {}
            data_file['chunks_labelled'][index]['index'] = chunk.index
            data_file['chunks_labelled'][index]['samples_meta_total'] = samples_meta_total
            data_file['chunks_labelled'][index]['samples_real_x'] = samples_real_x_str
            data_file['chunks_labelled'][index]['samples_real_y'] = samples_real_y_str
            data_file['chunks_labelled'][index]['columns'] = str(cols)
            time_offset = round(chunk.time_offset, 2) if chunk.time_offset else chunk.time_offset
            data_file['chunks_labelled'][index]['time_offset'] = str(time_offset)
            duration = round(chunk.duration, 2) if chunk.duration else chunk.duration
            data_file['chunks_labelled'][index]['duration'] = str(duration)
            data_file['chunks_labelled'][index]['finalized'] = chunk.finalized
            data_file['chunks_labelled'][index]['label'] = chunk.label
            data_file['chunks_labelled'][index]['percentage_upload_total'] = percentage_upload_total

            index += 1

        # markers
        for index, chunk in enumerate(self.markers):

            data_file['markers'][index] = {}
            data_file['markers'][index]['index'] = chunk.index
            time_offset = round(chunk.time_offset, 2) if chunk.time_offset else chunk.time_offset
            data_file['markers'][index]['time_offset'] = str(time_offset)
            data_file['markers'][index]['label'] = chunk.label

            index += 1

        return json.dumps(data_file)

    @property
    def stats_chunks(self):

        data_file = json.loads(self.stats_chunks_json)

        table_data = []

        space = '--------'
        header = ['index', 'chunk_type', 'time_offset', 'duration', 'samples meta', 'samples real x', 'samples real y',
                  'cols with data', 'label', 'finalized', '%upload']

        row = []
        for item in header:
            row.append(item)
        table_data.append(row)

        # data chunks
        if data_file['chunks']:

            row = []
            row.append('Data')
            row.append(f'Total: {len(data_file["chunks"])}')
            table_data.append(row)

            for chunks in data_file['chunks']:
                chunk = data_file['chunks'][chunks]

                row = []
                row.append(str(chunk['index']))
                row.append('data_chunk')
                row.append(chunk['time_offset'])
                row.append(chunk['duration'])
                row.append(chunk['samples_meta_total'])
                row.append(chunk['samples_real_x'])
                row.append(chunk['samples_real_y'])
                row.append(chunk['columns'])
                row.append('-')
                row.append(str(chunk['finalized']))
                row.append(chunk['percentage_upload_total'])

                table_data.append(row)

        # labelled chunks
        if data_file['chunks_labelled']:

            row = []
            for _ in header:
                row.append(space)
            table_data.append(row)

            row = []
            row.append('Labelled')
            row.append(f'Total: {len(data_file["chunks_labelled"])}')
            table_data.append(row)

            for chunks in data_file['chunks_labelled']:
                chunk = data_file['chunks_labelled'][chunks]

                row = []
                row.append(chunk['index'])
                row.append('labelled_chunk')
                row.append(str(chunk['time_offset']))
                row.append(str(chunk['duration']))
                row.append(chunk['samples_meta_total'])
                row.append(chunk['samples_real_x'])
                row.append(chunk['samples_real_y'])
                row.append(chunk['columns'])
                row.append(chunk['label'])
                row.append('-')
                row.append(chunk['percentage_upload_total'])
                table_data.append(row)

        # markers
        if data_file['markers']:

            row = []
            for _ in header:
                row.append(space)
            table_data.append(row)

            row = []
            row.append('Markers')
            row.append(f'Total: {len(data_file["markers"])}')
            table_data.append(row)

            for chunks in data_file['markers']:
                chunk = data_file['markers'][chunks]

                row = []
                row.append(chunk['index'])
                row.append('marker')
                row.append(str(chunk['time_offset']))
                row.append('-')
                row.append('-')
                row.append('-')
                row.append('-')
                row.append('-')
                row.append(chunk['label'])
                row.append('-')
                row.append('-')
                table_data.append(row)

        return AsciiTable(table_data).table

    def chunks_to_json(self):

        chunks_list = []

        for chunk in self.chunks:
            chunks_list.append(chunk.to_json())

        return chunks_list

    @property
    def bin_size(self):

        size_bytes = 0
        for data_type in self.cols:
            # print(self.cols[data_type].bin_size)
            size_bytes += self.cols[data_type].bin_size

        # ToDo consider shared time_slices -> ppg_ir, red, ambient with same time_rec, ...
        #       just ignore the bin_size of tim_slices if there is a ref_time_slices

        return size_bytes

    @property
    def slices(self):

        slices = 0
        for data_type in self.cols:
            # print(self.cols[data_type].bin_size)
            slices += self.cols[data_type].slices

        return slices

    @property
    def samples(self):

        samples = 0
        for data_type in self.cols:
            # print(self.cols[data_type].bin_size)
            samples += self.cols[data_type].samples

        return samples

    @property
    def file_size(self):
        return DcHelper.file_size_str(self.bin_size)

    @property
    def compressed_size(self):

        compressed_size = 0
        for data_type in self.cols:
            # print(self.cols[data_type].bin_size)
            compressed_size += self.cols[data_type].compressed_size

        # ToDo consider shared time_slices -> ppg_ir, red, ambient with same time_rec, ...
        #       just ignore the bin_size of tim_slices if there is a ref_time_slices

        return compressed_size

    @property
    def file_compressed_size(self):

        return DcHelper.file_size_str(self.compressed_size)

    @property
    def file_compressed_size_meta(self):

        return DcHelper.file_size_str(self.compressed_size_meta)

    @property
    def compression_ratio(self):
        try:
            return round(self.bin_size / self.compressed_size, 1)
        except ZeroDivisionError:
            return 1

    @property
    def afe_config(self):
        return self._afe_config

    # ToDo: do we still need checksum?
    def append_value(self, data_type, value, time_rec):

        # todo: catch too high int/float? values

        if not self.date_time_start and self.live_data:
            self._date_time_start = datetime.now(timezone.utc)
        elif not self.date_time_start:
            self.logger.warning('To start appending data you need to specify date_time_start first (Timezone aware e.g. datetime(2020, 5, 4, 10, 55, 59, 3, tzinfo=timezone.utc)).')
            return False

        if not self._hash_id:
            self.save()

        if self.status_closed:
            self.logger.warning('This datafile has already been closed. append_value() is not possible.')
            return

        # check unallowed negative numbers
        if time_rec < 0:
            self.logger.warning(f'it is not possible to append a negative time_rec value {time_rec}! ({data_type}, {value}, {time_rec})')
            return False

        # all data must be part of a chunk
        if not self.chunks or self.chunks[-1].finalized:
            self.chunk_start()

        # combined cols
        if data_type in list(self.combined_columns):

            if any(isinstance(item, (list, tuple, np.ndarray, range)) for item in [time_rec]):
                self.logger.error('Cannot append list of times. Aborting append_value()')
                return False

            if not (isinstance(value, list) or isinstance(value, tuple)):
                self.logger.error(f'Cannot append value to a combined_column. It has to be a list or tuple. Aborting append_value({value})')
                return False

            if len(value) != len(self.combined_columns[data_type]):
                self.logger.error('Cannot append value to combined_column. len(value) != len(self.combined_columns[data_type]). Aborting append_value()')
                return False

            # check all values before adding to avoid uneven combined_cols
            for i, data_type_2 in enumerate(self.combined_columns[data_type]):
                if 'uint' in config.data_types_dict[data_type_2]['dtype'] and value[i] < 0:
                    self.logger.error(f'Appending a negative {data_type_2} value {value[i]} is not allowed for {config.data_types_dict[data_type_2]["dtype"]}'
                                   f'({data_type_2}, {value[i]}, {time_rec})')
                    return False
                if 'int' in config.data_types_dict[data_type_2]['dtype'] and type(value[i]) is not int:
                    value[i] = int(round(value[i]))

            for i, data_type_2 in enumerate(self.combined_columns[data_type]):
                self.cols[data_type_2].append_value(value[i], time_rec)

        # normal behavior
        else:

            if any(isinstance(item, (list, tuple, np.ndarray, range)) for item in [value, time_rec]):
                self.logger.error('Cannot append list of values. Aborting append_value()')
                return False

            if 'uint' in config.data_types_dict[data_type]['dtype'] and value < 0:
                self.logger.error(f'Appending a negative {data_type} value {value} is not allowed for {config.data_types_dict[data_type]["dtype"]} '
                               f'({data_type}, {value}, {time_rec})')
                return False

            if data_type in self.combined_columns_flatten:
                self.logger.error('This data_type is part of a combined_column. Aborting append_value()')
                return False

            if 'int' in config.data_types_dict[data_type]['dtype'] and type(value) is not int:
                self.logger.warning(f'Warning! You are appending a non-int data type for {data_type}. Converting it to int...')
                value = int(round(value))

            # cast values
            # define when to start a new slice: file_size vs amount of files
            if not data_type in self.cols:
                # self.cols[data_type] = DataColumn(data_type, self)
                self._initiate_new_col(data_type)
                # todo for mongoengine here must be done more (see add_column)

            self.cols[data_type].append_value(value, time_rec)

    def append_binary(self, data_type, byte_list, time_rec, store_immediately=True, save_changes=True, final_analyse=False):

        # Hint: This method has less checks built in than the normal append_value() method to increase speed.
        #       Therefore make sure that you are inputting the correct data (bytes / bytearray) only.
        #
        #       Warning! This only fills up the slice binary buffers, to write the files to the hard drive set the store_immediately to True
        #       It's better to append bulk data to avoid too many small write procedures.
        #
        #       store_immediately: after appending, flush all. Alternatively call df.flush_appended_binaries() at a later point
        #       save_changes(final_analyse=final_analyse): store the db changes (usually usefull to do that to avoid incorrect lazy_loads after loading the df from the db)

        # todo are these checks enough or too much?
        if not byte_list:
            self.logger.debug(f'{self.hash_id} cannot append empty binary')
            return

        if not self.date_time_start and self.live_data:
            self._date_time_start = datetime.now(timezone.utc)
        elif not self.date_time_start:
            self.logger.warning('To start appending data you need to specify date_time_start first (Timezone aware e.g. datetime(2020, 5, 4, 10, 55, 59, 3, tzinfo=timezone.utc)).')
            return False

        if not self._hash_id:
            self.save()

        if self.status_closed:
            self.logger.warning('This datafile has already been closed. append_binary() is not possible.')
            return

        # in case sending is in progress, we hold back the append_binary task
        while self._saving_db:
            self.logger.debug(f'{self.hash_id}: while loop in append_binary: waiting for db save to finish')
            time.sleep(0.1)

        # all data must be part of a chunk
        if not self.chunks or self.chunks[-1].finalized:
            self.chunk_start()

        # combined cols
        if data_type in list(self.combined_columns):

            if len(byte_list) != len(self.combined_columns[data_type]):
                self.logger.error('Cannot append value to combined_column. len(value) != len(self.combined_columns[data_type]). Aborting append_value()')
                return False

            for i, data_type_2 in enumerate(self.combined_columns[data_type]):
                if byte_list[i]:
                    self.cols[data_type_2].append_binary(byte_list[i], time_rec)
                else:
                    self.logger.warning(f'append_binary {self.hash_id} skipped a byte_list because it was empty ({data_type_2})!')

        else:
            if data_type not in self.cols:
                self._initiate_new_col(data_type)

            self.cols[data_type].append_binary(byte_list, time_rec)

        if store_immediately:
            self.write_appended_binaries(data_type=data_type, save_changes=save_changes, final_analyse=final_analyse)

    def write_appended_binaries(self, data_type=None, save_changes=True, final_analyse=False):
        # store all appended binaries in the slices._values_bin

        while self._saving_db:
            self.logger.debug(f'{self.hash_id}: while loop in write_appended_binaries: waiting for db save to finish')
            time.sleep(0.1)

        t0 = time.monotonic()

        if data_type:
            if data_type in self.combined_columns:
                for data_type2 in self.combined_columns[data_type]:
                    self.cols[data_type2].write_appended_binaries()
            else:
                self.cols[data_type].write_appended_binaries()
        # if no data_type provided, then iterate through all data_types
        else:
            for data_type in self.cols:
                self.cols[data_type].write_appended_binaries()

        t1 = time.monotonic()

        if save_changes:
            self.save_changes(final_analyse=final_analyse)

        t2 = time.monotonic()

        self.logger.debug(f'write_appended_binaries: time for binaries: {t1 - t0}, time for database: {t2 - t1}')

    # ToDo: you must not use this method subsequently for the same data_type -> ends up in multiple unfilled slices
    def set_values(self, data_type, value_list, time_rec_list, store=True, compression=False,
                   finalize=True):

        self.logger.warning('set_values() is not yet fully implemented and can therefore not be used at the moment. Please use append_value() instead. Have a nice day.')
        return False

        # todo
        #  - mongodb adjustments
        #  - all in chunks
        #  - filter negativ values with uint
        #  - developer and gateway check (self.live_data)

        if not self._hash_id:
            self.save()

        if self.status_closed:
            self.logger.warning('This datafile has already been closed. set_values() is not possible anymore.')
            return
        # just set the whole list of values
        # cast values
        # define when to start a new slice: file_size vs amount of files

        if data_type not in config.data_types_dict:
            self.logger.error(f'The specified data_type {data_type} does not exist.')
            return

        try:
            value_list[0]
            time_rec_list[0]
        except TypeError:
            self.logger.error('value_list or time_rec_list is not a list. Aborting set_values()...')
            return

        # use of python lists only
        if type(value_list) is np.ndarray:
            value_list = value_list.tolist()
        if type(time_rec_list) is np.ndarray:
            time_rec_list = time_rec_list.tolist()

        if not data_type in self.cols:
            # self.cols[data_type] = DataColumn(data_type, self)
            self._initiate_new_col(data_type)

        # all data must be part of a chunk
        if not self.chunks or self.chunks[-1].finalized:
            self.chunk_start()

        self.cols[data_type].set_values(value_list, time_rec_list, store=store, compression=compression, finalize=finalize)

    def add_column(self, data_type, dry=False):

        if not self._hash_id:
            self.save()

        if data_type not in config.data_types_dict:
            self.logger.error(f'The specified data_type {data_type} does not exist.')
            return False

        if not data_type in self.cols:
            # only create instance if it's not a dry run/test
            if dry is False:
                self._initiate_new_col(data_type)

            return True
        else:
            self.logger.warning(f'Cannot add empty column of type {data_type} because this column already exists.')
            return False

    def add_combined_columns(self, data_type_list, identifier):

        # if not self._hash_id:
        #     self.save()

        # check identifier
        if identifier in self.combined_columns:
            self.logger.error(f'The identifier {identifier} is already in self.combined_columns.')
            return False

        # check identifier
        if identifier in config.data_types_dict:
            self.logger.error(f'The identifier {identifier} is already in data_types_dict.')
            return False

        # check for duplicates
        if len(list({x: None for x in data_type_list})) != len(data_type_list):
            self.logger.error(f'The specified data_type_list {data_type_list} includes duplicates.')
            return False

        # dry run on self.add_column()
        for data_type in data_type_list:

            if not self.add_column(data_type, dry=True):
                return False

        # all checks fine, so let's go for it
        time_slices_ref = None
        for i, data_type in enumerate(data_type_list):
            self._initiate_new_col(data_type, time_slices_ref=time_slices_ref)
            # first data_type in the combined_columns is the time_slices_ref
            if i == 0:
                time_slices_ref = data_type

        self.combined_columns[identifier] = data_type_list

    def _initiate_new_col(self, data_type, time_slices_ref=None):

        self.cols[data_type] = DataColumn()
        self.cols[data_type].data_type = data_type
        self.cols[data_type].dtype = config.data_types_dict[data_type]['dtype']
        self.cols[data_type].time_slices_ref = time_slices_ref
        self.cols[data_type].df = self
        self.update_attributes()

    @property
    def combined_columns_flatten(self):

        # a bit too pythonic?
        return [data_type for identifier in self.combined_columns for data_type in self.combined_columns[identifier]]

    def close(self, send=False, num_workers=3):
        # Call this to finish the File (eg. at the end of the day)
        # After that, it is not possible to append any more values

        t = time.monotonic()

        # save binaries first to get the pointers right...
        self.logger.debug(f'close: storing df {self.hash_id}, no final annalyse yet...')

        self.store(final_analyse=False)

        # todo: allow close again => change date?
        if not self.status_closed:
            self.status_closed = True

        self._check_all_chunks()

        # for compression, the binarys need to be stored on the harddrive already
        self.logger.debug(f'close: compressing df {self.hash_id}...')
        self.compress()

        # store + Final analyse (compress must happen before, otherwise the compressed file sizes are wrong
        self.logger.debug(f'close: storing df {self.hash_id}...')
        self.store()

        if send:
            self.logger.debug(f'close: sending df {self.hash_id}...')
            self.send(num_workers=num_workers)

        self.logger.info('df {} closed in {} sec'.format(self._hash_id, round(time.monotonic() - t, 1)))

    def _check_all_chunks(self):
        # finalize all chunks that were not correctly finalized (should not happen actually but just make sure)
        # This should only happen once when df.close()

        if self.chunks:
            # start from the last slice to profit from the completely finalized following chunk
            for list_index, chunk in reversed(list(enumerate(self.chunks))):
                # todo: istart == iend
                #  + 0 chunk => index 0
                #  bei chunk stop / start
                if not chunk.finalized:
                    # unfinished chunks will be finalized with indices ending at the start indices of the next chunk.
                    chunk.finalize(chunk_list_index=list_index)
                    # delete the last chunk if it is empty
                    if list_index == len(self.chunks) - 1:
                        if self.check_if_chunk_empty(chunk) and chunk.finalized:
                            del self.chunks[-1]

    def check_if_chunk_empty(self, chunk):

        total_samples = 0

        for col in chunk.cols.values():
            total_samples += col.samples
            if total_samples > 0:
                return False

        if total_samples == 0:
            return True

    def check_all_slices_sent(self, ignore_missing_slices=False):
        # returns True if all slices are sent
        # ignore_missing_slices: if True then ignore slices that are no longer on the harrddrive

        # should only exist if all slices are uploaded
        if self.date_time_upload:
            return True

        # go through all slices ...
        for data_type in self.cols:
            col = self.cols[data_type]

            # todo: are embeded listfields by standard empty lists?
            if col._slices_y:
                y_slices = col._slices_y
            else:
                y_slices = []
            if col._slices_time_rec:
                time_rec_slices = col._slices_time_rec
            else:
                time_rec_slices = []

            all_slices = y_slices + time_rec_slices

            for sl in all_slices:
                if not sl.status_sent_server:
                    if ignore_missing_slices:
                        if sl.file_exists:
                            return False
                        else:
                            self.logger.warning(f'check_all_slices_sent for df {self.hash_id}: ignoring {sl._path.name}. File not found.')
                    else:
                        return False

        return True

    def find_missing_slices(self):
        # check if all slices are on the harddrive

        missing_slices_list = []

        for data_type in self.cols:
            col = self.cols[data_type]

            # todo: are embeded listfields by standard empty lists?
            if col._slices_y:
                y_slices = col._slices_y
            else:
                y_slices = []
            if col._slices_time_rec:
                time_rec_slices = col._slices_time_rec
            else:
                time_rec_slices = []

            all_slices = y_slices + time_rec_slices

            for sl in all_slices:
                if not sl.file_exists:
                    missing_slices_list.append(sl._path.name)

        if missing_slices_list:
            self.logger.warning(f'find_missing_slices: slices not found {missing_slices_list}')

        return missing_slices_list

    def final_analyse(self):

        t = time.monotonic()

        duration_list = []

        for data_type in self.cols:

            self.cols[data_type].final_analyse()
            current_duration = self.cols[data_type].duration
            # filter out None
            if current_duration:
                duration_list.append(current_duration)

        if len(duration_list) == 0:
            self.duration = 0
        else:
            self.duration = round(max(duration_list), 2)

        self.duration_netto_meta = self.duration_netto

        if self.status_closed:
            # set date_time_end to last data point
            if not self._date_time_start:
                self._date_time_start = datetime.now(timezone.utc) - timedelta(0, self.duration)
            self.date_time_end = self._date_time_start + timedelta(0, self.duration)

        self.samples_meta = self.samples
        self.bin_size_meta = self.bin_size
        self.compressed_size_meta = self.compressed_size
        self.compression_ratio_meta = round(self.compression_ratio, 1)

        # self.logger.debug(f'df.final_analyse(): dur: {self.duration}, s_meta {self.samples_meta}, bin_s_meta {self.bin_size_meta}, compr_s {self.compressed_size_meta}, compr_r {self.compression_ratio_meta}')

        self.logger.debug('df {} final_analyse() in {} sec'.format(self._hash_id, round(time.monotonic() - t, 1)))

    def compress(self, algorithm='zstd', level=2):

        t = time.monotonic()

        # compress all - but only if file is closed
        if self._hash_id:

            self.logger.debug(f'compressing: {self}')
            # compress binary slices
            for data_type in self.cols:
                col = self.cols[data_type]
                col.compress(algorithm=algorithm, level=level)

            self._status = 'compressed'

        self.logger.debug('df {} compressed in {} sec'.format(self._hash_id, round(time.monotonic() - t, 1)))

    def send_meta(self):

        if not self._hash_id:
            self.save()

        # break here if you're not the producer of the df
        if config.producer_hash != self.producer_hash:
            self.logger.warning(f'Sending meta is only possible if you are the producer of the file. Producer hash: {config.producer_hash}, df_hash_id: {self.hash_id}')
            return False

        # use self.api_client for the server request
        if self.api_client:

            # convert df to json
            time_start = time.monotonic()
            json_data = self.to_json()
            time_to_json = round(time.monotonic() - time_start, 1)
            self.logger.debug('time elapsed send_meta {} in {} s'.format(self.hash_id, time_to_json))

            # do the server request
            return self.api_client.request('push_df/' + self.hash_id,
                                           data=json_data,
                                           timeout=config.request_timeout,
                                           log_time=True)

        # old api_v01, will get obsolete
        elif self.server:

            t = time.monotonic()
            self.logger.debug(f'sending meta for {self.hash_id}')

            try:
                print(self.server + '/api_v01/push_df/')
                t2 = time.monotonic()
                data = self.to_json()
                time_to_json = round(time.monotonic() - t2, 1)
                response = requests.post(self.server + '/api_v01/push_df', data=data, timeout=config.request_timeout)

                if response.status_code == 200:
                    self.logger.debug(
                        'time elapsed send_meta: {} s (self.to_json: {} s)'.format(round(time.monotonic() - t, 1),
                                                                                   time_to_json))
                    return True
                else:
                    self.logger.warning(f'sending meta for {self.hash_id} failed with status code {response.status_code}.')
                    return False

            except requests.exceptions.RequestException as e:
                self.logger.warning(f'sending meta for {self.hash_id} failed with RequestException. server down?')
                return False

        else:
            self.logger.warning('no server selected')
            return False

    # just for testing
    # old api_v01, will get obsolete
    def _send_other(self):
        _ = requests.post(self.server + '/api_v01/person/', data=self.person.to_json(), timeout=config.request_timeout)
        _ = requests.post(self.server + '/api_v01/project/', data=self.project.to_json(), timeout=config.request_timeout)
        _ = requests.post(self.server + '/api_v01/device/', data=self.device.to_json(), timeout=config.request_timeout)
        _ = requests.post(self.server + '/api_v01/receiver/', data=self.receiver.to_json(), timeout=config.request_timeout)

    def send(self, partially=False, rawdata=False, data_type_list=None, ignore_missing_slices=False, num_workers=3):

        """docstring description
        Args:

            partially: If True, slices will be sent partially (even if the slices are not full yet). Sends only data which was not sent yet and only the data with are marked with "send_json" in
            data_types.json. Compressed data will also NOT be sent! (keep in mind that this will be done after df.compress() or df.close()

            rawdata: If True AND partially=True all data types will be sent partially.

            data_type_list: (requires partially=True) A list of data types which shall be sent partially. e.g. ['ppg_green', 'heart_rate]. If this is set, ONLY the specified data types will be

            sent, unless rawdata=True.

        Returns:
            meta_sent: True if the last meta.json which was sent had status code 200

        Other:

            No slice that has been sent will be sent again. Only a crash in the program might cause it.

            If you use no parameters with send() there are the following usecases:
                1) the file is not closed and not compressed:
                    => nothing will be sent
                2) the file is compressed but not closed:
                    => only full slices (of any data type) will be sent
                    => empty slices will not be sent
                    => the meta.json will be sent after the slices
                3) The file is closed
                    => all slices will be sent which were not already sent
                    => the meta.json will be sent after the slices

            When sending partially, the meta.json will be sent to the server first (to reduce the chance that wrong partial data is sent)
            When sending partially, the send data will be NOT compressed.
            If the data IS already compressed, it cannot be sent partially!

        Examples:

            Let's say the labclient stores every 10 seconds and wants to send only the newly generated vitals
            (data types which have "json_send": true in data_types.json) to the server that are new:

                df.append_value()
                ...
                df.store()
                df.send(partially=True)
                df.send_json()

                => sends only

            Instead, if the labclient shall send ALL data types partially:

                ...
                df.send(partially=True, rawdata=True)
                ...

            If only ppg_ir and heart_rate shall be partially sent:

                ...
                df.send(partially=True, data_type_list=['ppg_ir', 'heart_rate']
                ...

            If the labclient wants to send full slices, without closing the file they need to be compressed first
                df.compress()
                df.send()

            Closing the file:

                Closing with:

                    df.close(send=True)

                is the same as:

                    df.close()
                    df.send()

                -------------------

                The file was closed and something went wrong during the send() and some slices were NOT sent, they can be sent again with just

                    df.send()
                    => will send all unsent slices
                    => if all are sent, the df._meta['date_time_upload'] will be set.
                    => at the end the final meta.json will be sent again to the server. Check the return value of send() for this

        """

        t = time.monotonic()

        if not self._hash_id:
            self.save()

        # break here if you're not the producer of the df
        if config.producer_hash != self.producer_hash:
            self.logger.warning(f'Sending meta is only possible if you are the producer of the file. Producer hash: {config.producer_hash}, df_hash_id: {self.hash_id}')
            return False

        if not num_workers:
            num_workers = 1

        # checks
        if data_type_list:
            if not type(data_type_list) is list:
                self.logger.error(f' the provided data_type_list is not of type list! Aborting send()')
                return
            for data_type in data_type_list:
                # check if any item is not a supported data_type...
                if data_type not in config.data_types_dict:
                    self.logger.error(f' the provided data_type {data_type} in data_type_list is not in config.data_types_dict! Aborting send()')
                    return

        # use self.api_client for the server request
        if self.api_client:

            self.logger.debug(f'initiating send to server: {self} with status_closed={self.status_closed}, partially={partially}')

            # when sending patially, the meta data should be sent first, because the server check the values_sent pointer for consistency
            if partially:
                if not self.send_meta():
                    self.logger.warning('Partially send to server not successfully because meta was not correctly sent!')
                    return False

            # select cols to be sent
            cols_to_send_list = []
            for data_type in self.cols:
                col = self.cols[data_type]

                # send non partially, don't send if already uploaded
                if not partially and not self.date_time_upload:
                    cols_to_send_list.append(col)

                # send everything partially
                elif rawdata:
                    cols_to_send_list.append(col)

                # only specified list
                elif data_type_list and data_type in data_type_list:
                    cols_to_send_list.append(col)

                # only if specified in data_types.json
                elif config.data_types_dict[data_type]['send_json']:
                    cols_to_send_list.append(col)

            # send cols with threads, amount of worker defined with num_workers
            thread_local = threading.local()
            thread_local.session = requests.Session()
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                for col in cols_to_send_list:
                    executor.submit(col.send, thread_local.session, partially)

            # send() may change the _meta of the slices. If this does not get stored, next time the file is opened,
            # this information would get lost and thus not reach the server
            if self.check_all_slices_sent() and self.status_closed:

                # if date_time_upload is set, the file was already fully uploaded
                if not self.date_time_upload:
                    self.date_time_upload = datetime.now(timezone.utc)

                self.logger.debug(f'all slices from {self} sent.')

            # send the meta to the server so that the server is up to date
            t = time.monotonic()
            self.save_changes(final_analyse=False)
            self.logger.debug('time elapsed save_changes: {} s'.format(round(time.monotonic() - t, 1)))

            meta_sent_status = self.send_meta()

            if self.check_all_slices_sent() and self.status_closed and not meta_sent_status:
                # if the file is closed but the meta has not arrived, try another 5 times to send the meta file
                for i in range(5):
                    meta_sent_status = self.send_meta()
                    if meta_sent_status:
                        break
                    time.sleep(0.5)

            if meta_sent_status:
                self.logger.debug('df {} send to server done successfully in {} sec'.format(self._hash_id, round(time.monotonic()-t, 1)))
                return True
            else:
                self.logger.warning('send to server not successfully, last meta file not sent.!')
                return False

        # old api_v01, will get obsolete
        elif self.server:

            t = time.monotonic()

            self.logger.info(f'initiating send to server: {self} with status_closed={self.status_closed}, partially={partially}')

            if partially:
                # when sending patially, the meta data should be sent first, because the server check the values_sent pointer for consistency

                meta_sent_status = self.send_meta()

                if meta_sent_status:

                    thread_local = threading.local()
                    thread_local.session = requests.Session()

                    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                        for data_type in self.cols:
                            col = self.cols[data_type]
                            if rawdata:
                                # send everything partially
                                executor.submit(col.send_old, self.server, thread_local.session, partially)
                            elif data_type_list:
                                # only specified list
                                if data_type in data_type_list:
                                    executor.submit(col.send_old, self.server, thread_local.session, partially)
                            elif config.data_types_dict[data_type]['send_json']:
                                # end only if specified in data_types.json
                                executor.submit(col.send_old, self.server, thread_local.session, partially)

                else:

                    self.logger.warning('Partially send to server not successfully because meta was not correctly sent!')
                    return False
            # send full slices or not-full slices if the data file is closed
            # in this case it is sufficient to send the meta data after the slices
            else:

                thread_local = threading.local()
                thread_local.session = requests.Session()
                with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:

                    # don't send if already uploaded
                    if not self.date_time_upload:
                        for data_type in self.cols:
                            col = self.cols[data_type]
                            executor.submit(col.send_old, self.server, thread_local.session)

            # send() may change the _meta of the slices. If this does not get stored, next time the file is opened,
            # this information would get lost and thus not reach the server
            if self.check_all_slices_sent() and self.status_closed:

                # if date_time_upload is set, the file was already fully uploaded
                if not self.date_time_upload:
                    self.date_time_upload = datetime.now(timezone.utc)

                self.logger.debug(f'all slices from {self} sent.')
            # send the meta to the server so that the server is up to date
            t = time.monotonic()

            self.save_changes(final_analyse=False)

            self.logger.debug('time elapsed save_changes: {} s'.format(round(time.monotonic() - t, 1)))

            meta_sent_status = self.send_meta()

            if self.check_all_slices_sent() and self.status_closed and not meta_sent_status:
                # if the file is closed but the meta has not arrived, try another 5 times to send the meta file
                for i in range(5):
                    meta_sent_status = self.send_meta()
                    if meta_sent_status:
                        break
                    time.sleep(0.5)

            if meta_sent_status:
                self.logger.debug('df {} send to server done successfully in {} sec'.format(self._hash_id,
                                                                                       round(time.monotonic() - t, 1)))
                return True
            else:
                self.logger.warning('send to server not successfully, last meta file not sent.!')
                return False

        else:
            self.logger.warning('no server selected')
            return False

    def send_json(self):
        # send only the last data points of data_types that are not heavy in data usage heart_rate, temperature, ... for "live stream"

        if not self._hash_id:
            self.store()

        if not self.server and not self.api_client:
            self.logger.warning(f'send_json failed: no server or api_client selected')
            return False

        if not self.live_data or not self.receiver or not self.device:
            self.logger.warning(f'send_json failed: live_data ({self.live_data}), receiver ({self.receiver}) and device ({self.device}) necesssary')
            return False

        t = time.monotonic()

        data_dict = {}
        person_hash = self.person.id
        receiver_hash = self.receiver.hash_id
        data_dict['person_hash'] = person_hash
        data_dict['device'] = self.device.hash_id
        data_dict['data'] = {}

        # set a threshold in seconds to not send data older than this threshold
        # by default, use the given interval of the receiver config to avoid sending data again
        if self.receiver.receiver_config.live_data_interval:
            threshold = self.receiver.receiver_config.live_data_interval
        else:
            threshold = 30

        for data_type in self.cols:
            if config.data_types_dict[data_type]['send_json']:

                col = self.cols[data_type]
                tol = (datetime.now(tz=timezone.utc) - self.date_time_start).total_seconds() - threshold

                # check if data exists and is not too old
                if col._slices_time_rec and col._slices_time_rec[-1].values and col._slices_time_rec[-1].values[-1] > tol:
                    value = round(col._slices_y[-1].values[-1], 1)
                else:
                    # the server api also uses empty strings for non existing values
                    value = ''

                col_abbreviation = config.data_types_dict[data_type]['abbreviation']
                data_dict['data'][col_abbreviation] = value

        data_dict = json.dumps(data_dict)

        if self.api_client:
            req_url = 'push_json/'
            req_result = self.api_client.request(req_url,
                                                 data=data_dict,
                                                 timeout=config.request_timeout,
                                                 log_time=True,
                                                 attempts=1)

            if not req_result:
                return False

        else:
            # todo remove once api v01 is deprecated
            req_url = self.server + '/api_v01/push_json/' + receiver_hash
            try:
                response = requests.post(req_url, data=data_dict, timeout=config.request_timeout)
                if response.status_code != 200:
                    self.logger.warning(f'send_json for {self.hash_id} person {person_hash} failed with status code {response.status_code}.')
                    return False

            except requests.exceptions.RequestException as e:
                self.logger.warning('df.send_json() failed with RequestException. server down?')
                self.logger.warning(str(e))
                return False

        self.logger.debug(f'send_json for {self.hash_id} person {person_hash} successful {data_dict} - Time elapsed: {round(time.monotonic() - t, 1)} s')
        return True

    def get_slice(self, ds_hash):

        for data_type in self.cols:
            col = self.cols[data_type]

            # todo: are embeded listfields by standard empty lists?
            if col._slices_y:
                y_slices = col._slices_y
            else:
                y_slices = []
            if col._slices_time_rec:
                time_rec_slices = col._slices_time_rec
            else:
                time_rec_slices = []

            all_slices = y_slices + time_rec_slices

            for sl in all_slices:
                if ds_hash == sl.hash:
                    return sl

        # not found...
        self.logger.warning('slice ' + ds_hash + ' not found')
        return None

    def get_slice_list(self):
        # return a list of all slice hashes of this datafile
        slice_list = []
        for data_type in self.cols:
            col = self.cols[data_type]

            all_slices = col._slices_y + col._slices_time_rec

            for sl in all_slices:
                slice_list.append(sl.hash)

        return slice_list

    def is_identical(self, df):
        # check if current data_file is identical with given data_file df

        # check number of slices
        if self.slices != df.slices:
            return False

        # check bin_size
        if self.bin_size != df.bin_size:
            return False

        # compare hash lists
        self_hash = self.get_slices_md5_hash_list()
        df_hash = df.get_slices_md5_hash_list()

        try:
            for i in range(len(self_hash)):
                if self_hash[i] != df_hash[i]:
                    return False
        except IndexError:
            return False

        return True

    def get_slices_md5_hash_list(self):

        hash_list = []

        for data_type in self.cols:

            col = self.cols[data_type]

            for slice_type in col._slices:

                for sl in col._slices[slice_type]:
                    data = sl.get_bin_data()
                    current_md5_hash = hashlib.md5(data).digest()
                    hash_list.append(current_md5_hash)

        hash_list.sort()

        return hash_list

    @property
    def number_of_all_chunks(self):

        return len(self.chunks) + len(self.chunks_labelled) + len(self.markers)

    def chunk_start(self, date_time_start=None):

        if not self.status_closed:

            if not self._hash_id:
                self.save()

            if not self.date_time_start and self.live_data:
                self._date_time_start = datetime.now(timezone.utc)
            elif not self.date_time_start:
                self.logger.warning('To start a chunk without live_data_mode, you need to specify date_time_start first (Timezone aware!).')
                return False

            next_chunk_index = len(self.chunks)

            # set start time of the chunk before finalizing the last chunk (takes quite some time for long recordings)
            # because the gateway already appends data points in live mode
            # => chunks with x_offset() would have negative times when waiting for the finalization of the chunk
            if self.live_data:
                # if it is a continuous measurement then take the date_time_end of the last chunk if available
                # self._continue_recording is set by chunk_stop
                if self._continue_recording:
                    if self.chunks and self.chunks[-1].finalized:
                        chunk_date_time_start_live = self.chunks[-1].date_time_end
                    else:
                        chunk_date_time_start_live = datetime.now(timezone.utc)
                else:
                    chunk_date_time_start_live = datetime.now(timezone.utc)

                self.logger.debug('chunk_start {} at index {} (continue_recording {}, next start time {}) '.format(
                    self.hash_id,
                    next_chunk_index,
                    self._continue_recording,
                    chunk_date_time_start_live,
                ))
                # reset to default
                self._continue_recording = False

            if self.chunks:
                if not self.chunks[-1].finalized:
                    self.chunk_stop(final_analyse=False)

            t = time.monotonic()

            data_chunk = DataChunk()

            if self.live_data:
                data_chunk.date_time_start = chunk_date_time_start_live
            else:
                if date_time_start:
                    if date_time_start > self.date_time_start:
                        if len(self.chunks) == 0 or (self.chunks[-1].finalized and date_time_start > self.chunks[-1].date_time_end):
                            data_chunk.date_time_start = date_time_start
                        else:
                            self.logger.warning('date_time_start for chunk_start incorrect')
                            return False
                    else:
                        self.logger.warning('date_time_start for chunk_start must be bigger than df.date_time_start and date_time_end of the chunk before.')
                        return False
                # only the first chunk can have the date_time_start of the data file
                elif len(self.chunks) == 0:
                    data_chunk.date_time_start = self.date_time_start
                elif self.chunks[-1].finalized:
                    data_chunk.date_time_start = self.chunks[-1].date_time_end
                else:
                    self.logger.warning('date_time_start for chunk_start incorrect')
                    return False

            data_chunk._initialize_data_chunk(df=self, index=next_chunk_index)
            data_chunk.time_offset = (data_chunk.date_time_start - self.date_time_start).total_seconds()

            self.chunks.append(data_chunk)

            self.logger.debug(f'chunk_start {self.hash_id} at index {next_chunk_index} in {time.monotonic() - t} s')

    def chunk_stop(self, store=True, final_analyse=True, continue_recording=False):
        # continue_recording: This is important for the Gateway if this param is True, the next chunk start (in live mode)
        #                     will use date_time_end of the previous chunk.
        # store: especially if you have appended binaries, make sure to store, otherwise the indices will be wrong

        t = time.monotonic()

        if self.chunks:

            self.logger.debug(f'chunk_stop {self.hash_id}: finalizing data chunk at index {len(self.chunks) - 1} (continue_recording={continue_recording})')

            self._continue_recording = continue_recording

            # Make sure all pointers are correct
            if store:
                # if you append binaries make sure to store, otherwise the indices will be wrong!!
                self.store(final_analyse=False)

            self.chunks[-1].finalize()
            t2 = time.monotonic() - t

            if self.check_if_chunk_empty(self.chunks[-1]) and self.chunks[-1].finalized:
                self.logger.debug(f'data chunk {self.hash_id} index {len(self.chunks) - 1} without any columns finalized in {t2} seconds. Chunk will be discarded.')
                del self.chunks[-1]
            else:
                self.logger.debug(f'data chunk {self.hash_id} index {len(self.chunks) - 1} with {len(self.chunks[-1].cols)} columns finalized in {t2} seconds.')

        if store:
            self.store(final_analyse=final_analyse)
        self.logger.debug(f'data chunk {self.hash_id} index {len(self.chunks) - 1} finalized and stored in {time.monotonic() - t} seconds.')

    def add_labelled_chunk(self, label, time_start, time_end=None, store=True, final_analyze=False):
        # mark a time region or time point with a specific label

        if type(label) is not str:
            self.logger.warning(f'add_labelled_chunk: label must be a string but is type: {type(label)}.')
            return

        if not self._hash_id:
            self.save()
        if not self._date_time_start:
            self._date_time_start = datetime.now(timezone.utc)

        try:

            if time_end is not None:
                next_chunk_index = len(self.chunks_labelled)
            else:
                next_chunk_index = len(self.markers)

            label_chunk = DataChunk()
            label_chunk._inizialize_labelled_chunk(self, next_chunk_index, label, time_start, time_end)
            label_chunk.date_time_modified = datetime.now(timezone.utc)

            if time_end is not None:
                self.chunks_labelled.append(label_chunk)
            else:
                self.markers.append(label_chunk)

            if store:
                self.store(final_analyse=final_analyze)

        except (ChunkTimeError, ChunkNoValuesError):
            self.logger.warning(f'Aborting Label chunk with label={label}, time_start={time_start}, time_end={time_end}')

    def return_chunks_with_label(self, label, exact=True):
        # returns all labelled chunks that match or contain a specified label

        chunk_list = []

        for chunk in self.chunks_labelled:

            if exact:
                if chunk.label == label:
                    chunk_list.append(chunk)
            else:
                if label in chunk.label:
                    chunk_list.append(chunk)

        return chunk_list

    def return_markers_with_label(self, label, exact=True):
        # returns all labelled chunks that match or contain a specified label

        chunk_list = []

        for chunk in self.markers:

            if exact:
                if chunk.label == label:
                    chunk_list.append(chunk)
            else:
                if label in chunk.label:
                    chunk_list.append(chunk)

        return chunk_list

    @property
    def chunk_labels(self):
        # return a list of all available labels

        label_list = []

        for chunk in self.chunks_labelled:

            if chunk.label not in label_list:
                label_list.append(chunk.label)

        return label_list

    @property
    def marker_labels(self):
        # return a list of all available labels

        label_list = []

        for chunk in self.markers:

            if chunk.label not in label_list:
                label_list.append(chunk.label)

        return label_list

    def preload_slice_values(self, all_slices=True):
        # provoke a lazy_load in all slices so that they can be loaded
        # when the Gateway is idle. With all_slices=False only the last slices
        # of each data_type will be loaded.
        self.logger.debug(f'pre-loading slice values (all_slices={all_slices})...')
        t = time.monotonic()

        # with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        for data_type in self.cols:
            self._load_slice_values(data_type, all_slices)
            # executor.submit(self._load_slice_values, data_type, all_slices)

        self.logger.debug(f'time elapsed preload slices: {round(time.monotonic() - t, 1)} s')

    def _load_slice_values(self, data_type, all_slices):

        col = self.cols[data_type]

        if all_slices:
            for sl in col._slices_y:
                sl.values
            for sl in col._slices_time_rec:
                sl.values
        else:
            if col._slices_y:
                col._slices_y[-1].values
            if col._slices_time_rec:
                col._slices_time_rec[-1].values

    def free_memory(self):

        current_process = psutil.Process(os.getpid())
        memory_usage_before = current_process.memory_info().rss

        # iterate through all full & saved slices to delete ._values and free up memory
        loaded_slices = 0
        loaded_samples = 0
        freed_slices = 0
        freed_samples = 0
        for sl_hash in self.get_slice_list():
            sl = self.get_slice(sl_hash)
            # sl.status_finally_analzyed means it's stored and status_slice_full
            if sl.status_finally_analzyed:
                if sl._values:
                    freed_slices += 1
                    freed_samples += len(sl._values)
                sl._values = None
            else:
                loaded_slices += 1
                if sl._values:
                    loaded_samples += len(sl._values)
                else:
                    pass

        memory_usage_after = current_process.memory_info().rss
        memory_freed = memory_usage_before - memory_usage_after
        memory_freed = round(memory_freed / (1024 * 1024), 1)
        memory_usage_after = round(memory_usage_after / (1024 * 1024), 1)

        self.logger.debug(f'free_memory() for df {self.hash_id}: freed slices {freed_slices}, freed samples {freed_samples}, freed memory: {memory_freed} MB')
        self.logger.debug(f'free_memory() for df {self.hash_id}: loaded slices {loaded_slices}, loaded samples {loaded_samples}, current memory {memory_usage_after} MB')

    # def from_json(self, *args, **kwargs):
    #     super(DataFile, self).from_json(*args, **kwargs)
    #     self.init()

    def convert_df_from_json(self, json_data, created=False):

        self.from_json(json_data, created)
        self.init()
        # super(DataFile, self).save(force_insert=True, *args, **kwargs)

    def add_afe_config(self):
        # todo optimize this for developers (non-gateways)

        if not self.live_data:
            self.logger.warning(f'add_afe_config currently only optimized for the gateway')
            return

        if not self.device or not self.device.device_config:
            self.logger.warning(f'can only add device config if a device with a config is given')
            return

        if not self._hash_id:
            self.store()

        if not self.date_time_start:
            self.date_time_start = datetime.now(tz=timezone.utc)

        offset = (datetime.now(tz=timezone.utc) - self.date_time_start).total_seconds()
        current_config = {'time': offset}

        for db_field, attr in self.device.device_config._reverse_db_field_map.items():
            value = getattr(self.device.device_config, attr)
            current_config[db_field] = value

        self._afe_config.append(current_config)

    def export_csv(self,
                   dir_path=None,
                   data_types=None,
                   file_name=None,
                   meta_header=False,
                   separator=',',
                   digits=3,
                   ):
        # dir_path: if None then just the csv text and filename will be returned, but no csv is stored on the disk.
        # data_types: list of data_types to be exported, if None, then all will be exported. Combined cols in one file.
        #  example: ['heart_rate', 'ppg_ambient_red_ir', ['acc_x', 'acc_y']] >> acc will also be a combined col.
        # for the description of the parameters, see data_column.export_csv() method

        if separator not in [' ', '\t', ',', ';']:
            self.logger.error(f"The specified separator '{separator}' is not possible, choose from: [' ', '\\t' and ',']")
            return False

        # separate data_types in the combined ones and the single ones to avoid double exports
        export_cols = []
        if data_types:
            # kick out data types that are not in this data file... (if someone specified 'spo2' but this file has no spo2 cols)
            for data_type in data_types:
                # data_type = 'heart_rate' or 'ppg_ambient_red_ir'
                if type(data_type) is str:
                    if data_type in self.cols or data_type in self.combined_columns:
                        export_cols.append(data_type)
                elif type(data_type) in [list, tuple]:
                    # data_type = ['acc_x', 'acc_y']
                    dt_list = []
                    for dt in data_type:
                        if dt in self.cols:
                            dt_list.append(dt)
                    if dt_list:
                        export_cols.append(dt_list)
        else:
            # export all data types in this data file
            export_cols.extend(list(self.combined_columns))
            export_cols.extend([data_type for data_type in self.cols if data_type not in self.combined_columns_flatten])

        csv = {}

        for dt in export_cols:
            if type(dt) in [list, tuple]:
                dt_str = '_'.join(dt)
            else:
                dt_str = dt
            csv[dt_str] = {}
            csv[dt_str]['csv'], csv[dt_str]['file_name'] = self._export_csv_col(dt,
                                                                        dir_path=dir_path,
                                                                        file_name=file_name,
                                                                        meta_header=meta_header,
                                                                        separator=separator,
                                                                        digits=digits,
                                                                        )

        self.logger.info(f'CSV export for {self.hash_id} done.')

        if dir_path:
            return None
        else:
            return csv

    def _export_csv_col(self,
                        data_type,
                        dir_path=None,
                        file_name=None,
                        meta_header=False,
                        separator=',',
                        digits=3,
                        ):
        # data_type: either a combined col's name or a normal data_type or a list/tuple of data_types which shall be combined
        # dir_path: if not specified, then the csv will not be stored but csv string and the filename will be returned instead as a dict 'csv'.
        # file_name: List of items which shall appear in that order in the file name. You may choose from:
        #   ['person_hash', 'person_label','date_time_start', 'date_time_end', 'df_hash', 'project_hash', 'project_name', 'device']
        #   Any strings other than the above can be used as separators.
        #   Example: ['person_hash', '_start_', 'date_time_start', '_proj_', 'project_hash'] will yield
        #     >>      M9KH.03GS_start_2020-12-14_15-36-59_proj_M9K_acc_x.csv
        #   The default is ['person_hash', '_', 'date_time_start'] >> M9KH.03GS_2020-12-14_15-36-59_acc_x.csv
        # meta_header: set to True if you want some meta information above the csv data
        # separator: choose from: ' ', '\t' and ','
        # digits: how many places to round float values

        if type(data_type) is str:
            # 'acc_x_y_z'
            if data_type in self.combined_columns:
                data = {}
                data_type_str = '_' + '_'.join(self.combined_columns[data_type])
                for dt in self.combined_columns[data_type]:
                    data[dt] = self.cols[dt].y
                data['time'] = self.cols[dt].x
            # 'heart_rate'
            else:
                data = {
                    'time': self.cols[data_type].x,
                    data_type: self.cols[data_type].y,
                }
                data_type_str = f'_{data_type}'
        # ['acc_x', 'acc_y']
        elif type(data_type) in [list, tuple]:
            data = {}
            data_type_str = '_' + '_'.join(data_type)
            for dt in data_type:
                data[dt] = self.cols[dt].y
            data['time'] = self.cols[dt].x
        else:
            self.logger.error(f'_export_csv data_type not valid: {data_type}')

        # find if x, y data has not the same length
        length_error_str = 'no'
        length_list = []
        for data_type in data:
            length_list.append(len(data[data_type]))
        all_same_length = all(length == length_list[0] for length in length_list)

        if not all_same_length:
            minimum = min(length_list)
            length_error_str = '_xy_length_difference'
            for data_type in data:
                data[data_type] = data[data_type][:minimum]

        pd_df = pd.DataFrame(data=data).set_index('time')

        device = self.device.hash_id if self.device else '-'
        date_time_start_str = self.date_time_start.strftime("%Y-%m-%d_%H-%M-%S.%f_%Z")
        date_time_end_str = self.date_time_end.strftime("%Y-%m-%d_%H-%M-%S.%f_%Z") if self.date_time_end else '-'

        meta = {
            'df_hash': self.hash_id,
            'device': device,
            'project_hash': self.project.hash_id,
            'project_name': self.project.name,
            'person_hash': self.person.hash_id,
            'person_label': self.person.label,
            'date_time_start': date_time_start_str,
            'date_time_end': date_time_end_str,
            'length_difference_noticed': length_error_str,
        }

        if not file_name:
            # Default file name
            file_name_str = f'{self.person.hash_id}_{date_time_start_str[:19]}{data_type_str}.csv'
        else:
            # Custom file name
            file_name_str = ''
            for name_part in file_name:
                if name_part not in meta:
                    file_name_str += name_part
                else:
                    file_name_str += meta[name_part][:19] if 'date_time_' in name_part else meta[name_part]
            file_name_str = file_name_str[:-1] + data_type_str + '.csv'

        header_lines = ''

        if meta_header:
            for key, value in meta.items():
                header_lines += f'{key}{separator}{value}\n'
            header_lines += '\n'

        if dir_path:
            file_path = Path(dir_path) / file_name_str
            with open(file_path, 'w') as fp:
                fp.write(header_lines)
                pd_df.to_csv(
                    fp,
                    header=True,
                    decimal='.',
                    sep=separator,
                    float_format=f'%.{digits}f',
                )
            return None, file_name_str

        else:
            # just return the string representation...
            csv_str = header_lines
            csv_str += pd_df.to_csv(
                None,
                header=True,
                decimal='.',
                sep=separator,
                float_format=f'%.{digits}f',
            )

            return csv_str, file_name_str
