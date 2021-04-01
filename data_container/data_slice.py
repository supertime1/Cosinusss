import random
import string
import os
from pathlib import Path
from collections import deque
import time
import numpy as np
import zstd
import gzip
import requests
from mongoengine import EmbeddedDocument, ListField, BooleanField, StringField, FloatField, IntField
import mongoengine

# import package modules
# ToDo: change anywhere import to: import .dc_helper and then use it like that: dc_helper.producer_hash
from .dc_helper import DcHelper, AttributesContainer
from .odm import User, Project, Person, Receiver, Device, EventLog, Comment

from . import config

class DataSlice(EmbeddedDocument):

    # ToDo clarify: if hash_str given -> load existing slice. Same for df. change the function head to make this clear!

    # id
    _hash = StringField(primary_key=True)

    # relationships
    producer_hash = StringField(db_field='ph')
    data_type = StringField(db_field='d_ty', choices=config.data_types)
    dtype = StringField(db_field='dty')
    slice_type = StringField(db_field='sl_t', choices=('y', 'time_rec'))
    slice_time_offset = FloatField(db_field='sto')
    # for algo -> algo version hash
    source = StringField(db_field='src', null=True)
    source_producer_hash = StringField(db_field='src_ph', null=True)
    status_slice_full = BooleanField(default=False, db_field='s_sf')
    # todo fix this typoe in analyzed!
    status_finally_analzyed = BooleanField(default=False, db_field='s_fa')
    status_compressed = BooleanField(default=False, db_field='s_zip')
    status_sent_server = BooleanField(default=False, db_field='s_se')
    status_loaded = BooleanField(default=False, db_field='s_lo')

    min = FloatField(db_field='min', null=True)
    max = FloatField(db_field='max', null=True)
    mean = FloatField(db_field='avg', null=True)
    last_val = FloatField(db_field='lst', null=True)
    data_gaps = ListField(db_field='dgps')
    samples_meta = IntField(default=0, db_field='s')
    compressed_size_meta = IntField(default=0, db_field='cs')
    compression_ratio_meta = IntField(default=0, db_field='cr')
    bin_size_meta = IntField(default=0, db_field='bsz')
    values_write_pointer = IntField(default=0, db_field='vwp')
    values_analyse_pointer = IntField(default=0, db_field='vap')
    values_send_pointer = IntField(default=0, db_field='vsp')

    def __init__(self, *args, **kwargs):

        super(DataSlice, self).__init__(*args, **kwargs)

        self._values = None
        self._path = None
        self.df = None
        self.max_upload_attempts = 2
        self.logger = config.logger
        # self._values_bin is a buffer for appending binaries. Then it turns into a bytearray as soon as binaries are appended.
        # It is important to always distinguish between _values_bin==None and _values_bin beeing an empty bytearray!!
        self._values_bin = None

    def _init(self, df):
        self.df = df

        # check hash
        if not self._hash:

            slice_list = self.df.get_slice_list()

            while True:

                hash_gen = config.generate_hash(3)

                # break if it's not a duplicate hash
                if not hash_gen in slice_list:
                    break

            self.logger.debug('new DataSlice ' + self.df.hash_id + '/' + self.data_type + '.' + hash_gen + '.' + self.slice_type)
            self._hash = hash_gen

        df_path = config.df_path / Path(self.df.hash_id)
        if os.path.isfile(df_path / Path(self._hash + '.bin.zst')):
            self._path = df_path / Path(self._hash + '.bin.zst')
        elif os.path.isfile(df_path / Path(self._hash + '.bin.gz')):
            self._path = df_path / Path(self._hash + '.bin.gz')
        else:
            self._path = df_path / Path(self._hash + '.bin')

        # ToDo: optimize this, we need only self.values[and a self._write_index
        #       self._values gets freed with final storing and write position for writing to binary is handled with self._write_index
        #       less RAM than like it's done now

    def __str__(self):

        # ToDos: ideas: file size, device, person, ...
        format_str = 'DataSlice(hash_long={}, slice_time_offset={}, samples_meta={}, file_size={}, dtype={})'
        return format_str.format(
                                self.hash_long,
                                self.slice_time_offset,
                                self.samples_meta,
                                self.file_compressed_size,
                                self.dtype
                                )

    @property
    def hash(self):
        return self._hash

    # i know it's the wrong way round, but self._values does not work with mongo
    @property
    def values(self):

        self._initiate_values()
        return self._values

    # @values.setter
    # def values(self, value):
    #     # probably not a good idea to make people wanna use that
    #     self._initiate_values()
    #     self._values = value

    @property
    def hash_long(self):

        return self.df.hash_id + '/' + self.data_type + '.' + self._hash + '.' + self.slice_type

    @property
    def samples(self):

        if (self.df.live_data or self.df.date_time_upload) and self.status_finally_analzyed:
            # do this to avoid unnecessary loads
            return self.samples_meta
        else:
            # only if binaries were appended (to avoid lazy_loads)
            if self.binaries_appended:
                return self.values_write_pointer
            # normal case
            else:
                return len(self.values)

    @property
    def bin_size(self):

        return self.dtype_size * self.samples

    @property
    def file_size(self):

        return DcHelper.file_size_str(self.bin_size)

    @property
    def compressed_size(self):

        try:
            return os.path.getsize(self._path)
        except FileNotFoundError:
            return self.bin_size

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
    def dtype_size(self):
        return DcHelper.helper_dtype_size(self.dtype)

    @property
    def binaries_appended(self):
        # check whether binaries have been appended to this slice during runtime
        if self._values_bin is not None:
            return True
        else:
            return False

    def _initiate_values(self):

        # slice is empty or the datafile is loaded again
        if self._values == None:

            # load binary files...
            if self.values_write_pointer > 0:
                # todo: list? numpy arrays?? => currently conversion to list probably bad performance ... but numpy array does not support .append() method
                # todo: sometimes (ppg uint24) values is None if file was not found. how can np.asarray([], dtype='uint32') be None?
                #  https://stackoverflow.com/questions/54186190/why-does-numpy-ndarray-allow-for-a-none-array
                values = self.lazy_load()
                if values is not None:
                    # cast to python list with python values since mongoengine and some other functions might not work with numpy datatypes
                    self._values = values.tolist()
                else:
                    self._values = []
                    self.logger.warning(f'could not properly lazy_load() _values of slice {self.hash_long}. Returning empty list []')
            # there are no values yet ...
            else:
                self._values = []

    def append(self, value):

        self._initiate_values()
        self._values.append(value)

    def extend(self, value):

        self._initiate_values()
        self._values.extend(value)

    def values_write_bin(self, value_list, store=True, compression=False):

        self.values_write_pointer = 0
        self._values = value_list

        if store:
            self.write_bin()
            if compression:
                self.compress()

    def write_bin(self):
        # JOO6EF.PAE8N/heart_rate.json              meta from DataFileColumn
        # JOO6EF.PAE8N/slices/heart_rate.pkl.gz     slices dict
        # loop slices:
        #   slice.save()
        # It is very important to use fp.flush() and os.fsync(fp) to make sure the data is immeadiately written
        # to the hard drive. Otherwise it can take up to 35 s for the data to actually be written. This would
        # cause data loss when unplugging the device in this time period.

        if not self.df.consistent:
            # If this is not checked, the following could happen: the data file gets loaded with 150 samples (binaries are on the harddrive) but the write pointer is at 100
            # because the db was not stored. => file is inconsistent => it would append the 150-100=50 samples AGAIN to the slice binary which is not what we want.
            self.logger.warning(f'Skipping write_bin for {self.hash_long} due to inconsistencies')
            return

        # binaries were appended, so avoid the normal write procedure (you must compare against None!)
        if self._values_bin is not None:
            self.logger.debug(f'Skipping write_bin for {self.hash_long} since binaries were appended to this slice before.')

        if self.values:
            self.last_val = float(self.values[-1])

        if len(self.values) > self.values_write_pointer:

            if self.dtype == 'uint24':
                slice_binary = DcHelper.int_list_to_uint24_lsb_first(self.values[self.values_write_pointer:])

            elif self.dtype == 'int24':
                slice_binary = DcHelper.int_list_to_int24_lsb_first(self.values[self.values_write_pointer:])

            else:
                slice_binary = np.asarray(self.values[self.values_write_pointer:], dtype=self.dtype).tobytes()

            if str(self._path).endswith('.zst'):
                try:
                    with open(str(self._path), 'rb') as fp:
                        uncompressed_binary = zstd.ZSTD_uncompress(fp.read())
                except FileNotFoundError:
                    self.logger.error(f'write_bin: decompressing with zstd failed. File {self._path} not found.')
                    # todo: ok ????? or better write to non compressed file?
                    slice_binary = b''

                with open(str(self._path), 'wb') as fp:
                    uncompressed_binary += slice_binary
                    compressed_binary = zstd.ZSTD_compress(uncompressed_binary, 2)
                    fp.write(compressed_binary)
                    fp.flush()
                    os.fsync(fp)

            elif str(self._path).endswith('.gz'):
                try:
                    with open(str(self._path), 'rb') as fp:
                        uncompressed_binary = gzip.decompress(fp.read())
                except FileNotFoundError:
                    self.logger.error(f'write_bin: decompressing with gz failed. File {self._path} not found.')
                    # todo: ok ????? or better write to non compressed file?
                    slice_binary = b''

                with open(str(self._path), 'wb') as fp:
                    uncompressed_binary += slice_binary
                    compressed_binary = gzip.compress(uncompressed_binary, 4)
                    fp.write(compressed_binary)
                    fp.flush()
                    os.fsync(fp)

            else:
                #with open(str(self._path), fp_mode) as fp:
                with open(str(self._path), 'ab') as fp:
                    # ToDo pretty sure this is fine; it's not possible that self.values changes length during the follwowing few steps!!!
                    # write values with index > self._values_write_pointer
                    fp.write(slice_binary)
                    fp.flush()
                    os.fsync(fp)

            self.values_write_pointer = len(self.values)
            self.logger.debug(f'write_bin for slice {self.hash_long} to {self._path}. values_write_pointer: {self.values_write_pointer}.')

        elif len(self.values) == self.values_write_pointer:
            self.logger.debug(f'write_bin not necessary: for slice {self.hash_long}, len(values) == values_write_pointer ({self.values_write_pointer})')

        else:
            if self.binaries_appended:
                self.logger.debug(f'write_bin {self.hash_long} skipped since binaries were appended')
            else:
                self.logger.error(f'write_bin inconsistency for slice {self.hash_long}, len(values)={len(self.values)} < values_write_pointer={self.values_write_pointer}. '
                             f'Data loss probable. Last 5 values: {self.values[-5:]}')

    def append_binary(self, byte_values):
        # appends binaries to binary buffer

        # None is the default value (!=empty bytearray !!)
        if self._values_bin is None:
            self._values_bin = bytearray()
        self._values_bin += byte_values

        # reset _values to ensure that another part of the program accessing .values has always the correct values loaded
        self._values = None

    def write_appended_binaries(self):
        # writes all in the binary buffer to the hard drive

        if not self._values_bin:
            self.logger.debug(f'Skipping write_append_binaries for {self.hash_long} since binary buffer is empty')
            return

        if str(self._path).endswith(('.zst', '.gz')):
            self.logger.error(f'write_appended_binaries {self.hash_long} cannot append binary to compressed file! {self._path.name}')
            return

        with open(str(self._path), 'ab') as fp:
            fp.write(self._values_bin)
            fp.flush()
            os.fsync(fp)

        self.values_write_pointer += int(len(self._values_bin) / self.dtype_size)
        # clear buffer (don't set this back to None!)
        self._values_bin = bytearray()
        # reset _values to ensure that another part of the program accessing .values has always the correct values loaded
        self._values = None

        current_slice_size = self.values_write_pointer * self.dtype_size
        if current_slice_size >= self.df.slice_max_size:
            self.status_slice_full = True
            if current_slice_size > self.df.slice_max_size:
                self.logger.error(f'append_binary {self.hash_long} slice is too big ({os.path.getsize(self._path)})')

        self.logger.debug(f'append_binary for slice {self.hash_long} to {self._path}. values_write_pointer: {self.values_write_pointer}.')

    def get_bin_data(self):
        # returns the binary data

        try:
            if '.zst' in str(self._path):
                with open(str(self._path), 'rb') as fp:
                    bin = zstd.ZSTD_uncompress(fp.read())

            if '.gz' in str(self._path):
                with open(str(self._path), 'rb') as fp:
                    bin = gzip.decompress(fp.read())

            else:
                with open(str(self._path), 'rb') as fp:
                    bin = fp.read()

            return bin

        except FileNotFoundError:

            return False

    def compress(self, algorithm='zstd', level=2):

        # Todo:
        #  - currently: if files are already compressed with another method, then nothing will happen
        # todo: if 'xy' in (self._path) change full path to path.name
        #  pfade überprüfen
        #  gezipptes da?
        #  wenn nicht => bin zippen
        #  was wenn beide nicht da??

        t = time.monotonic()

        if self.df.status_closed or self.status_slice_full:

            path_old = self._path

            if algorithm == 'zstd':
                # file already exists
                if str(self._path).endswith('.zst'):
                    # set this attribute again (there was a case where it was necessary due to a gatway restart before the database was stored!)
                    self.status_compressed = True
                    return True
                if str(self._path).endswith('.gz'):
                    self.logger.warning(f'compress slice {self.hash_long} stopped. Files are already compressed with another algorithm')
                    return False
                if level < 0 and level > 22:
                    self.logger.error(f'The compression level ({level}) you have specified is not valid!')
                    return False
                # compress
                try:
                    bin_data_compressed = zstd.ZSTD_compress(open(str(self._path), 'rb').read(), level)
                except FileNotFoundError:
                    self.logger.error(f'compress slice {self.hash_long} with zstd failed. File {self._path} not found')
                    return False
                except zstd.Error:
                    self.logger.error(f'compress slice {self.hash_long} with zstd failed. zstd.Error')
                    return False
                # change path
                self._path = Path(str(self._path) + '.zst')

            elif algorithm == 'gzip':
                if str(self._path).endswith('.gz'):
                    self.status_compressed = True
                    return True
                if str(self._path).endswith('.zst'):
                    self.logger.warning(f'compress slice {self.hash_long}. Files are already compressed with another algorithm')
                    return False
                if level < 0 and level > 9:
                    self.logger.error(f'The compression level ({level}) you have specified is not valid!')
                    return False
                try:
                    bin_data_compressed = gzip.compress(open(str(self._path), 'rb').read(), level)
                except FileNotFoundError:
                    self.logger.error(f'compress slice {self.hash_long} with gzip failed. File {self._path} not found')
                    return False
                # change path
                self._path = Path(str(self._path) + '.gz')

            else:
                self.logger.error('The compression algorithm you have specified is not valid! Choose "zstd" or "gzip" ')
                return False

            with open(str(self._path), 'wb') as fp:
                fp.write(bin_data_compressed)
                # do this to quickly write down the data to the hard drive
                fp.flush()
                os.fsync(fp)

            # remove raw bin data file
            os.remove(path_old)

            self.status_compressed = True
            # send compressed files again
            self.status_sent_server = False

            # set this information here (typically happens in final_analyse), just in case that the compress method was called in the send() method
            self.compressed_size_meta = self.compressed_size
            self.compression_ratio_meta = self.compression_ratio

            self.logger.debug(f'compressed slice {self.hash_long}. Old name: {path_old.name}, new name: {self._path.name}, status_compressed: {self.status_compressed}, status_sent_server: '
                         f'{self.status_sent_server}, in {round(time.monotonic()-t, 2)} sec')
        '''
        # gzip
        # level 1 ... 9 (here 4)
        compressed = gzip.compress(data_to_compress, 4)
        decompressed = gzip.decompress(compressed)
        # zstd
        # level 1 ... 22 (here 2)
        compressed = zstd.ZSTD_compress(data_to_compress, 2)
        decompressed = zstd.ZSTD_uncompress(data_to_compress)
        '''

    def send_partially_old(self, server, session):

        t = time.monotonic()
        if self.status_sent_server:
            self.logger.debug(f'not sending {str(self._path)} because it has already been sent.')
            return True

        if self.status_compressed:
            self.logger.warning(f'not partially sending {str(self._path)} because it is already compressed.')
            return False

        try:

            with open(self._path, 'rb') as fp:
                data = fp.read()

            if len(data) > self.values_send_pointer:

                self.logger.debug(f'partially sending {str(self._path)}')

                partial_data = data[self.values_send_pointer:]

                attempts = self.max_upload_attempts

                while attempts > 0:
                    attempts -= 1

                    try:
                        #print(server + '/api_v01/push_ds/' + self.df.hash_id + '/' + str(self._path.name) + '/1')
                        with session.post(server + '/api_v01/push_ds/' + self.df.hash_id + '/' + str(self._path.name) + '/1', data=partial_data, timeout=config.request_timeout) as response:
                            #print(f"Read {len(response.content)} from {url}")

                            if response.status_code == 200:

                                self.values_send_pointer = len(data)

                                if self.status_slice_full:
                                    self.status_sent_server = True

                                self.logger.debug(f'send_partially slice {self.hash_long} successful - Time elapsed: {round(time.monotonic()-t, 1)} s - Attempts: {self.max_upload_attempts-attempts}')
                                return True

                            else:
                                self.logger.error(f'send_partially slice {self.hash_long} not successful - Status code {response.status_code} - Attempts left: {attempts}')

                    except requests.exceptions.RequestException as e:
                        self.logger.warning(f'send_partially for {self.hash_long} failed with RequestException: {e} - Attempts left: {attempts}')

                else:
                    # while loop ended because attempts == 0 and no break
                    return False

            else:

                self.logger.debug(f'no new partial data to send for {str(self.hash_long)}')
                return True

        except FileNotFoundError:

            # file was not found because the data was not yet written => ok
            if self.values_write_pointer == 0:
                self.logger.warning(f'send_partially slice {self.hash_long} not yet possible. The slice data must be stored first.')
                return False

            # there should be data but was actually not found
            else:
                self.logger.error(f'send_partially slice {self.hash_long} failed. File not found! {self._path}')
                return False

    def send_old(self, server, session):

        t = time.monotonic()
        # skip if already sent to server
        if self.status_sent_server:
            self.logger.debug(f'not sending {str(self._path)} because it has already been sent.')
            return True

        df_closed = self.df.status_closed

        if not df_closed and not self.check_and_set_status_slice_full():
            self.logger.debug(f'not sending {str(self._path)}. status_slice_full: {self.check_and_set_status_slice_full()}. df_closed: {df_closed}.')
            return False

        # slice is full or file is closed => send
        try:

            # if for some reasonthe slice is not compressed (at this point it must be either full or closed)
            if not self.status_compressed:
                self.logger.debug(f'compressing slice {self.hash_long} before sending...')
                self.compress()

            self.logger.debug(f'sending {str(self._path)}. status_slice_full: {self.check_and_set_status_slice_full()}. df_closed: {df_closed}')
            with open(self._path, 'rb') as fp:
                data = fp.read()

            attempts = self.max_upload_attempts
            while attempts > 0:
                attempts -= 1
                try:
                    with session.post(server + '/api_v01/push_ds/' + self.df.hash_id + '/' + str(self._path.name) + '/0', data=data, timeout=config.request_timeout) as response:

                        if response.status_code == 200:
                            if self.status_slice_full is True or df_closed:
                                self.status_sent_server = True
                            self.logger.debug(f'send slice {self.hash_long} successful - Time elapsed: {round(time.monotonic()-t, 1)} s - Attempts: {self.max_upload_attempts-attempts}')
                            return True
                        else:
                            self.logger.error(f'send slice {self.hash_long} not successful - Status code {response.status_code} - Attempts left: {attempts}')

                except requests.exceptions.RequestException as e:
                    self.logger.warning(f'send slice {self.hash_long} failed with RequestException: {e} - Attempts left: {attempts}')
            else:
                # while loop ended because attempts == 0 and no break
                return False

        except FileNotFoundError:
            self.logger.error(f'send slice {self.hash_long} failed. File not found! {self._path}')
            return False

    def send(self, session, partially):

        t = time.monotonic()
        df_closed = self.df.status_closed

        # no api_client available
        if not self.df.api_client:
            self.logger.error(f'No api_client available {self.hash_long}')
            return False

        # skip if already sent to server
        if self.status_sent_server:
            self.logger.debug(f'not sending {self.hash_long} because it has already been sent.')
            return True

        # don't send compressed data partially
        if partially and self.status_compressed:
            self.logger.warning(f'not partially sending {self.hash_long} because it is already compressed.')
            return False

        # if not partially check close status
        if not partially and not df_closed and not self.check_and_set_status_slice_full():
            self.logger.debug(f'not sending {self.hash_long}. status_slice_full: {self.check_and_set_status_slice_full()}. df_closed: {df_closed}.')
            return False

        # uncompressed partially or
        # slice is full or file is closed
        #   => send

        # read file
        try:

            # check compression state if not partially (at this point it must be either full or closed)
            if not partially and not self.status_compressed:
                self.logger.debug(f'compressing slice {self.hash_long} before sending...')
                self.compress()

            with open(self._path, 'rb') as fp:
                data = fp.read()

        except FileNotFoundError:
            # file was not found because the data was not yet written => ok
            if self.values_write_pointer == 0:
                self.logger.warning(f'send_partially slice {self.hash_long} not yet possible. The slice data must be stored first.')
                return False
            # there should be data but was actually not found
            else:
                self.logger.error(f'send_partially slice {self.hash_long} failed. File not found! {self._path}')
                return False

        # if it's partially check that data's length > send pointer
        if not partially or len(data) > self.values_send_pointer:

            if partially:
                partially_str = 'partially '
            else:
                partially_str = ''
            self.logger.debug(f'{partially_str}sending {str(self._path)}. status_slice_full: {self.check_and_set_status_slice_full()}. df_closed: {df_closed}')

            if partially:
                data_to_send = data[self.values_send_pointer:]
            else:
                data_to_send = data

            # do the server request
            if partially:
                req_url = 'push_ds/' + self.df.hash_id + '/' + str(self._path.name) + '/1'
            else:
                req_url = 'push_ds/' + self.df.hash_id + '/' + str(self._path.name) + '/0'
            req_result = self.df.api_client.request(req_url,
                                           data=data_to_send,
                                           timeout=config.request_timeout,
                                           log_time=True,
                                           attempts=self.max_upload_attempts)

            # not successful
            if not req_result:
                return False

            if partially:
                self.values_send_pointer = len(data)
                if self.status_slice_full is True:
                    self.status_sent_server = True

            else:
                if self.status_slice_full is True or df_closed:
                    self.status_sent_server = True

            self.logger.debug(f'send slice {self.hash_long} successful - Time elapsed: {round(time.monotonic()-t, 1)} s')
            return True

    def lazy_load(self):

        if self.df.avoid_lazy_load and self.slice_type == 'y':
            # only avoid y since x is necessary for duration and is appended as real values
            self.logger.debug(f'lazy_load {self.hash_long} skipped')
            return np.asarray([])

        t = time.monotonic()
        try:
            self.logger.debug(f'lazy_load {self.hash_long}')

            with open(str(self._path), 'rb') as fp:
                slice_binary = fp.read()

            if '.zst' in str(self._path):
                slice_binary = zstd.ZSTD_uncompress(slice_binary)
            if '.gz' in str(self._path):
                slice_binary = gzip.decompress(slice_binary)

            if self.data_type.startswith('eeg') and self.slice_type == 'y':
                np_array = np.asarray(DcHelper.int24_msb_first_to_int_list(slice_binary), dtype='int32')
                # conversion of Smarting EEG data
                vref = 4.5
                gain = 24
                scale_factor = (vref / (2**23 - 1)) / gain
                np_array = np_array * scale_factor * 1e+6
            elif self.data_type.startswith('gyro') and self.slice_type == 'y':
                np_array = np.frombuffer(slice_binary, dtype=self.dtype)
                # conversion of Smarting Gyroscope data
                np_array = np_array * 250 / 32768
            else:
                if self.dtype == 'uint24':
                    np_array = np.asarray(DcHelper.uint24_lsb_first_to_int_list(slice_binary), dtype='uint32')
                elif self.dtype == 'int24':
                    np_array = np.asarray(DcHelper.int24_lsb_first_to_int_list(slice_binary), dtype='int32')
                else:
                    np_array = np.frombuffer(slice_binary, dtype=self.dtype)

            # integrity check
            if len(np_array) == self.values_write_pointer:
                self.logger.debug('lazy_load slice {}, samples {} in {} sec'.format(self.hash_long, len(np_array), round(time.monotonic()-t, 1)))
            # data is not as long as expected.
            # Should never happen on Gateway (uses df.live_data=True)!
            elif self.df.live_data:
                self.df.consistent = False
                self.logger.error('lazy_load {} inconsistent: expected samples {}, real samples {}, in {} sec'.format(
                    self.hash_long, self.values_write_pointer, len(np_array), round(time.monotonic()-t, 1)))
            # can be ok on the server
            else:
                self.logger.warning('lazy_load {} inconsistent: expected samples {}, real samples {}, in {} sec'.format(
                    self.hash_long, self.values_write_pointer, len(np_array), round(time.monotonic()-t, 1)))

            return np_array

        except FileNotFoundError:
            self.logger.warning(f'Lazy_load. slice file {str(self._path)} not found. Returning empty value.')
            if self.dtype == 'uint24':
                return np.asarray([], dtype='uint32')
            elif self.dtype == 'int24':
                return np.asarray([], dtype='int32')
            else:
                return np.frombuffer(b'', dtype=self.dtype)
            #raise FileNotFoundError

        # when trying to decompress empty byte array b''
        except zstd.Error as e:
            self.logger.error(f'Lazy_load: ZSTD.ERROR:\n{e}')
            if self._path.exists:
                exists = 'True'
                size = os.path.getsize(self._path)
            else:
                exists = 'False'
                size = 0
            self.logger.error(f'Lazy_load: ZSTD.ERROR with slice {str(self._path)}. Exists: {exists}, size: {size}. Returning empty value.')
            if self.dtype == 'uint24':
                return np.asarray([], dtype='uint32')
            elif self.dtype == 'int24':
                return np.asarray([], dtype='int32')
            else:
                return np.frombuffer(b'', dtype=self.dtype)

    @property
    def file_exists(self):

        return self._path.is_file()

    # todo: what is this function doing? self.m does not exist => remove?
    def dump(self):

        self.final_analyse()

        return dict(self.m)

    def final_analyse(self):
        '''
        consistency checks
        calc:
                'min': None,
                'max': None,
                'mean': None,
        '''

        # todo: .samples VS .samples_meta // .bin_size VS .bin_size_meta ?!?

        # if slice is full and has already been analyzed => do not analyze again to save performance
        if self.status_finally_analzyed:
            self.logger.debug(f'final_analyse of {self.hash_long} with {self.samples_meta} samples skipped because it is full and already analyzed')
            return

        # That means that binaries were appended.
        # Avoid final analyses if the slice is not full to save processing power (these slices will be full quit fast though...)
        if self.binaries_appended and not self.status_slice_full:
            self.logger.debug(f'skipping final analyse for {self.hash_long} due to appended binaries')
            return

        # appended binaries
        # >> less to analyse only huge data types which are not meant for being sent with json or statistics (median, ...)!
        # & definitely stored to hard drive
        if self.binaries_appended and self.values_analyse_pointer < self.values_write_pointer:
            self.logger.debug(f'final_analyse appended_bin: {self}')

            self.values_analyse_pointer = self.values_write_pointer
            self.samples_meta = self.values_write_pointer
            self.bin_size_meta = self.samples_meta * self.dtype_size
            self.compressed_size_meta = self.compressed_size
            self.compression_ratio_meta = self.compression_ratio
            self._check_status_full_and_free_memory()

        # "normal" data
        else:
            # any new data since last call of self.final_analyse()?
            if self.values_analyse_pointer < len(self.values):
                # todo: is it save to assume that all data was stored to the hard drive before final_analyse is called?
                #  then the code from above could be used...
                self.logger.debug(f'final_analyse: {self}')

                self.values_analyse_pointer = len(self.values)
                self.samples_meta = len(self.values)
                self.bin_size_meta = self.samples_meta * self.dtype_size
                self.compressed_size_meta = self.compressed_size
                self.compression_ratio_meta = self.compression_ratio

                if self.slice_type == 'y' and config.data_types_dict[self.data_type]['box_plot']:

                    # convert to normal Python Int/Float because mongodb/engine cannot handle uint8 in float fields!
                    # (whereas Python int is ok in a float field)
                    if self.values:
                        self.min = float(min(self.values))
                        self.max = float(max(self.values))
                        self.mean = float(sum(self.values) / self.samples_meta)
                    else:
                        self.min = None
                        self.max = None
                        self.mean = None

                self._check_status_full_and_free_memory()

    def _check_status_full_and_free_memory(self):
        # this method is used in final_analyze (only)
        # instead of calling check_and_set_status_slice_full() method (which depends on bin_size which was
        # calculated a few lines above) just do the comparison here
        if self.bin_size_meta >= self.df.slice_max_size:
            self.status_slice_full = True
            self.status_finally_analzyed = True

            # allow to free memory of the data_types which are not used for further calculations (col.final_analyze)
            # => free all non-boxplot data_types
            # => free the time_rec of boxplot data_types
            if self.df.free_slices_when_finally_analysed and \
                    (not config.data_types_dict[self.data_type]['box_plot'] or self.slice_type == 'time_rec'):
                freed_mem = DcHelper.file_size_str(len(self._values)*self.dtype_size) if self._values else 0
                self._values = None
                self.logger.debug(f'freed {freed_mem} of memory of sl {self.hash_long} after finally analyzing it.')


    def check_and_set_status_slice_full(self):
        # if the slice is full, then change its status
        # this may cause a load of sl.values and a lazyload

        if self.bin_size >= self.df.slice_max_size:
            self.status_slice_full = True
            return True
        else:
            self.status_slice_full = False
            return False






