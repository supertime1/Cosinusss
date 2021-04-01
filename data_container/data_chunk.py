from pathlib import Path
import numpy as np
from datetime import datetime, timezone, timedelta
from mongoengine import EmbeddedDocument, ListField, BooleanField, MapField, DictField, FloatField, IntField, \
    DateTimeField, EmbeddedDocumentField, StringField

# import package modules
from .dc_helper import DcHelper, ClassContainer
utc_to_local = DcHelper.utc_to_local
from . import config


class DataChunkCol(EmbeddedDocument):

    min = FloatField(db_field='min', null=True)
    max = FloatField(db_field='max', null=True)
    mean = FloatField(db_field='avg', null=True)
    median = FloatField(db_field='med', null=True)
    upper_quartile = FloatField(db_field='uqrt', null=True)
    lower_quartile = FloatField(db_field='lqrt', null=True)
    samples = IntField(default=0, db_field='s')

    _slices_y = ListField(DictField(), db_field='s_y')
    _slices_time_rec = ListField(DictField(), db_field='s_tr')

    def __init__(self, *args, **kwargs):

        super(DataChunkCol, self).__init__(*args, **kwargs)
        self.df = None
        self.logger = config.logger

    def __str__(self):

        format_str = 'DataChunkCol(hash_long={}, min={}, max={}, mean={}, median={}, upper_quartile={}, lower_quartile={}, samples={}, _slices_y={}, _slices_time_rec={})'
        return format_str.format(
            self.hash_long,
            self.min,
            self.max,
            self.mean,
            self.median,
            self.upper_quartile,
            self.lower_quartile,
            self.samples,
            self._slices_y,
            self._slices_time_rec,
        )

    def _init(self, df, chunk):
        # call this for reconstructing from the database
        self.df = df
        self.chunk = chunk
        # self.logger.debug(f'init chunk_col: med {self.median}, up_qt {self.upper_quartile}, l_qt {self.lower_quartile}, mix {self.min}, max {self.samples}, mean {self.mean}')
        # self.logger.debug(f'init chunk_col: {self._slices_y}, {self._slices_time_rec}')

    def _initialize_chunk_col(self, chunk_col_dict, df, chunk, labelled=False):
        # used, when creating the chunk_col the very first time
        # todo this is only needed for labelled chunks => change labelled chunks to also do this on the fly

        self.df = df
        self.chunk = chunk
        self._slices_y = chunk_col_dict['y']
        self._slices_time_rec = chunk_col_dict['time_rec']

        setattr(self, 'samples', chunk_col_dict['samples'])

        if not labelled:
            for key in ['median', 'upper_quartile', 'lower_quartile', 'min', 'max', 'mean']:
                setattr(self, key, chunk_col_dict[key])

        # self.logger.debug(f'_initialize chunk_col: med {self.median}, up_qt {self.upper_quartile}, l_qt {self.lower_quartile}, min {self.min}, max {self.max}, samples {self.samples}, '
        #              f'mean {self.mean}')
        # self.logger.debug(f'_initialize chunk_col: {self._slices_y}, {self._slices_time_rec}')

    @property
    def all_slices(self):
        slices = []
        if self._slices_y:
            for sl_info in self._slices_y:
                slices.append(self.df.get_slice(sl_info['hash']))
        if self._slices_time_rec:
            for sl_info in self._slices_time_rec:
                slices.append(self.df.get_slice(sl_info['hash']))

        return slices

    @property
    def data_type(self):
        # determine the data_type with the first y slice
        if self._slices_y:
            sl_hash = self._slices_y[0]['hash']
            return self.df.get_slice(sl_hash).data_type
        else:
            return None

    @property
    def dtype_y_numpy(self):
        # used for numpy conversion
        data_type = self.data_type
        if self.data_type:
            dtype_df = config.data_types_dict[data_type]['dtype']
        else:
            # default if nothing works
            return 'float64'

        if dtype_df == 'uint24':
            dtype = 'uint32'
        elif dtype_df == 'int24':
            dtype = 'int32'
        else:
            dtype = dtype_df

        return dtype

    @property
    def dtype_x_numpy(self):
        # used for numpy conversion
        data_type = self.data_type
        if self.data_type:
            dtype_df = config.data_types_dict[data_type]['dtype_time']
        else:
            # default if nothing works
            return 'float64'

        return dtype_df

    @property
    def hash_long(self):
        return self.chunk.hash_long + '.' + str(self.data_type)

    @property
    def x(self):

        try:
            time_rec = []

            if self._slices_time_rec:
                for sl_info in self._slices_time_rec:
                    sl = self.df.get_slice(sl_info['hash'])
                    time_rec += sl.values[sl_info['i_start']:sl_info['i_end']]

        # Chunk not finalized and probably therefore end indices not existing
        except KeyError:
            self.logger.warning(f'ChunkCol.x {self.hash_long} indices probably not complete ({self._slices_y}). Chunk finalized: {self.chunk.finalized}. Returning empty val.')
            time_rec = []

        return np.asarray(time_rec, dtype=self.dtype_x_numpy)

    @property
    def x_offset(self):

        if self.chunk.time_offset:
            return self.x - self.chunk.time_offset
        else:
            self.logger.warning(f'x_offset: The time_offset attribute for this chunk (index {self.chunk.index}) does not exist. Returning self.x.')
            return self.x

    @property
    def y(self):

        try:
            y = []

            if self._slices_y:
                for sl_info in self._slices_y:
                    sl = self.df.get_slice(sl_info['hash'])
                    y += sl.values[sl_info['i_start']:sl_info['i_end']]

        except KeyError:
            self.logger.warning(f'ChunkCol.y {self.hash_long} indices probably not complete ({self._slices_y}). Chunk finalized: {self.chunk.finalized}. Returning empty val.')
            y = []

        return np.asarray(y, dtype=self.dtype_y_numpy)

    @property
    def slices_available(self):

        if self._slices_y:
            for sl_info in self._slices_y:
                sl = self.df.get_slice(sl_info['hash'])
                # calculate which data is already available based on the byte length of the send pointer
                available_samples = int(sl.values_send_pointer / DcHelper.helper_dtype_size(sl.dtype))
                if sl_info['i_end'] > available_samples:
                    return False

        if self._slices_time_rec:
            for sl_info in self._slices_time_rec:
                sl = self.df.get_slice(sl_info['hash'])
                available_samples = int(sl.values_send_pointer / DcHelper.helper_dtype_size(sl.dtype))
                if sl_info['i_end'] > available_samples:
                    return False

        return True

    def stats_json(self):

        stats_json_dict = {
            'min': self.min,
            'max': self.max,
            'mean': self.mean,
            'median': self.median,
            'upper_quartile': self.upper_quartile,
            'lower_quartile': self.lower_quartile,
            'samples': self.samples
        }

        return stats_json_dict


class DataChunk(EmbeddedDocument):

    # todo: remove primary key but be careful when importing old datafiles, this can cause errors
    #  > think wisely before doing this
    index = IntField(primary_key=True)

    # todo: not needed! Test if there are any problems removing this ...
    status_closed = BooleanField(default=False, db_field='stc')

    label = StringField()

    _date_time_start = DateTimeField(db_field='dts')
    _date_time_end = DateTimeField(db_field='dte')
    _date_time_modified = DateTimeField(db_field='dtm')
    duration = FloatField(db_field='dur')
    time_offset = FloatField(db_field='toff')
    finalized = BooleanField(default=False, db_field='fin')
    # if the measurement is of good quality, "valid" can be set to true
    valid = BooleanField(default=False, db_field='val')

    cols = MapField(EmbeddedDocumentField('DataChunkCol'))

    def __init__(self, *args, **kwargs):
        """docstring description
            Don't manually change any attributes in the chunk
        Args:
            param1: The first parameter.
            param2: The second parameter.

        Returns:
            The return value. True for success, False otherwise.

        """

        super(DataChunk, self).__init__(*args, **kwargs)

        # self._cols = {}
        self.logger = config.logger
        self.df = None
        self.c = None

    def __str__(self):
        format_str = '{}(df_hash={}, index={}, date_time_start={}, time_offset={}, duration={})'
        return format_str.format(
            self.chunk_type,
            self.df.hash_id,
            self.index,
            self.date_time_start,
            self.time_offset,
            self.duration
        )

    def _init(self, df):
        # call this for reconstructing from the database

        self.df = df
        self.c = ClassContainer()

        for data_type in self.cols:
            col = self.cols[data_type]
            col._init(df=df, chunk=self)
            setattr(self.c, data_type, col)

    @property
    def date_time_start(self):
        return utc_to_local(self._date_time_start, self.df.project.timezone)

    @date_time_start.setter
    def date_time_start(self, datetime_new):
        # todo: make sure datetime_new is UTC?
        self._date_time_start = datetime_new

    @property
    def date_time_end(self):
        return utc_to_local(self._date_time_end, self.df.project.timezone)

    @date_time_end.setter
    def date_time_end(self, datetime_new):
        # todo: make sure datetime_new is UTC?
        self._date_time_end = datetime_new

    @property
    def date_time_modified(self):
        return utc_to_local(self._date_time_modified, self.scope.timezone)

    @date_time_modified.setter
    def date_time_modified(self, datetime_new):
        # todo: make sure datetime_new is UTC?
        self._date_time_modified = datetime_new

    @property
    def hash_long(self):
        return self.df.hash_id + '/' + str(self.chunk_type) + '.' + str(self.index)

    @property
    def chunk_type(self):

        if self.label:
            if self.duration == 0:
                chunk_type = 'Marker'
            else:
                chunk_type = 'LabelledChunk'
        else:
            chunk_type = 'DataChunk'

        return chunk_type

    def to_json(self):

        stats_json_dict = {}

        for data_type in self.cols:

            if config.data_types_dict[data_type]['send_json']:
                stats_json_dict[data_type] = self.cols[data_type].stats_json()

            stats_json_dict['date_time_start'] = str(self.date_time_start)
            stats_json_dict['time_offset'] = self.time_offset
            stats_json_dict['duration'] = self.duration

        return stats_json_dict

    def y(self, data_type):
        pass
        # if closed then == .c.ppg_ir.y
        # otherwise _cols[][][]...
        # 28800

    def _initialize_data_chunk(self, df, index):
        # call this right after the first instantiation of this chunk => df.chunk_start (and then never again)

        self.df = df
        self.index = index

        # iterate DataColumns
        for data_type in self.df.cols:
            # check if there are any slices alreay
            if self.df.cols[data_type]._slices_y:
                self._add_new_col(data_type)

    def _add_new_col(self, data_type, index=None, following_chunk=None):
        # index: index==0 is used when a data_type was added between df.chunk_start() and df.chunk_stop()
        # following_chunk: If this is passed, it means that the col is added retrospectively

        df_data_col = self.df.cols[data_type]

        if data_type not in self.cols:
            self.cols[data_type] = DataChunkCol()

        if df_data_col.time_slices_ref:
            data_col_ref = self.df.cols[df_data_col.time_slices_ref]
            sl_dict = {
                '_slices_y': data_col_ref._slices_y,
                '_slices_time_rec': data_col_ref._slices_time_rec,
            }
        else:
            data_col_ref = None
            sl_dict = {
                '_slices_y': df_data_col._slices_y,
                '_slices_time_rec': df_data_col._slices_time_rec,
            }
        # normal way
        if following_chunk is None:

            for sl_type in sl_dict:

                if sl_dict[sl_type]:

                    # it's a new column (no data yet), so start with very first slice and i_start = 0
                    if index == 0:

                        # combined cols
                        if sl_type in ['_slices_time_rec'] and data_col_ref:
                            # find referenced column and first time slice for time rec for combined cols
                            sl = data_col_ref._slices_time_rec[0]

                        # not combined cols
                        else:
                            if sl_type == '_slices_y':
                                sl = df_data_col._slices_y[0]
                            else:
                                sl = df_data_col._slices_time_rec[0]

                        getattr(self.cols[data_type], sl_type).append({'hash': sl._hash, 'i_start': 0})

                    # there is already data in the column => take the last slice and set i_start to current "values"-pointer
                    else:

                        # combined cols
                        if sl_type in ['_slices_time_rec'] and data_col_ref:
                            # find referenced column and last time slice for time rec for combined cols
                            sl = data_col_ref._slices_time_rec[-1]

                        # not combined cols
                        else:
                            if sl_type == '_slices_y':
                                sl = df_data_col._slices_y[-1]
                            else:
                                sl = df_data_col._slices_time_rec[-1]

                        getattr(self.cols[data_type], sl_type).append({'hash': sl._hash, 'i_start': self._get_slice_length(sl)})

        # adding slice information retrospectively is different (similar to adding labelled_chunks)
        else:
            # x-values and date_time_start of the chunks must be correct! Otherwise there will be no data in these chunks!

            # calculate times for finding the data indices in x and y
            time_start = self.date_time_start - self.df.date_time_start
            self.time_start = time_start.total_seconds()
            time_end = following_chunk.date_time_start - self.df.date_time_start
            self.time_end = time_end.total_seconds()

            if self.time_start > self.time_start:
                self.logger.error(f'When adding the empty col {data_type} retrospectively, date_time_start {self.date_time_start} of current slice is bigger than date_time_start of the following slice '
                             f'{following_chunk.date_time_start}.')
                raise ChunkTimeError

            time_rec_dict_list = self._find_x_indices_for_data_type(data_type=data_type)

            if time_rec_dict_list:

                _, y_i_start, sl_hash_start = self._find_y_index_from_x_index(
                    data_type, time_rec_dict_list[0]['sl_number'], time_rec_dict_list[0]['i_start'])

                self.cols[data_type]._slices_y.append({'hash': sl_hash_start, 'i_start': y_i_start})
                self.cols[data_type]._slices_time_rec.append({'hash': time_rec_dict_list[0]['hash'], 'i_start': time_rec_dict_list[0]['i_start'],})

            else:
                # there is no data in this slice!!
                self.logger.debug(f'There is no data in the range of {self.date_time_start} - {self.date_time_end} for a data chunk! Not creating col.')

    def finalize(self, chunk_list_index=None):
        # chunk_list_index: the index of THIS chunk inside the df.chunks list. When passing this parameter it is
        # assumed that you want to finalize a chunk which was not correctly finalized (e.g. gateway power unplugged)

        # the last chunk
        if chunk_list_index is not None and chunk_list_index != len(self.df.chunks)-1:
            # retrospectively means that a chunk which is not the last one in df.chunks
            # (=> it has been "forgotten" to be finalized at some point)
            retrospectively = True
        else:
            retrospectively = False

        # An old wise man once said: "Do not finalize twice."
        if self.finalized:
            return True

        # the "typical" way
        if not retrospectively:

            if self.df.live_data:
                # todo: this can be delayed if the gateway has to process a lot...
                self.date_time_end = datetime.now(timezone.utc)
            else:
                self.date_time_end = self.df.date_time_start + timedelta(seconds=self.df.last_time_val)

            following_chunk = None

        else:

            if chunk_list_index is None:
                self.logger.warning(f'Cannot finalize chunk retrospectively with no chunk_list_index_provided.')
                return False

            if chunk_list_index == len(self.df.chunks)-1:
                self.logger.warning(f'Cannot finalize chunk retrospectively with index {chunk_list_index} beacuse it is the last chunk in df.chunks. Use df.chunk_stop() instead.')
                return False

            following_chunk = self.df.chunks[chunk_list_index+1]
            self.date_time_end = following_chunk.date_time_start

        self.date_time_modified = datetime.now(timezone.utc)
        self.duration = round((self.date_time_end - self.date_time_start).total_seconds(), 2)
        self.time_offset = (self.date_time_start - self.df.date_time_start).total_seconds()
        self.finalized = True

        self._process_all_data_types_for_data_chunks(following_chunk=following_chunk)
        self.build_instances()

    def _process_all_data_types_for_data_chunks(self, following_chunk=None):
        # if following_chunk != None => analyse retrospectively

        # iterate DataColumns to find the slices and indices that belong to this chunk
        for data_type in self.df.cols:

            df_data_col = self.df.cols[data_type]

            # add data_types that did not exist when chunk_start() was called
            if data_type not in self.cols:
                # only if the slices contain data
                if self.df.cols[data_type]._slices_y:
                    self._add_new_col(data_type, index=0, following_chunk=following_chunk)

                # 2. condition: when creating the chunks retrospectively, this may happen
                # 3. condition: If this data_type had no slices at chunk_start() and STILL has no single value
                # => make an empty chunk col so that there is no error when accessing it later on
                # todo: do this for the labelled_chunks, too ?
                if data_type not in self.cols or not self.cols[data_type] or not self.df.cols[data_type]._slices_y:
                    self.cols[data_type] = DataChunkCol()
                    self.cols[data_type]._slices_y = None
                    self.cols[data_type]._slices_time_rec = None
                    self._set_empty_values(data_type)
                    continue

            # columns without time-reference
            if not df_data_col.time_slices_ref:
                sl_dict = {
                    '_slices_y': df_data_col._slices_y,
                    '_slices_time_rec': df_data_col._slices_time_rec,
                }
            # columns with time-reference
            else:
                data_col_ref = self.df.cols[df_data_col.time_slices_ref]
                # columns with time-reference have empty dict entries for time_rec in ._slices
                sl_dict = {
                    '_slices_y': df_data_col._slices_y,
                    '_slices_time_rec': data_col_ref._slices_time_rec,
                }

            # todo: this part is maybe not so nice with the return values and how the data is passed to _evaluate_values_list() ...
            if not following_chunk:
                values_list, samples = self._create_value_list_and_find_indices(data_type, sl_dict)
            else:
                # todo this method may still provoke lazy_loads (but should not happen very often). Optimize for using less sl.values
                values_list = self._retrospectively_creat_value_list_and_find_indices(data_type, sl_dict, following_chunk)
                samples = None

            self._evaluate_values_list(values_list, data_type, samples)

    def _create_value_list_and_find_indices(self, data_type, sl_dict):

        # values in this list are used to calculating median, min, max, ... of this chunk
        values_list = []
        # samples is equivalent to len(values) BUT the y values of data intensive data_types must be avoided to be lazyloaded...
        samples = 0

        # add slices - y and time_rec
        for sl_type in sl_dict:

            start_adding_slices = False

            if not getattr(self.cols[data_type], sl_type):
                continue

            # hash of the first slice in this chunk
            sl_hash = getattr(self.cols[data_type], sl_type)[0]['hash']

            for sl in sl_dict[sl_type]:

                # start with the slice where the chunk started
                if sl_hash == sl._hash:
                    start_adding_slices = True
                    # save current position of the _values pointer
                    getattr(self.cols[data_type], sl_type)[0]['i_end'] = self._get_slice_length(sl)

                    if sl_type == '_slices_y':
                        # count the samples
                        samples += self._get_slice_length(sl) - getattr(self.cols[data_type], sl_type)[0]['i_start']
                        # take all values from the start index till now for processing median ...
                        if config.data_types_dict[data_type]['box_plot'] or config.data_types_dict[data_type]['send_json']:
                            values_list += sl.values[getattr(self.cols[data_type], sl_type)[0]['i_start']:]

                # more slices in case there was a new slice created between chunk_start() and chunk_stop()
                elif start_adding_slices:
                    getattr(self.cols[data_type], sl_type).append({'hash': sl._hash, 'i_start': 0, 'i_end': self._get_slice_length(sl)})
                    if sl_type == '_slices_y':

                        samples += self._get_slice_length(sl)

                        if config.data_types_dict[data_type]['box_plot'] or config.data_types_dict[data_type]['send_json']:
                            values_list += sl.values

        return values_list, samples

    def _slice_values_needed(self, sl_type, data_type):
        # check if necessary to analyse the sl.values for median etc... (to avoid unnecessary lazy_loads)

        if sl_type == '_slices_y':# and (config.data_types_dict[data_type]['box_plot'] or config.data_types_dict[data_type]['send_json']):
            return True
        else:
            return False

    def _get_slice_length(self, sl):
        # if this is a slice with appended binaries, then avoid loading the values

        # binaries were appended
        if sl.binaries_appended:
            return sl.values_write_pointer
        else:
            return len(sl.values)

    def build_instances(self):

        self.c = ClassContainer()

        # for data_type in self._cols:
        for data_type in self.cols:

            self.cols[data_type].df = self.df
            self.cols[data_type].chunk = self
            setattr(self.c, data_type, self.cols[data_type])

    def _retrospectively_creat_value_list_and_find_indices(self, data_type, sl_dict, following_chunk):

        # values in this list are used to calculating median, min, max, ... of this chunk
        values_list = []

        # add slices - y and time_rec
        for sl_type in sl_dict:

            start_adding_slices = False
            skip_other_slices = False

            if not getattr(self.cols[data_type], sl_type):
                continue

            # hash of the first slice in this chunk
            sl_hash = getattr(self.cols[data_type], sl_type)[0]['hash']

            # get useful information of the following slice
            following_chunk_col = following_chunk.cols[data_type]
            if sl_type == 'y':
                first_sl_following_chunk = self.df.get_slice(following_chunk_col._slices_y[0]['hash'])
            else:
                first_sl_following_chunk = self.df.get_slice(following_chunk_col._slices_time_rec[0]['hash'])

            for sl in sl_dict[sl_type]:

                if skip_other_slices:
                    break

                # start with the slice where the chunk started
                if sl_hash == sl._hash:
                    start_adding_slices = True

                    # the next chunk starts with the same slice => use the indices of the following slice
                    if first_sl_following_chunk.hash == sl.hash:
                        if sl_type == 'y':
                            getattr(self.cols[data_type], sl_type)[0]['i_end'] = following_chunk_col._slices_y[0]['i_start']
                        else:
                            getattr(self.cols[data_type], sl_type)[0]['i_end'] = following_chunk_col._slices_time_rec[0]['i_start']
                        skip_other_slices = True
                    # the next chunk starts with a new slice => use the whole slice
                    else:
                        # todo: this len(sl.values) could be optimized by using the theoretical maximum length of the slice to make it
                        #  then lazy_load can be avoided (slices with accidentally too many samples would be excluded thoug)
                        getattr(self.cols[data_type], sl_type)[0]['i_end'] = len(sl.values)

                    if sl_type == 'y':
                        # take all values from the start index till now for processing median ...
                        values_list += sl.values[getattr(self.cols[data_type], sl_type)[0]['i_start']:getattr(self.cols[data_type], sl_type)[0]['i_end']]

                # more slices in case there was a new slice created between chunk_start() and chunk_stop()
                elif start_adding_slices:

                    current_indices = {
                        'hash': sl.hash,
                        'i_start': 0,
                    }

                    # this is the final slice
                    if sl.hash == first_sl_following_chunk.hash:

                        if sl_type == 'y':
                            current_indices['i_end'] = following_chunk_col._slices_y[0]['i_start']
                        else:
                            current_indices['i_end'] = following_chunk_col._slices_time_rec[0]['i_start']
                        skip_other_slices = True

                    # the next chunk starts again with a new slice => use the whole slice
                    else:
                        current_indices['i_end'] = len(sl.values)

                    getattr(self.cols[data_type], sl_type).append(current_indices)

                    if sl_type == 'y':
                        values_list += sl.values[:current_indices['i_end']]

        return values_list

    def _evaluate_values_list(self, values_list, data_type, samples):
        # samples is equivalent to len(values_list) but is passed for the data intensive data_types like ppg, eeg, ...

        # both conditions necessary because 'battery' needs the send_json / 'acc_' needs the box_plot option
        if config.data_types_dict[data_type]['box_plot'] or config.data_types_dict[data_type]['send_json']:

            if values_list:
                values_list.sort()
                length = len(values_list)
                half = int(length / 2)
                first_quarter = int(length / 4)
                third_quarter = int(length * 3 / 4)

                self.cols[data_type].median = round(float(values_list[half]), 2)
                self.cols[data_type].upper_quartile = round(float(values_list[third_quarter]), 2)
                self.cols[data_type].lower_quartile = round(float(values_list[first_quarter]), 2)
                self.cols[data_type].min = round(float(min(values_list)), 2)
                self.cols[data_type].max = round(float(max(values_list)), 2)
                self.cols[data_type].samples = length
                self.cols[data_type].mean = round(float(sum(values_list) / length), 2)

            else:
                self._set_empty_values(data_type)

        else:

            if samples:
                self._set_empty_values(data_type)
                # ppg etc only needs info about samples
                self.cols[data_type].samples = samples
            else:
                self._set_empty_values(data_type)

    def _set_empty_values(self, data_type):

        self.cols[data_type].median = None
        self.cols[data_type].upper_quartile = None
        self.cols[data_type].lower_quartile = None
        self.cols[data_type].min = None
        self.cols[data_type].max = None
        self.cols[data_type].samples = 0
        self.cols[data_type].mean = None

    # Labelled chunks and markers starting from here

    def _inizialize_labelled_chunk(self, df, index, label, time_start, time_end):
        # call this right after the first instantiation of this chunk (and then never again)

        self.df = df
        self.index = index
        self.label = label

        # this is not date_time_start! these 2 times are only necessary for defining this chunk.
        # They will not be stored in the database and therefore get lost.
        self.time_start = time_start
        self.time_end = time_end
        self._create_label_chunks()

    def _create_label_chunks(self):

        self._check_time_format()

        # only do data processing if a time range is given (otherwise this is just a marker)
        if self.time_end is not None:

            self.c = ClassContainer()

            if not self.df.cols:
                self.logger.warning(f'Did not create labelled chunk with label "{self.label}", '
                               f'time {self.time_start} - {self.time_end}. No data in this data file yet.')
                raise ChunkNoValuesError

            for data_type in self.df.cols:

                chunk_col = DataChunkCol()
                time_rec_dict_list = self._find_x_indices_for_data_type(data_type, first_start_only=True)
                if time_rec_dict_list:
                    chunk_col_dict = self.create_chunk_col_dict_from_x_indices(time_rec_dict_list, data_type)
                    # todo change this also to
                    if chunk_col_dict['y']:
                        chunk_col._initialize_chunk_col(chunk_col_dict=chunk_col_dict, df=self.df, chunk=self, labelled=True)
                        self.cols[data_type] = chunk_col
                        setattr(self.c, data_type, self.cols[data_type])

            # all cols are empty... => discard this chunk
            if not self.cols:
                self.logger.warning(f'Did not create labelled chunk with label "{self.label}", time {self.time_start} - {self.time_end}. No data points in the selected timeframe.')
                raise ChunkNoValuesError

    def _find_x_indices_for_data_type(self, data_type, first_start_only=False):
        # first_start_only: if True, then only the indices of the first slices are searched

        # list of dictionaries with slice hashes and start/stop indices
        time_rec_dict_list = []

        # combined slices?
        if self.df.cols[data_type].time_slices_ref:
            data_type = self.df.cols[data_type].time_slices_ref

        start_slice_found = False
        # first find all time_rec indices and hashes and store in a list of dictionaries
        for sl_number_time_rec in range(len(self.df.cols[data_type]._slices_time_rec)):

            # current time_rec slice
            sl_time_rec = self.df.cols[data_type]._slices_time_rec[sl_number_time_rec]

            # find maximum time in this slice
            if sl_time_rec.status_finally_analzyed:
                sl_last_time_value = sl_time_rec.last_val
            # this is necessary in case write_bin was not performed yet!
            else:
                if sl_time_rec.values[-1]:
                    sl_last_time_value = sl_time_rec.values[-1]
                # there are no values
                else:
                    sl_last_time_value = 0

            sl_dict_time_rec = {
                'hash': None,
                'sl_number': None,
                'i_start': 0,
                'i_end': None,
            }
            # Get the first slice
            if self.time_start <= sl_last_time_value:

                start_slice_found = True
                # find first time_rec value which is greater than or equal TIME_START (tolerance of 0 seconds)
                # https://www.geeksforgeeks.org/python-get-the-index-of-first-element-greater-than-k/
                index_time_rec_start = next(x for x, val in enumerate(sl_time_rec.values) if val >= self.time_start - 0)

                sl_dict_time_rec['hash'] = sl_time_rec.hash
                sl_dict_time_rec['i_start'] = index_time_rec_start
                sl_dict_time_rec['sl_number'] = sl_number_time_rec

                # end is in current slice
                if self.time_end <= sl_last_time_value:
                    # find last time_rec value which is smaller than or equal TIME_END (tolerance of 0 seconds)
                    bool_list = [x for x, val in enumerate(reversed(sl_time_rec.values[index_time_rec_start:])) if
                                 val <= self.time_end - 0]

                    # if this list is empty, this means that time_end is bigger than the highes value in the  previous
                    # slice but in the current slice, the smalles value is smaller than this value.
                    # => the current slice can be skipped
                    if bool_list:
                        reversed_index_time_rec_end = next(iter(bool_list))
                        index_time_rec_end = len(sl_time_rec.values) - reversed_index_time_rec_end
                        sl_dict_time_rec['i_end'] = index_time_rec_end
                        time_rec_dict_list.append(sl_dict_time_rec)

                    # Start and end can be used, don't iterate through the rest...
                    break

                # end is in next slice => use all values in this remaining slice
                else:
                    if sl_time_rec.status_finally_analzyed:
                        sl_dict_time_rec['i_end'] = sl_time_rec.samples_meta
                    else:
                        sl_dict_time_rec['i_end'] = len(sl_time_rec.values)
                    time_rec_dict_list.append(sl_dict_time_rec)

            # start slice was found and the end is in one of the following slice
            elif self.time_start >= sl_last_time_value and start_slice_found and not first_start_only:

                sl_dict_time_rec['hash'] = sl_time_rec.hash
                sl_dict_time_rec['i_start'] = index_time_rec_start
                sl_dict_time_rec['sl_number'] = sl_number_time_rec

                # todo: DRY above
                # end is in current slice
                if self.time_end <= sl_last_time_value:
                    # find last time_rec value which is smaller than or equal TIME_END (tolerance of 0 seconds)
                    reversed_index_time_rec_end = next(
                        x for x, val in enumerate(reversed(sl_time_rec.values[index_time_rec_start:])) if
                        val <= self.time_end - 0)
                    index_time_rec_end = len(sl_time_rec.values) - reversed_index_time_rec_end
                    sl_dict_time_rec['i_end'] = index_time_rec_end

                    # Start and end can be used, don't iterate through the rest...
                    time_rec_dict_list.append(sl_dict_time_rec)
                    break

                # end is in next slice => use all values in this remaining slice
                else:
                    if sl_time_rec.status_finally_analzyed:
                        sl_dict_time_rec['i_end'] = sl_time_rec.samples_meta
                    else:
                        sl_dict_time_rec['i_end'] = len(sl_time_rec.values)
                    time_rec_dict_list.append(sl_dict_time_rec)

        return time_rec_dict_list

    def create_chunk_col_dict_from_x_indices(self, time_rec_dict_list, data_type):

        # find y indices based on the found time_rec values. Take the first (and if present the last) indices / slice numbers
        y_dict_list = []

        # find the very first and very last y slices and indices
        first_sl_dict = time_rec_dict_list[0]
        last_sl_dict = time_rec_dict_list[-1]

        sl_index_start, y_i_start, sl_hash_start = self._find_y_index_from_x_index(data_type, first_sl_dict['sl_number'], first_sl_dict['i_start'])
        sl_index_end, y_i_end, _ = self._find_y_index_from_x_index(data_type, last_sl_dict['sl_number'], last_sl_dict['i_end'])

        for sl_dict_time_rec in time_rec_dict_list:
            # delete unnecessary keys of the dictionary
            sl_dict_time_rec.pop('sl_number', None)

        # only one slice
        if sl_index_start == sl_index_end:

            sl_dict_y = {
                'hash': sl_hash_start,
                'i_start': y_i_start,
                'i_end': y_i_end
            }

            y_dict_list.append(sl_dict_y)

        # the start and end lie in different slices ... collect all slices in between...
        else:

            for sl_index in range(sl_index_start, sl_index_end + 1):

                sl_dict_y = {
                    'hash': None,
                    'i_start': None,
                    'i_end': None
                }

                sl = self.df.cols[data_type]._slices_y[sl_index]

                sl_dict_y['hash'] = sl.hash

                # first slice starts with determined start index
                if sl_index == sl_index_start:
                    sl_dict_y['i_start'] = y_i_start
                else:
                    # if a new slice begins use index 0
                    sl_dict_y['i_start'] = 0

                if sl_index == sl_index_end:
                    # last index ends with determined end index
                    sl_dict_y['i_end'] = y_i_end
                else:
                    # all except for the last slice will be used untill the end
                    if sl.status_finally_analzyed:
                        sl_dict_y['i_end'] = sl.samples_meta
                    else:
                        # todo what if 0 samples? => -1
                        sl_dict_y['i_end'] = len(sl.values)

                y_dict_list.append(sl_dict_y)

        samples = self._find_no_of_samples(time_rec_dict_list)

        chunk_col_dict = {'y': y_dict_list, 'time_rec': time_rec_dict_list, 'samples': samples}
        # todo
        #  what if x/y values get lost? x y dicts will not be the same

        return chunk_col_dict

    def _find_y_index_from_x_index(self, data_type, sl_number_time_rec, index_time_rec):
        # find the corresponding y-values:

        # 1) determine the number of time values (since we only know the slice and index) based on time as float64
        size_of_one_time_rec_sample = DcHelper.helper_dtype_size(config.data_types_dict[data_type]['dtype_time'])
        samples_per_full_time_rec_slice = self.df.slice_max_size / size_of_one_time_rec_sample
        number_of_time_values = int(sl_number_time_rec * samples_per_full_time_rec_slice) + index_time_rec

        # 2) From this find the number of the corresponding y slice + indices in the last slice
        size_of_one_y_sample = DcHelper.helper_dtype_size(self.df.cols[data_type].dtype)
        samples_per_full_y_slice = int(self.df.slice_max_size / size_of_one_y_sample)

        # use only the full number to find the correct slice (drop the positions after decimal point)
        #  what if slices have more samples than expected

        # important: subtract - 1 because if the slice is exactly full, the slice index would be one too high!
        sl_index_y = int((number_of_time_values-1) / samples_per_full_y_slice)
        if sl_index_y < 0:
            sl_index_y = 0

        y_index = number_of_time_values - (sl_index_y * samples_per_full_y_slice)

        sl_y = self.df.cols[data_type]._slices_y[sl_index_y]
        sl_hash = sl_y.hash

        return sl_index_y, y_index, sl_hash

    def _find_no_of_samples(self, time_rec_dict_list):

        samples = 0
        for sl_info in time_rec_dict_list:
            samples += sl_info['i_end'] - sl_info['i_start']

        return samples

    def _check_time_format(self):

        time_start = self.time_start
        time_end = self.time_end

        # time offset in seconds relative to date_time_start
        if type(time_start) in [float, int] and type(time_end) in [float, int, type(None)]:

            if time_end is not None:
                if time_start < 0 or time_end < 0:
                    self.logger.warning(f'time_start ({time_start}) and time_end ({time_end}) may not be negative.')
                    raise ChunkTimeError

                if time_start > time_end:
                    self.logger.warning(f'time_start ({time_start}) > time_end ({time_end}) not possible.')
                    raise ChunkTimeError
            else:
                if time_start < 0:
                    self.logger.warning(f'time_start ({time_start}) may not be negative.')
                    raise ChunkTimeError
            # todo: set date_time_start/end here?

        # time in datetime format
        elif type(time_start) is datetime and (type(time_end) is datetime or type(None)):

            # check for timezone awareness
            if time_start.tzinfo is None or time_start.tzinfo.utcoffset(time_start) is None:
                self.logger.warning(f'datetime objects must be timezone aware.')
                raise ChunkTimeError

            if time_end and (time_end.tzinfo is None or time_end.tzinfo.utcoffset(time_end) is None):
                self.logger.warning(f'datetime objects must be timezone aware.')
                raise ChunkTimeError

            if time_start < self.df.date_time_start:
                self.logger.warning(
                    f'datetime of time_start ({time_start}) must be higher than df.date_time_start ({self.df.date_time_start}).')
                raise ChunkTimeError

            if time_end is not None:
                if time_start > time_end:
                    self.logger.warning(f'time_start ({time_start}) must be smaller than time_end ({time_end}).')
                    raise ChunkTimeError

            # convert datetime object to offset in seconds relative to df.date_time_start
            time_start = time_start - self.df.date_time_start
            self.time_start = time_start.total_seconds()

            if time_end is not None:
                time_end = time_end - self.df.date_time_start
                self.time_end = time_end.total_seconds()

        else:
            # todo make this nicer
            self.logger.warning(
                f'The specified types for time_start ({type(time_start)}) or time_end ({type(time_end)}) do not match a supported format.')
            raise ChunkTimeError

        if time_end is not None:
            self.duration = round(self.time_end - self.time_start, 3)
        else:
            self.duration = 0
        self.time_offset = self.time_start
        self._date_time_start = self.df.date_time_start + timedelta(seconds=self.time_start)
        self._date_time_end = self._date_time_start + timedelta(seconds=self.duration)

class ChunkTimeError(Exception):
    pass


class ChunkNoValuesError(Exception):
    pass
