import pytest
from data_container.tests.conftest import new_df
from data_container.tests.testing_helper_functions import eeg_scale_factor
from data_container import DataFile
from data_container.dc_helper import DcHelper
from data_container import config
from data_container.odm import Person, Project
from datetime import datetime, timezone, timedelta
from random import randint, uniform
import numpy as np
import logging
import json
import os

logger = config.logger
logger.setLevel('DEBUG')

# todo:
#  > set_values once it is implemented ...
#  > store() and load from db
#  > server ALL automatically?
#  > number of columns
#  > delete slices - program must not abort
#  >> send
#  >> compress
#  >> append?

@pytest.mark.parametrize('data_type, ', [
    ('heart_rate'),
    ('temperature'),
    ('ppg_red'),
])
def test_append_values_to_fill_different_amount_of_y_slices_of_different_data_types(fixture_empty_df,
                                                                                    fixture_reduce_slice_size_24,
                                                                                    data_type):
    # Append exactly as many samples so that silces are/are not exactly filled
    # 24 bytes per slice => Samples per slice
    # 24x hr y samples per slice
    # 12x temp y samples per slice
    # 8x ppg y samples per slice

    slice_size = config.SLICE_MAX_SIZE
    y_size = DcHelper.helper_dtype_size(config.data_types_dict[data_type]['dtype'])
    y_samples_per_slice = int(slice_size / y_size)

    slices = [1, 1, 2, 2, 2, 3]

    for i, samples in enumerate([
        int(y_samples_per_slice - 1),
        int(y_samples_per_slice),
        int(y_samples_per_slice + 1),
        int(2 * y_samples_per_slice - 1),
        int(2 * y_samples_per_slice),
        int(2 * y_samples_per_slice + 1),
    ]):
        x = []
        y = []
        df = new_df()

        for sample in range(samples):
            x_temp = sample
            y_temp = sample * 2
            df.append_value(data_type, y_temp, x_temp)
            x.append(x_temp)
            y.append(y_temp)

        assert np.allclose(df.cols[data_type].x, np.asarray(
            x)), f"Appended x values incorrect for data_type {data_type} and samples {samples}."
        assert np.allclose(df.cols[data_type].y, np.asarray(
            y)), f"Appended y values incorrect for data_type {data_type} and samples {samples}."
        assert len(df.cols[data_type]._slices_y) == slices[
            i], f'Expected {slices[i]} slices for y but got {df.cols[data_type]._slices_y} instead for data_type{data_type}'
        # assert len(df.cols[data_type]._slices_time_rec) == slices[i]


@pytest.mark.parametrize('data_type, ', [
    ('heart_rate'),
    ('ppg_red'),
])
def test_append_values_to_fill_different_amount_of_time_slices_of_different_data_types(fixture_empty_df,
                                                                                       fixture_reduce_slice_size_24,
                                                                                       data_type):
    # Append exactly as many samples so that silces are/are not exactly filled
    # 24 bytes per slice => Samples per slice
    # 6x hr x
    # 3x ppg x

    slice_size = config.SLICE_MAX_SIZE
    x_size = DcHelper.helper_dtype_size(config.data_types_dict[data_type]['dtype_time'])
    x_samples_per_slice = int(slice_size / x_size)

    slices = [1, 1, 2, 2, 2, 3]

    for i, samples in enumerate([
        int(x_samples_per_slice - 1),
        int(x_samples_per_slice),
        int(x_samples_per_slice + 1),
        int(2 * x_samples_per_slice - 1),
        int(2 * x_samples_per_slice),
        int(2 * x_samples_per_slice + 1),
    ]):
        x = []
        y = []
        df = new_df()

        for sample in range(samples):
            x_temp = sample
            y_temp = sample * 2
            df.append_value(data_type, y_temp, x_temp)
            x.append(x_temp)
            y.append(y_temp)

        assert len(df.cols[data_type]._slices_time_rec) == slices[
            i], f'Expected {slices[i]} slices for y but got {len(df.cols[data_type]._slices_y)} instead for data_type {data_type}'
        assert len(df.cols[data_type].x) == samples
        assert len(df.cols[data_type].y) == samples


def test_append_empty_and_nonsens_columns_and_check_number_of_columns(fixture_empty_df):
    df = fixture_empty_df

    # these get added
    df.add_column('heart_rate')
    df.add_column('temperature')
    df.add_combined_columns(['acc_x', 'acc_y'], 'acc')
    df.add_combined_columns(['ppg_red', 'ppg_ir'], 'ppg')

    # these will not get added
    df.add_column('quatsch')
    df.add_combined_columns(['quatsch', 'sosse'], 'other')
    df.add_combined_columns(['acc_x', 'acc_y'], 'acc')
    df.add_combined_columns(['perfusion_ir', 'battery'], 'acc')
    df.add_combined_columns(['ppg_red', 'ppg_ambient'], 'more')

    assert df.columns == 6
    assert df.slices == 0


@pytest.mark.parametrize('samples', [
    1,
    3,
    4,
    24,
    25,
    100
])
def test_samples_and_samples_meta_attributes_in_df_cols_and_slices_for_different_sample_sizes(fixture_empty_df,
                                                                                              fixture_reduce_slice_size_24,
                                                                                              samples):
    df = fixture_empty_df
    df.add_combined_columns(('ppg_red', 'ppg_ir'), 'ppg')

    for i in range(samples):
        df.append_value('heart_rate', randint(0, 2 ** 8 - 1), i)
        df.append_value('battery', randint(0, 2 ** 8 - 1), i)
        df.append_value('ppg', [randint(0, 2 ** 24 - 1), randint(0, 2 ** 24 - 1)], i)

    df.store()

    assert df.samples == df.samples_meta
    for data_type in df.cols:
        col = df.cols[data_type]
        assert col.samples == col.samples_meta, f'samples in col {data_type} != samples meta'
        for i, sl in enumerate(col._slices_y):
            assert sl.samples == sl.samples_meta, f'samples in sl {data_type} y sl index {i}!= samples meta'
        for i, sl in enumerate(col._slices_time_rec):
            assert sl.samples == sl.samples_meta, f'samples in sl {data_type} time_rec sl index {i}!= samples meta'


def test_append_values_to_normal_and_combined_columns_and_check_after_loading_from_db(fixture_empty_df,
                                                                                      fixture_reduce_slice_size_24):
    df = fixture_empty_df
    df.add_combined_columns(['ppg_red', 'ppg_ir'], 'ppg')

    x_ppg = []
    x_hr = []
    y_ir = []
    y_red = []
    y_hr = []

    for i in range(200):
        y_ir_temp = randint(0, 2 ** 24 - 1)
        y_red_temp = randint(0, 2 ** 24 - 1)
        y_hr_temp = randint(0, 2 ** 8 - 1)

        df.append_value('ppg', [y_red_temp, y_ir_temp], i)
        df.append_value('heart_rate', y_hr_temp, i * 2)

        x_ppg.append(i)
        x_hr.append(i * 2)
        y_ir.append(y_ir_temp)
        y_red.append(y_red_temp)
        y_hr.append(y_hr_temp)

    x_ppg = np.asarray(x_ppg)
    y_ir = np.asarray(y_ir)
    y_red = np.asarray(y_red)

    x_hr = np.asarray(x_hr)
    y_hr = np.asarray(y_hr)

    assert df.columns == 3
    assert np.allclose(x_ppg, df.c.ppg_ir.x)
    assert np.allclose(x_ppg, df.c.ppg_red.x)
    assert np.allclose(y_ir, df.c.ppg_ir.y)
    assert np.allclose(y_red, df.c.ppg_red.y)
    assert np.allclose(x_hr, df.c.heart_rate.x)
    assert np.allclose(y_hr, df.c.heart_rate.y)

    df.store()
    hash_id = df.hash_id

    # assert the same after loading from the db

    df = DataFile.objects(_hash_id=hash_id).first()

    assert df.columns == 3
    assert np.allclose(x_ppg, df.c.ppg_ir.x)
    assert np.allclose(x_ppg, df.c.ppg_red.x)
    assert np.allclose(y_ir, df.c.ppg_ir.y)
    assert np.allclose(y_red, df.c.ppg_red.y)
    assert np.allclose(x_hr, df.c.heart_rate.x)
    assert np.allclose(y_hr, df.c.heart_rate.y)


def test_append_values_to_a_closed_df(fixture_empty_df):
    # appending a values to a closed file should not be possible

    df = fixture_empty_df

    for i in range(100):
        df.append_value('heart_rate', i, i)

    df.close(send=False)

    x = df.c.heart_rate.x
    y = df.c.heart_rate.y

    df.append_value('heart_rate', 10, 101)

    assert np.allclose(x, df.c.heart_rate.x)
    assert np.allclose(y, df.c.heart_rate.y)


# def test_df_empty_columns():
#
#     df = new_df(CLIENT_TEST_DB)
#     df.add_column('heart_rate')
#     df.add_column('ppg_ir')
#     df.add_column('heart_rate')  # < this one should be ignored because already exists
#     df.add_column('dumbo')  # < not valid
#     assert df.columns == 2, f'Number of columns not consistent after adding new columns. Expected {2}, got {df.columns}'
#     df.compress()
#     df.store()  # < no errors during finalizing and so on
#     hash = df.hash_id
#     del df
#     df = DataFile.objects(_hash_id=hash).first()
#     df.close()
#     assert df.columns == 2
#     assert df.duration_str == '00:00:00'
#
#     del df
#     df = DataFile.objects(_hash_id=hash).first()
#     df.store()
#
#     disconnect('default')

def test_double_appearance_of_slice_names(fixture_reduce_slice_size_4, fixture_empty_df):
    # 34^3 = 39304 possible slice names but due to birthday problem the probability
    # with only 500 slices the possibility is almost 95% for a double appearance!
    # wolfram alpha https://tinyurl.com/y4998b8n
    # with the reduced slice size there should be one slice per time value

    df = fixture_empty_df

    for i in range(1000):
        df.append_value('heart_rate', randint(0, 2 ** 8 - 1), i)

    sl_list = df.get_slice_list()

    # check for double items in the list
    if len(sl_list) == len(set(sl_list)):
        assert True
    else:
        assert False, 'duplicate hash was found in slice list! Hashes not unique'


@pytest.mark.parametrize('close', [
    (False),
    (True),
])
def test_stats_json_with_correct_samples_and_percentage_upload(fixture_reduce_slice_size_24, fixture_empty_df, close):

    df = fixture_empty_df

    df.add_column('battery')  # this col will stay empty
    df.add_combined_columns(('ppg_red', 'ppg_ir'), 'ppg')

    for i in range(2):
        # 1 slice y (2x1byte) and 1 time (2x4byte) => max 10bytes
        df.append_value('heart_rate', 80, i)
    for i in range(4):
        # 1 slice y (4x2bytes), 2 time slices (4x8bytes) => 40
        df.append_value('acc_x', 100, i)
    for i in range(5):
        # 2x1 slices y 2x(5*3 bytes), 2 time slices (5x8bytes) => 70
        df.append_value('ppg', [10, 20], i)
    # => max 120 bytes

    # expected content
    ref_dict = {
        'battery': {
            'slices': 0,
            'samples_meta': 0,
            'samples_real_x': 'ok', #0,
            'samples_real_y': 'ok', #0,
            'dtype': config.data_types_dict['battery']['dtype'],
            'file_size': '0',
            'compression_ratio': 1.0,
            'percentage_upload': 0.0,
        },
        'heart_rate': {
            'slices': 2,
            'samples_meta': 2,
            'samples_real_x': 'ok', #2,
            'samples_real_y': 'ok', #2,
            'dtype': config.data_types_dict['heart_rate']['dtype'],
            'file_size': '10',
            'compression_ratio': 1,
            'percentage_upload': 100.0,
        },
        'ppg_red': {
            'slices': 3,
            'samples_meta': 5,
            'samples_real_x': 'ok', #5,
            'samples_real_y': 'ok', #5,
            'dtype': config.data_types_dict['ppg_red']['dtype'],
            'file_size': '55',
            'compression_ratio': 1.0,
            'percentage_upload': 100.0,
        },
        'ppg_ir': {
            'slices': 1,
            'samples_meta': 5,
            'samples_real_x': 'ok' + '⁺', #5,
            'samples_real_y': 'ok', #5,
            'dtype': config.data_types_dict['ppg_ir']['dtype'],
            'file_size': '15',
            'compression_ratio': 1.0,
            'percentage_upload': str(100.0) + '⁺',
        },
        'acc_x': {
            'slices': 3,
            'samples_meta': 4,
            'samples_real_x': 'ok', #4,
            'samples_real_y': 'ok', #4,
            'dtype': config.data_types_dict['acc_x']['dtype'],
            'file_size': '40',
            'compression_ratio': 1.0,
            'percentage_upload': 100.0,
        },
    }

    df.store()
    if close:
        df.close()

    hash = df.hash_id

    # load df again
    df = DataFile.objects(_hash_id=hash).first()
    data_file = json.loads(df.stats_json)

    # assert total df
    assert data_file['hash'] == hash
    assert data_file['slices'] == 9
    assert data_file['samples_meta'] == 2+4+5+5
    assert data_file['samples_real_x_total'] == 'ok' if df.samples == df.samples_meta else str(2+4+5+5)
    assert data_file['samples_real_x_total'] == 'ok' if df.samples == df.samples_meta else str(2+4+5+5)
    if not close:
        assert data_file['compression_ratio'] == 1.0
        assert data_file['file_size'] == '120'
    else:
        size = 0
        sample_size = 0
        for col in df.cols.values():
            for sl in col._slices_y + col._slices_time_rec:
                size += os.path.getsize(sl._path)
                sample_size += sl.samples * sl.dtype_size
        assert data_file['file_size'] == DcHelper.file_size_str(size)
        assert data_file['compression_ratio'] == round(sample_size/size, 1)

    # assert cols
    for data_type in df.cols:
        assert data_file['columns'][data_type]['slices'] == ref_dict[data_type]['slices'], f'for {data_type}'
        assert data_file['columns'][data_type]['samples_meta'] == ref_dict[data_type]['samples_meta'], f'for {data_type}'
        assert data_file['columns'][data_type]['samples_real_x'] == str(ref_dict[data_type]['samples_real_x']), f'for {data_type}'
        assert data_file['columns'][data_type]['samples_real_y'] ==  str(ref_dict[data_type]['samples_real_y']), f'for {data_type}'
        assert data_file['columns'][data_type]['dtype'] == ref_dict[data_type]['dtype'], f'for {data_type}'
        assert data_file['columns'][data_type]['percentage_upload'] == str(ref_dict[data_type]['percentage_upload']), f'for {data_type}'
        if not close or data_type == 'battery':
            assert data_file['columns'][data_type]['file_size'] == ref_dict[data_type]['file_size'], f'for {data_type}'
            assert data_file['columns'][data_type]['compression_ratio'] == ref_dict[data_type]['compression_ratio'], f'for {data_type}'
        else:
            col = df.cols[data_type]
            size = 0
            sample_size = 0
            for sl in col._slices_y + col._slices_time_rec:
                size += os.path.getsize(sl._path)
                sample_size += sl.samples * sl.dtype_size
            assert data_file['columns'][data_type]['file_size'] == DcHelper.file_size_str(size), f'for {data_type}'
            assert data_file['columns'][data_type]['compression_ratio'] == round(sample_size / size, 1), f'for {data_type}'
    print(df.stats)
    print(df.stats_slices)
    print(df.stats_chunks)
    
@pytest.mark.parametrize('manipulate, live_data', [
    (False, False), 
    (True, False), 
    (False, True), 
    (True, True), 
])
def test_lazy_load_with_inconsistent_samples_must_set_consisten_attribute(fixture_empty_df, manipulate, live_data):
    # when inconsistencies in live_mode are detected during lazy load the consistent attribute must be set
    
    df = fixture_empty_df
    df.append_value('ppg_red', 10, 1)
    df.store()
    hash = df.hash_id
    
    if manipulate:
        sl = df.c.ppg_red._slices_time_rec[0]
        with open(sl._path, 'wb') as fp:
            fp.write(b'')
    
    df = DataFile.objects(_hash_id=hash).first()
    df.live_data = live_data
    df.preload_slice_values()
    
    # the server is not using live_data => this attribute must not be set
    if manipulate and live_data:
        assert not df.consistent
    else:
        assert df.consistent

@pytest.mark.parametrize('store_method', ['store', 'close'])
def test_lazy_load_must_not_happen_for_finally_analyzed_slices(fixture_reduce_slice_size_24, fixture_empty_df, store_method):
    # avoid unnecessary lazy loads during store and close which slows down the Gateway 
    
    df = fixture_empty_df
    for i in range(10):
        # 9x3 bytes (ppg y)        = 27 bytes => 1st y slice full
        # 9x8 bytes (ppg time_rec) = 72 bytes => first and more time slices full
        df.append_value('ppg_red', 10, 1)

    # if the last chunk is not finalized, then values will be loaded again when finalizing the chunk
    df.chunk_stop(store=True, final_analyse=True)
    df.free_memory()
    
    for i in range(10):
        df.append_value('ppg_red', 10, 1)
    
    if store_method == 'store':
        df.store()
    elif store_method == 'close':
        df.close()

    # assert
    # the first ppg slices must have no _values 
    #  => they were not lazilly loaded after free_memory() kicked them out
    
    assert df.c.ppg_red._slices_time_rec[0].status_finally_analzyed is True
    assert df.c.ppg_red._slices_time_rec[0]._values == None
    
    assert df.c.ppg_red._slices_y[0].status_finally_analzyed is True
    assert df.c.ppg_red._slices_y[0]._values == None

def test_append_binary_output_with_single_data_types(fixture_empty_df, fixture_reduce_slice_size_24):
    # this test asserts different possibilities of appending binary data
    # - different amounts of samples to be added to see if new slices are instantiated properly
    # - different amounts of successive appends
    # - store() and not store between the appends
    # - load the df after an append and append again
    # - load from the database and see if the input values are still correct

    slice_max_size = 24
    max_samples = 13
    max_number_of_appends = 3
    # generate some values time and then use them as input for the rest of the test
    x_base = []
    y_base = []
    for i in range(int(max_samples * max_number_of_appends)):
        x_base.append(float(i))
        # y_base.append(np.random.randint(0, 2 ** 8 - 1))
        y_base.append(np.random.randint(0, 2 ** 15 - 1))
    # split the append into 2 parts
    data_type = 'eeg_1'
    for multi_append in [True, False]:
        for number_of_appends in range(1, max_number_of_appends):
            # iterate through all possible sample amounts an different datatypes
            for samples in range(1, max_samples):
                # whether or not to store after the first append
                for store in [False, True]:
                    # whether or not to load the df after the store again
                    for load_after_store in [False, True]:

                        logger.info(f'\n\nCurrent test run: multi_append={multi_append}, '
                                    f'number_of_appends={number_of_appends}, '
                                    f'samples={samples}, '
                                    f'data_type={data_type}, '
                                    f'store={store}, load_after_store'
                                    f'={load_after_store}')

                        # todo use helper.new_df()

                        df = DataFile(person=Person.objects.first(), project=Project.objects.first())
                        df.date_time_start = datetime.now(tz=timezone.utc)
                        df.live_data = True
                        dtype = config.data_types_dict[data_type]['dtype']

                        # use some of the generated data pool
                        x_in = x_base[:samples*number_of_appends]
                        y_in = y_base[:samples*number_of_appends]
                        y_ref = np.array(y_in) * eeg_scale_factor()

                        # append pieces of the prepared data
                        for append in range(number_of_appends):
                            # convert piecewise to correct form and to bytes

                            x = x_in[append*samples:(append+1)*samples]
                            y = y_in[append*samples:(append+1)*samples]

                            if dtype == 'uint24':
                                y = DcHelper.int_list_to_uint24_lsb_first(y)
                            elif dtype == 'int24':
                                y = DcHelper.int_list_to_int24_msb_first(y)
                            else:
                                y = np.asarray(y, dtype=dtype).tobytes()

                            if multi_append and samples > 1:
                                half = int(len(x)/2)
                                half_bytes = int(half*DcHelper.helper_dtype_size(config.data_types_dict[data_type]['dtype']))
                                df.append_binary(data_type, y[:half_bytes], x[:half], store_immediately=False)
                                df.append_binary(data_type, y[half_bytes:], x[half:], store_immediately=True)
                            else:
                                df.append_binary(data_type, y, x)

                            if store:
                                df.store()
                                if load_after_store:
                                    hash = df.hash_id
                                    df = DataFile.objects(_hash_id=hash).first()

                        logger.info(f'Intput\n {x_in} \n {y_in}')

                        # store and load to see if values are loaded correctly
                        df.store()
                        hash = df.hash_id
                        df = DataFile.objects(_hash_id=hash).first()

                        # make sure that stats work in this condition
                        df.stats_slices
                        df.stats_chunks

                        skip = False
                        if samples == 0:
                            assert data_type not in df.cols
                            skip = True

                        if not skip:

                            # check if status_slice_full is correctly set
                            if len(df.cols[data_type]._slices_y) > 1:
                                for sl in df.cols[data_type]._slices_y[:-1]:
                                    assert sl.status_slice_full
                                    assert os.path.getsize(sl._path) == slice_max_size
                            if len(df.cols[data_type]._slices_time_rec) > 1:
                                for sl in df.cols[data_type]._slices_time_rec[:-1]:
                                    assert sl.status_slice_full
                                    assert os.path.getsize(sl._path) == slice_max_size

                            assert np.allclose(x_in, df.cols[data_type].x)
                            assert np.allclose(y_ref, df.cols[data_type].y, atol=2.0)

                            if samples > 0:
                                for sl in df.cols[data_type]._slices_y + df.cols[data_type]._slices_time_rec:
                                    assert sl.slice_time_offset is not None
                                # test at least that the first time offset is correct
                                assert df.cols[data_type]._slices_y[0].slice_time_offset == df.cols[data_type]._slices_time_rec[0].slice_time_offset

                            # assert everything again after close
                            df.close()

                            # check if status_slice_full is correctly set
                            if len(df.cols[data_type]._slices_y) > 1:
                                for sl in df.cols[data_type]._slices_y[:-1]:
                                    assert sl.status_slice_full
                            if len(df.cols[data_type]._slices_time_rec) > 1:
                                for sl in df.cols[data_type]._slices_time_rec[:-1]:
                                    assert sl.status_slice_full

                            assert np.allclose(x_in, df.cols[data_type].x)
                            assert np.allclose(y_ref, df.cols[data_type].y, atol=1)

                            if samples > 0:
                                for sl in df.cols[data_type]._slices_y + df.cols[data_type]._slices_time_rec:
                                    assert sl.slice_time_offset is not None
                                # test at least that the first time offset is correct
                                assert df.cols[data_type]._slices_y[0].slice_time_offset == df.cols[data_type]._slices_time_rec[0].slice_time_offset

                            # make sure stats work
                            df.stats_slices
                            df.stats_chunks

def test_append_binary_output_combined_columns(fixture_empty_df, fixture_reduce_slice_size_24):
    # this test asserts different possibilites of appending binary data
    # - different amounts of samples to be added to see if new slices are instantiated properly
    # - different amounts of successive appends
    # - store() and not () store between the appends
    # - load the df after an append and append again
    # - load from the database and see if the input values are still correct
    # - different data types are covered

    max_samples = 13
    max_number_of_appends = 3
    slice_max_size = 24
    # generate some values time and then use them as input for the rest of the test
    x_base = []
    y_base1 = []
    y_base2 = []
    y_base3 = []
    for i in range(int(max_samples * max_number_of_appends)):
        x_base.append(float(i))
        y_base1.append(np.random.randint(0, 2 ** 8 - 1))
        y_base2.append(np.random.randint(0, 2 ** 8 - 1))
        y_base3.append(np.random.randint(0, 2 ** 8 - 1))

    for number_of_appends in range(1, max_number_of_appends):
        # iterate through all possible sample
        for samples in range(0, max_samples):
            # whether or not to store after the first append
            for store in [False, True]:
                # whether or not to load the df after the store again
                for load_after_store in [False, True]:

                    logger.info(f'\nCurrent test run:  number_of_appends={number_of_appends}, samples={samples}, store={store}, '
                                f'load_after_store'
                                f'={load_after_store}')

                    df = DataFile(person=Person.objects.first(), project=Project.objects.first())
                    df.date_time_start = datetime.now(tz=timezone.utc)
                    df.live_data = True
                    combined_cols = ['eeg_1', 'eeg_2', 'eeg_3']
                    df.add_combined_columns(combined_cols, 'combined')
                    dtype = config.data_types_dict['eeg_1']['dtype']

                    # use some of the generated data pool
                    x_in = x_base[:samples*number_of_appends]
                    y_in1 = y_base1[:samples*number_of_appends]
                    y_in2 = y_base2[:samples*number_of_appends]
                    y_in3 = y_base3[:samples*number_of_appends]

                    # append pieces of the prepared data
                    for append in range(number_of_appends):
                        # convert piecewise to correct form and to bytes

                        x = x_in[append*samples:(append+1)*samples]
                        y1 = y_in1[append*samples:(append+1)*samples]
                        y2 = y_in2[append*samples:(append+1)*samples]
                        y3 = y_in3[append*samples:(append+1)*samples]

                        if dtype == 'uint24':
                            y1 = DcHelper.int_list_to_uint24_lsb_first(y1)
                            y2 = DcHelper.int_list_to_uint24_lsb_first(y2)
                            y3 = DcHelper.int_list_to_uint24_lsb_first(y3)
                        elif dtype == 'int24':
                            y1 = DcHelper.int_list_to_int24_msb_first(y1)
                            y2 = DcHelper.int_list_to_int24_msb_first(y2)
                            y3 = DcHelper.int_list_to_int24_msb_first(y3)
                        else:
                            y1 = np.asarray(y1, dtype=dtype).tobytes()
                            y2 = np.asarray(y2, dtype=dtype).tobytes()
                            y3 = np.asarray(y3, dtype=dtype).tobytes()

                        df.append_binary('combined', [y1, y2, y3], x)

                        if store:
                            df.store()
                            if load_after_store:
                                hash = df.hash_id
                                df = DataFile.objects(_hash_id=hash).first()

                    # store and load to see if values are loaded correctly
                    df.store()
                    hash = df.hash_id
                    df = DataFile.objects(_hash_id=hash).first()

                    # assert

                    # make sure that stats work in this condition
                    df.stats_slices
                    df.stats_chunks

                    skip = False
                    if samples == 0:
                        for data_type in combined_cols:
                            assert data_type in df.cols
                        skip = True

                    if not skip:

                        y_in_dict = {'eeg_1': y_in1, 'eeg_2': y_in2, 'eeg_3': y_in3}

                        for data_type in combined_cols:

                            # check if status_slice_full is correctly set
                            if len(df.cols[data_type]._slices_y) > 1:
                                for sl in df.cols[data_type]._slices_y[:-1]:
                                    assert sl.status_slice_full
                                    assert os.path.getsize(sl._path) == slice_max_size
                            if len(df.cols[data_type]._slices_time_rec) > 1:
                                for sl in df.cols[data_type]._slices_time_rec[:-1]:
                                    assert sl.status_slice_full
                                    assert os.path.getsize(sl._path) == slice_max_size

                            assert np.allclose(x_in, df.cols[data_type].x)
                            y_ref = np.array(y_in_dict[data_type]) * eeg_scale_factor()
                            assert np.allclose(y_ref, df.cols[data_type].y, atol=1)

                            if samples > 0:
                                assert df.cols[data_type]._slices_y[0].slice_time_offset == df.cols['eeg_1']._slices_time_rec[0].slice_time_offset
                                for i in range(len(df.cols['eeg_1']._slices_y)):
                                    assert df.cols['eeg_1']._slices_y[i].slice_time_offset == \
                                           df.cols['eeg_2']._slices_y[i].slice_time_offset == df.cols['eeg_3']._slices_y[i].slice_time_offset

                            # make sure stats work
                            df.stats_slices
                            df.stats_chunks

                        df.close()

                        for data_type in combined_cols:

                            # check if status_slice_full is correctly set
                            if len(df.cols[data_type]._slices_y) > 1:
                                for sl in df.cols[data_type]._slices_y[:-1]:
                                    assert sl.status_slice_full
                            if len(df.cols[data_type]._slices_time_rec) > 1:
                                for sl in df.cols[data_type]._slices_time_rec[:-1]:
                                    assert sl.status_slice_full

                            assert np.allclose(x_in, df.cols[data_type].x)
                            y_ref = np.array(y_in_dict[data_type]) * eeg_scale_factor()
                            assert np.allclose(y_ref, df.cols[data_type].y, atol=1)

                            if samples > 0:
                                assert df.cols[data_type]._slices_y[0].slice_time_offset == df.cols['eeg_1']._slices_time_rec[0].slice_time_offset
                                for i in range(len(df.cols['eeg_1']._slices_y)):
                                    assert df.cols['eeg_1']._slices_y[i].slice_time_offset == \
                                           df.cols['eeg_2']._slices_y[i].slice_time_offset == df.cols['eeg_3']._slices_y[i].slice_time_offset

                            # make sure stats work
                            df.stats_slices
                            df.stats_chunks

# todo test chunks with append binary

def test_append_binary_with_chunks(fixture_empty_df, fixture_reduce_slice_size_24):
    # this test creates some chunks with different amount of data in

    max_samples = 13
    max_number_of_appends = 3
    # generate some values time and then use them as input for the rest of the test
    x_base = []
    y_base = []
    y_base1 = []
    y_base2 = []
    for i in range(int(max_samples * max_number_of_appends)):
        x_base.append(float(i))
        y_base.append(np.random.randint(0, 2 ** 8 - 1))
        y_base1.append(np.random.randint(0, 2 ** 8 - 1))
        y_base2.append(np.random.randint(0, 2 ** 8 - 1))

    for samples in range(1, max_samples):
        for number_of_chunks in range(1, 4):
            for load_after_store in [False, True]:

                logger.info(f'\nCurrent run: samples {samples}, number_of_chunks {number_of_chunks}, load_after_store'
                            f' {load_after_store}')

                df = DataFile(person=Person.objects.first(), project=Project.objects.first())
                df.date_time_start = datetime.now(tz=timezone.utc)
                df.live_data = True
                combined_cols = ['acc_x', 'acc_y']
                df.add_combined_columns(combined_cols, 'acc')

                # use some of the generated data pool
                x_in = x_base[:samples * number_of_chunks]
                y_in = y_base[:samples * number_of_chunks]
                y_in1 = y_base1[:samples * number_of_chunks]
                y_in2 = y_base2[:samples * number_of_chunks]

                chunk_dict = {
                    'perfusion_ir': {'x': x_in, 'y': y_in},
                    'acc_x': {'x': x_in, 'y': y_in1},
                    'acc_y': {'x': x_in, 'y': y_in2},
                }

                # append pieces of the prepared data
                for chunk_no in range(number_of_chunks):
                    # convert piecewise to correct form and to bytes

                    x = x_in[chunk_no * samples:(chunk_no + 1) * samples]
                    y = y_in[chunk_no * samples:(chunk_no + 1) * samples]
                    y1 = y_in1[chunk_no * samples:(chunk_no + 1) * samples]
                    y2 = y_in2[chunk_no * samples:(chunk_no + 1) * samples]

                    y = np.asarray(y, dtype=config.data_types_dict['perfusion_ir']['dtype']).tobytes()
                    y1 = np.asarray(y1, dtype=config.data_types_dict['acc_x']['dtype']).tobytes()
                    y2 = np.asarray(y2, dtype=config.data_types_dict['acc_x']['dtype']).tobytes()

                    df.append_binary('acc', [y1, y2], x)
                    df.append_binary('perfusion_ir', y, x)

                    df.chunk_stop()
                    if load_after_store:
                        hash = df.hash_id
                        df = DataFile.objects(_hash_id=hash).first()

                # assert

                df.stats_chunks

                for chunk_no in range(number_of_chunks):
                    for data_type in chunk_dict:
                        assert np.allclose(df.chunks[chunk_no].cols[data_type].x, chunk_dict[data_type]['x'][chunk_no * samples:(chunk_no + 1) * samples])
                        assert np.allclose(df.chunks[chunk_no].cols[data_type].y, chunk_dict[data_type]['y'][chunk_no * samples:(chunk_no + 1) * samples])
                        assert np.allclose(df.chunks[chunk_no].cols[data_type].samples, len(chunk_dict[data_type]['x'][chunk_no * samples:(chunk_no + 1) * samples]))
                    assert df.chunks[chunk_no].finalized
                    assert df.chunks[chunk_no].duration

@pytest.mark.parametrize('store_method, free_slices', [
    ('store', True),
    ('store', False),
    ('close', True),
    ('close', False)
])
def test_if_parameter__free_slices_when_finally_analysed__acutally_frees_the_slices(fixture_empty_df, fixture_reduce_slice_size_24, store_method, free_slices):

    df = fixture_empty_df

    df.free_slices_when_finally_analysed = True if free_slices else False

    for i in range(25):
        df.append_value('heart_rate', 80+i, i)
        df.append_value('ppg_ir', 100+i, i)

    # assert

    for sl in df.all_slices():
        assert sl._values is not None

    if store_method == 'store':
        df.store()
    elif store_method == 'close':
        df.close(send=False)

    for sl in df.all_slices():
        # nothing must be freed or the slice are not full
        if not free_slices or not sl.status_finally_analzyed:
            assert sl._values is not None
        # freeing only applies to finally analyzed slices
        elif sl.status_finally_analzyed:
            # ppg and all values time should be freed but not y of heart-rate (since it is boxplot data)
            if sl.data_type == 'ppg_ir' or sl.slice_type == 'time_rec':
                assert sl._values is None
            else:
                assert sl._values is not None


@pytest.mark.parametrize('store_method, free_slices_when_finally_analysed', (
        ('chunk_stop', False),
        ('chunk_stop', True)
))
def test_chunks_created_by_appended_binaries_must_not_provoke_a_lazy_load(fixture_empty_df, fixture_reduce_slice_size_24, store_method, free_slices_when_finally_analysed):

    logger.setLevel('DEBUG')

    df = fixture_empty_df
    df.free_slices_when_finally_analysed = free_slices_when_finally_analysed
    df.add_combined_columns(['eeg_1','eeg_2'], 'eeg')
    samples = 13

    x_in, y_in, y_in1, y_in2 = [], [], [], [],

    for i in range(samples):
        x_in.append(float(i))
        y_in.append(randint(0, 2**8-1))
        y_in1.append(randint(0, 2**8-1))
        y_in2.append(randint(0, 2**8-1))

    # convert to binaries
    df.append_binary('eeg_3', DcHelper.int_list_to_int24_lsb_first(y_in), x_in)
    df.append_binary('eeg', [DcHelper.int_list_to_int24_lsb_first(y_in1), DcHelper.int_list_to_int24_lsb_first(y_in2)], x_in)

    df.chunk_stop()

    # assert

    for col in df.cols.values():
        for sl in col._slices_y:
            # no values were loaded with lazy load
            assert sl._values is None
            # all binaries were stored
            assert sl._values_bin == bytearray()
            assert sl.binaries_appended


@pytest.mark.parametrize('size', (None, 24))
def test_set_and_check_slice_max_size_attribute_of_df_before_and_after_loading(fixture_empty_df, size):

    if size:
        config._SLICE_MAX_SIZE = size
    else:
        size = config.SLICE_MAX_SIZE

    df = new_df()

    assert df.slice_max_size == size

    df.store()
    hash = df.hash_id
    df = DataFile.objects(_hash_id=hash).first()

    assert df.slice_max_size == size

@pytest.mark.parametrize('avoid', (True, False))
def test_avoid_lazy_load_for_appended_binaries(fixture_empty_df, fixture_reduce_slice_size_24, avoid):

    df = fixture_empty_df

    x = [1, 2,]
    y1 = [80, 90]
    yb1 = DcHelper.int_list_to_int24_msb_first(y1)
    y1 = np.array(y1)

    df.append_binary('eeg_1', yb1, x)
    df.store()
    hash = df.hash_id
    df = DataFile.objects(_hash_id=hash).first()
    df.avoid_lazy_load = avoid
    # now here must or must not be a lazy load for y...
    df.preload_slice_values()

    # assert

    # x must be normal
    for sl in df.all_slices(time_only=True):
        # there is only one slice anyway...
        assert np.allclose(x, sl._values)

    # y is different depending on the setting
    for sl in df.all_slices(y_only=True):
        if avoid:
            assert sl._values == []
            assert sl._values_bin == bytearray()
        else:
            y_ref = y1*eeg_scale_factor()
            assert np.allclose(y_ref, sl._values)
            assert sl._values_bin is None