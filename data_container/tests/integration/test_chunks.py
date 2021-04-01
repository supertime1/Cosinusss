import pytest
from data_container import DataFile, DcHelper
from data_container import config
from data_container.tests.testing_helper_functions import now
from data_container.tests.conftest import new_df
from datetime import datetime, timezone, timedelta
from random import randint

import numpy as np

logger = config.logger

# todo: Testing
#  - what if x/y inconsistent ? 
#  - time_start/end is inbetween two slices e.g. 11.5 in 9, 10, 11 |new slice| 12, 13, 14, ...

# todo: same with full slices
@pytest.mark.parametrize('start, end', [
    (1, 2),
    (2, 1),
    (-1, 2),
    (1, -2),
    (1.0, 2.2),
    (1, 2.2),
    (1, '123'),
    ('1', '123'),
    (datetime.now(timezone.utc) + timedelta(seconds=2), datetime.now(timezone.utc) + timedelta(seconds=3)),
    (datetime.now(timezone.utc) + timedelta(seconds=3), datetime.now(timezone.utc) + timedelta(seconds=2)),
    (datetime.now() + timedelta(seconds=-10), datetime.now(timezone.utc) + timedelta(seconds=2)),
    (datetime.now(timezone.utc) + timedelta(seconds=-10), datetime.now() + timedelta(seconds=2)),
])
def test_labelled_chunk_with_different_time_ranges_in_empty_df(fixture_empty_df, start, end):
    # > No chunks get added because there is no data, but no crashes in the program
    # >> positive, negative, float, int numbers. string is not supported, datetime with/without timezone
    
    df = fixture_empty_df
    df.add_labelled_chunk(label='test_label', time_start=start, time_end=end, store=True)
    
    assert df.chunks_labelled == []


@pytest.mark.parametrize('start, end', [
    (1, 9.9),
    (20.1, 30),
])
def test_empty_labelled_chunk_with_time_ranges_in_seconds_before_and_after_the_actual_data(fixture_reduce_slice_size_24, fixture_empty_df, start, end):
    # No chunks get added because there is no data inside the markers

    df = fixture_empty_df
    
    for i in range(10, 20):
        df.append_value('ppg_ir', i, i)
        df.append_value('heart_rate', i, i)
    
    df.add_labelled_chunk(label='test_label', time_start=start, time_end=end)

    assert df.chunks_labelled == []


@pytest.mark.parametrize('start, end', [
    (1, 10),
    (19, 30),
    (10.5, 11.5),
])
def test_labelled_chunk_with_exactly_one_data_point(fixture_reduce_slice_size_24,fixture_empty_df, start, end):
    # Chunks include only one point at the end
    # ppg & hr have different time_rec dtypes

    df = fixture_empty_df
    for i in range(10, 20):
        df.append_value('ppg_ir', i, i)
        df.append_value('heart_rate', i, i)

    df.add_labelled_chunk(label='test_label', time_start=start, time_end=end)

    # assert 
    
    assert len(df.chunks_labelled) == 1
    
    x = df.chunks_labelled[0].c.heart_rate.x
    y = df.chunks_labelled[0].c.heart_rate.y
    
    assert len(x) == 1
    assert len(y) == 1

    x = df.chunks_labelled[0].c.ppg_ir.x
    y = df.chunks_labelled[0].c.ppg_ir.y
    
    assert len(df.chunks_labelled[0].c.ppg_ir.x) == 1
    assert len(df.chunks_labelled[0].c.ppg_ir.y) == 1

@pytest.mark.parametrize('samples', [
    (5),  # all slices are not full
    (6),    # time slice exactly full but y not
    (7),    # next time slice begun
    (23),   # multiple time slices but hr slice not full
    (24),   # hr slice exactly full
    (25),   # hr slice full + 1
    (48),   # 2nd hr slice full
])
def test_labelled_chunks_with_different_number_of_existing_slices(fixture_reduce_slice_size_24, fixture_empty_df, samples):
    # Check if all data is in the labelled chunk no matter if there are one or more slice
    
    # Slices are reduced to 24 bytes...
    # dtype sizes:
    # hr y: 1 bytes => 24 samples till slice is full
    # hr x: 4 bytes => 6 samples till slice is full 
    
    df = fixture_empty_df
    
    x = []
    y = []
    
    for i in range(samples):
        x_temp = i
        y_temp = randint(0, 2**8-1)
        df.append_value('heart_rate', y_temp, x_temp)
        x.append(x_temp)
        y.append(y_temp)
    
    df.add_labelled_chunk('test', time_start=0, time_end=samples)
    
    # assert
    
    x_chunk = df.chunks_labelled[0].c.heart_rate.x
    y_chunk = df.chunks_labelled[0].c.heart_rate.y
    x = np.asarray(x)
    y = np.asarray(y)
    
    assert np.allclose(x, x_chunk)
    assert np.allclose(y, y_chunk)

@pytest.mark.parametrize('samples', [
    0,
    1
])
def test_labelled_chunks_with_empty_data_col(fixture_empty_df, fixture_reduce_slice_size_24, samples):

    df = fixture_empty_df

    df.add_combined_columns(['acc_x', 'acc_y'], 'acc')
    df.add_column('battery')

    for i in range(samples):
        # leave the 0 as x value, there was already a case where exactly this led to a mistake
        df.append_value('heart_rate', 1, 0)

    df.add_labelled_chunk('test', time_start=0, time_end=2)

    # assert
    print()
    # ok that there are no errors?

def test_labelled_chunks_samples_statistics(fixture_empty_df, fixture_reduce_slice_size_24):

    for samples in range(1, 25):

        # counted samples for the second label including only half of the samples
        samples_half = 0

        df = new_df()
        df.add_combined_columns(['acc_x', 'acc_y'], 'acc')
        df.add_column('battery')
        for i in range(samples):

            df.append_value('heart_rate', randint(0, 2**8-1), i)
            df.append_value('acc', [randint(0, 1000), randint(0, 1000)], i)

            if i <= samples/2-0.25:
                samples_half += 1

        df.add_labelled_chunk('test', time_start=0, time_end=samples)
        df.add_labelled_chunk('test2', time_start=0, time_end=samples/2- 0.25)

        # assert
        assert df.chunks_labelled[0].c.heart_rate.samples == samples
        assert df.chunks_labelled[0].c.acc_x.samples == samples
        assert df.chunks_labelled[0].c.acc_y.samples == samples

        assert df.chunks_labelled[1].c.heart_rate.samples == samples_half
        assert df.chunks_labelled[1].c.acc_x.samples == samples_half
        assert df.chunks_labelled[1].c.acc_y.samples == samples_half


def test_labelled_chunks_with_combined_and_normal_columns_at_different_positions(fixture_reduce_slice_size_288, fixture_empty_df):
    # create 3 labelled chunks and verify that their x y content is correct
    
    df = fixture_empty_df
    df.add_combined_columns(['ppg_ir', 'ppg_red'], 'ppg')
    # add an empty column which is not of interest but must not cause any problems
    df.add_column('battery')
    
    # define start and stop for generated data and for the labelled chunks
    start = 1
    stop = 50
    start_1 = 3
    stop_1 = 23
    start_2 = 20
    stop_2 = stop+10
    
    # collect all generated data here for later comparison
    x_hr = []
    y_hr = []
    x_ppg = []
    y_ppg_ir = []
    y_ppg_red = []
    
    x_hr_1 = []
    y_hr_1 = []
    x_ppg_1 = []
    y_ppg_ir_1 = []
    y_ppg_red_1 = []
    
    x_hr_2 = []
    y_hr_2 = []
    x_ppg_2 = []
    y_ppg_ir_2 = []
    y_ppg_red_2 = []

    for i in range(start, stop+1):
        # add 10 times more ppg values
        for j in range(0, 10):
            
            y_ppg1 = randint(0, 2**24-1)
            y_ppg2 = randint(0, 2**24-1)
            x_ppg0 = i+j/10
            df.append_value('ppg', [y_ppg1, y_ppg2], x_ppg0)
            
            # store for later assertions
            x_ppg.append(x_ppg0)
            y_ppg_ir.append(y_ppg1)
            y_ppg_red.append(y_ppg2)
            
            if start_1 <= x_ppg0 <= stop_1:
                x_ppg_1.append(x_ppg0)
                y_ppg_ir_1.append(y_ppg1)
                y_ppg_red_1.append(y_ppg2)
            if start_2 <= x_ppg0 <= stop_2:
                x_ppg_2.append(x_ppg0)
                y_ppg_ir_2.append(y_ppg1)
                y_ppg_red_2.append(y_ppg2)
                
        df.append_value('heart_rate', i, i)
        
        # store for later assertions
        x_hr.append(i)
        y_hr.append(i)
        if start_1 <= i <= stop_1:
            x_hr_1.append(i)
            y_hr_1.append(i)
        if start_2 <= i <= stop_2:
            x_hr_2.append(i)
            y_hr_2.append(i)

    df.add_labelled_chunk('first_half', time_start=start_1, time_end=stop_1)
    df.add_labelled_chunk('second_half', time_start=start_2, time_end=stop_2)
    df.add_labelled_chunk('all', time_start=0, time_end=stop+10)
    # add one labelled chunk wich will be empty and therefore discarded
    df.add_labelled_chunk('booom', time_start=100, time_end=200)
    
    # assert
    
    # all tags are available
    assert len(df.chunks_labelled) == 3
    tag_list = ['second_half', 'first_half', 'all']
    for label in df.chunk_labels:
        assert label in tag_list
        tag_list.remove(label)
    assert 'booom' not in df.chunk_labels
    
    # some tolerance because sometimes int and float are compared
    tol = 0.1
    
    # now compare all x and y values
    all_chunk = df.return_chunks_with_label('all')[0]    
    assert np.allclose(all_chunk.c.heart_rate.x, np.asarray(x_hr), atol=tol)
    assert np.allclose(all_chunk.c.heart_rate.y, np.asarray(y_hr), atol=tol)
    assert np.allclose(all_chunk.c.ppg_ir.x, np.asarray(x_ppg), atol=tol)
    assert np.allclose(all_chunk.c.ppg_ir.y, np.asarray(y_ppg_ir), atol=tol)
    assert np.allclose(all_chunk.c.ppg_red.x, np.asarray(x_ppg), atol=tol)
    assert np.allclose(all_chunk.c.ppg_red.y, np.asarray(y_ppg_red), atol=tol)
    
    chunk_1 = df.return_chunks_with_label('first_half')[0]
    assert np.allclose(chunk_1.c.heart_rate.x, np.asarray(x_hr_1), atol=tol)
    assert np.allclose(chunk_1.c.heart_rate.y, np.asarray(y_hr_1), atol=tol)
    assert np.allclose(chunk_1.c.ppg_ir.x, np.asarray(x_ppg_1), atol=tol)
    assert np.allclose(chunk_1.c.ppg_ir.y, np.asarray(y_ppg_ir_1), atol=tol)
    assert np.allclose(chunk_1.c.ppg_red.x, np.asarray(x_ppg_1), atol=tol)
    assert np.allclose(chunk_1.c.ppg_red.y, np.asarray(y_ppg_red_1), atol=tol)

    chunk_2 = df.return_chunks_with_label('second_half')[0]
    assert np.allclose(chunk_2.c.heart_rate.x, np.asarray(x_hr_2), atol=tol)
    assert np.allclose(chunk_2.c.heart_rate.y, np.asarray(y_hr_2), atol=tol)
    assert np.allclose(chunk_2.c.ppg_ir.x, np.asarray(x_ppg_2), atol=tol)
    assert np.allclose(chunk_2.c.ppg_ir.y, np.asarray(y_ppg_ir_2), atol=tol)
    assert np.allclose(chunk_2.c.ppg_red.x, np.asarray(x_ppg_2), atol=tol)
    assert np.allclose(chunk_2.c.ppg_red.y, np.asarray(y_ppg_red_2), atol=tol)


def test_store_df_with_labelled_chunks_and_load_from_db(fixture_reduce_slice_size_24, fixture_empty_df):
    
    df = fixture_empty_df
    for i in range(20):
        df.append_value('heart_rate', randint(0, 2**8-1), i)
    
    df.add_labelled_chunk('test1', time_start=3, time_end=10)
    df.add_labelled_chunk('test2', time_start=5, time_end=17)
    a = df.return_chunks_with_label('test1')
    print(a)
    x1 = df.return_chunks_with_label('test1')[0].c.heart_rate.x
    y1 = df.return_chunks_with_label('test1')[0].c.heart_rate.y
    x2 = df.return_chunks_with_label('test2')[0].c.heart_rate.x
    y2 = df.return_chunks_with_label('test2')[0].c.heart_rate.y
    
    df.store()
    hash = df.hash_id
    
    # load
    df = DataFile.objects(_hash_id=hash).first()
    x1_2 = df.return_chunks_with_label('test1')[0].c.heart_rate.x
    y1_2 = df.return_chunks_with_label('test1')[0].c.heart_rate.y
    x2_2 = df.return_chunks_with_label('test2')[0].c.heart_rate.x
    y2_2 = df.return_chunks_with_label('test2')[0].c.heart_rate.y
    
    assert np.allclose(x1, x1_2)
    assert np.allclose(y1, y1_2)
    assert np.allclose(x2, x2_2)
    assert np.allclose(y2, y2_2)
    assert len(df.chunk_labels) == 2

def test_markers_with_time_as_seconds_and_datetime_and_return_all_markers_and_labels(fixture_empty_df):
    
    df = fixture_empty_df
    df._date_time_start = datetime.now(timezone.utc)
    df.add_labelled_chunk('test1', time_start=3)
    df.add_labelled_chunk('test2', time_start=df._date_time_start+timedelta(seconds=+4))
    df.add_labelled_chunk('test3', time_start=6)
    
    assert len(df.marker_labels) == 3
    assert 'test1' in df.marker_labels
    assert 'test2' in df.marker_labels
    assert 'test3' in df.marker_labels
    
    for i in range(len(df.markers)):
        assert type(df.markers[i].label) is str
        assert type(df.markers[i]._date_time_start) is datetime
        assert df.markers[i].duration == 0

def test_markers_create_some_and_load_df_from_db(fixture_empty_df):
    
    df = fixture_empty_df
    df._date_time_start = datetime.now(timezone.utc)
    df.add_labelled_chunk('test1', time_start=3)
    df.add_labelled_chunk('test2', time_start=df._date_time_start + timedelta(seconds=+4))
    df.add_labelled_chunk('test3', time_start=6)
    
    df.store()
    hash = df.hash_id
    df = DataFile.objects(_hash_id=hash).first()
    
    assert len(df.marker_labels) == 3
    assert 'test1' in df.marker_labels
    assert 'test2' in df.marker_labels
    assert 'test3' in df.marker_labels

    for i in range(len(df.markers)):
        assert type(df.markers[i].label) is str
        assert type(df.markers[i]._date_time_start) is datetime
        assert df.markers[i].duration == 0

def test_create_labelled_chunk_and_data_chunk_and_markers(fixture_empty_df):
    # make sure the different types of chunks don't distrube each other
    
    df = fixture_empty_df
    
    df.chunk_start()
    
    t_start = datetime.now(timezone.utc)
    df._date_time_start = t_start
    
    for i in range(20):
        df.append_value('battery', randint(0, 2**8-1), i)
        
    df.add_labelled_chunk('the_marker1', time_start=1)
    df.chunk_stop()
    df.add_labelled_chunk('the_label', time_start=t_start, time_end=t_start+timedelta(seconds=20))
    df.add_labelled_chunk('the_marker2', time_start=1)
    
    assert len(df.chunks) == 1
    assert len(df.chunks_labelled) == 1
    assert len(df.markers) == 2
    assert df.chunks[0].index == 0
    assert df.markers[0].index == 0
    assert df.chunks_labelled[0].index == 0
    assert df.markers[1].index == 1
    assert np.allclose(df.chunks[0].c.battery.x, df.chunks_labelled[0].c.battery.x)
    assert np.allclose(df.chunks[0].c.battery.y, df.chunks_labelled[0].c.battery.y)

@pytest.mark.parametrize('no_of_chunks_with_data', (
    0,
    1,
    2,
    3
))
def test_chunks_may_not_be_empty(fixture_empty_df, no_of_chunks_with_data):

    df = fixture_empty_df
    df.date_time_start = now()

    # empty chunk
    df.chunk_start()
    # real chunk
    df.chunk_start()
    if no_of_chunks_with_data >= 1:
        df.append_value('heart_rate', 10, now(df))
    df.chunk_start()
    df.chunk_start()
    if no_of_chunks_with_data >= 2:
        df.append_value('heart_rate', 10, now(df))
    df.chunk_start()
    if no_of_chunks_with_data >= 3:
        df.append_value('heart_rate', 10, now(df))

    df.close()

    assert len(df.chunks) == no_of_chunks_with_data
    if no_of_chunks_with_data:
        chunk = df.chunks[no_of_chunks_with_data-1]
        assert np.allclose(chunk.c.heart_rate.y, np.asarray(10))


def test_chunks_there_must_be_no_data_points_which_are_not_part_of_a_chunk(fixture_empty_df):
    # even though when not calling chunk_start() it must happen always if there is no unfinalized chunk

    df = fixture_empty_df

    df.append_value('heart_rate', 1, 2)
    df.append_value('heart_rate', 11, 22)
    df.chunk_stop()

    df.append_value('heart_rate', 4, 5)
    df.append_value('heart_rate', 44, 55)
    df.close()

    # assert

    assert len(df.chunks) == 2
    assert df.chunks[0].finalized
    assert df.chunks[1].finalized
    assert np.allclose(df.chunks[0].c.heart_rate.x, np.asarray([2, 22]))
    assert np.allclose(df.chunks[0].c.heart_rate.y, np.asarray([1, 11]))
    assert np.allclose(df.chunks[1].c.heart_rate.x, np.asarray([5, 55]))
    assert np.allclose(df.chunks[1].c.heart_rate.y, np.asarray([4, 44]))

# todo: check if this still makes sense since it may take quite long
def test_chunks_create_different_data_amounts_in_first_and_second_unfinished_chunks_with_normal_and_combined_columns(fixture_empty_df,fixture_reduce_slice_size_24):
    # systematically test every possible chunk size with two chunks. The second one starting at different slice-positions

    # reduce the log level to speed up this test by 1-2 seconds
    level = logger.level
    logger.setLevel('ERROR')

    # 24 bytes => 8 ppg y values or 3 ppg_x values per slice
    #          => let the first chunk start at any possible position (1. slice, 2. slice, ...) for x and y slices to make sure there are no problems
    for samples_chunk0 in range(15):
        # make sure that the length of the chunk does not influence the results (chunk will cover 1 and more slices)
        for samples_chunk1 in range(15):

            x_all_ir = {}
            y_all_ir = {}

            x_all_comb = {}
            y_all_red = {}
            y_all_am = {}

            df = new_df()
            df.add_combined_columns(['ppg_red', 'ppg_ambient'], 'ppg')
            df.add_column('ppg_ir')
            df.date_time_start = now()

            # 1. unfinished chunk
            x_all_ir[0] = []
            y_all_ir[0] = []
            x_all_comb[0] = []
            y_all_red[0] = []
            y_all_am[0] = []

            df.chunk_start()
            for i in range(samples_chunk0):
                x = now(df)
                y = randint(0, 2 ** 24 - 1)
                df.append_value('ppg_ir', y, x)
                x_all_ir[0].append(x)
                y_all_ir[0].append(y)

                x = now(df)
                y1 = randint(0, 2 ** 24 - 1)
                y2 = randint(0, 2 ** 24 - 1)
                df.append_value('ppg', [y1, y2], x)
                x_all_comb[0].append(x)
                y_all_red[0].append(y1)
                y_all_am[0].append(y2)

            # 2nd unfinished chunk
            x_all_ir[1] = []
            y_all_ir[1] = []
            x_all_comb[1] = []
            y_all_red[1] = []
            y_all_am[1] = []

            df.chunk_start()
            for i in range(samples_chunk1):
                x = now(df)
                y = randint(0, 2 ** 24 - 1)
                df.append_value('ppg_ir', y, x)
                x_all_ir[1].append(x)
                y_all_ir[1].append(y)

                x = now(df)
                y1 = randint(0, 2 ** 24 - 1)
                y2 = randint(0, 2 ** 24 - 1)
                df.append_value('ppg', [y1, y2], x)
                x_all_comb[1].append(x)
                y_all_red[1].append(y1)
                y_all_am[1].append(y2)

            # add the last (3rd) chunk with one datapoint
            df.chunk_start()
            last_x = now(df)
            last_y = randint(0, 2 ** 24 - 1)
            df.append_value('ppg_ir', last_y, last_x)
            last_x_comb = now(df)
            last_y_red = randint(0, 2 ** 24 - 1)
            last_y_am = randint(0, 2 ** 24 - 1)
            df.append_value('ppg', [last_y_red, last_y_am], last_x_comb)
            x_all_ir[2] = [last_x]
            y_all_ir[2] = [last_y]
            x_all_comb[2] = [last_x_comb]
            y_all_red[2] = [last_y_red]
            y_all_am[2] = [last_y_am]

            df.close()

            # assert

            # get the expected amount of chunks
            no_chunks = 1

            if x_all_ir[0]:
                no_chunks += 1
            if x_all_ir[1]:
                no_chunks += 1

            assert len(df.chunks) == no_chunks, f'Number of expected chunks not correct. Iteration currently at c0: {samples_chunk0} c1: {samples_chunk1}.'

            chunk_index = 0

            for i in range(3):

                if not x_all_ir[i]:
                    continue

                assert np.allclose(df.chunks[chunk_index].c.ppg_ir.x, np.asarray(x_all_ir[i])), f'ppg_ir x in chunk {i} not correct. Iteration currently at c0: {samples_chunk0} c1: {samples_chunk1}.'
                assert np.allclose(df.chunks[chunk_index].c.ppg_ir.y, np.asarray(y_all_ir[i])), f'ppg_ir y in chunk {i} not correct. Iteration currently at c0: {samples_chunk0} c1: {samples_chunk1}.'

                assert np.allclose(df.chunks[chunk_index].c.ppg_red.x, np.asarray(x_all_comb[i])), f'ppg_red x in chunk {i} not correct. Iteration currently at c0: {samples_chunk0} c1: {samples_chunk1}.'
                assert np.allclose(df.chunks[chunk_index].c.ppg_red.y, np.asarray(y_all_red[i])), f'ppg_red y in chunk {i} not correct. Iteration currently at c0: {samples_chunk0} c1: {samples_chunk1}.'

                assert np.allclose(df.chunks[chunk_index].c.ppg_ambient.x, np.asarray(x_all_comb[i])), f'ppg_amb x in chunk {i} not correct. Iteration currently at c0: {samples_chunk0} c1: {samples_chunk1}.'
                assert np.allclose(df.chunks[chunk_index].c.ppg_ambient.y, np.asarray(y_all_am[i])), f'ppg_amb y in chunk {i} not correct. Iteration currently at c0: {samples_chunk0} c1: {samples_chunk1}.'

                chunk_index += 1

    logger.setLevel(level)


def test_chunks_create_random_amounts_of_finalized_and_unfinalized_chunks_and_finalize_everything_at_the_end(fixture_empty_df, fixture_reduce_slice_size_24):

    for i in range(5):
        df = new_df()
        df._date_time_start = now()

        x_all = {}
        y_all = {}

        # 15 chunks total
        no_chunks = 15
        for chunk_no in range(no_chunks):
            x_all[chunk_no] = []
            y_all[chunk_no] = []

            df.chunk_start()
            # each chunk with differnt amount of data
            for i in range(randint(0, 50)):
                x = now(df)
                y = randint(0, 2 ** 24 - 1)
                df.append_value('ppg_ir', y, x)

                x_all[chunk_no].append(x)
                y_all[chunk_no].append(y)

            # 50% probability that the chunk will get closed
            if randint(0,1):
                df.chunk_stop()

        df.close()

        # assert

        chunk_index = 0
        # for i, chunk in enumerate(df.chunks):
        for i in range(no_chunks):

            if not y_all[i]:
                continue

            chunk = df.chunks[chunk_index]
            assert chunk.finalized, f'Chunk index {i} not finalized'
            assert np.allclose(chunk.c.ppg_ir.x, np.asarray(x_all[i])), f'x values not consistent in chunk index {i}'
            assert np.allclose(chunk.c.ppg_ir.y, np.asarray(y_all[i])), f'y values not consistent in chunk index {i}'
            chunk_index += 1


@pytest.mark.parametrize('add_new_data_type_at_chunk, add_new_data_type_at_sample', (
    (1, 0),
    (1, 1),
    (1, 2),
    (1, 3),
    (1, 4),
    (1, 5),
    (1, 6),
    (1, 7),
    (1, 8),
    (1, 9),
    (2, 0),
    (2, 1),
    (2, 2),
    (2, 3),
    (2, 4),
    (2, 5),
    (2, 6),
    (2, 7),
    (2, 8),
    (2, 9),
))
def test_chunks_unfinished_chunks_when_adding_a_new_data_type_at_some_point_for_normal_and_combined_cols(fixture_empty_df, fixture_reduce_slice_size_24, add_new_data_type_at_chunk,
                                                                                                add_new_data_type_at_sample):
    # the first chunk does not contain ppg_ir, but the following ones. This should not cause any troubles

    def append_hr(chunk_idx):
        x = now(df)
        y_hr = randint(0, 2 ** 8 - 1)
        df.append_value('heart_rate', y_hr, x)
        x_all_hr[chunk_idx].append(x)
        y_all_hr[chunk_idx].append(y_hr)

    def append_ir(chunk_idx):
        x = now(df)
        y_ppg = randint(0, 2 ** 24 - 1)
        df.append_value('ppg_ir', y_ppg, x)
        x_all_ir[chunk_idx].append(x)
        y_all_ir[chunk_idx].append(y_ppg)

    def append_combined(chunk_idx):
        x = now(df)
        y_ppg_red = randint(0, 2 ** 24 - 1)
        y_ppg_am = randint(0, 2 ** 24 - 1)
        df.add_combined_columns(['ppg_red', 'ppg_ambient'], 'ppg')
        df.append_value('ppg', [y_ppg_red, y_ppg_am], x)
        x_all_comb[chunk_idx].append(x)
        y_all_red[chunk_idx].append(y_ppg_red)
        y_all_am[chunk_idx].append(y_ppg_am)

    df = fixture_empty_df
    df._date_time_start = now()

    x_all_hr = {0: [], 1: [], 2: []}
    y_all_hr = {0: [], 1: [], 2: []}

    x_all_ir = {0: [], 1: [], 2: []}
    y_all_ir = {0: [], 1: [], 2: []}

    x_all_comb = {0: [], 1: [], 2: []}
    y_all_red = {0: [], 1: [], 2: []}
    y_all_am = {0: [], 1: [], 2: []}

    # unfinished chunk with no ppg_ir
    df.chunk_start()    # << if this chunk start is left out, then the x value gets generated before the chunk._date_time_start
                        #    => therefore the value would not appear in the chunk
    for i in range(10):
        append_hr(0)
        if add_new_data_type_at_chunk == 1:
            if i >= add_new_data_type_at_sample:
                append_ir(0)
                append_combined(0)

    # next unfinished chunk with new data_col ppg_ir
    df.chunk_start()
    for i in range(10):
        append_hr(1)

        if add_new_data_type_at_chunk == 2:
            if i >= add_new_data_type_at_sample:
                append_ir(1)
                append_combined(1)

    # last chunk
    df.chunk_start()

    append_hr(2)
    append_ir(2)
    append_combined(2)

    df.close()

    # assert

    for i, chunk in enumerate(df.chunks):
        assert np.allclose(chunk.c.heart_rate.x, np.asarray(x_all_hr[i])), f'hr x values inconsistent in chunk index {i}'
        assert np.allclose(chunk.c.heart_rate.y, np.asarray(y_all_hr[i])), f'hr y values inconsistent in chunk index {i}'
        if i == add_new_data_type_at_chunk:
            assert np.allclose(chunk.c.ppg_ir.x, np.asarray(x_all_ir[i])), f'ppg ir x values inconsistent in chunk index {i}'
            assert np.allclose(chunk.c.ppg_ir.y, np.asarray(y_all_ir[i])), f'ppg ir y values inconsistent in chunk index {i}'
            assert np.allclose(chunk.c.ppg_red.x, np.asarray(x_all_comb[i])), f'ppg red x values inconsistent in chunk index {i}'
            assert np.allclose(chunk.c.ppg_red.y, np.asarray(y_all_red[i])), f'ppg red y values inconsistent in chunk index {i}'
            assert np.allclose(chunk.c.ppg_ambient.x, np.asarray(x_all_comb[i])), f'ppg red x values inconsistent in chunk index {i}'
            assert np.allclose(chunk.c.ppg_ambient.y, np.asarray(y_all_am[i])), f'ppg red y values inconsistent in chunk index {i}'

@pytest.mark.parametrize('store_and_load_from_db', [
    False,
    True,
])
def test_chunks_whether_finalized_and_unfinalized_chunks_have_correct_statistics_for_normal_and_combined_cols(store_and_load_from_db, fixture_empty_df, fixture_reduce_slice_size_8):
    # test if min, max, median, ... are correct for all finalized and unfinalized slices

    df = fixture_empty_df
    df._date_time_start = now()

    y_all_hr = {}

    y_all_spo2 = {}
    y_all_qt = {}

    attributes = ['min', 'max', 'mean', 'median', 'samples', 'upper_quartile', 'lower_quartile']

    df._date_time_start = now()
    df.add_combined_columns(['spo2', 'quality'], 'other')

    # 15 chunks total
    no_chunks = 30
    i = None
    for chunk_no in range(no_chunks):
        y_all_hr[chunk_no] = {'values': []}
        y_all_spo2[chunk_no] = {'values': []}
        y_all_qt[chunk_no] = {'values': []}

        df.chunk_start()
        # each chunk with differnt amount of data
        for i in range(randint(0, 15)):
            y = randint(0, 2 ** 8 - 1)
            df.append_value('heart_rate', y, now(df))
            y_all_hr[chunk_no]['values'].append(y)

            y1 = randint(0, 2 ** 8 - 1)
            y2 = randint(0, 2 ** 8 - 1)
            df.append_value('other', [y1, y2], now(df))
            y_all_spo2[chunk_no]['values'].append(y1)
            y_all_qt[chunk_no]['values'].append(y2)

        # 50% probability that the chunk will get finalized. First one will never be finalized
        if i == 1:
            df.chunk_stop()
        elif randint(0, 1):
            df.chunk_stop()

        # calculate expected statistics for each chunk
        for data_type_dic in [y_all_spo2, y_all_hr, y_all_qt]:

            if data_type_dic[chunk_no]['values']:
                data_type_dic[chunk_no]['values'].sort()
                length = len(data_type_dic[chunk_no]['values'])
                half = int(length / 2)
                first_quarter = int(length / 4)
                third_quarter = int(length * 3 / 4)
                data_type_dic[chunk_no]['min'] = round(float(min(data_type_dic[chunk_no]['values'])), 2)
                data_type_dic[chunk_no]['max'] = round(float(max(data_type_dic[chunk_no]['values'])), 2)
                data_type_dic[chunk_no]['mean'] = round(float(sum(data_type_dic[chunk_no]['values']) / length), 2)
                data_type_dic[chunk_no]['samples'] = length
                data_type_dic[chunk_no]['median'] = round(float(data_type_dic[chunk_no]['values'][half]), 2)
                data_type_dic[chunk_no]['upper_quartile'] = round(float(data_type_dic[chunk_no]['values'][third_quarter]), 2)
                data_type_dic[chunk_no]['lower_quartile'] = round(float(data_type_dic[chunk_no]['values'][first_quarter]), 2)
            else:
                data_type_dic[chunk_no]['median'] = None
                data_type_dic[chunk_no]['upper_quartile'] = None
                data_type_dic[chunk_no]['lower_quartile'] = None
                data_type_dic[chunk_no]['min'] = None
                data_type_dic[chunk_no]['max'] = None
                data_type_dic[chunk_no]['samples'] = 0
                data_type_dic[chunk_no]['mean'] = None

    df.close()

    if store_and_load_from_db:
        hash = df.hash_id
        df = DataFile.objects(_hash_id=hash).first()

    # assert
    chunk_index = 0
    # for i, chunk in enumerate(df.chunks):
    for i in range(no_chunks):

        if not y_all_hr[i]['values']:
            continue

        chunk = df.chunks[chunk_index]

        for attr in attributes:
            assert y_all_hr[i][attr] == getattr(chunk.c.heart_rate, attr), f'hr: statistics {attr} wrong in chunk {i}'
            assert y_all_spo2[i][attr] == getattr(chunk.c.spo2, attr), f'spo2: statistics {attr} wrong in chunk {i}'
            assert y_all_qt[i][attr] == getattr(chunk.c.quality, attr), f'quality: statistics {attr} wrong in chunk {i}'

        chunk_index += 1

# todo test wenn nach chunk start daten geschrieben wurden und der chunk nach einem "Neustart" finalisiert werden soll
@pytest.mark.parametrize('finalization_method', ['close', 'chunk_stop', 'chunk_start'])
def test_beginn_chunk_write_bin_but_load_df_from_db_again_before_finalizing_the_chunk(fixture_empty_df, finalization_method):

    df = fixture_empty_df

    df.add_column('battery')
    df.add_combined_columns(['ppg_ir', 'ppg_red'], 'ppg')

    df.append_value('heart_rate', 1, 1.0)
    df.append_value('ppg', [2,3], 1.5)
    df.store()
    hash = df.hash_id

    # simulate a restart of the gatway with an unfinalized chunk
    del df
    df = DataFile.objects(_hash_id=hash).first()

    df.append_value('heart_rate', 3, 2.0)
    df.append_value('ppg', [4,5], 2.5)

    if finalization_method == 'close':
        df.close()
    elif finalization_method == 'chunk_stop':
        df.chunk_stop()
    elif finalization_method == 'chunk_start':
        df.chunk_start()

    # assert

    x_hr = [1.0, 2.0]
    y_hr = [1, 3]
    x_ppg = [1.5, 2.5]
    y_ppg_ir = [2, 4]
    y_ppg_red = [3, 5]

    assert df.chunks[0].finalized
    # heart rate
    assert np.allclose(df.chunks[0].c.heart_rate.x, np.array(x_hr))
    assert np.allclose(df.chunks[0].c.heart_rate.y, np.array(y_hr))
    # combined cols
    assert np.allclose(df.chunks[0].c.ppg_red.x, np.array(x_ppg))
    assert np.allclose(df.chunks[0].c.ppg_ir.x, np.array(x_ppg))

    assert np.allclose(df.chunks[0].c.ppg_red.y, np.array(y_ppg_red))
    assert np.allclose(df.chunks[0].c.ppg_ir.y, np.array(y_ppg_ir))
