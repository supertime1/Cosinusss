import pytest
from pathlib import Path
from data_container.data_slice import DataSlice
from data_container.tests.testing_helper_functions import EmptyClass
import builtins
import gzip
import zstd
import os


# todo:
#  - somehow mock the "with open()" otherwise it will write useless data # https://pypi.org/project/mock-open/
#  - find a way to provoke the FileNotFound when decompressing
@pytest.mark.parametrize('len_values, values_write_pointer, logger_level, logger_message, path_name,', [
    (1,2,'ERROR', 'write_bin inconsistency', ''),  # write pointer longer than values
    (2,2,'DEBUG', 'write_bin not necessary', ''),  # normal behavior nothing to write
    # (3,2,'DEBUG', 'write_bin for slice HASH_LONG', ''),  # normal behavior writing
    # (3,2,'ERROR', 'zstd failed', '.zst'),  # zstd file not found
    # (3,2,'ERROR', 'gz failed', '.gz'),
])
def test_write_file_with_wrong_write_pointer_provokes_error(caplog, mocker, monkeypatch,
        len_values, values_write_pointer, logger_level, logger_message, path_name
):
    # data loss might occur so that the meta data in the db is not consistent with the binary data

    # https://santexgroup.com/wp-content/uploads/2014/10/mock_python.html#slide6
    
    caplog.set_level(logger_level, logger='data_container')

    class Df: consistent = True

    path_mock = mocker.MagicMock()
    if path_name:
        path_mock.__str__.return_value = 'somecrazyrandompathnamethatdoesnotexist'+path_name
        a = str(path_mock)

    # mock this property
    monkeypatch.setattr(DataSlice, 'hash_long', 'HASH_LONG')

    example_values = [1, 2, 3, 4, 5]

    sl = DataSlice()
    sl.df = Df()
    sl._values = example_values[:len_values]
    sl.values_write_pointer = values_write_pointer
    sl.dtype = 'uint24'
    sl._path = path_mock

    # actual test

    sl.write_bin()

    # assert logger messages

    assert caplog.records[0].levelname == logger_level
    assert logger_message in caplog.records[0].message


@pytest.mark.parametrize('algorithm', [
    'zstd',
    'gzip',
])
def test_compress_method_must_create_error_log_if_file_does_not_exist(monkeypatch, caplog, algorithm):
    # there must be an error log but the program may not crash if the slice is not there
    caplog.set_level('ERROR')
    
    def mock_open(*args): raise FileNotFoundError
    monkeypatch.setattr(builtins, 'open', mock_open)
    
    sl = DataSlice()
    sl.df = EmptyClass()
    sl.df.status_closed = True
    sl._path = Path('some_path/ABC.bin')
    monkeypatch.setattr(DataSlice, 'hash_long', 'some_name')
    
    sl.compress(algorithm=algorithm)
    
    assert f'File {str(sl._path)} not found' in caplog.text, f'Error log did not contain the required log. Log: {caplog.text}'


@pytest.mark.parametrize('algorithm, ending', [
    ('zstd', 'zst'),
    ('gzip', 'gz'),
])
def test_compress_a_file_which_is_already_compressed_but_status_compressed_is_wrong(algorithm, ending):
    # it already happened that this status was wrong ... 
    
    sl = DataSlice()
    sl.df = EmptyClass()
    sl.df.status_closed = True
    sl._path = Path(f'some_path/ABC.{ending}')
    sl.status_compressed = False

    sl.compress(algorithm=algorithm)

    assert sl.status_compressed is True, 'The status_compressed was not corrected'

# todo: this test is not finished yet...
@pytest.mark.xfail
@pytest.mark.parametrize('algorithm, ending', [
    ('zstd', 'zst'),
    ('gzip', 'gz'),
])
def test_compress_must_correctly_set_status_compressed_and_status_sent_server(monkeypatch, algorithm, ending):
    
    def mock_do_nothing(*args): pass
    class EmptyClass2(EmptyClass):
        def read(self):
            return ''
    ec2 = EmptyClass2()
    
    monkeypatch.setattr(builtins, 'open', mock_do_nothing)
    # monkeypatch.setattr(, 'write', mock_do_nothing)
    monkeypatch.setattr(gzip, 'compress', ec2)
    monkeypatch.setattr(zstd, 'ZSTD_compress', ec2)
    monkeypatch.setattr(os, 'remove', mock_do_nothing)

    sl = DataSlice()
    sl.df = EmptyClass()
    sl.df.status_closed = True
    sl._path = Path('some_path/ABC.bin')
    monkeypatch.setattr(DataSlice, 'hash_long', 'some_name')

    sl.compress(algorithm=algorithm)