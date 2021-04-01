import pytest
from data_container.dc_helper import DcHelper
from random import randint

@pytest.mark.skip
def test_convert_all_possible_uint24_to_bytes_and_back_to_integer():
    # this test takes so much time and probably is of no big help to do it every time
    numbers = int(2**24)

    int_value_list = []
    # generate a list with all uint24 numbers
    for number in range(numbers):
        int_value_list.append(int(number))
    
    # pass the list to the function
    bin_data = DcHelper.int_list_to_uint24_lsb_first(int_value_list)
    assert type(bin_data) is bytearray
    
    # convert it back
    back_converted_list = DcHelper.uint24_lsb_first_to_int_list(bin_data)
    assert int_value_list == back_converted_list

def test_convert_negative_numbers_to_uint24_and_back():
    
    # pass a list with one negative integer
    byte_list = DcHelper.int_list_to_uint24_lsb_first([1000, 2400000, 0, -1, 10000, 2**24-1, 2**24])
    back_converted_list = DcHelper.uint24_lsb_first_to_int_list(byte_list)
    
    # negative values must turn 0 
    assert [1000, 2400000, 0, 0, 10000, 2**24-1, 2**24-1] == back_converted_list

def test_convert_positive_and_negative_numbers_to_int24_and_back():

    # pass a list with one negative integer
    byte_list = DcHelper.int_list_to_int24_lsb_first([-2**24, -2*23, -1, 0, 1, 2**23-1, 2**23, 2**24])
    back_converted_list = DcHelper.int24_lsb_first_to_int_list(byte_list)

    # negative values must turn 0
    assert [-2**23, -2*23, -1, 0, 1, 2**23-1, 2**23-1, 2**23-1] == back_converted_list

