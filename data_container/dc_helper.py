import random
import string
import os
from pathlib import Path
import sys
import json
from datetime import datetime
import pytz
import re

from . import config
from builtins import staticmethod
logger = config.logger

# ToDo: check json content for consistency -> no duplicate for the ids...
class DcHelper():

    @staticmethod
    def seconds_to_time_str(seconds):

        if seconds == None:

            #return 'None'
            seconds = 0

        days = int(seconds / (3600*24))
        if days:
            return str(days) + ' days'
        seconds = seconds - days * (3600*24)
        hours = int(seconds / 3600)
        seconds = seconds - hours * 3600
        minutes = int(seconds / 60)
        seconds = seconds - minutes * 60

        time_str = str(hours).zfill(2) + ':' + str(minutes).zfill(2) + ':' + str(int(seconds)).zfill(2)

        return time_str

    @staticmethod
    def seconds_to_time_str_2(seconds):

        if seconds == None:

            #return 'None'
            seconds = 0

        days = int(seconds / (3600*24))
        if days:
            return str(days) + ' days'

        hours = int(seconds / 3600)
        if hours:
            return str(hours) + ' hours'

        minutes = int(seconds / 60)
        if minutes:
            return str(minutes) + ' min'

        else:
            return str(int(seconds)) + ' sec'

    @staticmethod
    def file_size_str(size_bytes):

        if size_bytes < 1024:
            return str(size_bytes)

        elif size_bytes < (1024*1024):
            size_new = size_bytes/(1024)
            if size_new >= 10:
                return str(int(round(size_new, 0))) + ' KB'
            else:
                return str(round(size_new, 1)) + ' KB'

        elif size_bytes < (1024*1024*1024):
            size_new = size_bytes/(1024*1024)
            if size_new >= 10:
                return str(int(round(size_new, 0))) + ' MB'
            else:
                return str(round(size_new, 1)) + ' MB'

        else:
            size_new = size_bytes/(1024*1024*1024)
            if size_new >= 10:
                return str(int(round(size_new, 0))) + ' GB'
            else:
                return str(round(size_new, 1)) + ' GB'

    @staticmethod
    def helper_dtype_size(dtype):

        if dtype in ['int8', 'uint8']:
            return 1
        elif dtype in ['int16', 'uint16', 'float16']:
            return 2
        elif dtype in ['int24', 'uint24']:
            return 3
        elif dtype in ['int32', 'uint32', 'float32']:
            return 4
        elif dtype in ['float64']:
            return 8
        else:
            logger.error('helper_dtype_size() ' + str(dtype) +' not found')
            raise ValueError

    @staticmethod
    def int_to_uint24_lsb_first(int_value):

        # lsb first (byte_0 = lsb)
        byte_2 = int_value >> 16
        byte_1 = (int_value >> 8) & 0xFF
        byte_0 = int_value & 0xFF

        try:
            uint24 = bytes([byte_0, byte_1, byte_2])
        except ValueError:
            logger.warning('int_to_uint24_lsb_first() cast error for: ' + str(int_value) + ' -> ' + hex(int_value))
            raise ValueError

        return uint24

    @staticmethod
    def int_list_to_int24_lsb_first(int_value_list):

        bin_data = bytearray()

        for int_value in int_value_list:

            if int_value > int(((2**24)/2)-1):
                logger.error(f'int_list_to_int24_lsb_first(): too big int_value {int_value} found! Replacing it with ((2**24)/2)-1')
                int_value = int(((2**24)/2)-1)
            if int_value < int(-(2**24)/2):
                logger.error(f'int_list_to_int24_lsb_first(): too small int_value {int_value} found! Replacing it with -((2**24)/2)')
                int_value = int(-(2**24)/2)

            int24 = int_value.to_bytes(length=3, byteorder='little', signed=True)

            bin_data += int24

        return bin_data

    @staticmethod
    def int_list_to_int24_msb_first(int_value_list):

        bin_data = bytearray()

        for int_value in int_value_list:

            if int_value > int(((2**24)/2)-1):
                logger.error(f'int_list_to_int24_lsb_first(): too big int_value {int_value} found! Replacing it with ((2**24)/2)-1')
                int_value = int(((2**24)/2)-1)
            if int_value < int(-(2**24)/2):
                logger.error(f'int_list_to_int24_lsb_first(): too small int_value {int_value} found! Replacing it with -((2**24)/2)')
                int_value = int(-(2**24)/2)

            int24 = int_value.to_bytes(length=3, byteorder='big', signed=True)

            bin_data += int24

        return bin_data

    @staticmethod
    def int_list_to_uint24_lsb_first(int_value_list):

        bin_data = bytearray()

        for int_value in int_value_list:

            if int_value < 0:
                logger.error(f'int_list_to_uint24_lsb_first(): negative int_value {int_value} found! Replacing it with 0')
                int_value = 0
            if int_value >= 2**24:
                logger.error(f'int_list_to_uint24_lsb_first(): too big int_value {int_value} found! Replacing it with 2**24-1')
                int_value = 2**24-1

            # lsb first (byte_0 = lsb)
            byte_2 = int_value >> 16
            byte_1 = (int_value >> 8) & 0xFF
            byte_0 = int_value & 0xFF

            try:
                bin_data += bytes([byte_0, byte_1, byte_2])
            except ValueError:
                logger.error('int_list_to_uint24_lsb_first() cast error for: ' + str(int_value) + ' -> ' + hex(int_value) + ' byte_0: ' + str(byte_0) + ' byte_1: ' + str(byte_1) + ' byte_2: ' + str(
                    byte_2))
                raise ValueError

        return bin_data

    @staticmethod
    def uint24_lsb_first_to_int_list(bin_data):

        value_list = []
        i = 0
        iterations_float = len(bin_data)/3
        iterations = int(iterations_float)

        if abs(iterations_float - iterations) > 0:
            logger.error('to_uint24_lsb_first_to_int_list() something went wrong (iterations_float)!')

        for i in range(iterations):

            byte_data = bin_data[3*i:3*i+3]
            value = 0
            for exp in range(3):
                value = value + 0x100**exp * byte_data[exp]
            value_list.append(value)

        return value_list

    @staticmethod
    def int24_lsb_first_to_int_list(bin_data):

        value_list = []
        i = 0
        iterations_float = len(bin_data)/3
        iterations = int(iterations_float)

        if abs(iterations_float - iterations) > 0:
            logger.error('to_int24_lsb_first_to_int_list() something went wrong (iterations_float)!')

        for i in range(iterations):

            byte_data = bin_data[3*i:3*i+3]
            value = int.from_bytes(byte_data, byteorder='little', signed=True)
            #value = byte_data[0] << 16 | byte_data[1] << 8 | byte_data[2]
            value_list.append(value)

        return value_list

    @staticmethod
    def int24_msb_first_to_int_list(bin_data):
        # e.g. EEG Data comes as MSB data

        value_list = []
        i = 0
        iterations_float = len(bin_data)/3
        iterations = int(iterations_float)

        if abs(iterations_float - iterations) > 0:
            logger.error('to_int24_lsb_first_to_int_list() something went wrong (iterations_float)!')

        for i in range(iterations):

            byte_data = bin_data[3*i:3*i+3]
            value = int.from_bytes(byte_data, byteorder='big', signed=True)
            #value = byte_data[0] << 16 | byte_data[1] << 8 | byte_data[2]
            value_list.append(value)

        return value_list

    @staticmethod
    def datetime_validation(datetime_in, timezone=None):

        if type(datetime_in) is str:

            datetime_in_orig = datetime_in
            datetime_in = datetime_in.strip()

            # do some replacements

            # 2020-11-15 13:25:13 -> 2020-11-15_13:25:13
            if len(datetime_in.split(' ')) == 2:
                datetime_in = datetime_in.replace(' ', '_')

            # 2020-11-15T13:25:13 -> 2020-11-15_13:25:13
            elif len(datetime_in.split('T')) == 2:
                datetime_in = datetime_in.replace('T', '_')

            # 20201115132513 -> 20201115_132513
            elif re.search('[0-9]{14}', datetime_in):
                datetime_in = datetime_in[:8] + '_' + datetime_in[-6:]

            # 202011151325 -> 20201115_132500
            elif re.search('[0-9]{12}', datetime_in):
                datetime_in = datetime_in[:8] + '_' + datetime_in[-4:] + '00'

            try:
                date_in, time_in = datetime_in.split('_')
            except ValueError:
                logger.error('no valid datetime format found for ' + str(datetime_in_orig))
                return None

            date_in = date_in.replace('/', '')
            date_in = date_in.replace('-', '')
            time_in = time_in.replace(':', '')
            datetime_in = date_in + '_' + time_in

            # eval datetime format_str
            format_str = None

            # match with format 20201115_132513
            if re.search('[0-9]{8}_[0-9]{6}', datetime_in) and len(datetime_in) == 15:
                format_str = '%Y%m%d_%H%M%S'

            # match with format 20201115_1325
            elif re.search('[0-9]{8}_[0-9]{4}', datetime_in) and len(datetime_in) == 13:
                format_str = '%Y%m%d_%H%M'

            # match with format 20201115_1325.34323454
            elif re.search('[0-9]{8}_[0-9]{6}.[0-9]', datetime_in) and len(datetime_in.split('.')[0]) == 15 and len(datetime_in.split('.')) == 2:
                format_str = '%Y%m%d_%H%M%S.%f'

            if not format_str:
                logger.error('no valid datetime format found for ' + str(datetime_in_orig))
                return None

            try:
                datetime_in = datetime.strptime(datetime_in, format_str)
            except ValueError:
                logger.error('no valid datetime format found for ' + str(datetime_in_orig))
                return None

        # check timezone
        if timezone is None and datetime_in.tzinfo is None:
            logger.error('datetime input needs to be timezone-aware or second param timezone needs to be defined')
            return None

        # check datetime_in
        if datetime_in is None:
            logger.error('datetime is None')
            return None

        # check timezone
        if timezone:
            timezone = pytz.timezone(timezone)
        else:
            timezone = pytz.timezone(str(datetime_in.tzinfo))

        # timezone-aware datetime instance
        if datetime_in.tzinfo:
            datetime_out = datetime_in
        else:
            datetime_out = timezone.localize(datetime_in)

        dt_now = timezone.localize(datetime.now())
        #print(dt_now)
        if dt_now < datetime_out:
            logger.error('Your datetime is in the future ' + str(datetime_out))
            return None

        return datetime_out

    @staticmethod
    def utc_to_local(dt, timezone_str):

        # logger.debug(f'utc_to_local: dt={dt}, timezone_str={timezone_str}')

        if dt:

            if not dt.tzinfo:
                utc_dt = pytz.utc.localize(dt)
            else:
                utc_dt = dt

            if not timezone_str:
                timezone_str = 'Europe/Berlin'

            return utc_dt.astimezone(pytz.timezone(timezone_str))

        else:
            return None

    def generate_hash(self, hash_len=None):
        """docstring description

        Args:
            param1: The first parameter.
            param2: The second parameter.

        Returns:
            The return value. True for success, False otherwise.

        """

        # ToDo check that hash does not exist yet

        if hash_len is None:
            hash_len = 5

        characters = string.ascii_uppercase + string.digits

        # remove ambiguous characters
        characters = characters.replace('O', '')
        characters = characters.replace('I', '')

        hash_str = ''.join(random.choice(characters) for i in range(hash_len))

        return hash_str

    @staticmethod
    def remove_keys_from_dict(dictionary, keys_to_exclude):
        # iterate through dictionary in full depth and remove unknown fields
        # stolen from https://stackoverflow.com/questions/10179033/how-to-recursively-remove-certain-keys-from-a-multi-dimensionaldepth-not-known
        # hardcore recursive one-liner B-) Tim: @author actually not an one liner, but a nice one :-P
        if isinstance(dictionary, dict):
            dictionary = {
                key: DcHelper.remove_keys_from_dict(value, keys_to_exclude)
                for key, value in dictionary.items()
                if key not in keys_to_exclude}
            return dictionary
        else:
            return dictionary

    @staticmethod
    def get_number_of_iterations(samples_list, samples_per_slice):
        # determine the numbers of iterations when filling slices with a list of samples

        # if sample_list size is an integer multiple of the slice size, there would be one iteration too much
        if len(samples_list) % samples_per_slice == 0:
            n = 0
        else:
            n = 1

        iterations = int(len(samples_list) / samples_per_slice) + n
        # at least one iteration (assuming there is at least one data value)
        if iterations == 0: iterations = 1

        return iterations


# ######################################################################
# HELPER CLASSES

class AttributesContainer():

    def __init__(self, attr_dic):

        if attr_dic:
            self._attr_dic = attr_dic

            for attr in attr_dic:

                setattr(self, attr, attr_dic[attr])
        else:
            self._attr_dic ={}

    def __iter__(self):

        for attr in self._attr_dic:
            yield attr, self._attr_dic[attr]

class InstancesContainer():

    def __init__(self, attr_dic):

        if attr_dic:

            self._attr_dic = attr_dic
            for attr in attr_dic:

                setattr(self, attr, attr_dic[attr])
        else:
            self._attr_dic ={}

    def __iter__(self):

        for attr in self._attr_dic:
            yield self._attr_dic[attr]

class ClassContainer():

    def __init__(self):
        pass
