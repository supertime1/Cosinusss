import logging
from logging.handlers import RotatingFileHandler
from logging.config import fileConfig
from pathlib import Path
import os
import json
import random
import string
import sys
import traceback
from mongoengine import connect
import pymongo
import platform

class DcConfig():

    def __init__(self):

        self._init_called = False

        self._file_path = Path(os.path.dirname(os.path.realpath(__file__)))
        self._DC_VERSION = 6
        self._SLICE_MAX_SIZE = 2880000
        self._request_timeout = 60
        self._data_types_dict = json.load(open(self._file_path / Path('data_types.json')))
        self._data_types = tuple(self.data_types_dict.keys())
        self._logger = logging.getLogger('data_container')
        self._data_path = None
        self._df_path = None
        self._log_path = None
        self._log_file_path = None
        self._repos_path = None
        self._repos_name = None
        self._data_container_config_path = None
        self._producer_hash = None
        self._operating_system = platform.system()
        self._numpy_size = 'optimized'

        # default logger (for initialization only)
        self.logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)5s - %(module)15s:%(lineno)4s - %(message)s', "%Y-%m-%d %H:%M:%S")
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.debug('DcConfig initialized')

    def init(self, db_name=None, data_path=None, redis_db_index=None,
             logger_path=None, logger_config_file_path=None, logger_level=None, producer_hash=None,
             live_data=False, SLICE_MAX_SIZE=None, numpy_size=None):

        if self._init_called:
            self.logger.error('data_container.config.init() can only be called once')
            return False

        # set data_path, data/df, data/logs
        if data_path is not None:
            self.data_path = data_path
            if self.data_path is None:
                return False
        else:
            # default path
            new_data_path = self.file_path.parents[0] / Path('data')
            if not self.__check_dir_path(new_data_path):
                os.mkdir(new_data_path)
            self.data_path = new_data_path

        # set path of logs
        if logger_path is not None:
            self.log_path = logger_path
            if self.log_path is None:
                return False
        else:
            self._log_path = self.data_path / Path('logs')

        self._df_path = self.data_path / Path('df')
        self._log_file_path = self.log_path / Path('logger.log')
        self._repos_path = os.path.dirname(__file__)
        self._repos_name = os.path.basename(self.repos_path)
        self._data_container_config_path = self.data_path / Path('data_container_config.json')

        # creating all directories
        if not os.path.exists(self.data_path):
            os.mkdir(str(self.data_path))
        if not os.path.exists(str(self.df_path)):
            os.mkdir(str(self.df_path))
        if not os.path.exists(str(self.log_path)):
            os.mkdir(str(self.log_path))

        # remove the logger handler created in the __init__ and use the correct logger
        self.logger.handlers = []
        if logger_config_file_path:

            try:
                fileConfig(logger_config_file_path, defaults={'log_file_path': self._log_file_path})
                self._logger = logging.getLogger('labgateway.data_container')
                self.logger.info('{}: labgateway_logger.conf found - logger initialized'.format(self._repos_name))
            except Exception as e:
                self._setup_logger(logger_level)
                self.logger.error('logger initialization failed')
                self.logger.error(e)
                self.logger.error(str(traceback.format_exc()))

        else:
            self._setup_logger(logger_level)

        try:
            self._producer_hash = json.load(open(self.data_container_config_path))['producer_hash']
        except (FileNotFoundError, json.JSONDecodeError):
            self._producer_hash = self.generate_hash(10)
            json.dump({'producer_hash': self.producer_hash}, open(self.data_container_config_path, 'w'))

        # connect to database
        if db_name:
            self.connect_db(db_name)

        # just for special tests!
        if SLICE_MAX_SIZE:
            self.logger.warning('Be aware, changing SLICE_MAX_SIZE is only for testing not for productive systems!')
            self._SLICE_MAX_SIZE = SLICE_MAX_SIZE

        #
        if numpy_size:
            # if you do e.g. df.c.acc_x.y and then do something to these values and they are bigger than float16, then it would lead to an overflow.
            # this will not happen (that fast) in 'maximize' setting
            if numpy_size == 'maximize':
                self._numpy_size = 'maximize'
                self.logger.info(f'Using maximum numpy size to avoid overflows when working with the data')
            elif numpy_size == 'optimized':
                self._numpy_size = 'optimized'
                self.logger.info(f'Using optimized numpy size (works only on Linux and MacOS)')
            else:
                self.logger.warning(f'The specified numpy_size={numpy_size} is unknown')

        # all done
        self.logger.info('init of data_container successful')
        self._init_called = True

    def connect_db(self, db_name):
        # connect to database
        self.logger.info(f'connect to database "{db_name}"')
        self._db_name = db_name
        pymongo_client = pymongo.MongoClient(serverSelectionTimeoutMS=1)
        try:
            pymongo_client[self._db_name].command('ping')
        except pymongo.errors.ServerSelectionTimeoutError:
            self.logger.error('connection timeout to mongodb: start with "sudo systemctl start mongod"')
            sys.exit()
            return False
        connect(self._db_name)

    def _setup_logger(self, logger_level):

        if logger_level is None:
            logger_level = 'info'
        if logger_level.lower() == 'debug':
            self.logger.setLevel(logging.DEBUG)
        elif logger_level.lower() == 'info':
            self.logger.setLevel(logging.INFO)
        elif logger_level.lower() == 'warning':
            self.logger.setLevel(logging.WARNING)
        else:
            self.logger.setLevel(logging.DEBUG)

        rfh = logging.handlers.RotatingFileHandler(str(self.log_file_path), 'a', 10000000, 20)
        rfh.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(levelname)5s - %(module)15s:%(lineno)4s - %(message)s', "%Y-%m-%d %H:%M:%S")

        rfh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(ch)
        self.logger.addHandler(rfh)

    def __check_dir_path(self, dir_path):

        # ToDo: further tests here?
        if os.path.isdir(dir_path):
            return True
        else:
            return False

    @property
    def file_path(self):
        return self._file_path

    @property
    def DC_VERSION(self):
        return self._DC_VERSION

    @property
    def SLICE_MAX_SIZE(self):
        return self._SLICE_MAX_SIZE

    @property
    def request_timeout(self):
        return self._request_timeout

    @property
    def data_types_dict(self):
        return self._data_types_dict

    @property
    def data_types(self):
        return self._data_types

    @property
    def logger(self):
        return self._logger

    @property
    def data_path(self):
        return self._data_path

    @data_path.setter
    def data_path(self, data_path):
        if not self.__check_dir_path(data_path):
            self.logger.error(f'"data_path {data_path}" is not a valid and existing directory path.')
            self._data_path = None
            return False
        self._data_path = data_path
        self.logger.info(f'data_path is "{self.data_path}"')

    @property
    def df_path(self):
        return self._df_path

    @property
    def log_path(self):
        return self._log_path

    @log_path.setter
    def log_path(self, log_path):
        if not self.__check_dir_path(log_path):
            self.logger.error(f'"logger_path {log_path}" is not a valid and existing directory path.')
            self._log_path = None
            return False
        self._log_path = log_path
        self.logger.info(f'log_path is "{self.log_path}"')

    @property
    def log_file_path(self):
        return self._log_file_path

    @property
    def repos_path(self):
        return self._repos_path

    @property
    def repos_name(self):
        return self._repos_name

    @property
    def data_container_config_path(self):
        return self._data_container_config_path

    @property
    def producer_hash(self):
        return self._producer_hash

    @property
    def operating_system(self):
        return self._operating_system

    @property
    def numpy_size(self):
        return self._numpy_size

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

#print('data_container: Cosinuss GmbH, all rights reserved')
#print('data_container: call init(db_name="your_db_name") before using the data container')

