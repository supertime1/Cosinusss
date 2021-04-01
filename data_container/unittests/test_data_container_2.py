#!/usr/bin/env python3
import pytest
import time
import os
import pickle
from random import randint, uniform
import requests
import subprocess
from subprocess import PIPE
import json
import psutil
import shutil
from pathlib import Path
import hashlib
import math
from datetime import datetime
import functools
import numpy as np
import pymongo
from mongoengine import connect, disconnect
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
from data_container import Data, DataFile, DataColumn, DataSlice, DataChunk
from data_container.db_sync import DB_Sync
from data_container.dc_helper import DcHelper
from data_container.dc_config import config
from data_container.odm import Scope, Person, Project, User, Receiver, Device, EventLog, Comment, Config, ReceiverConfig
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
dc_helper = DcHelper()
logger = config.logger

data_types_dict = config.data_types_dict
SLICE_MAX_SIZE = config.SLICE_MAX_SIZE
BASE_DIR_LOCAL_SERVER = '/Users/felix/Cosinuss/pseudo_remote_server/'
BASE_DIR_UNITTEST = '/Users/felix/Cosinuss/lab_client/'

DF_DIR_LOCAL_SERVER = BASE_DIR_LOCAL_SERVER + 'data/df/'
DF_DIR_UNITTEST = BASE_DIR_UNITTEST + 'data/df/'

CLIENT_TEST_DB = 'integration_testing'
SERVER_TEST_DB = 'pseudo_remote_server_testing'
ORIGINAL_SERVER_DB = 'pseudo_remote_server'
SERVER_ADDRESS = 'http://localhost:8888'


#  __    __   _______  __      .______    _______ .______          _______  __    __  .__   __.   ______ .___________. __    ______   .__   __.      _______.
# |  |  |  | |   ____||  |     |   _  \  |   ____||   _  \        |   ____||  |  |  | |  \ |  |  /      ||           ||  |  /  __  \  |  \ |  |     /       |
# |  |__|  | |  |__   |  |     |  |_)  | |  |__   |  |_)  |       |  |__   |  |  |  | |   \|  | |  ,----'`---|  |----`|  | |  |  |  | |   \|  |    |   (----`
# |   __   | |   __|  |  |     |   ___/  |   __|  |      /        |   __|  |  |  |  | |  . `  | |  |         |  |     |  | |  |  |  | |  . `  |     \   \
# |  |  |  | |  |____ |  `----.|  |      |  |____ |  |\  \----.   |  |     |  `--'  | |  |\   | |  `----.    |  |     |  | |  `--'  | |  |\   | .----)   |
# |__|  |__| |_______||_______|| _|      |_______|| _| `._____|   |__|      \______/  |__| \__|  \______|    |__|     |__|  \______/  |__| \__| |_______/
#

def print_df_and_all_cols_and_slices(df):

    print(df)
    for col in df.c:
        print(col)
        for sl_type in col._slices:
            for sl in col._slices[sl_type]:
                print(sl)

    for chunk in df.chunks:
        print(chunk)
        for data_type in chunk.cols:
            chunk_col = chunk.cols[data_type]
            print(chunk_col)


def compare_lists(list1, list2, tolerance=0.5):
    # todo: why 0.2 difference sometimes???

    if len(list1) != len(list2):
        return False

    list1 = np.asarray(list1)
    list2 = np.asarray(list2)

    # check if both lists contain integers
    if isinstance(list1[0], (int, np.integer)) and isinstance(list2[0], (int, np.integer)):

        return np.array_equal(list1, list2)

    # at least one list contains float
    else:
        # numpy float values are not that precise to compare elementwise,
        # => see only if the differences are not larger than a certain threshold
        difflist = list1 - list2
        # print(max(difflist))
        if np.all(difflist < tolerance):
            return True
        else:
            return False


    # https://www.geeksforgeeks.org/python-check-if-two-lists-are-identical/
    # if functools.reduce(lambda i, j: i and j, map(lambda m, k: m == k, list1, list2), True):
    #     return True
    # else:
    #     return False

def generate_md5(data_path_str):

    with open(data_path_str, 'rb') as f:
        data = f.read()

    md5_hash = hashlib.md5(data).digest()

    return md5_hash

def compare_hash_md5_of_local_files(hash_long, ds_filename):
    server_data_path = DF_DIR_LOCAL_SERVER + hash_long + '/' +ds_filename
    working_data_path = DF_DIR_UNITTEST + hash_long + '/' + ds_filename

    hash_server = generate_md5(server_data_path)
    hash_working = generate_md5(working_data_path)

    return hash_server == hash_working

def compare_all_sent_slices_server_vs_working(df):

    for c in df.cols:
        for sl_type in df.cols[c]._slices:
            for sl in df.cols[c]._slices[sl_type]:
                ds_filename = sl._path.name
                if compare_hash_md5_of_local_files(df.hash_long, ds_filename) == False:
                    return False

    # todo: compare stuff in database??

    return True


def append_random_json_data(df, x=1):

    df.append_value('temperature',      uniform(36.0, 39.0), x)
    df.append_value('heart_rate',       randint(70, 160), x)
    df.append_value('quality',          randint(10, 90), x)
    df.append_value('respiration_rate', randint(10, 20), x)
    df.append_value('battery',          randint(0, 100), x)
    df.append_value('perfusion_ir',     randint(1, 10), x)
    df.append_value('spo2',             randint(1, 10), x)

    return df

def append_n_times_random_rawdata(df, x=1, sampling_rate=1):

    if sampling_rate == 0: sampling_rate=1

    for i in range(sampling_rate):
        df.append_value('ppg_ir', randint(1000, 10000), x + i / sampling_rate)
        df.append_value('ppg_red', randint(1000.0, 10000.0), x + i / sampling_rate)
        df.append_value('ppg_ambient', randint(1000.0, 10000.0), x + i / sampling_rate)
        df.append_value('acc_x', uniform(-1, 1), x + i / sampling_rate)
        df.append_value('acc_y', uniform(-1, 1), x + i / sampling_rate)
        df.append_value('acc_z', uniform(-1, 1), x + i / sampling_rate)

    return df

def start_webserver_process():

    # stop running instances
    stop_webapp()

    web_server_process = subprocess.Popen(BASE_DIR_LOCAL_SERVER + 'start_webapp.py', stdout=PIPE, stderr=PIPE, preexec_fn=os.setsid)

    return web_server_process

def stop_webapp():

    # Iterate over the all the running process
    for proc in psutil.process_iter():
        try:

            pinfo = proc.as_dict(attrs=['pid', 'name', 'create_time', 'cmdline'])
            python_cond = False
            webapp_cond = False

            for cmd in pinfo['cmdline']:
                if 'python' in cmd:
                    python_cond = True
                if 'start_webapp' in cmd:
                    webapp_cond = True
            if python_cond and webapp_cond:

                proc.terminate()
                for ch_proc in proc.children(recursive=True):
                    ch_proc.kill()
                proc.kill()
                print(f'Killed proc {proc}.')

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass


def findProcessIdByName(processName):
    """
    Get a list of all the PIDs of a all the running process whose name contains
    the given string processName
    """

    listOfProcessObjects = []

    # Iterate over the all the running process
    for proc in psutil.process_iter():
        try:
            pinfo = proc.as_dict(attrs=['pid', 'name', 'create_time', 'cmdline'])
            # Check if process name contains the given name string.
            if processName.lower() in pinfo['cmdline'].lower():
                listOfProcessObjects.append(pinfo)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

    return listOfProcessObjects;

#  _______  __  ___   ___ .___________. __    __  .______       _______     _______.
# |   ____||  | \  \ /  / |           ||  |  |  | |   _  \     |   ____|   /       |
# |  |__   |  |  \  V  /  `---|  |----`|  |  |  | |  |_)  |    |  |__     |   (----`
# |   __|  |  |   >   <       |  |     |  |  |  | |      /     |   __|     \   \
# |  |     |  |  /  .  \      |  |     |  `--'  | |  |\  \----.|  |____.----)   |
# |__|     |__| /__/ \__\     |__|      \______/  | _| `._____||_______|_______/

@pytest.fixture(autouse=True)
def fixture_autouse():

    # remember all files that are currently in server and client directory
    remember_all_current_files()

    # prepare the local server
    server_db = connect(SERVER_TEST_DB)
    server_db.drop_database(SERVER_TEST_DB)
    disconnect('default')
    setup_pseudo_remote_server()

    # create a basic db to be able to create a df
    client_db = connect(CLIENT_TEST_DB)
    client_db.drop_database(CLIENT_TEST_DB)
    disconnect('default')
    setup_client_db()


def remember_all_current_files():
    # after the test clean up all created files

    global server_dir_hashes_before, working_dir_hashes_before

    server_dir_hashes_before = []
    working_dir_hashes_before = []

    for entry in os.scandir(DF_DIR_LOCAL_SERVER):
        server_dir_hashes_before.append(entry.path)

    for entry in os.scandir(DF_DIR_UNITTEST):
        working_dir_hashes_before.append(entry.path)


def setup_pseudo_remote_server():

    global ORIGINAL_SERVER_DB

    # 1) change db in config file
    webserver_config = json.load(open(BASE_DIR_LOCAL_SERVER+'data/webapp_config.json', 'r'))
    ORIGINAL_SERVER_DB = webserver_config['db_name']
    webserver_config['db_name'] = SERVER_TEST_DB
    with open(BASE_DIR_LOCAL_SERVER+'data/webapp_config.json', 'w') as fp:
        json.dump(webserver_config, fp, indent=4)

    connect(SERVER_TEST_DB)
    setup_database('from_server')
    disconnect('default')

def setup_client_db():

    connect(CLIENT_TEST_DB)
    setup_database('for_df')
    disconnect('default')

def setup_database(description=None):

    if not description:
        description = 'for_df'

    scope = Scope()
    scope.name = description
    scope.timezone = 'Europe/Berlin'
    scope.email = description+'@web.de'
    scope.store()

    user = User()
    user.scope = scope
    user.username = description
    user.email = description+'@quark.de'
    user.password_hash = description
    user.password_salt = description
    user.store()

    db_init_config()
    db_init_superadmin()

    project = Project()
    project.scope = scope
    project.name = description
    project.store()

    person = Person()
    person.scope = scope
    person.project = project
    person.store()

    receiver = Receiver()
    receiver._hash_id = description
    receiver.scope = scope
    receiver.mac_address = 'RE:CE:IV:ER'
    receiver.udi_type = 'udi_type'
    receiver.udi_lot = 'udi_type'
    receiver.device_model = 'device_model'
    receiver.store()

    device = Device()
    device._hash_id = description
    device.scope = scope
    device.mac_address = 'DE:VI:CE'
    device.cap_size = 'M'
    device.udi_type = 'udi_type'
    device.udi_lot = 'udi_lot'
    device.device_model = 'device_model'
    device.store()

    return {
        'scope': scope,
        'user': user,
        'project': project,
        'person': person,
        'receiver': receiver,
        'device': device,
    }

def db_init_superadmin():

    if User.objects(username='superadmin'):

        print('superadmin already created.')

    else:

        user = User()
        user.username = 'superadmin'
        user.roles.append('superadmin')
        # new_password = subprocess.check_output('pwgen -B 18 1'.split()).decode().strip()
        new_password = 'eiW3si4aephag4jaes'
        user.password = new_password
        user.store()
        print('new superadmin with password: ' + new_password)

def db_init_config():

    if Config.objects(config_id='default'):

        print('default config already created.')

    else:

        config = Config()

        config.config_id = 'default'
        config.receiver_config = ReceiverConfig()
        config.receiver_config.sync_db_interval = 300
        config.receiver_config.df_store_interval = 900
        config.receiver_config.chunk_interval = 900
        config.receiver_config.temp_thres = 33
        config.receiver_config.receive_data_mode = 'soc'
        config.receiver_config.latest_release = '8'
        config.receiver_config.rec_cycles_time = 180
        config.receiver_config.rec_cycles_step = 900
        config.receiver_config.rec_cycles_tol = 300
        config.receiver_config.rawdata_service = True
        config.receiver_config.calc_ppg_quality = True
        config.receiver_config.calc_hr = False
        config.receiver_config.calc_spo2 = False
        config.receiver_config.calc_br = True

        config.save()

def new_df(database):

    connect(database)
    df = DataFile()
    df.project = Project.objects.first()
    df.person = Person.objects.first()

    return df

@pytest.fixture
def start_local_web_server():

    start_local_web_server = {'web_server_process': start_webserver_process()}
    time.sleep(2)
    return start_local_web_server

@pytest.fixture()
def start_local_web_server_plus_df(start_local_web_server, setup_database):

    setup_database['df'].server= SERVER_ADDRESS

    for key, val in setup_database.items():
        start_local_web_server[key] = val

    # hand over the whole package
    return start_local_web_server

@pytest.fixture
def fix_id_data_dic():

    # download some data and pass it as dictionary for use with DataFile
    # !! make sure you have access to this id !!
    id = 6891

    try:
        data_dic = pickle.load(open(f'unittest_data/test_data_dic_{id}', 'rb'))
    except FileNotFoundError:
        from lab_api_client import lab_api_client
        lac = lab_api_client.LabApiClient(server='lab.earconnect.de')
        lac.id(id)

        # load rawdata
        data_dic = {}
        for col in lac.rd.available_cols:
            # for col in ['heart_rate', '']:
            if col in ['ppg_quality', 'ble_service_debug_data_inserts']:
                continue
            y = lac.rd.cols[col].y
            x = lac.rd.cols[col].time_calc
            time_rec = lac.rd.cols[col].time_rec
            # shorten data with inconsistent length
            lx = len(x)
            ly = len(y)
            lr = len(time_rec)
            if lx != ly or lx != lr:

                minlen = min([lx, ly, lr])
                y = y[:minlen]
                x = x[:minlen]
                time_rec = time_rec[:minlen]

            # this was renamed in new data_types.json
            if col == 'breathing_rate':
                col = 'respiration_rate'
            if col == 'battery_percentage':
                col = 'battery'

            data_dic[col] = {}
            data_dic[col]['x'] = x
            data_dic[col]['y'] = y

        if not os.path.exists('unittest_data'):
            os.mkdir('unittest_data')
        pickle.dump(data_dic, open(f'unittest_data/test_data_dic_{id}', 'wb'))

    return data_dic
# .___________. _______     ___      .______       _______   ______   ____    __    ____ .__   __.
# |           ||   ____|   /   \     |   _  \     |       \ /  __  \  \   \  /  \  /   / |  \ |  |
# `---|  |----`|  |__     /  ^  \    |  |_)  |    |  .--.  |  |  |  |  \   \/    \/   /  |   \|  |
#     |  |     |   __|   /  /_\  \   |      /     |  |  |  |  |  |  |   \            /   |  . `  |
#     |  |     |  |____ /  _____  \  |  |\  \----.|  '--'  |  `--'  |    \    /\    /    |  |\   |
#     |__|     |_______/__/     \__\ | _| `._____||_______/ \______/      \__/  \__/     |__| \__|
#

def teardown_module(module):

    global ORIGINAL_SERVER_DB

    stop_webapp()

    disconnect(CLIENT_TEST_DB)

    # change back server db in config file
    webserver_config = json.load(open(BASE_DIR_LOCAL_SERVER+'data/webapp_config.json', 'r'))
    webserver_config['db_name'] = ORIGINAL_SERVER_DB
    with open(BASE_DIR_LOCAL_SERVER+'data/webapp_config.json', 'w') as fp:
        json.dump(webserver_config, fp, indent=4)

    # clean up new server hashes
    for entry in os.scandir(DF_DIR_LOCAL_SERVER):
        if entry.path not in server_dir_hashes_before:
            shutil.rmtree(entry.path)

    # clean up new local hashes
    for entry in os.scandir(DF_DIR_UNITTEST):
        if entry.path not in working_dir_hashes_before:
            shutil.rmtree(entry.path)


# .___  ___.   ______     ______  __  ___     _______  __  ___   ___ .___________. __    __  .______       _______     _______.
# |   \/   |  /  __  \   /      ||  |/  /    |   ____||  | \  \ /  / |           ||  |  |  | |   _  \     |   ____|   /       |
# |  \  /  | |  |  |  | |  ,----'|  '  /     |  |__   |  |  \  V  /  `---|  |----`|  |  |  | |  |_)  |    |  |__     |   (----`
# |  |\/|  | |  |  |  | |  |     |    <      |   __|  |  |   >   <       |  |     |  |  |  | |      /     |   __|     \   \
# |  |  |  | |  `--'  | |  `----.|  .  \     |  |     |  |  /  .  \      |  |     |  `--'  | |  |\  \----.|  |____.----)   |
# |__|  |__|  \______/   \______||__|\__\    |__|     |__| /__/ \__\     |__|      \______/  | _| `._____||_______|_______/
#

@pytest.mark.skip
@pytest.fixture(autouse=False)
def mock_data_request_receive_data_file(monkeypatch):

    class Response:
        def __init__(self):

            self.status_code = None
            self.conten = None

    def mock_requests_get(address):

        response = Response()
        if '/api_v01/pull_df/UNITTEST.ABCDEF' in address:
            # receive_data_file in Data()
            response.status_code = 200
            response.content = open('/unittest_data/meta.json', 'r').read()
        elif '/api_v01/pull_ds/UNITTEST.ABCDEF/' in address:
            # _download_sliice in DataSlice()
            ds_hash = address.split('.')[-2:]
            response.status_code = 200
            response.content = open('/unittest_data/receive_data_file/ds_hash', 'r').read()
        else:
            response.status_code = 500
            response.content = None

        return response

    monkeypatch.setattr(requests, 'get', mock_requests_get)
    # response = requests.get(self.server + '/api_v01/pull_df/' + df_hash_long)
    #
    #             # Store the data
    #             df_path = data_root / df_hash_long
    #             print(response.status_code)
    #
    #             if response.status_code == 200:
    #
    #                 if not df_path.exists():
    #                     df_path.mkdir()
    #
    #                 response_str = response.content.decode('utf8')
    #
    #                 meta_path = df_path / 'meta.json'
    #
    #                 with open(meta_path, 'w') as fp:
    #                     fp.write(response_str)
    #
    #                 self.df = DataFile(df_hash_long, self.server)

# Tests #############################################################

# @pytest.fixture(autouse=True)
# def change_storage_path(monkeypatch):
#     monkeypatch.setenv('dc_helper.DATA_ROOT', 'SHIT!!')  => ok wenn groß geschrieben...
#     monkeypatch.setitem(dc_helper, 'data_root' , new_data_root) => does not work!!

# @pytest.fixture(autouse=True)
# def disable_network_calls(monkeypatch):
#
#     def stunted_get():
#         raise RuntimeError("Network access not allowed during testing!")
#
#     monkeypatch.setattr(requests, "get", lambda *args, **kwargs: stunted_get())
#
#     # By placing disable_network_calls() in conftest.py and adding the
#     # autouse=True option, you ensure that network calls will be disabled in
#     # every test across the suite. Any test that executes code calling requests.get()
#     # will raise a RuntimeError indicating that an unexpected network call would
#     # have occurred.

#  _______       ___   .___________.    ___          _______  __   __       _______
# |       \     /   \  |           |   /   \        |   ____||  | |  |     |   ____|
# |  .--.  |   /  ^  \ `---|  |----`  /  ^  \       |  |__   |  | |  |     |  |__
# |  |  |  |  /  /_\  \    |  |      /  /_\  \      |   __|  |  | |  |     |   __|
# |  '--'  | /  _____  \   |  |     /  _____  \     |  |     |  | |  `----.|  |____
# |_______/ /__/     \__\  |__|    /__/     \__\    |__|     |__| |_______||_______|

def test_df_call_all_attributes(fix_id_data_dic, monkeypatch):
    # just call every attribute / property in different states of the df just to make sure they don't throw errors
    # the test will fail if any errors happen

    monkeypatch.setattr(config, 'SLICE_MAX_SIZE', 288)

    def call_all_attributets_df(df):
        # attributes
        df._hash_id
        df.project
        df.person
        df.owner
        df.receiver
        df.device
        df._time_c
        df._time_m
        df.date_time_start
        df.date_time_end
        df.date_time_upload
        df.combined_columns
        df.status_closed
        df.duration
        df.dc_version
        df.comments

        # properties
        df.path
        df.hash_id
        df.hash_short
        df.columns
        df.duration
        df.combined_columns_flatten
        df.stats
        df.stats_slices
        df.bin_size
        df.c
        df.slices
        df.samples
        df.file_size
        df.file_compressed_size
        df.compression_ratio

        # methods
        df.get_slice_list()

        if df.columns > 0:
            for data_type in df.cols:
                col = df.cols[data_type]
                # properties
                col.hash_long
                col.slices
                col.samples
                col.bin_size
                col.file_size
                col.compressed_size
                col.file_compressed_size
                col.compression_ratio
                col.dtype_size
                col.chunks_x
                col.chunks_y
                col.x
                col.y
                col.time_rec
                col.x_min
                col.x_hours

                for sl_type in col._slices:
                    for sl in col._slices[sl_type]:
                        sl.hash
                        sl.values
                        sl.hash_long
                        sl.samples
                        sl.bin_size
                        sl.file_size
                        sl.compressed_size
                        sl.file_compressed_size
                        sl.compression_ratio
                        sl.dtype_size
                        sl.file_exists

    # call on unsaved df
    df = new_df(CLIENT_TEST_DB)
    try:
        call_all_attributets_df(df)
    except:
        pytest.fail("Calling the attributes and properties of DataFile empty, unsaved DataFile failed.")
    try:
        print_df_and_all_cols_and_slices(df)
    except:
        pytest.fail("Printing df, and all cols/slices failed on empty unsaved DataFile.")


    df = new_df(CLIENT_TEST_DB)

    # set_values and try all methods again
    df.set_values('heart_rate',  fix_id_data_dic['heart_rate']['x'],  fix_id_data_dic['heart_rate']['y'])
    df.add_combined_columns(['ppg_red', 'ppg_ir'], 'ppg')
    df.store()
    try:
        call_all_attributets_df(df)
    except:
        pytest.fail("Calling the attributes and properties of DataFile failed after set_values().")
    try:
        print_df_and_all_cols_and_slices(df)
    except:
        pytest.fail("Printing df, and all cols/slices failed on filled, unsaved DataFile.")

    df.store()

    for i in range(288):

        if i%20 == 0:
            df.chunk_start()
        df.append_value('heart_rate', randint(0, 2**8-1), i)
        df.append_value('ppg', [randint(0, 2*24-1), randint(0, 2*24-1)], i)
        df.append_value('acc_x', uniform(-1000, 1000), i)

        if i%23 == 0:
            df.chunk_stop()

    df.chunk_stop()

    try:
        call_all_attributets_df(df)
    except:
        pytest.fail("Calling the attributes and properties of DataFile failed after loading big df.")
    try:
        print_df_and_all_cols_and_slices(df)
    except:
        pytest.fail("Printing df, and all cols/slices failed after loading big df.")

    df.store()

    hash = df._hash_id

    df = DataFile.objects(_hash_id=hash).first()

    try:
        call_all_attributets_df(df)
    except:
        pytest.fail("Calling the attributes and properties of DataFile failed after appending values (serveral slices).")
    try:
        print_df_and_all_cols_and_slices(df)
    except:
        pytest.fail("Printing df, and all cols/slices failed on appending many values.")


    # new df with append
    df = new_df(CLIENT_TEST_DB)

    df.append_value('heart_rate', fix_id_data_dic['heart_rate']['x'][0], fix_id_data_dic['heart_rate']['y'][0])
    df.append_value('ppg', [1,2], 1)
    df.store()
    try:
        call_all_attributets_df(df)
    except:
        pytest.fail("Calling the attributes and properties of DataFile failed after append_values().")
    try:
        print_df_and_all_cols_and_slices(df)
    except:
        pytest.fail("Printing df, and all cols/slices failed after appending on empty file.")

    # load by hash
    df = DataFile.objects(_hash_id=hash).first()
    try:
        call_all_attributets_df(df)
    except:
        pytest.fail("Calling the attributes and properties of DataFile failed after loading from DataFile(hash).")
    try:
        print_df_and_all_cols_and_slices(df)
    except:
        pytest.fail("Printing df, and all cols/slices failed after laading from db.")

    disconnect('default')

def test_df_append_in_different_states():

    #just store
    df = new_df(CLIENT_TEST_DB)
    df.store()
    df.append_value('heart_rate', 1, 2)

    assert df.c.heart_rate.x == [2]
    assert df.c.heart_rate.y == [1]
    assert df.columns == 1
    assert df.slices == 2
    assert df.bin_size == 5

    # ignore the above and just add some things
    df = new_df(CLIENT_TEST_DB)

    df.add_combined_columns(['ppg_ir','ppg_red'], 'ppg')
    df.add_column('heart_rate')
    df.add_column('battery')

    df.append_value('ppg', [1,2], 3)
    df.append_value('heart_rate', 4, 5)

    assert df.c.ppg_red.x == [3]
    assert df.c.ppg_ir.y == [1]
    assert df.c.heart_rate.y == [4]
    assert df.c.heart_rate.x == [5]
    assert df.columns == 4
    assert df.slices == 5
    size = int(
        2*dc_helper.helper_dtype_size(config.data_types_dict['ppg_ir']['dtype']) +
        1*dc_helper.helper_dtype_size(config.data_types_dict['ppg_ir']['dtype_time']) +
        1 * dc_helper.helper_dtype_size(config.data_types_dict['heart_rate']['dtype']) +
        1 * dc_helper.helper_dtype_size(config.data_types_dict['heart_rate']['dtype_time'])
    )
    assert df.bin_size == size

    df.store()
    hash = df.hash_id

    # same after loadding the file
    df = DataFile.objects(_hash_id=hash).first()
    assert df.c.ppg_red.x == [3]
    assert df.c.ppg_ir.y == [1]
    assert df.c.heart_rate.y == [4]
    assert df.c.heart_rate.x == [5]
    assert df.columns == 4
    assert df.slices == 5
    assert df.bin_size_meta == size
    disconnect('default')


def test_df_compare_values_before_and_after_load(fix_id_data_dic):
    # Set some values, store the df and load it with DataFile(hash)
    # => compare if x and y values are identical before and after loading
    # Todo: check if input data is the same as output data

    df = new_df(CLIENT_TEST_DB)
    # x originals
    x0 = []
    y0 = []

    # x right from df
    x = []
    y = []
    for col in fix_id_data_dic:
        # put in if some data type is incompatible
        if col not in ['']:
            xval = fix_id_data_dic[col]['x']
            yval = fix_id_data_dic[col]['y']
            df.set_values(col, yval, xval)
            x0.extend(xval)
            y0.extend(yval)
            x.extend(df.cols[col].x)
            y.extend(df.cols[col].y)

    df.store()
    hash = df.hash_id

    df1 = DataFile.objects(_hash_id=hash).first()
    # x after loading df
    x1 = []
    y1 = []
    for col in df1.cols:
        if col not in ['']:
            x1.extend(df1.cols[col].x)
            y1.extend(df1.cols[col].y)

    assert compare_lists(x0, x1)
    assert compare_lists(y0, x1)
    assert compare_lists(x, x1)
    assert compare_lists(y, y1)

    # assert df.is_identical(df1), 'df and df1 are not identical after loading with DataFile(hash).'

# Todo: test with set_val + append + store => .x .y aufrufen
# todo more realistic gateway test


# todo: same with set_values
def test_df_set_values_of_different_length(monkeypatch):

    def get_and_assert_x_and_y(df, x_input, y_input_hr, y_input_ppg, y_input_rr, y_input_acc):

        x_hr = df.c.heart_rate.x
        x_ppg_ir = df.c.ppg_ir.x
        x_ppg_red = df.c.ppg_red.x
        x_ppg_ambient = df.c.ppg_ambient.x
        x_rr = df.c.rr_int.x
        x_acc = df.c.acc_x.x

        y_hr = df.c.heart_rate.y
        y_ppg_ir = df.c.ppg_ir.y
        y_ppg_red = df.c.ppg_red.y
        y_ppg_ambient = df.c.ppg_ambient.y
        y_rr = df.c.rr_int.y
        y_acc = df.c.acc_x.y

        assert compare_lists(y_input_hr, y_hr)
        assert compare_lists(y_input_ppg, y_ppg_ambient)
        assert compare_lists(y_input_ppg, y_ppg_red)
        assert compare_lists(y_input_ppg, y_ppg_ir)
        assert compare_lists(y_input_rr, y_rr)
        assert compare_lists(y_input_acc, y_acc)

        assert compare_lists(x_input, x_ppg_red)
        assert compare_lists(x_ppg_red, x_ppg_ambient)
        assert compare_lists(x_ppg_red, x_ppg_ir)
        assert compare_lists(x_ppg_red, x_rr)
        assert compare_lists(x_ppg_red, x_hr)
        assert compare_lists(x_ppg_red, x_acc)

    # make the slices temporarily much smaller to save some time:
    monkeypatch.setattr(config, 'SLICE_MAX_SIZE', 2880)

    # divide by 8 so that the ppg/acc time slices will also have non-full slices
    samples = int(config.SLICE_MAX_SIZE / 8)

    # append data of different length
    #                time_slices ppg exactly full,    second slice    several exactly full   several full + one more
    for length in [samples - 1, # No full slices
                   samples + 0, # ppg time slices exactly full
                   samples + 1, # ppg time slices exactly full +1 more
                   samples * 8, # y slice for heart rate exactly full, other data_types have more than 1 slice
                   samples * 10 + 1 # all data_type has at least 2 slices
                   ]:

        df = new_df(CLIENT_TEST_DB)
        df.add_combined_columns(['ppg_ir', 'ppg_red'], 'ppg')

        x_input = list(range(length))
        y_input_hr = [randint(0, 2**8-1) for i in range(length)]
        y_input_ppg = [randint(0, 2**24-1) for i in range(length)]
        y_input_rr = [randint(0, 2**16-1) for i in range(length)]
        y_input_acc = [uniform(-1000, 1000) for i in range(length)]

        for i in range(length):
            df.append_value('heart_rate', y_input_hr[i], x_input[i])
            df.append_value('ppg_ambient', y_input_ppg[i], x_input[i])
            df.append_value('ppg', [y_input_ppg[i], y_input_ppg[i]], x_input[i])
            df.append_value('rr_int', y_input_rr[i], x_input[i])
            df.append_value('acc_x', y_input_acc[i], x_input[i])

        # assert after appeding
        get_and_assert_x_and_y(df, x_input, y_input_hr, y_input_ppg, y_input_rr, y_input_acc)

        # assert after storing
        df.store()
        get_and_assert_x_and_y(df, x_input, y_input_hr, y_input_ppg, y_input_rr, y_input_acc)

        # assert after loading
        hash = df.hash_id
        df = DataFile.objects(_hash_id=hash).first()
        get_and_assert_x_and_y(df, x_input, y_input_hr, y_input_ppg, y_input_rr, y_input_acc)

        print(f'Passed {length}.............................................')

    disconnect('default')


# todo: data_chunks
#  - normal
#  - with long slices
# todo: negeative values
#  - append unallowed values

def test_df_4_set_and_append_values_before_and_after_loading():
    # see if number of samples is correct

    # cols with different dtypes
    for col in ['heart_rate', 'rr_int', 'acc_x','ppg_ir']:

        # create new df
        df = new_df(CLIENT_TEST_DB)
        hash = df._hash_id
        x = [100] * 720000
        y = [70] * 720000
        # df.set_values(col, y, x)

        for i in range(720000*2):
            df.append_value(col, 70, 100)
        df.store()

        # print(df.stats)

        # reload file several times

        for i in range(2):
            df = DataFile.objects(_hash_id=hash).first()
            for i in range(1440000):
                df.append_value(col, 70, 100)
            df.store()

        # print(df.stats)

        x = [100] * 123
        y = [70] * 123

        df.set_values(col, y, x)
        df.store()

        df = DataFile.objects(_hash_id=hash).first()

        # print(df.stats)

        for i in range(123):
            df.append_value(col, 70, 100)
        df.store()

        # print(df.stats)

        assert df.samples == (1 + 2) * 1440000 + 123 + 123, f'Number of samples inconsistent. Expected {str((1 + 2) * 1440000 + 123 + 123)} got {df.samples}'

        disconnect('default')

# def test_df_close_and_append():
# 
#     df = new_df(CLIENT_TEST_DB)
# 
#     df.set_values('heart_rate', [1,2,3], [1,2,3])
# 
#     samples1 = df.samples
# 
#     df.close()
# 
#     df.set_values('heart_rate', [1,2,3], [1,2,3])
#     df.append_value('ppg_ir', 1,2)
#     df.store()
# 
#     # adding more values is not possible
#     assert samples1 == df.samples
# 
#     disconnect('default')

def test_df_compress_and_delete_clice(monkeypatch, start_local_web_server):
    # the program should not die from a missing file (but logs should be printed)

    monkeypatch.setattr(config, 'SLICE_MAX_SIZE', 2880)

    df = new_df(CLIENT_TEST_DB)
    df.server = SERVER_ADDRESS

    for i in range(2881):
        df.append_value('heart_rate', 1,1)
    df.store()

    slice_1 = df.c.heart_rate._slices_y[-2]._path.name
    df.c.heart_rate._slices_y[-2]._path.unlink()
    assert df.c.heart_rate._slices_y[-2].file_exists == False

    slice_2 = df.c.heart_rate._slices_time_rec[-2]._path.name
    df.c.heart_rate._slices_time_rec[-2]._path.unlink()
    assert df.c.heart_rate._slices_time_rec[-2].file_exists == False

    df.compress()

    slice_3 = df.c.heart_rate._slices_time_rec[-3]._path.name
    df.c.heart_rate._slices_time_rec[-3]._path.unlink()
    assert df.c.heart_rate._slices_time_rec[-3].file_exists == False
    db_sync = DB_Sync(server=SERVER_ADDRESS)
    db_sync.push_ds()

    df.close(send=True)
    assert df.check_all_slices_sent(ignore_missing_slices=False) == False
    assert df.check_all_slices_sent(ignore_missing_slices=True) == True

    missing_slices_list = df.find_missing_slices()
    assert len(missing_slices_list) == 3
    assert sum([x in missing_slices_list for x in [slice_1, slice_2, slice_3]]) == 3

    disconnect('default')

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

# def test_df_append_slice_overshoot_initial_df():
#     # the slices should be
# 
#     df = new_df(CLIENT_TEST_DB)
# 
#     # append values so that several slices must be generated
#     number_of_new_values = int(SLICE_MAX_SIZE+1)
#     for i in range(number_of_new_values):
#         df.append_value('ppg_ir' ,1,1)
# 
#     expected_number_of_samples = number_of_new_values
#     expected_samples_per_y_slice = SLICE_MAX_SIZE / dc_helper.helper_dtype_size(data_types_dict['ppg_ir']['dtype']) # ppg_ir = uint24 => 3 bytes per value
#     expected_samples_per_time_slice = SLICE_MAX_SIZE / 4 # float32 => 4 bytes per value
#     expected_number_of_slices = math.ceil(expected_number_of_samples/expected_samples_per_y_slice) + math.ceil(expected_number_of_samples/expected_samples_per_time_slice) # x2 because the time_rec
#     # slices have to be counted, too => also 4 bytes per value
# 
#     assert df.slices == expected_number_of_slices, f'Number of slices is not correct after append_value (initial df). Expected {expected_number_of_slices} got: {df.slices}'
#     disconnect('default')
    
# def test_df_append_slice_overshoot_after_loading_df():
#     # in the past the sl.bin_size depended on the _meta information (only changes after a df.store())
#     # => the df.append_value() would always compare the slice bin_size against a fixed _meta['bin_size']
#     # => it was possible to append too many values in one slice
# 
#     df = new_df(CLIENT_TEST_DB)
#     hash = df.hash_long
# 
#     df.append_value('ppg_ir' ,1,1)   # 1 sample added
#     df.store()
# 
#     del df
# 
#     df = DataFile.objects(_hash_id=hash).first()
# 
#     # append values so that several slices must be generated
#     number_of_new_values = int(SLICE_MAX_SIZE*2+1)
#     for i in range(number_of_new_values):
#         df.append_value('ppg_ir' ,1,1)
# 
#     df.store()
# 
#     expected_number_of_samples = number_of_new_values + 1  # +1 one for the first append
#     expected_samples_per_y_slice = SLICE_MAX_SIZE / dc_helper.helper_dtype_size(data_types_dict['ppg_ir']['dtype'])  # ppg_ir = uint24 => 3 bytes per value
#     expected_samples_per_time_slice = SLICE_MAX_SIZE / 4  # float32 => 4 bytes per value
#     expected_number_of_slices = math.ceil(expected_number_of_samples / expected_samples_per_y_slice) + math.ceil(expected_number_of_samples / expected_samples_per_time_slice)  # x2 because the time_rec
# 
#     assert df.slices == expected_number_of_slices, f'Number of slices is not correct after append_value (after loading df). Expected {expected_number_of_slices} got: {df.slices}'

# @pytest.mark.skip   # todo: fix assertion (see test above)
# def test_df_combined_columns_append_slice_overshoot_after_loading_df():
#     # same as above but for combined columns
# 
#     df = DataFile()
#     hash = df.hash_long
#     df.add_combined_columns(['ppg_ir', 'ppg_red', 'ppg_green'], 'ppg')
#     df.append_value('ppg', 1, 1)  # 4x1 sample added
#     df.store()
# 
#     del df
# 
#     df = DataFile(hash)
# 
#     # append values so that several slices must be generated
#     number_of_new_values = int(SLICE_MAX_SIZE * 1 + 1)
#     for i in range(number_of_new_values):
#         df.append_value('ppg', (1,2,3), 1)
# 
#     df.store()
# 
#     expected_number_of_samples = number_of_new_values + 1  # +1 one for the first append
#     expected_samples_per_slice = SLICE_MAX_SIZE / dc_helper.helper_dtype_size(data_types_dict['ppg_ir']['dtype'])
#     expected_number_of_slices_first_slice = math.ceil(expected_number_of_samples / expected_samples_per_slice) * 2  # x2 because the time_rec slices have to be counted, too => also 4 bytes per value
#     expected_number_of_slices = math.ceil(expected_number_of_samples / expected_samples_per_slice)  # other combined slices dont have time slices
# 
#     assert df.c.ppg_ir.slices == expected_number_of_slices_first_slice,f'Number of slices for FIRST slice is not correct after append_value (after loading df with combined slices). ' \
#         f'Expected {expected_number_of_slices_first_slice} got {df.c.ppg_ir.slices}'
# 
#     assert df.c.ppg_red.slices == df.c.ppg_green.slices == expected_number_of_slices, \
#         f'Number of slices for combined slice is not correct after append_value (after loading df with combined slices). ' \
#         f'Expected {expected_number_of_slices_first_slice} got ppg_red: {df.c.ppg_red.slices} ppg_green: {df.c.ppg_green.slices}'

# def test_df_create_and_load_combined_slices():
#     # create some shared slices, append and store. Then reload and append even more
# 
#     df = new_df(CLIENT_TEST_DB)
# 
#     hash = df.hash_id
# 
#     # create some shared columns (some should not be possible but also not disturbe the programm)
#     df.add_combined_columns(['ppg_ir', 'ppg_red', 'ppg_ambient'], 'ppg')
#     df.add_combined_columns(['acc_x'], 'accx')
#     df.add_combined_columns(['acc_y', 'acc_z'], 'acc')
#     df.add_combined_columns(['respiration_rate', 'battery'], 'other')  # 2 empty cols / 0 slices
# 
#     df.add_combined_columns(['ppg_ir', 'ppg_ambient'], 'ppg_2')  # << should not work (already used)
#     df.add_combined_columns(['banane', 'quark'], 'ppg_2')  # << should not work
#     df.add_combined_columns(['respiration_rate', 'battery'], 'ppg')  # << should not work (same identifier)
#     df.add_combined_columns(['acc_x'], 'ppg')  # << should not work (same identifier)
# 
#     # append some random values
#     number_of_first_values = 100
#     for i in range(number_of_first_values):
#         df.append_value('ppg', (200, 250, 300), i)  # 3 cols / 4 slices
#         df.append_value('ppg_green', 70, i)  # 1 col / 2 slices
#         df.append_value('acc', (100, 150), i)  # 2 cols / 3 slices
#         df.append_value('accx', [120], i)  # 1 col / 2 slices
# 
#         df.append_value('ppg_ir', (100 + i, 200 + i, 300 + i), i)  # < should not work (ppg_ir key in other combined val)
#         df.append_value('ppg', (200, 250, 300, 213, 12, 123, 213,), i)  # < should not work (too many values)
#         df.append_value('ppg', 100 + i, i + randint(1, 2))  # should not work (only one value for combined slice)
# 
#     expected_cols = 3 + 1 + 2 + 1 + 2  # last 2 cols are empty repsiration_rate and battery
#     expected_slices = 4 + 2 + 3 + 2 + 0
#     assert df.columns == expected_cols, f'Expectd {expected_cols} columns, got {df.columns}'
#     assert df.slices == expected_slices, f'Expectd {expected_slices} slices, got {df.slices}'
# 
#     df.store()
#     df.compress()
# 
#     assert list(df.c.ppg_red.x) == list(df.c.ppg_ir.x) == list(df.c.ppg_ambient.x), 'x-values of shared slices from group "ppg" not identical.'
#     x_before = df.c.ppg_red.x
# 
#     print(df.stats)
#     print(df.stats_slices)
# 
#     stats_before = df.stats
#     stats_slices_before = df.stats_slices
#     stats_slices_json_before = df.stats_slices_json
# 
#     combined_columns_before = df.combined_columns
#     combined_columns_flatten_before = df.combined_columns_flatten
# 
#     del df
# 
#     # reload file
#     df = DataFile.objects(_hash_id=hash).first()
#     logger.error('+++++++++++++++')
#     logger.error(stats_before)
#     logger.error(df.stats)
#     logger.error('+++++++++++++++')
#     assert stats_before == df.stats, 'df.stats after loading file not identical'
#     assert stats_slices_before == df.stats_slices, 'df.stats_slices after loading file not identical'
#     assert stats_slices_json_before == df.stats_slices_json, 'df.stats_slices_json after loading file not identical'
# 
#     assert list(df.c.ppg_red.x) == list(df.c.ppg_ir.x) == list(df.c.ppg_ambient.x) == list(x_before), 'x-values of shared slices from group "ppg" not identical after loading'
# 
#     assert df.combined_columns == combined_columns_before, f'combined_columns after loading inconsistent. Expected {combined_columns_before} got {df.combined_columns}'
#     assert df.combined_columns_flatten == combined_columns_flatten_before, f'combined_columns_flatten after loading inconsistent. Expected {combined_columns_flatten_before} got {df.combined_columns_flatten}'
# 
#     # add more data so that several slices get appended
#     # dtype of ppg/acc > 1 byte => multiple slices
#     number_of_new_samples = SLICE_MAX_SIZE + 1
#     for i in range(number_of_first_values, number_of_new_samples + number_of_first_values):
#         df.append_value('ppg', (200, 250, 300), i)
#         # df.append_value('accx', [200], i)
#         df.append_value('ppg_green', 70, i)
# 
#     df.store()
#     df.close(send=False)
# 
#     expected_ppg_samples = number_of_first_values + number_of_new_samples
# 
#     assert df.c.ppg_red.samples == expected_ppg_samples
#     assert df.c.ppg_ir.samples == expected_ppg_samples
#     assert df.c.ppg_ambient.samples == expected_ppg_samples
#     assert df.c.ppg_green.samples == expected_ppg_samples

def test_df_7_close_and_send(start_local_web_server_plus_df):

    df = start_local_web_server_plus_df['df']

    for i in range(100):
        df = append_random_json_data(df, x=i)
        df = append_n_times_random_rawdata(df, x=i, sampling_rate=50)

        if i%10 == 0 :
            df.store()
            df.send_json()
        time.sleep(0.05)

    df.store()
    df.close(send=True)

    assert compare_all_sent_slices_server_vs_working(df)
    stop_webapp()

def test_df_8_send_partially():

    pass

@pytest.mark.df_send
def test_df_send_partially_with_opening_and_closing_df(start_local_web_server):
    # send data partially, close the file, reopen it, several slices => similar to what labclient does
    # in the end check, if the received data is identical

    number_of_df_loads = 10
    for k in range(number_of_df_loads):
        if k == 0:
            df= DataFile()
            hash = df.hash_long
            counter = 0
            df._meta['person_hash'] = 'unittest'
            df.add_combined_columns(['ppg_ir', 'ppg_red'], 'ppg')
            df.store()
        else:
            df = DataFile(hash)

        # random_break = randint(90, 100)
        # print('random break at: ', random_break)
        df.server = SERVER_ADDRESS

        print(hash)

        if df.c.ppg_ir.slices > 4:
            # enough slices
            break

        cycles = 50
        for i in range(cycles):
            # for data_type in dc_helper.data_types_dict:
            for data_type in ['heart_rate', 'acc_x', 'ppg']:
                if data_type is not 'ppg':
                        df.append_value(data_type, i ,i)
                else:
                    # append a lot of rawdata to get several slices
                    for k in range(10000):
                        df.append_value(data_type, (i, i) ,i)

            if i == cycles-1:
                df.store()
                df.send_json()
                df.send(partially=True, rawdata=True)
                del df

                counter += 1
                break

    # give the server some time to store the files
    time.sleep(1)
    assert compare_all_sent_slices_server_vs_working(df)

    # final part
    df = DataFile(hash)
    df.server = SERVER_ADDRESS
    df.close(send=True)

    assert compare_all_sent_slices_server_vs_working(df)

    # end the webserver
    stop_webapp()

# @pytest.mark.df_send
# def test_df_send_json(start_local_web_server_plus_df):
# 
#     df = start_local_web_server_plus_df['df']
# 
#     for i in range(10):
#         df = append_random_json_data(df, i)
#         df = append_n_times_random_rawdata(df, i, 50)
#         df.send_json()
#         time.sleep(1)
# 
#     stop_webapp()

# @pytest.mark.skip
# def test_df_send_without_server_then_activate_the_server_and_send():
# 
#     pass


def test_df_send_empty_columns(start_local_web_server):

    df = DataFile()
    df.server = SERVER_ADDRESS
    df._meta['person_hash'] = 'unittest'

    df.add_combined_columns(['ppg_ir', 'ppg_gree', 'ppg_red', 'ppg_ambient'], 'ppg')
    df.add_combined_columns(['acc_x', 'acc_y', 'acc_z'], 'acc')
    for col in data_types_dict:
        df.add_column(col)
    df.store()

    df.send_json()
    df.send(partially=True)
    df.close(send=True)

def test_df_send_not_fully_kill_server_and_send_the_rest(start_local_web_server_plus_df):

    df = start_local_web_server_plus_df['df']
    server = df.server
    hash = df.hash_long

    df.append_value('heart_rate', 1,1)
    df.append_value('ppg_green', 1,1)

    df.store()
    df.send(partially=True)

    del df
    df = DataFile(hash)
    df.server = server

    df.append_value('heart_rate', 1, 1)
    df.append_value('ppg_green', 1, 1)

    df.close(send=False)

    # todo: simulate that only some columns got sent

    # no send possible...
    stop_webapp()
    df.send()

    del df
    df = DataFile(hash)
    df.server = server

    new_webserver = start_webserver_process()

    time.sleep(2)

    # send again...
    df.send()

    assert compare_all_sent_slices_server_vs_working(df)

    stop_webapp()

#  _______       ___   .___________.    ___           ______   ______    __       __    __  .___  ___. .__   __.
# |       \     /   \  |           |   /   \         /      | /  __  \  |  |     |  |  |  | |   \/   | |  \ |  |
# |  .--.  |   /  ^  \ `---|  |----`  /  ^  \       |  ,----'|  |  |  | |  |     |  |  |  | |  \  /  | |   \|  |
# |  |  |  |  /  /_\  \    |  |      /  /_\  \      |  |     |  |  |  | |  |     |  |  |  | |  |\/|  | |  . `  |
# |  '--'  | /  _____  \   |  |     /  _____  \     |  `----.|  `--'  | |  `----.|  `--'  | |  |  |  | |  |\   |
# |_______/ /__/     \__\  |__|    /__/     \__\     \______| \______/  |_______| \______/  |__|  |__| |__| \__|
#

#  _______       ___   .___________.    ___              _______. __       __    ______  _______
# |       \     /   \  |           |   /   \            /       ||  |     |  |  /      ||   ____|
# |  .--.  |   /  ^  \ `---|  |----`  /  ^  \          |   (----`|  |     |  | |  ,----'|  |__
# |  |  |  |  /  /_\  \    |  |      /  /_\  \          \   \    |  |     |  | |  |     |   __|
# |  '--'  | /  _____  \   |  |     /  _____  \     .----)   |   |  `----.|  | |  `----.|  |____
# |_______/ /__/     \__\  |__|    /__/     \__\    |_______/    |_______||__|  \______||_______|
#



#  _______   ______        __    __   _______  __      .______    _______ .______
# |       \ /      |      |  |  |  | |   ____||  |     |   _  \  |   ____||   _  \
# |  .--.  |  ,----'      |  |__|  | |  |__   |  |     |  |_)  | |  |__   |  |_)  |
# |  |  |  |  |           |   __   | |   __|  |  |     |   ___/  |   __|  |      /
# |  '--'  |  `----.      |  |  |  | |  |____ |  `----.|  |      |  |____ |  |\  \----.
# |_______/ \______| _____|__|  |__| |_______||_______|| _|      |_______|| _| `._____|
#                   |______|

# ÜBERNOMMEN IN ANDERE TESTS ... 


# def test_dc_helper_int_list_to_uint24_lsb_first_normal_range():
# 
#     numbers = int(2**24)
# 
#     int_value_list = []
#     # pass a list with all uint24 numbers
#     for number in range(numbers):
#         int_value_list.append(int(number))
# 
#     bin_data = dc_helper.int_list_to_uint24_lsb_first(int_value_list)
#     assert type(bin_data) is bytearray
# 
#     back_converted_list = dc_helper.uint24_lsb_first_to_int_list(bin_data)
#     assert int_value_list == back_converted_list
# 
#     # do the same with random numers in the list
#     int_value_list = []
#     # pass a list with all uint24 numbers
#     for number in range(numbers):
#         int_value_list.append(randint(0, number))
# 
#     bin_data = dc_helper.int_list_to_uint24_lsb_first(int_value_list)
#     assert type(bin_data) is bytearray
# 
#     back_converted_list = dc_helper.uint24_lsb_first_to_int_list(bin_data)
#     assert int_value_list == back_converted_list
# 
# def test_dc_helper_int_list_to_uint24_lsb_first_negative_numbers():
#     # pass negative integer
#     with pytest.raises(ValueError):
#         _ = dc_helper.int_list_to_uint24_lsb_first([1000, 2400000, -1, 10000])
# 
# def test_dc_helper_int_list_to_uint24_lsb_first_too_large_numbers():
#     # pass a list with too high numbers
#     with pytest.raises(ValueError):
#         _ = dc_helper.int_list_to_uint24_lsb_first([2 ** 24])
# 
# @pytest.mark.xfail
# def test_dc_helper_int_list_to_uint24_lsb_first_float():
#     # pass a float
#     _ = dc_helper.int_list_to_uint24_lsb_first([1000.0])
# 
# @pytest.mark.xfail
# def test_dc_helper_int_list_to_uint24_lsb_first_no_list():
#     # pass no list
#     _ = dc_helper.int_list_to_uint24_lsb_first(10000)










#  __       __  .__   __.  __  ___      _______.
# |  |     |  | |  \ |  | |  |/  /     /       |
# |  |     |  | |   \|  | |  '  /     |   (----`
# |  |     |  | |  . `  | |    <       \   \
# |  `----.|  | |  |\   | |  .  \  .----)   |
# |_______||__| |__| \__| |__|\__\ |_______/

# https://docs.pytest.org/en/latest/monkeypatch.html
# https://realpython.com/pytest-python-testing/
# => pytest-randomly
# => https://coverage.readthedocs.io/en/coverage-5.1/
# pytest Plugins: http://plugincompat.herokuapp.com/

# http://www.network-science.de/ascii/ => starwars font
