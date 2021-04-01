import pytest
import os
import shutil
from pathlib import Path
import sys
import pymongo
from mongoengine import connect, disconnect

# add repo path to being able to import from data_container package
repo_path = Path(__file__).parents[2]
sys.path.append(str(repo_path))

from data_container import DataFile
import data_container as dc
from data_container.dc_config import DcConfig

config = dc.config
config.init()

from data_container.odm import Scope, Config, Person, Project, Receiver, Device, Comment


TEST_DB_CLIENT = 'test_db_client'
TEST_DB_SERVER = 'test_db_server'

PYMONGO_CLIENT = pymongo.MongoClient()

#  _______  __  ___   ___ .___________. __    __  .______       _______     _______.
# |   ____||  | \  \ /  / |           ||  |  |  | |   _  \     |   ____|   /       |
# |  |__   |  |  \  V  /  `---|  |----`|  |  |  | |  |_)  |    |  |__     |   (----`
# |   __|  |  |   >   <       |  |     |  |  |  | |      /     |   __|     \   \
# |  |     |  |  /  .  \      |  |     |  `--'  | |  |\  \----.|  |____.----)   |
# |__|     |__| /__/ \__\     |__|      \______/  | _| `._____||_______|_______/


# @pytest.fixture()
@pytest.fixture(autouse=True, scope='session')
def fixture_clear_all_dbs():
    reset_db([TEST_DB_CLIENT, TEST_DB_SERVER])
    disconnect('default')

# call this explicitely in a test if it is necessary to clear the db
# (takes quite long so don't use too often)
@pytest.fixture()
def fixture_clear_all_dbs_single_test():
    reset_db([TEST_DB_CLIENT, TEST_DB_SERVER])
    disconnect('default')

@pytest.fixture
def fixture_empty_df():

    connect(TEST_DB_CLIENT)
    # if there is a df in the db then it's probably not necessary to clear the whole db (create new person... which takes a lot of time)
    some_df = DataFile.objects.only('_hash_id').first()
    if not some_df:
        reset_db(TEST_DB_CLIENT)

    df = new_df()
    # todo: write tests for True and false
    df.live_data = True

    return df


@pytest.fixture()
def fixture_reduce_slice_size_4(monkeypatch):
    # dramatically shortens the slice size to 4 bytes (instead of 2880000)

    monkeypatch.setattr(DcConfig, 'SLICE_MAX_SIZE', 4)


@pytest.fixture()
def fixture_reduce_slice_size_8(monkeypatch):
    # dramatically shortens the slice size to 8 bytes (instead of 2880000)

    monkeypatch.setattr(DcConfig, 'SLICE_MAX_SIZE', 8)


@pytest.fixture()
def fixture_reduce_slice_size_24(monkeypatch):
    # dramatically shortens the slice size to 24 bytes (instead of 2880000)
    
    monkeypatch.setattr(DcConfig, 'SLICE_MAX_SIZE', 24)


@pytest.fixture()
def fixture_reduce_slice_size_288(monkeypatch):
    # dramatically shortens the slice size to 288 bytes (instead of 2880000)

    monkeypatch.setattr(DcConfig, 'SLICE_MAX_SIZE', 288)

@pytest.fixture
def fixture_temp_path():
    temp_path = Path(__file__).parent / 'temp'
    # monkeypatch.setattr(config, 'df_path', temp_path)

    if not temp_path.exists():
        temp_path.mkdir()

    for root, dirs, files in os.walk(temp_path):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))

    return temp_path
# .___________. _______     ___      .______       _______   ______   ____    __    ____ .__   __.
# |           ||   ____|   /   \     |   _  \     |       \ /  __  \  \   \  /  \  /   / |  \ |  |
# `---|  |----`|  |__     /  ^  \    |  |_)  |    |  .--.  |  |  |  |  \   \/    \/   /  |   \|  |
#     |  |     |   __|   /  /_\  \   |      /     |  |  |  |  |  |  |   \            /   |  . `  |
#     |  |     |  |____ /  _____  \  |  |\  \----.|  '--'  |  `--'  |    \    /\    /    |  |\   |
#     |__|     |_______/__/     \__\ | _| `._____||_______/ \______/      \__/  \__/     |__| \__|



#  __    __   _______  __      .______    _______ .______
# |  |  |  | |   ____||  |     |   _  \  |   ____||   _  \
# |  |__|  | |  |__   |  |     |  |_)  | |  |__   |  |_)  |
# |   __   | |   __|  |  |     |   ___/  |   __|  |      /
# |  |  |  | |  |____ |  `----.|  |      |  |____ |  |\  \----.
# |__|  |__| |_______||_______|| _|      |_______|| _| `._____|

def new_df():

    person = Person.objects.first()
    project = Project.objects.first()

    df = DataFile()
    df.live_data = True
    df.person = person
    df.project = project

    return df

def reset_db(dbs):
    # delete db
    # create necessary db entries to work with a data file
    # Do this for all dbs
    import time
    t1 = time.time()

    if not type(dbs) in [list, tuple]:
        dbs = [dbs]

    for i, db in enumerate(dbs):
        disconnect('default')
        connect(db)

        # reset the database
        PYMONGO_CLIENT.drop_database(db)

        t2 = time.time()
        if i == 0:
            config = Config()
            config.config_id = 'default'
            config.store()

            scope = Scope()
            scope.name = 'test_scope'
            scope.timezone = 'Europe/Berlin'
            scope.email = 's@test.de'
            scope.store()

            proj = Project()
            proj.scope = scope
            proj.name = 'test_project'
            proj.store()

            dev = Device()
            dev._hash_id = 'YQB6BK'
            dev.scope = scope
            dev.project = proj
            dev.mac_address = '33:22:11:00'
            dev.cap_size = 'M'
            dev.udi_type = 'udi_type'
            dev.udi_lot = 'udi_lot'
            dev.store()

            person = Person()
            person.scope = scope
            person.project = proj
            person.devices = [dev]
            person.store()

            rec = Receiver()
            rec._hash_id = 'TESTREC'
            rec.scope = scope
            rec.project = proj
            rec.persons = [person]
            rec.mac_address = '11:22:33:44'
            rec.udi_type = 'udi_type'
            rec.udi_lot = 'udi_lot'
            rec.device_model = 'gateway_test'
            rec.store()

            # avoid unnecessary json_conversions
            if len(dbs) > 1:

                config_json = config.to_json()
                scope_json = scope.to_json()
                proj_json = proj.to_json()
                dev_json = dev.to_json()
                person_json = person.to_json()
                rec_json = rec.to_json()

            t3 = time.time()

        else:

            scope = Scope.from_json(scope_json, created=True)
            scope.store()

            config = config.from_json(config_json, created=True)
            config.save()

            proj = Project.from_json(proj_json, created=True)
            proj.store()

            dev = Device.from_json(dev_json, created=True)
            dev.store()

            person = Person.from_json(person_json, created=True)
            person.store()

            rec = Receiver.from_json(rec_json, created=True)
            rec.store()

    print(f'delete db took {t2-t1}, refill db took {t3-t2}')

# http://www.network-science.de/ascii/ => starwars font
