import pytest
from pathlib import Path
import json
from mongoengine import connect, disconnect
from data_container.db_sync import DB_Sync
from data_container.tests.conftest import TEST_DB_CLIENT

def test_delete_data_base(fixture_clear_all_dbs, monkeypatch, mocker):

    from data_container import DataFile
    from data_container.odm import Scope, Person, Receiver, Project, Device
    from data_container.dc_config import DcConfig

    disconnect('default')
    connect(TEST_DB_CLIENT)

    # change the df path to be a mock
    path_mock = mocker.Mock()
    monkeypatch.setattr(DcConfig, 'df_path', path_mock)

    # instead of deleting real folders, see if it was called with the correct argument
    shutil_mock = mocker.Mock()
    mocker.patch('shutil.rmtree', shutil_mock)

    # do this to test private functions
    db_sync = DB_Sync()
    db_sync._DB_Sync__delete_scope_db()

    # assert

    assert len(Scope.objects) == 0
    assert len(Receiver.objects) == 0
    assert len(Project.objects) == 0
    assert len(Person.objects) == 0
    assert len(Device.objects) == 0
    assert len(DataFile.objects) == 0

    # assert that rmtree was called with the path and that the path.exists() was called
    shutil_mock.assert_called_with(path_mock)
    assert path_mock.exists.called

@pytest.mark.parametrize('store', [True, False])
def test_if_wrong_json_key_gets_properly_removed_during_json_import(caplog, store):

    from data_container.odm import Scope, ReceiverConfig
    connect(TEST_DB_CLIENT)
    Scope.objects().delete()

    receiver_config = ReceiverConfig()
    receiver_config.sync_db_interval = 600

    scope = Scope()
    scope.name = 'test_scope'
    scope.timezone = 'Europe/Berlin'
    scope.email = 's@test.de'
    scope.receiver_config = receiver_config
    scope.store()

    # add two unknown fields to the json object
    json_obj_untouched = scope.to_json()
    json_dict = json.loads(json_obj_untouched)
    json_dict['unknwown_field1'] = 'test'
    json_dict['unknwown_field2'] = 'test'
    json_dict['rc']["unknwown_field3"] = 'test_Rc'
    json_dict['rc']["unknwown_field4"] = 'test_Rc'
    json_obj = json.dumps(json_dict)

    # clear the database so that it is obvious if scope got stored or not
    Scope.objects().delete()

    scope_new = DB_Sync()._import_json_to_db(json_obj=json_obj, odm_class=Scope, store=store)

    if store:
        assert Scope.objects.count() == 1
        scope_new_loaded = Scope.objects.first()
        assert not hasattr(scope_new_loaded, 'unknwown_field1')
        assert not hasattr(scope_new_loaded, 'unknwown_field2')
        assert not hasattr(scope_new_loaded.receiver_config, 'unknwown_field3')
        assert not hasattr(scope_new_loaded.receiver_config, 'unknwown_field4')
        assert scope_new_loaded.name == scope.name
        assert scope_new_loaded.timezone == scope.timezone
        assert scope_new_loaded.email == scope.email
        assert scope_new_loaded.receiver_config.sync_db_interval == scope.receiver_config.sync_db_interval

    else:
        assert Scope.objects.count() == 0

    assert not hasattr(scope_new, 'unknwown_field1')
    assert not hasattr(scope_new, 'unknwown_field2')
    assert not hasattr(scope_new.receiver_config, 'unknwown_field3')
    assert not hasattr(scope_new.receiver_config, 'unknwown_field4')
    assert scope_new.name == scope.name
    assert scope_new.timezone == scope.timezone
    assert scope_new.email == scope.email
    assert scope_new.receiver_config.sync_db_interval == scope.receiver_config.sync_db_interval


    # assert hasattr(scope_new)


    # assert json_obj_untouched == scope_new.to_json()
