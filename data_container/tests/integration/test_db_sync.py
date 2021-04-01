import pytest
import requests
import json
from mongoengine import connect, disconnect
from data_container.tests.conftest import TEST_DB_SERVER, TEST_DB_CLIENT
from data_container.odm import Scope, Person, Receiver, Device
from data_container.db_sync import DB_Sync

# todo: does not work anymore. Has probably to do with how the
@pytest.mark.parametrize('full_update, db_change, allow_db_deletion, new_scope', [
    [1, False, True, 'same'],
    [1, True, True, None],
    [1, True, True, 'new'],
    [1, False, False, 'same'],  # same but the db must not be deleted
    [1, True, False, None],
    [1, True, False, 'new'],
])
def test_request_server_db_when_scope_changes(fixture_clear_all_dbs_single_test, monkeypatch, mocker,
                                              full_update, db_change, allow_db_deletion, new_scope):
    # fixture_empty_df only for preparing the database
    db_sync = DB_Sync(server='fake_server', receiver_serial='TESTREC')

    # server response mock
    class Content:

        def __init__(self):
            # change something at the server db
            disconnect('default')
            connect(TEST_DB_SERVER)
            receiver = Receiver.objects(_hash_id='TESTREC').first()
            if new_scope == 'same':
                pass
            elif new_scope == None:
                receiver.scope = None
            elif new_scope == 'new':
                scope = Scope()
                scope.email = 'newscope@web.de'
                scope.name = 'new_scope'
                scope.timezone = 'Europe/Berlin'
                scope.store()
                receiver.scope = scope
            receiver.store()

        def decode(self):
            # provide information from the server data_base
            json_data = json.dumps(db_sync.provide_db('TESTREC', full_update=1))
            disconnect('default')
            connect(TEST_DB_CLIENT)
            return json_data

    class GetMock:
        status_code = 200

        def __init__(self):
            self.content = Content()

    # mock request.get() and all that it should return from the server
    monkeypatch.setattr(requests, 'get', lambda *args, **kwargs: GetMock())

    # don't actually delete things - this method gets tested separately
    # hint: it is not possible to mock DB_sync private methods => workaround like this
    delete_scope_db_mock = mocker.Mock()
    monkeypatch.setattr(db_sync, '_DB_Sync__delete_scope_db', delete_scope_db_mock)
    db_sync.request_server_db(full_update=full_update, allow_db_deletion=allow_db_deletion)
    # assert
    if db_change and allow_db_deletion:
        delete_scope_db_mock.assert_called_once()
    else:
        delete_scope_db_mock.called is False