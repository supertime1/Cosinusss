import requests
import json
import time
from pathlib import Path
import shutil
import re
from datetime import datetime, timedelta, timezone
from bson.objectid import ObjectId
from mongoengine.errors import FieldDoesNotExist
from . import DataFile
from .odm import User, Project, Person, Receiver, Device, EventLog, Comment, Scope, Config
from .dc_helper import DcHelper
from . import config

class DB_Sync():

    def __init__(self, server=None, receiver_serial=None):

        self.server = server
        self.receiver_serial = receiver_serial
        self.logger = config.logger
        # todo:
        #  - more efficient (only things that changed, single data chunks ... )

        # todo Future feature:
        #  - authentication
        #  - slices to be sent here

    def push_ds(self, df=None, df_hash_id=None, partially=False, rawdata=False, data_type_list=None):
        # this will push the slices to the server.
        # Make sure to store() the data file before sending slices!

        if self.server and (df or df_hash_id):
            try:
                if df:
                    hash_id = df.hash_id
                else:
                    df = DataFile.objects(_hash_id=df_hash_id).first()

                if df:
                    df.server = self.server
                    df.send(partially=partially, rawdata=rawdata, data_type_list=data_type_list)
                else:
                    self.logger.warning(f'push_ds: no df found for hash_id {hash_id}')

            except requests.exceptions.RequestException as e:
                return False
                self.logger.warning(f'push_ds {hash_id} failed with RequestException. server down?')
        else:
            self.logger.warning(f'push_ds: not all requirements fulfilled server: {self.server}, df: {df} or df_hash_id {df_hash_id}')
            return False

    def push_df(self, df=None, hash_id=None):
        # this will push (send) the datafile to the server (the binary slices will not be sent => push_ds)
        # when passing the df instance it is not necessary to store() before but with hash_id storing before is necessary (otherwise old data will be sent)

        if self.server and (df or hash_id):

            try:
                if df:
                    hash_id = df.hash_id
                else:
                    df = DataFile.objects(_hash_id=hash_id).first()

                if df:
                    df.server = self.server
                    df.send_meta()
                else:
                    self.logger.warning(f'push_df: no df found for hash_id {hash_id}')

            except requests.exceptions.RequestException as e:
                return False
                self.logger.warning('push_df failed with RequestException. server down?')

        else:
            self.logger.warning(f'push_df: not all requirements fulfilled server: {self.server}, df: {df} or hash_id {hash_id}')
            return False

    def request_server_db(self, full_update=False, allow_db_deletion=False):
        # The labgateway can use this method to receive the necessary
        # database elements from the server
        # allow_db_deletion: if a new scope is received, all database elements and dfs will be deleted

        if full_update:
            full_update = 1
        else:
            full_update = 0

        if self.server and self.receiver_serial:

            config_url = self.server + '/api_v01/update_config/' + self.receiver_serial + '/' + str(full_update)
            # todo: last update time?
            try:

                response = requests.get(config_url, timeout=config.request_timeout)

                self.logger.debug('{}: Response status: {}'.format(config_url, response.status_code))

                if response.status_code == 200:
                    content = json.loads(response.content.decode())

                    if content['request_successful']:

                        updated_str = ''

                        if content['scope']:
                            scope = self._import_json_to_db(content['scope'], Scope, store=False)
                            local_scopes = Scope.objects
                            # a scope change causes a reset of the db and df folder
                            for local_scope in local_scopes:
                                if local_scope.id != scope.id and allow_db_deletion:
                                    self.__delete_scope_db()
                                    break

                            scope.store()
                            updated_str += 'Scope, '
                        elif allow_db_deletion:
                            self.__delete_scope_db()
                            self.logger.info('deleted database because new Scope is none')
                            return True

                        if content['receiver']:
                            self._import_json_to_db(content['receiver'], Receiver)
                            updated_str += 'Receiver, '

                        if content['config']:
                            self._import_json_to_db(content['config'], Config)
                            updated_str += 'Config, '

                        if content['users']:
                            users = json.loads(content['users'])
                            for user in users:
                                self._import_json_to_db(json.dumps(user), User)
                            updated_str += f'User ({len(users)}), '

                        if content['projects']:
                            projects = json.loads(content['projects'])
                            for project in projects:
                                self._import_json_to_db(json.dumps(project), Project)
                            updated_str += f'Projects ({len(projects)}), '

                        if content['persons']:
                            persons = json.loads(content['persons'])
                            for person in persons:
                                self._import_json_to_db(json.dumps(person), Person)
                            updated_str += f'Persons ({len(persons)}), '

                        if content['devices']:
                            devices = json.loads(content['devices'])
                            for device in devices:
                                self._import_json_to_db(json.dumps(device), Device)
                            updated_str += f'Devices ({len(devices)})'

                        if updated_str == '':
                            updated_str = 'No new updates.'

                        self.logger.debug(f'Updated the database with server information (full_update: {full_update}): {updated_str}')

                        return True

                else:
                    self.logger.warning(f'Status code when requesting db from server: {response.status_code}')
                    return False

            except requests.exceptions.RequestException as e:
                self.logger.warning('{}: RequestException: {}'.format(config_url, e))
                return False

        else:
            self.logger.debug('no server or receiver_serial provided. request_server_db not possible.')
            return False

    def _import_json_to_db(self, json_obj, odm_class, store=True):
        # json input of db elements will be imported even though some fields are unknown
        # odm_class: e.g. Scope, Receiver, ...
        try:
            db_object = odm_class.from_json(json_obj, created=True)
        except FieldDoesNotExist as e:
            error = True
            counter = 0
            # typical message: The fields "{'test2', 'test3', 'test1'}" do not exist on the document "Scope"
            # convert the error message to a string and parse the unknown fields
            message = str(e)
            # sometimes errors may appear in a series before all of them are detected
            while error:
                # find content within { } parentesis
                unknown_fields = str(re.findall('\{(.*?)\}', message))
                # find the unknown fields from comma separated content to get a list of fields
                unknown_fields = re.findall("\'(.*?)\'", unknown_fields)

                self.logger.debug(f'_import_json_to_db: {message}. Fields removed: {unknown_fields}.')

                # convert json to dict, remove keys and convert back for import
                json_dict = json.loads(json_obj)
                json_dict = DcHelper.remove_keys_from_dict(json_dict, unknown_fields)
                json_obj = json.dumps(json_dict)

                try:
                    db_object = odm_class.from_json(json_obj, created=True)
                    error = False
                except FieldDoesNotExist as e:
                    message = str(e)

                counter += 1
                # break endless loop
                if counter > 100:
                    self.logger.error(f'100000 repetitions during json import')
                    store = False
                    db_object = None
                    break
        if store:
            db_object.store()

        return db_object

    def provide_db(self, receiver_serial, full_update=0):
        # the server uses this method to proved necessary database information to the labgateway/labclient

        receiver = Receiver.objects(_hash_id=receiver_serial).first()

        # ToDo: work around for now. We have to implement and test the partial db_sync!!!
        full_update = 1

        if receiver:

            last_sync = receiver.last_db_sync
            # this store method does not change _time_m in the receiver
            receiver.store_last_db_sync()
            # scope is not meant to change (ever)
            scope = receiver.scope
            # todo: need to try also scope.config??
            conf = Config.objects(config_id='default').first()

            # send everything
            if not last_sync or full_update:

                if scope:
                    projects = Project.objects(scope=scope)
                    persons = Person.objects(scope=scope)
                    # todo: remove when old receivers are gone. currently limited to "Two" until all receivers out there
                    #  can handle missing fields which were "required" fields up to version 23.
                    if not receiver.release_id or receiver.release_id <= 23:
                        devices = Device.objects(scope=scope, device_model='two')
                    else:
                        devices = Device.objects(scope=scope)
                    users = User.objects(scope=scope)
                else:
                    projects = []
                    persons = []
                    devices = []
                    users = []

                users = self._to_json_or_empty(users)
                conf_json = conf.to_json()

                if scope:
                    scope_json = scope.to_json()
                else:
                    scope_json = {}

                receiver_json = receiver.to_json()

                # ###################################################### START
                # temp hack for receiver with release_id <= 23
                if not receiver.release_id or receiver.release_id <= 23:

                    if not receiver.release_id or receiver.release_id <= 17:
                        # remove rec_cycles_active from scope, config, receiver and projects
                        conf_json = self._remove_attribute_from_db_object(conf_json, 'rcac', embedded_document='rc')
                        scope_json = self._remove_attribute_from_db_object(scope_json, 'rcac', embedded_document='rc')
                        receiver_json = self._remove_attribute_from_db_object(receiver_json, 'rcac', embedded_document='rc')

                        project_list = []
                        for project in projects:
                            project_json = self._remove_attribute_from_db_object(project, 'rcac', embedded_document='rc', return_instance=True)
                            project_list.append(project_json)
                        projects = project_list

                        # Remove trigger_recording
                        person_list = []
                        for person in persons:
                            person_json = self._remove_attribute_from_db_object(person, 'trec', return_instance=True)
                            person_list.append(person_json)
                        persons = json.dumps(person_list)

                    else:
                        # projects = self._to_json_or_empty(projects)
                        persons = self._to_json_or_empty(persons)

                    if not receiver.release_id or receiver.release_id <= 23:
                        # reboot OS
                        receiver_json = self._remove_attribute_from_db_object(receiver_json, 'ros')
                        # live data interval
                        receiver_json = self._remove_attribute_from_db_object(receiver_json, 'ldi', embedded_document='rc')
                        # device config
                        device_list = []
                        for device in devices:
                            device_json = self._remove_attribute_from_db_object(device, 'dc', return_instance=True)
                            device_list.append(device_json)
                        devices = json.dumps(device_list)
                        # event labels
                        project_list = []
                        for project in projects:
                            project_json = self._remove_attribute_from_db_object(project, 'ev_ls', return_instance=True)
                            project_list.append(project_json)
                        projects = json.dumps(project_list)

                # ###################################################### END
                else:
                    devices = self._to_json_or_empty(devices)
                    projects = self._to_json_or_empty(projects)
                    persons = self._to_json_or_empty(persons)

                # todo: check if any of these is empty...
                response_dict = {
                    'request_successful': True,
                    'receiver': receiver_json,
                    'config': conf_json,
                    'scope': scope_json,
                    'projects': projects,
                    'persons': persons,
                    'devices': devices,
                    'users': users,
                }

            # only things that changed since the last sync
            else:

                # query for changed Documents
                if scope:
                    projects = Project.objects(scope=scope, _time_m__gt=last_sync)
                    persons = Person.objects(scope=scope, _time_m__gt=last_sync)
                    devices = Device.objects(scope=scope, _time_m__gt=last_sync)
                    users = User.objects(scope=scope, _time_m__gt=last_sync)
                else:
                    projects = []
                    persons = []
                    devices = []
                    users = []

                # if the querry set is empty, then return an empty list instead of a json representation
                projects = self._to_json_or_empty(projects)
                persons = self._to_json_or_empty(persons)
                devices = self._to_json_or_empty(devices)
                users = self._to_json_or_empty(users)

                # single entities treated slightly different
                if scope:
                    receiver_json = self._compare_time_and_to_json_or_empty(receiver, last_sync)
                    # if the receiver changed, also send the scope -> workaround for an old bug (receiver previously receiver.scope == None)
                    if receiver_json:
                        scope = scope.to_json()
                else:
                    # ################################################## START
                    # THIS IS A TERRIBLE HACK!!!!
                    # ToDo: when all receivers have a release_id > 14 we should be able to remove this?
                    scope = Scope.objects(id=ObjectId('5efafe0c85ab561f890dc41d')).first()
                    # scope = Scope.objects(id=ObjectId('5eebc4d28f4ec8676a629426')).first()
                    receiver.scope = scope
                    scope = self._to_json_or_empty(scope)
                    # THIS IS A TERRIBLE HACK!!!!
                    # ################################################## END
                    # scope = None
                    receiver_json = receiver.to_json()

                response_dict = {
                    'request_successful': True,
                    'receiver': receiver_json,
                    'config': conf.to_json(),
                    'scope': scope,
                    'projects': projects,
                    'persons': persons,
                    'devices': devices,
                    'users': users,
                }

            # print(json.dumps(response_dict, indent=4))

            return response_dict

        else:

            self.logger.warning(f'Update request from unknown receiver with serial {receiver_serial}')

            # fetch info from dms
            json_request = {
                'key': 'device',
                'data': {
                    'device_model': 'labgateway',
                    'device_serial': receiver_serial
                }
            }

            resp = requests.post('https://dms.earconnect.de/57898452361548795016', json=json.dumps(json_request),
                                 timeout=config.request_timeout)

            if resp.status_code == 200 and json.loads(resp.text):
                
                json_return = json.loads(resp.text)

                receiver = Receiver()
                receiver._hash_id = json_return['serial']
                receiver.udi_type = json_return['udi'].split('-')[5]
                receiver.udi_lot = json_return['udi'].split('-')[6]
                receiver.mac_address = json_return['mac_address']
                receiver.device_model = json_return['device_model']

                receiver.save()

                self.logger.info(f'Receiver fetched from dms {receiver.serial}')

            return {'request_successful': False}

    def provide_df(self, hash_id):
        # the server uses this method to proved a datafile and the necessary database information

        df = DataFile.objects(_hash_id=hash_id).first()

        if df:

            response_dict = {}

            response_dict['df'] = df.to_json()

            scope = df.project.scope
            response_dict['config'] = Config.objects().first().to_json()
            response_dict['scope'] = scope.to_json()
            response_dict['projects'] = self._to_json_or_empty(Project.objects(scope=scope))
            response_dict['receivers'] = self._to_json_or_empty(Receiver.objects(scope=scope))
            response_dict['persons'] = self._to_json_or_empty(Person.objects(scope=scope))
            response_dict['devices'] = self._to_json_or_empty(Device.objects(scope=scope))
            response_dict['users'] = self._to_json_or_empty(User.objects(scope=scope))

        else:
            response_dict = {}

        return response_dict

    def _remove_attribute_from_db_object(self, db_object, key, embedded_document=None, return_instance=False):
        # sorry this method is quite ugly. We can remove it once all Gateways are past version 23 ;)
        
        skip_from_json = False
        
        if db_object:
            if type(db_object) is not str:
                try:
                    db_object_json = db_object.to_json()
                except AttributeError:
                    # this happens when the given object is no longer a db query object but already a python dict
                    skip_from_json = True
                    db_object_json = db_object
            else:
                db_object_json = db_object

            # self.logger.debug(db_object_json)

            if not embedded_document:
                
                if not skip_from_json:
                    db_object_json = json.loads(db_object_json)
                db_object_json.pop(key, None)

            else:
                
                if not skip_from_json:
                    db_object_json = json.loads(db_object_json)
                if embedded_document in db_object_json:
                    db_object_json[embedded_document].pop(key, None)

            # self.logger.debug(f'removed key {key} from {db_object}')

            if not return_instance:
                db_object_json = json.dumps(db_object_json)

            return db_object_json
        else:
            self.logger.debug(f'Did not provide a db_object for removing an attribute')
            return db_object

    def _to_json_or_empty(self, db_queryset):
        # Check if the queryset has any entries and if yes, convert it ot a json representation. Otherwise an empty list

        if db_queryset:
            answer = db_queryset.to_json()
        else:
            answer = {}

        return answer

    def _compare_time_and_to_json_or_empty(self, db_entity, last_sync):

        # ToDo: is it here time_m or _time_m?
        if db_entity.time_m and db_entity.time_m > last_sync:
            answer = db_entity.to_json()
        else:
            answer = None

        return answer

    def pull_df(self, df_hash_id, download_slices=True):
        # this method downloads a specific data_file

        if self.server:
            try:

                response = requests.get(self.server + '/api_v01/pull_df/' + df_hash_id, timeout=config.request_timeout)

                if response.status_code == 200:

                    content = json.loads(response.content.decode())

                    # content = json.loads(response.content.decode())
                    if content:

                        created = True
                        updated_str = ''

                        if content['scope']:
                            scope = Scope.from_json(content['scope'], created=created)
                            scope.store()
                            updated_str += 'Scope, '

                        if content['config']:
                            conf = Config.from_json(content['config'], created=created)
                            conf.store()
                            updated_str += 'Config, '

                        if content['projects']:
                            projects = json.loads(content['projects'])
                            for project in projects:
                                project = Project.from_json(json.dumps(project), created=created)
                                project.store()
                            updated_str += f'Projects ({len(projects)}), '

                        if content['receivers']:
                            receivers = json.loads(content['receivers'])
                            for receiver in receivers:
                                receiver = Receiver.from_json(json.dumps(receiver), created=created)
                                receiver.store()
                            updated_str += f'Receivers ({len(receivers)}), '

                        if content['persons']:
                            persons = json.loads(content['persons'])
                            for person in persons:
                                person = Person.from_json(json.dumps(person), created=created)
                                person.store()
                            updated_str += f'Persons ({len(persons)}), '

                        if content['devices']:
                            devices = json.loads(content['devices'])
                            for device in devices:
                                device = Device.from_json(json.dumps(device), created=created)
                                device.store()
                            updated_str += f'Devices ({len(devices)})'

                        if content['users']:
                            users = json.loads(content['users'])
                            for user in users:
                                user = User.from_json(json.dumps(user), created=created)
                                user.store()
                            updated_str += f'User ({len(users)}), '

                        DataFile.objects(_hash_id=df_hash_id).delete()

                        df = DataFile.from_json(content['df'], created=True)
                        df.save_changes(final_analyse=False)

                        if download_slices:

                            for sl_hash in df.get_slice_list():
                                if not self.pull_ds(df_hash_id, sl_hash, df=df):
                                    self.logger.warning(f'downloading slice {sl_hash} from df {df_hash_id} failed')

                            self.logger.info(f'Data file {df_hash_id} completely downloaded + database_entries: {updated_str}')
                        else:
                            self.logger.info(f'Data file {df_hash_id} meta downloaded + database_entries: {updated_str}')

                        return df
                    else:
                        return False
                        self.logger.info(f'Data file {df_hash_id} does not exist on the server')
                else:
                    self.logger.warning(f'Downloading data file {df_hash_id} failed. Status code {response.status_code}')
                    return False
            except requests.exceptions.RequestException as e:
                return False
                self.logger.warning('pull_df failed with RequestException. server down?')
        else:
            self.logger.warning(f'no server or receiver_serial provided. pull_df not possible.')
            return False

    def pull_ds(self, df_hash_id, sl_hash, df=None):
        # this method downloads a specific slice from a data_file
        # hash_id: id of datafile
        # sl_hash: slice hash
        # df: pass the df instance if you intend to work with the df file right away => slice._path needs to be adjusted

        if self.server:
            try:

                if not df:
                    df = DataFile.objects(_hash_id=df_hash_id).first()

                if not df:
                    self.logger.warning(f'df meta {df_hash_id} does not exist. Download this first.')
                    return False

                # check if the slice is still up to date to not download things twice:
                sl = df.get_slice(sl_hash)
                exists = sl.file_exists
                meta_size = sl.compressed_size_meta
                if exists:
                    size_harddrive = sl._path.stat().st_size
                else:
                    size_harddrive = 0

                # print(exists, meta_size, size_harddrive)

                if not exists or (exists and meta_size != size_harddrive):

                    # self.logger.debug(f'sl_hash: {sl_hash}, exists: {exists}, meta_size: {meta_size}, size_harddrive: {size_harddrive}')
                    
                    response = requests.get(self.server + '/api_v01/pull_ds/' + df_hash_id + '/' + sl_hash, timeout=config.request_timeout)

                    if response.status_code == 200:

                        if response.content:

                            df_path = df.path
                            if not df_path.exists():
                                df_path.mkdir()

                            download_path = str(df_path / response.headers['filename'])

                            # todo: test this!
                            #  - if a file was previously not compressed (.bin) but is now (downloadpath .zstd) => delete the old file
                            if exists and str(sl._path) != download_path:
                                sl._path.unlink()

                            with open(download_path, 'wb') as fp:
                                fp.write(response.content)

                            sl._path = Path(download_path)

                            self.logger.debug(f'slice {sl_hash} of df {df_hash_id} downloaded')
                            return True
                        # content = json.loads(response.content.decode())

                        # else:
                        #     return False
                        #     self.logger.info(f'Data file {hash_id} does not exist on the server')
                    else:
                        self.logger.warning(f'Downloading data slice {sl_hash} of df {df_hash_id} failed. Status code {response.status_code}')
                        return False
                else:
                    self.logger.debug(f'downloading slice {sl_hash} not necessary: exists {exists}, harddrive_size: {size_harddrive}, meta_size {meta_size}')
                    return True
            except requests.exceptions.RequestException as e:
                return False
                self.logger.warning('pull_ds failed with RequestException. server down?')
        else:
            self.logger.warning(f'no server or receiver_serial provided. pull_ds not possible.')
            return False

    def request_server_df_list(self):
        # receive a list of all data files that are stored on the server

        if self.server:

            response = requests.get(self.server + '/api_v01/get_df_list')

            if response.status_code == 200:
                # convert the string-representation of the list into a Python list
                server_df_list = eval(response.content.decode())
                server_df_list.sort()

                return server_df_list
            else:
                self.logger.warning(f'requesting df list failed. Status code {response.status_code}')
                return False
        else:
            self.logger.warning('no server selected')

    def __delete_scope_db(self):

        self.logger.info('Deleting data files and database entries due to a scope change.')

        Receiver.objects.delete()
        User.objects.delete()
        Device.objects.delete()
        Project.objects.delete()
        Person.objects.delete()
        Scope.objects.delete()
        DataFile.objects.delete()

        path_df = config.df_path
        shutil.rmtree(path_df)

        if not path_df.exists():
            path_df.mkdir()
