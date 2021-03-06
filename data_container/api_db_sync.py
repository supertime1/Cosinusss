import requests
import json
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from bson.objectid import ObjectId
from mongoengine.errors import FieldDoesNotExist, DoesNotExist
import re
import shutil
from terminaltables import AsciiTable

from . import DataFile
from .odm import User, Project, Person, Receiver, Device, EventLog, Comment, Scope, Config
from .dc_helper import DcHelper
from . import config
from jose import jwt

from .api_login import APILogin
from .api_client import APIClient

logger = config.logger

class DBSync(APIClient):

    def __init__(self, *args, **kwargs):

        self.project = None

        super(DBSync, self).__init__(*args, **kwargs)

        # todo:
        #  - more efficient (only things that changed, single data chunks ... )

        # todo Future feature:
        #  - slices to be sent here

    def push_df(self, df=None, df_hash_id=None, meta_only=False,
                partially=False, rawdata=True, data_type_list=None):
        # this will push the slices to the server.
        # Make sure to store() the data file before sending slices!

        if df:
            hash_id = df.hash_id
        else:
            df = DataFile.objects(_hash_id=df_hash_id).first()

        if df:
            df.api_client = self
            if meta_only:
                df.send_meta()
            else:
                df.send(partially=partially, rawdata=rawdata, data_type_list=data_type_list)
        else:
            logger.warning(f'push_ds: no df found for hash_id {hash_id}')

    def db_sync(self, allow_db_deletion=False):
        # The labgateway can use this method to receive the necessary
        # database elements from the server
        # allow_db_deletion: if a new scope is received, all database elements and dfs will be deleted

        # do the server request
        resp = self.request(f'db_sync/{self.username}', timeout=config.request_timeout)

        updated_str = ''

        if resp['scope']:
            scope = self._import_json_to_db(resp['scope'], Scope, store=False)
            # a scope change causes a reset of the db and df folder
            for local_scope in Scope.objects().all():
                if local_scope.id != scope.id and allow_db_deletion:
                    self.__delete_scope_db()
                    break
            scope.store()
            updated_str += 'Scope, '
        elif allow_db_deletion:
            self.__delete_scope_db()

        if resp['config']:
            self._import_json_to_db(resp['config'], Config)
            updated_str += 'Config, '

        if resp['users']:
            users = json.loads(resp['users'])
            for user in users:
                self._import_json_to_db(json.dumps(user), User)
            updated_str += f'User ({len(users)}), '

        if resp['projects']:
            projects = json.loads(resp['projects'])
            for project in projects:
                self._import_json_to_db(json.dumps(project), Project)
            updated_str += f'Projects ({len(projects)}), '

        if resp['persons']:
            persons = json.loads(resp['persons'])
            for person in persons:
                self._import_json_to_db(json.dumps(person), Person)
            updated_str += f'Persons ({len(persons)}), '

        if resp['receivers']:
            receivers = json.loads(resp['receivers'])
            for receiver in receivers:
                self._import_json_to_db(json.dumps(receiver), Receiver)
            updated_str += f'Receivers ({len(receivers)}), '

        if resp['devices']:
            devices = json.loads(resp['devices'])
            for device in devices:
                self._import_json_to_db(json.dumps(device), Device)
            updated_str += f'Devices ({len(devices)})'

        if updated_str == '':
            updated_str = 'No new updates.'

        logger.debug(f'Updated the database with server information: {updated_str}')

        return True

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

                logger.debug(f'_import_json_to_db: {message}. Fields removed: {unknown_fields}.')

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
                if counter > 10000:
                    logger.error(f'100000 repetitions during json import')
                    store = False
                    db_object = None
                    break
        if store:
            db_object.store()

        return db_object

    def provide_db(self, scope):
        # the server uses this method to proved necessary database information to the labgateway/labclient

        # this store method does not change _time_m in the receiver
        # ToDo: this is getting obsolete:
        #receiver.store_last_db_sync()

        # todo: need to try also scope.config??
        conf = Config.objects(config_id='default').first()
        projects = Project.objects(scope=scope)
        persons = Person.objects(scope=scope)
        devices = Device.objects(scope=scope)
        receivers = Receiver.objects(scope=scope)
        users = User.objects(scope=scope)

        response_dict = {
            'config': self._to_json_or_empty(conf),
            'scope': self._to_json_or_empty(scope),
            'projects': self._to_json_or_empty(projects),
            'persons': self._to_json_or_empty(persons),
            'receivers': self._to_json_or_empty(receivers),
            'devices': self._to_json_or_empty(devices),
            'users': self._to_json_or_empty(users),
        }

        return response_dict

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

        if db_object:
            if type(db_object) is not str:
                db_object_json = db_object.to_json()
            else:
                db_object_json = db_object

            # logger.debug(db_object_json)

            if not embedded_document:

                db_object_json = json.loads(db_object_json)
                db_object_json.pop(key, None)

            else:

                db_object_json = json.loads(db_object_json)
                if embedded_document in db_object_json:
                    db_object_json[embedded_document].pop(key, None)

            # logger.debug(f'removed key {key} from {db_object}')

            if not return_instance:
                db_object_json = json.dumps(db_object_json)

            return db_object_json
        else:
            logger.debug(f'Did not provide a db_object for removing an attribute')
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

    def overview_dfs(self, prj_hash_id=None):

        dur_sum = 0
        df_count = 0
        sample_count = 0
        people_dic = {}

        table = [['person', 'when', 'device', 'df id', 'duration', 'samples', 'cols']]

        if prj_hash_id:
            prj = Project.objects(_hash_id=prj_hash_id).first()
            df_list = DataFile.objects(project=prj).order_by('_date_time_start').all()
        else:
            df_list = DataFile.objects().order_by('_date_time_start').all()

        for df in df_list:
            dur_sum += df.duration
            df_count += 1
            sample_count += df.samples_meta
            print('\t' + 'df: ' + df._hash_id, end='\r')
            row = []
            if df.person:
                person = f'{df.person.hash_id} ({df.person.label})'
                people_dic[df.person.hash_id] = True
            else:
                person = 'None'
            if df.date_time_start:
                date_time_start = datetime.strftime(df.date_time_start, '%Y-%m-%d %H:%M:%S')
            else:
                date_time_start = 'None'
            try:
                if df.device:
                        device = f'{df.device.device_model} ({df.device.serial})'
                else:
                    device = df.device_model
            except DoesNotExist:
                device = f'{df.device_model} (Device not in local db!)'

            col_str = ''
            for col in list(df.cols):
                col_str += col + ', '
            col_str = col_str[:-2]
            if len(col_str) > 35:
                col_str = col_str[:32] + '...'
            row.append(person)
            row.append(date_time_start)
            row.append(device)
            row.append(df._hash_id)
            row.append(df.duration_netto_meta_str)
            row.append(df.samples_str)
            row.append(col_str)
            table.append(row)

        return table
        # print('')
        # people_str = len(list(people_dic))
        # dur_str = '{} hours'.format(round(dur_sum/3600, 1))
        # samples_str = '{} M'.format(round(sample_count/1000000, 2))
        #table = [
        #            ['people', 'data files', 'duration', 'samples'],
        #            [people_str, df_count, dur_str, samples_str]
        #        ]
        #print(AsciiTable(table).table)

    def df_list(self, prj_hash_id=None):

        if prj_hash_id:
            prj = Project.objects(_hash_id=prj_hash_id).first()
            df_list = DataFile.objects(project=prj).order_by('_date_time_start').all()
        else:
            df_list = DataFile.objects().order_by('_date_time_start').all()

        return df_list

    def pull_all_dfs(self, prj_hash_id=None, download_slices=True):

        self.logger.info('pull all dfs from server')
        df_list = self.data_files(project=prj_hash_id)
        df_pull_count = 0
        for df_hash_id in df_list:
            # local db query
            df = DataFile.objects(_hash_id=df_hash_id).first()
            # if local query is empty or df not finished yet: pull from server
            if not df or df.status_closed==False:
                df = self.pull_df(df_hash_id, download_slices=download_slices)
                df_pull_count += 1
            print('\t' + 'df: ' + df._hash_id, end='\r')
        print('')
        self.logger.debug('overall ' + str(df_pull_count) + ' dfs pulled from server')

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
                                    logger.warning(f'downloading slice {sl_hash} from df {df_hash_id} failed')

                            logger.info(f'Data file {df_hash_id} completely downloaded + database_entries: {updated_str}')
                        else:
                            logger.info(f'Data file {df_hash_id} meta downloaded + database_entries: {updated_str}')

                        return df
                    else:
                        return False
                        logger.info(f'Data file {df_hash_id} does not exist on the server')
                else:
                    logger.warning(f'Downloading data file {df_hash_id} failed. Status code {response.status_code}')
                    return False
            except requests.exceptions.RequestException as e:
                return False
                logger.warning('pull_df failed with RequestException. server down?')
        else:
            logger.warning(f'no server or receiver_serial provided. pull_df not possible.')
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
                    logger.warning(f'df meta {df_hash_id} does not exist. Download this first.')
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

                    # logger.debug(f'sl_hash: {sl_hash}, exists: {exists}, meta_size: {meta_size}, size_harddrive: {size_harddrive}')

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

                            logger.debug(f'slice {sl_hash} of df {df_hash_id} downloaded')
                            return True
                        # content = json.loads(response.content.decode())

                        # else:
                        #     return False
                        #     logger.info(f'Data file {hash_id} does not exist on the server')
                    else:
                        logger.warning(f'Downloading data slice {sl_hash} of df {df_hash_id} failed. Status code {response.status_code}')
                        return False
                else:
                    logger.debug(f'downloading slice {sl_hash} not necessary: exists {exists}, harddrive_size=meta_size={size_harddrive} bytes.')
                    return True
            except requests.exceptions.RequestException as e:
                return False
                logger.warning('pull_ds failed with RequestException. server down?')
        else:
            logger.warning(f'no server or receiver_serial provided. pull_ds not possible.')
            return False

    def __delete_scope_db(self):

        logger.info('Deleting data files and database entries due to a scope change.')

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
