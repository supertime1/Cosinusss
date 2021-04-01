#!/usr/bin/env python3

from mongoengine import *
import mongoengine
from pymongo import MongoClient
from datetime import datetime, timezone
import time
import json
import uuid
import hashlib
import string
import random
import pytz

from .dc_helper import DcHelper
utc_to_local = DcHelper.utc_to_local

from . import config
logger = config.logger

# we have chosen:
#       Randomly choose a unique identifier in the application and retry if it is already assigned
#       https://www.mongodb.com/blog/post/generating-globally-unique-identifiers-for-use-with-mongodb
#       be aware of: https://docs.mongodb.com/manual/core/sharding-shard-key/#unique-indexes
# override save()
#       https://stackoverflow.com/questions/6102103/using-mongoengine-document-class-methods-for-custom-validation-and-pre-save-hook

def generate_hash(hash_len=5):

    characters = string.ascii_uppercase + string.digits

    # remove ambiguous characters
    characters = characters.replace('O', '')
    characters = characters.replace('I', '')

    return ''.join(random.choice(characters) for i in range(hash_len))

def get_receiver_config(receiver_serial):

    config_instance = Config.objects(config_id='default').first()

    if config_instance:
        config_default = config_instance.receiver_config
    else:
        config_default = {
                        "sync_db_interval": 300,
                        "df_store_interval": 900,
                        "chunk_interval": 900,
                        "temp_thres": 33,
                        "receive_data_mode": "soc",
                        "latest_release": "14",
                        "rec_cycles_active": True,
                        "rec_cycles_time": 180,
                        "rec_cycles_step": 900,
                        "rec_cycles_tol": 120,
                        "rawdata_service": True,
                        "calc_ppg_quality": True,
                        "calc_hr": False,
                        "calc_spo2": False,
                        "calc_br": True
                    }

    receiver = Receiver.objects(_hash_id=receiver_serial).first()

    if receiver and receiver.scope:
        config_scope = receiver.scope.receiver_config
    else:
        config_scope = None

    # config_project = receiver.project.receiver_config

    if receiver:
        config_receiver = receiver.receiver_config
    else:
        config_receiver = None

    final_receiver_config = {}
    for attr in config_default:
        value = None
        if config_receiver:
            value = getattr(config_receiver, attr)
        # if value is None and config_project:
        #    value = getattr(config_project, attr)
        if value is None and config_scope:
            value = getattr(config_scope, attr)
        if value is None:
            value = getattr(config_default, attr)
        final_receiver_config[attr] = value

    return final_receiver_config


class Comment(EmbeddedDocument):

    # relationships
    user = ReferenceField('User', db_field='us')

    # further fields
    __time_c = DateTimeField(db_field='t_c', required=True)

    comment = StringField(max_length=1024, db_field='co', required=True)

    def __str__(self):

        if self.user:
            user = str(self.user)
        else:
            user = 'unknown'

        return f'Comment(user: {user}, time_c: {self.time_c}, comment: "{self.comment}"'

    @property
    def time_c(self):
        return self.__time_c

    @time_c.setter
    def time_c(self, datetime_new):
        # todo: make sure datetime_new is UTC?
        self.__time_c = datetime_new

class DeviceConfig(EmbeddedDocument):
    
    # AFE current of Two and C-Med in mA
    afe_led_current_ir = IntField(db_field='i_ir', null=True)
    afe_led_current_red = IntField(db_field='i_r', null=True)
    # ADC ranges of Two and C-Med in bytes from 0...3
    adc_range = IntField(db_field='adcr', null=True)
    
    def __str__(self):

        attr_list = list(self)

        return_str = 'DeviceConfig('
        values = []
        for attr in attr_list:
            return_str += attr + '={}, '
            values.append(getattr(self, attr))
        return_str = return_str[:-2] + ')'

        return return_str.format(*values)

class ReceiverConfig(EmbeddedDocument):

    # interval in seconds: sync db with server: DB_Sync().request_server_db()
    sync_db_interval = IntField(db_field='sdi', null=True)
    # interval in seconds: store data_file
    df_store_interval = IntField(db_field='dsi', null=True)
    # interval in seconds: finalize chunk and start new one
    chunk_interval = IntField(db_field='chi', null=True)
    # threshold to connect ot sensor in °C
    temp_thres = IntField(db_field='tth', null=True)
    # how to receive vital signs data: soc service (soc) or standard services (std)
    receive_data_mode = StringField(max_length=3, db_field='rdm')
    # set release, currently it's an integer (8) later it will be a string (0.7.3)
    latest_release = StringField(max_length=10, db_field='lr')
    # recording cycles: rec_cycles_time (recording duration), rec_cycles_step (step size), rec_cycles_tol (tolerance)
    rec_cycles_active = BooleanField(db_field='rcac', default=True)
    rec_cycles_time = IntField(db_field='rcti', null=True)
    rec_cycles_step = IntField(db_field='rcst', null=True)
    rec_cycles_tol = IntField(db_field='rcto', null=True)
    # activate live data stream at interval in seconds
    live_data_interval = IntField(db_field='ldi', null=None)
    # activate rawdata
    rawdata_service = BooleanField(db_field='rs', null=True)
    # activate algo for: ppg quality, heart rate, spo2, respiration rate
    calc_ppg_quality = BooleanField(db_field='cp', null=True)
    calc_hr = BooleanField(db_field='ch', null=True)
    calc_spo2 = BooleanField(db_field='cs', null=True)
    calc_br = BooleanField(db_field='cb', null=True)

    def __str__(self):

        attr_list = list(self)

        return_str = 'ReceiverConfig('
        values = []
        for attr in attr_list:
            return_str += attr + '={}, '
            values.append(getattr(self, attr))
        return_str = return_str[:-2] + ')'

        return return_str.format(*values)


class Config(Document):

    # id
    config_id = StringField(primary_key=True)

    _time_c = DateTimeField(db_field='t_c')
    _time_m = DateTimeField(db_field='t_m')

    # relationships
    receiver_config = EmbeddedDocumentField('ReceiverConfig', db_field='rc')

    def save(self, *args, **kwargs):

        self._time_c = datetime.now(timezone.utc)
        self._time_m = datetime.now(timezone.utc)
        super(Config, self).save(force_insert=True, *args, **kwargs)

    def save_changes(self, *args, **kwargs):

        self._time_m = datetime.now(timezone.utc)
        super(Config, self).save(*args, **kwargs)

    def store(self):

        if self.config_id:
            self.save_changes()
        else:
            self.save()

    @property
    def time_c(self):
        return utc_to_local(self._time_c, None)

    @property
    def time_m(self):
        return utc_to_local(self._time_m, None)

class Scope(Document):

    # id
    # just default BsonId

    # relationships
    receiver_config = EmbeddedDocumentField('ReceiverConfig', db_field='rc')

    # further fields
    _time_c = DateTimeField(db_field='t_c')
    _time_m = DateTimeField(db_field='t_m')

    name = StringField(db_field='name', unique=True)
    timezone = StringField(max_length=30, required=True, db_field='tz')
    organisation = StringField(max_length=50, db_field='org')
    first_name = StringField(max_length=50, db_field='fname')
    last_name = StringField(max_length=50, db_field='lname')
    email = EmailField(db_field='em', unique=True)
    desc = StringField(max_length=1024)

    def __str__(self):

        return 'Scope(id={}, time_c={})'.format(self.id, self.time_c)

    @property
    def time_c(self):
        return utc_to_local(self._time_c, self.timezone)

    @property
    def time_c_date(self):
        return str(self.time_c).split(' ')[0]

    @property
    def time_m(self):
        return utc_to_local(self._time_m, self.timezone)

    def save(self, *args, **kwargs):

        # set _time_c
        self._time_c = datetime.now(timezone.utc)
        self._time_m = datetime.now(timezone.utc)

        # set name to id if name not defined
        if not self.name:
            self.name = generate_hash(18)
            # call save() of super; ensure uniqueness with force_insert
            super(Scope, self).save(force_insert=True, *args, **kwargs)
            self.name = str(self.id)
            super(Scope, self).save(*args, **kwargs)

        # customized name
        else:
            # call save() of super; ensure uniqueness with force_insert
            super(Scope, self).save(force_insert=True, *args, **kwargs)

    def save_changes(self, *args, **kwargs):

        # set time_m
        self._time_m = datetime.now(timezone.utc)

        # set name to id if name not defined
        if not self.name:
            self.name = str(self.id)
            super(Scope, self).save(*args, **kwargs)

        # customized name
        else:
            super(Scope, self).save(*args, **kwargs)

    def store(self):

        if not self._time_c:
            self.save()
        else:
            self.save_changes()


class User(Document):

    # id
    # ToDo: setter and getter -> validation
    #       https://stackoverflow.com/questions/16881624/mongoengine-0-8-0-breaks-my-custom-setter-property-in-models
    username = StringField(primary_key=True)

    # relationships
    scope = ReferenceField('Scope')
    person = ReferenceField('Person')
    projects = ListField(ReferenceField('Project'))

    # further fields
    _time_c = DateTimeField(db_field='t_c')
    _time_m = DateTimeField(db_field='t_m')

    roles = ListField(StringField(max_length=50))

    password_hash = StringField(required=True, max_length=128, db_field='pwh')
    password_salt = StringField(required=True, max_length=32, db_field='pws')

    _last_login = DateTimeField(db_field='ll')
    last_login_ip = StringField(max_length=50, db_field='ll_ip')
    is_active = BooleanField(default=True, db_field='ac')
    # is_authenticated = BooleanField(default=False, db_field='auth')

    email = EmailField(db_field='em', unique=True)
    _email_confirmed = DateTimeField(db_field='em_c')
    email_secondary = EmailField(db_field='em2')
    _email_secondary_confirmed = DateTimeField(db_field='em2_c')

    first_name = StringField(max_length=50, db_field='fname')
    last_name = StringField(max_length=50, db_field='lname')
    organisation = StringField(max_length=50, db_field='org')

    # ~ def __init__(self, *args, **kwargs):

    # ~ super(User, self).__init__(*args, **kwargs)
    # ~ self._is_authenticated = False

    def __str__(self):

        return 'User(username={}, time_c={}, is_active={})'.format(self.username, self.time_c, self.is_active)

    # ~ @property
    # ~ def is_authenticated(self):
    # ~ return self._is_authenticated

    # @is_authenticated.setter
    # def is_authenticated(self, is_authenticated):
    #    self._is_authenticated = is_authenticated

    @property
    def time_c(self):
        if self.scope:
            return utc_to_local(self._time_c, self.scope.timezone)
        else:
            return utc_to_local(self._time_c, 'Europe/Berlin')

    @property
    def time_c_date(self):
        return str(self.time_c).split(' ')[0]

    @property
    def time_m(self):
        if self.scope:
            return utc_to_local(self._time_m, self.scope.timezone)
        else:
            return utc_to_local(self._time_m, 'Europe/Berlin')

    @property
    def last_login(self):
        if self.scope:
            return utc_to_local(self._last_login, self.scope.timezone)
        else:
            return utc_to_local(self._last_login, 'Europe/Berlin')

    @last_login.setter
    def last_login(self, datetime_new):
        # todo: make sure datetime_new is UTC?
        self._last_login = datetime_new

    @property
    def email_confirmed(self):
        if self.scope:
            return utc_to_local(self._email_confirmed, self.scope.timezone)
        else:
            return utc_to_local(self._email_confirmed, 'Europe/Berlin')

    @email_confirmed.setter
    def email_confirmed(self, datetime_new):
        # todo: make sure datetime_new is UTC?
        self._email_confirmed = datetime_new

    @property
    def email_secondary_confirmed(self):
        if self.scope:
            return utc_to_local(self._email_secondary_confirmed, self.scope.timezone)
        else:
            return utc_to_local(self._email_secondary_confirmed, 'Europe/Berlin')

    @email_secondary_confirmed.setter
    def email_secondary_confirmed(self, datetime_new):
        # todo: make sure datetime_new is UTC?
        self._email_secondary_confirmed = datetime_new

    @property
    def is_authenticated(self):
        return True

    @property
    def roles_str(self):
        return ', '.join(self.roles)

    @property
    def is_active_str(self):
        if self.is_active:
            return 'active'
        else:
            return '<span style="color: red;">disabled</span>'

    @property
    def is_anonymous(self):
        return False

    def get_id(self):
        return self.username

    @property
    def password(self):
        return self.password_hash

    @password.setter
    def password(self, password):
        salt = uuid.uuid4().hex
        hashed_password = hashlib.sha512(password.encode() + salt.encode()).hexdigest()
        self.password_hash = hashed_password
        self.password_salt = salt

    def check_password(self, password):

        # if password empty or is None return False
        if not password:
            return False

        hashed_password = hashlib.sha512(password.encode() + self.password_salt.encode()).hexdigest()

        if self.password_hash == hashed_password:
            # self._is_authenticated = True
            # self.is_authenticated = True
            # self.save_changes()
            return True

        else:
            # self._is_authenticated = False
            # self.is_authenticated = False
            # self.save_changes()
            return False

    def save(self, *args, **kwargs):

        # set _time_c
        self._time_c = datetime.now(timezone.utc)
        self._time_m = datetime.now(timezone.utc)
        # call save() of super; ensure uniqueness with force_insert
        super(User, self).save(force_insert=True, *args, **kwargs)

    def save_changes(self, *args, **kwargs):

        # set time_m
        self._time_m = datetime.now(timezone.utc)
        # call save() of super
        super(User, self).save(*args, **kwargs)

    def store(self):

        if not self._time_c:
            self.save()
        else:
            self.save_changes()


class Project(Document):

    # id
    _hash_id = StringField(primary_key=True)

    # relationships
    scope = ReferenceField('Scope', required=True)
    receiver_config = EmbeddedDocumentField('ReceiverConfig', db_field='rc')
    event_labels = ListField(StringField(), db_field='ev_ls')
    
    # further fields
    _time_c = DateTimeField(db_field='t_c')
    _time_m = DateTimeField(db_field='t_m')

    name = StringField(max_length=50, required=True)
    desc = StringField(max_length=1024)
    timezone = StringField(max_length=30, db_field='tz')

    stat_df_files = LongField(db_field='df')
    stat_slices = LongField(db_field='sl')
    stat_samples = LongField(db_field='sa')
    stat_bytes = LongField(db_field='by')
    stat_duration = LongField(db_field='du')

    def __str__(self):

        return 'Project(hash_id={}, name={}, time_c={})'.format(self.hash_id, self.name, self.time_c)

    @property
    def time_c(self):
        return utc_to_local(self._time_c, self.timezone)

    @property
    def time_c_date(self):
        return str(self.time_c).split(' ')[0]

    @property
    def time_m(self):
        return utc_to_local(self._time_m, self.timezone)

    @property
    def timezone_offset(self):
        return datetime.now(pytz.timezone(self.timezone)).utcoffset().total_seconds()/60/60

    def save(self, *args, **kwargs):

        while True:

            try:

                # set unique _hash_id
                if config.producer_hash:
                    self._hash_id = config.producer_hash + '.' + generate_hash(4)
                else:
                    self._hash_id = generate_hash(4)

                # set _time_c
                self._time_c = datetime.now(timezone.utc)
                self._time_m = datetime.now(timezone.utc)
                # call save() of super; ensure uniqueness with force_insert
                super(Project, self).save(force_insert=True, *args, **kwargs)
                # break if there is no NotUniqueError
                break

            except mongoengine.errors.NotUniqueError as e:

                # try again with new _hash_id
                # logger.debug(e)
                print(e)

    def save_changes(self, *args, **kwargs):

        # set _time_m
        self._time_m = datetime.now(timezone.utc)
        # call save() of super
        super(Project, self).save(*args, **kwargs)

    def store(self):

        if not self._hash_id:
            self.save()
        else:
            self.save_changes()

    @property
    def hash_short(self):

        hash_split = self._hash_id.split('.')

        return hash_split[-1]

    @property
    def hash_id(self):

        return self._hash_id


class Person(Document):

    # id
    _hash_id = StringField(primary_key=True)

    # relationships
    scope = ReferenceField('Scope', required=True)
    project = ReferenceField('Project', required=True)
    devices = ListField(ReferenceField('Device'))

    # further fields
    _time_c = DateTimeField(db_field='t_c')
    _time_m = DateTimeField(db_field='t_m')

    label = StringField(max_length=50, db_field='la')
    is_active = BooleanField(default=True, db_field='ac')
    sex = StringField(max_length=10, db_field='se')
    year_of_birth = IntField(db_field='yb')
    # height = IntField()
    # weight = IntField()
    # bmi = FloatField()
    first_name = StringField(max_length=50, db_field='fn')
    last_name = StringField(max_length=50, db_field='ln')
    trigger_recording = IntField(db_field='trec', null=True, default=None)

    def __str__(self):

        return 'Person(hash_id={}, time_c={}, label={}, active={})'.format(self.hash_id, self.time_c, self.label, self.is_active)

    @property
    def time_c(self):
        return utc_to_local(self._time_c, self.project.timezone)

    @property
    def time_c_date(self):
        return str(self.time_c).split(' ')[0]

    @property
    def time_m(self):
        return utc_to_local(self._time_m, self.project.timezone)

    @property
    def is_active_str(self):
        if self.is_active:
            return 'active'
        else:
            return '<span style="color: red;">disabled</span>'

    def save(self, *args, **kwargs):

        while True:

            try:

                # set unique _hash_id
                # self._hash_id = self.project._hash_id + '.' + config.generate_hash(4)
                self._hash_id = self.project.id + '.' + config.generate_hash(4)
                # set _time_c
                self._time_c = datetime.now(timezone.utc)
                self._time_m = datetime.now(timezone.utc)
                # call save() of super; ensure uniqueness with force_insert
                super(Person, self).save(force_insert=True, *args, **kwargs)
                # break if there is no NotUniqueError
                break

            except mongoengine.errors.NotUniqueError as e:

                # try again with new _hash_id
                # logger.debug(e)
                print(e)

    def save_changes(self, *args, **kwargs):

        # set _time_m
        self._time_m = datetime.now(timezone.utc)
        # call save() of super
        super(Person, self).save(*args, **kwargs)

    def store(self):

        if not self._hash_id:
            self.save()
        else:
            self.save_changes()

    @property
    def hash_short(self):

        hash_split = self._hash_id.split('.')

        return hash_split[-1]

    @property
    def hash_id(self):

        return self._hash_id

    @property
    def full_name(self):

        if self.firstname:
            full_name = self.first_name
        else:
            full_name = ''

        if self.surname:
            full_name += ' ' + self.last_name
        else:
            full_name += ''

        return full_name

    @property
    def initials(self):

        if self.firstname:
            initials = self.first_name[0:1]
        else:
            initials = ''

        if self.surname:
            initials += self.last_name[0:1]
        else:
            initials += ''

        return initials

    @property
    def initials_long(self):

        if self.firstname:
            initials = self.first_name[0:2]
        else:
            initials = ''

        if self.surname:
            initials += self.last_name[0:2]
        else:
            initials += ''

        return initials

    @property
    def devices_str(self):

        dev_info = []
        for device in self.devices:
            if device.cap_size:
                dev_info.append('<a href="/devices/show/' + device.serial + '">' + device.serial + ' cap-' + device.cap_size + '</a>')
            else:
                dev_info.append('<a href="/devices/show/' + device.serial + '">' + device.serial + '</a>')

        return ', '.join(dev_info)

    @property
    def device_1_serial(self):

        if self.devices:
            return self.devices[0].serial

        else:
            return ''

    @property
    def device_1_cap_size(self):

        if self.devices:
            return self.devices[0].cap_size

        else:
            return ''


class Receiver(Document):

    # id
    _hash_id = StringField(primary_key=True)

    # relationships
    scope = ReferenceField('Scope')
    project = ReferenceField('Project')
    persons = ListField(ReferenceField('Person'))
    receiver_config = EmbeddedDocumentField('ReceiverConfig', db_field='rc')

    # further fields
    _time_c = DateTimeField(db_field='t_c')
    _time_m = DateTimeField(db_field='t_m')

    mac_address = StringField(max_length=17, db_field='mac', required=True)
    release_id = IntField(db_field='rel_id')
    release = StringField(max_length=10, db_field='rel')
    udi_type = StringField(max_length=10, db_field='type', required=True)
    udi_lot = StringField(max_length=10, db_field='lot', required=True)
    device_model = StringField(max_length=40, db_field='model', required=True)
    _last_db_sync = DateTimeField(db_field='ldbs')
    send_logs_to_server = BooleanField(db_field='sls', null=True)
    reboot_os = BooleanField(db_field='ros', null=True, default=None)

    # not needed at all
    # udi = StringField(max_length=40)

    def __str__(self):

        return 'Receiver(serial={}, time_c={}, device_model={}, udi_lot={})'.format(self.serial, self.time_c, self.device_model, self.udi_lot)

    @property
    def time_c(self):
        tz = self.scope.timezone if self.scope else pytz.utc.zone
        return utc_to_local(self._time_c, tz)

    @property
    def time_c_date(self):
        return str(self.time_c).split(' ')[0]

    @property
    def time_m(self):
        tz = self.scope.timezone if self.scope else pytz.utc.zone
        return utc_to_local(self._time_m, tz)

    @property
    def last_db_sync(self):
        # ToDo: what if the timezone get changed -> we should use utc here in any case, that's always the same!?
        return utc_to_local(self._last_db_sync, pytz.utc.zone)

    @property
    def last_db_sync_local_time(self):
        tz = self.scope.timezone if self.scope else pytz.utc.zone
        return utc_to_local(self._last_db_sync, tz)

    @property
    def last_db_sync_time_str(self):

        if self._last_db_sync:
            total_sec = (datetime.utcnow() - self._last_db_sync).total_seconds()
            last_db_sync_str = DcHelper.seconds_to_time_str_2(total_sec)
        else:
            last_db_sync_str = 'NA'

        return last_db_sync_str

    @last_db_sync.setter
    def last_db_sync(self, datetime_new):
        # todo: make sure datetime_new is UTC?
        self._last_db_sync = datetime_new

    def save(self, *args, **kwargs):

        # set _time_c
        self._time_c = datetime.now(timezone.utc)
        self._time_m = datetime.now(timezone.utc)
        # call save() of super; ensure uniqueness with force_insert
        super(Receiver, self).save(force_insert=True, *args, **kwargs)

    def save_changes(self, *args, **kwargs):

        # set _time_m
        self._time_m = datetime.now(timezone.utc)
        # call save() of super
        super(Receiver, self).save(*args, **kwargs)

    def store(self):

        if not self._time_c:
            self.save()
        else:
            self.save_changes()

    def store_last_db_sync(self, *args, **kwargs):
        # this method changes only the _last_db_sync instead of _time_m

        self._last_db_sync = datetime.now(timezone.utc)
        super(Receiver, self).save(*args, **kwargs)

    @property
    def hash_short(self):

        hash_split = self._hash_id.split('.')

        return hash_split[-1]

    @property
    def hash_id(self):

        return self._hash_id

    @property
    def serial(self):

        return self._hash_id


class Device(Document):

    # id
    _hash_id = StringField(primary_key=True)

    # relationships
    scope = ReferenceField('Scope')
    project = ReferenceField('Project')

    # further fields
    _time_c = DateTimeField(db_field='t_c')
    _time_m = DateTimeField(db_field='t_m')

    mac_address = StringField(max_length=17, db_field='mac', required=True)
    release = StringField(max_length=10, db_field='rel')
    cap_size = StringField(choices=('XS', 'S', 'M', 'L', 'XL', 'XXL'), db_field='size')
    udi_type = StringField(max_length=10, db_field='type')
    udi_lot = StringField(max_length=10, db_field='lot')
    device_model = StringField(max_length=40, db_field='model')
    device_config = EmbeddedDocumentField('DeviceConfig', db_field='dc')
    # not needed at all
    # udi = StringField(max_length=40)

    def __str__(self):

        return 'Device(serial={}, time_c={}, device_model={}, udi_lot={})'.format(self.serial, self.time_c, self.device_model, self.udi_lot)

    @property
    def time_c(self):
        return utc_to_local(self._time_c, self.scope.timezone)

    @property
    def time_c_date(self):
        return str(self.time_c).split(' ')[0]

    @property
    def time_m(self):
        return utc_to_local(self._time_m, self.scope.timezone)

    def save(self, *args, **kwargs):

        # set _time_c
        self._time_c = datetime.now(timezone.utc)
        self._time_m = datetime.now(timezone.utc)
        # call save() of super; ensure uniqueness with force_insert
        super(Device, self).save(force_insert=True, *args, **kwargs)

    def save_changes(self, *args, **kwargs):

        # set _time_m
        self._time_m = datetime.now(timezone.utc)
        # call save() of super
        super(Device, self).save(*args, **kwargs)

    def store(self):

        if not self._time_c:
            self.save()
        else:
            self.save_changes()

    @property
    def hash_short(self):

        hash_split = self._hash_id.split('.')

        return hash_split[-1]

    @property
    def hash_id(self):

        return self._hash_id

    @property
    def serial(self):

        return self._hash_id


class PersonEvent(Document):
    
    _time_c = DateTimeField(db_field='t_c', required=True)
    label = StringField(required=True)
    
    # relationships
    scope = ReferenceField('Scope', required=True)
    person = ReferenceField('Person', required=True) 
    project = ReferenceField('Project', required=True)
    
    @property
    def time_c(self):
        return utc_to_local(self._time_c, self.scope.timezone)
    
    def save(self, *args, **kwargs):

        # set _time_c
        self._time_c = datetime.now(timezone.utc)
        # call save() of super; ensure uniqueness with force_insert
        super(PersonEvent, self).save(force_insert=True, *args, **kwargs)
    
class EventLog(Document):

    # id
    # just default BsonId

    # relationships
    scope = ReferenceField('Scope', required=True)
    creator = ReferenceField('User', db_field='cr', required=True)
    user = ReferenceField('User', db_field='us')
    project = ReferenceField('Project', db_field='pr')
    person = ReferenceField('Person', db_field='pe')
    receiver = ReferenceField('Receiver', db_field='re')
    device = ReferenceField('Device', db_field='de')
    data_file = ReferenceField('DataFile', db_field='da')

    # further fields
    _time_c = DateTimeField(db_field='t_c', required=True)

    message_type = StringField(max_length=30, db_field='m_t', required=True)
    message = StringField(max_length=256, db_field='m', required=True)

    def __str__(self):

        return 'EventLog(id={}, time_c={}, message_type={}, message={})'.format(self.id, self.time_c, self.message_type, self.message_short)

    @property
    def time_c(self):
        return utc_to_local(self._time_c, self.scope.timezone)

    @property
    def time_c_date(self):
        return str(self.time_c).split(' ')[0]

    @property
    def message_short(self):

        if len(self.message) > 13:

            message_short = self.message[:10] + '...'

        else:

            message_short = self.message

        return message_short

    def save(self, *args, **kwargs):

        if not self._time_c:
            self._time_c = datetime.now(timezone.utc)

        super(EventLog, self).save(*args, **kwargs)

    def store(self):
        self.save()

class ServerStats(Document):

    # id
    # just default BsonId

    # pseudo primary keys
    scope = StringField()
    day = StringField()
    hour = IntField()

    finalized = BooleanField(default=False)

    # user
    users_online = ListField()
    users_online_count = IntField(default=0)

    # dash
    dash_request_json = IntField(default=0)
    dash_request_person = IntField(default=0)
    dash_request_person_chunk = IntField(default=0)

    # person
    persons_data = ListField()
    persons_data_count = IntField(default=0)

    # receiver
    receivers_online = ListField()
    receivers_online_count = IntField(default=0)
    receivers_data = ListField()
    receivers_data_count = IntField(default=0)

    # api notify_receiver_release
    receiver_new_release = IntField(default=0)
    # api update_config
    receiver_db_sync = IntField(default=0)
    # api push_df
    receiver_push_df = IntField(default=0)
    receiver_push_df_samples = IntField(default=0)
    # api push_ds
    receiver_push_ds = IntField(default=0)
    receiver_push_ds_bytes = IntField(default=0)

    # api
    api_projects = IntField(default=0)
    api_people = IntField(default=0)
    api_data_files = IntField(default=0)
    api_data_file_meta = IntField(default=0)
    api_data_file_data = IntField(default=0)
    api_data_file_data_samples = IntField(default=0)
    api_trigger_record = IntField(default=0)

    # server load
    # how to measure this? -> linux shell command: uptime
    # The load average represents the work being done by the system. The three numbers show the load averages for the last minute, 5 minutes and 15 minutes, respectively.
    server_load_average_list = ListField()
    server_load_average = FloatField(default=0)
    # mermory usage
    # free -m
    server_ram_average_list = ListField()
    server_ram_average = IntField(default=0)

    def __str__(self):

        return 'ServerStats(scope={}, day={}, hour={})'.format(self.scope, self.day, self.hour)

    @property
    def users_online_count_prop(self):
        return len(self.users_online)

    @property
    def receivers_online_count_prop(self):
        return len(self.receivers_online)

    @property
    def persons_data_count_prop(self):
        return len(self.persons_data)

    def final_analyse(self):

        self.users_online_count = len(self.users_online)
        self.persons_data_count = len(self.persons_data)
        self.receivers_online_count = len(self.receivers_online)
        self.receivers_data_count = len(self.receivers_data)

        if self.server_ram_average_list:
            self.server_ram_average = int(sum(self.server_ram_average_list) / len(self.server_ram_average_list))
        else:
            self.server_ram_average = None

        if self.server_load_average_list:
            self.server_load_average = round(sum(self.server_load_average_list) / len(self.server_load_average_list), 2)
        else:
            self.server_load_average = None

        self.finalized = True

def server_stats(key, value=None, user=None, scope=None):

    if scope:
        scope = str(scope.id)
    elif user:
        try:
            if user.scope:
                scope = str(user.scope.id)
            else:
                scope = 'superadmin'
        except AttributeError:
            pass
    else:
        return False

    day = datetime.strftime(datetime.utcnow(), '%Y-%m-%d')
    hour = int(datetime.strftime(datetime.utcnow(), '%H'))

    server_stat = ServerStats.objects(scope=scope, day=day, hour=hour).first()

    if not server_stat:

        server_stat = ServerStats()
        server_stat.scope=scope
        server_stat.day=day
        server_stat.hour=hour

    if key == 'update_users_online':
        if not value in server_stat.users_online:
            server_stat.users_online.append(value)

    elif key == 'dash_request_json':
        server_stat.dash_request_json += 1

    elif key == 'dash_request_person':
        server_stat.dash_request_person += 1

    elif key == 'dash_request_person_chunk':
        server_stat.dash_request_person_chunk += 1

    elif key == 'receiver_new_release':
        server_stat.receiver_new_release += 1

    elif key == 'receiver_db_sync':
        server_stat.receiver_db_sync += 1

    elif key == 'receiver_push_df':
        server_stat.receiver_push_df += 1

    elif key == 'receiver_push_ds':
        server_stat.receiver_push_ds += 1

    elif key == 'receiver_push_df_samples':
        server_stat.receiver_push_df_samples += value

    elif key == 'receiver_push_ds_bytes':
        server_stat.receiver_push_ds_bytes += value

    elif key == 'persons_data':
        if not value in server_stat.persons_data:
            server_stat.persons_data.append(value)

    elif key == 'receivers_data':
        if not value in server_stat.receivers_data:
            server_stat.receivers_data.append(value)

    elif key == 'receivers_online':
        if not value in server_stat.receivers_online:
            server_stat.receivers_online.append(value)

    server_stat.save()




