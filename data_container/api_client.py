import requests
import json
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from bson.objectid import ObjectId

from . import config
from getpass import getpass

from .api_login import APILogin

class APIClient(APILogin):

    def __init__(self, project=None, *args, **kwargs):

        self.project = project
        self.logger = config.logger

        super(APIClient, self).__init__(*args, **kwargs)

    # get help
    def help(self, nr=None):
        help_dict = self.request('help')
        try:
            nr = int(nr)
        except TypeError:
            nr = None
        if nr:
            route = list(help_dict['routes'])[nr-1]
            print(route)
            print('')
            print(help_dict['routes'][route])
        else:
            print('get help to the route nr with help(nr)')
            print('')
            print('nr | route')
            print('-- | -----')
            for i, route in enumerate(list(help_dict['routes'])):
                print('{:02} | {}'.format(i+1, route))

    # get a list of projects' ids
    def projects(self):
        return self.request('projects')

    # get a list of people's ids
    def people(self, project=None):
        if project:
            return self.request('people/project/' + project)
        else:
            return self.request('people')

    # get a list of people's meta data
    def people_meta(self, project=None):
        if project:
            return self.request('people/meta/project/' + project)
        else:
            return self.request('people/meta')

    # get a list of people's meta data
    def person_meta(self, person):
        return self.request('person/meta/' + person)

    # get a list of data file's ids
    def data_files(self, project=None, person=None):
        if person:
            return self.request('data_files/person/' + person)
        elif project:
            return self.request('data_files/project/' + project)
        else:
            return self.request('data_files')

    # get data file meta data
    def data_file_meta(self, data_file):
        return self.request('data_file/' + data_file + '/meta')

    # get data file raw/vitals data
    def data_file_data(self, data_file, data_type):
        return self.request('data_file/' + data_file + '/' + data_type + '/data')

    # get data file data by chunk
    def data_file_data_by_chunk(self, data_file):
        return self.request('data_file/' + data_file + '/data_by_chunk')

    # get data file chunks statistic
    def data_file_chunks(self, data_file):
        return self.request('data_file/' + data_file + '/chunks')

    # get last vital signs
    def vitals(self, project=None, person=None):
        if person:
            return self.request('vitals/person/' + person)
        elif project:
            return self.request('vitals/project/' + project)

    # trigger recording for a person
    def trigger_record(self, person, seconds):
        return self.request('trigger_record/' + person + '/' + str(seconds))

    # reset trigger for a person
    def trigger_reset(self, person):
        return self.request('triggered_recording_done/' + person)

    # check if import_md5 exists in server db
    def check_import_md5(self, import_md5):
        return self.request('check_import_md5/' + import_md5)

    # one3_hr_algo
    def one3_hr_algo(self, algo_vers, df_hash_id, fkt=1, c_vers='v0', led='1', filt='IIR',
                     hamming=0, win=6):
        return self.request('one3_hr_algo/' + algo_vers + '/' + df_hash_id + '/' + str(fkt) + '/' +
                           str(c_vers) + '/' + str(led) + '/' + str(filt) + '/' + str(hamming) +
                            '/' + str(win))



