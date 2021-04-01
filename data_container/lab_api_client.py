import requests
import json
from datetime import datetime
from jose import jwt
from getpass import getpass
import sys
import os
import pandas as pd
import logging
import sqlalchemy as sqla
import base64
import shutil

# ToDo: later to be integrated to db_sync.py

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
#print(logger)

URL_DIR = '/api_v02/'

class LabApiClient:
    
    def __init__(self, server='https://telecovid.earconnect.de'):
        
        self._server = server
        self._username = None
        self._password = None

        # use token.tmp if available
        try:
            token_dic = json.load(open('token.tmp'))
            x_auth_token = token_dic['x_auth_token']
            x_auth_token_expire = datetime.strptime(token_dic['x_auth_token_expire'], '%Y-%m-%d %H:%M:%S')
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            x_auth_token = None
            x_auth_token_expire = None

        self.x_auth_token = x_auth_token
        self.x_auth_token_expire = x_auth_token_expire

        self._base_url = self._server + URL_DIR
        
        self.header = {'Content-Type': 'application/json'}
        self.header_auth = {'Content-Type': 'application/json', 'X-AUTH-TOKEN': self.x_auth_token}
        
        if self._server:
            server_version = self._get_version()
            if server_version == 'network_error':
                logger.warning('Could not get server API version. Please check your internet connection.')
            elif server_version == 'server_error':
                logger.warning('Could not get server API version due to a server issue.')

            logger.info('lab_api_client using lab_server API version: ' + server_version)
            logger.info('\t' + 'server: ' + str(self._server))
            logger.info('\t' + 'username: ' + str(self._username))
        
        self.response_code = None
        self.response_error = None
        
        self.failed_login = None

    # setter and getter for x_auth_token -> set also x_auth_token in self.header_auth
    # def...
    
    @property
    def base_url(self):
        return self._base_url

    def _login(self):

        if self._username is None:

            self._username = input('username: ')
            self._password = getpass()

        elif self._password is None:

            self.password = getpass()

        try:
            response = requests.get(self.base_url + 'login', headers={'username': self._username, 'password': self._password})
        except:
            # bad network connection...
            logger.warning('Network error. Login failed. Please check your network connection.')
            return False

        if response.status_code == 200:

            response_str = response.content.decode()
            self.x_auth_token = json.loads(response_str)['x_auth_token']
            self.x_auth_token_expire = datetime.fromtimestamp(jwt.get_unverified_claims(self.x_auth_token)['exp'])
            logger.info('successful login, it expires ' + str(self.x_auth_token_expire) + ' expiring in ' +
                            str(round((self.x_auth_token_expire-datetime.now()).total_seconds()/60, 1)) + ' min')

            token_dic = {
                        'x_auth_token': self.x_auth_token,
                        'x_auth_token_expire': self.x_auth_token_expire
                        }

            json.dump(token_dic, open('token.tmp', 'w'), default=str)
            return self.x_auth_token

        else:
            logger.warning('_login() failed with code ' + str(response.status_code))
            try:
                logger.error('error message: ' + json.loads(response.content.decode())['error'])
            except json.decoder.JSONDecodeError:
                logger.error('server return no valid json')
            return False
    
    def _get_version(self):
        # this function returns the server api version and can also be used to check
        # if the internet connection is working and if the server responds anything
        try:
            response = requests.get(self._base_url + 'version')
        except:  # ok
            logger.warning('Network error.')
            return 'network_error'

        if response.status_code == 200:
            version_response = json.loads(response.content.decode())
            logger.debug(version_response)
            return version_response['version']
        else:
            logger.warning('Server error. Status code: {}'.format(response.status_code))
            return 'server_error'
    
    def projects(self):

        self._check_login()
        
        header = {'X-AUTH-TOKEN': self.x_auth_token, 'Content-Type': 'application/octet-stream'}
        response = requests.get(self.base_url + 'projects', headers=header)

        return self._request(response)
    
    def people(self, project_hash=None):

        self._check_login()
        
        header = {'X-AUTH-TOKEN': self.x_auth_token, 'Content-Type': 'application/octet-stream'}
        
        if project_hash:
            response = requests.get(self.base_url + 'project/' + project_hash + '/people', headers=header)
        else:
            response = requests.get(self.base_url + 'people', headers=header)

        return self._request(response)
    
    def data_files(self, person_hash=None, project_hash=None):

        self._check_login()
        
        header = {'X-AUTH-TOKEN': self.x_auth_token, 'Content-Type': 'application/octet-stream'}

        if person_hash:
            response = requests.get(self.base_url + 'person/'+ person_hash +'/data_files', headers=header)
        elif project_hash:
            response = requests.get(self.base_url + 'project/' + project_hash + '/data_files', headers=header)
        else:
            response = requests.get(self.base_url + 'data_files', headers=header)

        return self._request(response)
    
    def data_file_meta(self, hash_id):

        self._check_login()
        
        header = {'X-AUTH-TOKEN': self.x_auth_token, 'Content-Type': 'application/octet-stream'}

        response = requests.get(self.base_url + 'data_file_meta/'+ hash_id, headers=header)

        return self._request(response)
    
    def data_file_data(self, hash_id, data_type):

        self._check_login()
        
        header = {'X-AUTH-TOKEN': self.x_auth_token, 'Content-Type': 'application/octet-stream'}

        response = requests.get(self.base_url + 'data_file_data/'+ hash_id +'/' + data_type, headers=header)

        return self._request(response)

    def _request(self, response):
        # ToDo: put this DRY part in a wrapper function -> _request?
        if response.status_code == 200:
            response_str = response.content.decode()
            #print(response_str)
            response_result = json.loads(response_str)['result']
            #print(response_result)
            return response_result
        else:
            logger.error('failed with code ' + str(response.status_code))
            # try:
            #    logger.error('error message: ' + json.loads(response.content.decode())['error'])
            # except json.decoder.JSONDecodeError:
            #    logger.error('server return no valid json')
            return False

    def _check_login(self):

        # you are not logged in
        if not self.x_auth_token_expire:
            logger.debug('log in first.')
            self._login()
        elif self.x_auth_token_expire <= datetime.now():
            logger.debug('re-login after expired session.')
            self._login()
        else:
            logger.debug('use token ' + self.x_auth_token + ' expiring in ' +
                            str(round((self.x_auth_token_expire-datetime.now()).total_seconds()/60, 1)) + ' min')
