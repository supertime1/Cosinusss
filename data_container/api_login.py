import requests
import json
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from bson.objectid import ObjectId
import sys

from . import config
from jose import jwt
import redis
from getpass import getpass

r = redis.Redis(db=12)

API_VERS = 3
API_VERS_STR = 'api/v{}'.format(API_VERS)

# when does a token expire
# this is the buffer time, so that the token is not used anymore if it expires soon
TOKEN_EXP_BUFFER_TIME = 600

class APILogin():

    def __init__(self, server=None, username=None, password=None):

        # path to the json file storing the accounts
        self.json_path = config.data_path / Path('api_login.json')

        # account defined by server, user, pass
        self._server = None
        self.username = None
        self.password = None

        self._token = None
        self._token_expiration = None
        self.accounts = None
        self._accounts_load()

        # login if username defined
        if username:
            self.login(server=server, username=username, password=password)

        module = str(self.__class__).split("'")[1].split('.')[-2]
        class_name = str(self.__class__).split("'")[1].split('.')[-1]

        self.logger = config.logger

        self.logger.debug('this is an instance of ' + module + '.' + class_name + '()')
        self.logger.debug('call the method help() of this instance to get help')

        try:
            r.ping()
        except redis.exceptions.ConnectionError:
            self.logger.error('redis server not running: sudo systemctl start redis')
            sys.exit()

    @property
    def token(self):

        if not self.account and self.accounts:
            self._select_account()

        elif not self.account in self.accounts:
            return False

        token = self.accounts[self.account]['token']
        if token:
            token_exp = datetime.fromtimestamp(jwt.get_unverified_claims(token)['exp'])
            token_time = int((token_exp - datetime.now()).total_seconds()) - TOKEN_EXP_BUFFER_TIME
        else:
            token_time = 0

        # login if token is expired with a buffer of TOKEN_EXP_BUFFER_TIME sec
        #self.logger.debug('token expires in ' + str(token_time) + ' sec')
        if not token or token_time <= 0:
            self.login()
            if self.account in self.accounts:
                token = self.accounts[self.account]['token']
            else:
                token = False

        return token

    @property
    def base_url(self):
        return self.server + '/' + API_VERS_STR

    @property
    def account(self):

        if not self.server or not self.username:
            return None

        else:
            return self.username + '@' + self.server

    @property
    def accounts_list(self):
        return list(self.accounts)

    @account.setter
    def account(self, account):
        account_spl = account.split('@')
        self.username = account_spl[0]
        self.server = account_spl[1]

    @property
    def server(self):
        return self._server

    @server.setter
    def server(self, server):
        if not server.startswith('http'):
            if 'localhost' in server:
                server = 'http://' + server
            else:
                server = 'https://' + server
        if server.endswith('/'):
            server = server[:-1]
        result = self.request('version', use_token=False, server=server)
        if result:
            self.logger.debug(f'{server} {result}')
            self._server = server

    def login(self, username=None, password=None, server=None):

        if server is None and self.server is None:
            # set default server
            self._server = 'https://telecovid.earconnect.de'
        elif server:
            self.server = server

        if username:
            self.username = username

        if password:
            self.password = password

        # select account from stored accounts
        if not self.account and self.accounts:
            self._select_account()

        # enter new account data
        elif not self.account and not self.accounts:

            if not self.server:
                self.server = input('server: ')

            if not self.username:
                self.username = input('username: ')

        # try to get the password from redis
        redis_key = 'dc_api_login_password_' + self.account
        redis_password = r.get(redis_key)
        if redis_password:
            self.logger.debug('load password for ' + str(self.account) + ' from redis')
            self.password = redis_password

        # do the server login
        login_resp = self._login_server()
        if not login_resp:
            self.reset_redis_cache(self.account)
            self.password = None
            login_resp = self._login_server()

        # update account
        if self.account in self.accounts:

            self.accounts[self.account]['last_login'] = datetime.now()
            self.accounts[self.account]['last_login_resp'] = login_resp

        # add new account of the successful login
        elif login_resp:

            account = {
                        'server': self.server,
                        'username': self.username,
                        'last_login': datetime.now(),
                        'last_login_resp': True
                        }
            self.accounts[self.account] = account

        if login_resp:
            self.accounts[self.account]['token'] = self._token
            self.accounts[self.account]['token_expiration'] = self._token_expiration
            # store password in redis
            r.set(redis_key, self.password)

        # dump accounts to json
        self._accounts_dump()

    def reset_redis_cache(self, account=None):
        if account:
            base_key = 'dc_api_login_password_' + account
        else:
            base_key = 'dc_api_login_password_*'
        for key in r.scan_iter(base_key):
            # delete the key
            print('del redis key ' + key.decode())
            r.delete(key)

    def reset_token(self):

        if self.account:

            self.accounts[self.account]['token'] = None
            self.accounts[self.account]['token_expiration'] = None

        self._accounts_dump()

    def reset_tokens(self):

        for account in self.accounts:

            self.accounts[account]['token'] = None
            self.accounts[account]['token_expiration'] = None

        self._accounts_dump()

    def _select_account(self):

        select_dic = {}
        print('Available accounts:')
        for i, account in enumerate(self.accounts):
            print('\t' + str(i+1) + '\t' + account)
            select_dic[i+1] = account
        while True:
            selected = input('select account: ')
            try:
                selected_int = int(selected)
            except ValueError:
                selected_int = False
            if selected_int in select_dic:
                self.account = select_dic[selected_int]
                break
            print('Please slect a number in ' + str(list(select_dic)))

    def _login_server(self):

        self.logger.info('login: ' + str(self.username) + ' @ ' + str(self.server))
        if not self.password:
            self.password = getpass()

        resp = self.request('login', use_token=False, headers={'username': self.username, 'password': self.password})

        if resp:

            x_auth_token = resp
            x_auth_token_expiration = datetime.fromtimestamp(jwt.get_unverified_claims(x_auth_token)['exp'])
            self.logger.info(('successful login, it expires ' +
                         str(x_auth_token_expiration) + ' expiring in ' +
                   str(round((x_auth_token_expiration - datetime.now()).total_seconds() / 60, 1))) + ' min')

            self._token = x_auth_token
            self._token_expiration = x_auth_token_expiration

            return True

        else:
            self.logger.error('login failed')
            return False

    def request(self, url_path, use_token=True, headers=None,
                data=None, timeout=(3.05,27), log_time=False,
                session=None, attempts=1, server=None):
        '''
        performs a API request to the labserver

            parameters:
                url_path (str): the api request url
                use_token (bool): whether to use a valid token
                headers (dict): hand over the headers to be sent
                data (str, bin): data to post
                timeout (bool, tuple): the timeout of the request
                    It’s a good practice to set connect timeouts to slightly larger than a multiple of 3,
                    which is the default TCP packet retransmission window.
                log_time (bool): whether to log the request time
                session (threading instance): use threading.local() instance for the request
                attempts (int): how many attempts for the request
                server (str): the server of the request; normally it's None, self.server is used instead

            returns:
                bool: True if successful
        '''

        time_start = time.time()
        if use_token and self.server is None:
            self.login()
        if server:
            url = server + '/' + API_VERS_STR + '/' + url_path
        elif self.server is None and use_token is False:
            self.logger.error('server is None. Maybe the server is down?')
            return False
        else:
            url = self.base_url + '/' + url_path
        self.logger.debug(url)

        attempts_max = attempts
        while attempts > 0:
            attempts -= 1

            try:
                # if session use this threading instance for the request
                if session:
                    req = session
                else:
                    req = requests

                # the default
                if use_token:
                    if not self.account or not self.account in self.accounts:
                        self.login()
                    if self.token:
                        if data:
                            response = req.post(url, data=data, headers={'X-AUTH-TOKEN': self.token}, timeout=timeout)
                        else:
                            response = req.get(url, headers={'X-AUTH-TOKEN': self.token}, timeout=timeout)
                    else:
                        self.logger.error(url + ' request failed, no valid token')
                        return False
                # actually used for the login
                elif headers:
                    response = req.get(url, headers=headers, timeout=timeout)
                # for requests without auth
                else:
                    response = req.get(url, timeout=timeout)
                response_str = response.content.decode()

            # all exceptions from the module requests inherit from requests.exceptions.RequestException
            except requests.exceptions.RequestException as e:
                self.logger.error(url + ' failed with RequestException: ' + str(e) + ' attempts left: ' + str(attempts))
                return False

        if response.status_code == 200:
            if log_time:
                req_time = round(time.time()-time_start, 1)
                self.logger.debug('{} time elapsed {} s, attempts: {}'.format(url, req_time, attempts_max-attempts))
            resp = json.loads(response_str)
            #print(resp)

            if 'x_auth_token' in resp:
                return resp['x_auth_token']
            elif 'error' in resp:
                self.logger.warning(resp['error'])
                return False
            elif 'data' in resp:
                return resp['data']
            else:
                self.logger.error(url + ' server response unknown: ' + str(resp))
                return False

        elif response.status_code == 500:
            self.logger.error(url + ' failed with http code ' +
                           str(response.status_code) + ', server error.')
            return False

        elif response.status_code == 401:
            try:
                error = json.loads(response_str)['error']
            except Exception as e:
                self.logger.error('Unknown error: ' + str(e))
                error = 'unknown'
            if 'x_auth_token invalid' in error:
                self.logger.info('reset token of account: ' + str(self.account))
                self.reset_token()
            self.logger.warning(url + ' failed with http code ' +
                           str(response.status_code) + ', access denied. Error: ' +
                           error)
            return False

        elif response.status_code == 403:
            try:
                error = json.loads(response_str)['error']
            except Exception as e:
                self.logger.error('Unknown error: ' + str(e))
                error = 'unknown'
            self.logger.warning(url + ' failed with http code ' +
                           str(response.status_code) + ', access denied. Error: ' +
                           error)
            return False

        elif response.status_code == 404:
            self.logger.error(url + ' failed with http code ' +
                           str(response.status_code) + ', route not found.')
            return False

        else:
            self.logger.error(url + ' failed with http code ' +
                           str(response.status_code) + ', response ' +
                           str(response_str))
            return False

    def _accounts_load(self):

        try:
            self.accounts = json.load(open(str(self.json_path)))
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            self.accounts = {}

    def _accounts_dump(self):

        json.dump(self.accounts, open(str(self.json_path), 'w'), indent=4, default=str)

