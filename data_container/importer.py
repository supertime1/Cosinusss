from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pytz
import hashlib
import os
import pandas as pd
import ggps
import sys
from io import StringIO
import re
import shutil

# importing cosinuss repo
from .api_db_sync import DBSync
from . import DataFile, DcHelper
from .odm import *
from . import config

logger = config.logger

class Importer(DBSync):

    def __init__(self, *args, **kwargs):

        super(DBSync, self).__init__(*args, **kwargs)

        # set file attributes
        self.file_name = None
        self.file_path = None
        self.file_path_dir = None
        self.file_content = None
        self.file_md5 = None

        # set project attr
        self.project = None
        self.time_zone = None
        self.person = None
        self.people_hash_list = None
        self.df = None

        self.server_upload = True
        self.dry_run = False
        self.db_sync()

    def import_file(self, file_path, date_time_start=None, person_hash=None, project_hash=None,
                    server_upload=True, dry_run=False):

        # skip folders
        if os.path.isdir(str(file_path)):
            return False

        if type(file_path) == str:
            file_path = Path(file_path)
        file_name = file_path.name

        if dry_run:
            self.dry_run = dry_run
            logger.warning('this is a dry run for "' + file_name)

        # skip other non-csv/tcx files
        if file_path.suffix not in ['.csv', '.tcx']:
            logger.warning('no importer defined for this file "' + file_name)
            return False

        self.server_upload = server_upload

        try:
            with open(str(file_path)) as fp:
                file_content = fp.read()
        except FileNotFoundError:
            logger.error('FileNotFoundError: ' + str(file_path))
            return False
        file_md5 = hashlib.md5(file_content.encode('utf8')).hexdigest()

        # check for existing imports in local db
        df = DataFile.objects(import_md5=file_md5).first()
        if df:
            if df.status_closed:
                logger.debug('skip ' + file_name + ': same MD5 ' + file_md5 + ' exists in local db:\n\t' + str(df))
                # upload file if not already uploaded
                if not df.date_time_upload:
                    logger.debug('upload ' + file_name + ' to server:\n\t' + str(df))
                    df.api_client = self
                    df.send()
                    df.store()
                return False
            # it might be a df in an undefined state
            else:
                logger.warning(file_name + ': same MD5 ' + file_md5 + ' exists in local db, but maybe it is in undefined state:\n\t' + str(df))
                self.__confirm('Delete this df (' + df._hash_id + ') to be able to import it again?')
                shutil.rmtree(df.path)
                df.delete()

        # check for existing imports in server db
        df_hash = self.check_import_md5(file_md5)['df_hash']
        if df_hash:
            df_meta = self.data_file_meta(df_hash)
            logger.info('skip ' + file_name + ': same MD5 ' + file_md5 + ' exists in server db:\n\t' + str(df_meta))
            return False

        # try to guess the file type
        file_type = self.__guess_file_type(file_content, file_name)
        if not file_type:
            return False
        elif file_type == 'sonova_step_csv':
            logger.debug(f'skipping {file_name}: it is a steps counter file which is imported with the corresponding app csv.')
            return False

        print('')
        print(file_name)
        print(len(file_name)*'=')

        if person_hash:
            # all fine nothing special to do here
            pass

        elif project_hash:
            project = Project.objects(_hash_id=project_hash).first()
            if not project:
                logger.error('project not found: ' + str(project_hash))
                return False
            people_hash_list = []
            for person in Person.objects(project=project).all():
                if person.label:
                    person_str = person._hash_id + ' (' + person.label + ')'
                else:
                    person_str = person._hash_id
                people_hash_list.append(person_str)
            if not people_hash_list:
                logger.error('no persons found in project: ' + str(project_hash))
                return False

            # select the person
            person_str = self.__selector(people_hash_list, file_name, 'person')
            person_hash = person_str.split(' ')[0]

        else:
            logger.error('person_hash or project_hash is needed!')
            return False

        # query person and project
        person = Person.objects(_hash_id=person_hash).first()
        if not person:
            logger.error('person not found: ' + str(person_hash))
            return False
        project = Project.objects(_hash_id=person.project._hash_id).first()

        if date_time_start:
            date_time_start = DcHelper.datetime_validation(date_time_start, project.timezone)
            if date_time_start is None:
                logger.error('no valid datetime')
                return False

        # set file attributes
        self.file_name = file_name
        self.file_path = file_path
        self.file_path_dir = file_path.parent
        self.file_content = file_content
        self.file_md5 = file_md5

        # set project attr
        self.project = project
        self.time_zone = pytz.timezone(project.timezone)
        self.person = person

        self.df = DataFile()
        self.df.api_client = self

        if file_type == 'sonova_app_csv':
            self.__import_sonova_app_csv()
        elif file_type == 'polar_csv':
            self.__import_polar_csv()
        elif file_type == 'garmin_tcx':
            self.__import_garmin_tcx()
        else:
            return False

    def __confirm(self, message):

        print('')
        print('----------------------------------------')
        print(message)
        print('----------------------------------------')

        while True:
            sel = input('Enter your choice (y/n): ')
            if sel.lower() == 'y':
                break
            elif sel.lower() == 'n':
                sys.exit()
            else:
                print('Please select from ' + str(['y', 'n']))

    def __selector(self, select_list, file_name, select_name):

        select_dic = {}

        print('')
        print('----------------------------------------')
        print('select a ' + select_name + ' for ' + file_name + ':')
        for i, select_value in enumerate(select_list):
            print('\t', i+1, select_value)
            select_dic[i+1] = select_value
        print('----------------------------------------')

        while True:
            sel = input('Enter your choice (cancel with c): ')
            if sel == 'c':
                sys.exit()
            try:
                sel = int(sel)
            except ValueError:
                sel = None
            if sel in list(select_dic):
                select_value = select_dic[sel]
                break
            else:
                print('Please select an integer from ' + str(list(select_dic)) + ' or cancel with c')

        print('')
        print('You selected ' + select_value + ' for ' + file_name)
        print('')

        return select_value

    def __guess_file_type(self, file_content, file_name):

        # todo polar: ersten paar zeichen müssen charakteristisch sein

        first_line = file_content.split('\n')[0]
        file_type = None
        file_type_mess = None

        # check if it's a garmin tcx
        if file_name.endswith('.tcx'):
            first_line.startswith('<?xml version')
            file_type = 'garmin_tcx'
            file_type_mess = 'tcx from garmin'

        # check if it's a sonova app or polar csv
        elif file_name.endswith('.csv'):

            # polar
            file_type = 'polar_csv'
            file_type_mess = 'csv from polar'
            for word in ['Timestamp', 'heartRate', 'RRI']:
                if word not in first_line:
                    file_type = None
                    file_type_mess = None
                    break

            # sonova app
            if not file_type:
                file_type = 'sonova_app_csv'
                file_type_mess = 'csv from sonova app'
                for word in ['Timestamp', 'Estimated', 'sample', 'alsVis', 'alsIr', 'ps1', 'ppgValid']:
                    if word not in first_line:
                        file_type = None
                        file_type_mess = None
                        break

            # sonova app steps counter
            if not file_type:
                file_type = 'sonova_step_csv'
                file_type_mess = 'csv from sonova app stepcounter'
                for word in ['Time of day', 'Steps', 'Activity']:
                    if word not in first_line:
                        file_type = None
                        file_type_mess = None
                        break

        if file_type:
            logger.debug('the file "' + file_name + '" is a ' + file_type_mess)
            return file_type
        else:
            logger.warning('no importer defined for this file "' + file_name)
            return False

    def __import_pre(self):

        # set attr of df
        self.df.project = self.project
        self.df.person = self.person
        self.df.date_time_start = self.date_time_start
        self.df.source = 'importer'
        self.df.import_md5 = self.file_md5
        self.df.import_file_name = self.file_name

    def __import_post(self):

        self.df.chunk_stop()
        if self.dry_run == True:
            self.df.close(send=False)
        else:
            self.df.close(send=self.server_upload)

        # Show an overview
        print('')
        print('file successfully progressed:')
        print(self.df)
        print('Stats:')
        print(self.df.stats)
        print(self.df.stats_chunks)
        print('')

        if self.dry_run == True:
            # delete df in db again
            logger.warning('delete df (' + self.df._hash_id + ') again, it has been a dry run!')
            shutil.rmtree(self.df.path)
            self.df.delete()

    def __import_garmin_tcx(self):

        garmin_handler = ggps.TcxHandler()

        logger.warning('Fix tcx file: missing AltitudeMeters')
        tmp_fix_path = str(self.file_path)+'_tmp_fix'
        with open(tmp_fix_path, 'w') as fp:
            for line in self.file_content.split('\n'):
                #print(line)
                if not 'AltitudeMeters' in line:
                    fp.write(line + '\n')
                if '<Trackpoint>' in line:
                    fp.write('            <AltitudeMeters>0.0</AltitudeMeters>' + '\n')
        garmin_handler.parse(tmp_fix_path)

        for i, d_point in enumerate(garmin_handler.trackpoints):

            #print(d_point.values)
            time_stamp_str = d_point.values['time'].split('.')[0]
            time_stamp = DcHelper.datetime_validation(time_stamp_str, 'utc')

            # get date_time_start
            if i == 0:
                self.date_time_start = time_stamp
                # prepare import
                self.__import_pre()

            # add data point
            x = (time_stamp-self.date_time_start).total_seconds()
            if not 'heartratebpm' in d_point.values:
                logger.warning('missing value heartratebpm for x=' + str(x))
                continue
            self.df.append_value('heart_rate', int(d_point.values['heartratebpm']), x)

        # finish import
        self.df.device_model = 'garmin'
        self.__import_post()

        os.remove(tmp_fix_path)

    def __select_sonova_app_csv_steps_counter(self):

        files_dict = {}
        file_list = os.listdir(self.file_path_dir)
        for file_name in file_list:
            if 'stepcounter' in file_name.lower() and file_name.endswith('.csv'):
                date_time_start_str = re.search('[0-9]{8}_[0-9]{6}', file_name)[0]
                date_time_start = DcHelper.datetime_validation(date_time_start_str, self.project.timezone)
                delta_time = abs((self.date_time_start-date_time_start).total_seconds())
                #print(file_name, date_time_start_str, delta_time)
                if delta_time < 600:
                    #print(file_name, date_time_start_str, delta_time)
                    files_dict[delta_time] = file_name

        files_list = []
        for key in sorted(list(files_dict)):
            files_list.append(files_dict[key])

        if not files_list:
            self.__confirm('There is no appropriate steps counter csv file. Import anyway?')
            return None
        if len(files_list) == 1:
            file_name = files_list[0]
        else:
            file_name = self.__selector(files_list, self.file_name, 'steps counter csv file')

        file_path = self.file_path_dir / Path(file_name)
        with open(file_path) as fp:
            file_content = fp.read()
        for sep in ['\t', ',', ';']:
            if sep in file_content[:100]:
                break

        return pd.read_csv(file_path, index_col=None, header=0, sep=sep)

    def __import_polar_csv(self):

        # select device type
        device_type = 'polar'

        # get date_time_start
        date_time_start_str = re.search('[0-9]{8}_[0-9]{6}', self.file_name)[0]
        if date_time_start_str is None:
            logger.error('date time in file name not found: ' + str(self.file_name))
            return None
        self.date_time_start = DcHelper.datetime_validation(date_time_start_str, self.project.timezone)

        # prepare import
        self.__import_pre()

        for sep in ['\t', ',', ';']:
            if sep in self.file_content[:100]:
                break
                
        pd_df = pd.read_csv(StringIO(self.file_content), index_col=None, header=0, sep=sep)
        if self.dry_run:
            print(pd_df)

        time_start = datetime.strptime(pd_df['Timestamp[HH:mm:ss.MM]'][0], '%H:%M:%S.%f')

        # loop pandas data frame
        for i, row in pd_df.iterrows():
            # time samples to seconds
            time_str = row['Timestamp[HH:mm:ss.MM]']
            time_x = datetime.strptime(time_str, "%H:%M:%S.%f")
            x = (time_x - time_start).total_seconds()

            # heart rate
            hr_row = row['heartRate']
            if type(hr_row) is str:
                # might be ' 80 ' or even just empty spaces '  '
                hr_row = hr_row.strip()
                if hr_row:
                    self.df.append_value('heart_rate', int(hr_row), x)
            else:
                if not np.isnan(hr_row):
                    self.df.append_value('heart_rate', hr_row, x)

            # rri
            rri_row = str(row['RRI'])
            if type(rri_row) is str:
                # sometimes there are multiple rr-intervals separated by dash '785-725'
                rri_row = rri_row.split('-')
            elif type(rri_row) in [float, int] and not np.isnan(rri_row):
                rri_row = [rri_row]

            for rri in rri_row:
                if rri != 'nan':
                    self.df.append_value('rr_int', int(rri), x)

        # finish import
        self.df.device_model = device_type
        self.__import_post()

    def __import_sonova_app_csv(self):

        # select device type
        device_type = self.__selector(['biometRIC', 'Cshell'], self.file_name, 'device type')

        # get date_time_start
        date_time_start_str = re.search('[0-9]{8}_[0-9]{6}', self.file_name)[0]
        if date_time_start_str is None:
            logger.error('date time in file name not found: ' + str(self.file_name))
            return None
        self.date_time_start = DcHelper.datetime_validation(date_time_start_str, self.project.timezone)

        # prepare import
        self.__import_pre()

        # select steps counter csv file
        pd_df_steps = self.__select_sonova_app_csv_steps_counter()

        if self.dry_run:
            print(pd_df_steps)

        for sep in ['\t', ',', ';']:
            if sep in self.file_content[:100]:
                break
        pd_df = pd.read_csv(StringIO(self.file_content), index_col=None, header=0, sep=sep)
        pd_df_len = len(pd_df)

        if self.dry_run:
            print(pd_df)

        # loop pandas data frame
        steps_data = []
        for i, row in pd_df_steps.iterrows():

            # time samples
            time_str = row['Time of day [HH:mm:ss.MM]']
            time_x = datetime.strptime(time_str, "%H:%M:%S.%f")
            steps_count = row['Steps']
            steps_data.append({'time': time_x, 'steps': steps_count})

        # add combined cols
        self.df.add_combined_columns(['ble_packet_counter', 'ble_sample_counter', 'ble_sample_amount'], 'ble_package')
        self.df.add_combined_columns(['acc_x', 'acc_y', 'acc_z'], 'accelerometer')
        self.df.add_combined_columns(['ppg_ambient', 'ppg_ir', 'ppg_ir_2', 'ppg_ir_3'], 'ppg')

        time_str_prev = None
        package_count = 0
        sample_amount = 0
        sample_count = 0
        activity_label_pre = None
        label_date_time_start = None
        time_start = datetime.strptime(pd_df['Estimated sample time [HH:mm:ss.MM]'][0], '%H:%M:%S.%f')

        # loop pandas data frame
        k = 0
        for i, row in pd_df.iterrows():

            # time samples
            time_str = row['Estimated sample time [HH:mm:ss.MM]']
            time_x = datetime.strptime(time_str, "%H:%M:%S.%f")
            # transform it to seconds
            x = (time_x - time_start).total_seconds()

            # get data from steps counter
            while True:
                if steps_data[k]['time'] <= time_x:
                    x_steps = (steps_data[k]['time'] - time_start).total_seconds()
                    self.df.append_value('steps_counter', steps_data[k]['steps'], x_steps)
                    k += 1
                else:
                    break

            acc_x = self.__to_g_force(row['x'])
            acc_y = self.__to_g_force(row['y'])
            acc_z = self.__to_g_force(row['z'])

            # add data points
            self.df.append_value('temperature', row['Temperature'], x)
            self.df.append_value('accelerometer', [acc_x, acc_y, acc_z], x)
            self.df.append_value('ppg', [row['alsIr'], row['ps1'], row['ps2'], row['ps3']], x)

            # time ble package
            time_str = row['Timestamp of Read Request [HH:mm:ss.MM]']

            # activity
            activity_label = row['Activity']
            # whenever there is a change of the activity, or for the very last row
            if activity_label != activity_label_pre or i+1 == pd_df_len:
                label_date_time_end = self.df.date_time_start + timedelta(seconds=x)
                #if type(activity_label) is str:
                #    print('')
                #    print(i, label_date_time_start, label_date_time_end, activity_label, activity_label_pre)
                #    print('')
                # define the chunk_label
                if label_date_time_start:
                    #print(activity_label_pre, label_date_time_start, label_date_time_end)
                    self.df.add_labelled_chunk(activity_label_pre, label_date_time_start, label_date_time_end)
                if type(activity_label) is str:
                    label_date_time_start = self.df.date_time_start + timedelta(seconds=x)
                else:
                    label_date_time_start = None
                activity_label_pre = activity_label

            # it's a new ble package
            if time_str_prev != time_str:
                # it's the very first iteration
                if time_str_prev is None:
                    time_str_prev = time_str
                else:
                    package_count += 1
                    # time_x = datetime.strptime(time_str, "%H:%M:%S.%f")
                    time_x = datetime.strptime(time_str_prev, "%H:%M:%S.%f")
                    x = (time_x - time_start).total_seconds()
                    self.df.append_value('ble_package', [package_count, sample_count, sample_amount], x)
                    sample_amount = 0
                    time_str_prev = time_str

            # count samples
            sample_amount += 1
            sample_count += 1

            # the very last iteration
            if i == pd_df_len-1:
                package_count += 1
                self.df.append_value('ble_package', [package_count, sample_count, sample_amount], x)

            if i % 50 == 0:
                perc = int(100*i/float(pd_df_len))
                bar = (int(perc/3)*'#') + '>' + ((33-int(perc/3))*' ')
                print(f'{bar} | {perc}% ({i}/{pd_df_len}) lines processed', end='\r')

        # finish import
        self.df.device_model = device_type
        self.__import_post()

    def __to_g_force(self, y, bits=16, max_g=8):
        # max g can be 2g, 4g or 8g

        # maximum positive and negative amplitude
        max_y = 2**(bits-1)

        # Dreisatz
        #     2**(bits-1) ~ max_g
        # <=> x           ~ max_g / (2**(bits-1)) * x

        y_new = max_g / max_y * y

        return y_new

