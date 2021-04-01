import zipfile
import os
from pathlib import Path

from time import time

from .odm import Person, Project
from .api_db_sync import DBSync
from . import DataFile
from . import config

class Data(DBSync):

    '''
    you can group several DataFile into Data
    for plotting, analysis, etc.
    take features from Biosignal()
    '''

    def __init__(self, *args, **kwargs):

        super(DBSync, self).__init__(*args, **kwargs)

        # results from query() end up here
        self.query_df_list = []
        self.query_people_list = []
        self.server_df_list = []
        self.person_df_list = []
        self.logger = config.logger

    def query_dfs(self,
                  person=None,
                  project=None,
                  min_date_time_start=None,
                  max_date_time_start=None,
                  min_duration=None,
                  max_duration=None,
                  download_dfs=False,
                  order_by=None
                  ):
        # person and project: either the hash or the db item to be passed here.
        # download_dfs: tries to completely download all dfs which were returned from this query
        #   this is useful if you have only downloaded the meta information of the datafiles
        # duration: queries for the netto duration!
        # order_by: string or list of strings with the field name with + or -: '+_date_time_start', '-duration'
        #   https://docs.mongoengine.org/guide/querying.html#sorting-ordering-results

        # empty the query list
        self.query_df_list = []

        query_dict = {
            '_date_time_start__gt': min_date_time_start,
            '_date_time_start__lt': max_date_time_start,
            'person': person,
            'project': project,
            'duration_netto_meta__gt': min_duration,
            'duration_netto_meta__lt': max_duration,
        }

        # remove all unnecessary queries
        for key in list(query_dict):
            if query_dict[key] is None:
                query_dict.pop(key)

        # if nothing is specified, then return all dfs
        self.query_df_list = DataFile.objects(**query_dict) if query_dict else DataFile.objects()
        if order_by:
            if type(order_by) is str:
                self.query_df_list = self.query_df_list.order_by(order_by)
            elif type(order_by) in [list, tuple]:
                self.query_df_list = self.query_df_list.order_by(*order_by)

        if self.query_df_list.count():
            self.logger.info(f'Query {query_dict} found {self.query_df_list.count()} data file(s)')
        else:
            self.logger.info(f'Query returned no results.')

        if download_dfs:
            self.pull_dfs_in_query_df_list()

    def query_people(self, project=None):

        self.query_people_list = Person.objects(project=project) if project else Person.objects()

        return self.query_people_list

    def pull_dfs_in_query_df_list(self):
        # download all data files in the query list

        for df in self.query_df_list:
            self.pull_df(df.hash_id)

    def plot(self):
        '''
        nice plotting with formatting
        also complex subplots
        format colors...
        '''
        pass

    def set_df(self, df_hash):

        try:
            self.df = DataFile(df_hash)
        except FileNotFoundError:
            self.logger.warning(f'The specified data file {df_hash} was not found.')

    def set_person(self, person_hash=None):
        # set the person that you want to get data from
        self.person = person_hash

    def x(self, data_type):
        # get all x values of this data_type for the specified person and time range
        x = []

        # iterate through all data files of this person
        for df in self.person_data_file_list:
            # get all matching x values in the current data file
            current_x = df._cols[data_type].x(self.start, self.stop)
            # alternatively
            # current_x = getattr(df.c, data_type).x(self.start, self.stop)

            x.append(current_x)

        return x

    def __slicing(self):
        '''
        for all actions here we need a priate slciing method,
            which handles: start, end, offsets of timestamps, selection of slices, loading, ...
        '''
        pass

    def bland_altman(self):
        pass

    def bland_altman_plot(self):
        pass

    def basic_filter_ect(self):
        pass

    def resampling(self):
        # interpolation
        pass

    def analyse(self):
        pass

    def analyse_deaviations(self):
        pass

    def export_csv(self,
                   dir_path=None,
                   data_types=None,
                   csv_file_name=None,
                   zip_to_file_name=None,
                   meta_header=False,
                   separator=',',
                   digits=3,
                   compress_level=4,
                   allow_to_free_ram=False,
                   ):
        # zip_to_file_name: if a name is provided, then all data will be zipped into one or more zip files with maximum 4 GB of content.
        #  otherwise the data is just stored as csv.
        # allow_to_free_ram: set to True when exporting many data files to avoid RAM overflow
        # for the description of the other parameters, see data_column.export_csv() method

        if not os.path.exists(dir_path):
            self.logger.error(f'The selected path {dir_path} does not exist.')
            return False

        if separator not in [' ', '\t', ',', ';']:
            self.logger.error(f"The specified separator '{separator}' is not possible, choose from: [' ', '\\t' and ',']")
            return False

        if data_types and type(data_types) not in [list, tuple]:
            self.logger.error(f"data_types_only: must be either list or tuple!")
            return False

        compress = True if zip_to_file_name else False
        dir_path = Path(dir_path) if type(dir_path) is str else dir_path
        self.logger.info(f'Starting to export {len(self.query_df_list)} data files as csv (compress={compress}).')

        if compress:
            comp = zipfile.ZIP_DEFLATED
            zip_files = 0
            file_name_counter = ''
            zip_file = zipfile.ZipFile(dir_path / f'{zip_to_file_name}.zip', 'w', compression=comp, compresslevel=compress_level, allowZip64=False)
            for df in self.query_df_list:

                try:
                    df_csv = df.export_csv(
                        dir_path=None,
                        file_name=csv_file_name,
                        meta_header=meta_header,
                        data_types=data_types,
                        separator=separator,
                        digits=digits,
                    )
                    for data_type in df_csv:

                        try:
                            zip_file.writestr(df_csv[data_type]['file_name'], df_csv[data_type]['csv'])

                        # split zip files after 4 GiB of size
                        except zipfile.LargeZipFile:
                            zip_file.close()
                            os.rename(dir_path /f'{zip_to_file_name}.zip', dir_path / f'{zip_to_file_name}_{zip_files}.zip')
                            zip_files += 1
                            file_name_counter = f'_{zip_files}'
                            self.logger.info(f'Created zip file {zip_to_file_name}_{zip_files}.zip. Now starting new zip file...')

                            zip_file = zipfile.ZipFile(dir_path / f'{zip_to_file_name}{file_name_counter}.zip', 'w', compression=comp, compresslevel=compress_level, allowZip64=False)
                            zip_file.writestr(df_csv[data_type]['file_name'], df_csv[data_type]['csv'])

                    if allow_to_free_ram:
                        df.free_memory()

                except Exception as e:
                    self.logger.error(str(e))
                    self.logger.error(f'CSV Export error with df {df.hash_id}')
                    with open(dir_path/'failed_dfs.txt', 'a') as f:
                        f.write(f'{df.hash_id}\n')

            zip_file.close()
            self.logger.info(f'Created zip file {zip_to_file_name}{file_name_counter}.zip.')

        # just as a bunch of csv_files in the specified folder
        else:
            for df in self.query_df_list:
                df.export_csv(
                    dir_path=dir_path,
                    file_name=csv_file_name,
                    meta_header=meta_header,
                    data_types=data_types,
                    separator=separator,
                    digits=digits,
                )

                if allow_to_free_ram:
                    df.free_memory()

            self.logger.info('All data files exported to csv.')

    def csv_export(self):
        # interpolation
        pass