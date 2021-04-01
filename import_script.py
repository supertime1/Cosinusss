#!/usr/bin/env python3
from pathlib import Path
import os
import sys
file_path = Path(os.path.abspath(__file__))
sys.path.append(str(file_path.parents[1]))

from data_container import config, DataFile
from data_container.importer import Importer
# choose the local mongoDB database

config.init(db_name='sonova_data', logger_level='info')
# Show locally existing files
print('Files already imported')
for df in DataFile.objects(source='importer').all():
    print(' >> ', df.date_time_upload, ' >> ', df.import_file_name, '\n\t\t>> ', df)

# some global settings
import_path = Path('C:\\Users\\57lzhang.US04WW4008\\Downloads\\PPG\\7')

# import_path = Path('c:\\Users\\felix\\Downloads\\sonova')
project_hash = '0FU5'
username = 'sonova.fremont.api'
server = 'https://telecovid.earconnect.de'

imp = Importer(username=username, server=server)

# import sonova csv data
for filename in os.listdir(import_path):
    imp.import_file(import_path / Path(filename), project_hash=project_hash)