from data_container import config, DataFile
from data_container.api_db_sync import DBSync
from utils import util
import os

# configure csv file saving path
dir_path = os.path.dirname(os.path.realpath(__file__))
save_path = os.path.join(dir_path, "csv")

# try to create a csv folder in the working diretory, pass if
# a csv folder has already exist
try:
    os.mkdir(save_path)
except:
    pass

print(f'csv file will be saved in {save_path} \n')

# configure api
db_name = input('database name(e.g. sonova_analysis):')
username = input('username(e.g. sonova.fremont.api):')
prj_hash_id = input('project hash id(e.g. M9KH):')
update_local = False
while True:
    print('Update data in local host:\n0. False \n1. True')
    update_local_option = input()
    if update_local_option == '0':
        update_local = False
        break
    elif update_local_option == '1':
        update_local = Trueb
        break
    else:
        print('Choose 0 or 1')

print('variable set!')
api = util.configure_api(db_name=db_name,
                         username=username,
                         prj_hash_id=prj_hash_id,
                         update_local=update_local
                         )

print('API configured successfully!')

# filter database by date
date = input('Earliest date that data was collected(e.g. 2020-12-31):')
table = util.generate_table(date, api)

print('Table is generated')

# Correct the label of 'cShell' and 'Biometric',
# in case they were mistakenly entered by Cosinuss
correct_table = table
while True:
    print('Correct Floyer device label?:\n0. No \n1. Yes')
    correct_label_opt = input()
    if correct_label_opt == '0':
        correct_label = False
        break
    elif correct_label_opt == '1':
        correct_label = True
        break
    else:
        print('Choose 0 or 1')

if correct_label:
    correct_table = util.correct_label(table, api)
    print('Correcting the label')

# find the row indices of paired floyer-polar device

floyer_device = input("Floyer device for exporting ? (Type either 'cshell' or 'biometric'):")
paired_idx_lst = util.find_pairs_row_index(correct_table, floyer_device)

# save paired csv files
util.paired_csv(correct_table=correct_table,
                floyer_device=floyer_device,
                api=api,
                paired_idx_lst=paired_idx_lst,
                file_path=save_path
                )
