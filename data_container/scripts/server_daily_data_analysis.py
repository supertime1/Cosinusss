#!/usr/bin/env python3
import sys
import os
import numpy as np
import pickle
import json
from pathlib import Path
from datetime import datetime, date, timedelta
from copy import deepcopy
from statistics import median
from terminaltables import AsciiTable
from mongoengine import connect
import matplotlib as mpl
mpl.rcParams['agg.path.chunksize'] = 10000  # against error Warning: Source ID 85 was not found when attempting to remove it GLib.source_remove(self._idle_draw_id)
from matplotlib import pyplot as plt
###########################################################################################################
file_path = Path(os.path.dirname(os.path.realpath(__file__)))
if file_path != "":
    os.chdir(file_path)
sys.path.append(str(file_path.parents[1]))
from data_container.dc_config import config
logger = config.logger
from data_container import DataFile
from data_container.scripts.analysis_helper import determine_bin_size
from data_container.db_sync import DB_Sync
SLICE_MAX_SICE = config.SLICE_MAX_SIZE
###########################################################################################################
###########################################################################################################
###########################################################################################################

ERROR_DICT = {}
ERROR_DICT['ppg_low'] = ''  # values of red/ir < 1000
ERROR_DICT['ppg_0'] = ''  # when trying to add negative uint24 values
ERROR_DICT['ppg_2**24'] = ''  # when trying to add uint24 bigger than 2**24-1
ERROR_DICT['bin_size_div_3'] = ''  # bin size not dividable by 3
ERROR_DICT['bin_size_property'] = ''  # bin_size not correct
ERROR_DICT['xy_length'] = ''  # different lengths
ERROR_DICT['xy_0'] = ''  # there is a x=0, y=0
ERROR_DICT['time_jump'] = ''  # x values not all in rising order
ERROR_DICT['slice_overshoot'] = ''  # > SLICE_MAXSIZE
ERROR_DICT['data_type'] = ''  # type does not exist
ERROR_DICT['no_values'] = ''  # different lengths
ERROR_DICT['temp_low'] = ''  # temperature too lw
ERROR_DICT['time_identical'] = ''  # x values not all in rising order
ERROR_DICT['slices_not_downloaded'] = ''  # slice not found
# ERROR_DICT['battery_const'] = ''  # battery does not change


def get_all_dates_sorted(analyse_dict):
    # collect all available dates and sort them

    date_time_list = []

    for df_dict in analyse_dict['analyzed'].values():
        df = DataFile.from_json(df_dict['meta'])
        current_date = df.date_time_start.date()
        if current_date not in date_time_list:
            date_time_list.append(current_date)

    date_time_list.sort()

    return date_time_list


def analyze_dict(analyse_dict=None):
    global STORAGE_FOLDER

    if not analyse_dict:
        analyse_dict = pickle.load(open(STORAGE_FOLDER + 'analyse_dict.pkl', 'rb'))

    # collect all available dates and sort them
    date_time_list = get_all_dates_sorted(analyse_dict)

    table_data = []

    separator_row = [''] * int(len(ERROR_DICT) + 2)

    row = []
    # '\033[1m' will write the following text *BOLD*
    row.append('\033[1m' + 'Date')
    row.append('Person')
    row.append('df_hash')
    
    # don't display some keys in the final table
    exclude_errors = ['temp_low', ]
    for key in exclude_errors:
        ERROR_DICT.pop(key)
    
    for error_key in ERROR_DICT:
        row.append(error_key)

    table_data.append(row)

    errors_day = {}
    errors_df = {}

    for day in date_time_list:

        # reset the error counts for each error on this day
        for error_key in ERROR_DICT:
            errors_day[error_key] = 0

        new_day = True
        dfs_this_day_total = 0
        dfs_this_day_with_errors = 0

        for df_hash, df_dict in analyse_dict['analyzed'].items():
            df = DataFile.from_json(df_dict['meta'])
            df_date = df.date_time_start.date()
            if df_date == day:

                dfs_this_day_total +=1

                for error_key in ERROR_DICT:
                    errors_df[error_key] = 0

                # count errors for each df
                for data_type, data_type_dict in df_dict['data_types'].items():

                    for error_key in ERROR_DICT:
                        if error_key in data_type_dict['errors']:
                            error = data_type_dict['errors'][error_key]
                            if error:
                                errors_df[error_key] += 1
                        else:
                            print(f'error key {error_key} not found for {data_type} in {df_dict}!')

                df_err_list = []
                for error_key in ERROR_DICT:
                    # count the errors of each df to the current day errors
                    errors_day[error_key] += errors_df[error_key]

                    # add all items to sum over the errors
                    df_err_list.append(errors_df[error_key])

                    # replace 0 with '-'
                    if errors_df[error_key] == 0:
                        errors_df[error_key] = '-'

                err_sum = sum(df_err_list)

                row = []
                if new_day:
                    row.append('\033[1m' + str(day) + '\033[0m')
                    row.extend(['-'*9])
                    row.extend(['-'*13])
                    for error_key in ERROR_DICT:
                        row.extend(['-'*len(error_key)])
                    table_data.append(row)
                else:
                    row.append('')

                if err_sum != 0:

                    dfs_this_day_with_errors += 1

                    row = []
                    row.append('')

                    row.append(df.person.hash_id)
                    row.append(df_hash)

                    for error_key in ERROR_DICT:
                        row.append(errors_df[error_key])
                    table_data.append(row)

                new_day = False

        # day summary
        # table_data.append(separator_row)
        row = []
        row.append('')
        row.append('\033[1m' + 'TOTAL:')
        row.append('err/tot:  ' + str(dfs_this_day_with_errors) + '/' + str(dfs_this_day_total) + '\033[0m')

        for error_key in ERROR_DICT:
            row.append('\033[1m' + str(errors_day[error_key]) + '\033[0m')

        table_data.append(row)
        table_data.append(separator_row)

        new_day = True

    full_table = AsciiTable(table_data).table
    print(full_table)

    # remove bold print (works only in console)
    full_table = full_table.replace("\033[1m", "")
    full_table = full_table.replace("\033[0m", "")
    with open('df_analysis/results.txt', 'w') as f:
        f.write(full_table)


def analyse_data_type(df, data_type):
    global ERROR_DICT

    values_dict = {}
    error_dict = deepcopy(ERROR_DICT)

    # load values
    try:
        x = df.cols[data_type].x
        y = df.cols[data_type].y

    # data type not there
    except AttributeError:
        x = []
        y = []
        error_dict['data_type'] = f'data_type {data_type} not existant'
    except TypeError:
        x = []
        y = []
        error_dict['slices_not_downloaded'] = f'slice for {data_type} not found'

    # length check
    xl = len(x)
    yl = len(y)
    diff = yl - xl
    minimum = min(xl, yl)

    ratio = None
    medi = None
    lower_quartile = None
    ppg_0 = None
    ppg_24 = None

    if diff != 0:
        error_dict['xy_length'] = f'y-x-len diff: {diff}'

    if xl == 0 and yl == 0:
        error_dict['no_values'] = 'no x&y values'
    elif yl == 0:
        error_dict['no_values'] = 'no y values'
    elif xl == 0:
        error_dict['no_values'] = 'no x values'

    if xl > 0: ratio = yl / xl

    if yl > 0:

        if 'ppg' in data_type and 'ppg_quality' not in data_type:
            # find numer of 0 and 2**24 values
            ppg_0 = np.sum(y == 0)
            if ppg_0 > 0:
                error_dict['ppg_0'] = f'{ppg_0} Zeroes'
            ppg_24 = np.sum(y == 2**24-1)
            if ppg_24 > 0:
                error_dict['ppg_2**24'] = f'{ppg_0} 2**24'

        # if 'battery' in data_type:
        #     # constant battery bug
        #     if y[0] == y[-1]:
        #         error_dict['battery_const'] = f'battery const {y[-1]}%'

    if xl > 0 and yl > 0:

        # y = 0, x = 0 is error is present
        # only those where it goes back to x=0 (but not too strict)
        x0 = x[x < 0.0001]
        if x0.any and len(x0):
            error_dict['xy_0'] += f' x=0({len(x0)}x)'
            if diff == 0:
                y0 = y[x < 0.0001]
                if y0.any and len(y0):
                    error_dict['xy_0'] += f' y=0({len(y0)}x)'

        # todo: calculate standard deviation?

        # median calculations
        if data_type in ['ppg_ir', 'ppg_red', 'temperature']:

            if data_type in ['ppg_ir', 'ppg_red']:

                y_copy = deepcopy(y)
                y_copy.sort()
                lower_quartile = y_copy[int(yl / 4)]

                if lower_quartile < 1000:
                    error_dict['ppg_low'] = f'{data_type} lower quartile below 1000!'

            elif data_type in ['temperature']:
                medi = median(y)
                if medi < 30:
                    error_dict['temp_low'] = 'Temperature median below 30°C'

    x_elements_decreased_idx = None
    number_of_jumps = None
    jump_differences = None
    max_jump = None
    min_jump = None
    identical_x_idx = None
    number_of_identical_x = None
    # for the following check, xl must be > 1 otherwise element_comparison will return an empty list and cause an error in max()
    if xl > 1:

        # (exclude already counted x ~= 0)
        x_without_0 = x[x > 0.0001]

        # check for time JUMPS
        # compare neighbouring elements to find elements that are smaller than the predecessor (works only with numpy arrays!)
        element_comparison = x_without_0[1:] < x_without_0[:-1]

        if max(element_comparison) == True:
        # if not no_elements_decreasing:
            # add one to get the index of the decreased element
            x_elements_decreased_idx = np.where(element_comparison)[0] + 1
            number_of_jumps = len(x_elements_decreased_idx)
            # how many seconds backwards is the time jump?
            jump_differences = x_without_0[x_elements_decreased_idx-1] - x_without_0[x_elements_decreased_idx]
            # the biggest/smallest jump
            max_jump = np.round(max(jump_differences), 1)
            min_jump = np.round(min(jump_differences), 1)

            # print(f'{df.hash_id}, {data_type}, {number_of_jumps} time jumps with maximum difference of {max_jump}s at: {x_elements_decreased_idx}')
            error_dict['time_jump'] = f'{number_of_jumps}x time_jump, max:{max_jump}s'

        # check for identical times
        element_comparison = x_without_0[1:] == x_without_0[:-1]

        if max(element_comparison) == True:
            identical_x_idx = np.where(element_comparison)[0] + 1
            number_of_identical_x = len(identical_x_idx)
            # how many seconds backwards is the time jump?
            error_dict['time_identical'] = f'{number_of_identical_x}x identical timestamps'

    # check for uint24 loading errors
    if 'ppg' in data_type and data_type != 'ppg_quality':

        for slice_idx in range(len(df.cols[data_type]._slices_y)):

            slice = df.cols[data_type]._slices_y[slice_idx]

            if slice.file_exists:

                true_bin_size = determine_bin_size(slice._path)

                if true_bin_size % 3 != 0:
                    error_dict['bin_size_div_3'] = 'div by 3 error'

                if true_bin_size > config.SLICE_MAX_SIZE:
                    error_dict['slice_overshoot'] = f'slice too big {true_bin_size}'

                if true_bin_size != slice.bin_size_meta:
                    error_dict['bin_size_property'] = 'bin_size @prop incorrect'

            else:

                error_dict['slices_not_downloaded'] += f'{slice.hash}'


    values_dict['xl'] = xl
    values_dict['yl'] = yl
    values_dict['diff'] = diff
    values_dict['minimum'] = minimum
    values_dict['ratio'] = ratio
    values_dict['median'] = medi
    values_dict['quartile'] = lower_quartile
    values_dict['x_elements_decreased_idx'] = x_elements_decreased_idx
    values_dict['number_of_jumps'] = number_of_jumps
    values_dict['jump_differences'] = jump_differences
    values_dict['max_jump'] = max_jump
    values_dict['min_jump'] = min_jump
    values_dict['identical_x_idx'] = identical_x_idx
    values_dict['number_of_identical_x'] = number_of_identical_x
    values_dict['ppg_0'] = ppg_0
    values_dict['ppg_24'] = ppg_24

    return values_dict, error_dict


def determine_plot_format(types):
    if len(types) == 1:
        rows = 1
        cols = 1
    elif len(types) == 2:
        rows = 1
        cols = 2
    elif len(types) == 3:
        rows = 3
        cols = 1
    elif len(types) == 4:
        rows = 2
        cols = 2
    elif len(types) == 5:
        rows = 3
        cols = 2
    elif len(types) == 6:
        rows = 3
        cols = 2
    else:
        return False, False

    return rows, cols

# helper plot
def plot_jumps_and_identicals(axis, df_dict, data_type, x, y, diff):

    if df_dict['data_types'][data_type]['errors']['time_identical']:
        idx_identical = df_dict['data_types'][data_type]['values']['identical_x_idx']
        if diff != 0:
            idx_identical = np.where((idx_identical < len(x)) & (idx_identical < len(y)))
        identicals_x = x[idx_identical]
        identicals_y = y[idx_identical]
        axis.plot(identicals_x, identicals_y, marker='s', markerfacecolor='red', linewidth=0.25)

    if df_dict['data_types'][data_type]['errors']['time_jump']:
        idx_jump = df_dict['data_types'][data_type]['values']['x_elements_decreased_idx']
        if diff != 0:
            idx_jump = np.where((idx_jump < len(x)) & (idx_jump < len(y)))
        jumps_x = x[idx_jump]
        jumps_y = y[idx_jump]
        axis.plot(jumps_x, jumps_y, marker='D', markerfacecolor='magenta', linewidth=0.25)

    return axis

def plot_subplots(df_dict, types):

    global STORAGE_FOLDER
    df = df_dict['df']

    w = 11
    h = 8

    rows, cols = determine_plot_format(types)
    if not rows and not cols:
        return

    warning_str = ''

    fig, axs = plt.subplots(rows, cols, sharex=True)
    fig.suptitle(f'df: {df.hash_id} person: {df.person.hash_id} {str(df.date_time_start)[0:16]}')
    # iterate through subplots
    for data_type_index, a in enumerate(axs.reshape(-1)):

        data_type = types[data_type_index]

        # plot only if this data type exists
        if not df_dict['data_types'][data_type]['errors']['data_type']:
            x = df.cols[data_type].x
            y = df.cols[data_type].y
            diff = df_dict['data_types'][data_type]['values']['diff']
            minimum = df_dict['data_types'][data_type]['values']['minimum']

            error_textstr = ''
            # collect all errors that appeared
            for error in df_dict['data_types'][data_type]['errors'].values():
                if error:
                    error_textstr += error + '\n'

            # plot the data
            if diff == 0:
                a.plot(x, y)

            else:
                a.plot(x[:minimum], y[:minimum], label='xy shortened', color='orange')

            a = plot_jumps_and_identicals(a, df_dict, data_type, x, y, diff)

        else:
            error_textstr = df_dict['data_types'][data_type]['errors']['data_type']

        # error textbox
        props = dict(boxstyle='round', facecolor='wheat', alpha=0.4)
        a.text(0.02, 0.95, error_textstr, transform=a.transAxes, fontsize=9,
               verticalalignment='top', bbox=props)

        # title color
        if error_textstr != '':
            color = 'red'
            warning_str = '_!!!'
        else:
            color = 'black'

        a.set_title(f'{data_type}', color=color)

    plt.tight_layout(pad=0.1)
    fig.set_size_inches(w, h)

    storage_folder = STORAGE_FOLDER + str(df.date_time_start)[0:10] + '/'
    if not os.path.exists(storage_folder):
        os.mkdir(storage_folder)

    fig.savefig(f'{storage_folder}{df.person.hash_id}_{str(df.date_time_start)[0:16]}_{df.hash_id}_{types[0]}_{types[-1]}{warning_str}.png')

    return fig, axs

def check_analyse_conditions(df_hash):
    # download meta only
    df = db_sync.pull_df(df_hash, download_slices=False)

    # analyse conditions
    if df.date_time_start.date() >= analyse_from_date and df.duration > 60 * 5:
        return True
    else:
        return False

###########################################################################################################
#      _______.___________.    ___      .______     .___________.
#     /       |           |   /   \     |   _  \    |           |
#    |   (----`---|  |----`  /  ^  \    |  |_)  |   `---|  |----`
#     \   \       |  |      /  /_\  \   |      /        |  |
# .----)   |      |  |     /  _____  \  |  |\  \----.   |  |
# |_______/       |__|    /__/     \__\ | _| `._____|   |__|

if __name__ == '__main__':

    connect('server_daily_analysis')
    file_path = Path(__file__)
    STORAGE_FOLDER = str(file_path.parent / 'df_analysis') + '/'
    if not os.path.exists(STORAGE_FOLDER):
        os.mkdir(STORAGE_FOLDER)

    # df = DataFile.objects(_hash_id='RKSTHP.R1X1G7').first()
    # print(df.stats_slices)
    # exit()

    # shortcut if only result table is wanted
    # analyze_dict()
    # exit()

    # todo: count all the appearances aleady while processing => only read out in analyze_dict()
    # todo: make better ppg analysis?
    # todo: count 0 and 2*24-1 of ppg values appearances

    try:
        analyse_dict = pickle.load(open(STORAGE_FOLDER + 'analyse_dict.pkl', 'rb'))
    except FileNotFoundError:
        analyse_dict = {}
        analyse_dict['skipped_list'] = []
        analyse_dict['analyzed_list'] = []
        analyse_dict['analyzed'] = {}
    # plotting groups
    groups = [
        ['ppg_ir', 'ppg_red', 'ppg_ambient', ],
        ['perfusion_ir', 'respiration_rate', 'heart_rate', 'rr_int', 'temperature', 'spo2'],
        ['battery', 'quality', 'ppg_quality', 'ble_packet_counter', 'ble_sample_counter', 'ble_sample_amount']
    ]
    ########    ########    ########    ########    ########
    RESET_DICT = False

    if RESET_DICT:
        analyse_dict['skipped_list'] = []
        analyse_dict['analyzed_list'] = []
        analyse_dict['analyzed'] = {}

    print(f'analyse_dict["analyzed_list"]: {analyse_dict["analyzed_list"]}')

    white_list = [

    ]
    analyse_from_date = date(2020, 8, 1)
    db_sync = DB_Sync(server='https://telecovid.earconnect.de/')
    server_df_list = db_sync.request_server_df_list()
    server_df_list.extend(white_list)
    server_df_list.sort()
    print(server_df_list)

    # server_df_list = ['RKSTHP.R1X1G7']

    # iterate through files
    for df_hash in server_df_list:

        if df_hash not in analyse_dict['analyzed_list'] or df_hash in white_list:

            # download meta and check conditions for processing
            if check_analyse_conditions(df_hash):
                print(f'Now processing df: {df_hash} -------------------------------------\n')

                df = db_sync.pull_df(df_hash)

                df_dict = {}
                df_dict['df'] = df
                df_dict['meta'] = df.to_json()
                df_dict['data_types'] = {}

                # actual analysis ############################################
                for group in groups:

                    for data_type in group:
                        values_dict, error_dict = analyse_data_type(df, data_type)
                        df_dict['data_types'][data_type] = {}
                        df_dict['data_types'][data_type]['values'] = values_dict
                        df_dict['data_types'][data_type]['errors'] = error_dict

                # plot everything after calculating all data_types
                for group in groups:
                    plot_subplots(df_dict, group)
                plt.close('all')

                # save some space before storing all
                del df_dict['df']

                analyse_dict['analyzed'][df_hash] = df_dict

                # append to list of analyzed files
                if df.status_closed and df.check_all_slices_sent() or df.date_time_start.date() + timedelta(days=3) < datetime.now().date():
                    analyse_dict['analyzed_list'].append(df_hash)

                print('Saving...')
                pickle.dump(analyse_dict, open(STORAGE_FOLDER + 'analyse_dict.pkl', 'wb'))

        else:
            analyse_dict['skipped_list'].append(df_hash)
            print(f'skipping {df_hash}')

    # save as pickle
    print('Saving...')
    pickle.dump(analyse_dict, open(STORAGE_FOLDER + 'analyse_dict.pkl', 'wb'))

    # SUMMARY
    print('Summary')
    analyze_dict()
