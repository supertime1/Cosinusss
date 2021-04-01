#!/usr/bin/env python3
import pytest
import sys
import os
import zstd
from pathlib import Path
import numpy as np
import logging
import json

import matplotlib as mpl
mpl.rcParams['agg.path.chunksize'] = 10000
from matplotlib import pyplot as plt

file_path = Path(os.path.dirname(os.path.realpath(__file__)))
if file_path != "":
    os.chdir(file_path)
sys.path.append(str(file_path.parents[1]))
import data_container as dc

FOLDER = 'plots/'

###############################################################################
def determine_bin_size(path):

    with open(path, 'rb') as f:
        bin_data = f.read()

    if '.zst' in str(path):
        bin_data = zstd.ZSTD_uncompress(bin_data)

    return len(bin_data)

def plot_optimized_data(df, x_spo2, y_spo2, x_hr, y_hr, x_rr, y_rr, x_temp, y_temp, x_score, y_score):

    w = 14
    h = 10
    label_font_size = 9
    linewidth = 0.3
    color = '#1f77b4'

    fig, a = plt.subplots(5, 1, sharex=True)
    a[0].set_title(df._meta['person_hash'] + '  ' + df._meta['date_time_start'][0:10])

    a[0].plot(x_spo2, y_spo2, color=color, linewidth=linewidth, marker='.')
    a[1].plot(x_hr, y_hr, color=color, linewidth=linewidth, marker='.')
    a[2].plot(x_rr, y_rr, color=color, linewidth=linewidth, marker='.')
    a[3].plot(x_temp, y_temp, color=color, linewidth=linewidth, marker='.')
    max_length = min([len(x_score), len(y_score)])
    a[4].plot(x_score[:max_length], y_score[:max_length], color=color, linewidth=linewidth, marker='.')

    a[0].set_ylabel('spo2 (%)', fontsize=label_font_size, labelpad=14)
    # a[0].set_xticks(major_ticks)
    a[0].set_ylim([60, 100])

    a[1].set_ylabel('heart rate (bpm)', fontsize=label_font_size, labelpad=14)
    # a[1].set_xticks(major_ticks)
    a[1].set_ylim([0, 200])

    a[2].set_ylabel('respiration rate (rpm)', fontsize=label_font_size, labelpad=21)
    # a[2].set_xticks(major_ticks)
    a[2].set_ylim([0, 50])

    a[3].set_ylabel('temperature (°C)', fontsize=label_font_size, labelpad=21)
    # a[3].set_xticks(major_ticks)
    a[3].set_ylim([35, 41])

    a[4].set_xlabel('time (h)', fontsize=label_font_size)
    a[4].set_ylabel('score (a.u)', fontsize=label_font_size, labelpad=29)
    # a[4].set_xticks(major_ticks)
    a[4].set_ylim([0, 6])

    for ax in range(5):
        a[ax].xaxis.grid(True, which='Major')
        a[ax].yaxis.grid(True, which='Major')
        # a[ax].set_xlim([15, 24])

    fig.set_size_inches(w, h)

    plt.subplots_adjust(left=0.1,
                        bottom=0.07,
                        right=0.95,
                        top=0.9,
                        wspace=0.2,
                        hspace=0.2)

    fig.savefig(f'{FOLDER}{df._meta["person_hash"]}_{df._meta["date_time_start"][0:16]}_block_vitals.png')

def optimize_data(x, y, method):
    """
    method: min, max, median
    """

    global TIME_OFFSET

    # find blocks of data
    y_block_data = []
    x_block_data = []

    time_index = 0
    current_block = 0
    while time_index < len(x)-2:

        print(f'current block: {current_block}')
        time_diff = x[time_index + 1] - x[time_index]

        block_x_data = []
        block_y_data = []
        # collect all data in a block. No data for 5 minute => next block
        while time_diff < 5*60 and time_index < len(x)-2:

            block_x_data.append(x[time_index])
            block_y_data.append(y[time_index])

            time_index += 1
            time_diff = x[time_index + 1] - x[time_index]

        if block_y_data != []:

            # process data block
            if method == 'median':
                block_y_data.sort()
                middle = int(len(block_y_data)/2)
                median = block_y_data[middle]

                y_block_data.append(median)  # start y-value of the block
                # y_block_data.append(median)  # end y-value
            elif method == 'max':
                y_block_data.append(max(block_y_data))
                # y_block_data.append(max(block_y_data))
            elif method == 'min':
                y_block_data.append(min(block_y_data))
                # y_block_data.append(min(block_y_data))

            # x borders and convert to hours
            x_block_data.append( (min(block_x_data)+TIME_OFFSET) / 3600)
            # x_block_data.append( (max(block_x_data)+TIME_OFFSET) /3600)

            current_block += 1

        # jump over to next block
        else:
            time_index += 1
            time_diff = x[time_index + 1] - x[time_index]

    print(f'Blocks: {current_block}')

    return x_block_data, y_block_data