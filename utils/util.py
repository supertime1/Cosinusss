from data_container import config, DataFile
from data_container.api_db_sync import DBSync
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import datetime
import seaborn as sns
import scipy
from scipy import stats
import pingouin as pg


def configure_api(db_name, username, prj_hash_id, update_local=False):
    """
    Configure api as the data handler
    """
    try:
        config.init(db_name=db_name)
        api = DBSync(username=username,
                     server='https://telecovid.earconnect.de')
    except:
        print('Configure API failed')
        return
    # password: teeshau7aiMonoh9ee
    if update_local:
        # download data from the server
        api.pull_all_dfs(prj_hash_id=prj_hash_id)

    df_list_local = api.df_list(prj_hash_id=prj_hash_id)
    print('There are', len(df_list_local), 'files in local database')
    return api


def generate_table(date, api):
    """
    Convert local database into a Pandas dataframe, for ease of data analysis

    params:
    date: select the date that the data is generated AFTER

    outputs:
    an overview table in the form of pandas dataframe
    """
    table = api.overview_dfs()
    overview_df = pd.DataFrame(table[1:], columns=table[0])
    # change to lower case for the device name
    overview_df['device'] = overview_df['device'].str.lower()
    overview_df.loc[overview_df.device.str.match(r'^polar'), 'device'] = 'polar'
    overview_df = overview_df[overview_df.when > date]
    overview_df['when'] = [datetime.datetime.strptime(i, '%Y-%m-%d %H:%M:%S') for
                           i in list(overview_df['when'])]
    overview_df['duration'] = [datetime.datetime.strptime(i, '%H:%M:%S') for
                               i in list(overview_df['duration'])]
    # add a column of 'end' date, this will be useful to pair polar and floyer devices
    overview_df['end'] = [(overview_df.when.loc[i]
                           - datetime.datetime(1900, 1, 1)
                           + overview_df.duration.loc[i]) for i in overview_df.index]
    return overview_df


def correct_label(table, api):
    """
    fix the labeling problem of cshell and biometric

    params:
    overview table from generate_table function

    outputs:
    updated table with correct cshell and biometric labels

    """

    def get_device_model(df):
        """
        get device

        params:
        df - output of api.pull_df

        outputs:
        correct device label for df
        """
        # get device
        if df.device:
            device_model = df.device.device_model
        else:
            device_model = df.device_model

        if 'polar' in device_model.lower():
            return 'polar'

        elif 'biometric' in device_model.lower() or 'cshell' in device_model.lower():
            if 'ppg_ir_2' in list(df.cols):
                some_data = df.c.ppg_ir_2.y[600:650]
                if list(some_data):
                    ppg_mean = np.mean(some_data)
                else:
                    return 'na'
            else:
                ppg_mean = 1000

            if ppg_mean < 500:
                return 'cshell'
            else:
                return 'biometric'

    def get_correct_label(hash_ids, target_device, api):
        """
        Correct the incorrect Floyer device labels
        """
        # output list of corrected labels
        corrected_name = []
        # a counter to record how many labels have been corrected after processing
        cnt = 0
        print(f'There are in total {len(hash_ids)} files with {target_device} label')
        for i in range(len(hash_ids)):
            try:
                print(f'api reading {i}th file...')
                df = api.pull_df(list(hash_ids)[i])
                # in case reading file failed
            except:
                print(f'api read {i}th file failed!')
                corrected_name.append(table.loc[hash_ids.index[i]].device)
                continue
            # get the new label by api reading the file
            new_label = get_device_model(df)
            corrected_name.append(new_label)
            if new_label != table.loc[hash_ids.index[i]].device:
                print(f'Person {table.loc[hash_ids.index[i]].person} and \
            {target_device} label has been corrected to {new_label}')
                cnt += 1
        print(f'There are in total {cnt} files been corrected')
        return corrected_name

    # get the hash ids of cshell and biometric in original table
    cshell_hash_ids = table[table.device == 'cshell']['df id']
    biometric_hash_ids = table[table.device == 'biometric']['df id']

    # get the row index of cshell and biometric in original table
    original_cshell_index = table[table.device == 'cshell'].index
    original_biometric_index = table[table.device == 'biometric'].index

    # update the original table with corrected labels of cshell and biometric
    table.device.loc[original_cshell_index] = get_correct_label(cshell_hash_ids,
                                                                'cshell',
                                                                api)
    table.device.loc[original_biometric_index] = get_correct_label(biometric_hash_ids,
                                                                   'biometric',
                                                                   api)

    return table


def find_pairs_row_index(table, floyer_device):
    """
    Filter table with paired polar and floyer devices

    params:
    table - output of correct_label() function
    floyer_device - floyer device type that this function will output its paired index with polar

    output:
    paired_idx_lst - list of paired indexes of specified floyer_device and its corresponding polar device
    """
    paired_table = table[table.device.isin(['polar', floyer_device])]

    paired_idx_lst = []
    # find paried sample ids
    for person in list(paired_table['person'].unique()):
        person_paired_table = paired_table[paired_table.person == person]
        polar_idx_lst = person_paired_table[person_paired_table.device == 'polar'].index
        floyer_idx_lst = person_paired_table[person_paired_table.device == floyer_device].index

        # find pairs by checking the overlapping time (2*O(n^2))
        for polar_idx in polar_idx_lst:
            polar_start_time = person_paired_table.when.loc[polar_idx]
            polar_end_time = person_paired_table.end.loc[polar_idx]

            for floyer_idx in floyer_idx_lst:
                floyer_start_time = person_paired_table.when.loc[floyer_idx]
                floyer_end_time = person_paired_table.end.loc[floyer_idx]

                if floyer_start_time >= polar_end_time or polar_start_time >= floyer_end_time:
                    continue
                else:
                    paired_idx_lst.append([polar_idx, floyer_idx])

    return paired_idx_lst


def paired_csv(correct_table, floyer_device, api, paired_idx_lst, file_path):
    """
    Return a paired csv files of floyer device and its corresponding polar device
    """
    floyer_hash_ids = correct_table[correct_table.device == floyer_device]['df id']

    for i in range(len(floyer_hash_ids)):
        df = api.pull_df(list(floyer_hash_ids)[i])

        # those info go to the csv file name
        test_date = df.date_time_start.date()
        device_type = floyer_device
        person = df.person.label
        file_hash_id = df._hash_id
        # those info go into the csv file columns
        time_stamp = df.c.ppg_ir.x
        ppg_ambient = df.c.ppg_ambient.y
        ppg_ir = df.c.ppg_ir.y
        ppg_ir_2 = df.c.ppg_ir_2.y
        ppg_ir_3 = df.c.ppg_ir_3.y
        acc_x = df.c.acc_x.y
        acc_y = df.c.acc_y.y
        acc_z = df.c.acc_z.y

        dic = {'time_stamp': time_stamp,
               'alsVis': ppg_ambient,
               'ps1': ppg_ir,
               'ps2': ppg_ir_2,
               'ps3': ppg_ir_3,
               'x': acc_x,
               'y': acc_y,
               'z': acc_z
               }

        df_csv = pd.DataFrame(data=dic, index=None)
        file_name = str(test_date) + '_' + device_type + '_' + person + '_' + file_hash_id + '_' + str(i)
        df_csv.to_csv(file_path + '/' + file_name + '.csv', index=False)

        # also export the corresponding polar data
        polar_idx = [p_idx for p_idx, f_idx in paired_idx_lst if f_idx == floyer_hash_ids.index[i]]
        polar_hash_id = correct_table[correct_table.index == polar_idx[0]]['df id']
        p_df = DataFile.objects(_hash_id=list(polar_hash_id)[0]).first()
        # those info go to the csv file name
        p_test_date = p_df.date_time_start.date()
        p_device_type = p_df.device_model
        p_person = p_df.person.label
        p_file_hash_id = p_df._hash_id
        # those info go into the csv file columns
        p_time_stamp = p_df.c.heart_rate.x
        p_hr = p_df.c.heart_rate.y

        dic = {'time_stamp': p_time_stamp,
               'hr': p_hr,
               }

        p_df_csv = pd.DataFrame(data=dic, index=None)
        p_file_name = str(p_test_date) + '_' + p_device_type + '_' + p_person + '_' + p_file_hash_id + '_' + str(i)
        p_df_csv.to_csv(file_path + '/' + p_file_name + '.csv', index=False)


class PairedSample:

    def __init__(self, polar_hash_id, floyer_hash_id, hr_algo_version, api):
        self.polar_hash_id = polar_hash_id
        self.floyer_hash_id = floyer_hash_id
        self.header_polar = api.pull_df(polar_hash_id)
        self.header_floyer = api.pull_df(floyer_hash_id)
        self.person_id = self.header_floyer.person.hash_id
        self.hr_algo_version = hr_algo_version
        self.df = pd.DataFrame()
        self.api = api

    def process(self):
        """
        Generate a pandas table for data analysis
        """
        # read polar data
        polar = DataFile.objects(_hash_id=self.polar_hash_id).first()
        # polar_hr_x = polar.c.heart_rate.x
        polar_hr_y = polar.c.heart_rate.y

        # process floyer data with heart rate algorithm
        try:
            floyer = self.api.one3_hr_algo(self.hr_algo_version, self.floyer_hash_id)
            floyer_hr_x = np.asarray(floyer['heart_rate_t'])
            floyer_hr_y = floyer['heart_rate']
            floyer_quality = floyer['quality']

        except:
            print(f"{self.floyer_hash_id} cannot be found")
            return

        offset = (self.header_floyer.date_time_start - self.header_polar.date_time_start).total_seconds()

        # get the activity label
        activity_label = ['Unknown'] * len(floyer_hr_x)
        for i in range(len(self.header_floyer.chunks_labelled)):
            i_th_activity_start = self.header_floyer.chunks_labelled[i].time_offset
            i_th_activity_duration = self.header_floyer.chunks_labelled[i].duration
            i_th_activity_end = i_th_activity_start + i_th_activity_duration

            activity_label[round(i_th_activity_start):round(i_th_activity_end)] = \
                [self.header_floyer.chunk_labels[i]] * (round(i_th_activity_end) - round(i_th_activity_start))

        # if floyer starts later
        if offset > 0:
            polar_hr = polar_hr_y[int(offset):]
            duration = min(len(polar_hr), len(floyer_hr_y))

            final_polar_hr = polar_hr[:duration]
            final_floyer_hr = floyer_hr_y[:duration]
            final_floyer_quality = floyer_quality[:duration]
            final_floyer_activity = activity_label[:duration]

        # if polar starts later
        else:
            floyer_hr = floyer_hr_y[-int(offset):]
            floyer_qa = floyer_quality[-int(offset):]
            floyer_activity = activity_label[-int(offset):]
            duration = min(len(floyer_hr), len(polar_hr_y))

            final_polar_hr = polar_hr_y[:duration]
            final_floyer_hr = floyer_hr[:duration]
            final_floyer_quality = floyer_qa[:duration]
            final_floyer_activity = floyer_activity[:duration]

        # add person id
        final_person_id = [self.person_id] * len(final_floyer_hr)

        dic = {'floyer_hr': final_floyer_hr,
               'polar_hr': final_polar_hr,
               'quality': final_floyer_quality,
               'activity': final_floyer_activity,
               'person': final_person_id
               }

        self.df = pd.DataFrame(data=dic, index=None)

        return self.df


def generate_paired_samples(paired_idx_lst, table, hr_algo_version, api):
    """
    Use the index of paired devices (output from find_pairs_row_index) to generate
    a list of PairedSample instances

    params:
    paired_idx_lst - a list containing the row index of paired devices,
                    It is the output of function find_pairs_row_index.
    table - a Pandas dataframe that stores test infomation, it is the output of the
           correct_label()

    outputs:
    A list of PairedSample instances
    """
    paired_sample_lst = []
    for polar_idx, floyer_idx in paired_idx_lst:
        polar_hash_id = table['df id'].loc[polar_idx]
        floyer_hash_id = table['df id'].loc[floyer_idx]
        paired_sample_lst.append(PairedSample(polar_hash_id, floyer_hash_id, hr_algo_version, api))
    return paired_sample_lst


def generate_stats_table(paired_sample_lst):
    """
    Process the paired_sample_lst into a statistical table
    """
    stats_table = pd.DataFrame(index=None)
    for paired_sample in paired_sample_lst:
        stats_table = stats_table.append(paired_sample.process())
    return stats_table


class Stats_Table:
    def __init__(self, stats_table, device_type, algo_version):
        self.stats_table = stats_table
        self.device_type = device_type
        self.algo_version = algo_version
        self.num_subject = len(np.unique(stats_table['person']))

    def visualize_subjects(self):
        """
        Check if each subject performs same activity with similar time span
        """
        sns.set_style('darkgrid')

        time_span_lst = []
        activity_lst = []
        person_lst = []

        for person in np.unique(self.stats_table['person']):
            for activity in np.unique(self.stats_table['activity']):
                if activity == 'Unknown': continue
                temp_table = self.stats_table[(self.stats_table.person == person) &
                                              (self.stats_table.activity == activity)].reset_index()
                time_span_lst.append(len(temp_table))
                activity_lst.append(activity)
                person_lst.append(person)

        dic = {'time_span': time_span_lst,
               'activity': activity_lst,
               'person': person_lst
               }

        df = pd.DataFrame(data=dic, index=None)

        # plot activity vs time_span for each test subject
        g = sns.FacetGrid(df,
                          col='person',
                          col_wrap=2,
                          height=2,
                          aspect=4,
                          hue='activity'
                          )
        g = g.map(plt.barh, 'activity', 'time_span')
        axes = g.axes.flatten()

        for i in range(len(axes)):
            axes[i].set_title("Person:  " + g.col_names[i], fontsize=14)
        g.set_xlabels('Time_Span (s)', fontsize=14)
        g.set_ylabels('Activity', fontsize=14)

    def create_mape_table(self, quality_level):
        """
        Create a table with MAPE for each activity
        """
        mape_lst = []
        da_lst = []
        activity_lst = []
        person_lst = []

        for person in np.unique(self.stats_table['person']):
            for activity in np.unique(self.stats_table['activity']):
                if activity == 'Unknown': continue
                temp_table = self.stats_table[(self.stats_table.person == person) &
                                              (self.stats_table.activity == activity)].reset_index()
                # sanity check, in case some test subjects didn't finish all activities
                if len(temp_table) == 0:
                    break

                sum = 0
                count = 0
                for i in range(len(temp_table)):
                    if temp_table['quality'][i] < quality_level or temp_table['quality'][i] > 100:
                        continue

                    sum += abs(temp_table['floyer_hr'][i] -
                               temp_table['polar_hr'][i]) / temp_table['polar_hr'][i] * 100
                    count += 1
                mape = round(sum / count, 2)
                da = round(count / len(temp_table), 2) * 100

                mape_lst.append(mape)
                da_lst.append(da)
                activity_lst.append(activity)
                person_lst.append(person)

        dic = {'mape': mape_lst,
               'da': da_lst,
               'activity': activity_lst,
               'person': person_lst
               }

        mape_table = pd.DataFrame(data=dic, index=None)
        return mape_table

    def mape_boxplot(self, qf_lvl, person_level=False):
        """
        Make MAPE boxplot
        """
        sns.set_style('darkgrid')
        mape_df = self.create_mape_table(qf_lvl)

        if not person_level:
            fig, ax = plt.subplots(1, 1, figsize=(12, 6))
            ax.set_xticks(np.arange(0, 100, 10), minor=False)
            ax.grid(b=True, which='major', color='w', linewidth=2.0)
            ax.grid(b=True, which='minor', color='w', linewidth=1)

            sns.boxplot(x='mape', y='activity', data=mape_df, palette="Set2")
            plt.suptitle('Boxplot of MAPE per Activity', fontsize=24, y=1.05)
            plt.title(f'Num of Subjects = {self.num_subject},   Device = {self.device_type},    '
                      f'Algo_Version = {self.algo_version},   QF_lvl = {qf_lvl}',
                      fontsize=20)
            plt.xticks(fontsize=20)
            plt.yticks(fontsize=20)
            plt.xlabel('MAPE', fontsize=26)
            plt.ylabel('Activity', fontsize=26)

        else:
            g = sns.FacetGrid(mape_df,
                              col='person',
                              col_wrap=1,
                              height=2,
                              aspect=5,
                              hue='activity'
                              )
            g = g.map(plt.bar, 'activity', 'mape')
            axes = g.axes.flatten()

            for i in range(len(axes)):
                axes[i].set_title("Person:  " + g.col_names[i], fontsize=14)
            g.set_ylabels('MAPE', fontsize=14)
            g.set_xlabels('Activity', fontsize=14)
            plt.xticks(rotation=45, fontsize=14)
            plt.legend(bbox_to_anchor=(1.4, 5))

    def da_boxplot(self, qf_lvl, person_level=False):
        """
        Make data availability boxplot
        """
        sns.set_style('darkgrid')
        mape_df = self.create_mape_table(qf_lvl)

        if not person_level:
            fig, ax = plt.subplots(1, 1, figsize=(12, 6))
            ax.set_xticks(np.arange(0, 100, 10), minor=False)
            ax.grid(b=True, which='major', color='w', linewidth=2.0)
            ax.grid(b=True, which='minor', color='w', linewidth=1)

            sns.boxplot(x='da', y='activity', data=mape_df, palette="Set2")
            plt.suptitle('Boxplot of Data Availability per Activity', fontsize=24, y=1.05)
            plt.title(f'Num of Subjects = {self.num_subject},   Device = {self.device_type},    '
                      f'Algo_Version = {self.algo_version},   QF_lvl = {qf_lvl}',
                      fontsize=20)
            plt.xticks(fontsize=20)
            plt.yticks(fontsize=20)
            plt.xlabel('Data Availability (%)', fontsize=24)
            plt.ylabel('Activity', fontsize=24)

        else:
            g = sns.FacetGrid(mape_df,
                              col='person',
                              col_wrap=1,
                              height=2,
                              aspect=5,
                              hue='activity'
                              )
            g = g.map(plt.bar, 'activity', 'da')
            axes = g.axes.flatten()

            for i in range(len(axes)):
                axes[i].set_title("Person:  " + g.col_names[i], fontsize=14)
            g.set_ylabels('DA', fontsize=14)
            g.set_xlabels('Activity', fontsize=14)
            plt.xticks(rotation=45, fontsize=14)
            plt.legend(bbox_to_anchor=(1.4, 5))

    def create_qa_table(self):
        """
        Generate a table with MAPE in every cut-off quality level
        """
        mape_lst = []
        mape_std_lst = []
        da_lst = []
        da_std_lst = []
        qa_lst = []
        activity_lst = []

        qa_range = [30, 35, 40, 45, 50, 55, 60, 65]

        for activity in np.unique(self.stats_table['activity']):
            if activity == 'Unknown': continue
            activity_table = self.stats_table[self.stats_table.activity == activity].reset_index()

            for qa in qa_range:
                temp_mape_lst = []
                temp_da_lst = []
                # calculate each person's mape and da, to get the mean and std of mape and da
                for person in np.unique(activity_table['person']):
                    person_activity_table = activity_table[activity_table.person == person].reset_index()
                    sum = 0
                    count = 0
                    for i in range(len(person_activity_table)):
                        if qa <= person_activity_table['quality'][i] <= 100:
                            sum += abs(person_activity_table['floyer_hr'][i] -
                                       person_activity_table['polar_hr'][i]) / person_activity_table['polar_hr'][
                                       i] * 100
                            count += 1
                    if count == 0: continue
                    mape = round(sum / count, 2)
                    da = round(count / len(person_activity_table) * 100, 2)
                    temp_mape_lst.append(mape)
                    temp_da_lst.append(da)

                mean_mape = np.mean(np.asarray(temp_mape_lst))
                std_mape = np.std(np.asarray(temp_mape_lst))
                mean_da = np.mean(np.asarray(temp_da_lst))
                std_da = np.std(np.asarray(temp_da_lst))

                mape_lst.append(mean_mape)
                mape_std_lst.append(std_mape)
                da_lst.append(mean_da)
                da_std_lst.append(std_da)
                qa_lst.append(qa)
                activity_lst.append(activity)

        dic = {'mape': mape_lst,
               'mape_std': mape_std_lst,
               'da': da_lst,
               'da_std': da_std_lst,
               'qa': qa_lst,
               'activity': activity_lst}

        qa_table = pd.DataFrame(data=dic, index=None)

        return qa_table

    def mape_vs_qa(self):

        qa_table = self.create_qa_table()

        sns.set_style('darkgrid')
        g = sns.FacetGrid(qa_table,
                          col='activity',
                          col_wrap=4,
                          height=8,
                          aspect=0.6
                          )

        g = g.map(plt.errorbar, 'qa', 'mape', 'mape_std', capsize=10, marker='o', ms=10, color='green', label='MAPE')
        g = g.map(plt.errorbar, 'qa', 'da', 'da_std', capsize=10, marker='v', ms=10, color='red',
                  label='Data Availability')
        axes = g.axes.flatten()

        for i in range(len(axes)):
            axes[i].set_title("Activity = " + g.col_names[i], fontsize=20)
        g.set_yticklabels(fontsize=20)
        g.set_xticklabels([25, 30, 35, 40, 45, 50, 55, 60, 65], fontsize=20)
        g.set_ylabels('MAPE / Data Availability', fontsize=20)
        g.set_xlabels('Quality Level', fontsize=20)
        g.add_legend(fontsize=20)
        plt.suptitle(f'{self.device_type} ({self.num_subject}x subject, '
                     f'algo = {self.algo_version})', x=0.4, y=1.05, fontsize=24)

    def make_ba_plot(self):
        '''
        make_ba_plot for each activity
        '''
        sns.set_style('darkgrid')

        temp_table = self.stats_table.copy()
        temp_table['diff'] = temp_table['floyer_hr'] - temp_table['polar_hr']
        temp_table = temp_table[temp_table.activity != 'Unknown'].reset_index()

        def mean_diff_loa(data):
            mean = np.mean(data)
            std = np.std(data)
            loa_high = mean + 1.96 * std
            loa_low = mean - 1.96 * std
            return [loa_high, loa_low]

        g = sns.FacetGrid(temp_table,
                          col='activity',
                          col_wrap=2,
                          height=4,
                          aspect=2
                          )

        g = g.map(plt.scatter, 'polar_hr', 'diff', alpha=0.1)

        axes = g.axes.flatten()

        for i in range(len(axes)):
            activity_temp_table = temp_table[temp_table.activity == g.col_names[i]].reset_index()
            loa_high, loa_low = np.round(
                mean_diff_loa(activity_temp_table['floyer_hr'] - activity_temp_table['polar_hr']), 1)
            mean_bias = round(np.mean(activity_temp_table['floyer_hr'] - activity_temp_table['polar_hr']), 1)
            x = np.arange(40, 200)
            y_mean_bias = [mean_bias] * len(x)
            y_loa_high = [loa_high] * len(x)
            y_loa_low = [loa_low] * len(x)
            axes[i].set_title(g.col_names[i], fontsize=20)
            axes[i].plot(x, y_mean_bias, 'r')
            axes[i].plot(x, y_loa_high, 'g')
            axes[i].plot(x, y_loa_low, 'g')
            axes[i].text(0.12, 0.1,
                         f'Mean bias = {mean_bias},'
                         f'\nLOA High = {loa_high},'
                         f'\nLOA Low = {loa_low}',
                         size='x-large',
                         horizontalalignment='center',
                         verticalalignment='center',
                         transform=axes[i].transAxes)

            # axes[i].text(x[-1], y_mean_bias[-1], 'mean', size='x-large')
            # axes[i].text(x[-1], y_loa_high[-1], 'LOA-h', size='x-large')
            # axes[i].text(x[-1], y_loa_low[-1], 'LOA-l', size='x-large')

        g.set_xticklabels([40, 60, 80, 100, 120, 140, 160, 180, 200], fontsize=20)
        g.set_yticklabels(fontsize=20)
        g.set_xlabels('Polar HR (bpm)', fontsize=20)
        g.set_ylabels('Diff (bpm)', fontsize=20)
        g.set(xlim=(40, 200))
        g.fig.subplots_adjust(wspace=0.1, hspace=0.12)
        plt.suptitle(f'{self.device_type} ({self.num_subject}x subject, '
                     f'algo = {self.algo_version})', x=0.5, y=1.05, fontsize=24)
        plt.legend(bbox_to_anchor=(1.4, 5))

    # Make statistic table
    def create_overview_table(self, quality_level):
        """
        Create a table with all statistical metrics in it
        """
        activity_lst = []
        floyer_mean_lst = []
        floyer_std_lst = []
        polar_mean_lst = []
        polar_std_lst = []
        bias_mean_lst = []
        bias_loa_lst = []
        cor_lst = []
        mape_lst = []
        da_lst = []
        icc_lst = []

        # calculate limits of agreement of the mean of difference
        def mean_diff_loa(data):
            mean = np.mean(data)
            std = np.std(data)
            loa_high = mean + 1.96 * std
            loa_low = mean - 1.96 * std
            return [loa_high, loa_low]

        # conver heart rate table to icc table
        def convert_to_icc_df(temp_table):
            target_lst = []
            rater_lst = []
            score_lst = []

            for i in range(len(temp_table)):
                target_lst += [temp_table['person'][i], temp_table['person'][i]]
                rater_lst += ['floyer', 'polar']
                score_lst += [temp_table['floyer_hr'][i], temp_table['polar_hr'][i]]

            dic = {'target': target_lst,
                   'rater': rater_lst,
                   'score': score_lst
                   }

            icc_df = pd.DataFrame(dic)
            return icc_df

        for activity in np.unique(self.stats_table['activity']):
            if activity == 'Unknown': continue
            temp_table = self.stats_table[(self.stats_table.activity == activity)].reset_index()

            # calculate mape and da
            sum = 0
            count = 0
            for i in range(len(temp_table)):
                if quality_level <= temp_table['quality'][i] <= 100:
                    sum += abs(temp_table['floyer_hr'][i] -
                               temp_table['polar_hr'][i]) / temp_table['polar_hr'][i] * 100
                    count += 1
            mape = round(sum / count, 2)
            da = round(count / len(temp_table), 2) * 100
            mape_lst.append(mape)
            da_lst.append(da)

            # calculate mean and std
            floyer_mean_lst.append(round(temp_table['floyer_hr'].mean()))
            polar_mean_lst.append(round(temp_table['polar_hr'].mean()))
            floyer_std_lst.append(round(temp_table['floyer_hr'].std()))
            polar_std_lst.append(round(temp_table['polar_hr'].std()))
            bias_mean_lst.append(round(np.mean(temp_table['floyer_hr'] - temp_table['polar_hr'])))
            bias_loa_lst.append(np.round(mean_diff_loa(temp_table['floyer_hr'] - temp_table['polar_hr'])))
            cor_lst.append(round(scipy.stats.pearsonr(temp_table['floyer_hr'], temp_table['polar_hr'])[0], 2))

            icc = pg.intraclass_corr(data=convert_to_icc_df(temp_table),
                                     targets='target', raters='rater',
                                     ratings='score').round(2)
            icc_lst.append(icc.set_index('Type')['ICC'][1])

            activity_lst.append(activity)

        dic = {'Activity': activity_lst,
               'Floyer hr_mean': floyer_mean_lst,
               'Floyer hr_std': floyer_std_lst,
               'Polar hr_mean': polar_mean_lst,
               'Polar hr_std': polar_std_lst,
               'Pearson Correlation': cor_lst,
               'Mean Difference (Bias)': bias_mean_lst,
               'Bias 95% LOA': bias_loa_lst,
               'MAPE': mape_lst,
               'DA (%)': da_lst,
               'ICC': icc_lst
               }

        overview_table = pd.DataFrame(data=dic, index=None)
        return overview_table
