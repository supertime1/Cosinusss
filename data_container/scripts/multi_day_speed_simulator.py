#!/usr/bin/env python3
import time
from datetime import datetime, date, timezone, timedelta
from random import randint, uniform
from threading import Thread
import json
import numpy as np
from mongoengine import connect, disconnect
from pathlib import Path

from data_container.odm import Config, Scope, User, Project, Person, Receiver, Device, EventLog
from data_container.data_file import DataFile
from data_container.db_sync import DB_Sync
from data_container.scripts.simulation_helper import random_mac, seconds_to_time_string, daterange
simulation_config = json.load(open('multi_day_speed_simulator_config.json', 'r'))
from data_container import config #, DBSync

logger = config.logger
logger.setLevel('DEBUG')

def now():
    return datetime.now(tz=timezone.utc)


class LabClientMultiDaySpeedSimulator(Thread):

    def __init__(self, receiver_serial):

        super().__init__(name='LabClientMultiDaySpeedSimulator_' + receiver_serial)

        # take configuration parameters
        self.server = simulation_config['server']
        self.receiver_serial = receiver_serial
        self.db_name = simulation_config['simulation_database'] + '_' + self.receiver_serial
        self.db_path = Path('simulation_data' + '_' + self.receiver_serial)
        if not self.db_path.exists():
            self.db_path.mkdir()

        self.simulation_speed = simulation_config['simulation_speed']
        self.df = None
        self.df_hash = None
        self.current_date = None
        self.last_recording = None
        self.last_live_interval = None
        self.recording_ongoing = False
        self.counter = 0

        self.rec_cycles_time = simulation_config['rec_cycles_time']
        self.rec_cycles_step = simulation_config['rec_cycles_step']
        self.send_df = simulation_config['send']
        self.send_partially = simulation_config['send_partially']
        self.send_rawdata_partially = simulation_config['send_rawdata_partially']
        self.generate_rawdata = simulation_config['generate_rawdata']

        # every person's random start value
        self.base_heart_rate = randint(60, 95)
        self.base_quality = randint(40, 70)
        self.base_spo2 = randint(91, 99)
        self.base_temp = uniform(36.5, 39.0)
        self.base_respiration_rate = randint(9, 22)
        self.battery_level = randint(30, 100)

        # clean local database
        disconnect('default')
        db = connect(self.db_name)
        db.drop_database(self.db_name)

        # init of dc and api_client

        self.api_client = DB_Sync(server=self.server, receiver_serial=self.receiver_serial)
        # self.api_client = DBSync()
        # self.api_client.server = self.server
        # self.api_client.username = 'receiver:' + self.receiver_serial
        # self.api_client.password = 'xxx'
        # self.api_client.receiver_serial = self.receiver_serial

        while True:
            self.logger.debug(f'{self.name} attempting to request database from server {self.server}')
            #if self.api_client.db_sync():
            if self.api_client.request_server_db():
                break
            time.sleep(3)

        self.receiver = Receiver.objects(_hash_id=self.receiver_serial).get()
        if self.receiver.receiver_config and hasattr(self.receiver.receiver_config, 'live_data_interval') and self.receiver.receiver_config.live_data_interval:
            self.live_data_interval = self.receiver.receiver_config.live_data_interval
        else:
            self.live_data_interval = None

    def check_and_open_datafile(self):

        if not self.df:
            self.df = DataFile.objects(_hash_id=self.df_hash).first()
            self.df.live_data = True
            # self.df.server = self.server
            # self.df.api_client = self.api_client

    def force_end(self):

        self.close_and_send_df()
        self.logger.info(f'{self.name}: Simulation terminated.')
        exit()

    def new_df(self):

        self.df = DataFile()
        self.df.date_time_start = now()
        self.df.live_data = True
        self.df.server = self.server
        # self.df.api_client = self.api_client
        try:
            self.df.project = self.receiver.persons[0].project
        except IndexError:
            self.logger.error(f'{self.name} got no person assigned. Exiting simulation')
            exit()

        self.df.person = self.receiver.persons[0]
        self.df.receiver = self.receiver
        self.df.device = self.receiver.persons[0].devices[0]
        self.df.store()
        # self.df.server = self.server
        self.df.add_combined_columns(['ppg_ir', 'ppg_red'], 'ppg_ir_red')
        self.df.add_column('heart_rate')
        self.df.add_column('quality')
        self.df.add_column('battery')
        self.df.add_column('spo2')
        self.df.add_column('perfusion_ir')
        self.df.add_column('temperature')
        self.df.store()
        self.df_hash = self.df.hash_id

    def close_and_send_df(self):

        self.recording_ongoing = False
        self.df.close(send=self.send_df)
        if self.send_df:
            send_counter = 0
            while not self.df.check_all_slices_sent():

                self.logger.info(f'{self.name}: Found unsent slices. sending again')
                # self.api_client.push_df(df=self.df)
                self.df.close(send=self.send_df)
                time.sleep(10)
                send_counter += 1
                if send_counter > 10:
                    pass
                    self.logger.warning(f'{self.name} could not send df {self.df} within 10 attempts. Continuing without sending')

    def x(self):

        return (now() - self.df.date_time_start).total_seconds()

    def within_record_interval(self):

        if not self.last_recording:
            # no recoring for this df so far...
            time_diff_to_last_recording = 0
            self.last_recording = now()
        else:
            time_diff_to_last_recording = (now() - self.last_recording).total_seconds()

        if 0 <= time_diff_to_last_recording <= self.rec_cycles_time:
            response = True
            self.recording_ongoing = True
        else:
            response = False

        return response

    def send_live_data(self):

        if self.live_data_interval:

            if not self.last_live_interval:
                time_diff_to_last_live_data = 0
                self.last_live_interval = now()
            else:
                time_diff_to_last_live_data = (now() - self.last_live_interval).total_seconds()

            if time_diff_to_last_live_data > self.live_data_interval:
                self.df.send_json()
                self.last_live_interval = now()

    def decrease_battery(self):

        if self.counter % 180 == 0:
            self.battery_level -= 1
        if self.battery_level <= 0:
            self.battery_level = 100

        self.counter += 1

        if self.counter > 180:
            self.counter = 0

    def run(self):

        while True:

            # still same day => record data
            if self.current_date == now().date():

                # record data
                if self.within_record_interval():

                    # open the file at the beginning of the recording (if not already open)
                    self.check_and_open_datafile()

                    # append random data
                    self.df.append_value('heart_rate', randint(self.base_heart_rate, self.base_heart_rate + 2), self.x())
                    self.df.append_value('quality', randint(self.base_quality, self.base_quality + 10), self.x())
                    self.df.append_value('battery', int(self.battery_level), self.x())
                    self.df.append_value('spo2', randint(self.base_spo2, self.base_spo2 + 1), self.x())
                    self.df.append_value('perfusion_ir', uniform(0.5, 1.5), self.x())
                    self.df.append_value('respiration_rate', randint(self.base_respiration_rate, self.base_respiration_rate + 2), self.x())
                    self.df.append_value('temperature', uniform(self.base_temp, self.base_temp + 0.3), self.x())

                    if simulation_config['generate_rawdata']:
                        # create 200 Hz ppg signal
                        ppg_hz = 200
                        base_x = self.x()
                        for rd_point in np.linspace(0, 0.9999999999, ppg_hz):
                            # use the whole 24 bits
                            self.df.append_value('ppg_ir_red', [randint(0, 2 ** 24 - 1), randint(0, 2 ** 24 - 1)], base_x + rd_point)

                    # send live data
                    self.send_live_data()

                    # decrease battery
                    self.decrease_battery()

                if (now() - self.last_recording).total_seconds() > self.rec_cycles_time and self.recording_ongoing:
                    # end chunk, store and send
                    self.df.chunk_stop()
                    self.recording_ongoing = False

                    if self.send_df:
                        # self.df.send(partially=simulation_config['send_partially'], rawdata=simulation_config['send_rawdata_partially'])
                        # self.api_client.push_df(df=self.df, partially=self.send_partially, rawdata=self.send_rawdata_partially)
                        self.df.send(partially=self.send_partially, rawdata=self.send_rawdata_partially)

                    self.api_client.request_server_db()
                    # print(self.df.stats_slices)
                    # forget the datafile until next recording starts
                    if simulation_config['close_and_open_df']:
                        self.df = None

                # allow next recording
                if (now() - self.last_recording).total_seconds() > self.rec_cycles_step:
                    self.last_recording = now()

                # seconds += 1
                time.sleep(1 / self.simulation_speed)

            # new day => new df
            else:
                # todo: dont do this if there is a recording going on
                self.current_date = now().date()
                if self.df:
                    self.check_and_open_datafile()
                    self.close_and_send_df()
                self.new_df()
                self.last_recording = None
                self.last_live_interval = None
