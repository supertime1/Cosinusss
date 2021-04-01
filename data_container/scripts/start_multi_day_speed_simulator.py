#!/usr/bin/env python3
import sys
import time
import os
from pathlib import Path
import json

file_path = Path(os.path.dirname(os.path.realpath(__file__)))
if file_path != "":
    os.chdir(file_path)
sys.path.append(str(file_path.parents[1]))

from data_container import config
from data_container.scripts.multi_day_speed_simulator import LabClientMultiDaySpeedSimulator

simulation_config = json.load(open('multi_day_speed_simulator_config.json', 'r'))

logger = config.logger
logger.info('########################################################################')
logger.info('START SIMULATION')

for receiver_hash in simulation_config['receiver_serials']:

    sim = LabClientMultiDaySpeedSimulator(receiver_hash)
    logger.info(f'Continue Simulation: {sim.name} of receiver_hash {receiver_hash}')

    sim.start()
    time.sleep(simulation_config['delay_between_simulations'])


# Monitor CPU / RAM
# pid = os.getpid()
# while True:
#
#     py = psutil.Process(pid)
#     memoryUse = py.memory_info()[0] / 2. ** 30
#     cpu = py.cpu_percent()
#     print(f'CPU: {cpu}%, RAM: {memoryUse}')
#
#     time.sleep(1)
