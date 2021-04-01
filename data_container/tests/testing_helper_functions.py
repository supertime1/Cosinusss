import numpy as np
from data_container import config
import time
from datetime import datetime, timezone
logger = config.logger

def compare_lists(list1, list2, tolerance=0.5):
    # todo: why 0.2 difference sometimes???

    if len(list1) != len(list2):
        return False

    list1 = np.asarray(list1)
    list2 = np.asarray(list2)

    # check if both lists contain integers
    if isinstance(list1[0], (int, np.integer)) and isinstance(list2[0], (int, np.integer)):

        return np.array_equal(list1, list2)

    # at least one list contains float
    else:
        # numpy float values are not that precise to compare elementwise,
        # => see only if the differences are not larger than a certain threshold
        difflist = list1 - list2
        # print(max(difflist))
        if np.all(difflist < tolerance):
            return True
        else:
            return False

def now(df=None, sleep=0.00001):
    # use this to generate x time stamps relative to date_time_start or just the current time
    # introduce an artificial delay to make time stamps in x values separable
    if sleep:
        time.sleep(sleep)

    if not df:
        return datetime.now(timezone.utc)
    else:
        return (datetime.now(timezone.utc) - df.date_time_start).total_seconds()

class EmptyClass:
    
    pass

def eeg_scale_factor():
    # eeg data get's appended as binary but when doing a lazy load, a scale factor is added...

    # conversion of Smarting EEG data
    vref = 4.5
    gain = 24
    scale_factor = (vref / (2 ** 23 - 1)) / gain * 1e+6

    return scale_factor