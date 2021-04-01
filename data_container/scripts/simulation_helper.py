import random
from random import randint
import string
from datetime import timedelta, date
import pickle

def random_mac():
    mac = "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}".format(
            random.randint(0, 255),
            random.randint(0, 255),
            random.randint(0, 255),
            random.randint(0, 255),
            random.randint(0, 255),
            random.randint(0, 255)
            ).upper()
    return mac

def seconds_to_time_string(seconds):
    # convert seconds to minutes/hours/days and add some random microseconds to it...
    min, sec = divmod(seconds, 60)
    hour, min = divmod(min, 60)
    return "%02d:%02d:%02d.%06d" % (hour, min, sec, 0)

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

    # usage:
    # start_date = date(2013, 1, 1)
    # end_date = date(2015, 6, 2)
    # for single_date in daterange(start_date, end_date):
    #     print(single_date.strftime("%Y-%m-%d"))

def generate_person():

    raspi_mac = random_mac()
    raspi_serial = ''.join(random.choice(string.ascii_uppercase + string.digits) for i in range(6))
    person_hash = ''.join(random.choice(string.ascii_uppercase + string.digits) for i in range(6))
    device_serial = ''.join(random.choice(string.ascii_uppercase + string.digits) for i in range(6))

    meta = {}
    meta['person_hash'] = person_hash
    meta['receiver_serial'] = raspi_serial
    meta['receiver_mac'] = raspi_mac
    meta['receiver_model'] = 'labclient'
    meta['device_serial'] = device_serial

    return meta

def generate_persons(number):

    list = []

    for i in range(number):
        list.append(generate_person())

    return list

if __name__ == '__main__':
    # use this to generate and save person hashes with corresponding device information...

    person_list = generate_persons(500)

    pickle.dump(person_list, open('simulation/person_list.pickle', 'wb'))