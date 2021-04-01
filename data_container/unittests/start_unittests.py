#!/usr/bin/env python3
import pytest

# start this script to run all tests (python files that start with "test_" (test_basic_unittests.py)
# in the fixture for the first two tests, in the "def fix_id_data_dic():" you can adjust the df_id (currently id = 6891)
# to any rawdata file on the server where you have access to. Make sure you have the lab_api_client_confg.json setup an
# that the lab_api_client is working properly

# With Pycharm you can start single tests in test_data_container_2.py so you don't have to run all of them

# make sure to install
# sudo pip3 install pytest
# sudo pip3 install pytest-html => html extensions so that the results will be in a nice html file (click on the test to expand the results)

# pytest.ini configures the test (and for example the html extension.
# Remove "--html=results.html" if you don't want to use the html extension.
# The output will be logged into the unittest_log.log file, too. The level can also be specified.


if __name__ == '__main__':
    pytest.main()