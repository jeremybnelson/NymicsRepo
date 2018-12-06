#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_common.py
"""


# standard libs
import datetime
import filecmp
import logging
import os
import psutil
import shutil
from stat import S_IREAD, S_IRGRP, S_IROTH, S_IWUSR



# common lib
from common import duration
from common import datetime_to_iso
from common import iso_to_datetime
from common import datetime_to_seconds
from common import seconds_to_datetime
from common import now
from common import trim_seconds
from common import is_file
from common import is_file_readonly
from common import file_size
from common import file_create_datetime
from common import file_modify_datetime
from common import copy_file_if_exists
from common import delete_file
from common import delete_files
from common import move_file
from common import rename_file
from common import is_folder
from common import clear_folder
from common import create_folder
from common import delete_folder
from common import move_folder
from common import rename_folder

from common import boot_datetime
from common import diskspace_available
from common import diskspace_used
from common import memory_available
from common import memory_total


from common import to_int

from common import save_text

# 3rd party libs
import pytest


# module level logger
logger = logging.getLogger(__name__)
# test_folder_path = '../tmp'

# there's a better way att
# @pytest.fixture

# need a better name, too generic, eg. you may have multiple types of setups
# you should also code a tear down to insure you leave test environment in
# a known state of "cleanliness"

# GOOD START on the internal logic

# Globally declare test_folder_path variable
test_folder_path = '../tmp'


def setup_test_files():
    # ensure clean (empty) ../tmp folder
    teardown_test_files()

    # set up files
    create_folder(test_folder_path)
    readonly_file_name = f'{test_folder_path}/readonly.txt'
    readwrite_file_name = f'{test_folder_path}/readwrite.txt'

    # ... OR BEST ... leverage common's save_text()
    save_text(readwrite_file_name, 'Hello world')

    # create a read only file
    save_text(readonly_file_name, 'Hello world')

    # do this type of operation after a file is closed
    os.chmod(readonly_file_name, S_IREAD | S_IRGRP | S_IROTH)

    # create a working dir in tmp folder
    create_folder(f'{test_folder_path}/working')

    # TODO: A file created outside of your folder that you don't have read or write permission
    # since this file will be static, it could sit in a hard coded location and
    # be set up once outside of the script or your script could build and execute
    # a shell script (batch or PowerShell) to create this permission challenged file

    # jeremy notes: Powershell script to
    #  1) create/change user
    #  2) create a permission challenged folder in tmp
    #  3) change back to current user


def teardown_test_files():
    # set the read_only.txt file to read/write so it can be deleted if it exists
    readonly_file_name = f'{test_folder_path}/readonly.txt'
    if is_file(readonly_file_name):
        os.chmod(readonly_file_name, S_IWUSR | S_IREAD)

    # clear the test/working folder
    # DISCUSS: We may want to remove entire folder and recreate in case there are sub-folders?
    # eg, clear_folder(test_folder_path)
    clear_folder(f'{test_folder_path}/')


# AWESOME!
def test_duration():
    assert duration(60) == '60.0 sec(s)'
    assert duration(300) == '5.0 min(s)'
    assert duration(3600) == '60.0 min(s)'
    assert duration(3601) == '1.0 hour(s)'
    assert duration(86400) == '24.0 hour(s)'
    assert duration(86401) == '1.0 day(s)'


# AWESOME!
def test_datetime_to_iso():
    # add a test for a datetime.datetime
    date_value = datetime.date(year=2018, month=12, day=1)
    iso_date = datetime_to_iso(date_value)
    assert iso_date == '2018-12-01'


# AWESOME!
def test_iso_to_datetime():
    iso_date = '2018-12-01'
    iso_datetime = '2018-11-21T19:53:12+00:00'
    iso_datetime_utc = '2018-11-21T19:53:12Z'
    converted_iso_date = iso_to_datetime(iso_date)
    converted_iso_datetime = iso_to_datetime(iso_datetime)
    converted_iso_datetime_utc = iso_to_datetime(iso_datetime_utc)

    # converted_iso_date assertions
    assert converted_iso_date.year == 2018
    assert converted_iso_date.month == 12
    assert converted_iso_date.day == 1

    # converted_iso_datetime assertions
    assert converted_iso_datetime.year == 2018
    assert converted_iso_datetime.month == 11
    assert converted_iso_datetime.day == 21

    # converted_iso_datetime_utc assertions
    assert converted_iso_datetime_utc.year == 2018
    assert converted_iso_datetime_utc.month == 11
    assert converted_iso_datetime_utc.day == 21


# AWESOME!
def test_datetime_to_seconds():
    datetime_datetime = datetime.datetime(year=2018, month=12, day=1, hour=1, minute=1, second=1, microsecond=1)
    sec_date = datetime_to_seconds(datetime_datetime)
    assert sec_date == 1543651261.000001


# AWESOME!
def test_seconds_to_datetime():
    datetime_date = seconds_to_datetime(1543651261.000001)
    assert datetime_date.year == 2018
    assert datetime_date.month == 12
    assert datetime_date.day == 1
    assert datetime_date.hour == 1
    assert datetime_date.minute == 1
    assert datetime_date.second == 1
    assert datetime_date.microsecond == 1


# VERY GOOD!
def test_now():
    assert isinstance(now(), datetime.datetime)


def test_trim_seconds():
    # sec_date has non-zero values for second and microsecond
    sec_date = datetime.datetime(year=2018, month=12, day=1, hour=1, minute=1, second=59, microsecond=59)
    trimmed_sec_date = trim_seconds(sec_date)

    # no_sec_date has zero values for second and microsecond
    zero_sec_date = datetime.datetime(year=2018, month=12, day=1, hour=1, minute=1, second=0, microsecond=0)
    trimmed_zero_sec_date = trim_seconds(zero_sec_date)

    # sec_date assertions
    assert trimmed_sec_date.second == 0
    assert trimmed_sec_date.microsecond == 0

    # no_sec_date assertions
    assert trimmed_zero_sec_date.second == 0
    assert trimmed_zero_sec_date.microsecond == 0


# file tests
# create assertion for permission challenged, open (unclosed), and read only file for all file tests
# try creating files with different modes (x, r, w, w+) and write assertions for them
def test_is_file():
    setup_test_files()
    assert is_file(f'{test_folder_path}/readonly.txt') is True
    assert is_file(f'{test_folder_path}/ThisDoesNotExist.txt') is False
    teardown_test_files()


def test_is_file_readonly():
    setup_test_files()
    assert is_file_readonly(f'{test_folder_path}/readonly.txt') is True
    assert is_file_readonly(f'{test_folder_path}/readwrite.txt') is False
    teardown_test_files()


def test_file_size():
    setup_test_files()
    assert file_size(f'{test_folder_path}/readwrite.txt') == 11
    # Need to either add logic to common to deal with file not existing
    # OR need to read from strerr and assert this check evaluates FileNotFoundError
    # assert file_size('C:/udp-app-master/dev/tests/working/this_not_does_exist.txt') == 0
    teardown_test_files()


def test_file_create_datetime():
    setup_test_files()
    assert isinstance(file_create_datetime(f'{test_folder_path}/readwrite.txt'), datetime.datetime) is True
    teardown_test_files()


def test_file_modify_datetime():
    setup_test_files()
    assert isinstance(file_modify_datetime(f'{test_folder_path}/readwrite.txt'), datetime.datetime) is True
    teardown_test_files()


# file operation tests
def test_copy_file_if_exists():
    setup_test_files()

    copy_file_if_exists(f'{test_folder_path}/readwrite.txt',
                        f'{test_folder_path}/readwrite_copy.txt')
    assert filecmp.cmp(f'{test_folder_path}/readwrite.txt'
                       , f'{test_folder_path}/readwrite_copy.txt') is True
    teardown_test_files()


def test_delete_file():
    setup_test_files()

    # Set the readonly.txt file to read/write so it can be deleted
    readonly_file_name = f'{test_folder_path}/readonly.txt'
    if is_file(readonly_file_name):
        os.chmod(readonly_file_name, S_IWUSR | S_IREAD)

    # delete both the readonly.txt and the readwrite.txt files
    delete_file(f'{test_folder_path}/readonly.txt')
    delete_file(f'{test_folder_path}/readwrite.txt')

    # assert both files have been deleted
    assert is_file(f'{test_folder_path}/readwrite.txt') is False
    assert is_file(f'{test_folder_path}/readonly.txt') is False

    teardown_test_files()


def test_delete_files():
    setup_test_files()

    # Set the readonly.txt file to read/write so it can be deleted
    readonly_file_name = f'{test_folder_path}/readonly.txt'
    if is_file(readonly_file_name):
        os.chmod(readonly_file_name, S_IWUSR | S_IREAD)

    # delete both the readonly.txt and the readwrite.txt files
    delete_files(f'{test_folder_path}/*.txt')

    # assert both files have been deleted
    assert is_file(f'{test_folder_path}/readwrite.txt') is False
    assert is_file(f'{test_folder_path}/readonly.txt') is False

    teardown_test_files()


def test_move_file():
    setup_test_files()

    move_file(f'{test_folder_path}/readwrite.txt',
              f'{test_folder_path}/working/readwrite_copy.txt')
    assert is_file(f'{test_folder_path}/working/readwrite_copy.txt') is True

    teardown_test_files()


def test_rename_file():
    setup_test_files()

    rename_file(f'{test_folder_path}/readwrite.txt', f'{test_folder_path}/readwrite_renamed.txt')
    assert is_file(f'{test_folder_path}/readwrite_renamed.txt') is True

    teardown_test_files()


# folder operations
def test_is_folder():
    setup_test_files()

    assert is_folder(f'{test_folder_path}/working') is True
    assert is_folder(f'{test_folder_path}/DoesNotExist') is False

    teardown_test_files()


def test_clear_folder():
    setup_test_files()
    # ToDo: add exception handling test
    # set the read_only.txt file to read/write so it can be deleted if it exists
    readonly_file_name = f'{test_folder_path}/readonly.txt'
    if is_file(readonly_file_name):
        os.chmod(readonly_file_name, S_IWUSR | S_IREAD)

    clear_folder(f'{test_folder_path}/')
    assert len(os.listdir(f'{test_folder_path}/')) == 0

    teardown_test_files()


def test_create_folder():
    setup_test_files()

    create_folder(f'{test_folder_path}/createdfolder')
    # set up folders list
    folders = []
    # find all folders in the tmp dir and append them to the folders list
    for folder in (objects for objects in os.listdir(f'{test_folder_path}/') if os.path.isdir(os.path.join(f'{test_folder_path}/', objects))):
        folders.append(folder)
    # sort the list alphabetically
    folders.sort()

    # assert that the directories we expect exist
    assert folders[0] == 'createdfolder'
    assert folders[1] == 'working'
    assert len(folders) == 2

    teardown_test_files()


def test_delete_folder():
    setup_test_files()

    # delete the working dir inside tmp dir
    delete_folder(f'{test_folder_path}/working')
    # set up folders list
    folders = []
    # find all folders in the tmp dir and append them to the folders list
    for folder in (objects for objects in os.listdir(f'{test_folder_path}/') if
                   os.path.isdir(os.path.join(f'{test_folder_path}/', objects))):
        folders.append(folder)
    assert len(folders) == 0

    teardown_test_files()


def test_move_folder():
    setup_test_files()

    # delete the working dir inside tmp dir
    create_folder(f'{test_folder_path}/FolderToBeMoved')
    move_folder(f'{test_folder_path}/FolderToBeMoved', f'{test_folder_path}/working/FolderToBeMoved')
    # set up folders list
    tmp_folders = []
    working_folders = []
    # find all folders in the tmp dir and append them to the folders list
    for folder in (objects for objects in os.listdir(f'{test_folder_path}/') if
                   os.path.isdir(os.path.join(f'{test_folder_path}/', objects))):
        tmp_folders.append(folder)

    # find all folders in the tmp dir and append them to the folders list
    for folder in (objects for objects in os.listdir(f'{test_folder_path}/working/') if
                   os.path.isdir(os.path.join(f'{test_folder_path}/working/', objects))):
        working_folders.append(folder)

    assert tmp_folders[0] == 'working'
    assert len(tmp_folders) == 1
    assert working_folders[0] == 'FolderToBeMoved'
    assert len(working_folders) == 1

    teardown_test_files()


def test_rename_folder():
    setup_test_files()

    rename_folder(f'{test_folder_path}/working', f'{test_folder_path}/renamed')
    # set up folders list
    folders = []
    # find all folders in the tmp dir and append them to the folders list
    for folder in (objects for objects in os.listdir(f'{test_folder_path}/') if
                   os.path.isdir(os.path.join(f'{test_folder_path}/', objects))):
        folders.append(folder)
    assert len(folders) == 1
    assert folders[0] == 'renamed'

    teardown_test_files()


# host machine properties section
def test_boot_datetime():
    # 1 = 1 test. This is here in to detect changes in the memory_available() logic
    assert boot_datetime() == seconds_to_datetime(psutil.boot_time())
    # standard assertions
    assert isinstance(boot_datetime(), datetime.datetime) is True
    # ToDo: Maybe add a few tests to check the boot time is within a certain range??


def test_diskspace_available():
    setup_test_files()
    # 1 = 1 test. This is here in to detect changes in the memory_available() logic
    assert diskspace_available(path_name=f'{test_folder_path}/') == shutil.disk_usage(f'{test_folder_path}/')[2]
    # standard assertions
    assert diskspace_available(path_name=f'{test_folder_path}/') > 0
    assert diskspace_available(path_name=f'{test_folder_path}/') < 150000000000
    assert isinstance(diskspace_available(), int) is True
    teardown_test_files()


def test_diskspace_used():
    setup_test_files()
    # 1 = 1 test. This is here in to detect changes in the memory_available() logic
    assert diskspace_used(path_name=f'{test_folder_path}/') == shutil.disk_usage(f'{test_folder_path}/')[1]
    # standard assertions
    assert diskspace_used(path_name=f'{test_folder_path}/') > 0
    assert diskspace_used(path_name=f'{test_folder_path}/') < 120000000000
    assert isinstance(diskspace_used(), int) is True
    teardown_test_files()


def test_memory_available():
    # 1 = 1 test. This is here in to detect changes in the memory_available() logic
    assert memory_available() == psutil.virtual_memory().available
    # standard assertions
    assert memory_available() > 0
    assert memory_available() < 10000000000
    assert memory_available() == psutil.virtual_memory().total - psutil.virtual_memory().used
    assert isinstance(memory_available(), int) is True


def test_memory_total():
    # 1 = 1 test. This is here in to detect changes in the memory_total() logic
    assert memory_total() == psutil.virtual_memory().total
    # standard assertions
    assert memory_total() > 0
    assert memory_total() < 10000000000
    assert memory_total() == psutil.virtual_memory().free + psutil.virtual_memory().used
    assert isinstance(memory_total(), int) is True


# int operations
def test_to_int():
    assert to_int('012') == 12
    assert to_int('01234.4321') == 1234
    assert to_int("01234.9") == 1234
    assert to_int('-1234') == -1234
    assert to_int('abc', 'FailedToConvert', False) == 'FailedToConvert'
    assert to_int(-32.54e7, 'FailedToConvert', False) == -325400000
    assert to_int(70.2e5, 'FailedToConvert', False) == 7020000


# list operations ...
def test_delete_empty_entries():
    pass


def test_is_empty():
    pass


def test_is_sequence():
    pass

"""
def to_list(obj, delimiters=', '):
    pass


# object operations ...
def describe(obj, attribute_names):
    pass


# path operations ...
def actual_file_name(file_name):
    pass


def full_path(path_name):
    pass


def just_path(path_name):
    pass


def just_file_name(path_name):
    pass


def just_file_stem(path_name):
    pass


def just_file_ext(path_name):
    pass


def parent_path(path_name):
    pass


def path_with_separator(path_name):
    pass


def strip_file_ext(path_name):
    pass


def strip_file_alias(file_name):
    pass


def strip_path_delimiter(path_name):
    pass


# process operations ...
def is_process(pid):
    pass


def process_memory_used():
    pass


def process_memory_percentage():
    pass


# script operations ...


def script_name():
    pass


def script_path():
    pass


# serializer operations ...


def _json_serializer(obj):
    pass


def export_json(file_name, obj):
    pass


def load_jsonpickle(file_name):
    pass


def save_jsonpickle(file_name, obj):
    pass


def load_lines(file_name, default='', line_count=1, encoding='UTF8'):
    pass


def load_text(file_name, default=None, encoding='UTF8'):
    pass


def save_text(file_name, text):
    pass


# string operations (encoding/decoding) ....


def decode_uri(text):
    pass


def decode_entities(text):
    pass


# string operations (cleanup) ...
def compress_char(text, char):
    pass


def compress_whitespace(text, preserve_indent=False):
    pass


def delete_blank_lines(text):
    pass


def get_indentation(text):
    pass


def all_trim(text, item, case_sensitive=True):
    pass


def left_trim(text, item, case_sensitive=True):
    pass


def right_trim(text, item, case_sensitive=True):
    pass


def strip_c_style_comments(text):
    pass


# string operations (expressions/formatting) ...


def expand(expression):
    pass


def make_fdqn(name):
    pass


def make_key(*items, delimiter=':'):
    pass


def quote(items):
    pass


def spaces(n):
    pass


# string operations (parsing) ...


def get_lines(text, line_count):
    pass


def key_value(text):
    pass


def option_value(text):
    pass


def split(items, delimiters=', ', strip=True):
    pass


# string operations (regular expressions and glob patterns) ...


def case_insensitive_replace(text, old, new):
    pass


def delete_regex_pattern(regex_pattern, text):
    pass


def expand_template(text, key_value_dict, left_delimiter='{%', right_delimiter='%}'):
    pass


def extract_matches(text, left_delimiter, right_delimiter):
    pass


def is_glob_match(glob_pattern, text):
    pass


def is_glob_pattern(text):
    pass

# type operations ...


def is_function(obj):
    pass


# xml/html operations ...


def get_attrs(element):
    pass


def get_tag(element):
    pass

"""



