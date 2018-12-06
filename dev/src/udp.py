#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
udp.py

UDP platform code.
"""


# standard lib
import logging


# common lib
from common import log_setup
from common import log_session_info


# udp classes
from config import ConfigSectionKey
from database import MSSQL


# udp lib
import database


# module level logger
logger = logging.getLogger(__name__)


# global names
udp_stage_database = 'udp_stage'
udp_catalog_schema = 'udp_catalog'


def setup(config):
	# TODO: Create a project file for all udp cloud based ETL scripts (archive, stage, udp, etc)
	# TODO: 2018-06-04 Take database/cloud _connect names from caller's *.project file !!!!!

	# TODO: Add activity_log (vs job_log/stat_log) references.
	# TODO: debug should also enable/disable sql output and timing/record count stats

	# db = database.MSSQL(project.database))
	# conn = db.conn
	# cursor = conn.cursor()
	# db_conn = database.Database('mssql', conn)

	connection = config('database:udp_aws_stage_01_datalake')
	db = MSSQL(connection)
	db_conn = database.Database('mssql', db.conn)

	# create data stage database if not present; then use
	db_conn.create_database(udp_stage_database)
	db_conn.use_database(udp_stage_database)

	# create data catalog schema if not present
	db_conn.create_schema(udp_catalog_schema)

	# create data catalog tables if not present
	db_conn.create_named_table(udp_catalog_schema, 'nst_lookup')
	db_conn.create_named_table(udp_catalog_schema, 'job_log')
	db_conn.create_named_table(udp_catalog_schema, 'stat_log')
	db_conn.create_named_table(udp_catalog_schema, 'table_log')
	db_conn.create_named_table(udp_catalog_schema, 'stage_arrival_queue')
	db_conn.create_named_table(udp_catalog_schema, 'stage_pending_queue')


# test code
def main():
	# load standard config
	config = ConfigSectionKey('conf', 'local')
	config.load('bootstrap.ini', 'bootstrap')
	config.load('init.ini')
	config.load('connect.ini')
	setup(config)


# test code
if __name__ == '__main__':
	log_setup()
	log_session_info()
	main()
