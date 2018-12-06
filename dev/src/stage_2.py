#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
stage_2.py (next version)


FUTURE:
- Track change type (I, U)
- Track update version (udp_row_version +1) on every update cycle
- Add optional history table - new records, changed records, deleted records
- Track U, I, D metrics and times
- History and udp_row_version info lost on schema change ???
- Split into load changes (insert to temp tables) and single transaction of merges (server side, async)

FUTURE:
- Split audit into capture side audit steps and stage side steps


TODO: Preserve diagnostics on stage failure and block future updates via block on pending queue!!!
Item remains in pending queue until blocking condition(s) cleared. If job fails on re-run, then
pending will block again.

Operational note: If a particular update is bad, eg. a problem on the source side was the root cause
or if the capture process had a bug, we need an easy way to do the following in a single auditiable
step. By auditiable step I mean a step that gets logged to our stat log as an event.

Steps:

0. Stop the source capture process. We could do this by updating SDLC specific project file
with a pause_project = 1 value and promoting this as a new version. Capture process will continue running
but when time to run happens, we will see the pause request and skip this update, logging why we did this.
Later on, after we fix the problem at source, or release a new capture, we will update the project
file and clear the pause_project setting (set to 0 or delete the entire line), promote an update
and the new code and updated project file will run. We can do this entire workflow without having to
login to the capture server and manually stop and restart the capture process. The other benefit of
the pause_project setting is that a server reboot and site start of many processes will start all
projects. If we manually stopped a project, then it could be accidentally restarted if the server
rebooted.

1. Set a capture setting to ignore namespace updates from <namespace> until this temp blocker gets
cleared. This makes sure that interim captures don't sneak in while we're cleaning up. This prevents
archive from updating stat_log for this <namespace> as well.

2. Clear <namespace> capture files from failure point forward. If we blocked stage updates for awhile
then we could have large set of queued capture updates archived.

3. Clear <namespace> capture file entries from capture arrival/pending queues.

4. Clear <namespace> capture/archive entries from stat_log that cover the capture range covered.

5. Fix problem on source/capture side.

6. Reset the capture <namespace>'s state file to reset the last_time value to the timestamp of the
last good capture. We can optionally reset the job id to match the next good job id or keep the job
id current (there will be a gap in our stat_log for capture jobs we deleted).

7. Restart the capture process (if manually stopped and commented out of site file) or update the
project's pause_project setting, promote, and have the capture re-read the new project settings.


TODO: 2018-08-15 IMPORTANT !!!
- recognize first_time capture events and drop-if-exist target table
- first_time capture events will happen if a first_time date moves forward or backward
- OR when we want to reload a table because of a schema change or project table:<name> metadata change
- like ignore_columns changing the columns affected

TODO: When capture detects a schema change it should reset first_time event so that
stage knows to drop and recreate table and our stat_log should update a new column that
is not present that indicates an initial load = 0,1

When we have to re-load a table from archive capture history, we only need to go back to the
most recent initial load and load from there !!!!

TODO: Need a capture side command to drop stage tables.
drop_table = 1

This drops table from stage side. This [table:<table-name>] section can remain in the project file
or it can be deleted entirely.

IMPORTANT: This command should also drop the first, last, and current timestamps from our state file
and clear this table's schema/pk info from our state file/object as well.

Operational workflow: If a source table schema changes or is updated without updating timestamps,
we can reload a single table by setting its drop_table = 1, waiting for an update, and then
re-adding the [table:<table-name>] block to the project file and the table will appear to the
capture system as a new table.

We could encapsulate this workflow in a drop-table.cmd script that would add the drop_table=1
statement to the project file immediately after the [table:<table-name>] section and then
clear the table's references from our state file (table object).

"""


# standard lib
import glob
import logging
import pathlib
import time


# common lib
from common import FileList
from common import clear_folder
from common import duration
from common import extract_archive
from common import just_file_name
from common import load_json
from common import load_text


# udp lib
import cdc_merge
import cloud_aws as cloud
import config
import database
import tableschema
import udp


# 3rd party lib
import arrow


# daemon - customized for stage - refactor
import collections
import datetime
import os
import sys


# module level logger
logger = logging.getLogger(__name__)


# import stats/stat


def convert_data_types(rows, table_schema):
	for column_index, column in enumerate(table_schema.columns.values()):
		# print(f'{column.column_name} ({column.data_type}) (index {column_index})')

		# convert all date, datetime, time strings to datetime values
		if column.data_type in ('date', 'datetime', 'datetime2', 'smalldatetime', 'time'):
			# print(f'Converting {column.column_name} ({column.data_type}) (index {column_index}) to datetime')
			for row in rows:
				if row[column_index] is not None:
					# shorten high precision values to avoid ODBC Datetime field overflow errors
					if len(row[column_index]) > 23:
						row[column_index] = row[column_index][0:25]
					row[column_index] = arrow.get(row[column_index]).datetime

		# make sure nvarchar are really strings
		if column.data_type == 'nvarchar':
			# print(f'Converting {column.column_name} ({column.data_type}) (index {column_index}) to str')
			for row in rows:
				if row[column_index] is not None:
					row[column_index] = str(row[column_index])


'''
ARRAY: cast(<column> as text)
BIGINT
BOOLEAN: cast(<column> as integer)
CHARACTER VARYING
DATE
INTEGER
JSONB: cast(<column> as text)
TEXT
TIMESTAMP WITHOUT TIME ZONE
USER DEFINED: cast(<column> as text)
UUID: cast(<column> as text)
'''


def convert_to_mssql(table_schema, extended_definitions=None):
	# add extended definitions if present
	if extended_definitions:
		for definition in extended_definitions:
			column = tableschema.Column()
			column.column_name = definition.split()[0]
			column.data_type = definition.split()[1]
			table_schema.columns[column.column_name] = column

	for column_name, column in table_schema.columns.items():
		data_type = column.data_type.lower()

		if data_type == 'array':
			column.data_type = 'nvarchar'
			column.character_maximum_length = 512

		elif data_type == 'bigint':
			column.data_type = 'bigint'

		elif data_type == 'boolean':
			column.data_type = 'tinyint'

		elif data_type == 'character varying':
			column.data_type = 'nvarchar'
			column.character_maximum_length = 768

		# PostgreSQL/SQL Server
		elif data_type == 'date':
			column.data_type = 'date'

		elif data_type == 'integer':
			column.data_type = 'int'

		elif data_type == 'jsonb':
			column.data_type = 'nvarchar'
			column.character_maximum_length = -1

		# PostgreSQL/SQL Server
		elif data_type == 'text':
			column.data_type = 'nvarchar'
			column.character_maximum_length = -1

		elif data_type == 'timestamp without time zone':
			column.data_type = 'datetime2'
			column.datetime_precision = 7

		elif data_type in ('user defined', 'user-defined'):
			column.data_type = 'nvarchar'
			column.character_maximum_length = 128

		elif data_type == 'uuid':
			column.data_type = 'nvarchar'
			column.character_maximum_length = 36

		else:
			# pass it through as native sql server type
			# int (all sizes), money (all sizes), decimal/numeric, float/real
			pass


def stage_file(db_conn, archive_objectstore, object_key):

	# make sure work folder exists and is empty
	work_folder = 'stage_work'
	clear_folder(work_folder)
	if not os.path.exists(work_folder):
		os.mkdir(work_folder)

	# get the posted file
	source_file_name = f'{work_folder}/' + just_file_name(object_key)
	logger.info(f'Getting {source_file_name} from archive::{object_key}')
	archive_objectstore.get(source_file_name, object_key)

	# create the file's namespace schema if missing
	namespace = object_key.split('/')[0]
	job_id = object_key
	db_conn.create_schema(namespace)

	# unzip the file
	# shutil.unpack_archive(source_file_name, extract_dir=work_folder)
	file_names = FileList(source_file_name)
	file_names.include('*')
	extract_archive(source_file_name, work_folder, file_names)

	# process all table files in our work folder
	for file_name in sorted(glob.glob(f'{work_folder}/*.table')):
		table_name = pathlib.Path(file_name).stem
		logger.info(f'Processing {table_name} ...')

		# TODO: rename files to use a _table, _schema suffix and .json file extension

		# always load table objects
		# input_stream = open(f'{work_folder}/{table_name}.table', 'rb')
		# table_object = pickle.load(input_stream)
		# input_stream.close()
		table_object = load_json(f'{work_folder}/{table_name}.table')

		# always load table schema
		# input_stream = open(f'{work_folder}/{table_name}.schema', 'rb')
		# table_schema = pickle.load(input_stream)
		# input_stream.close()
		table_schema = load_json(f'{work_folder}/{table_name}.schema')

		# always load table pk
		# input_stream = open(f'{work_folder}/{table_name}.pk')
		# table_pk = input_stream.read().strip()
		# input_stream.close()
		table_pk = load_text(f'{work_folder}/{table_name}.pk').strip()

		# extend table object with table table and column names from table_schema object
		table_object.table_name = table_name
		table_object.column_names = [column_name for column_name in table_schema.columns]

		# if drop_table, drop table and exit
		if table_object.drop_table:
			logger.info(f'Table drop request; table_drop=1')
			db_conn.drop_table(namespace, table_name)
			return

		# convert table schema to our target database and add extended column definitions
		extended_definitions = 'udp_jobid int, udp_timestamp datetime2'.split(',')
		convert_to_mssql(table_schema, extended_definitions)

		# 2018-09-12 support custom staging table type overrides
		# [table].table_type = < blank > | standard, columnar, memory, columnar - memory


		# create target table if it doesn't exist
		if not db_conn.does_table_exist(namespace, table_name):
			# FUTURE: Add udp_pk, udp_nk, udp_nstk and other extended columns
			logger.info(f'Creating table: {namespace}.{table_name}')
			db_conn.create_table_from_table_schema(namespace, table_name, table_schema, extended_definitions)

		# handle cdc vs non-cdc table workflows differently
		logger.debug(f'{table_name}.cdc={table_object.cdc}, timestamp={table_object.timestamp}')
		if not table_object.cdc or table_object.cdc.lower() == 'none' or not table_pk:
			# if table cdc=none, drop the target table
			logger.info(f'Table cdc=[{table_object.cdc}]; rebuilding table')
			db_conn.drop_table(namespace, table_name)

			# no cdc in effect for this table - insert directly to target table
			work_folder_obj = pathlib.Path(work_folder)
			batch_number = 0
			for json_file in sorted(work_folder_obj.glob(f'{table_name}#*.json')):
				# load rows from json file
				# input_stream = open(json_file)
				# rows = json.load(input_stream)
				# input_stream.close()
				rows = load_json(json_file)

				# insert/upsert/merge *.json into target tables
				if not rows:
					logger.info(f'Table {table_name} has 0 rows; no updates')
				else:
					batch_number += 1
					logger.info(f'Job {job_id}, batch {batch_number}, table {table_name}')

					# convert date/datetime columns to date/datetime values
					convert_data_types(rows, table_schema)

					# db_conn.insert_many( namespace, table_name, rows )
					db_conn.bulk_insert_into_table(namespace, table_name, table_schema, rows)

		else:
			# table has cdc updates

			# create temp table to receive captured changes
			# FUTURE: Create a database wrapper function for creating 'portable' temp table names vs hard-coding '#'.
			temp_table_name = f'_{table_name}'
			db_conn.drop_table(namespace, temp_table_name)

			# print(f'namespace = {namespace}')
			# print(f'temp_table_name = {temp_table_name}')
			# print(f'table_object = {dir(table_object)}')
			# print(f'extended definitions = {extended_definitions}')

			db_conn.create_table_from_table_schema(namespace, temp_table_name, table_schema, extended_definitions)

			# insert captured updates into temp table
			work_folder_obj = pathlib.Path(work_folder)
			batch_number = 0
			for json_file in sorted(work_folder_obj.glob(f'{table_name}#*.json')):
				# load rows from json file
				# input_stream = open(json_file)
				# rows = json.load(input_stream)
				# input_stream.close()
				rows = load_json(json_file)

				# insert/upsert/merge *.json into target tables
				if not rows:
					logger.info(f'Table {table_name} has 0 rows; no updates')
					break
				else:
					batch_number += 1
					logger.info(f'Job {job_id}, batch {batch_number}, table {table_name}')

					# convert date/datetime columns to date/datetime values
					convert_data_types(rows, table_schema)

					# db_conn.insert_many( namespace, table_name, rows )
					db_conn.bulk_insert_into_table(namespace, temp_table_name, table_schema, rows)
			else:
				# merge (upsert) temp table to target table
				merge_cdc = cdc_merge.MergeCDC(table_object, extended_definitions)
				sql_command = merge_cdc.merge(namespace, table_pk)

				# TODO: Capture SQL commands in a sql specific log.
				logger.debug(sql_command)
				db_conn.cursor.execute(sql_command)

			# drop temp table after merge
			db_conn.drop_table(namespace, temp_table_name)


def process_next_file_to_stage(db_conn, archive_objectstore, stage_queue):

	# any new arrivals that we can process? job_id=1 or next job in sequence?
	cursor = db_conn.execute('select_from_stage_arrival_queue')
	row = cursor.fetchone()
	if not row:
		return False
	else:
		# get object_key we should fetch for staging
		logger.info(f'Found next file to stage: {row}')
		archive_file_name = row.archive_file_name
		job_id = int(archive_file_name.split('.')[0].rsplit('#', 1)[-1])
		namespace = archive_file_name.rsplit('#', 1)[0]
		object_key = f'{namespace}/{archive_file_name}'

		# stage the file we found
		stage_file(db_conn, archive_objectstore, object_key)

		# after archive capture file processed then remove it from arrival/pending queues
		db_conn.execute('delete_from_stage_arrival_queue', archive_file_name)
		db_conn.execute('delete_from_stage_pending_queue', archive_file_name)

		# post the next file in sequence for namespace to pending queue
		next_archive_file_name = f'{namespace}#{job_id+1:09}.zip'
		next_file = dict(archive_file_name=next_archive_file_name)
		db_conn.insert_into_table('udp_catalog', 'stage_pending_queue', **next_file)

		# post a message to stage queue that namespace has been updated
		if stage_queue:
			stage_queue.put(archive_file_name)

		# FUTURE: Update schedule's poll message
		# last_job_info = f'last job {self.job_id} on {datetime.datetime.now():%Y-%m-%d %H:%M}'
		# schedule_info = f'schedule: {self.schedule}'
		# self.schedule.poll_message = f'{script_name()}({self.namespace}), {last_job_info}, {schedule_info}'

		# return True to indicate we should continue processing queued up archived files
		return True


def stage_archive_file(archive_object_store, notification):

	logger.info(f'Archive object store update notification: {notification}')

	# TODO: Add activity_log (vs job_log/stat_log) references.

	# get file key and name
	work_folder = 'stage_work'
	object_key = notification.object_key
	archive_file_name = f'{work_folder}/' + just_file_name(object_key)

	logger.info(f'Getting {archive_file_name} from archive_object_store::{object_key}')
	archive_object_store.get(archive_file_name, object_key)

	# TODO: Insert stage file code block here
	# unzip archive_file_name
	# load schema info so we can build target tables if missing
	# loop through all <table>.json files
	# conditional: cdc = timestamp vs none vs ...
	# note: if no nk/pk then cdc must be none - override and log warning
	# load json data
	# create target table if missing with capture (udp_jobid, udp_jobpk, udp_timestamp) and stage (udp_pk, udp_nk)
	# create #source table
	# insert into #source via insertmany()
	# update #source with udp_nk using pk from capture
	# merge #source to target on udp_nk
	# update job_log, table_log with stats

	# return name of processed file
	archive_file_name = object_key.rsplit('-', 1) + '/' + object_key
	return archive_file_name


# main
def main():

	# bootstrap configuration settings
	bootstrap = config.Bootstrap()
	bootstrap.debug_flag = False
	bootstrap.load('conf/init.ini')
	bootstrap.load('conf/bootstrap.ini')

	# project
	project_name = 'udp_aws_stage_01_etl'
	project_config = config.Config(f'conf/{project_name}.project', config.ProjectSection, bootstrap)
	project_config.debug_flag = False
	project_config.dump()
	project_object = project_config.sections['stage_project']

	# make sure core database environment in place
	udp.setup()

	# get references to stage database and catalog schema
	# udp_stage_database = udp.udp_stage_database
	# udp_catalog_schema = udp.udp_catalog_schema

	# connections
	database_connect_name = f'{project_object.database}'
	cloud_connect_name = f'{project_object.cloud}'

	# SQL Server
	connect_config = config.Config('conf/_connect.ini', config.ConnectionSection, bootstrap)
	sql_server_connect = connect_config.sections[database_connect_name]

	db = database.MSSQL(sql_server_connect)
	db.debug_flag = True
	conn = db.conn
	# cursor = conn.cursor()

	# create udp_staging database if not present; then use
	db_conn = database.Database('mssql', conn)
	db_conn.use_database('udp_stage')

	# Todo: These names should come from project file
	# Todo: queue_name should be input_queue_name, output_queue_name
	# archive_objectstore_name = 'udp-s3-archive-sandbox'
	# queue_name = 'udp-sqs-archive-sandbox'
	archive_objectstore_name = f'{project_object.archive_objectstore}'
	archive_queue_name = f'{project_object.archive_queue}'
	stage_queue_name = f'{project_object.stage_queue}'

	# get connection info
	connect_config = config.Config('conf/_connect.ini', config.ConnectionSection, bootstrap)
	cloud_connection = connect_config.sections[cloud_connect_name]

	archive_object_store = cloud.ObjectStore(archive_objectstore_name, cloud_connection)
	if project_object.archive_queue:
		archive_queue = cloud.Queue(archive_queue_name, cloud_connection)
	else:
		archive_queue = None

	if project_object.stage_queue:
		stage_queue = cloud.Queue(stage_queue_name, cloud_connection)
	else:
		stage_queue = None

	# main poll loop
	while True:
		# logger.info(f'{datetime.datetime.today():%Y-%m-%d %H:%M:%S}: Polling for archive updates ...')
		archive_file_found = process_next_file_to_stage(db_conn, archive_object_store, stage_queue)

		# clear archive queue messages
		# TODO: Drop archive queue except as a diagnostic monitoring tool?
		if archive_queue:
			response = archive_queue.get()
			notification = cloud.ObjectStoreNotification(response)
			if notification.message_id:
				if not notification.objectstore_name:
					# unexpected notification - log it, then delete it
					logger.debug(f'Ignoring message: {notification.message}')
					archive_queue.delete(notification.message_id)
				else:
					# archive_file_name = stage_archive_file(archive_object_store, notification)
					archive_queue.delete(notification.message_id)

		# poll if we didn't find an archived file, otherwise keep processing
		if not archive_file_found:
			# poll
			time.sleep(int(project_object.poll_frequency))


# main
if __name__ == '__main__':
	main()
