#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
capture_2.py (next version)

Capture data from a source database based on following configuration files.
- <namespace>.project
- <namespace>.tables

Cloud (S3 capture bucket) and database connections sourced from:
- init.ini
- bootstrap.ini (AWS cloud conf for current SDLC environment)
- upd._connect

Options
--onetime - run once/immediately, use when this script called via external scheduler
--nowait - don't wait for internal scheduler to trigger; execute immediately and then follow regular schedule
--notransfer - don't transfer captured data to objectstore; use for local testing

TODO:
-

"""


# standard lib
import contextlib
import datetime
import fnmatch
import json
import logging
import os
import pathlib
import pickle
import shutil
import socket
import sys


# common lib
from common import create_archive
from common import copy_file_if_exists
from common import clear_folder
from common import create_folder
from common import delete_files
from common import describe
from common import expand
from common import hash_file
from common import hash_files
from common import hash_str
from common import iso_to_datetime
from common import json_load
from common import json_save
from common import json_serializer
from common import key_value
from common import save_json
from common import script_name
from common import split


# udp classes
from cloud_aws import Objectstore
from daemon import Daemon


# udp lib
import cdc_select
import database


# module level logger
logger = logging.getLogger(__name__)


# job.ini - job_id counter and last_run_time for range checks
# allow last_run_time to be overridden for new tables being onboarded

# table definition parser - need a test mode as well
# .name
# .create
# .pull (capture, extract, ...) - SQL with <job>, <pk>, <timestamp> markers ???
# .pk (expression)
# .timestamp (expression)
# .columns (pulled from create)
# .types (pulled from create)
# .starttime (initial start time, used if table.history doesn't have table reference)
# .namespace (pulled from parent folder)


class Stat:

	def __init__(self, stat_name, stat_type=None):
		self.stat_stage = pathlib.Path(sys.argv[0]).stem
		self.stat_name = stat_name
		self.stat_type = stat_type
		self.start_time = None
		self.end_time = None
		self.run_time = 0
		self.row_count = 0
		self.data_size = 0

	def start(self):
		self.start_time = datetime.datetime.now()
		logger.info(f'{self.stat_name.capitalize()} started ...')

	def stop(self, row_count=0, data_size=0):
		self.end_time = datetime.datetime.now()
		self.run_time = (self.end_time - self.start_time).total_seconds()
		self.row_count = row_count
		self.data_size = data_size
		logger.info(f'{self.stat_name.capitalize()} complete in {self.run_time} secs ({self.row_count:,} records, {self.data_size:,} bytes)')

	def row(self):
		""""Return stat properties as a dict that can be round tripped via JSON."""
		row = dict()
		row['stat_name'] = self.stat_name
		row['stat_type'] = self.stat_type
		row['start_time'] = self.start_time
		row['end_time'] = self.end_time
		row['run_time'] = self.run_time
		row['row_count'] = self.row_count
		row['data_size'] = self.data_size
		return row


# script > version > instance > server > account/user > namespace > job > step_name > step_type (job, step, table)
class Stats:

	def __init__(self, file_name=None, namespace=None, job_id=None, script_instance=None):
		self.file_name = file_name
		self.stats = dict()

		# script version
		script_timestamp = pathlib.Path(sys.argv[0]).stat().st_mtime
		script_datetime = datetime.datetime.fromtimestamp(script_timestamp)
		script_version = f'{script_datetime:%Y-%m-%d %H:%M}'

		# system info
		self.script_name = pathlib.Path(sys.argv[0]).stem
		self.script_version = script_version
		self.script_instance = script_instance
		self.server_name = socket.gethostname()
		self.account_name = os.getlogin()

		# current job info
		self.namespace = namespace
		self.job_id = job_id

		# extra columns added to output
		extra_columns = 'script_name, script_version, script_instance, server_name, service_name, namespace, job_id'
		self.extra_columns = [column_name.strip() for column_name in extra_columns]

	def start(self, stat_name, stat_type=None):
		self.stats[stat_name] = Stat(stat_name, stat_type)
		self.stats[stat_name].start()

	def stop(self, stat_name, row_count=0, data_size=0):
		self.stats[stat_name].stop(row_count, data_size)

	# TODO: stat_type = job, step (extract, compress, upload)
	# save stat info in a json file format to preserve data types
	def save(self, file_name=None):
		# make name and path of log output an option
		# also allow saving in a json vs csv format (with column header row)
		if not file_name and not self.file_name:
			file_name = 'job.log'
		elif not file_name:
			file_name = self.file_name

		rows = []
		for stat_name, stat in self.stats.items():
			row = dict()

			# session wide properties
			row['script_name'] = self.script_name
			row['script_version'] = self.script_version
			row['script_version'] = self.script_version
			row['script_instance'] = self.script_instance
			row['server_name'] = self.server_name
			row['account_name'] = self.account_name
			row['namespace'] = self.namespace
			row['job_id'] = self.job_id

			# merge in stat properties
			row = {**row, **stat.row()}

			# save the row for output
			rows.append(row)

		# save the output
		output_stream = open(file_name, 'w')
		json.dump(rows, output_stream, indent=2, default=json_serializer)
		output_stream.close()


class TableHistory:

	def __init__(self, table_name):
		self.table_name = table_name
		self.last_timestamp = None
		self.last_rowversion = None
		self.last_filehash = None

	def __str__(self):
		# return f'{self.table_name}: last_timestamp={self.last_timestamp}, last_filehash={self.last_filehash}'
		return describe(self, 'table_name, last_timestamp, last_filehash')


class JobHistory:

	def __init__(self, file_name):
		self.file_name = file_name

		self.job_id = 1
		self.tables = dict()

	def __str__(self):
		return describe(self, 'job_id, tables')

	def dump(self):
		for table_name in sorted(self.tables):
			logger.info(self.get_table_history(table_name))

	def load(self):
		if not pathlib.Path(self.file_name).exists():
			# file doesn't exist, initialize object with default values
			logger.info(f'Initializing {self.file_name}')
			self.job_id = 1
			self.tables = dict()
		else:
			logger.info(f'Loading {self.file_name}')

			"""
			with open(self.file_name, 'rb') as input_stream:
				obj = pickle.load(input_stream)
			"""
			obj = json_load(self.file_name)

			# load key attributes
			self.job_id = obj.job_id
			self.tables = obj.tables

	def save(self):
		logger.info(f'Saving file {self.file_name}')

		# increment job_id on save
		self.job_id += 1
		with open(self.file_name, 'wb') as output_stream:
			pickle.dump(self, output_stream)

	def get_table_history(self, table_name):
		table_name = table_name.lower()
		if table_name in self.tables:
			table_history = self.tables[table_name]
		else:
			table_history = TableHistory(table_name)
			self.tables[table_name] = table_history

		logger.info(table_history)
		return table_history

	# updating get_table_history()'s returned table_history object updates original in self.tables[]
	def set_table_history(self, table_history):
		# self.tables[table_history.table_name] = table_history
		pass



class CaptureDaemon(Daemon):

	def __init__(self):
		super().__init__()

		# common conf folder
		self.option = None
		self.config_folder_name = 'conf'

		# job specific folders
		self.state_folder_name = 'capture_state'
		self.work_folder_name = 'capture_work'
		self.publish_folder_name = 'capture_publish'

		# job specific files
		self.capture_file_name = None
		self.zip_file_name = None

		# capture specific properties
		self.project_name = None
		self.project = None
		self.namespace = None
		self.connect_config = None
		self.table_config = None
		self.database = None
		self.stats = None
		self.job_id = None

		# overall job stats
		self.job_row_count = 0
		self.job_file_size = 0

	def setup(self):
		# get project name
		if len(sys.argv) == 1:
			raise Exception('No project file supplied')

		# bootstrap configuration settings
		bootstrap = config.Bootstrap()
		bootstrap.debug_flag = self.is_debug()
		bootstrap.load('conf/init.ini')
		bootstrap.load('conf/bootstrap.ini')

		# project name and file
		project_name = sys.argv[1].lower()
		self.project_name = project_name
		project_file = pathlib.Path(f'{self.config_folder_name}/{project_name}.project')
		if not project_file.exists():
			raise Exception(f'Project file does not exist ({project_file.stem})')

		# project conf
		project_config = config.Config(str(project_file), config.ProjectSection, bootstrap)
		project = project_config.sections['capture_project']
		self.project = project

		# build namespace
		# TODO: 2018-05-29 > add SDLC suffix ???
		namespace_components = []
		for attr in 'entity location system instance subject'.split():
			namespace_components.append(getattr(project, attr))
		self.namespace = '_'.join(namespace_components)

		# tables conf
		tables_file = pathlib.Path(f'{self.config_folder_name}/{project_name}.tables')
		if not tables_file.exists():
			raise Exception(f'Tables file does not exist ({tables_file})')
		self.table_config = config.Config(str(tables_file), config.TableSection, bootstrap)

		# _connect conf
		connect_file = pathlib.Path(f'{self.config_folder_name}/_connect.ini')
		self.connect_config = config.Config(str(connect_file), config.ConnectionSection, bootstrap)

		# database info
		self.database = self.connect_config.sections[project.database]


	def current_timestamp(self, db_engine):
		"""Return current timestamp for capture CDC date range with step back and fast forward logic."""

		# determine current timestamp based on data source's database server time
		current_timestamp = db_engine.current_timestamp()

		# adjust current timestamp back one minute (step back) to account for open transactions
		current_timestamp = current_timestamp - datetime.timedelta(minutes=1)

		# always begin a capture pull on an integer second boundary
		current_timestamp = current_timestamp.replace(microsecond=0)

		logger.info(f'Current timestamp: {current_timestamp}')

		return current_timestamp

	def process_table(self, db, db_engine, schema_name, table_name, table_object, table_history, current_timestamp):
		"""Process a specific table."""

		# skip default table and ignored tables
		if table_name == 'default':
			return
		elif table_object.ignore_table:
			logger.info(f'Skipping table: {table_name} (ignore_table=1)')
			return
		elif table_object.drop_table:
			logger.info(f'Skipping table: {table_name} (drop_table=1)')
			return

		# initialize table history's last time stamp to first timestamp if not set yet
		if not table_history.last_timestamp:
			# default first timestamp to 1900-01-01 if project has no first timestamp
			if not table_object.first_timestamp:
				table_object.first_timestamp = '1900-01-01'
			table_history.last_timestamp = iso_to_datetime(table_object.first_timestamp)

		# skip table if last timestamp > current timestamp, eg. tables pre-configured for the future
		if table_history.last_timestamp > current_timestamp:
			explanation = f'first/last timestamp {table_history.last_timestamp} > current timestamp {current_timestamp}'
			logger.info(f'Skipping table: {table_name} ({explanation})')
			return

		# if we're here then we have a legit last timestamp value to use for CDC
		last_timestamp = table_history.last_timestamp

		self.stats.start(table_name, 'table')
		# logger.info(f'Processing {table_name} ...')

		# create a fresh cursor for each table
		cursor = db.conn.cursor()

		# save table object for stage
		output_stream = open(f'{self.work_folder_name}/{table_name}.table', 'wb')
		pickle.dump(table_object, output_stream)
		output_stream.close()

		# discover table schema
		table_schema = db_engine.select_table_schema(schema_name, table_name)

		# remove ignored columns from table schema
		if table_object.ignore_columns:
			# find columns to ignore (remove) based on ignore column names/glob-style patterns
			ignore_columns = []
			for column_name in table_schema.columns:
				for pattern in split(table_object.ignore_columns):
					# use fnmatch() to provide glob style matching
					if fnmatch.fnmatch(column_name.lower(), pattern.lower()):
						ignore_columns.append(column_name)

			# delete ignored columns from our table schema
			for column_name in ignore_columns:
				logger.info(f'Ignore_column: {table_name}.{column_name}')
				table_schema.columns.pop(column_name)

		# save table schema for stage to use
		output_stream = open(f'{self.work_folder_name}/{table_name}.schema', 'wb')
		pickle.dump(table_schema, output_stream)
		output_stream.close()

		# save table pk for stage to use
		pk_columns = db_engine.select_table_pk(schema_name, table_name)
		if not pk_columns and table_object.primary_key:
			pk_columns = table_object.primary_key
		output_stream = open(f'{self.work_folder_name}/{table_name}.pk', 'w')
		output_stream.write(pk_columns)
		output_stream.close()

		# clear cdc if it doesn't match timestamp/rowversion
		table_object.cdc = table_object.cdc.lower()
		if not table_object.cdc or table_object.cdc not in ('timestamp', 'rowversion'):
			table_object.cdc = ''

		# if no pk_columns, then clear table cdc
		if not pk_columns:
			if table_object.cdc and table_object.cdc != 'none':
				logger.info(f'Warning: {table_name} cdc={table_object.cdc} but table has no pk column(s)')
				table_object.cdc = 'none'

			# we still keep timestamp because its required for filtering first_timestamp - current_timestamp
			# if table_object.timestamp:
			# 	logger.info(f'Warning: {table_name} timestamp={table_object.timestamp} but table has no pk column(s)')
			# 	table_object.timestamp = ''

		# update table object properties for cdc select build
		column_names = list(table_schema.columns.keys())
		table_object.schema_name = schema_name
		table_object.table_name = table_name
		table_object.column_names = column_names
		select_cdc = cdc_select.SelectCDC(table_object)
		sql = select_cdc.select(self.job_id, current_timestamp, last_timestamp)

		# logger.info(f'Capture SQL:\n{sql}\n')

		# run sql here vs via db_engine.capture_select
		# cursor = db_engine.capture_select(schema_name, table_name, column_names, last_timestamp, current_timestamp)
		cursor.execute(sql)

		# capture rows in fixed size batches to support unlimited size record counts
		# Note: Batching on capture side allows stage to insert multiple batches in parallel.

		if self.project.batch_size:
			batch_size = int(self.project.batch_size)
			# logger.info(f'Using project specific batch size: {self.project.batch_size}')
		else:
			batch_size = 1_000_000

		batch_number = 0
		row_count = 0
		file_size = 0
		while True:
			batch_number += 1
			rows = cursor.fetchmany(batch_size)
			if not rows:
				break

			logger.info(f'Table({table_name}): batch={batch_number} using batch size {batch_size:,}')

			# flatten rows to list of column values
			json_rows = [list(row) for row in rows]
			output_file = f'{self.work_folder_name}/{table_name}#{batch_number:04}.json'
			with open(output_file, 'w') as output_stream:
				# indent=2 for debugging
				json.dump(json_rows, output_stream, indent=2, default=json_serializer)

			# track stats
			row_count += len(json_rows)
			file_size += pathlib.Path(output_file).stat().st_size

		# if no cdc, but order set, do a file hash see if output the same time as last file hash
		if (not table_object.cdc or table_object.cdc == 'none') and table_object.order:
			print(f'Checking {table_name} file hash based on cdc={table_object.cdc} and order={table_object.order}')
			table_data_files = f'{self.work_folder_name}/{table_name}#*.json'
			current_filehash = hash_files(table_data_files)
			if table_history.last_filehash == current_filehash:
				# suppress this update
				print(f'Table({table_name}): identical file hash, update suppressed')
				logger.info(f'Table({table_name}): identical file hash, update suppressed')
				row_count = 0
				file_size = 0

				# delete exported json files
				delete_files(table_data_files)
			else:
				print(f'Table({table_name}): {table_history.last_filehash} != {current_filehash}')
				table_history.last_filehash = current_filehash

		# update table history with new last timestamp value
		table_history.last_timestamp = current_timestamp

		# track total row count and file size across all of a table's batched json files
		self.stats.stop(table_name, row_count, file_size)

		# save interim state of stats for diagnostics
		self.stats.save()

		self.job_row_count += row_count
		self.job_file_size += file_size

		# explicitly close cursor when finished
		# cursor.close()
		return

	def compress_work_folder(self):
		"""Compress all files in work_folder to single file in publish_folder."""

		# setup
		self.stats.start('compress', 'step')
		self.capture_file_name = f'{self.namespace}#{self.job_id:09}'
		self.zip_file_name = f'{self.publish_folder_name}/{self.capture_file_name}'

		# copy capture_state files to work folder to be included in capture zip package as well
		copy_file_if_exists(f'{self.state_folder_name}/last_job.log', self.work_folder_name)

		# compress
		# Note: zip_file_name gets updated by make_archive()
		self.zip_file_name = shutil.make_archive(self.zip_file_name, format='zip', root_dir=self.work_folder_name)

		# finish
		zip_file_size = pathlib.Path(self.zip_file_name).stat().st_size
		self.stats.stop('compress', 0, zip_file_size)

	def upload_to_objectstore(self):
		"""Upload publish_folder's <namespace>-<job_id>.zip to objectstore."""

		# don't upload captured data if we're in --notransfer mode
		if self.option('notransfer') == '1':
			return

		# setup
		self.stats.start('upload', 'step')
		cloud_connection = self.connect_config.sections[self.project.cloud]
		capture_objectstore = Objectstore(self.project.capture_objectstore, cloud_connection)
		objectstore_file_name = f'{self.namespace}/{self.capture_file_name}.zip'

		# upload
		# capture_objectstore.put(f'{self.publish_folder_name}/{self.capture_file_name}.zip', objectstore_file_name)
		capture_objectstore.put(self.zip_file_name, objectstore_file_name)

		# finish
		zip_file_size = pathlib.Path(self.zip_file_name).stat().st_size
		self.stats.stop('upload', 0, zip_file_size)

	def save_recovery_state_file(self):

		# don't upload captured data if we're in --notransfer mode
		if self.option('notransfer') == '1':
			return

		# FUTURE: Save recovery file in capture.zip file and have archive extract and push back to namespace folder.
		# This way capture_state.zip is only updated AFTER its container file has been successfully archived.

		# setup
		cloud_connection = self.connect_config.sections[self.project.cloud]
		capture_objectstore = Objectstore(self.project.capture_objectstore, cloud_connection)
		objectstore_file_name = f'{self.namespace}/capture_state.zip'

		# create capture_state archive file
		zip_file_name = f'{self.publish_folder_name}/capture_state'
		zip_file_name = shutil.make_archive(zip_file_name, format='zip', root_dir=self.state_folder_name)

		# upload
		capture_objectstore.put(zip_file_name, objectstore_file_name)

		# finish

	def main(self):
		db = None
		try:
			# get job id and table history
			job_history_file_name = f'{self.state_folder_name}/capture.job'
			job_history = JobHistory(job_history_file_name)
			job_history.load()
			job_id = job_history.job_id
			self.job_id = job_id
			logger.info(f'\nCapture job {job_id} for {self.namespace} ...')

			# track job (and table) stats
			self.stats = Stats(f'{self.work_folder_name}/job.log', namespace=self.namespace, job_id=job_id)
			self.stats.start('capture', 'job')

			# track overall job row count and file size
			self.job_row_count = 0
			self.job_file_size = 0

			# create/clear job folders
			create_folder(self.state_folder_name)
			clear_folder(self.work_folder_name)
			clear_folder(self.publish_folder_name)

			# _connect to source database
			db = None
			db_engine = None
			if self.database.platform == 'postgresql':
				db = database.PostgreSQL(self.database)
				db_engine = database.Database('postgresql', db.conn)

			elif self.database.platform == 'mssql':
				db = database.MSSQL(self.database)
				db_engine = database.Database('mssql', db.conn)

			# cursor = db.conn.cursor()

			# determine current timestamp for this job's run

			# get current_timestamp() from source database with step back and fast forward logic
			current_timestamp = self.current_timestamp(db_engine)

			# process all tables
			self.stats.start('extract', 'step')
			for table_name, table_object in self.table_config.sections.items():
				table_history = job_history.get_table_history(table_name)
				self.process_table(db, db_engine, self.database.schema, table_name, table_object, table_history, current_timestamp)
			self.stats.stop('extract', self.job_row_count, self.job_file_size)

			# save interim job stats to work_folder before compressing this folder
			self.stats.stop('capture', self.job_row_count, self.job_file_size)
			self.stats.save()

			# compress work_folder files to publish_folder zip file
			self.compress_work_folder()

			# upload publish_folder zip file
			self.upload_to_objectstore()

			# save final stats for complete job run
			self.stats.stop('capture', self.job_row_count, self.job_file_size)
			self.stats.save(f'{self.state_folder_name}/last_job.log')
			self.stats.save()

			# update job_id and table histories
			job_history.save()

			# compress capture_state and save to capture objectstore for recovery
			self.save_recovery_state_file()

			# update schedule's poll message
			last_job_info = f'last job {self.job_id} on {datetime.datetime.now():%Y-%m-%d %H:%M}'
			schedule_info = f'schedule: {self.schedule}'
			self.schedule.poll_message = f'{script_name()}({self.namespace}), {last_job_info}, {schedule_info}'

		# force unhandled exceptions to be exposed
		except Exception:
			logger.exception('Unexpected exception')
			raise

		finally:
			# explicitly close database connection when finished with job
			with contextlib.suppress(Exception):
				db.conn.close()


# test
def main():
	# main
	daemon = CaptureDaemon()
	daemon.debug_flag = True
	daemon.run()


# main
if __name__ == '__main__':
	# FUTURE: Re-wrap in app.py and daemon.py (class wrappers for NT Service).
	main()
