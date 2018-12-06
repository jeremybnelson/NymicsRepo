#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
stage.py
"""


# standard lib
import glob
import json
import pathlib
import pickle
import shutil
import time


# common lib
from common import duration


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


# Todo: remove this after migration to daemon.py

class DaemonStop(Exception):
	"""Raise to stop the daemon."""
	pass


class DaemonRestart(Exception):
	"""Raise to restart daemon."""
	pass


class DaemonCancel(Exception):
	"""Raise to cancel current process without restarting or stopping."""
	pass


class Daemon:

	def __init__(self):
		self.trace_flag = False
		self.start_time = 0
		self.counters = collections.defaultdict(int)

		'''
		# listener file name defaults to class name + '.listen'
		self.listener_file_name = self.__class__.__name__.lower() + '.listen'
		'''

		# listener file name defaults to script name + '.listen'
		script_name = sys.argv[0]
		self.listener_file_name = pathlib.Path(script_name).stem + '.listen'

	def get_command(self):
		listener_file_name = self.listener_file_name

		command = ''
		if os.path.exists(listener_file_name):
			# ignore IOError's when attempting to read or delete a locked command file
			try:
				with open(listener_file_name) as input_stream:
					command = input_stream.read()
				os.remove(listener_file_name)
			except IOError:
				pass

		return command

	def listen(self):
		command = self.get_command()
		command = ' '.join(command.split())
		command, separator, message = command.partition(' ')
		command = command.lower()

		if command:
			self.trace(f'Command: {command}({message})')

		method_name = f'do_{command}'
		if hasattr(self, method_name):
			self.count(command)
			getattr(self, method_name)(message)
		elif command:
			self.count('unknown')
			self.unhandled_command(command, message)
		return

	def trace(self, message):
		if self.trace_flag:
			print(message)

	def count(self, counter_name):
		self.counters[counter_name] += 1

	def run(self, *args, **kwargs):
		self.trace(f'Starting daemon - monitoring {self.listener_file_name}')
		self.start_time = int(time.time())
		self.setup(args, kwargs)
		while True:
			self.trace(f'Daemon (re)starting')
			self.start()
			try:
				while True:
					try:
						self.main()
						self.count('run')
					except DaemonCancel:
						pass

			except DaemonRestart:
				pass

			except DaemonStop:
				break

		self.cleanup()
		self.trace(f'Daemon stopped')

	def do_stop(self, message):
		raise DaemonStop

	def do_restart(self, message):
		raise DaemonRestart(message)

	def do_cancel(self, message):
		raise DaemonCancel(message)

	def do_pause(self, message):
		while True:
			time.sleep(0.5)
			command = self.get_command()
			if command in ['stop', 'restart', 'continue']:
				break
			elif command:
				self.trace(f'Command: {command}({message}) (while paused)')

	# noinspection PyUnusedLocal
	def do_uptime(self, message):
		"""Report uptime and running since timestamp."""
		up_time = time.time() - self.start_time
		start_time = datetime.datetime.fromtimestamp(self.start_time)
		self.trace(f'Uptime: {duration(up_time)}; running since {start_time}')

	def do_counters(self, message=''):
		counters = message.split()
		if not counters:
			counters = 'restart cancel run'.split()
		elif counters[0] == '-':
			counters = sorted(self.counters.keys())

		for counter_name in counters:
			self.trace(f'{counter_name.capitalize()}: {self.counters[counter_name]}')

	# noinspection PyUnusedLocal
	def do_help(self, message):
		"""Display commands or help for a specific command."""
		commands = [command[3:] for command in dir(self) if command.startswith('do_')]
		# self.trace(f'Help: {", ".join(commands)}')

		for command in commands:
			method = getattr(self, f'do_{command}')
			command_help = getattr(method, '__doc__')
			if command_help:
				self.trace(f'{command}: {command_help}')

	def unhandled_command(self, command, message):
		self.trace(f'Unhandled command: {command}({message})')

	def setup(self, *args, **kwargs):
		"""Override: Optional setup code (called once)."""
		pass

	def start(self):
		"""Override: Code called on initial start and subsequent restarts."""
		pass

	def main(self):
		"""Override: Main code goes here."""
		pass

	def cleanup(self):
		"""Override: Optional cleanup code."""
		pass


# import stats/stat

def log(message):
	print(message)


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


def convert_to_sqlserver(table_schema, extended_definitions=None):
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

	# make sure work folder exists
	work_folder = 'stage_work'
	if not os.path.exists(work_folder):
		os.mkdir(work_folder)

	# make sure work folder is empty
	for file_name in glob.glob(f'{work_folder}/*'):
		# print(f'Deleting: {file_name}')
		os.remove(file_name)

	# get the posted file
	source_file_name = f'{work_folder}/' + pathlib.Path(object_key).name
	log(f'Getting {source_file_name} from archive::{object_key}')
	archive_objectstore.get(source_file_name, object_key)

	# create the file's namespace schema if missing
	namespace = object_key.split('/')[0]
	job_id = object_key
	db_conn.create_schema(namespace)

	# unzip the file
	shutil.unpack_archive(source_file_name, extract_dir=work_folder)

	# process all table files in our work folder
	for file_name in sorted(glob.glob(f'{work_folder}/*.table')):
		table_name = pathlib.Path(file_name).stem
		print(f'Processing {table_name} ...')

		# # 2018-05-10 workaround for cart schema that changed; fix !!!!
		# if table_name in ['carts', 'groups']:
		# 	continue

		# always load table objects
		input_stream = open(f'{work_folder}/{table_name}.table', 'rb')
		table_object = pickle.load(input_stream)
		input_stream.close()

		# always load table schema
		input_stream = open(f'{work_folder}/{table_name}.schema', 'rb')
		table_schema = pickle.load(input_stream)
		input_stream.close()

		# always load table pk
		input_stream = open(f'{work_folder}/{table_name}.pk')
		table_pk = input_stream.read().strip()
		input_stream.close()

		# extend table object with table table and column names from table_schema object
		table_object.table_name = table_name
		table_object.column_names = [column_name for column_name in table_schema.columns]

		# if drop_table, drop table and exit
		if table_object.drop_table:
			print(f'Table drop request; table_drop=1')
			db_conn.drop_table(namespace, table_name)
			return

		# convert table schema to our target database and add extended column definitions
		extended_definitions = 'udp_jobid int, udp_timestamp datetime2'.split(',')
		convert_to_sqlserver(table_schema, extended_definitions)

		# create target table if it doesn't exist
		if not db_conn.does_table_exist(namespace, table_name):
			# FUTURE: Add udp_pk, udp_nk, udp_nstk and other extended columns
			print(f'Creating table: {namespace}.{table_name}')
			db_conn.create_table_from_table_schema(namespace, table_name, table_schema, extended_definitions)

		# handle cdc vs non-cdc table workflows differently
		print(f'{table_name}.cdc={table_object.cdc}, timestamp={table_object.timestamp}')
		if not table_object.cdc or table_object.cdc.lower() == 'none' or not table_pk:
			# if table cdc=none, drop the target table
			print(f'Table cdc=[{table_object.cdc}]; rebuilding table')
			db_conn.drop_table(namespace, table_name)

			# no cdc in effect for this table - insert directly to target table
			work_folder_obj = pathlib.Path(work_folder)
			batch_number = 0
			for json_file in sorted(work_folder_obj.glob(f'{table_name}#*.json')):
				# load rows from json file
				input_stream = open(json_file)
				rows = json.load(input_stream)
				input_stream.close()

				# insert/upsert/merge *.json into target tables
				if not rows:
					print(f'Table {table_name} has 0 rows; no updates')
				else:
					batch_number += 1
					print(f'Job {job_id}, batch {batch_number}, table {table_name}')

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
				input_stream = open(json_file)
				rows = json.load(input_stream)
				input_stream.close()

				# insert/upsert/merge *.json into target tables
				if not rows:
					print(f'Table {table_name} has 0 rows; no updates')
					break
				else:
					batch_number += 1
					log(f'Job {job_id}, batch {batch_number}, table {table_name}')

					# convert date/datetime columns to date/datetime values
					convert_data_types(rows, table_schema)

					# db_conn.insert_many( namespace, table_name, rows )
					db_conn.bulk_insert_into_table(namespace, temp_table_name, table_schema, rows)
			else:
				# merge (upsert) temp table to target table
				merge_cdc = cdc_merge.MergeCDC(table_object, extended_definitions)
				sql_command = merge_cdc.merge(namespace, table_pk)
				print(sql_command)
				db_conn.cursor.execute(sql_command)

			# drop temp table after merge
			db_conn.drop_table(namespace, temp_table_name)

	print()


def process_next_file_to_stage(db_conn, archive_objectstore, stage_queue):

	# any new arrivals that we can process? job_id=1 or next job in sequence?
	cursor = db_conn.execute('select_from_stage_arrival_queue')
	row = cursor.fetchone()
	if not row:
		return False
	else:
		# get object_key we should fetch for staging
		print(f'Found next file to stage: {row}')
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

	print(f'Archive object store update notification: {notification}')

	# TODO: Add activity_log (vs job_log/stat_log) references.

	# get file key and name
	work_folder = 'stage_work'
	object_key = notification.object_key
	archive_file_name = f'{work_folder}/' + pathlib.Path(object_key).name

	log(f'Getting {archive_file_name} from archive_object_store::{object_key}')
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

	db = database.SQLServerConnection(sql_server_connect)
	db.debug_flag = True
	conn = db.conn
	# cursor = conn.cursor()

	# create udp_staging database if not present; then use
	db_conn = database.Database('sqlserver', conn)
	db_conn.use_database('udp_stage')

	# Todo: These names should come from project file
	# Todo: queue_name should be input_queue_name, output_queue_name
	# archive_object_store_name = 'udp-s3-archive-sandbox'
	# queue_name = 'udp-sqs-archive-sandbox'
	archive_object_store_name = f'{project_object.archive_objectstore}'
	archive_queue_name = f'{project_object.archive_queue}'
	stage_queue_name = f'{project_object.stage_queue}'

	# get connection info
	connect_config = config.Config('conf/_connect.ini', config.ConnectionSection, bootstrap)
	cloud_connection = connect_config.sections[cloud_connect_name]

	archive_object_store = cloud.ObjectStore(archive_object_store_name, cloud_connection)
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
		print(f'{datetime.datetime.today():%Y-%m-%d %H:%M:%S}: Polling for archive updates ...')
		archive_file_found = process_next_file_to_stage(db_conn, archive_object_store, stage_queue)

		# clear archive queue messages
		# TODO: Drop archive queue except as a diagnostic monitoring tool?
		if archive_queue:
			response = archive_queue.get()
			notification = cloud.ObjectStoreNotification(response)
			if notification.message_id:
				if not notification.objectstore_name:
					# unexpected notification - log it, then delete it
					print(f'Ignoring message: {notification.message}')
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
