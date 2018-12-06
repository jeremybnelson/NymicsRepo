#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
archive.py

"""


# standard lib
import glob
import json
import pathlib
import time
import zipfile


# udp lib
import cloud_aws as cloud
import config
import database
import udp


# 3rd party lib
import arrow


# daemon - customized for archive - refactor
import collections
import datetime
import os
import sys


def duration(seconds):
	seconds = int(seconds)
	if seconds > (60*60*24):
		duration_text = f'{seconds/(60*60*24):.1f} day(s)'
	elif seconds > (60*60):
		duration_text = f'{seconds/(60*60):.1f} hour(s)'
	elif seconds > 60:
		duration_text = f'{seconds/60:.1f} min(s)'
	else:
		duration_text = f'{seconds} sec(s)'
	return duration_text


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


def test():

	# test data
	connect_name = 'aws_amc_capture_dev'
	object_store_name = 'udp-s3-capture-amc-sandbox'

	# get connection info
	connect_config = config.Config('conf/_connect.ini', config.ConnectionSection)
	cloud_connection = connect_config.sections[connect_name]

	# push test files to capture bucket to give polling loop something to test
	object_store = cloud.ObjectStore(object_store_name, cloud_connection)
	object_store.put('test1.data', 'test/test1.data')
	time.sleep(2)
	object_store.put('test1.data', 'test/test2.data')
	time.sleep(2)
	object_store.put('test1.data', 'test/test3.data')
	time.sleep(2)

	# not required - drop: sleep for 5 seconds to give notification messages time to post to queue
	# time.sleep(5)


def archive_capture_file(archive_object_store, cloud_connection, notification, db_conn):

	log(f'Notification: {notification}')

	# get name of file (key) that triggered this call
	source_object_key = notification.object_key

	# extract out the file name
	source_file_name = pathlib.Path(source_object_key).name

	# if source_file_name is empty, ignore notification
	if not source_file_name:
		log(f'Ignoring notification without object key (file name)')
		return

	# if source_file_name is capture_state.zip, ignore it
	# Note: This keeps the latest capture_state.zip file in each capture folder for recovery purposes.
	if source_file_name == 'capture_state.zip':
		log(f'Ignoring capture_state.zip file notification')
		return

	# TODO: Add activity_log (vs job_log/stat_log) references.

	# make sure work folder exists
	work_folder = 'archive_work'
	if not os.path.exists(work_folder):
		os.mkdir(work_folder)

	# make sure work folder is empty
	for file_name in glob.glob(f'{work_folder}/*'):
		# print(f'Deleting: {file_name}')
		os.remove(file_name)

	# get file, copy to archive then delete from capture
	source_object_store_name = notification.object_store_name
	source_file_name = f'{work_folder}/' + pathlib.Path(source_object_key).name

	# _connect to source object_store
	# TODO: Cache these object_store connections vs reconnecting each time.
	source_object_store = cloud.ObjectStore(source_object_store_name, cloud_connection)

	# get the posted file
	log(f'Getting {source_file_name} from {source_object_store_name}::{source_object_key}')
	source_object_store.get(source_file_name, source_object_key)

	# move (copy) the posted file to the archive object_store
	log(f'Moving {source_file_name} to archive_object_store::{source_object_key}')
	archive_object_store.put(source_file_name, source_object_key)

	# extract job.log/last_job.log from capture zip and merge these into stat_log table
	archive = zipfile.ZipFile(source_file_name, 'r')
	if 'job.log' in archive.namelist():
		job_log_json = json.loads(archive.read('job.log'))
		for row in job_log_json:
			row['start_time'] = arrow.get(row['start_time']).datetime
			row['end_time'] = arrow.get(row['end_time']).datetime

			# skip capture stats which only have intermediate end_time and run_time values
			# next capture file will include an accurate version of this stat in last_job.job file
			if row['stat_name'] != 'capture':
				db_conn.insert_into_table('udp_catalog', 'stat_log', **row)

	if 'last_job.log' in archive.namelist():
		last_job_log_json = json.loads(archive.read('last_job.log'))
		for row in last_job_log_json:
			row['start_time'] = arrow.get(row['start_time']).datetime
			row['end_time'] = arrow.get(row['end_time']).datetime
			if row['stat_name'] in ('capture', 'compress', 'upload'):
				db_conn.insert_into_table('udp_catalog', 'stat_log', **row)

	# close archive when done
	archive.close()

	# then delete file from source object_store	and local work folder
	log(f'Deleting {source_object_key} from {source_object_store_name}')
	source_object_store.delete(source_object_key)
	pathlib.Path(source_file_name).unlink()

	# TODO: Move tested component code here
	# extract stat.log from capture*.zip
	# update nst_lookup, job_log, table_log

	# register new file in stage_arrival_queue table
	file_name = pathlib.Path(source_object_key).name
	job_id = int(file_name.split('.')[0].rsplit('#', 1)[-1])
	new_file = dict(archive_file_name=file_name, job_id=job_id)
	db_conn.insert_into_table('udp_catalog', 'stage_arrival_queue', **new_file)

	# FUTURE: Update schedule's poll message
	# last_job_info = f'last job {self.job_id} on {datetime.datetime.now():%Y-%m-%d %H:%M}'
	# schedule_info = f'schedule: {self.schedule}'
	# self.schedule.poll_message = f'{script_name()}({self.namespace}), {last_job_info}, {schedule_info}'


# main
def main():
	# TODO: Create a project file for all udp cloud based ETL scripts (archive, stage, udp, etc)

	# bootstrap configuration settings
	bootstrap = config.Bootstrap()
	bootstrap.debug_flag = False
	bootstrap.load('conf/init.ini')
	bootstrap.load('conf/bootstrap.ini')

	# project info
	project_name = 'udp_aws_archive_01_etl'
	project_config = config.Config(f'conf/{project_name}.project', config.ProjectSection, bootstrap)
	project_config.debug_flag = True
	project_config.dump()
	project_object = project_config.sections['archive_project']

	# make sure core database environment in place
	udp.setup()

	# get references to stage database and catalog schema
	# data_stage_database = udp.udp_stage_database
	# data_catalog_schema = udp.udp_catalog_schema

	database_connect_name = f'{project_object.database}'
	cloud_connect_name = f'{project_object.cloud}'
	# print(f'database_connect_name = {database_connect_name}')

	# SQL Server
	connect_config = config.Config('conf/_connect.ini', config.ConnectionSection, bootstrap)
	sql_server_connect = connect_config.sections[database_connect_name]

	db = database.SQLServerConnection(sql_server_connect)
	conn = db.conn
	# cursor = conn.cursor()

	db_conn = database.Database('sqlserver', conn)
	db_conn.debug_flag = False
	db_conn.use_database('udp_stage')

	# create udp_staging database if not present; then use
	# db_conn.create_database('udp_staging')
	# db_conn.use_database('udp_staging')
	# db_conn.create_schema('udp_admin')
	# db_conn.create_named_table('udp_admin', 'nst_lookup')

	# Todo: These names should come from project file
	# Todo: queue_name should be input_queue_name, output_queue_name
	# archive_objectstore_name = 'udp-s3-archive-sandbox'
	# queue_name = 'udp-sqs-capture-sandbox'
	archive_objectstore_name = f'{project_object.archive_objectstore}'
	capture_queue_name = f'{project_object.capture_queue}'

	# get connection info
	connect_config = config.Config('conf/_connect.ini', config.ConnectionSection, bootstrap)
	cloud_connection = connect_config.sections[cloud_connect_name]

	# main poll loop
	archive_object_store = cloud.ObjectStore(archive_objectstore_name, cloud_connection)
	queue = cloud.Queue(capture_queue_name, cloud_connection)
	while True:
		print(f'{datetime.datetime.today():%Y-%m-%d %H:%M:%S}: Polling for capture updates ...')
		response = queue.get()
		notification = cloud.ObjectStoreNotification(response)
		if notification.message_id:
			archive_capture_file(archive_object_store, cloud_connection, notification, db_conn)
			queue.delete(notification.message_id)

		# poll if we didn't find a captured file, otherwise keep processing
		if not notification.message_id:
			# poll
			time.sleep(int(project_object.poll_frequency))


# main code
if __name__ == '__main__':
	# test() - push files into bucket to trigger polling loop processing
	main()
