#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
archive_2.py (next version)

Polls capture queue for objectstore notification messages indicating the presence of capture objectstore files.
Safely moves (copy-then-delete) capture*.zip files to archive objectstore.
Loads capture file job metrics.
Registers capture*.zip file in stage arrival queue.

Notes:
- reconnects to queue/objectstore resources each poll cycle to support brief session-time role based access
- processes all messages in queue before re-entering outer polling loop

Future:
- does it make sense to support capture filename filters by glob* pattern and/or prefix/suffix ???

###

TODO:
- place archive logic and common properties in class vs pass parms between functions
- read schedule for polling cycle and times not to run (maintenance windows?)
- look for maintenance window flag file which pauses operation (this should be universal feature) !!!!
- track session/instance info - we want to run multiple instances
- track archive job stats
- when retrieving a file, if there's a problem, add to log with an error flag set so stage doesn't process
- extract out capture.state file and push back to source capture bucket
- properly ignore S3/SQS test messages !!!!
- don't break on errors; log exceptions to log table and keep moving

###

FUTURE: Update schedule's poll message
last_job_info = f'last job {self.job_id} on {datetime.datetime.now():%Y-%m-%d %H:%M}'
schedule_info = f'schedule: {self.schedule}'
self.schedule.poll_message = f'{script_name()}({self.namespace}), {last_job_info}, {schedule_info}'

"""


# standard lib
import json
import logging


# common lib
from common import clear_folder
from common import delete_file
from common import iso_to_datetime
from common import just_file_name
from common import read_archived_file


# udp class
from cloud_aws import Objectstore
from cloud_aws import ObjectstoreNotification
from cloud_aws import Queue
from daemon import Daemon


# udp lib
import database
import udp


# module level logger
logger = logging.getLogger(__name__)


class ArchiveDaemon(Daemon):

	"""Daemon class integrates core config, option, and schedule functionality."""

	def start(self):
		super().start()

		# make sure core database environment in place
		udp.setup(self.config)

	# TODO: Handle as a plugin so we can handle heartbeats, logs, files that publish w/out stage, etc.
	# def archive_capture_file(archive_object_store, cloud_connection, notification, db_conn):
	def archive_capture_file(self, notification):

		# get name of file (key) that triggered this call
		source_object_key = notification.object_key

		# extract out the file name
		source_file_name = just_file_name(source_object_key)

		# if source_file_name is empty, ignore notification
		if not source_file_name:
			logger.debug(f'Ignoring notification without object key (file name): {notification}')
			return

		# if source_file_name is capture_state.zip, ignore it
		# Note: This keeps the latest capture_state.zip file in each capture folder for recovery purposes.
		if source_file_name == 'capture_state.zip':
			logger.debug(f'Ignoring capture_state.zip file notification')
			return

		# TODO: Add activity_log (vs job_log/stat_log) references.

		# make sure work folder exists and is empty
		work_folder = 'sessions/archive_work'
		clear_folder(work_folder)

		# get file, copy to archive then delete from capture
		source_objectstore_name = notification.objectstore_name
		source_file_name = f'{work_folder}/' + source_file_name

		# get the posted file
		logger.info(f'Getting {source_file_name} from {source_objectstore_name}::{source_object_key}')
		source_objectstore = Objectstore(source_objectstore_name, self.cloud)
		source_objectstore.get(source_file_name, source_object_key)

		# move (copy) the posted file to the archive object_store
		logger.info(f'Moving {source_file_name} to archive_object_store::{source_object_key}')
		archive_objectstore_name = self.cloud.archive_objectstore
		archive_objectstore = Objectstore(archive_objectstore_name, self.cloud)
		archive_objectstore.put(source_file_name, source_object_key)

		# then delete file from source object_store	and local work folder
		logger.info(f'Deleting {source_object_key} from {source_objectstore_name}')
		source_objectstore.delete(source_object_key)
		delete_file(source_file_name)

		# TODO: update stat_log
		# TODO: update stage queue

		return

	# main
	def main(self):
		# force unexpected exceptions to be exposed (at least during development)
		try:
			# reconnect to capture queue each poll cycle to support brief session-time role based access

			# get project's cloud connection structure
			cloud_id = self.config('project').cloud
			self.cloud = self.config(cloud_id)

			# connect to capture queue
			capture_queue = Queue(self.cloud.capture_queue, self.cloud)

			# process all messages in queue before returning to polling loop
			while True:
				# poll queue for a response
				response = capture_queue.get()

				# attempt to convert response to an objectstore notification message
				notification = ObjectstoreNotification(response)

				# if response lacks a message id then we've emptied the queue
				if not notification.message_id:
					break
				else:
					logger.info(f'SQS notification={notification}')
					self.archive_capture_file(notification)
					capture_queue.delete(notification.message_id)

		# force unhandled exceptions to be exposed
		except Exception:
			logger.exception('Unexpected exception')
			raise

	def update_stage_queue(self):
		# TODO: update project/datapond/datapool, session, job_log, table_log

		# register new file in stage_arrival_queue table
		"""
		file_name = pathlib.Path(source_object_key).name
		job_id = int(file_name.split('.')[0].rsplit('#', 1)[-1])
		new_file = dict(archive_file_name=file_name, job_id=job_id)
		db_conn.insert_into_table('udp_catalog', 'stage_arrival_queue', **new_file)
		"""
		pass

	def update_stat_log(self, source_file_name):

		# OLD: get references to stage database and catalog schema
		# data_stage_database = udp.udp_stage_database
		# data_catalog_schema = udp.udp_catalog_schema

		db = database.MSSQL(self.config(self.project.database))

		conn = db.conn
		# cursor = conn.cursor()

		db_conn = database.Database('mssql', conn)
		db_conn.use_database('udp_stage')

		# TODO: Will json_pickle restore datetime values without explict conversion ???
		# TODO: Wrapper for stat insert that does intersection of json and target record's schema
		#       and explicitly inserts specific column/value pairs they have in common; this will
		#       require that our json send includes column names, not just rows of column values !!!!

		# extract job.log/last_job.log from capture zip and merge these into stat_log table
		job_log_data = read_archived_file(source_file_name, 'job.log', default=None)
		if job_log_data:
			job_log_json = json.loads(job_log_data)
			for row in job_log_json:
				row['start_time'] = iso_to_datetime(row['start_time']).datetime
				row['end_time'] = iso_to_datetime(row['end_time']).datetime

				# skip capture stats which only have intermediate end_time and run_time values
				# next capture file will include an accurate version of this stat in last_job.job file
				if row['stat_name'] != 'capture':
					db_conn.insert_into_table('udp_catalog', 'stat_log', **row)

		# if 'last_job.log' in archive.namelist():
		job_log_data = read_archived_file(source_file_name, 'last_job.log', default=None)
		if job_log_data:
			last_job_log_json = json.loads(job_log_data)
			for row in last_job_log_json:
				row['start_time'] = iso_to_datetime(row['start_time']).datetime
				row['end_time'] = iso_to_datetime(row['end_time']).datetime
				if row['stat_name'] in ('capture', 'compress', 'upload'):
					db_conn.insert_into_table('udp_catalog', 'stat_log', **row)


# main code
if __name__ == '__main__':
	project_file = 'project_archive.ini'
	daemon = ArchiveDaemon(project_file)
	daemon.run()
