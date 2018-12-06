#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
daemon.py

Light-weight daemon framework for all long running processes.
"""


# standard lib
import logging


# common lib
from common import just_file_stem
from common import log_setup
from common import log_session_info
from common import create_folder
from common import script_name


# udp class
from config import ConfigSectionKey
from option import Option
from schedule import Schedule


# module level logger
logger = logging.getLogger(__name__)


class Daemon:

	def __init__(self, project_file):
		self.config = None
		self.option = None
		self.project = None

		# resource attributes
		self.cloud = None
		self.database = None
		self.schedule = None

		# project file controls configuration
		self.project_file = project_file

	@staticmethod
	def cleanup():
		"""Override: Optional cleanup code."""
		logger.info('Application exit')

	def main(self):
		"""Override: Main code goes here."""
		pass

	def run(self, *args, **kwargs):
		"""
		Options
		--onetime[=1] run once, then exit; use if called by an external scheduler.
		--nonwait[=1] run immediately without waiting for scheduler to determine execution.
		"""

		# make sure root sessions folder exists
		create_folder('../sessions')

		# TODO: We start logging before we read config and options so we don't know datapool or anything else.
		# TODO: We should log to a default app log and then SWITCH LOG file over after we process options and
		# TODO: and config files ??? (2018-09-25)
		log_setup(log_file_name=f'../sessions/{script_name()}.log')
		log_session_info()

		self.setup(*args, **kwargs)
		self.start()

		# scheduling behavior based on --onetime, --nowait option
		if self.option('onetime') == '1':
			# one-time run; use when this script is being called by an external scheduler
			logger.info('Option(onetime=1): executing once')
			self.main()
		else:
			if self.option('nowait') == '1':
				# no-wait option; execute immediately without waiting for scheduler to initiate
				logger.info('Option(nowait=1): executing immediately, then following regular schedule')
				self.main()

			# standard wait for scheduled time slot and run logic
			while True:
				if self.schedule.wait():
					self.main()
				else:
					break

		self.cleanup()

	def setup(self, *args, **kwargs):
		"""Override: Optional setup code (called once)."""
		pass

	def start(self):
		"""
		Override: Code called on initial start and subsequent restarts.

		Must set:
		- self.option = Option()
		- self.schedule = Schedule()

		Note: We don't load resources here; resources are loaded on demand.
		"""

		# load standard config
		config = ConfigSectionKey('../conf', '../local')
		self.config = config
		config.load('bootstrap.ini', 'bootstrap')
		config.load('init.ini')
		config.load('connect.ini')

		# load project specific config
		self.config.load(self.project_file)

		# load project specific options from optional project specific environ var
		environ_var = just_file_stem(self.project_file).lower()
		self.option = Option(environ_var, options=config('project').options)

		# load project specific schedule
		self.schedule = Schedule(config('schedule'))

		# diagnostics
		self.option.dump()
		self.config('project').dump(False)
		self.config('schedule').dump(False)


# temp test scaffolding ...


# test code
def main():
	pass


# test code
if __name__ == '__main__':
	main()
