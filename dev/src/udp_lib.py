# udp_lib.py

'''
Library of common code.
'''


import os
import platform
import sys
import time


def version_info():
	'''
	Capture version information on tech stack, code, runtime environment.

	See my _python_diagnostics.py

	Diff across runs to discover what changed between runs.
	'''

	# tech stack:
	# Windows version, service pack
	# VMWware version, service pack
	# Docker version, service pack
	# Python version
	# pyODBC version
	# SQL Server version
	# psycopg2 version
	# PostgreSQL version
	# AWS Boto3 version

	# code:
	# udp_lib version
	# <script> version

	# runtime environment:
	# running as user
	# python virtual env
	# startup path
	# current path
	# environ variables
	# command line values
	# conf file version
	# conf file

	print(f'Version = {sys.version}')
	print(f'Platform = {platform.release()}')


def clean_text(text):
	'''Convert tabs to spaces and remove doubled spaces.'''
	text = text.replace('\t', ' ')
	return ' '.join(text.split())


def strip_left(text, *prefixes):
	'''Strip one or more prefixes from the start of text.'''

	# normalize doubled whitespace
	text = ' '.join(text.split())

	for prefix in prefixes:
		if text.startswith(prefix):
			text = text[len(prefix):]
			text = strip_left(text, *prefixes)
	return text


def strip_right(text, *suffixes):
	'''Strip one or more suffixes from the end of text.'''

	# normalize doubled whitespace
	text = ' '.join(text.split())

	for suffix in suffixes:
		if text.endswith(suffix):
			text = text[:-len(suffix)]
			text = strip_right(text, *suffixes)
	return text



class EventLoop:
	'''
	Base class for daemons/NT Service type application event loops.

	Use run() to start event loop.

	Monitors the udp.cmd file for the following single word commands:
	- stop - forces application exit
	- restart - "restarts" app to reload updated configs, reconnect to database
	- pause - pauses code until a stop, restart, or continue command detected
	- continue - clears a pause condition

	All code running as daemons or as NT Services should:
	- subclass this base class
	- override the methods below

	Override the following methods:
	- setup() - onetime setup code; not executed on a restart
	- start() - code called on initial start and following a restart
	- process() - core logic goes here
	- cleanup() - cleanup code goes here

	'''


	def __init__(self, cmd_file_name='', is_traced=False):
		''' Initialize module behavior.'''
		self.is_traced = is_traced

		self.is_stopped = False
		self.is_restarted = False
		self.is_paused = False

		if cmd_file_name:
			self.cmd_file_name = cmd_file_name
		else:
			self.cmd_file_name = 'udp.cmd'

	def stop(self):
		'''Process stop command.'''
		self.trace('stop')
		self.is_stopped = True

	def restart(self):
		'''Process restart command.'''
		self.trace('restart')
		self.is_restarted = True

	def pause(self):
		'''Process pause command; responds to stop, restart, and continue commands.'''

		# don't nest pause commands
		if self.is_paused:
			return

		self.trace('pause')
		self.is_paused = True
		while self.is_continue() and self.is_paused:
			time.sleep(1)
		self.is_paused = False

	def unpause(self):
		'''Process continue command.'''
		self.trace('continue')
		self.is_paused = False

	def run(self):
		'''Generic event loop.'''
		self.clear_cmd()
		self.setup()
		while not self.is_stopped:
			self.start()

			self.is_restarted = False
			while self.is_continue():
				self.process()
		self.cleanup()

	def trace(self, text=''):
		'''Generate trace output if is_traced=True.'''
		if self.is_traced:
			print(f'Event_Loop({text})')

	def clear_cmd(self):
		'''Clear last command.'''
		if os.path.exists(self.cmd_file_name):
			os.remove(self.cmd_file_name)

	def is_continue(self):
		'''Recognizes following commands: stop, restart, pause, continue.'''
		if os.path.exists(self.cmd_file_name):
			cmd = open(self.cmd_file_name).readline().strip().lower()
			self.clear_cmd()
			if cmd == 'stop':
				self.stop()
			elif cmd == 'restart':
				self.restart()
			elif cmd == 'pause':
				self.pause()
			elif cmd == 'continue':
				self.unpause()
			else:
				# unrecognized cmd
				pass

		return not self.is_stopped and not self.is_restarted

	def setup(self):
		'''Override: Setup code that's called once; Use start() for code that runs on a restart.'''
		self.trace('setup')

	def cleanup(self):
		'''Override: Cleanup code.'''
		self.trace('cleanup')

	def start(self):
		'''Override: Called at start/restart.'''
		self.trace('start')

	def process(self):
		'''Override: Main logic goes here.'''
		self.trace('process')
		time.sleep(2)

