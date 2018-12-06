#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
diagnostics.py

Code to capture diagnostic information during development, testing, and at production runtime.
"""


# standard lib
import datetime
import inspect
import logging
import time


# common lib
from common import log_setup
from common import log_session_info


# module level logger
logger = logging.getLogger(__name__)


def caller_function_name():
	"""
	Return caller function's name.
	Ref: https://stackoverflow.com/questions/5067604/determine-function-name
	"""
	return inspect.stack()[2][3]


def current_function_name():
	"""
	Return current function's name.
	Ref: https://stackoverflow.com/questions/5067604/determine-function-name
	"""
	return inspect.stack()[1][3]


class CodeTimer:

	def __init__(self, message=None):
		self.start_time = None
		self.finish_time = None
		self.elapsed_time = 0

		self.message = message
		if not self.message:
			self.message = caller_function_name()

	def __enter__(self):
		self.start_time = datetime.datetime.now()
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.finish_time = datetime.datetime.now()
		self.elapsed_time = (self.finish_time - self.start_time).seconds
		logger.info(f'{self.message}: {self.elapsed_time} secs')


# temp test harness ...


# test code
def test_diagnostics():
	# test code timer
	logger.info(f'This message from: {current_function_name()}')
	logger.info(f'Called by: {caller_function_name()}')
	with CodeTimer():
		time.sleep(3)


# main
def main():
	test_diagnostics()


# test code
if __name__ == '__main__':
	log_setup()
	log_session_info()
	main()
