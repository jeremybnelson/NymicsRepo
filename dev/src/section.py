#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
section.py

Defines custom section blocks referenced by conf.py.

Section properties are validated unless <section>._is_validated = False.
"""


# standard lib
import logging


# module level logger
logger = logging.getLogger(__name__)


class Section:

	"""Subclass for each section type block of properties."""

	def __init__(self, section_key=''):
		self._is_validated = True
		self._section_key = section_key.strip().lower()
		self.setup()

	def setup(self):
		"""Subclass with section properties and default values."""
		pass

	def dump(self, dump_blank_values=True):
		# dump section attributes
		logger.info(f'[{self._section_key}]')
		for key, value in self.__dict__.items():
			# ignore hidden attributes
			if key.startswith('_'):
				pass
			elif dump_blank_values:
				logger.info(f'{key} = {value}')
			elif not dump_blank_values and value:
				logger.info(f'{key} = {value}')

		# REMOVE: to separate output while testing
		# print()


class SectionAccess(Section):

	# noinspection PyAttributeOutsideInit
	def setup(self):
		self.allow = list()
		self.block = list()


class SectionBootstrap(Section):

	# noinspection PyAttributeOutsideInit
	def setup(self):
		self._is_validated = False


class SectionCloud(Section):

	# noinspection PyAttributeOutsideInit
	def setup(self):
		self.platform = ''
		self.account_id = ''
		self.account_alias = ''
		self.region = ''

		# security
		self.role = ''
		self.username = ''
		self.public_key = ''
		self.private_key = ''
		# Jeremy added these specific properties 11/6/2018
		self.sas_token = ''
		self.storage_key = ''

		# resources
		self.admin_objectstore = ''
		self.archive_objectstore = ''
		self.capture_objectstore = ''
		self.system_objectstore = ''
		self.capture_queue = ''


class SectionDatabase(Section):

	# noinspection PyAttributeOutsideInit
	def setup(self):
		self.platform = ''
		self.driver = ''
		self.host = ''
		self.port = ''
		self.timezone = ''
		self.database = ''
		self.schema = ''

		# security
		self.username = ''
		self.password = ''


# TODO: 2018-09-15 - add extended datapool attributes.
# TODO: Consider renaming datapool to datapond.
# NOTE: Pools are clean; ponds (staging) are dirty. Pond > Pool
# Note: Formerly known as Namespace.
class SectionDatapool(Section):

	# noinspection PyAttributeOutsideInit
	def setup(self):
		self.datapool_id = ''
		self.entity = ''
		self.location = ''
		self.system = ''
		self.instance = ''
		self.subject = ''
		self.sdlc = ''


# TODO: See note above. Rename datapool to datapond. A
class SectionDatapools(Section):

	"""Collections of datapools."""

	# noinspection PyAttributeOutsideInit
	def setup(self):
		# will split key|id = value and add to key as a dict
		# setting a value to a list will just append key| = value
		# how to clear a key acting as a dict/list? key = @clear ???
		self.datapool = dict()


class SectionEnvironment(Section):

	# noinspection PyAttributeOutsideInit
	def setup(self):
		self.sdlc_type = ''
		self.sdlc_name = ''
		self.build_date = ''


class SectionFile(Section):

	# noinspection PyAttributeOutsideInit
	def setup(self):
		self.copy_file = ''
		self.move_file = ''
		self.delete_file = ''
		self.ignore_file = ''
		self.column = dict()


class SectionProject(Section):
	# noinspection PyAttributeOutsideInit
	def setup(self):
		self.script = ''

		# runtime option
		self.options = ''
		self.batch_size = ''

		# resources
		self.cloud = ''
		self.database = ''


class SectionSchedule(Section):

	# noinspection PyAttributeOutsideInit
	def setup(self):
		# default
		self.poll_frequency = '5'
		self.heartbeat_frequency = ''

		# schedule rules
		self.daily_at = ''
		self.hourly_at = ''
		self.hours_of_day = ''
		self.days_of_week = ''
		self.days_of_month = ''
		self.days_of_year = ''

		# schedule exceptions (skip rules)
		self.skip_hours_of_day = ''
		self.skip_days_of_week = ''
		self.skip_days_of_month = ''
		self.skip_days_of_year = ''


class SectionTable(Section):

	# noinspection PyAttributeOutsideInit
	def setup(self):
		self.schema_name = ''

		# optional catalog attributes
		self.table_comment = ''

		# tags are searchable tags to identify tables via an agile classification scheme
		# TODO: Tags could be a single space/comma delimited string, a list, or a dict
		# Tag examples: customer, product, sale, usage-lift, usage-lodging, finance, reference, ...
		# Dict syntax would be tag|<tag-name>=1 or tag|<tag-name>= (to clear specific tag)
		# Dict syntax would allow inheriting/clone multi-value tags; adding/deleting individual tags with precision
		self.table_tags = ''

		# table_type: <blank> | standard, columnar, memory, or columnar-memory; stage uses when creating table
		self.table_type = ''
		self.table_name = ''
		self.table_prefix = ''
		self.table_suffix = ''
		self.drop_table = ''
		self.ignore_table = ''

		# TODO: convert these to singular (vs plural) dict() for easier to read configurations
		self.ignore_columns = ''
		self.sensitive_columns = ''

		# override auto column conversion; specify a specific target column type
		# Use case: PostgreSQL char columns specified without size constraint.
		self.column = dict()

		# TODO: was natural_key - rename back to natural key (dp_nk)
		self.primary_key = ''
		self.cdc = ''
		self.timestamp = ''
		self.first_timestamp = ''
		self.rowversion = ''
		self.first_rowversion = ''
		self.join = ''
		self.where = ''
		self.order = ''
		self.delete_when = ''
