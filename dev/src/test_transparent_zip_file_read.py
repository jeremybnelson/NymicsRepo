#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
test_transparent_zip_file_read.py

Experiments to read files in zip files no different than files on local folders.

Future: Could also "read" remote objectstore "files" by reading the file and
creating a virtual file-like object (what are these called) than has a handle
and supports read/readlines, seek, close.

TODO: Also read shadow files that don't get picked up by git, eg _<file_name>.<ext>

TODO: How to have both local files and dev specific files, eg. a local-local?
IDEA: Do this via _<file_name>.<ext> that are developer specific and get loaded from
local after all other files get loaded. .gitignore should ignore _*.* files.

Remote file-like objects:
- AWS S3
- Azure Blobstore/GeneralPurposeFiles
- https://url
- sftp://
- api://restful-expression
- {%expression%} that looks up contents in database or resource file
- file from a database record (clob, blob, nvarchar(max), varbinary(max)

"""


# standard lib
import logging


# common lib
from common import is_file
from common import log_setup
from common import log_session_info
from common import strip_path_delimiter


# module level logger
logger = logging.getLogger(__name__)


class FindFiles:

	def __init__(self, *path_names, root_path=None):
		self.root_path = root_path

		# TODO: Support no *path_names and append a '' to path_names (use local folder)s

		# strip path delimiters from the end of all paths
		self.path_names = list()
		for path_name in path_names:
			self.path_names.append(strip_path_delimiter(path_name))

	# TODO: This should be a generator
	def find_file(self, file_name):
		# search for file across multiple paths/file types
		logger.info(f'Searching for {file_name} across: {self.path_names}')
		for path_name in self.path_names:
			load_file_name = f'{path_name}/{file_name}'
			if is_file(load_file_name):
				# return file handle
				pass

				# TODO: should yield file handle (stream handle)

# temp test harness ...


# test code
def main():
	pass


# test code
if __name__ == '__main__':
	log_setup()
	log_session_info()
	main()
