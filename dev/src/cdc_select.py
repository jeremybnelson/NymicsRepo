#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
# cdc_select.py

Generate CDC select statement.

table_object.schema_name = schema_name
table_object.table_name = table_name
table_object.column_names = '*'
select_cdc = SelectCDC(table_object)
sql = select_cdc.select(job_id, current_timestamp, last_timestamp)

TODO: Add validation to insure we have minimum required components, eg. pk's.

"""


# standard lib
import datetime
import logging
import pathlib


# common lib
from common import delete_blank_lines
from common import expand
from common import log_setup
from common import log_session_info
from common import split
from common import spaces


# udp lib
import config


# module level logger
logger = logging.getLogger(__name__)


def q(items):
	"""Decorates item/items with double-quotes to protect table/column names that may be reserved words."""
	if isinstance(items, (list, tuple, set)):
		# don't double double-quote items that are already double-quoted
		return [item if item.startswith('"') else f'"{item}"' for item in items]
	elif not items.startswith('"'):
		# don't double double-quote items that are already double-quoted
		return items
	else:
		return f'"{items}"'


def add_alias(column_name, table_alias):
	"""Adds table_alias (if missing) and double-quotes table alias and column name."""
	column_name = column_name.replace('"', '')
	if '.' in column_name:
		table_alias, separator, column_name = column_name.partition('.')
	column_name = f'{q(table_alias)}.{q(column_name)}'
	return column_name


def add_aliases(column_names, table_alias='s'):
	"""Performs add_alias() on a list of column names."""
	return [add_alias(column_name, table_alias) for column_name in column_names]

###


"""Test code"""


# test - part of TableSchema
class Column:

	def __init__(self, column):
		self.column_name = column.column_name
		self.data_type = column.data_type
		self.is_nullable = column.is_nullable
		self.character_maximum_length = column.character_maximum_length
		self.numeric_precision = column.numeric_precision
		self.numeric_scale = column.numeric_scale
		self.datetime_precision = column.datetime_precision
		self.character_set_name = column.character_set_name
		self.collation_name = column.collation_name


# test
class Table:

	def __init__(self, schema_name, table_name, column_names):
		self.schema_name = schema_name
		self.table_name = table_name
		self.column_names = split(column_names)

		self.table_prefix = ''
		self.table_suffix = ''
		self.natural_key = ''
		self.cdc = ''
		self.timestamp = ''
		self.first_timestamp = ''
		self.rowversion = ''
		self.first_rowversion = ''
		self.select = ''
		self.where = ''
		self.ignore = ''
		self.order = ''

###


def indent(text):
	"""Protect logical indentation of indented multi-line text values."""
	output = []
	for line in text.strip().splitlines():
		line = line.strip()
		if line.startswith('_ '):
			line = line[1:]
		elif line.startswith('__'):
			line = line[2:].strip()
		output.append(line)
	return '\n'.join(output)


def clean_sql(text):
	# convert text to lowercase so we can match keywords consistently
	text = text.lower()

	# strange SQL formatting corrections go here ...

	# correct missing spaces before square-brackets
	text = text.replace('join[', 'join [')

	# remove square-bracket quoting; we'll re-quote later with ANSI double-quotes
	text = text.replace('[', '').replace(']', '')

	# make sure '=, (, )' are space delimited so they don't "stick" to adjacent tokens
	text = text.replace('=', ' = ')
	text = text.replace('(', ' ( ')
	text = text.replace(')', ' ) ')

	# remove -- comments from each line
	output = []
	for line in text.strip().splitlines():
		line = line.partition('--')[0]
		output.append(line)
	text = '\n'.join(output)

	# normalize text to single-space delimited string
	text = ' '.join(text.split())

	# after special chars are space delimited and whitespace normalized ...

	# remove WITH (NOLOCK) clauses
	text = text.replace('with ( nolock )', '')

	return text


"""

quote join/on table items ???
join processing: select > join

before \n and \t to space conversion run clean_sql()
clean_sql(text) - remove -- comments, replace [/] square-braces with ANSI double-quotes

join on clauses can have parentheses join table t on (t.col=s.col) 
we need to split out the parentheses, =, <, >, !, quote the elements, and recombine - or just use as-is ???
after converting \n and \t to spaces, remove extra spaces 
full|left|right [outer] join, inner join, cross join, join ... vs just "join " support
remove "WITH (NOLOCK)" clauses
replace "RTPIkon..TransactionLine" (database..table) with schema.table
quote schema.table and alias.column in join/on clauses; do after fixing '..' expressions

no support for embedded selects: join (select distinct rtp1_source_code, lobcode from ##ProductCode_Ascent) t
embedded selects (and unions/intersections) must be implemented as views or temp tables
SQL Server does not support: join ... using(...), natural join 
"""

join_keyword_phrases = '''
full inner join
full outer join
left inner join
left outer join
right inner join
right outer join
cross join
full join
left join
right join
inner join
outer join
join
'''.splitlines()


# [FULL|LEFT|RIGHT] [INNER|OUTER] ]CROSS] <JOIN> [<database>>..]<table> <alias> [with (NOLOCK)] ON <condition> [-- *]
def format_join(text, schema_name):
	text = clean_sql(text)

	join_keywords = split('full, left, right, inner, outer, cross, join, on, and, or, not')
	output = []
	last_token = ''
	for token in text.split():
		if token in join_keywords or not token[0].isalpha():
			output.append(token)
		else:
			if '..' in token:
				token = q(token.partition('..')[2])
			elif token.startswith('dbo.'):
				token = q(token[4:])
			elif '.' in token:
				alias_name, separator, table_name = token.partition('.')
				token = f'{q(alias_name)}.{q(table_name)}'
			else:
				token = q(token)

			# add schema name if last token ends with 'join' and token missing schema name
			if last_token.endswith('join') and '.' not in token:
				token = f'{q(schema_name)}.{token}'

			output.append(token)

		last_token = token

	text = ' '.join(output)

	# convert join keyword phrases to tokens
	for join_keyword_phrase in join_keyword_phrases:
		join_keyword_token = join_keyword_phrase.replace(' ', '::')
		text = text.replace(join_keyword_phrase, join_keyword_token)

	# format joins into 2-line clauses
	output = []
	for token in text.split():
		if token.endswith('join'):
			token = f'\n{spaces(2)}{token}'
		elif token == 'on':
			token = f'\n{spaces(4)}{token}'
		output.append(token + ' ')

	# expand join keyword tokens back to join keyword phrases
	text = ''.join(output)
	text = text.replace('::', ' ')

	return text


class SelectCDC:

	select_template = '''
	__select
	_  {column_names},
	_  {job_id} as "udp_job",
	_  {timestamp_value} as "udp_timestamp"
	_  from "{schema_name}"."{table_name}" as "s"
	_  {join_clause}
	_  {where_clause}
	_  {order_clause}
	'''

	timestamp_where_template = '''
	__   (
	_      {timestamp_value} >= '{last_timestamp}' and
	_      {timestamp_value} < '{current_timestamp}'
	_    )
	'''

	def __init__(self, table):
		# indent template text
		self.select_template = indent(self.select_template)
		self.timestamp_where_template = indent(self.timestamp_where_template)

		# object scope properties
		self.table = table
		self.timestamp_value = ''
		self.timestamp_where_condition = ''

	def column_names(self):
		if self.table.column_names == '*':
			return '*'
		else:
			column_names = add_aliases(self.table.column_names, 's')
			return ', '.join(column_names)

	# noinspection PyUnusedLocal
	# Note: last_timestamp referenced in expanded f-string.
	def timestamp_logic(self, current_timestamp, last_timestamp=None):
		timestamp_columns = add_aliases(split(self.table.timestamp))
		if not timestamp_columns:
			self.timestamp_value = f"'{current_timestamp:%Y-%m-%d %H:%M:%S}'"
			self.timestamp_where_condition = ''
		else:
			if len(timestamp_columns) == 1:
				timestamp_value = q(timestamp_columns[0])
			else:
				# build timestamp column values as ("created_at"), ("updated_at"), ("other_timestamp")
				timestamp_values = ', '.join([f'({q(column_name)})' for column_name in timestamp_columns])
				timestamp_value = f'(select max("v") from (values {timestamp_values}) as value("v"))'

			self.timestamp_value = timestamp_value
			self.timestamp_where_condition = expand(self.timestamp_where_template)

	def join_clause(self):
		schema_name = self.table.schema_name
		join_clause = self.table.join.strip('\\')
		if join_clause:
			join_clause = '\n' + format_join(join_clause, schema_name)
		return join_clause

	def where_clause(self):
		if not self.table.where and not self.timestamp_where_condition:
			where_clause = ''
		elif self.table.where and not self.timestamp_where_condition:
			where_clause = f'where\n{spaces(4)}({self.table.where})'
		elif not self.table.where and self.timestamp_where_condition:
			where_clause = f'where\n{spaces(4)}{self.timestamp_where_condition}'
		else:
			where_clause = f'where\n{spaces(4)}({self.table.where}) and\n{spaces(4)}{self.timestamp_where_condition}'
		return where_clause

	def order_clause(self):
		# order by option
		order_clause = ''
		if self.table.order:
			order_columns = add_aliases(split(self.table.order))
			order_clause = f'order by {", ".join(order_columns)}'
		return order_clause

	# noinspection PyUnusedLocal
	def select(self, job_id, current_timestamp, last_timestamp):
		self.timestamp_logic(current_timestamp, last_timestamp)

		schema_name = self.table.schema_name
		table_name = self.table.table_name
		column_names = self.column_names()
		timestamp_value = self.timestamp_value
		join_clause = self.join_clause()
		where_clause = self.where_clause()
		order_clause = self.order_clause()
		sql = expand(self.select_template)
		return delete_blank_lines(sql.strip() + ';')


test_join_1 = '''
-- -- comment with join, left join, outer join
_ select * 
_  from database..table s
_  left \t join  CloseHeader   t1 -- comment
_    on s.CloseID = t1.CloseID -- comment
_  right  join CloseHeader t2
_    on s.CloseID = t2.CloseID
_  left  outer  join CloseHeader t3 with   (NOLOCK)
_    on (s.CloseID = t3.CloseID)
_  right outer join CloseHeader t4
_    on s.CloseID = t4.CloseID
_  join CloseHeader t5
_    on s.CloseID = t5.CloseID and s.CloseID= t5.CloseID and s.CloseID=t5.CloseID  
_  join [CloseHeader] t6
_    on s.Database..CloseID = t6.CloseID
_  join CloseHeader t7
_    on s.CloseID = t7.CloseID
'''

# noinspection PyPep8
test_join_2 = '''
-- Ascent360
Inner join [dbo].[rtp1_TransactionProduct] t with (nolock) On h.[rtp1_source_code] = t.[rtp1_source_code] and h.TransactionID = t.TransactionID
Inner join ##ProductCode_Ascent pc On pc.[rtp1_source_code] = t.[rtp1_source_code] and pc.productcode = t.productcode
inner join [dbo].[rtp1_ProductCategory] pc with (nolock) on pg.ProductGroupCode = pc.ProductGroupCode and pg.[rtp1_source_code] = pc.[rtp1_source_code]
inner join [dbo].[rtp1_Product] p with (nolock) on pc.ProductCategoryCode = p.ProductCategoryCode and pc.[rtp1_source_code] = p.[rtp1_source_code]
inner join [dbo].[rtp1_LOB] l with (nolock) on l.lobcode = pg.lobcode and l.[rtp1_source_code] = pg.[rtp1_source_code]
inner join [dbo].[rtp1_LOBSummary] ls with (nolock) on l.lobsummarycode = ls.lobsummarycode and l.[rtp1_source_code] = ls.[rtp1_source_code]
inner join [dbo].[rtp1_LOBSuperSummary] lss with (nolock) on lss.lobSuperSummaryCode = ls.lobSuperSummaryCode and lss.[rtp1_source_code] = lss.[rtp1_source_code]
Inner join[dbo].[rtp1_TransactionProduct] t with (nolock) On h.[rtp1_source_code] = t.[rtp1_source_code] and h.TransactionID = t.TransactionID
Inner join ##ProductCode_Ascent pc On pc.[rtp1_source_code] = t.[rtp1_source_code] and pc.productcode = t.productcode
join ##TransactionIDsToExtract_Ascent t on p.rtp1_source_code = t.rtp1_source_code and p.TransactionID = t.TransactionID
join ##IPCodes_Ascent t on p.rtp1_source_code = t.rtp1_source_code and p.IPCode = t.IPCode
inner join [dbo].[rtp1_ProductCategory] pc with (nolock) on pg.ProductGroupCode = pc.ProductGroupCode and pg.[rtp1_source_code] = pc.[rtp1_source_code]
inner join [dbo].[rtp1_Product] p with (nolock) on pc.ProductCategoryCode = p.ProductCategoryCode and pc.[rtp1_source_code] = p.[rtp1_source_code]
inner join [dbo].[rtp1_LOB] l with (nolock) on l.lobcode = pg.lobcode and l.[rtp1_source_code] = pg.[rtp1_source_code]
inner join [dbo].[rtp1_LOBSummary] ls with (nolock) on l.lobsummarycode = ls.lobsummarycode and l.[rtp1_source_code] = ls.[rtp1_source_code]
inner join [dbo].[rtp1_LOBSuperSummary] lss with (nolock) on lss.lobSuperSummaryCode = ls.lobSuperSummaryCode and lss.[rtp1_source_code] = lss.[rtp1_source_code]

-- Scott
INNER JOIN RTPOne..ProductProfileType ppt WITH (NOLOCK) ON p.ProductProfileTypeCode = ppt.ProductProfileTypeCode
INNER JOIN RTPOne..ProductHeaderLocation phl WITH (NOLOCK) ON p.ProductCode = phl.ProductCode
INNER JOIN RTPOne..ProductHeader ph WITH (NOLOCK) ON phl.ProductHeaderCode = ph.ProductHeaderCode
INNER JOIN RTPOne..ProductCategory pc WITH (NOLOCK) ON p.ProductCategoryCode = pc.ProductCategoryCode
INNER JOIN RTPOne..ProductGroup pg WITH (NOLOCK) ON pc.ProductGroupCode = pg.ProductGroupCode
INNER JOIN RTPOne..LOB lob WITH (NOLOCK) ON pg.LOBCode = lob.LOBCode
INNER JOIN RTPOne..LOBSummary lobs WITH (NOLOCK) ON lob.LOBSummaryCode = lobs.LOBSummaryCode
INNER JOIN RTPOne..LOBSuperSummary lobss WITH (NOLOCK) ON lobs.LOBSuperSummaryCode = lobss.LOBSuperSummaryCode
INNER JOIN RTPOne..SaleLocation sl WITH (NOLOCK) ON phl.SaleLocationCode = sl.SaleLocationCode
LEFT OUTER JOIN RTPOne..TransactionLine tl WITH (NOLOCK) ON th.TransactionID = tl.TransactionID
LEFT OUTER JOIN RTPOne..TransactionProduct tp WITH (NOLOCK) ON tl.TransactionId = tp.TransactionId AND tl.TransactionLine = tp.TransactionLine
LEFT OUTER JOIN RTPOne..Product p WITH (NOLOCK) ON tp.ProductCode = p.ProductCode
LEFT OUTER JOIN RTPOne..ProductHeader ph WITH (NOLOCK) ON tl.ProductHeaderCode = ph.ProductHeaderCode
LEFT OUTER JOIN RTPOne..OrderMaster om WITH (NOLOCK) ON th.OrderID = om.OrderID
LEFT OUTER JOIN RTPOne..LineType lt WITH (NOLOCK) ON tl.LineTypeCode = lt.LineTypeCode
LEFT OUTER JOIN RTPOne..TransactionType tt WITH (NOLOCK) ON th.TransactionTypeCode = tt.TransactionTypeCode
LEFT OUTER JOIN RTPOne..SaleLocation sl WITH (NOLOCK) ON th.SaleLocationCode = sl.SaleLocationCode
LEFT OUTER JOIN RTPOne..Location l WITH (NOLOCK) ON sl.SaleLocationCode = l.LocationCode
LEFT OUTER JOIN RTPOne..ProductProfileType ppt WITH (NOLOCK) ON tp.ProductProfileTypeCode = ppt.ProductProfileTypeCode
LEFT OUTER JOIN RTPOne..SalesChannel sc WITH (NOLOCK) ON tl.SalesChannelCode = sc.SalesChannelCode
LEFT OUTER JOIN RTPOne..Currency c WITH (NOLOCK) ON tl.HomeCurrencyCode = c.CurrencyCode -- deliberately trying to make CAD or other currency visible
LEFT OUTER JOIN RTPOne..TransactionLineIP tli WITH (NOLOCK) ON tl.TransactionID = tli.TransactionID and tl.TransactionLIne = tli.TransactionLine
LEFT OUTER JOIN RTPOne..TransactionLineOriginalTransactionLine tlotl WITH (NOLOCK) ON tl.TransactionId = tlotl.TransactionID AND tl.TransactionLine  = tlotl.TransactionLine
LEFT OUTER JOIN RTPOne..TransactionLine tl2 WITH (NOLOCK) ON tlotl.TransactionID = tl2.TransactionID and tlotl.TransactionLine = tl2.TransactionLine
LEFT OUTER JOIN RTPOne..TransactionHeader th2 WITH (NOLOCK) ON tl2.TransactionID = th2.TransactionID
LEFT OUTER JOIN #DimProductUDP p ON t.PHC_PC = p.PHC_PC
INNER JOIN RTPOne..ProductProfileType ppt WITH (NOLOCK) ON p.ProductProfileTypeCode = ppt.ProductProfileTypeCode
INNER JOIN RTPOne..ProductHeaderLocation phl WITH (NOLOCK) ON p.ProductCode = phl.ProductCode
INNER JOIN RTPOne..ProductHeader ph WITH (NOLOCK) ON phl.ProductHeaderCode = ph.ProductHeaderCode
INNER JOIN RTPOne..ProductCategory pc WITH (NOLOCK) ON p.ProductCategoryCode = pc.ProductCategoryCode
INNER JOIN RTPOne..ProductGroup pg WITH (NOLOCK) ON pc.ProductGroupCode = pg.ProductGroupCode
INNER JOIN RTPOne..LOB lob WITH (NOLOCK) ON pg.LOBCode = lob.LOBCode
INNER JOIN RTPOne..LOBSummary lobs WITH (NOLOCK) ON lob.LOBSummaryCode = lobs.LOBSummaryCode
INNER JOIN RTPOne..LOBSuperSummary lobss WITH (NOLOCK) ON lobs.LOBSuperSummaryCode = lobss.LOBSuperSummaryCode
INNER JOIN RTPOne..SaleLocation sl WITH (NOLOCK) ON phl.SaleLocationCode = sl.SaleLocationCode
LEFT OUTER JOIN RTPOne..TransactionLine tl WITH (NOLOCK) ON th.TransactionID = tl.TransactionID
LEFT OUTER JOIN RTPOne..TransactionProduct tp WITH (NOLOCK) ON tl.TransactionId = tp.TransactionId AND tl.TransactionLine = tp.TransactionLine
LEFT OUTER JOIN RTPOne..Product p WITH (NOLOCK) ON tp.ProductCode = p.ProductCode
LEFT OUTER JOIN RTPOne..ProductHeader ph WITH (NOLOCK) ON tl.ProductHeaderCode = ph.ProductHeaderCode
LEFT OUTER JOIN RTPOne..OrderMaster om WITH (NOLOCK) ON th.OrderID = om.OrderID
LEFT OUTER JOIN RTPOne..LineType lt WITH (NOLOCK) ON tl.LineTypeCode = lt.LineTypeCode
LEFT OUTER JOIN RTPOne..TransactionType tt WITH (NOLOCK) ON th.TransactionTypeCode = tt.TransactionTypeCode
LEFT OUTER JOIN RTPOne..SaleLocation sl WITH (NOLOCK) ON th.SaleLocationCode = sl.SaleLocationCode
LEFT OUTER JOIN RTPOne..Location l WITH (NOLOCK) ON sl.SaleLocationCode = l.LocationCode
LEFT OUTER JOIN RTPOne..ProductProfileType ppt WITH (NOLOCK) ON tp.ProductProfileTypeCode = ppt.ProductProfileTypeCode
LEFT OUTER JOIN RTPOne..SalesChannel sc WITH (NOLOCK) ON tl.SalesChannelCode = sc.SalesChannelCode
LEFT OUTER JOIN RTPOne..Currency c WITH (NOLOCK) ON tl.HomeCurrencyCode = c.CurrencyCode -- deliberately trying to make CAD or other currency visible
LEFT OUTER JOIN RTPOne..TransactionLineIP tli WITH (NOLOCK) ON tl.TransactionID = tli.TransactionID and tl.TransactionLIne = tli.TransactionLine
LEFT OUTER JOIN RTPOne..TransactionLineOriginalTransactionLine tlotl WITH (NOLOCK) ON tl.TransactionId = tlotl.TransactionID AND tl.TransactionLine  = tlotl.TransactionLine
LEFT OUTER JOIN RTPOne..TransactionLine tl2 WITH (NOLOCK) ON tlotl.TransactionID = tl2.TransactionID and tlotl.TransactionLine = tl2.TransactionLine
LEFT OUTER JOIN RTPOne..TransactionHeader th2 WITH (NOLOCK) ON tl2.TransactionID = th2.TransactionID

'''

"""
TODO: 2018-06-05 Tue

TODAY:
format_join()
table_prefix, table_suffix added to from AND join tables !!!
current_timestamp comes from database (w/optional TZ conversion) vs app servers
history files with last_timestamp (or first_timestamp) and checksum if cdc = None/empty

STATE:
<namespace>.job - job_id, last_timestamp, last_status (if not empty, stop until cleared)
<table>.history - last_timestamp, last_filehash
<table>.definition vs *.schema

EARLIER DESIGN: IGNORE AND REMOVE THESE VESTIGES FROM CODE:
select should have a place holder for columns (*)
from gets removed and should always have an alias of s for source
joins and where get parsed out
joins and where can not include nested queries - use views/temp tables for this
we parse out the join rules and the where clause

"""


# temporary test harness ...


# test code
def main():
	job_id = 100
	current_timestamp = datetime.datetime.now()
	current_timestamp = current_timestamp.replace(second=0, microsecond=0)
	# last_timestamp = current_timestamp - datetime.timedelta(hours=1)
	last_timestamp = current_timestamp - datetime.timedelta(days=30)

	# test_table = Table('dbo', 'closeheader', 'a, "b", c, d')
	# test_table.timestamp = 'a'
	# test_table.select = indent(test_join)
	# test_table.where = '"a" > 2 or c = 129'
	# test_table.order = '"d", c, "a"'
	# select_cdc = SelectCDC(test_table)
	# sql = select_cdc.select(job_id, current_timestamp, last_timestamp)
	# print(sql)

	# schema_name = 'public'
	# print(format_join(indent(test_join_1), schema_name))
	# print()
	# print(format_join(indent(test_join_2), schema_name))
	# sys.exit()

	for tables_file in sorted(pathlib.Path('conf/').glob('*.tables')):
		if 'bad' in str(tables_file):
			continue
		print(f'-- {tables_file}')
		schema_name = pathlib.Path(tables_file).stem
		schema_name = schema_name.rsplit('_', 1)[0]
		schema_name = schema_name.replace('svr_01', 'svr_02')

		table_config = config.Config(str(tables_file), config.TableSection)
		for table_name, table_object in table_config.sections.items():
			if table_name == 'default' or table_object.ignore_table:
				continue

			if not table_object.join:
				continue

			table_object.schema_name = schema_name
			table_object.table_name = table_name
			table_object.column_names = '*'
			select_cdc = SelectCDC(table_object)
			sql = select_cdc.select(job_id, current_timestamp, last_timestamp)
			print(f'{sql}\n')


# test code
if __name__ == '__main__':
	log_setup()
	log_session_info()
	main()
