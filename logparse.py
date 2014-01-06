#!/usr/bin/python

# Log parsing proof of concept
#
#
#


import sys
import fileinput
import re
import pprint
from datetime import datetime

def new_session(log, line, fields):
	if log.sessions == [{}]:
		log.sessions == []
	log.sessions.append({
		'start_date': line['date'],
	})

def save_flush(log, line, fields):
	if 'flushes' not in log.sessions[-1]:
		log.sessions[-1]['flushes'] = []
	if save_flush.current_flush is None:
		save_flush.current_flush = {}
		save_flush.current_flush.update(fields)
	else:
		save_flush.current_flush.update(fields)
		log.sessions[-1]['flushes'].append(save_flush.current_flush)
		save_flush.current_flush = None
save_flush.current_flush = None

line_pattern = r'(.{5}) \[([A-Za-z]*):?([0-9]*)\] (.{10} .{12}) ([^ ]*) \(line ([0-9]*)\) (.*)'
line_fields = ['level', 'thread_name', 'thread_id', 'date', 'source_file', 'source_line', 'message']
date_format = '%Y-%m-%d %H:%M:%S,%f'

message_rules = {
	'CassandraDaemon.java': [
		{
			'regex': r'Logging initialized',
			'action': new_session
		},
		{
			'regex': r'JVM vendor/version: (.*)',
			'fields': ['jvm'],
			'action': 'update'
		},
		{
			'wrapper': 'heap_size',
			'regex': r'Heap size: ([0-9]*)/([0-9]*)',
			'fields': ['used', 'total'],
			'action': 'update'
		},
		{
			'regex': r'Classpath: (.*)',
			'fields': ['classpath'],
			'field_parsers': { 'classpath': lambda cp: cp.split(':') },
			'action': 'update'
		}
	],
	'DseDaemon.java': [
		{
			'regex': r'([A-Za-z]*) version: (.*)',
			'fields': ['component', 'version'],
			'index_name': 'versions'
		}
	],
	'StorageService.java': [
		{
			'regex': r'([A-Za-z ]*) versions?: (.*)',
			'fields': ['component', 'version'],
			'index_name': 'versions'
		}
	],
	'GCInspector.java': [
		{
			# Heap is 0.7648984666406755 full...
			'regex': r'Heap is ([0-9.]*) full.*',
			'fields': ['percent_full'],
			'index_name': 'heap_warnings'
		},
		{
			# GC for ParNew: 1020 ms for 2 collections, 172365056 used; max is 8506048512
			'regex': r'GC for ([A-Za-z]*): ([0-9]*) ms for ([0-9]*) collections, ([0-9]*) used; max is ([0-9]*)',
			'fields': ['type', 'duration', 'collections', 'used', 'max'],
			'index_name': 'garbage_collections'
		}
	],
	'Memtable.java': [
		{ 
			# Writing Memtable-DeviceUDID@810851995(1151990/2097152 serialized/live bytes, 38951 ops)
			'regex': r'Writing Memtable-([^@]*)@([0-9]*)\(([0-9]*)/([0-9]*) serialized/live bytes, ([0-9]*) ops\)',
			'fields': ['column_family', 'address', 'serialized_bytes', 'live_bytes', 'ops'],
			'action': save_flush
		},
		{
			#Completed flushing /var/lib/cassandra/data/Mobile/OfferReservation/Mobile-OfferReservation-ic-29240-Data.db (32373 bytes) for commitlog position ReplayPosition(segmentId=1381283075980, position=3580)
			'regex': r'Completed flushing ([^ ]*) \(([0-9]*) bytes\) for commitlog position ReplayPosition\(segmentId=([0-9]*), position=([0-9]*)\)',
			'fields': ['filename', 'file_size', 'segment_id', 'position'],
			'action': save_flush
		}
	]
}

# pre-compile regexes
line_pattern = re.compile(line_pattern)
for rule_group in message_rules.values():
	for rule in rule_group:
		rule['regex'] = re.compile(rule['regex'])

class SystemLogParser:
	def apply_rules(self, line):
		if line['source_file'] in message_rules:
			for rule in message_rules[line['source_file']]:
				submatch = rule['regex'].match(line['message'])
				if submatch is not None:
					if 'fields' in rule:
						fields = dict(zip(rule['fields'], submatch.groups()))
						if 'field_parsers' in rule:
							for name, value in fields.iteritems():
								if name in rule['field_parsers']:
									fields[name] = rule['field_parsers'][name](value)
						if 'wrapper' in rule:
							fields = { rule['wrapper']: fields }
						line.update(fields)
					else:
						fields = None
					if 'action' in rule:
						if callable(rule['action']):
							rule['action'](self, line, fields)
						elif rule['action'] == 'update':
							self.sessions[-1].update(fields)
					if 'index_name' in rule:
						if rule['index_name'] not in self.sessions[-1]:
							self.sessions[-1][rule['index_name']] = []
						self.sessions[-1][rule['index_name']].append(line)

	def __init__(self):
		self.sessions = [{}]
		self.lines = []

		for line in fileinput.input():
			match = line_pattern.match(line)
			if match is not None:
				line = dict(zip(line_fields, match.groups()))
				line['date'] = datetime.strptime(line['date'], date_format)
				line['log_file'] = fileinput.filename()
				line['log_line'] = fileinput.filelineno()
				self.apply_rules(line)
				self.lines.append(line)

log = SystemLogParser()
pp = pprint.PrettyPrinter()
pp.pprint(log.sessions)
