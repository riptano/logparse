#!/usr/bin/python
# Log parsing proof of concept

import sys
import fileinput
import re
from datetime import datetime
from rules import line_pattern, line_fields, date_format, message_rules

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

	def __init__(self, files=None):
		if files is None:
			files = sys.argv[1:]
		self.sessions = [{}]
		self.lines = []
		
		fi = fileinput.FileInput(files)
		for line in fi:
			match = line_pattern.match(line)
			if match is not None:
				line = dict(zip(line_fields, match.groups()))
				line['date'] = datetime.strptime(line['date'], date_format)
				line['log_file'] = fi.filename()
				line['log_line'] = fi.filelineno()
				self.apply_rules(line)
				self.lines.append(line)