from fileinput import FileInput
from datetime import datetime
from baserules import Rule, RuleSet

class LineRules(RuleSet):
	class LogLine(Rule):
		def __init__(self, ruleset, target):
			super(LineRules.LogLine, self).__init__(ruleset, target)
			self.message_rules = MessageRules(target)
		regex = r'(?P<level>.{5}) \[(?P<thread_name>[A-Za-z]*):?(?P<thread_id>[0-9]*)\] (?P<date>.{10} .{12}) (?P<source_file>[^ ]*) \(line (?P<source_lineno>[0-9]*)\) (?P<message>.*)'
		@staticmethod
		def parse_date(date):
			return datetime.strptime(date, '%Y-%m-%d %H:%M:%S,%f')
		@staticmethod
		def parse_thread_id(thread_id):
			if thread_id == '':
				return None
			else: 
				return long(thread_id)
		parse_source_lineno = long
		def action(self, fields):
			self.message_rules.apply(fields['message'], fields['source_file'], fields)
			self.target.append_line(fields)


class MessageRules(RuleSet):

	class LoggingInitialized(Rule):
		rule_group = 'CassandraDaemon.java'
		regex = r'Logging initialized'
		def action(self, fields):
			self.target.new_session(fields)
	
	class JVMVendor(Rule):
		rule_group = 'CassandraDaemon.java'
		regex = r'JVM vendor/version: (?P<jvm>.*)'
		def action(self, fields):
			self.target.update_session(fields)
		
	class HeapSize(Rule):
		rule_group = 'CassandraDaemon.java'
		regex = r'Heap size: (?P<heap_used>[0-9]*)/(?P<total_heap>[0-9]*)'
		parse_heap_used = long
		parse_total_heap = long				
		def action(self, fields):
			self.target.update_session(fields)
		
	class Classpath(Rule):
		rule_group = 'CassandraDaemon.java'
		regex = r'Classpath: (?P<classpath>.*)'
		@staticmethod
		def parse_classpath(cp):
			return cp.split(':')
		def action(self, fields):
			self.target.update_session(fields)
			
	class Version(Rule):
		rule_groups = ('CassandraDaemon.java', 'StorageService.java')
		regex = r'(?P<component>[A-Za-z ]*) versions?: (?P<version>.*)'
		def action(self, fields):
			self.target.update_session(fields, 'versions')
		
	class HeapFull(Rule):
		rule_group = 'GCInspector.java'
		regex = r'Heap is (?P<percent_full>[0-9.]*) full.*'
		parse_percent_full = float
		def action(self, fields):
			self.target.update_session(fields, 'heap_full')
	
	class GarbageCollection(Rule):
		rule_group = 'GCInspector.java'
		regex = r'GC for (?P<type>[A-Za-z]*): (?P<duration>[0-9]*) ms for (?P<collections>[0-9]*) collections, (?P<used>[0-9]*) used; max is (?P<max>[0-9]*)'
		parse_duration = long
		parse_collections = long
		parse_used = long
		parse_max = long
		def action(self, fields):
			self.target.update_session(fields, 'garbage_collections')
		
	class BeginFlush(Rule):
		rule_group = 'Memtable.java'
		regex = r'Writing Memtable-(?P<column_family>[^@]*)@(?P<address>[0-9]*)\((?P<serialized_bytes>[0-9]*)/(?P<live_bytes>[0-9]*) serialized/live bytes, (?P<ops>[0-9]*) ops\)'
		def action(self, fields):
			self.target.begin_flush(fields)
			
	class EndFlush(Rule):
		rule_group = 'Memtable.java'
		regex = r'Completed flushing (?P<filename>[^ ]*) \((?P<file_size>[0-9]*) bytes\) for commitlog position ReplayPosition\(segmentId=(?P<segment_id>[0-9]*), position=(?P<position>[0-9]*)\)'
		def action(self, fields):
			self.target.end_flush(fields)
			

class SystemLog(object):
	def __init__(self, files=None):
		self.lines = []
		self.sessions = [{}]
		self.threads = {}
		line_rules = LineRules(self)
		fi = FileInput(files)
		for line in fi:
			line_rules.apply(line, fields=dict(log_file=fi.filename(), log_lineno=fi.filelineno()))
			
	def append_line(self, fields):
		self.lines.append(fields)

	def new_session(self, fields):
		if self.sessions != [{}]:
			self.sessions.append({})
		self.sessions[-1]['start_date'] = fields['date']
		
	def update_session(self, fields, group=None):
		if group is None:
			self.sessions[-1].update(fields)
		elif group in self.sessions[-1]:
			self.sessions[-1][group].append(fields)
		else:
			self.sessions[-1][group] = [fields]
		
	def begin_flush(self, fields):
		self.current_flush = fields

	def end_flush(self, fields):
		if self.current_flush is None:
			self.current_flush = {}
		self.current_flush.update(fields)
		self.update_session(self.current_flush, 'memtable_flushes')
		self.current_flush = None