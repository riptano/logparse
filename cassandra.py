import re
import collections
import datetime
import fileinput


class regex:
    'Decorator converts a single string into a dictionary of fields using the provided regex.'
    def __init__(self, regex):
        if type(regex) == str:
            self.regex = re.compile(regex)
        else:
            self.regex = regex
    
    def __call__(self, action):
        def rule(log, input, extra_fields=None):
            match = self.regex.match(input)
            if match:
                fields = match.groupdict()
                action(log, fields, extra_fields)
                return fields
            else:
                return None
        return rule

    
class group:
    'Decorator registers a rule in the specified list.'
    def __init__(self, *args):
        self.rule_lists = args
    
    def __call__(self, rule):
        for rule_list in self.rule_lists:
            rule_list.append(rule)
        return rule


def convert(dictionary, field_list, func):
    'Convenience function to convert a list of fields using a function.'
    for field in field_list:
        dictionary[field] = func(dictionary[field])

    
class SystemLog:
    'Cassandra system.log parser'
    line_rules = []
    message_rules = collections.defaultdict(list)
    
    def __init__(self, files=None):
        self.lines = []
        self.sessions = [{}]
        self.field_stack = collections.defaultdict(list)
        self.unknown_lines = []
        self.unknown_messages = []
        fi = fileinput.FileInput(files)
        for line in fi:
            for rule in self.line_rules:
                line_fields = rule(self, line.strip(), extra_fields=dict(log_file=fi.filename(), log_lineno=fi.filelineno()))
                if line_fields is not None:
                    break
            else:
                self.unknown_lines.append(line)
                    
    def update_session(self, key, fields):
        if key not in self.sessions[-1]:
            self.sessions[-1][key] = {}
        self.sessions[-1][key].update(fields)
        
    def append_session(self, key, fields):
        if key not in self.sessions[-1]:
            self.sessions[-1][key] = []
        self.sessions[-1][key].append(fields)
        
    def push_fields(self, key, fields):
        self.field_stack[key].append(fields)
        
    def pop_fields(self, key):
		return self.field_stack[key].pop() if self.field_stack[key] else {}


    @group(line_rules)
    @regex(r'(?P<level>[A-Z]{4,5}) \[(?P<thread>[^\]]*)\] (?P<date>.{10} .{12}) (?P<source_file>[^ ]*) \(line (?P<source_lineno>[0-9]*)\) (?P<message>.*)')
    def message_line(self, line_fields, extra_fields):
        'Parse main message line'
        line_fields['level'] = line_fields['level'].strip()
        line_fields['date'] = datetime.datetime.strptime(line_fields['date'], '%Y-%m-%d %H:%M:%S,%f')
        if extra_fields is not None:
            line_fields.update(extra_fields)
        for rule in self.message_rules[line_fields['source_file'][:-5]]:
            if rule(self, line_fields['message'], line_fields):
                break
        else:
            self.unknown_messages.append(line_fields)
        if line_fields['level'] == 'ERROR':
        	self.append_session('errors', line_fields)
        elif line_fields['level'] == 'WARN':
        	self.append_session('warnings', line_fields)
        self.lines.append(line_fields)

    @group(line_rules)
    @regex(r'^(?!Caused by:)(?P<exception>[^:]*): (?P<exception_message>.*)')
    def exception_line(self, line_fields, extra_fields):
        'Parse exception line and update previous message line'
        self.lines[-1].update(line_fields)

    @group(line_rules)
    @regex(r'^Caused by: (?P<exception>[^:]*): (?P<exception_message>.*)')
    def caused_by_line(self, line_fields, extra_fields):
        'Parse nested exceptions'
        if 'caused_by' not in self.lines[-1]:
            self.lines[-1]['caused_by'] = []
        self.lines[-1]['caused_by'].append(line_fields)

    @group(line_rules)
    @regex(r'at (?P<method>[^(]*)\((?P<source_file>[^:)]*):?(?P<source_lineno>[0-9]*)\)')
    def trace_line(self, line_fields, extra_fields):
        'Parse stack trace line and append to previous message line'
        line_fields['source_lineno'] = None if line_fields['source_lineno'] == '' else int(line_fields['source_lineno'])
        components = line_fields['method'].split('.')
        line_fields['package'] = '.'.join(components[:-3])
        line_fields['class'] = components[-2]
        line_fields['method'] = components[-1]
        if 'caused_by' in self.lines[-1]:
            if 'stack_trace' not in self.lines[-1]['caused_by'][-1]:
        	    self.lines[-1]['caused_by'][-1]['stack_trace'] = []
            self.lines[-1]['caused_by'][-1]['stack_trace'].append(line_fields)
        else:
            if 'stack_trace' not in self.lines[-1]:
                self.lines[-1]['stack_trace'] = []
            self.lines[-1]['stack_trace'].append(line_fields)
        
    @group(message_rules['CassandraDaemon'])
    @regex(r'Logging initialized')
    def new_session(self, message_fields, line_fields):
        'Catch cassandra restart and begin a new session'
        if self.sessions != [{}]:
            self.sessions.append({})
        self.sessions[-1]['start_date'] = line_fields['date']
    
    @group(message_rules['CassandraDaemon'])
    @regex(r'JVM vendor/version: (?P<jvm>.*)')
    def jvm_vendor(self, message_fields, line_fields):
        self.update_session('environment', message_fields)
    
    @group(message_rules['CassandraDaemon'])
    @regex(r'Heap size: (?P<heap_used>[0-9]*)/(?P<total_heap>[0-9]*)')
    def heap_size(self, message_fields, line_fields):
        convert(message_fields, ('heap_used', 'total_heap'), int)
        self.update_session('environment', message_fields)
    
    @group(message_rules['CassandraDaemon'])
    @regex(r'Classpath: (?P<classpath>.*)')
    def classpath(self, message_fields, line_fields):
        message_fields['classpath'] = message_fields['classpath'].split(':')
        self.update_session('environment', message_fields)
        
    @group(message_rules['DseDaemon'], message_rules['StorageService'])
    @regex(r'(?P<component>[A-Za-z ]*) versions?: (?P<version>.*)')
    def component_version(self, message_fields, line_fields):
        self.append_session('versions', message_fields)
        
    @group(message_rules['GCInspector'])
    @regex(r'Heap is (?P<percent_full>[0-9.]*) full.*')
    def heap_full(self, message_fields, line_fields):
        message_fields['date'] = line_fields['date']
        message_fields['percent_full'] = float(message_fields['percent_full'])
        self.append_session('heap_warnings', message_fields)
	
    @group(message_rules['GCInspector'])
    @regex(r'GC for (?P<type>[A-Za-z]*): (?P<duration>[0-9]*) ms for (?P<collections>[0-9]*) collections, (?P<used>[0-9]*) used; max is (?P<max>[0-9]*)')
    def garbage_collection(self, message_fields, line_fields):
        message_fields['date'] = line_fields['date']
        convert(message_fields, ('duration', 'collections', 'used', 'max'), int)
        self.append_session('garbage_collections', message_fields)

    @group(message_rules['Memtable'])
    @regex(r'Writing Memtable-(?P<column_family>[^@]*)@(?P<hash_code>[0-9]*)\((?P<serialized_bytes>[0-9]*)/(?P<live_bytes>[0-9]*) serialized/live bytes, (?P<ops>[0-9]*) ops\)')
    def begin_flush(self, message_fields, line_fields):
        message_fields['begin_date'] = line_fields['date']
        convert(message_fields, ('hash_code', 'serialized_bytes', 'live_bytes', 'ops'), int)
        self.push_fields(line_fields['thread'], message_fields)
    
    @group(message_rules['Memtable'])
    @regex(r'Completed flushing (?P<filename>[^ ]*) \((?P<file_size>[0-9]*) bytes\) for commitlog position ReplayPosition\(segmentId=(?P<segment_id>[0-9]*), position=(?P<position>[0-9]*)\)')
    def end_flush(self, message_fields, line_fields):
        message_fields['end_date'] = line_fields['date']
        convert(message_fields, ('file_size', 'segment_id', 'position'), int)
        message_fields.update(self.pop_fields(line_fields['thread']))
        self.append_session('flushes', message_fields)
			
    @group(message_rules['CompactionTask'])
    @regex(r'Compacting \[(?P<input_sstables>[^\]]*)\]')
    def begin_compaction(self, message_fields, line_fields):
        message_fields['begin_date'] = line_fields['date']
        message_fields['input_sstables'] = [sstable[20:-2] for sstable in message_fields['input_sstables'].split(', ')]
        self.push_fields(line_fields['thread'], message_fields)

    @group(message_rules['CompactionTask'])
    @regex(r'Compacted (?P<sstable_count>[0-9]*) sstables to \[(?P<output_sstable>[^,]*),\].  (?P<input_bytes>[0-9,]*) bytes to (?P<output_bytes>[0-9,]*) \(~(?P<percent_of_original>[0-9]*)% of original\) in (?P<duration>[0-9,]*)ms = (?P<rate>[0-9.]*)MB/s.  (?P<total_rows>[0-9,]*) total rows, (?P<unique_rows>[0-9,]*) unique.  Row merge counts were \{(?P<row_merge_counts>[^}]*)\}')
    def end_compaction(self, message_fields, line_fields):
        message_fields['end_date'] = line_fields['date']
        message_fields['rate'] = float(message_fields['rate'])
        convert(message_fields, 
                ('sstable_count', 'input_bytes', 'output_bytes', 'percent_of_original', 'duration', 'total_rows', 'unique_rows'), 
                lambda value: int(value.replace(',', '')))
        message_fields.update(self.pop_fields(line_fields['thread']))
        self.append_session('compactions', message_fields)

    @group(message_rules['CompactionController'])
    @regex(r'Compacting large row (?P<keyspace>[^/]*)/(?P<table>[^:]*):(?P<row_key>[0-9]*) \((?P<row_size>[0-9]*) bytes\) incrementally')
    def incremental_compaction(self, message_fields, line_fields):
        message_fields['row_size'] = int(message_fields['row_size'])
        message_fields['date'] = line_fields['date']
        compaction = self.pop_fields(line_fields['thread'])
        if 'incremental_rows' not in compaction:
            compaction['incremental_rows'] = []
        compaction['incremental_rows'].append(message_fields)
        self.push_fields(line_fields['thread'], compaction)
