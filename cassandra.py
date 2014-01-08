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
			'regex': r'Heap size: ([0-9]*)/([0-9]*)',
			'fields': ['heap_used', 'total_heap'],
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
			'field_parsers': { 'percent_full': float },
			'index_name': 'heap_warnings'
		},
		{
			# GC for ParNew: 1020 ms for 2 collections, 172365056 used; max is 8506048512
			'regex': r'GC for ([A-Za-z]*): ([0-9]*) ms for ([0-9]*) collections, ([0-9]*) used; max is ([0-9]*)',
			'fields': ['type', 'duration', 'collections', 'used', 'max'],
			'field_parsers': { 'duration': long, 'collections': long, 'used': long, 'max': long },
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
import re

line_pattern = re.compile(line_pattern)
for rule_group in message_rules.values():
	for rule in rule_group:
		rule['regex'] = re.compile(rule['regex'])