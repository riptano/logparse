import re
import collections
import datetime
import fileinput
import requests
import json
import pytz


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
                return action.__name__, fields
            else:
                return None, None
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
        try:
            dictionary[field] = func(dictionary[field])
        except:
            pass


class SystemLog:
    'Cassandra system.log parser'
    line_rules = []
    message_rules = collections.defaultdict(list)

    def __init__(self, files=None):
        self.lines = []
        self.sessions = [{}]
        self.saved_fields = collections.defaultdict(dict)
        self.unknown_lines = []
        self.unknown_messages = []
        fi = fileinput.FileInput(files)
        for line in fi:
            for rule in self.line_rules:
                if rule(self, line.strip(), extra_fields=dict(log_id=fi.filename(), log_lineno=fi.filelineno())):
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

    def elastic_index(self, url):
        handler = lambda obj: obj.isoformat() if hasattr(obj, 'isoformat') else obj
        for line in self.lines:
            requests.post(url, json.dumps(line, default=handler))

    def solr_index(self, url):
        handler = lambda obj: obj.strftime('%Y-%m-%dT%H:%M:%SZ') if hasattr(obj, 'strftime') else obj
        if not url.endswith('/update'):
            url += '/update'
        docs = []
        for line in self.lines:
            line_fields = {}
            line_fields.update(line)
            line_fields['date'] = pytz.UTC.localize(line_fields['date'])
            if 'message_fields' in line_fields:
                message_fields = line_fields.pop('message_fields')
                for key, value in message_fields.iteritems():
                    if type(value) == int:
                        line_fields['i_' + key] = value
                    elif type(value) == float:
                        line_fields['f_' + key] = value
                    elif type(value) == str:
                        line_fields['s_' + key] = str(value)
                    elif type(value) == datetime.datetime:
                        line_fields['d_' + key] = pytz.UTC.localize(value)
                    else:
                        line_fields['s_' + key] = json.dumps(value, default=handler)
            docs.append(line_fields)
        requests.post(url, json.dumps(docs, default=handler), headers={'Content-type': 'application/json'})

    @group(line_rules)
    @regex(r'(?P<level>[A-Z]{4,5}) \[(?P<thread>[^\]]*)\] (?P<date>.{10} .{12}) (?P<source_file>[^ ]*) \(line (?P<source_lineno>[0-9]*)\) (?P<message>.*)')
    def message_line(self, line_fields, extra_fields):
        'Parse main message line'
        line_fields['level'] = line_fields['level'].strip()
        line_fields['date'] = datetime.datetime.strptime(line_fields['date'], '%Y-%m-%d %H:%M:%S,%f')
        line_fields['session'] = len(self.sessions) - 1
        if extra_fields is not None:
            line_fields.update(extra_fields)
        for rule in self.message_rules[line_fields['source_file'][:-5]]:
            message_type, message_fields = rule(self, line_fields['message'], line_fields)
            if message_type is not None:
                line_fields['type'] = message_type
                line_fields['message_fields'] = message_fields
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
        message_fields['percent_full'] = float(message_fields['percent_full']) * 100
        self.append_session('heap_warnings', message_fields)

    @group(message_rules['GCInspector'])
    @regex(r'GC for (?P<type>[A-Za-z]*): (?P<duration>[0-9]*) ms for (?P<collections>[0-9]*) collections, (?P<used>[0-9]*) used; max is (?P<max>[0-9]*)')
    def garbage_collection(self, message_fields, line_fields):
        message_fields['date'] = line_fields['date']
        convert(message_fields, ('duration', 'collections', 'used', 'max'), int)
        self.append_session('garbage_collections', message_fields)

    #Enqueuing flush of Memtable-hints@424700025(76/760 serialized/live bytes, 4 ops)
    @group(message_rules['ColumnFamilyStore'])
    @regex(r'Enqueuing flush of Memtable-(?P<column_family>[^@]*)@(?P<hash_code>[0-9]*)\((?P<serialized_bytes>[0-9]*)/(?P<live_bytes>[0-9]*) serialized/live bytes, (?P<ops>[0-9]*) ops\)')
    def begin_flush(self, message_fields, line_fields):
        message_fields['enqueue_date'] = line_fields['date']
        convert(message_fields, ('hash_code', 'serialized_bytes', 'live_bytes', 'ops'), int)
        self.saved_fields[message_fields['column_family'], message_fields['hash_code']] = message_fields

    @group(message_rules['Memtable'])
    @regex(r'Writing Memtable-(?P<column_family>[^@]*)@(?P<hash_code>[0-9]*)\((?P<serialized_bytes>[0-9]*)/(?P<live_bytes>[0-9]*) serialized/live bytes, (?P<ops>[0-9]*) ops\)')
    def begin_flush(self, message_fields, line_fields):
        convert(message_fields, ('hash_code', 'serialized_bytes', 'live_bytes', 'ops'), int)
        enqueue_fields = self.saved_fields.get((message_fields['column_family'], message_fields['hash_code']), {})
        message_fields['enqueue_date'] = enqueue_fields.get('enqueue_date')
        message_fields['begin_date'] = line_fields['date']
        if line_fields['thread'] not in self.saved_fields:
            self.saved_fields[line_fields['thread']] = {}
        self.saved_fields[line_fields['thread']].update(message_fields)

    @group(message_rules['Memtable'])
    @regex(r'Completed flushing (?P<filename>[^ ]*) \((?P<file_size>[0-9]*) bytes\) for commitlog position ReplayPosition\(segmentId=(?P<segment_id>[0-9]*), position=(?P<position>[0-9]*)\)')
    def end_flush(self, message_fields, line_fields):
        message_fields['end_date'] = line_fields['date']
        convert(message_fields, ('file_size', 'segment_id', 'position'), int)
        message_fields.update(self.saved_fields[line_fields['thread']])
        del self.saved_fields[line_fields['thread']]
        self.append_session('flushes', message_fields)

    @group(message_rules['CompactionTask'])
    @regex(r'Compacting \[(?P<input_sstables>[^\]]*)\]')
    def begin_compaction(self, message_fields, line_fields):
        message_fields['begin_date'] = line_fields['date']
        message_fields['input_sstables'] = [sstable[20:-2] for sstable in message_fields['input_sstables'].split(', ')]
        self.saved_fields[line_fields['thread']] = message_fields

    @group(message_rules['CompactionTask'])
    @regex(r'Compacted (?P<sstable_count>[0-9]*) sstables to \[(?P<output_sstable>[^,]*),\].  (?P<input_bytes>[0-9,]*) bytes to (?P<output_bytes>[0-9,]*) \(~(?P<percent_of_original>[0-9]*)% of original\) in (?P<duration>[0-9,]*)ms = (?P<rate>[0-9.]*)MB/s.  (?P<total_rows>[0-9,]*) total rows, (?P<unique_rows>[0-9,]*) unique.  Row merge counts were \{(?P<row_merge_counts>[^}]*)\}')
    def end_compaction(self, message_fields, line_fields):
        message_fields['end_date'] = line_fields['date']
        message_fields['rate'] = float(message_fields['rate'])
        convert(message_fields,
                ('sstable_count', 'input_bytes', 'output_bytes', 'percent_of_original', 'duration', 'total_rows', 'unique_rows'),
                lambda value: int(value.replace(',', '')))
        message_fields.update(self.saved_fields[line_fields['thread']])
        del self.saved_fields[line_fields['thread']]
        self.append_session('compactions', message_fields)

    @group(message_rules['CompactionController'])
    @regex(r'Compacting large row (?P<keyspace>[^/]*)/(?P<table>[^:]*):(?P<row_key>[0-9]*) \((?P<row_size>[0-9]*) bytes\) incrementally')
    def incremental_compaction(self, message_fields, line_fields):
        message_fields['row_size'] = int(message_fields['row_size'])
        message_fields['date'] = line_fields['date']
        compaction = self.saved_fields[line_fields['thread']]
        if 'incremental_rows' not in compaction:
            compaction['incremental_rows'] = []
        compaction['incremental_rows'].append(message_fields)

    @group(message_rules['AntiEntropyService'])
    @regex(r'\[repair #(?P<session_id>[^\]]*)\] new session: will sync (?P<nodes>[^o]*) on range \((?P<range_begin>[^,]*),(?P<range_end>[^\]]*)\] for (?P<keyspace>[^.]*)\.\[(?P<column_families>[^\]]*)\]')
    def begin_repair(self, message_fields, line_fields):
        message_fields['start_date'] = line_fields['date']
        message_fields['nodes'] = message_fields['nodes'].split(', ')
        message_fields['column_families'] = message_fields['column_families'].split(', ')
        self.saved_fields[message_fields['session_id']] = message_fields

    @group(message_rules['AntiEntropyService'])
    @regex(r'\[repair #(?P<session_id>[^\]]*)\] requesting merkle trees for (?P<column_family>[^ ]*) \(to \[(?P<nodes>[^\]]*)\]\)')
    def merkle_requested(self, message_fields, line_fields):
        merkle_requests = self.saved_fields[message_fields['session_id']].get('merkle_requests', {})
        for node in message_fields['nodes'].split(', '):
            merkle_requests[message_fields['column_family'], node] = {
                'request_date': line_fields['date'],
                'column_family': message_fields['column_family'],
                'node': node
            }
        self.saved_fields[message_fields['session_id']]['merkle_requests'] = merkle_requests

    @group(message_rules['AntiEntropyService'])
    @regex(r'\[repair #(?P<session_id>[^\]]*)\] Received merkle tree for (?P<column_family>[^ ]*) from (?P<node>.*)')
    def merkle_received(self, message_fields, line_fields):
        merkle_requests = self.saved_fields[message_fields['session_id']].get('merkle_requests', {})
        merkle_requests[message_fields['column_family'], message_fields['node']]['receive_date'] = line_fields['date']
        self.saved_fields[message_fields['session_id']]['merkle_requests'] = merkle_requests

    @group(message_rules['AntiEntropyService'])
    @regex(r'\[repair #(?P<session_id>[^\]]*)\] Sending completed merkle tree to (?P<node>[^ ]*) for \((?P<keyspace>[^,]*),(?P<column_family>[^)]*)\)')
    def merkle_sent(self, message_fields, line_fields):
        message_fields['date'] = line_fields['date']
        sent_merkles = self.saved_fields[message_fields['session_id']].get('sent_merkles', [])
        sent_merkles.append(message_fields)
        self.saved_fields[message_fields['session_id']]['sent_merkles'] = sent_merkles

    @group(message_rules['AntiEntropyService'])
    @regex(r'\[repair #(?P<session_id>[^\]]*)\] Endpoints (?P<node1>[^ ]*) and (?P<node2>[^ ]*) are consistent for (?P<column_family>.*)')
    def endpoints_consistent(self, message_fields, line_fields):
        message_fields['date'] = line_fields['date']
        consistent_endpoints = self.saved_fields[message_fields['session_id']].get('consistent_endpoints', [])
        consistent_endpoints.append(message_fields)
        self.saved_fields[message_fields['session_id']]['consistent_endpoints'] = consistent_endpoints

    @group(message_rules['AntiEntropyService'])
    @regex(r'\[repair #(?P<session_id>[^\]]*)\] Endpoints (?P<node1>[^ ]*) and (?P<node2>[^ ]*) have (?P<ranges>[0-9]*) range\(s\) out of sync for (?P<column_family>.*)')
    def endpoints_inconsistent(self, message_fields, line_fields):
        message_fields['date'] = line_fields['date']
        inconsistent_endpoints = self.saved_fields[message_fields['session_id']].get('inconsistent_endpoints', [])
        inconsistent_endpoints.append(message_fields)
        self.saved_fields[message_fields['session_id']]['inconsistent_endpoints'] = inconsistent_endpoints

    @group(message_rules['AntiEntropyService'])
    @regex(r'\[repair #(?P<session_id>[^\]]*)\] (?P<column_family>[^ ]*) is fully synced( \((?P<cfs_remaining>[0-9]*) remaining column family to sync for this session\))?')
    def columnfamily_synced(self, message_fields, line_fields):
        message_fields['date'] = line_fields['date']
        synced_columnfamilies = self.saved_fields[message_fields['session_id']].get('synced_columnfamilies', [])
        synced_columnfamilies.append(message_fields)
        self.saved_fields[message_fields['session_id']]['synced_columnfamilies'] = synced_columnfamilies

    @group(message_rules['AntiEntropyService'])
    @regex(r'\[repair #(?P<session_id>[^\]]*)\] session completed successfully')
    def end_repair(self, message_fields, line_fields):
        repair_session = self.saved_fields[message_fields['session_id']]
        repair_session['end_date'] = line_fields['date']
        repair_session['merkle_requests'] = repair_session['merkle_requests'].values()
        self.append_session('repair_sessions', repair_session)
        del self.saved_fields[message_fields['session_id']]

    @group(message_rules['StreamInSession'])
    @regex(r'Finished streaming session (?P<session_id>[^ ]*) from (?P<node>.*)')
    def finished_streaming(self, message_fields, line_fields):
        message_fields['end_date'] = line_fields['date']
        self.append_session('streaming_sessions', message_fields)

    @group(message_rules['StreamReplyVerbHandler'])
    @regex(r'Successfully sent (?P<sstable_name>[^ ]*) to (?P<node>.*)')
    def finished_streaming(self, message_fields, line_fields):
        message_fields['date'] = line_fields['date']
        self.append_session('sstables_sent', message_fields)

    @group(message_rules['SSTableReader'])
    @regex(r'Opening (?P<sstable_name>[^ ]*) \((?P<bytes>[0-9]*) bytes\)')
    def finished_streaming(self, message_fields, line_fields):
        message_fields['date'] = line_fields['date']
        message_fields['bytes'] = int(message_fields['bytes'])
        self.append_session('opened_sstables', message_fields)

    @group(message_rules['StatusLogger'])
    @regex(r'Pool Name *Active *Pending *Completed *Blocked *All Time Blocked')
    def pool_header(self, message_fields, line_fields):
        self.append_session('status', {
            'date': line_fields['date'],
            'thread_pool': [],
            'caches': [],
            'memtables': []
        })

    @group(message_rules['StatusLogger'])
    @regex(r'(?P<pool_name>[A-Za-z_]+) +(?P<active>[0-9]+) +(?P<pending>[0-9]+) +(?P<completed>[0-9]+) +(?P<blocked>[0-9]+) +(?P<all_time_blocked>[0-9]+)')
    def pool_info(self, message_fields, line_fields):
        convert(message_fields, ('active', 'pending', 'completed', 'blocked', 'all_time_blocked'), int)
        if 'status' in self.sessions[-1]:
            self.sessions[-1]['status'][-1]['thread_pool'].append(message_fields)

    @group(message_rules['StatusLogger'])
    @regex(r'Cache Type *Size *Capacity *KeysToSave *Provider')
    def cache_header(self, message_fields, line_fields):
        pass

    @group(message_rules['StatusLogger'])
    @regex(r'(?P<type>[A-Za-z]*Cache(?! Type)) *(?P<size>[0-9]*) *(?P<capacity>[0-9]*) *(?P<keys_to_save>[^ ]*) *(?P<provider>[A-Za-z_.$]*)')
    def cache_info(self, message_fields, line_fields):
        convert(message_fields, ('size', 'capacity'), int)
        if 'status' in self.sessions[-1]:
            self.sessions[-1]['status'][-1]['caches'].append(message_fields)

    @group(message_rules['StatusLogger'])
    @regex(r'ColumnFamily *Memtable ops,data')
    def memtable_header(self, message_fields, line_fields):
        pass

    @group(message_rules['StatusLogger'])
    @regex(r'(?P<keyspace>[^.]*)\.(?P<column_family>[^ ]*) *(?P<ops>[0-9]*),(?P<data>[0-9]*)')
    def memtable_info(self, message_fields, line_fields):
        convert(message_fields, ('ops', 'data'), int)
        if 'status' in self.sessions[-1]:
            self.sessions[-1]['status'][-1]['memtables'].append(message_fields)
