import re
from datetime import datetime

def switch(rule_groups):
    def inner_switch(group, string):
        if group in rule_groups:
            return rule_groups[group](string)
        return None
    return inner_switch

def first(*rules):
    def inner_first(string):
        for rule in rules:
            fields = rule(string)
            if fields is not None:
                return fields
        return None
    return inner_first


def pipeline(source, *transforms):
    def inner_pipeline(string):
        fields = source(string)
        if fields is not None:
            for transform in transforms:
                transform(fields)
        return fields
    return inner_pipeline

def capture(*regex_strings):
    regexes = []
    for regex in regex_strings:
        regexes.append(re.compile(regex))
    def inner_capture(string):
        for regex in regexes:
            capture =  regex.match(string)
            if capture:
                return capture.groupdict()
        return None
    return inner_capture

def convert(func, *field_names):
    def inner_convert(fields):
        for field_name in field_names:
            if field_name in fields and fields[field_name] is not None:
                fields[field_name] = func(fields[field_name])
    return inner_convert


def update(**extras):
    return lambda fields: fields.update(extras)

def strip(string):
    return string.strip()

def date(format):
    return lambda date: datetime.strptime(date, format)

def split(delimiter):
    return lambda string: string.split(delimiter)

def percent(value):
    return float(value) * 100

def sstables(value):
    return [sstable[20:-2] for sstable in value.split(', ')]

def int_with_commas(value):
    return int(value.replace(',', ''))

capture_message = switch({

    'CassandraDaemon': 

        first(

            pipeline(
                capture(r'Logging initialized'), 
                update(event_type='new_session')),

            pipeline(
                capture(r'JVM vendor/version: (?P<jvm>.*)'), 
                update(event_type='jvm_vendor')),

            pipeline(
                capture(r'Heap size: (?P<heap_used>[0-9]*)/(?P<total_heap>[0-9]*)'), 
                convert(int, 'heap_used', 'total_heap'), 
                update(event_type='heap_size')),

            pipeline(
                capture(r'Classpath: (?P<classpath>.*)'),
                convert(split(':'), 'classpath'),
                update(event_type='classpath'))),

    'DseDaemon': 

        pipeline(
            capture(r'(?P<component>[A-Za-z ]*) versions?: (?P<version>.*)'),
            update(event_type='component_version')),

    'GCInspector': 

        first(

            pipeline(
                capture(r'Heap is (?P<percent_full>[0-9.]*) full.*'),
                convert(percent, 'percent_full'),
                update(event_type='heap_full')),

            pipeline(
                capture(r'GC for (?P<gc_type>[A-Za-z]*): (?P<duration>[0-9]*) ms for (?P<collections>[0-9]*) collections, (?P<used>[0-9]*) used; max is (?P<max>[0-9]*)'),
                convert(int, 'duration', 'collections', 'used', 'max'),
                update(event_type='garbage_collection'))),

    'ColumnFamilyStore':

        first(
    
            pipeline(
                capture(
                    r'Enqueuing flush of Memtable-(?P<table>[^@]*)@(?P<hash_code>[0-9]*)\((?P<serialized_bytes>[0-9]*)/(?P<live_bytes>[0-9]*) serialized/live bytes, (?P<ops>[0-9]*) ops\)',
                    r'Enqueuing flush of (?P<table>[^:]*): (?P<on_heap_bytes>[0-9]*) \((?P<on_heap_limit>[0-9]*)%\) on-heap, (?P<off_heap_bytes>[0-9]*) \((?P<off_heap_limit>[0-9]*)%\) off-heap'),
                convert(int, 'hash_code', 'serialized_bytes', 'live_bytes', 'ops', 'on_heap_bytes', 'off_heap_bytes', 'on_heap_limit', 'off_heap_limit'),
                update(event_type='enqueue_flush')), 

            pipeline(
                capture(r'Initializing (?P<keyspace>[^.]*).(?P<table>.*)'),
                update(event_type='initializing_table')),

            pipeline(
                capture(r'Flushing SecondaryIndex Cql3SolrSecondaryIndex\{columnDefs=\[(?P<column_defs>).*\]\}'),
                convert(split(', '), 'column_defs'),
                update(event_type='flushing_secondary_index'))),

    'Memtable': #'ColumnFamilyStore': 

        first( 

            pipeline(
                capture(
                    r'Writing Memtable-(?P<table>[^@]*)@(?P<hash_code>[0-9]*)\((?P<serialized_bytes>[0-9]*) serialized bytes, (?P<ops>[0-9]*) ops, (?P<on_heap_limit>[0-9]*)%/(?P<off_heap_limit>[0-9]*)% of on/off-heap limit\)',
                    r'Writing Memtable-(?P<table>[^@]*)@(?P<hash_code>[0-9]*)\((?P<serialized_bytes>[0-9]*)/(?P<live_bytes>[0-9]*) serialized/live bytes, (?P<ops>[0-9]*) ops\)'),
                convert(int, 'hash_code', 'serialized_bytes', 'live_bytes', 'ops', 'on_heap_limit', 'off_heap_limit'),
                update(event_type='begin_flush')),

            pipeline(
                capture(r'Completed flushing (?P<filename>[^ ]*) \((?P<file_size>[0-9]*) bytes\) for commitlog position ReplayPosition\(segmentId=(?P<segment_id>[0-9]*), position=(?P<position>[0-9]*)\)'),
                convert(int, 'file_size', 'segment_id', 'position'),
                update(event_type='end_flush'))),

    'CompactionTask': 

        first(

            pipeline(
                capture(r'Compacting \[(?P<input_sstables>[^\]]*)\]'),
                convert(sstables, 'input_sstables'),
                update(event_type='begin_compaction')), 

            pipeline(
                capture(
                    r'Compacted (?P<sstable_count>[0-9]*) sstables to \[(?P<output_sstable>[^,]*),\].  (?P<input_bytes>[0-9,]*) bytes to (?P<output_bytes>[0-9,]*) \(~(?P<percent_of_original>[0-9]*)% of original\) in (?P<duration>[0-9,]*)ms = (?P<rate>[0-9.]*)MB/s.  (?P<total_partitions>[0-9,]*) total (partitions|rows), (?P<unique_partitions>[0-9,]*) unique.  (Row|Partition) merge counts were \{(?P<partition_merge_counts>[^}]*)\}',
                    r'Compacted (?P<sstable_count>[0-9]*) sstables to \[(?P<output_sstable>[^,]*),\].  (?P<input_bytes>[0-9,]*) bytes to (?P<output_bytes>[0-9,]*) \(~(?P<percent_of_original>[0-9]*)% of original\) in (?P<duration>[0-9,]*)ms = (?P<rate>[0-9.]*)MB/s.  (?P<total_partitions>[0-9,]*) total (partitions|rows) merged to (?P<unique_partitions>[0-9,]*).  (Row|Partition) merge counts were \{(?P<partition_merge_counts>[^}]*)\}'),
                convert(int_with_commas, 'sstable_count', 'input_bytes', 'output_bytes', 'percent_of_original', 'duration', 'total_partitions', 'unique_partitions'),
                update(event_type='end_compaction'))),

    'CompactionController': 

        pipeline(
            capture(r'Compacting large (partition|row) (?P<keyspace>[^/]*)/(?P<table>[^:]*):(?P<partition_key>[0-9]*) \((?P<partition_size>[0-9]*) bytes\) incrementally'),
            convert(int, 'partition_size'),
            update(event_type='incremental_compaction')),

    'AntiEntropyService': 

        first(


            pipeline(
                capture(r'\[repair #(?P<session_id>[^\]]*)\] session completed successfully'),
                update(event_type='end_repair'))),

    'Differencer': # 'AntiEntropyService'

        first(

            pipeline(
                capture(r'\[repair #(?P<session_id>[^\]]*)\] Endpoints (?P<node1>[^ ]*) and (?P<node2>[^ ]*) are consistent for (?P<table>.*)'),
                update(event_type='endpoints_consistent')),

            pipeline(
                capture(r'\[repair #(?P<session_id>[^\]]*)\] Endpoints (?P<node1>[^ ]*) and (?P<node2>[^ ]*) have (?P<ranges>[0-9]*) range\(s\) out of sync for (?P<table>.*)'),
                convert(int, 'ranges'),
                update(event_type='endpoints_inconsistent'))), 

    'Validator': # 'AntiEntropyService'

        pipeline(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] Sending completed merkle tree to (?P<node>[^ ]*) for \(?(?P<keyspace>[^,]*)[/,](?P<table>[^)]*)\)?'),
            update(event_type='merkle_sent')), 

    'RepairSession': # 'AntiEntropyService'

        first(

            pipeline(
                capture(r'\[repair #(?P<session_id>[^\]]*)\] Received merkle tree for (?P<table>[^ ]*) from (?P<node>.*)'),
                update(event_type='merkle_received')), 

            pipeline(
                capture(r'\[repair #(?P<session_id>[^\]]*)\] (?P<table>[^ ]*) is fully synced'),
                update(event_type='table_fully_synced')),

            pipeline(
                capture(r'\[repair #(?P<session_id>[^\]]*)\] new session: will sync (?P<nodes>.*?) on range \((?P<range_begin>[^,]*),(?P<range_end>[^\]]*)\] for (?P<keyspace>[^.]*)\.\[(?P<tables>[^\]]*)\]'),
                convert(split(', '), 'nodes', 'tables'),
                update(event_type='begin_repair'))),

    'RepairJob': # 'AntiEntropyService'

        pipeline(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] requesting merkle trees for (?P<table>[^ ]*) \(to \[(?P<nodes>[^\]]*)\]\)'),
            convert(split(', '), 'nodes'),
            update(event_type='merkle_requested')), 

    'StreamInSession':  
        
        pipeline(
            capture(r'Finished streaming session (?P<session_id>[^ ]*) from (?P<node>.*)'),
            update(event_type='finished_streaming')),

    'StreamResultFuture':

        pipeline(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Session with (?P<node>[^ ]*) is complete'),
            update(event_type='finished_streaming')),

    'StreamingRepairTask':

        first(

            pipeline(
                capture(r'\[streaming task #(?P<session_id>[^\]]*)\] Performing streaming repair of (?P<ranges>[0-9]*) ranges with (?P<node>[^ ]*)'),
                update(event_type='performing_repair')),

            pipeline(
                capture(r'\[repair #(?P<session_id>[^\]]*)\] streaming task succeed, returning response to (?P<node>[^ ]*)'),
                update(event_type='stream_succeeded'))),

    'StreamReplyVerbHandler':

        pipeline(
            capture(r'Successfully sent (?P<sstable_name>[^ ]*) to (?P<node>.*)'),
            update(event_type='sstable_sent')),

    'SSTableReader':

        pipeline(
            capture(r'Opening (?P<sstable_name>[^ ]*) \((?P<bytes>[0-9]*) bytes\)'),
            convert(int, 'bytes'),
            update(event_type='opening_sstable')),

    'StatusLogger':

        first(

            pipeline(
                capture(r'Pool Name *Active *Pending *Completed *Blocked *All Time Blocked'),
                update(event_type='pool_header')),

            pipeline(
                capture(r'(?P<pool_name>[A-Za-z_]+) +(?P<active>[0-9]+) +(?P<pending>[0-9]+) +(?P<completed>[0-9]+) +(?P<blocked>[0-9]+) +(?P<all_time_blocked>[0-9]+)'),
                convert(int, 'active', 'pending', 'completed', 'blocked', 'all_time_blocked'),
                update(event_type='pool_info')),

            pipeline(
                capture(r'Cache Type *Size *Capacity *KeysToSave *Provider'),
                update(event_type='cache_header')),

            pipeline(
                capture(r'(?P<cache_type>[A-Za-z]*Cache(?! Type)) *(?P<size>[0-9]*) *(?P<capacity>[0-9]*) *(?P<keys_to_save>[^ ]*) *(?P<provider>[A-Za-z_.$]*)'),
                convert(int, 'size', 'capacity'),
                update(event_type='cache_info')),

            pipeline(
                capture(r'ColumnFamily *Memtable ops,data'),
                update(event_type='memtable_header')),


            pipeline(
                capture(r'(?P<keyspace>[^.]*)\.(?P<table>[^ ]*) *(?P<ops>[0-9]*),(?P<data>[0-9]*)'),
                convert(int, 'ops', 'data'),
                update(event_type='memtable_info'))),

    'CommitLogReplayer':

        first(
                
                pipeline(
                    capture(r'Replaying (?P<commitlog_file>[^ ]*)( \(CL version (?P<commitlog_version>[0-9]*), messaging version (?P<messaging_version>[0-9]*)\))?'),
                    convert(int, 'commitlog_version', 'messaging_version'),
                    update(event_type='begin_replay_commitlog')),

                pipeline(
                    capture(r'Finished reading (?P<commitlog_file>.*)'),
                    update(event_type='end_replay_commitlog'))),

    'SecondaryIndexManager':

        pipeline(
            capture(r'Creating new index : ColumnDefinition\{(?P<column_definition>[^}]*)\}'),
            convert(split(', '), 'column_definition'),
            update(event_type='creating_secondary_index')),

    'SolrCoreResourceManager':

        first(

            pipeline(
                capture(r"Wrote resource '(?P<resource>[^']*)' for core '(?P<keyspace>[^.]*)\.(?P<table>[^']*)'"),
                update(event_type='solr_write_resource')),

            pipeline(
                capture(r'Trying to load resource (?P<resource>[^ ]*) for core (?P<keyspace>[^.]*).(?P<table>[^ ]*) by querying from local node with (?P<consistency_level>.*)'),
                update(event_type='solr_load_resource_attempt')),

            pipeline(
                capture(r'Successfully loaded resource (?P<resource>[^ ]*) for core (?P<keyspace>[^.]*).(?P<table>[^ ]*)'),
                update(event_type='solr_load_resource_success')),

            pipeline(
                capture(r'No resource (?P<resource>[^ ]*) found for core (?P<keyspace>[^.]*).(?P<table>[^ ]*) on any live node\.'),
                update(event_type='solr_load_resource_failure')),

            pipeline(
                capture(r'Creating core: (?P<keyspace>[^.]*).(?P<table>.*)'),
                update(event_type='solr_create_core'))),

    'AbstractSolrSecondaryIndex':

        first(

            pipeline(
                capture(r'Configuring index commit log for (?P<keyspace>[^.]*).(?P<table>.*)'),
                update(event_type='solr_configure_index_commitlog')),

            pipeline(
                capture(r'Configuring index metrics for (?P<keyspace>[^.]*).(?P<table>.*)'),
                update(event_type='solr_configure_index_metrics')),

            pipeline(
                capture(r'Ensuring existence of index directory (?P<index_directory>.*)'),
                update(event_type='solr_ensure_index_directory')),

            pipeline(
                capture(r'Executing hard commit on index (?P<keyspace>[^.]*).(?P<table>.*)'),
                update(event_type='solr_execute_hard_commit')),

            pipeline(
                capture(r'Loading core on keyspace (?P<keyspace>[^ ]*) and column family (?P<table>.*)'),
                update(event_type='solr_load_core')),

            pipeline(
                capture(r'No commit log entries for core (?P<keyspace>[^.]*).(?P<table>.*)'),
                update(event_type='solr_no_commitlog_entries')),

            pipeline(
                capture(r'Start index TTL scheduler for (?P<keyspace>[^.]*).(?P<table>.*)'),
                update(event_type='solr_start_index_ttl_scheduler')),

            pipeline(
                capture(r'Start index initializer for (?P<keyspace>[^.]*).(?P<table>.*)'),
                update(event_type='solr_start_index_initializer')),

            pipeline(
                capture(r'Start index reloader for (?P<keyspace>[^.]*).(?P<table>.*)'),
                update(event_type='solr_start_index_reloader')),

            pipeline(
                capture(r'Start indexing pool for (?P<keyspace>[^.]*).(?P<table>.*)'),
                update(event_type='solr_start_indexing_pool'))),

    'YamlConfigurationLoader':

        first(
            
            pipeline(
                capture(r'Loading settings from file:(?P<yaml_file>.*)'),
                update(event_type='loading_settings')),

            pipeline(
                capture(r'Node configuration:\[(?P<node_configuration>.*)\]'),
                convert(split('; '), 'node_configuration'),
                update(event_type='node_configuration'))),

    'Worker':

        pipeline(
            capture(r'Shutting down work pool worker!'),
            update(event_type='work_pool_shutdown')),

    'SolrDispatchFilter':

        first(

            pipeline(
                capture(r'SolrDispatchFilter.init\(\) done'),
                update(event_type='solr_dispatch_filter_init_done')),

            pipeline(
                capture(r'Error request params: (?P<params>.*)'),
                convert(split('&'), 'params'),
                update(event_type='solr_error_request_params')),

            pipeline(
                capture(r'\[admin\] webapp=(?P<webapp>[^ ]*) path=(?P<path>[^ ]*) params=\{(?P<params>[^}]*)\} status=(?P<status>[0-9]*) QTime=(?P<qtime>[0-9]*)'),
                convert(split('&'), 'params'),
                convert(int, 'status', 'qtime'),
                update(event_type='solr_admin')),

            pipeline(
                capture(r'user.dir=(?P<user_dir>.*)'),
                update(event_type='solr_user_dir'))),

    'ExternalLogger':

        pipeline(
            capture(r'(?P<source>[^:]*): (?P<message>.*)'),
            update(event_type='spark_external_logger')),

    'SliceQueryFilter':

        pipeline(
            capture(r'Read (?P<live_cells>[0-9]*) live and (?P<tombstoned_cells>[0-9]*) tombstoned cells in (?P<keyspace>[^.]*).(?P<table>[^ ]*) \(see tombstone_warn_threshold\). (?P<requested_columns>[0-9]*) columns was requested, slices=\[(?P<slice_start>[^-]*)-(?P<slice_end>[^\]]*)\], delInfo=\{(?P<deletion_info>[^}]*)\}'),
            convert(int, 'live_cells', 'tombstoned_cells', 'requested_columns'),
            convert(split(', '), 'deletion_info'),
            update(event_type='tombstone_warning')),
    
    'MeteredFlusher':

        pipeline(
            capture(r"Flushing high-traffic column family CFS\(Keyspace='(?P<keyspace>[^']*)', ColumnFamily='(?P<table>[^']*)'\) \(estimated (?P<estimated_bytes>[0-9]*) bytes\)"),
            convert(int, 'estimated_bytes'),
            update(event_type='metered_flush'))
})

def update_message(fields):
    subfields = capture_message(fields['source_file'][:-5], fields['message'])
    if subfields is not None:
        fields.update(subfields)

def tag_unknown(fields):
    if 'event_type' not in fields:
        fields['event_type'] = 'unknown'


capture_line = pipeline(
    capture(
        r' *(?P<level>[A-Z]*) *\[(?P<thread_name>[^\]]*?)[:_-]?(?P<thread_id>[0-9]*)\] (?P<date>.{10} .{12}) *(?P<source_file>[^:]*):(?P<source_line>[0-9]*) - (?P<message>.*)',
        r' *(?P<level>[A-Z]*) \[(?P<thread_name>[^\]]*?)[:_-]?(?P<thread_id>[0-9]*)\] (?P<date>.{10} .{12}) (?P<source_file>[^ ]*) \(line (?P<source_line>[0-9]*)\) (?P<message>.*)'),
    convert(date('%Y-%m-%d %H:%M:%S,%f'), 'date'),
    convert(int, 'source_line'),
    update_message,
    tag_unknown)

def parse_log(lines, **extras):
    fields = None
    for line in lines:
        next_fields = capture_line(line)
        if next_fields is not None:
            if fields is not None:
                fields.update(extras)
                if 'exception' in fields:
                    fields['exception'] = ''.join(fields['exception'])
                yield fields
            fields = next_fields
        else:
            if fields is not None:
                if 'exception' in fields:
                    fields['exception'].append(line)
                else:
                    fields['exception'] = [line]
