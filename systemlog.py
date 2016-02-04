from rules import *

def sstables(value):
    return [sstable[20:-2] for sstable in value.split(', ')]

def fix_solr_exception(fields):
    fields['exception'] = fields['message'] + fields['exception']
    fields['message'] = ''

capture_message = switch((

    case('CassandraDaemon'), 

        rule(
            capture(r'Logging initialized'), 
            update(event_product='cassandra', event_category='startup', event_type='node_restart')),

        rule(
            capture(r'JVM vendor/version: (?P<jvm>.*)'), 
            update(event_product='cassandra', event_category='startup', event_type='jvm_vendor')),

        rule(
            capture(r'Heap size: (?P<heap_used>[0-9]*)/(?P<total_heap>[0-9]*)'), 
            convert(int, 'heap_used', 'total_heap'), 
            update(event_product='cassandra', event_category='startup', event_type='heap_size')),

        rule(
            capture(r'Classpath: (?P<classpath>.*)'),
            convert(split(':'), 'classpath'),
            update(event_product='cassandra', event_category='startup', event_type='classpath')),

        rule(
            capture(r'JMX is not enabled to receive remote connections. Please see cassandra-env.sh for more info.'),
            update(event_product='cassandra', event_category='startup', event_type='jmx_remote_disabled')),

        rule(
            capture(r'No gossip backlog; proceeding'),
            update(event_product='cassandra', event_category='startup', event_type='gossip_backlog_done')),

        rule(
            capture(r'Waiting for gossip to settle before accepting client requests...'),
            update(event_product='cassandra', event_category='startup', event_type='gossip_wait')),

        rule(
            capture(r'completed pre-loading \((?P<keys_loaded>[0-9]*) keys\) key cache.'),
            update(event_product='cassandra', event_category='startup', event_type='preload_keycache')),

        rule(
            capture(r'Waiting for gossip to settle before accepting client requests...'),
            update(event_product='cassandra', event_category='startup', event_type='gossip_settling')),

        rule(
            capture(r'Cassandra shutting down...'),
            update(event_product='cassandra', event_category='shutdown', event_type='begin_shutdown')),

        rule(
            capture(r'Hostname: (?P<hostname>.*)'),
            update(event_product='cassandra', event_category='startup', event_type='hostname')),

        rule(
            capture(r'(?P<memory_type>.*) memory: init = (?P<memory_init>[0-9]*)\([0-9]*K\) used = (?P<memory_used>[0-9]*)\([0-9]*K\) committed = (?P<memory_committed>[0-9]*)\([0-9]*K\) max = (?P<memory_max>[0-9-]*)\([0-9-]*K\)'),
            convert(int, 'memory_init', 'memory_used', 'memory_committed', 'memory_max'),
            update(event_product='cassandra', event_category='startup', event_type='memory_size')),

        rule(
            capture(r'Exception in thread Thread\[(?P<exception_thread>[^\]]*)\]'),
            update(event_product='cassandra', event_category='error', event_type='exception')),

    case('DseConfig', 'DseSearchConfig'),

        rule(
            capture(r'Load of settings is done.'),
            update(event_product='cassandra', event_category='startup', event_type='dse_settings_done')),

        rule(
            capture(r'(?P<feature>.*) (is|are) enabled'),
            update(event_product='cassandra', event_category='startup', event_type='dse_feature', enabled=True)),

        rule(
            capture(r'(?P<feature>.*) (is|are) not enabled'),
            update(event_product='cassandra', event_category='startup', event_type='dse_feature', enabled=False)),

    case('DseModule'),

        rule(
            capture(r'Using regular cql queries'),
            update(event_product='dse', event_category='startup', event_type='solr_cql_queries', enabled=False)),

        rule(
            capture(r'Using Solr-enabled cql queries'),
            update(event_product='dse', event_category='startup', event_type='solr_cql_queries', enabled=True)),

        rule(
            capture(r'CFS operations enabled'),
            update(event_product='dse', event_category='startup', event_type='cfs_operations', enabled=True)),

        rule(
            capture(r'CFS operations disabled. Install the dse-analytics module if needed.'),
            update(event_product='dse', event_category='startup', event_type='cfs_operations', enabled=False)),

        rule(
            capture(r'Loading DSE module'),
            update(event_product='dse', event_category='startup', event_type='load_dse_module')),

    case('DseDaemon'), 

        rule(
            capture(r'(?P<component>[A-Za-z ]*) versions?: (?P<version>.*)'),
            update(event_product='dse', event_category='startup', event_type='component_version')),

        rule(
            capture(r'Waiting for other nodes to become alive...'),
            update(event_product='dse', event_category='startup', event_type='wait_other_nodes')),

        rule(
            capture(r'Wait for nodes completed'),
            update(event_product='dse', event_category='startup', event_type='wait_other_nodes_complete')),

        rule(
            capture(r'DSE shutting down...'),
            update(event_product='dse', event_category='shutdown', event_type='dse_shutdown')),

        rule(
            capture(r'The following nodes seems to be down: \[(?P<endpoints>[^\]]*)\]. Some Cassandra operations may fail with UnavailableException.'),
            update(event_product='dse', event_category='gossip', event_type='down_nodes_warning')),

    case('CqlSlowLogWriter'),
        rule(
            capture(r'Recording statements with duration of (?P<duration>[0-9]+) in slow log'),
            convert(int, 'duration'),
            update(event_product='dse', event_category='cql', event_type='slow_query')),

    case('GCInspector'), 

        rule(
            capture(r'Heap is (?P<percent_full>[0-9.]*) full.*'),
            convert(percent, 'percent_full'),
            update(event_product='cassandra', event_category='garbage_collection', event_type='heap_full')),

        rule(
            capture(r'GC for (?P<gc_type>[A-Za-z]*): (?P<duration>[0-9]*) ms for (?P<collections>[0-9]*) collections, (?P<used>[0-9]*) used; max is (?P<max>[0-9]*)'),
            convert(int, 'duration', 'collections', 'used', 'max'),
            update(event_product='cassandra', event_category='garbage_collection', event_type='pause')),

        rule(
            capture(r'(?P<gc_type>[A-Za-z]*) GC in (?P<duration>[0-9]*)ms. (( CMS)? Old Gen: (?P<oldgen_before>[0-9]*) -> (?P<oldgen_after>[0-9]*);)?( Code Cache: (?P<codecache_before>[0-9]*) -> (?P<codecache_after>[0-9]*);)?( Compressed Class Space: (?P<compressed_class_before>[0-9]*) -> (?P<compressed_class_after>[0-9]*);)?( CMS Perm Gen: (?P<permgen_before>[0-9]*) -> (?P<permgen_after>[0-9]*);)?( Metaspace: (?P<metaspace_before>[0-9]*) -> (?P<metaspace_after>[0-9]*);)?( Par Eden Space: (?P<eden_before>[0-9]*) -> (?P<eden_after>[0-9]*);)?( Par Survivor Space: (?P<survivor_before>[0-9]*) -> (?P<survivor_after>[0-9]*))?'),
            convert(int, 'duration', 'oldgen_before', 'oldgen_after', 'permgen_before', 'permgen_after', 'codecache_before', 'codecache_after', 'compressed_class_before', 'compressed_class_after', 'metaspace_before', 'metaspace_after', 'eden_before', 'eden_after', 'survivor_before', 'survivor_after'),
            update(event_product='cassandra', event_category='garbage_collection', event_type='pause')),

        rule(
            capture(r'(?P<gc_type>.+) Generation GC in (?P<duration>[0-9]+)ms.  (Compressed Class Space: (?P<compressed_class_before>[0-9]+) -> (?P<compressed_class_after>[0-9]+);)?.((.+) Eden Space: (?P<eden_before>[0-9]+) -> (?P<eden_after>[0-9]+);)?.((.+) Old Gen: (?P<oldgen_before>[0-9]+) -> (?P<oldgen_after>[0-9]+);)?.((.+) Survivor Space: (?P<survivor_before>[0-9]+) -> (?P<survivor_after>[0-9]+);)?.(Metaspace: (?P<metaspace_before>[0-9]+) -> (?P<metaspace_after>[0-9]+))?'),
            convert(int, 'duration', 'oldgen_before', 'oldgen_after', 'permgen_before', 'permgen_after', 'compressed_class_before', 'compressed_class_after', 'metaspace_before', 'metaspace_after', 'eden_before', 'eden_after', 'survivor_before', 'survivor_after'),
            update(event_product='cassandra', event_category='garbage_collection', event_type='pause')),

    case('ColumnFamilyStore'),

        rule(
            capture(
                r'Enqueuing flush of Memtable-(?P<table>[^@]*)@(?P<hash_code>[0-9]*)\((?P<serialized_bytes>[0-9]*)/(?P<live_bytes>[0-9]*) serialized/live bytes, (?P<ops>[0-9]*) ops\)',
                r'Enqueuing flush of (?P<table>[^:]*): (?P<on_heap_bytes>[0-9]*) \((?P<on_heap_limit>[0-9]*)%\) on-heap, (?P<off_heap_bytes>[0-9]*) \((?P<off_heap_limit>[0-9]*)%\) off-heap'),
            convert(int, 'hash_code', 'serialized_bytes', 'live_bytes', 'ops', 'on_heap_bytes', 'off_heap_bytes', 'on_heap_limit', 'off_heap_limit'),
            update(event_product='cassandra', event_category='memtable', event_type='enqueue_flush')), 

        rule(
            capture(r'Initializing (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='cassandra', event_category='startup', event_type='table_init')),

        rule(
            capture(r'Unable to cancel in-progress compactions for (?P<table>[^.]*)\.  Perhaps there is an unusually large row in progress somewhere, or the system is simply overloaded\.'),
            update(event_product='cassandra', event_category='compaction', event_type='cancellation_failed')),

        rule(
            capture(r"Flushing largest CFS\(Keyspace='(?P<keyspace>[^']*)', ColumnFamily='(?P<table>[^']*)'\) to free up room. Used total: (?P<used_on_heap>\d+\.\d+)/(?P<used_off_heap>\d+\.\d+), live: (?P<live_on_heap>\d+\.\d+)/(?P<live_off_heap>\d+\.\d+), flushing: (?P<flushing_on_heap>\d+\.\d+)/(?P<flushing_off_heap>\d+\.\d+), this: (?P<this_on_heap>\d+\.\d+)/(?P<this_off_heap>\d+\.\d+)"),
            convert(float, 'used_on_heap', 'used_off_heap', 'live_on_heap', 'live_off_heap', 'flushing_on_heap', 'flushing_off_heap', 'this_on_heap', 'this_off_heap'),
            update(event_product='cassandra', event_category='memtable', event_type='flush_largest')),
        
        rule(
            capture(
                r"Flushing SecondaryIndex (?P<index_type>[^{]*)\{(?P<index_metadata>[^}]*)\}",
                r"Flushing SecondaryIndex (?P<index_class>[^@]*)@(?P<index_hash>.*)"),
            update(event_product='cassandra', event_category='secondary_index', event_type='flush')),

    case('Memtable', 'ColumnFamilyStore'),

        rule(
            capture(
                r'Writing Memtable-(?P<table>[^@]*)@(?P<hash_code>[0-9]*)\(((?P<serialized_bytes>[0-9]*)|(?P<serialized_kb>[0-9.]*)KiB|(?P<serialized_mb>[0-9.]*)MiB) serialized bytes, (?P<ops>[0-9]*) ops, (?P<on_heap_limit>[0-9]*)%/(?P<off_heap_limit>[0-9]*)% of on/off-heap limit\)',
                r'Writing Memtable-(?P<table>[^@]*)@(?P<hash_code>[0-9]*)\((?P<serialized_bytes>[0-9]*)/(?P<live_bytes>[0-9]*) serialized/live bytes, (?P<ops>[0-9]*) ops\)'),
            convert(int, 'hash_code', 'serialized_bytes', 'live_bytes', 'ops', 'on_heap_limit', 'off_heap_limit'),
            convert(float, 'serialized_kb'),
            update(event_product='cassandra', event_category='memtable', event_type='begin_flush')),

        rule(
            capture(
                r'Completed flushing (?P<filename>[^ ]*) \(((?P<file_size_mb>[0-9.]*)MiB|(?P<file_size_kb>[0-9.]*)KiB|(?P<file_size_bytes>[0-9]*) bytes)\) for commitlog position ReplayPosition\(segmentId=(?P<segment_id>[0-9]*), position=(?P<position>[0-9]*)\)',
                r'Completed flushing; nothing needed to be retained.  Commitlog position was ReplayPosition\(segmentId=(?P<segment_id>[0-9]*), position=(?P<position>[0-9]*)\)'),
            convert(int, 'file_size_bytes', 'segment_id', 'position'),
            convert(float, 'file_size_kb'),
            update(event_product='cassandra', event_category='memtable', event_type='end_flush')),

        rule(
            capture(r"CFS\(Keyspace='(?P<keyspace>[^']*)', ColumnFamily='(?P<table>[^']*)'\) liveRatio is (?P<live_ratio>[0-9.]*) \(just-counted was (?P<just_counted>[0-9.]*)\).  calculation took (?P<duration>[0-9]*)ms for (?P<cells>[0-9]*) (columns|cells)"),
            convert(float, 'live_ratio', 'just_counted'),
            convert(int, 'duration', 'cells'),
            update(event_product='cassandra', event_category='memtable', event_type='live_ratio_estimate')),

        rule(
            capture('setting live ratio to maximum of (?P<max_sane_ratio>[0-9.]*) instead of (?P<live_ratio_estimate>[0-9.]*)'),
            convert(float, 'max_sane_ratio', 'estimated_ratio'),
            update(event_product='cassandra', event_category='memtable', event_type='live_ratio_max')),

    case('SSTableDeletingTask'),

        rule(
            capture(r"Unable to delete (?P<sstable_file>[^ ]*) \(it will be removed on server restart; we'll also retry after GC\)"),
            update(event_product='cassandra', event_category='compaction', event_type='sstable_deletion_failed')),

    case('CompactionManager'),

        rule(
            capture(r"Will not compact (?P<sstable_name>[^:]*): it is not an active sstable"),
            update(event_product='cassandra', event_category='compaction', event_type='inactive_sstable')),

        rule(
            capture(r"Compaction interrupted: (?P<compaction_type>[^@]*)@(?P<compaction_id>[^(]*)\((?P<table>[^,]*), (?P<keyspace>[^,]*), (?P<bytes_complete>[0-9]*)/(?P<bytes_total>[0-9]*)\)bytes"),
            convert(int, 'bytes_complete', 'bytes_total'),
            update(event_product='cassandra', event_category='compaction', event_type='compaction_interrupted')),

        rule(
            capture(r"No files to compact for user defined compaction"),
            update(event_product='cassandra', event_category='compaction', event_type='no_files_to_compact')),

    case('CompactionTask'), 

        rule(
            capture(r'Compacting \[(?P<input_sstables>[^\]]*)\]'),
            convert(sstables, 'input_sstables'),
            update(event_product='cassandra', event_category='compaction', event_type='begin_compaction')), 

        rule(
            capture(
                r'Compacted (?P<sstable_count>[0-9]*) sstables to \[(?P<output_sstable>[^\]]*)\].  (?P<input_bytes>[0-9,]*) bytes to (?P<output_bytes>[0-9,]*) \(~(?P<percent_of_original>[0-9]*)% of original\) in (?P<duration>[0-9,]*)ms = (?P<rate>[0-9.]*)MB/s.  (?P<total_partitions>[0-9,]*) total (partitions|rows), (?P<unique_partitions>[0-9,]*) unique.  (Row|Partition) merge counts were \{(?P<partition_merge_counts>[^}]*)\}',
                r'Compacted (?P<sstable_count>[0-9]*) sstables to \[(?P<output_sstable>[^\]]*)\].  (?P<input_bytes>[0-9,]*) bytes to (?P<output_bytes>[0-9,]*) \(~(?P<percent_of_original>[0-9]*)% of original\) in (?P<duration>[0-9,]*)ms = (?P<rate>[0-9.]*)MB/s.  (?P<total_partitions>[0-9,]*) total (partitions|rows) merged to (?P<unique_partitions>[0-9,]*).  (Row|Partition) merge counts were \{(?P<partition_merge_counts>[^}]*)\}'),
            convert(int_with_commas, 'sstable_count', 'input_bytes', 'output_bytes', 'percent_of_original', 'duration', 'total_partitions', 'unique_partitions'),
            convert(float, 'rate'),
            convert(split(', '), 'partition_merge_counts'),
            update(event_product='cassandra', event_category='compaction', event_type='end_compaction')),

    case('CompactionController'), 

        rule(
            capture(r'Compacting large (partition|row) (?P<keyspace>[^/]*)/(?P<table>[^:]*):(?P<partition_key>.*) \((?P<partition_size>[0-9]*) bytes\) incrementally'),
            convert(int, 'partition_size'),
            update(event_product='cassandra', event_category='compaction', event_type='incremental')),

    case('SSTableWriter'),
        
        rule(
            capture(r'Compacting large partition (?P<keyspace>.+)/(?P<table>.+):(?P<partition_key>.+) \((?P<partition_size>\d+) bytes\)'),
            convert(int, 'partition_size'),
            update(event_product='cassandra', event_category='compaction', event_type='incremental')),


    case('Differencer', 'AntiEntropyService'),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] Endpoints (?P<node1>[^ ]*) and (?P<node2>[^ ]*) are consistent for (?P<table>.*)'),
            update(event_product='cassandra', event_category='repair', event_type='endpoints_consistent')),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] Endpoints (?P<node1>[^ ]*) and (?P<node2>[^ ]*) have (?P<ranges>[0-9]*) range\(s\) out of sync for (?P<table>.*)'),
            convert(int, 'ranges'),
            update(event_product='cassandra', event_category='repair', event_type='endpoints_inconsistent')),

    case('RepairSession', 'AntiEntropyService'),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] Received merkle tree for (?P<table>[^ ]*) from (?P<endpoint>.*)'),
            update(event_product='cassandra', event_category='repair', event_type='merkle_received')),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] (?P<table>[^ ]*) is fully synced'),
            update(event_product='cassandra', event_category='repair', event_type='table_synced')),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] session completed successfully'),
            update(event_product='cassandra', event_category='repair', event_type='session_success')),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] new session: will sync (?P<nodes>.*?) on range \((?P<range_begin>[^,]*),(?P<range_end>[^\]]*)\] for (?P<keyspace>[^.]*)\.\[(?P<tables>[^\]]*)\]'),
            convert(split(', '), 'nodes', 'tables'),
            update(event_product='cassandra', event_category='repair', event_type='new_session')),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] Cannot proceed on repair because a neighbor \((?P<endpoint>[^)]*)\) is dead: session failed'),
            update(event_product='cassandra', event_category='repair', event_type='session_cannot_proceed')), 

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] session completed with the following error'),
            update(event_product='cassandra', event_category='repair', event_type='session_error')),

    case('RepairJob', 'AntiEntropyService'),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] requesting merkle trees for (?P<table>[^ ]*) \(to \[(?P<nodes>[^\]]*)\]\)'),
            convert(split(', '), 'nodes'),
            update(event_product='cassandra', event_category='repair', event_type='merkle_requested')),

        rule(
            capture(r'Error occurred during snapshot phase'),
            update(event_product='cassandra', event_category='repair', event_type='error_during_snapshot')),

    case('StreamInSession'),  
        
        rule(
            capture(r'Finished streaming session (?P<session_id>[^ ]*) from (?P<endpoint>.*)'),
            update(event_product='cassandra', event_category='stream', event_type='stream_in_end')),

    case('StreamOut'),

        rule(
            capture(r'Stream context metadata \[(?P<metadata>[^\]]*)\], (?P<sstable_count>[0-9]*) sstables.'),
            convert(split(', '), 'metadata'),
            convert(int, 'sstable_count'),
            update(event_product='cassandra', event_category='stream', event_type='context_metadata')),

        rule(
            capture(r'Beginning transfer to (?P<endpoint>.*)'),
            update(event_product='cassandra', event_category='stream', event_type='transfer_begin')),

        rule(
            capture(r"Flushing memtables for \[CFS\(Keyspace='(?P<keyspace>[^']*)', ColumnFamily='(?P<table>[^']*)'\)\].*"),
            update(event_product='cassandra', event_category='stream', event_type='flush_memtables')),

    case('StreamOutSession'),

        rule(
            capture(r'Streaming to (?P<endpoint>.*)'),
            update(event_product='cassandra', event_category='stream', event_type='stream_out_begin')),

    case('FileStreamTask'),

        rule(
            capture(r'Finished streaming session to (?P<endpoint>.*)'),
            update(event_product='cassandra', event_category='stream', event_type='stream_out_end')),

    case('StreamResultFuture'),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Session with (?P<endpoint>[^ ]*) is complete'),
            update(event_product='cassandra', event_category='stream', event_type='session_complete')),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Prepare completed. Receiving (?P<receiving_files>[0-9]*) files\((?P<receiving_bytes>[0-9]*) bytes\), sending (?P<sending_files>[0-9]*) files\((?P<sending_bytes>[0-9]*) bytes\)'),
            convert(int, 'receiving_files', 'receiving_bytes', 'sending_files', 'sending_bytes'),
            update(event_product='cassandra', event_category='stream', event_type='prepare_complete')),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Executing streaming plan for (?P<action>.*)'),
            update(event_product='cassandra', event_category='stream', event_type='execute_plan')),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] All sessions completed'),
            update(event_product='cassandra', event_category='stream', event_type='all_sessions_complete')),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Received streaming plan for (?P<action>.*)'),
            update(event_product='cassandra', event_category='stream', event_type='plan_received')),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Stream failed'),
            update(event_product='cassandra', event_category='stream', event_type='failure')),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Creating new streaming plan for Repair'),
            update(event_product='cassandra', event_category='stream', event_type='create_plan')),

    case('StreamCoordinator', 'StreamResultFuture'),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Beginning stream session with (?P<endpoint>.*)'),
            update(event_product='cassandra', event_category='stream', event_type='begin_session')),

    case('StreamingRepairTask'),

        rule(
            capture(r'\[streaming task #(?P<session_id>[^\]]*)\] Performing streaming repair of (?P<ranges>[0-9]*) ranges with (?P<endpoint>[^ ]*)'),
            convert(int, 'ranges'),
            update(event_product='cassandra', event_category='stream', event_type='begin_task')),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] streaming task succeed(ed)?, returning response to (?P<endpoint>[^ ]*)'),
            update(event_product='cassandra', event_category='stream', event_type='task_succeeded')),

        rule(
            capture(r'\[(repair|streaming task) #(?P<session_id>[^\]]*)\] Forwarding streaming repair of (?P<ranges>[0-9]*) ranges to (?P<forwarded_endpoint>[^ ]*) \(to be streamed with (?P<target_endpoint>[^)]*)\)'),
            convert(int, 'ranges'),
            update(event_product='cassandra', event_category='stream', event_type='forwarding')),

        rule(
            capture(r'\[streaming task #(?P<session_id>[^\]]*)\] Received task from (?P<source_endpoint>[^ ]*) to stream (?P<ranges>[0-9]*) ranges to (?P<target_endpoint>.*)'),
            convert(int, 'ranges'),
            update(event_product='cassandra', event_category='stream', event_type='received_task')),

        rule(
            capture(r'\[streaming task #(?P<session_id>[^\]]*)\] task succeeded'),
            update(event_product='cassandra', event_category='stream', event_type='task_succeded')),

        rule(
            capture(r'\[streaming task #(?P<session_id>[^\]]*)\] task succ?eed(ed)?, forwarding response to (?P<endpoint>[^ ]*)'),
            update(event_product='cassandra', event_category='stream', event_type='forwarded_task_succeeded')),

    case('StreamSession'),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Streaming error occurred'),
            update(event_product='cassandra', event_category='stream', event_type='session_error')),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Starting streaming to (?P<endpoint>.*)'),
            update(event_product='cassandra', event_category='stream', event_type='begin_stream')),

    case('StreamReplyVerbHandler'),

        rule(
            capture(r'Successfully sent (?P<sstable_name>[^ ]*) to (?P<endpoint>.*)'),
            update(event_product='cassandra', event_category='stream', event_type='sstable_sent')),

    case('IncomingTcpConnection'),

        rule(
            capture(r'UnknownColumnFamilyException reading from socket; closing'),
            update(event_product='cassandra', event_category='messaging', event_type='unknown_table')),
            
    case('OutboundTcpConnection'),

        rule(
            capture(r'Handshaking version with (?P<endpoint>.*)'),
            update(event_product='cassandra', event_category='gossip', event_type='handshake_version')),

        rule(
            capture(r'Cannot handshake version with (?P<endpoint>.*)'),
            update(event_product='cassandra', event_category='gossip', event_type='handshake_failure')),

    case('Gossiper'),

        rule(
            capture(r'InetAddress (?P<endpoint>[^ ]*) is now UP'),
            update(event_product='cassandra', event_category='gossip', event_type='node_up')),

        rule(
            capture(r'InetAddress (?P<endpoint>[^ ]*) is now DOWN'),
            update(event_product='cassandra', event_category='gossip', event_type='node_down')),

        rule(
            capture(r'Node (?P<endpoint>[^ ]*) has restarted, now UP'),
            update(event_product='cassandra', event_category='gossip', event_type='node_restarted')),

        rule(
            capture(r'Node (?P<endpoint>[^ ]*) is now part of the cluster'),
            update(event_product='cassandra', event_category='gossip', event_type='node_joined')),

        rule(
            capture(r'FatClient (?P<endpoint>[^ ]*) has been silent for 30000ms, removing from gossip'),
            update(event_product='cassandra', event_category='gossip', event_type='remove_silent_client')),

        rule(
            capture(r'Sleeping for 30000ms to ensure (?P<endpoint>[^ ]*) does not change'),
            update(event_product='cassandra', event_category='gossip', event_type='sleeping')),

        rule(
            capture(r'Removing host: (?P<host_id>.*)'),
            update(event_product='cassandra', event_category='gossip', event_type='node_remove_host')),

        rule(
            capture(r'Completing removal of (?P<endpoint>[^ ]*)'),
            update(event_product='cassandra', event_category='gossip', event_type='node_remove_complete')),

        rule(
            capture(r'Announcing shutdown'),
            update(event_product='cassandra', event_category='gossip', event_type='announcing_shutdown')),

        rule(
            capture(r'Gossip stage has (?P<pending_tasks>[0-9]+) pending tasks; skipping status check \(no nodes will be marked down\)'),
            convert(int, 'pending_tasks'),
            update(event_product='cassandra', event_category='gossip', event_type='pending_tasks')),

    case('SSTableReader'),

        rule(
            capture(r'Opening (?P<sstable_name>[^ ]*) \((?P<bytes>[0-9]*) bytes\)'),
            convert(int, 'bytes'),
            update(event_product='cassandra', event_category='startup', event_type='sstable_open')),

    case('StatusLogger'),

        rule(
            capture(r'Pool Name +Active +Pending( +Completed)? +Blocked( +All Time Blocked)?'),
            update(event_product='cassandra', event_category='status', event_type='threadpool_header')),

        rule(
            capture(r'(?P<pool_name>[A-Za-z_]+) +((?P<active>[0-9]+)|n/a) +(?P<pending>[0-9]+)(/(?P<pending_responses>[0-9]+))?( +(?P<completed>[0-9]+) +(?P<blocked>[0-9]+) +(?P<all_time_blocked>[0-9]+))?'),
            convert(int, 'active', 'pending', 'pending_responses', 'completed', 'blocked', 'all_time_blocked'),
            update(event_product='cassandra', event_category='status', event_type='threadpool_status')),

        rule(
            capture(r'Cache Type +Size +Capacity +KeysToSave(Provider)?'),
            update(event_product='cassandra', event_category='status', event_type='cache_header')),

        rule(
            capture(r'(?P<cache_type>[A-Za-z]*Cache(?! Type)) *(?P<size>[0-9]*) *(?P<capacity>[0-9]*) *(?P<keys_to_save>[^ ]*) *(?P<provider>[A-Za-z_.$]*)'),
            convert(int, 'size', 'capacity'),
            update(event_product='cassandra', event_category='status', event_type='cache_status')),

        rule(
            capture(r'ColumnFamily +Memtable ops,data'),
            update(event_product='cassandra', event_category='status', event_type='memtable_header')),


        rule(
            capture(r'(?P<keyspace>[^.]*)\.(?P<table>[^ ]*) +(?P<ops>[0-9]*),(?P<data>[0-9]*)'),
            convert(int, 'ops', 'data'),
            update(event_product='cassandra', event_category='status', event_type='memtable_status')),

    case('CommitLogReplayer', 'CommitLog'),
                
        rule(
            capture(r'No commitlog files found; skipping replay'),
            update(event_product='cassandra', event_category='startup', event_type='commit_log_replay_skipped')),

        rule(
            capture(r'Replaying (?P<commitlog_file>.*)'),
            convert(split(', '), 'commitlog_file'),
            update(event_product='cassandra', event_category='startup', event_type='begin_commitlog_replay')),

        rule(
            capture(r'Replaying (?P<commitlog_file>[^ ]*) \(CL version (?P<commitlog_version>[0-9]*), messaging version (?P<messaging_version>[0-9]*)\)'),
            convert(int, 'commitlog_version', 'messaging_version'),
            update(event_product='cassandra', event_category='startup', event_type='begin_commitlog_replay')),

        rule(
            capture(r'Finished reading (?P<commitlog_file>.*)'),
            update(event_product='cassandra', event_category='startup', event_type='end_commitlog_replay')),

        rule(
            capture(r'Log replay complete, (?P<replayed_mutations>[0-9]*) replayed mutations'),
            convert(int, 'replayed_mutations'),
            update(event_product='cassandra', event_category='startup', event_type='end_commitlog_replay')),

    case('SecondaryIndex', 'SecondaryIndexManager'),

        rule(
            capture(r'Creating new index : ColumnDefinition\{(?P<column_definition>[^}]*)\}'),
            convert(split(', '), 'column_definition'),
            update(event_product='cassandra', event_category='secondary_index', event_type='create')),

        rule(
            capture(r"Submitting index build of \[?(?P<keyspace>[^.]*)\.(?P<table>[^\] ]*)\]? for data in (?P<sstables>.*)"),
            convert(sstables, 'sstables'),
            update(event_product='cassandra', event_category='secondary_index', event_type='submit_build')),

        rule(
            capture(r'Index build of \[?(?P<keyspace>[^.]*)\.(?P<table>[^\] ]*)\]? complete'),
            update(event_product='cassandra', event_category='secondary_index', event_type='build_complete')),

    case('WorkPool'),

        rule(
            capture(r'Throttling at (?P<work_requests>[0-9]*) work requests per second with target total queue size at (?P<target_queue_size>[0-9]*)'),
            convert(int, 'work_requests', 'target_queue_size'),
            update(event_product='solr', event_category='backpressure', event_type='throttling')),

        rule(
            capture(r'Back pressure is active for (work pool )?(?P<work_pool>[^ ]*) (work pool )?with total work queue size (?P<queue_size>[0-9]*) and average processing time (?P<processing_time>[0-9]*)'),
            convert(int, 'queue_size', 'processing_time'),
            update(event_product='solr', event_category='backpressure', event_type='active')),

        rule(
            capture(r'Back pressure disabled for work pool Index'),
            update(event_product='solr', event_category='backpressure', event_type='disabled')),

    case('ShardRouter'),

        rule(
            capture(r'Updating shards state due to endpoint (?P<endpoint>[^ ]*) changing state (?P<state>.*)'),
            update(event_product='solr', event_category='shard_routing', event_type='state_change')),

        rule(
            capture(r'Found routing endpoint: (?P<endpoint>[^ ]*)'),
            update(event_product='solr', event_category='shard_routing', event_type='found_endpoint')),

        rule(
            capture(r'Added live routing endpoint (?P<endpoint>[^ ]*) for range \((?P<range_begin>[^,]*),(?P<range_end>[^\]]*)\]'),
            update(event_product='solr', event_category='shard_routing', event_type='added_endpoint')),

    case('QueryProcessor'),

        rule(
            capture(r'Column definitions for (?P<keyspace>[^.]*)\.(?P<table>[^ ]*) changed, invalidating related prepared statements'),
            update(event_product='solr', event_category='schema', event_type='column_definition_changed')),

        rule(
            capture(r'Keyspace (?P<keyspace>[^ ]*) was dropped, invalidating related prepared statements'),
            update(event_product='solr', event_category='schema', event_type='keyspace_dropped')),

        rule(
            capture(r'Table (?P<keyspace>[^.]*)\.(?P<table>[^ ]*) was dropped, invalidating related prepared statements'),
            update(event_product='solr', event_category='schema', event_type='table_dropped')),

    case('IndexSchema'),

        rule(
            capture(r'\[null\] Schema name=(?P<schema_name>.*)'),
            update(event_product='solr', event_category='schema', event_type='schema_name')),

        rule(
            capture(r'Reading Solr Schema from (?P<schema_file>.*)'),
            update(event_product='solr', event_category='schema', event_type='reading_schema_file')),

        rule(
            capture(r'unique key field: (?P<unique_key_field>.*)'),
            update(event_product='solr', event_category='schema', event_type='unique_key_field')),

        rule(
            capture(r'default search field in schema is (?P<default_field>.*)'),
            update(event_product='solr', event_category='schema', event_type='default_search_field')),

    case('Cql3CassandraSolrSchemaUpdater'),

        rule(
            capture(r'No Cassandra column found for field: (?P<field_name>.*)'),
            update(event_product='solr', event_category='schema', event_type='missing_cassandra_column')),

    case('XMLLoader', 'XSLTResponseWriter'),

        rule(
            capture('xsltCacheLifetimeSeconds=(?P<lifetime_seconds>[0-9]*)'),
            update(event_product='solr', event_category='xslt', event_type='xslt_cache_lifetime')),

    case('SolrResourceLoader'),

        rule(
            capture(r'using system property solr.solr.home: (?P<solr_home>.*)'),
            update(event_product='solr', event_category='resource', event_type='home_system_property')),

        rule(
            capture(r'No /solr/home in JNDI'),
            update(event_product='solr', event_category='resource', event_type='home_jndi_missing')),

        rule(
            capture(r"new SolrResourceLoader for (?P<loader_type>[^:]*): '(?P<solr_home>[^']*)'"),
            update(event_product='solr', event_category='resource', event_type='resource_loader')),

    case('SolrCoreResourceManager'),

        rule(
            capture(r"Delete resources for core '(?P<keyspace>[^.]*)\.(?P<table>[^']*)' from 'solr_admin.solr_resources'"),
            update(event_product='solr', event_category='resource', event_type='delete_resource')),

        rule(
            capture(r"Wrote resource '(?P<resource>[^']*)' for core '(?P<keyspace>[^.]*)\.(?P<table>[^']*)'"),
            update(event_product='solr', event_category='resource', event_type='write_resource')),

        rule(
            capture(r'Trying to load resource (?P<resource>[^ ]*) for core (?P<keyspace>[^.]*).(?P<table>[^ ]*) by looking for legacy resources...'),
            update(event_product='solr', event_category='resource', event_type='load_resource_legacy')),

        rule(
            capture(r'Trying to load resources? ((?P<resource>[^ ]*) )?for core (?P<keyspace>[^.]*).(?P<table>[^ ]*) by querying from local node with CL (?P<consistency_level>.*)'),
            update(event_product='solr', event_category='resource', event_type='load_resource_query')),

        rule(
            capture(r'Successfully loaded (?P<count>[0-9]*)? ?resources?( (?P<resource>[^ ]*))? for core (?P<keyspace>[^.]*).(?P<table>[^ ]*)( by querying from local node with CL (?P<consistency_level>.*))?'),
            update(event_product='solr', event_category='resource', event_type='load_resource_success')),

        rule(
            capture(r'No resource (?P<resource>[^ ]*) found for core (?P<keyspace>[^.]*).(?P<table>[^ ]*).*'),
            update(event_product='solr', event_category='resource', event_type='load_resource_failure')),

        rule(
            capture(r"Unsupported schema version \((?P<schema_version>[^])*)\). Please use version '(?P<required_version>[^']*)' or greater."),
            update(event_product='solr', event_category='resource', event_type='unsupported_schema_version')),

        rule(
            capture(r'Creating core: (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='resource', event_type='create_core')),

        rule(
            capture(r'Reloading core: (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='resource', event_type='reload_core')),

        rule(
            capture(r'Ignoring request trying to reload not existent core: (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='resource', event_type='ignore_reload_nonexistent_core')),

        rule(
            capture(r'Ignoring request trying to create already existent core: (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='resource', event_type='ignore_create_existing_core')),

        rule(
            capture(r'Composite keys are not supported on Thrift-compatible tables: (?P<composite_key>.*)'),
            update(event_product='solr', event_category='resource', event_type='thrift_composite_key_error')),

#Plugin init failure for [schema.xml] fieldType "text_classes": Plugin init failure for [schema.xml] analyzer/tokenizer: Error instantiating class: 'org.apache.lucene.analysis.pattern.PatternTokenizerFactory'. Schema file is solr/conf/schema.xml

    case('AbstractSolrSecondaryIndex'),

        rule(
            capture(r'Configuring index commit log for (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='configure_index_commitlog')),

        rule(
            capture(r'Configuring index metrics for (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='configure_index_metrics')),

        rule(
            capture(r'Ensuring existence of index directory (?P<index_directory>.*)'),
            update(event_product='solr', event_category='index', event_type='ensure_index_directory')),

        rule(
            capture(r'Executing hard commit on index (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='execute_hard_commit')),

        rule(
            capture(r'Loading core on keyspace (?P<keyspace>[^ ]*) and column family (?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='load_core')),

        rule(
            capture(r'No commit log entries for core (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='no_commitlog_entries')),

        rule(
            capture(r'Start index TTL scheduler for (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='start_index_ttl_scheduler')),

        rule(
            capture(r'Start index initializer for (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='start_index_initializer')),

        rule(
            capture(r'Start index reloader for (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='start_index_reloader')),

        rule(
            capture(r'Start indexing pool for (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='start_indexing_pool')),

        rule(
            capture(r'Waiting for DSE to startup \(if not already done\) and activate index on keyspace (?P<keyspace>[^ ]*) and column family (?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='startup_wait')),

        rule(
            capture(r'Reindexing on keyspace (?P<keyspace>[^ ]*) and column family (?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='reindexing')),

        rule(
            capture(r'Invalidating index (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='invalidating_index')),

        rule(
            capture(r'Deleting all documents from index (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='delete_all_documents')),

        rule(
            capture(r'Truncating index (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='truncate_index')),

        rule(
            capture(r'Flushing commit log for core (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='flushing_commit_log')),

        rule(
            capture(r'Enabling index updates for core (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='index', event_type='enable_index_updates')),

        rule(
            capture(r'Solr validation error for row\(\[(?P<row>[^\]]*)\]\), field\((?P<field>[^)]*)\), type\((?P<type>[^)]*)\), error: (?P<error>.*)'),
            update(event_product='solr', event_category='index', event_type='validation_error')),

        rule(
            capture(r'Increasing soft commit max time to (?P<max_time>[0-9]+)'),
            convert(int, 'max_time'),
            update(event_product='solr', event_category='index', event_type='increasing_soft_commit')),

        rule(
            capture(r'Restoring soft commit max time back to (?P<max_time>[0-9]+)'),
            convert(int, 'max_time'),
            update(event_product='solr', event_category='index', event_type='restoring_soft_commit')),

    case('DSESearchProperties'),

        rule(
            capture(r'Using default DSE search properties for Solr core (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='properties', event_type='default_search_properties')),

        rule(
            capture(r'Refreshed DSE search properties for: (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='properties', event_type='refresh_search_properties')),

    case('CassandraShowFileRequestHandler'),

        rule(
            capture(r"Couldn't find resource: (?P<resource>[^,]*), ignoring..."),
            update(event_product='solr', event_category='resource', event_type='resource_not_found')),

    case('CoreContainer'),

        rule(
            capture('Shutting down CoreContainer instance=(?P<instance>[0-9]*)'),
            convert(int, 'instance'),
            update(event_product='solr', event_category='core', event_type='corecontainer_shutdown')),

        rule(
            capture('New CoreContainer (?P<instance>[0-9]*)'),
            convert(int, 'instance'),
            update(event_product='solr', event_category='core', event_type='corecontainer_new')),

        rule(
            capture('registering core: (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='core', event_type='registering_core')),

        rule(
            capture('replacing core: (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_product='solr', event_category='core', event_type='replacing_core')),

    case('CassandraCoreContainer'),

        rule(
            capture(r'WARNING: stored copy fields are deprecated, found stored copy field destination: (?P<field>[^{]*){type=(?P<type>[^,]*),properties=(?P<properties>[^}]*)}. All stored copy fields will be considered non-stored'),
            update(event_product='solr', event_category='core', event_type='stored_copy_field_warning')),

    case('SolrDispatchFilter'),

        rule(
            capture(r'SolrDispatchFilter.init\(\)'),
            update(event_product='solr', event_category='dispatch', event_type='begin_init_dispatch_filter')),

        rule(
            capture(r'SolrDispatchFilter.init\(\) done'),
            update(event_product='solr', event_category='dispatch', event_type='end_init_dispatch_filter')),

        rule(
            capture(r'Error request exception: (?P<error>.*)'),
            update(event_product='solr', event_category='dispatch', event_type='request_exception')),

        rule(
            capture(r'Error request params: (?P<params>.*)'),
            convert(split('&'), 'params'),
            update(event_product='solr', event_category='dispatch', event_type='request_params_error')),

        rule(
            capture(r'\[admin\] webapp=(?P<webapp>[^ ]*) path=(?P<path>[^ ]*) params=\{(?P<params>[^}]*)\} status=(?P<status>[0-9]*) QTime=(?P<qtime>[0-9]*)'),
            convert(split('&'), 'params'),
            convert(int, 'status', 'qtime'),
            update(event_product='solr', event_category='dispatch', event_type='admin_access')),

        rule(
            capture(r'user.dir=(?P<user_dir>.*)'),
            update(event_product='solr', event_category='dispatch', event_type='user_dir')),

    case('JmxMonitoredMap'),

        rule(
            capture(r'JMX monitoring is enabled. Adding Solr mbeans to JMX Server: (?P<mbean_class>[^@]*)@(?P<mbean_hash>.*)'),
            update(event_product='solr', event_category='jmx', event_type='adding_mbeans')),

        rule(
            capture(r'Could not (?P<mbean_operation>[^ ]*) on info bean (?P<mbean_class>[^@]*)'),
            update(event_product='solr', event_category='jmx', event_type='mbean_error')),

    case('RequestHandlers'),

        rule(
            capture('Multiple requestHandler registered to the same name: (?P<path>[^ ]*) ignoring: (?P<ignored_class>.*)'),
            update(event_product='solr', event_category='dispatch', event_type='ignore_multiple_request_handlers')),

    case('SolrException'),
        
        rule(
            capture(r'.*'),
            update(event_product='solr', event_category='error', event_type='exception')),

    case('SolrConfig'),

        rule(
            capture(r'Using Lucene MatchVersion: (?P<matchversion>.*)'),
            update(event_product='solr', event_category='config', event_type='lucene_matchversion')),

        rule(
            capture(r'Loaded SolrConfig: (?P<solrconfig>.*)'),
            update(event_product='solr', event_category='config', event_type='load_solrconfig')),

    case('CachingDirectoryFactory'),

        rule(
            capture(r'return new directory for (?P<directory>.*)'),
            update(event_product='solr', event_category='directory_factory', event_type='return_new_directory')),

        rule(
            capture(r'Closing directory: (?P<directory>.*)'),
            update(event_product='solr', event_category='directory_factory', event_type='closing_directory')),

        rule(
            capture(r'Timeout waiting for all directory ref counts to be released - gave up waiting on CachedDir<<refCount=(?P<ref_count>[0-9]*);path=(?P<path>[^;]*);done=(?P<done>[^>]*)>>'),
            convert(int, 'ref_count'),
            update(event_product='solr', event_category='directory_factory', event_type='close_directory_pending')),

        rule(
            capture(r'looking to close (?P<directory>[^ ]*) \[CachedDir<<refCount=(?P<ref_count>[0-9]*);path=(?P<path>[^;]*);done=(?P<done>[^>]*)>>\]'),
            convert(int, 'ref_count'),
            update(event_product='solr', event_category='directory_factory', event_type='close_directory_pending')),

        rule(
            capture(r'Closing (?P<directory_factory>[^ ]*) - (?P<directory_count>[0-9]*) directories currently being tracked'),
            convert(int, 'ref_count'),
            update(event_product='solr', event_category='directory_factory', event_type='close_directory_factory')),

    case('YamlConfigurationLoader', 'DseConfigYamlLoader', 'DatabaseDescriptor'),
            
        rule(
            capture(r'Loading settings from file:(?P<yaml_file>.*)'),
            update(event_product='cassandra', event_category='startup', event_type='config_file')),

        rule(
            capture(r'Node configuration:\[(?P<settings>.*)\]'),
            convert(lambda x: dict([y.split('=', 1) for y in x.split('; ')]), 'settings'),
            update(event_product='cassandra', event_category='startup', event_type='yaml_settings')),

    case('DatabaseDescriptor'),

        rule(
            capture(r'commit_failure_policy is (?P<policy>.*)'),
            update(event_product='cassandra', event_category='startup', event_type='commit_failure_policy')),

        rule(
            capture(r'disk_failure_policy is (?P<policy>.*)'),
            update(event_product='cassandra', event_category='startup', event_type='disk_failure_policy')),

        rule(
            capture(r'Data files directories: \[(?P<data_file_directories>[^\]]*)\]'),
            convert(split(', '), 'data_file_directories'),
            update(event_product='cassandra', event_category='startup', event_type='data_file_directories')),

        rule(
            capture(r'Commit log directory: (?P<commit_log_directory>.*)'),
            update(event_product='cassandra', event_category='startup', event_type='commit_log_directory')),

        rule(
            capture(r"DiskAccessMode 'auto' determined to be (?P<disk_access_mode>[^,]*), indexAccessMode is (?P<index_access_mode>.*)"),
            update(event_product='cassandra', event_category='startup', event_type='disk_access_mode', auto_determined=True)),

        rule(
            capture(r"DiskAccessMode is (?P<disk_access_mode>[^,]*), indexAccessMode is (?P<index_access_mode>.*)"),
            update(event_product='cassandra', event_category='startup', event_type='disk_access_mode', auto_determined=False)),

        rule(
            capture(r'Not using multi-threaded compaction'),
            update(event_product='cassandra', event_category='startup', event_type='multi_threaded_compaction', enabled=False)),

        rule(
            capture(r'using multi-threaded compaction'),
            update(event_product='cassandra', event_category='startup', event_type='multi_threaded_compaction', enabled=True)),

        rule(
            capture(r'Global memtable (?P<memtable_location>off-heap|on-heap) ?threshold is enabled at (?P<memtable_threshold>[0-9]*)MB'),
            update(event_product='cassandra', event_category='startup', event_type='global_memtable_threshold')),

    case('Worker', 'IndexWorker'),

        rule(
            capture(r'Shutting down work pool worker!'),
            update(event_product='solr', event_category='shutdown', event_type='work_pool_shutdown')),

    case('LeaderManagerWatcher'),

        rule(
            capture(
                r'OH GOD',
                r"Couldn't run initialization block"),
            update(event_product='dse', event_category='jobtracker', event_type='leader_manager_exception')),

        rule(
            capture(r'(?P<leader_manager>[^:]*): Leader (?P<dc_army>[^ ]*) changed from (?P<old_leader>[^ ]*) to (?P<new_leader>[^ ]*) \[(?P<reason>[^\]]*)\] \[notified (?P<listeners_notified>[0-9]*) listeners\]'),
            convert(int, 'listeners_notified'),
            update(event_product='dse', event_category='jobtracker', event_type='leader_changed')),

        rule(
            capture(r'(?P<leader_manager>[^:]*): new listener for (?P<dc_army>[^ ]*) initialized to (?P<listener>.*)'),
            update(event_product='dse', event_category='jobtracker', event_type='new_listener')),

        rule(
            capture(r'Local query timed out.+'),
            update(event_product='dse', event_category='jobtracker', event_type='timeout')),


    case('ExternalLogger', 'ExternalLogger SparkWorker-0'),

        rule(
            capture(r'.*'),
            update(event_product='spark', event_category='misc', event_type='external_logger')),

#SparkMaster: Adding host 10.1.40.10 (Analytics)	
#SparkMaster: Ignoring remote host 10.1.0.20 (Cassandra)	
#SparkMaster: Found host with 0.0.0.0 as rpc_address, using listen_address (/10.1.40.20) to contact it instead. If this is incorrect you should avoid the use of 0.0.0.0 server side.
#SparkWorker: Killing process!
#SparkWorker: Asked to kill executor app-20150627020010-0342/5
#SparkWorker: Launch command: "..." "..." "..."
#SparkWorker: Runner thread for executor app-20150627043009-0345/5 interrupted
#SparkWorker: Executor app-20150625220827-0264/5 finished with state KILLED exitStatus 1	

    case('SparkWorkerRunner'),

        rule(
            capture(r'Started Spark Worker, connected to master (?P<master_host>[^:]*):(?P<master_port>[0-9]+)'),
            convert(int, 'master_port'),
            update(event_product='spark', event_category='worker', event_type='worker_started')),

        rule(
            capture(
                r'Spark Master not ready( yet)? at (?P<master_host>[^:]*):(?P<master_port>[0-9]+).*',
                r'Spark Master not ready( yet)? at \(no configured master\)'),
            convert(int, 'master_port'),
            update(event_product='spark', event_category='master', event_type='master_not_ready')),

    case('AbstractConnector'),
        rule(
            capture(r'Started SelectChannelConnector@(?P<ip>.+):(?P<port>.+)'),
            convert(int, 'port'),
            update(event_product='spark', event_category='master', event_type='listening')),

    case('AbstractSparkRunner'),

        rule(
            capture(r'Starting Spark process: (?P<process>.*)'),
            update(event_product='spark', event_category='process', event_type='process_starting')),

        rule(
            capture(r'Process (?P<process>[^ ]*) has just received (?P<signal>.*)'),
            update(event_product='spark', event_category='process', event_type='received_signal')),

        rule(
            capture(r'(?P<process>[^ ]*) threw exception in state (?P<state>[^:]*):'),
            update(event_product='spark', event_category='process', event_type='process_exception')),

    case('JobTrackerManager'),

        rule(
            capture(r'Failed to retrieve jobtracker locations at CL.(?P<consistency_level>[^ ]*) \((?P<error>[^)]*)\)'),
            update(event_product='dse', event_category='jobtracker', event_type='location_failure')),

    case('SliceQueryFilter'),

        rule(
            capture(r'Scanned over (?P<tombstoned_cells>[0-9]*) tombstones in (?P<keyspace>[^.]*).(?P<table>[^;]*); query aborted \(see tombstone_failure_threshold\)'),
            convert(int, 'live_cells', 'tombstoned_cells', 'requested_columns'),
            convert(split(', '), 'deletion_info'),
            update(event_product='cassandra', event_category='tombstone', event_type='warning_threshold_exceeded')),

        rule(
            capture(r'Read (?P<live_cells>[0-9]*) live and (?P<tombstoned_cells>[0-9]*) tombstoned? cells in (?P<keyspace>[^.]*).(?P<table>[^ ]*)( for key: (?P<key>[^ ]*))? \(see tombstone_warn_threshold\). (?P<requested_columns>[0-9]*) columns (was|were) requested, slices=\[(?P<slice_start>[^-]*)-(?P<slice_end>[^\]]*)\](, delInfo=\{(?P<deletion_info>[^}]*)\})?'),
            convert(int, 'live_cells', 'tombstoned_cells', 'requested_columns'),
            convert(split(', '), 'deletion_info'),
            update(event_product='cassandra', event_category='tombstone', event_type='error_threshold_exceeded')),

    case('BatchStatement'),

        rule(
            capture(r'Batch of prepared statements for \[(?P<keyspace>[^.]*).(?P<table>[^\]]*)\] is of size (?P<batch_size>[0-9]*), exceeding specified threshold of (?P<batch_warn_threshold>[0-9]*) by (?P<threshold_exceeded_by>[0-9]*).'),
            convert(int, 'batch_size', 'batch_warn_threshold', 'threshold_excess'),
            update(event_product='cassandra', event_category='batch', event_type='size_warning')),
    
    case('CustomTThreadPoolServer'),

        rule(
            capture(r'Error occurred during processing of message.'),
            update(event_product='cassandra', event_category='thrift', event_type='message_processing_error')),

    case('TNegotiatingServerTransport'),

        rule(
            capture(r'Using TFramedTransport with a max frame size of (\{\}|(?P<max_frame_size>[0-9]*)) bytes.'),
            convert(int, 'max_frame_size'),
            update(event_product='cassandra', event_category='thrift', event_type='max_frame_size')),

        rule(
            capture(r'Failed to open server transport.'),
            update(event_product='cassandra', event_category='thrift', event_type='transport_open_error')),

    case('Message'),

       rule(
            capture(r'Read an invalid frame size of (?P<frame_size>[0-9-]*). Are you using TFramedTransport on the client side\?'),
            convert(int, 'frame_size'),
            update(event_product='cassandra', event_category='thrift', event_type='invalid_frame_size')),

       rule(
            capture(r'Invalid frame size got \((?P<frame_size>[0-9-]*)\), maximum expected (?P<max_frame_size>[0-9-]*)'),
            convert(int, 'frame_size', 'max_frame_size'),
            update(event_product='cassandra', event_category='thrift', event_type='invalid_frame_size')),

       rule(
            capture(r'Got an IOException in internalRead!'),
            update(event_product='cassandra', event_category='thrift', event_type='receive_exception')),

       rule(
            capture(r'Got an IOException during write!'),
            update(event_product='cassandra', event_category='thrift', event_type='send_exception')),

       rule(
            capture(r'Unexpected exception during request; channel = \[id: (?P<channel_id>[^,]*), (?P<client_host>[^:]*):(?P<client_port>[0-9]*) (=>|:>) (?P<server_host>[^:]*):(?P<server_port>[0-9]*)\]'),
            convert(int, 'client_port', 'server_port'),
            update(event_product='cassandra', event_category='native_protocol', event_type='request_exception')),

    case('ThriftServer'),

       rule(
            capture(r'Listening for thrift clients...'),
            update(event_product='cassandra', event_category='thrift', event_type='start_listen')),

       rule(
            capture(r'Stop listening to thrift clients'),
            update(event_product='cassandra', event_category='thrift', event_type='stop_listen')),

       rule(
            capture(r'Binding thrift service to (?P<thrift_host>[^:]*):(?P<thrift_port>[0-9]*)'),
            convert(int, 'thrift_port'),
            update(event_product='cassandra', event_category='thrift', event_type='bind_address')),

    case('Server'),
    
       rule(
            capture(r'Starting listening for CQL clients on (?P<native_host>[^:]*):(?P<native_port>[0-9]*)'),
            convert(int, 'thrift_port'),
            update(event_product='cassandra', event_category='native_transport', event_type='start_listen')),

       rule(
            capture(r'Stop listening for CQL clients'),
            update(event_product='cassandra', event_category='native_transport', event_type='stop_listen')),

       rule(
            capture(r'Netty using (?P<event_loop_type>native Epoll|Java NIO) event loop'),
            update(event_product='cassandra', event_category='native_transport', event_type='netty_event_loop')),

       rule(
            capture(r'Using Netty Version: \[(?P<netty_version>[^\]]*)\]'),
            convert(split(', '), 'netty_version'),
            update(event_product='cassandra', event_category='native_transport', event_type='netty_verision')),

       rule(
            capture(r'jetty-(?P<jetty_version>.*)'),
            update(event_product='cassandra', event_category='native_transport', event_type='jetty_verision')),

    case('MeteredFlusher'),

        rule(
            capture(r"[Ff]lushing high-traffic column family CFS\(Keyspace='(?P<keyspace>[^']*)', ColumnFamily='(?P<table>[^']*)'\) \(estimated (?P<estimated_bytes>[0-9]*) bytes\)"),
            convert(int, 'estimated_bytes'),
            update(event_product='cassandra', event_category='memtable', event_type='metered_flush')),
        
    case('Validator', 'AntiEntropyService'),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] Sending completed merkle tree to (?P<endpoint>[^ ]*) for \(?(?P<keyspace>[^,]*)[/,](?P<table>[^)]*)\)?'),
            update(event_product='cassandra', event_category='repair', event_type='merkle_sent')), 

    case('HintedHandOffManager'),

        rule(
            capture(r'Finished hinted handoff of (?P<rows>[0-9]*) rows to endpoint (?P<endpoint>.*)'),
            convert(int, 'rows'),
            update(event_product='cassandra', event_category='hinted_handoff', event_type='end_handoff')),

        rule(
            capture(r'Started hinted handoff for host: (?P<host_id>[^ ]*) with IP: (?P<endpoint>.*)'),
            update(event_product='cassandra', event_category='hinted_handoff', event_type='begin_handoff')),

        rule(
            capture(r'Timed out replaying hints to (?P<endpoint>.*); aborting \((?P<hints_delivered>[0-9]*) delivered\)'),
            convert(int, 'hints_delivered'),
            update(event_product='cassandra', event_category='hinted_handoff', event_type='hint_timeout')),

        rule(
            capture(r'Could not truncate all hints.'),
            update(event_product='cassandra', event_category='hinted_handoff', event_type='truncate_hints_failure')),

        rule(
            capture(r'Truncating all stored hints.'),
            update(event_product='cassandra', event_category='hinted_handoff', event_type='truncate_hints')),

        rule(
            capture(r'Deleting any stored hints for (?P<endpoint>.*)'),
            update(event_product='cassandra', event_category='hinted_handoff', event_type='delete_hints_for_endpoint')),

    case('HintedHandoffMetrics'),

        rule(
            capture(r'(?P<endpoint>[^ ]*) has (?P<hints_dropped>[0-9]*) dropped hints, because node is down past configured hint window.'),
            convert(int, 'hints_dropped'),
            update(event_product='cassandra', event_category='hinted_handoff', event_type='hints_dropped')),

    case('PluginLocator'),

        rule(
            capture(r'Scanning jar:(?P<jar>[^ ]*) for DSE plugins'),
            update(event_product='cassandra', event_category='plugin', event_type='scanning_jar')),

    case('PluginManager'),

        rule(
            capture(r'Plugin activated: (?P<plugin_class>.*)'),
            update(event_product='dse', event_category='plugin', event_type='activated')),

        rule(
            capture(r'Registered plugin (?P<plugin_class>.*)'),
            update(event_product='dse', event_category='plugin', event_type='registered')),

        rule(
            capture(r'Deactivating plugin: (?P<plugin_class>.*)'),
            update(event_product='dse', event_category='plugin', event_type='deactivating')),

        rule(
            capture(r'Activating plugin: (?P<plugin_class>.*)'),
            update(event_product='dse', event_category='plugin', event_type='activating')),

        rule(
            capture(r'All plugins are stopped.'),
            update(event_product='dse', event_category='plugin', event_type='all_stopped')),

    case('AsyncSnapshotInfoBean'),

        rule(
            capture('(?P<metric>[^ ]*) plugin using (?P<async_writers>[0-9]*) async writers'),
            convert(int, 'async_writers'),
            update(event_product='dse', event_category='metric', event_type='async_writers')),

    case('SnapshotInfoBean'),

        rule(
            capture('(?P<metric>[^ ]) refresh rate set to (?P<new_refresh_rate>[0-9]*) \(was (?P<old_refresh_rate>[0-9]*)\)'),
            convert(int, 'new_refresh_rate', 'old_refresh_rate'),
            update(event_product='dse', event_category='metric', event_type='refresh_rate_set')),

    case('ExplicitTTLSnapshotInfoBean'),

        rule(
            capture('Setting TTL to (?P<ttl>[0-9]*)'),
            convert(int, 'ttl'),
            update(event_product='dse', event_category='metric', event_type='ttl_set')),

    case('AutoSavingCache'),

        rule(
            capture(r'Saved (?P<cache_type>[^ ]*) \((?P<cache_items>[0-9]*) items\) in (?P<save_duration>[0-9]*) ms'),
            convert(int, 'cache_items', 'save_duration'),
            update(event_product='cassandra', event_category='cache', event_type='save')),

        rule(
            capture(r'reading saved cache (?P<cache_file>.*)'),
            update(event_product='cassandra', event_category='cache', event_type='read')),

        rule(
            capture(r'Harmless error reading saved cache.*'),
            update(event_product='cassandra', event_category='cache', event_type='read_error')),

        rule(
            capture(r'Completed loading \((?P<load_duration>[0-9]*) ms; (?P<cache_items>[0-9]*) keys\) KeyCache cache'),
            convert(int, 'load_duration', 'cache_items'),
            update(event_product='cassandra', event_category='cache', event_type='loaded')),

    case('CacheService'),

        rule(
            capture(r'Scheduling (?P<cache_type>[^ ]*) cache save to every (?P<save_interval>[0-9]*) seconds \(going to save (?P<keys_to_save>[^ ]*) keys\).'),
            convert(int, 'save_interval'),
            update(event_product='cassandra', event_category='cache', event_type='schedule_save')),

        rule(
            capture(r'Initializing (?P<cache_type>[^ ]*) cache with capacity of (?P<cache_capacity_mb>[0-9]*) MBs\.?( and provider (?P<cache_provider>.*))?'),
            update(event_product='cassandra', event_category='cache', event_type='init')),

    case('MigrationTask'),

        rule(
            capture(r"Can't send migration request: node (?P<endpoint>[^ ]*) is down."),
            update(event_product='cassandra', event_category='migration', event_type='request_failure')),

    case('MigrationManager'),

        rule(
            capture(r"Drop Keyspace '(?P<keyspace>[^']*)'"),
            update(event_product='cassandra', event_category='migration', event_type='drop_keyspace')),

        rule(
            capture(r"Drop ColumnFamily '(?P<keyspace>.+)/(?P<table>.+)'"),
            update(event_product='cassandra', event_category='migration', event_type='drop_table')),

        rule(
            capture(r"Update ColumnFamily '(?P<keyspace>[^/]*)/(?P<table>[^']*)' From org.apache.cassandra.config.CFMetaData@(?P<old_hash>[^\]]*)\[(?P<old_metadata>.*)\] To org.apache.cassandra.config.CFMetaData@(?P<new_hash>[^\]]*)\[(?P<new_metadata>.*)\]"),
            update(event_product='cassandra', event_category='migration', event_type='update_table')),

        rule(
            capture(r"Create new ColumnFamily: org.apache.cassandra.config.CFMetaData@(?P<hash>[^\]]*)\[(?P<metadata>.*)\]"),
            update(event_product='cassandra', event_category='migration', event_type='create_table')),

        rule(
            capture(r"Create new Keyspace: KSMetaData\{(?P<metadata>.*)\}"),
            update(event_product='cassandra', event_category='migration', event_type='create_keyspace')),

    case('DefsTables'),

        rule(
            capture(r'Loading org.apache.cassandra.config.CFMetaData@(?P<hash>[^\]]*)\[(?P<metadata>.*)\]'),
            update(event_product='cassandra', event_category='migration', event_type='load_table_metadata')),

    case('StorageService'),

        rule(
            capture(r'adding secondary index (?P<keyspace>[^.]*)\.(?P<table>[^ ]*) to operation'),
            update(event_product='cassandra', event_category='secondary_index', event_type='added_to_operation')),

        rule(
            capture(r'Repair session (?P<session_id>[^\]]*) for range \((?P<range_begin>[^,]*),(?P<range_end>[^\]]*)\] finished'),
            convert(int, 'range_begin', 'range_end'),
            update(event_product='cassandra', event_category='repair', event_type='session_finished')),

        rule(
            capture(r'Repair session (?P<session_id>[^\]]*) for range \((?P<range_begin>[^,]*),(?P<range_end>[^\]]*)\] failed with error (?P<error>)'),
            convert(int, 'range_begin', 'range_end'),
            update(event_product='cassandra', event_category='repair', event_type='session_failure')),

        rule(
            capture(r'Repair session failed:'),
            update(event_product='cassandra', event_category='repair', event_type='session_failure')),

        rule(
            capture(r'Starting repair command #(?P<command>[0-9]*), repairing (?P<ranges>[0-9]*) ranges for keyspace (?P<keyspace>.*)( \(parallelism=(?P<parallelism>[^,]), full=(?P<full>[^)])\))?'),
            convert(int, 'command', 'ranges'),
            update(event_product='cassandra', event_category='repair', event_type='command_begin')),

        rule(
            capture(r'starting user-requested repair of range \[?\((?P<range_begin>[^,]*),(?P<range_end>[^\]]*)\]\]? for keyspace (?P<keyspace>[^ ]*) and column families \[(?P<tables>[^\]]*)\]'),
            convert(split(','), 'tables'),
            update(event_product='cassandra', event_category='repair', event_type='session_failure')),

        rule(
            capture(r"Flushing CFS\(Keyspace='(?P<keyspace>[^']*)', ColumnFamily='(?P<table>[^']*)'\) to relieve memory pressure"),
            update(event_product='cassandra', event_category='memtable', event_type='memory_pressure_flush')),

        rule(
            capture(r'Endpoint (?P<target_endpoint>[^ ]*) is down and will not receive data for re-replication of (?P<source_endpoint>.*)'),
            update(event_product='cassandra', event_category='gossip', event_type='replication_endpoint_down')),

        rule(
            capture(r'Removing tokens \[(?P<tokens>[^\]]*)\] for (?P<endpoint>.*)'),
            convert(split(', '), 'tokens'),
            update(event_product='cassandra', event_category='gossip', event_type='removal_not_confirmed')),

        rule(
            capture(r'Removal not confirmed for (for )?(?P<endpoints>.*)'),
            convert(split(','), 'endpoints'),
            update(event_product='cassandra', event_category='gossip', event_type='removal_not_confirmed')),

        rule(
            capture(r'Node (?P<endpoint>[^ ]*) state jump to (?P<state>.*)'),
            update(event_product='cassandra', event_category='gossip', event_type='node_state_jump')),

        rule(
            capture(r'Stopping gossip by operator request'),
            update(event_product='cassandra', event_category='gossip', event_type='operator_stop')),

        rule(
            capture(r'Starting gossip by operator request'),
            update(event_product='cassandra', event_category='gossip', event_type='operator_start')),

        rule(
            capture(r'Startup completed! Now serving reads.'),
            update(event_product='cassandra', event_category='startup', event_type='serving_reads')),

        rule(
            capture(r'Starting up server gossip'),
            update(event_product='cassandra', event_category='startup', event_type='gossip_starting')),

        rule(
            capture(r'Loading persisted ring state'),
            update(event_product='cassandra', event_category='startup', event_type='loading_ring_state')),

        rule(
            capture(r'Thrift API version: (?P<version>.*)'),
            update(event_product='cassandra', event_category='startup', event_type='thrift_api_version')),

        rule(
            capture(r'CQL supported versions: (?P<supported_versions>[^ ]*) \(default: (?P<default_version>[^)]*)\)'),
            convert(split(','), 'supported_versions'),
            update(event_product='cassandra', event_category='startup', event_type='cql_version')),

        rule(
            capture(r'(?P<component>[A-Za-z ]*) version: (?P<version>.*)'),
            update(event_product='cassandra', event_category='startup', event_type='component_version')),

        rule(
            capture(r'Using saved tokens? \[(?P<tokens>[^\]]*)\]'),
            convert(split(','), 'tokens'),
            update(event_product='cassandra', event_category='startup', event_type='using_saved_tokens')),

        rule(
            capture(r'setstreamthroughput: throttle set to (?P<stream_throughput>[0-9]*)'),
            convert(int, 'stream_throughput'),
            update(event_product='cassandra', event_category='config', event_type='stream_throughput')),

        rule(
            capture(r'DRAINING: starting drain process'),
            update(event_product='cassandra', event_category='shutdown', event_type='drain_begin')),

        rule(
            capture(r'DRAINED'),
            update(event_product='cassandra', event_category='shutdown', event_type='drain_end')),

        rule(
            capture(r'Cannot drain node \(did it already happen\?\)'),
            update(event_product='cassandra', event_category='shutdown', event_type='drain_failed')),

    case('MessagingService'),

        rule(
            capture(r'(?P<messages_dropped>[0-9]*) (?P<message_type>[^ ]*) messages dropped in last 5000ms(: (?P<internal_timeout>[0-9]*) for internal timeout and (?P<cross_node_timeout>[0-9]*) for cross node timeout)?'),
            convert(int, 'messages_dropped', 'internal_timeout', 'cross_node_timeout'),
            update(event_product='cassandra', event_category='status', event_type='messages_dropped')),

        rule(
            capture(r'(?P<message_type>[^ ]*) messages were dropped in last 5000 ms: (?P<internal_timeout>[0-9]*) for internal timeout and (?P<cross_node_timeout>[0-9]*) for cross node timeout'),
            convert(int, 'internal_timeout', 'cross_node_timeout'),
            update(event_product='cassandra', event_category='status', event_type='messages_dropped')),

        rule(
            capture(r'Starting Messaging Service on port (?P<port>[0-9]*)'),
            convert(int, 'port'),
            update(event_product='cassandra', event_category='startup', event_type='start_messaging_service')),

        rule(
            capture(r'Waiting for messaging service to quiesce'),
            update(event_product='cassandra', event_category='shutdown', event_type='messaging_service_wait_to_quiesce')),

        rule(
            capture(r'MessagingService has terminated the accept\(\) thread'),
            update(event_product='cassandra', event_category='shutdown', event_type='messaging_service_terminate_accept')),

        rule(
            capture(r'MessagingService shutting down server thread.'),
            update(event_product='cassandra', event_category='shutdown', event_type='messaging_service_shutdown_thread'))))

def update_message(fields):
    subfields = capture_message(fields['source_file'][:-5], fields['message'])
    if subfields is not None:
        fields.update(subfields)

capture_line = rule(
    capture(
        r' *(?P<level>[A-Z]*) *\[(?P<thread_name>[^\]]*?)[:_-]?(?P<thread_id>[0-9]*)\] (?P<date>.{10} .{12}) *(?P<source_file>[^:]*):(?P<source_line>[0-9]*) - (?P<message>.*)',
        r' *(?P<level>[A-Z]*) \[(?P<thread_name>[^\]]*?)[:_-]?(?P<thread_id>[0-9]*)\] (?P<date>.{10} .{12}) (?P<source_file>[^ ]*) \(line (?P<source_line>[0-9]*)\) (?P<message>.*)'),
    convert(date('%Y-%m-%d %H:%M:%S,%f'), 'date'),
    convert(int, 'source_line'),
    update_message,
    default(event_product='unknown', event_category='unknown', event_type='unknown'))

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
