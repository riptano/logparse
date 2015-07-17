from rules import *

def sstables(value):
    return [sstable[20:-2] for sstable in value.split(', ')]

capture_message = switch(

    case('CassandraDaemon'), 

        rule(
            capture(r'Logging initialized'), 
            update(event_type='startup_begin')),

        rule(
            capture(r'JVM vendor/version: (?P<jvm>.*)'), 
            update(event_type='startup_jvm_vendor')),

        rule(
            capture(r'Heap size: (?P<heap_used>[0-9]*)/(?P<total_heap>[0-9]*)'), 
            convert(int, 'heap_used', 'total_heap'), 
            update(event_type='startup_heap_size')),

        rule(
            capture(r'Classpath: (?P<classpath>.*)'),
            convert(split(':'), 'classpath'),
            update(event_type='startup_classpath')),

        rule(
            capture(r'JMX is not enabled to receive remote connections. Please see cassandra-env.sh for more info.'),
            update(event_type='startup_jmx_remote_disabled')),

        rule(
            capture(r'No gossip backlog; proceeding'),
            update(event_type='startup_gossip_backlog_done')),

        rule(
            capture(r'Waiting for gossip to settle before accepting client requests...'),
            update(event_type='startup_gossip_wait')),

        rule(
            capture(r'completed pre-loading \((?P<keys_loaded>[0-9]*) keys\) key cache.'),
            update(event_type='startup_preload_keycache')),

        rule(
            capture(r'Waiting for gossip to settle before accepting client requests...'),
            update(event_type='startup_gossip_settling')),

        rule(
            capture(r'Cassandra shutting down...'),
            update(event_type='shutdown')),

        rule(
            capture(r'Hostname: (?P<hostname>.*)'),
            update(event_type='startup_hostname')),

        rule(
            capture(r'(?P<memory_type>.*) memory: init = (?P<memory_init>[0-9]*)\([0-9]*K\) used = (?P<memory_used>[0-9]*)\([0-9]*K\) committed = (?P<memory_committed>[0-9]*)\([0-9]*K\) max = (?P<memory_max>[0-9-]*)\([0-9-]*K\)'),
            convert(int, 'memory_init', 'memory_used', 'memory_committed', 'memory_max'),
            update(event_type='startup_memory_size')),

        rule(
            capture(r'Exception in thread Thread\[(?P<exception_thread>[^\]]*)\]'),
            update(event_type='exception')),

    case('DseConfig', 'DseSearchConfig'),

        rule(
            capture(r'Load of settings is done.'),
            update(event_type='startup_dse_settings_done')),

        rule(
            capture(r'(?P<feature>.*) (is|are) enabled.'),
            update(event_type='startup_dse_feature_enabled')),

        rule(
            capture(r'(?P<feature>.*) (is|are) not enabled.'),
            update(event_type='startup_dse_feature_disabled')),

    case('DseDaemon'), 

        rule(
            capture(r'(?P<component>[A-Za-z ]*) versions?: (?P<version>.*)'),
            update(event_type='startup_dse_component_version')),

    case('GCInspector'), 

        rule(
            capture(r'Heap is (?P<percent_full>[0-9.]*) full.*'),
            convert(percent, 'percent_full'),
            update(event_type='gc_heap_full')),

        rule(
            capture(r'GC for (?P<gc_type>[A-Za-z]*): (?P<duration>[0-9]*) ms for (?P<collections>[0-9]*) collections, (?P<used>[0-9]*) used; max is (?P<max>[0-9]*)'),
            convert(int, 'duration', 'collections', 'used', 'max'),
            update(event_type='gc_pause')),

    case('ColumnFamilyStore'),

        rule(
            capture(
                r'Enqueuing flush of Memtable-(?P<table>[^@]*)@(?P<hash_code>[0-9]*)\((?P<serialized_bytes>[0-9]*)/(?P<live_bytes>[0-9]*) serialized/live bytes, (?P<ops>[0-9]*) ops\)',
                r'Enqueuing flush of (?P<table>[^:]*): (?P<on_heap_bytes>[0-9]*) \((?P<on_heap_limit>[0-9]*)%\) on-heap, (?P<off_heap_bytes>[0-9]*) \((?P<off_heap_limit>[0-9]*)%\) off-heap'),
            convert(int, 'hash_code', 'serialized_bytes', 'live_bytes', 'ops', 'on_heap_bytes', 'off_heap_bytes', 'on_heap_limit', 'off_heap_limit'),
            update(event_type='flush_enqueue')), 

        rule(
            capture(r'Initializing (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_type='startup_table_init')),

        rule(
            capture(r'Flushing SecondaryIndex Cql3SolrSecondaryIndex\{columnDefs=\[(?P<column_defs>).*\]\}'),
            convert(split(', '), 'column_defs'),
            update(event_type='flush_secondary_index')),

    case('Memtable', 'ColumnFamilyStore'),

        rule(
            capture(
                r'Writing Memtable-(?P<table>[^@]*)@(?P<hash_code>[0-9]*)\((?P<serialized_bytes>[0-9]*) serialized bytes, (?P<ops>[0-9]*) ops, (?P<on_heap_limit>[0-9]*)%/(?P<off_heap_limit>[0-9]*)% of on/off-heap limit\)',
                r'Writing Memtable-(?P<table>[^@]*)@(?P<hash_code>[0-9]*)\((?P<serialized_bytes>[0-9]*)/(?P<live_bytes>[0-9]*) serialized/live bytes, (?P<ops>[0-9]*) ops\)'),
            convert(int, 'hash_code', 'serialized_bytes', 'live_bytes', 'ops', 'on_heap_limit', 'off_heap_limit'),
            update(event_type='flush_begin')),

        rule(
            capture(r'Completed flushing (?P<filename>[^ ]*) \((?P<file_size>[0-9]*) bytes\) for commitlog position ReplayPosition\(segmentId=(?P<segment_id>[0-9]*), position=(?P<position>[0-9]*)\)'),
            convert(int, 'file_size', 'segment_id', 'position'),
            update(event_type='flush_end')),

    case('SSTableDeletingTask'),

        rule(
            capture(r"Unable to delete (?P<sstable_file>[^ ]*) \(it will be removed on server restart; we'll also retry after GC\)"),
            update(event_type='sstable_deletion_failed')),

    case('CompactionTask'), 

        rule(
            capture(r'Compacting \[(?P<input_sstables>[^\]]*)\]'),
            convert(sstables, 'input_sstables'),
            update(event_type='compaction_begin')), 

        rule(
            capture(
                r'Compacted (?P<sstable_count>[0-9]*) sstables to \[(?P<output_sstable>[^\]]*)\].  (?P<input_bytes>[0-9,]*) bytes to (?P<output_bytes>[0-9,]*) \(~(?P<percent_of_original>[0-9]*)% of original\) in (?P<duration>[0-9,]*)ms = (?P<rate>[0-9.]*)MB/s.  (?P<total_partitions>[0-9,]*) total (partitions|rows), (?P<unique_partitions>[0-9,]*) unique.  (Row|Partition) merge counts were \{(?P<partition_merge_counts>[^}]*)\}',
                r'Compacted (?P<sstable_count>[0-9]*) sstables to \[(?P<output_sstable>[^\]]*)\].  (?P<input_bytes>[0-9,]*) bytes to (?P<output_bytes>[0-9,]*) \(~(?P<percent_of_original>[0-9]*)% of original\) in (?P<duration>[0-9,]*)ms = (?P<rate>[0-9.]*)MB/s.  (?P<total_partitions>[0-9,]*) total (partitions|rows) merged to (?P<unique_partitions>[0-9,]*).  (Row|Partition) merge counts were \{(?P<partition_merge_counts>[^}]*)\}'),
            convert(int_with_commas, 'sstable_count', 'input_bytes', 'output_bytes', 'percent_of_original', 'duration', 'total_partitions', 'unique_partitions'),
            update(event_type='compaction_end')),

    case('CompactionController'), 

        rule(
            capture(r'Compacting large (partition|row) (?P<keyspace>[^/]*)/(?P<table>[^:]*):(?P<partition_key>.*) \((?P<partition_size>[0-9]*) bytes\) incrementally'),
            convert(int, 'partition_size'),
            update(event_type='compaction_incremental')),

#Compacting large row exchangecf/maventenanterrors:710a03f5-10f6-4d38-9ff4-a80b81da590d (93368360 bytes) incrementally

    case('Differencer', 'AntiEntropyService'),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] Endpoints (?P<node1>[^ ]*) and (?P<node2>[^ ]*) are consistent for (?P<table>.*)'),
            update(event_type='repair_endpoints_consistent')),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] Endpoints (?P<node1>[^ ]*) and (?P<node2>[^ ]*) have (?P<ranges>[0-9]*) range\(s\) out of sync for (?P<table>.*)'),
            convert(int, 'ranges'),
            update(event_type='repair_endpoints_inconsistent')),

    case('RepairSession', 'AntiEntropyService'),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] Received merkle tree for (?P<table>[^ ]*) from (?P<node>.*)'),
            update(event_type='repair_merkle_received')),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] (?P<table>[^ ]*) is fully synced'),
            update(event_type='repair_table_synced')),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] session completed successfully'),
            update(event_type='repair_session_success')),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] new session: will sync (?P<nodes>.*?) on range \((?P<range_begin>[^,]*),(?P<range_end>[^\]]*)\] for (?P<keyspace>[^.]*)\.\[(?P<tables>[^\]]*)\]'),
            convert(split(', '), 'nodes', 'tables'),
            update(event_type='repair_session_begin')),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] Cannot proceed on repair because a neighbor \((?P<endpoint>[^)]*)\) is dead: session failed'),
            update(event_type='repair_session_cannot_proceed')), 

    case('RepairJob', 'AntiEntropyService'),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] requesting merkle trees for (?P<table>[^ ]*) \(to \[(?P<nodes>[^\]]*)\]\)'),
            convert(split(', '), 'nodes'),
            update(event_type='repair_merkle_requested')),

    case('StreamInSession'),  
        
        rule(
            capture(r'Finished streaming session (?P<session_id>[^ ]*) from (?P<node>.*)'),
            update(event_type='stream_session_end')),

    case('StreamResultFuture'),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Session with (?P<node>[^ ]*) is complete'),
            update(event_type='stream_session_end')),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Prepare completed. Receiving (?P<receiving_files>[0-9]*) files\((?P<receiving_bytes>[0-9]*) bytes\), sending (?P<sending_files>[0-9]*) files\((?P<sending_bytes>[0-9]*) bytes\)'),
            convert(int, 'receiving_files', 'receiving_bytes', 'sending_files', 'sending_bytes'),
            update(event_type='stream_prepare_complete')),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Executing streaming plan for (?P<action>.*)'),
            update(event_type='stream_plan_executing')),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] All sessions completed'),
            update(event_type='stream_all_sessions_complete')),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Beginning stream session with (?P<endpoint>.*)'),
            update(event_type='stream_session_begin')),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Received streaming plan for (?P<action>.*)'),
            update(event_type='stream_plan_received')),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Stream failed'),
            update(event_type='stream_failure')),

    case('StreamingRepairTask'),

        rule(
            capture(r'\[streaming task #(?P<session_id>[^\]]*)\] Performing streaming repair of (?P<ranges>[0-9]*) ranges with (?P<node>[^ ]*)'),
            update(event_type='stream_task_begin')),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] streaming task succeed, returning response to (?P<node>[^ ]*)'),
            update(event_type='stream_task_succeeded')),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] Forwarding streaming repair of (?P<ranges>[0-9]*) ranges to (?P<forwarded_endpoint>[^ ]*) \(to be streamed with (?P<target_endpoint>[^)]*)\)'),
            update(event_type='stream_forwarding')),

    case('StreamSession'),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Streaming error occurred'),
            update(event_type='stream_session_error')),

        rule(
            capture(r'\[Stream #(?P<session_id>[^\]]*)\] Starting streaming to (?P<endpoint>.*)'),
            update(event_type='stream_begin')),

    case('StreamReplyVerbHandler'),

        rule(
            capture(r'Successfully sent (?P<sstable_name>[^ ]*) to (?P<node>.*)'),
            update(event_type='stream_sstable_sent')),

    case('OutboundTcpConnection'),

        rule(
            capture(r'Handshaking version with (?P<endpoint>.*)'),
            update(event_type='version_handshake')),

        rule(
            capture(r'Cannot handshake version with (?P<endpoint>.*)'),
            update(event_type='version_handshake_failure')),

    case('Gossiper'),

        rule(
            capture(r'InetAddress (?P<endpoint>[^ ]*) is now UP'),
            update(event_type='node_up')),

        rule(
            capture(r'InetAddress (?P<endpoint>[^ ]*) is now DOWN'),
            update(event_type='node_down')),

        rule(
            capture(r'Node (?P<endpoint>[^ ]*) has restarted, now UP'),
            update(event_type='node_down')),

    case('SSTableReader'),

        rule(
            capture(r'Opening (?P<sstable_name>[^ ]*) \((?P<bytes>[0-9]*) bytes\)'),
            convert(int, 'bytes'),
            update(event_type='startup_sstable_open')),

    case('StatusLogger'),

        rule(
            capture(r'Pool Name +Active +Pending +Completed +Blocked +All Time Blocked'),
            update(event_type='pool_header')),

        rule(
            capture(r'(?P<pool_name>[A-Za-z_]+) +((?P<active>[0-9]+)|n/a) +(?P<pending>[0-9]+)(/(?P<pending_responses>[0-9]+))?( +(?P<completed>[0-9]+) +(?P<blocked>[0-9]+) +(?P<all_time_blocked>[0-9]+))?'),
            convert(int, 'active', 'pending', 'pending_responses', 'completed', 'blocked', 'all_time_blocked'),
            update(event_type='pool_status')),

        rule(
            capture(r'Cache Type +Size +Capacity +KeysToSave(Provider)?'),
            update(event_type='cache_header')),

        rule(
            capture(r'(?P<cache_type>[A-Za-z]*Cache(?! Type)) *(?P<size>[0-9]*) *(?P<capacity>[0-9]*) *(?P<keys_to_save>[^ ]*) *(?P<provider>[A-Za-z_.$]*)'),
            convert(int, 'size', 'capacity'),
            update(event_type='cache_status')),

        rule(
            capture(r'ColumnFamily +Memtable ops,data'),
            update(event_type='memtable_header')),


        rule(
            capture(r'(?P<keyspace>[^.]*)\.(?P<table>[^ ]*) +(?P<ops>[0-9]*),(?P<data>[0-9]*)'),
            convert(int, 'ops', 'data'),
            update(event_type='memtable_status')),

    case('CommitLogReplayer'),
                
        rule(
            capture(r'Replaying (?P<commitlog_file>[^ ]*)( \(CL version (?P<commitlog_version>[0-9]*), messaging version (?P<messaging_version>[0-9]*)\))?'),
            convert(int, 'commitlog_version', 'messaging_version'),
            update(event_type='commitlog_replay_begin')),

        rule(
            capture(r'Finished reading (?P<commitlog_file>.*)'),
            update(event_type='commitlog_replay_end')),

    case('SecondaryIndexManager'),

        rule(
            capture(r'Creating new index : ColumnDefinition\{(?P<column_definition>[^}]*)\}'),
            convert(split(', '), 'column_definition'),
            update(event_type='secondary_index_create')),

        rule(
            capture(r"Submitting index build of \[(?P<keyspace>[^.]*)\.(?P<table>[^\]]*)\] for data in (?P<sstables>.*)"),
            convert(sstables, 'sstables'),
            update(event_type='secondary_index_build_begin')),

        rule(
            capture(r'Index build of \[(?P<keyspace>[^.]*)\.(?P<table>[^\]]*)\] complete'),
            update(event_type='secondary_index_build_end')),

    case('ShardRouter'),

        rule(
            capture(r'Updating shards state due to endpoint (?P<endpoint>[^ ]*) changing state (?P<state>.*)'),
            update(event_type='solr_shard_state_change')),

    case('QueryProcessor'),

        rule(
            capture(r'Column definitions for (?P<keyspace>[^.]*)\.(?P<table>[^ ]*) changed, invalidating related prepared statements'),
            update(event_type='solr_column_definition_changed')),

        rule(
            capture(r'Keyspace (?P<keyspace>[^ ]*) was dropped, invalidating related prepared statements'),
            update(event_type='solr_keyspace_dropped')),

        rule(
            capture(r'Table (?P<keyspace>[^.]*)\.(?P<table>[^ ]*) was dropped, invalidating related prepared statements'),
            update(event_type='solr_table_dropped')),

    case('SolrCoreResourceManager'),

        rule(
            capture(r"Wrote resource '(?P<resource>[^']*)' for core '(?P<keyspace>[^.]*)\.(?P<table>[^']*)'"),
            update(event_type='solr_write_resource')),

        rule(
            capture(r'Trying to load resource (?P<resource>[^ ]*) for core (?P<keyspace>[^.]*).(?P<table>[^ ]*) by querying from local node with CL (?P<consistency_level>.*)'),
            update(event_type='solr_load_resource_attempt')),

        rule(
            capture(r'Successfully loaded resource (?P<resource>[^ ]*) for core (?P<keyspace>[^.]*).(?P<table>[^ ]*)'),
            update(event_type='solr_load_resource_success')),

        rule(
            capture(r'No resource (?P<resource>[^ ]*) found for core (?P<keyspace>[^.]*).(?P<table>[^ ]*) on any live node\.'),
            update(event_type='solr_load_resource_failure')),

        rule(
            capture(r'Creating core: (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_type='solr_create_core')),

    case('AbstractSolrSecondaryIndex'),

        rule(
            capture(r'Configuring index commit log for (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_type='solr_configure_index_commitlog')),

        rule(
            capture(r'Configuring index metrics for (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_type='solr_configure_index_metrics')),

        rule(
            capture(r'Ensuring existence of index directory (?P<index_directory>.*)'),
            update(event_type='solr_ensure_index_directory')),

        rule(
            capture(r'Executing hard commit on index (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_type='solr_execute_hard_commit')),

        rule(
            capture(r'Loading core on keyspace (?P<keyspace>[^ ]*) and column family (?P<table>.*)'),
            update(event_type='solr_load_core')),

        rule(
            capture(r'No commit log entries for core (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_type='solr_no_commitlog_entries')),

        rule(
            capture(r'Start index TTL scheduler for (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_type='solr_start_index_ttl_scheduler')),

        rule(
            capture(r'Start index initializer for (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_type='solr_start_index_initializer')),

        rule(
            capture(r'Start index reloader for (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_type='solr_start_index_reloader')),

        rule(
            capture(r'Start indexing pool for (?P<keyspace>[^.]*).(?P<table>.*)'),
            update(event_type='solr_start_indexing_pool')),

    case('YamlConfigurationLoader'),
            
        rule(
            capture(r'Loading settings from file:(?P<yaml_file>.*)'),
            update(event_type='config_load_settings')),

        rule(
            capture(r'Node configuration:\[(?P<node_configuration>.*)\]'),
            convert(split('; '), 'node_configuration'),
            update(event_type='config_output')),

    case('Worker'),

        rule(
            capture(r'Shutting down work pool worker!'),
            update(event_type='work_pool_shutdown')),

    case('SolrDispatchFilter'),

        rule(
            capture(r'SolrDispatchFilter.init\(\) done'),
            update(event_type='solr_dispatch_filter_init_done')),

        rule(
            capture(r'Error request params: (?P<params>.*)'),
            convert(split('&'), 'params'),
            update(event_type='solr_error_request_params')),

        rule(
            capture(r'\[admin\] webapp=(?P<webapp>[^ ]*) path=(?P<path>[^ ]*) params=\{(?P<params>[^}]*)\} status=(?P<status>[0-9]*) QTime=(?P<qtime>[0-9]*)'),
            convert(split('&'), 'params'),
            convert(int, 'status', 'qtime'),
            update(event_type='solr_admin')),

        rule(
            capture(r'user.dir=(?P<user_dir>.*)'),
            update(event_type='solr_user_dir')),

    case('ExternalLogger', 'SparkWorker-0 ExternalLogger'),

        rule(
            capture(r'(?P<source>[^:]*): (?P<message>.*)'),
            update(event_type='spark_external_logger')),

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
            update(event_type='spark_worker_started')),

        rule(
            capture(r'Spark Master not ready yet at (?P<master_host>[^:]*):(?P<master_port>[0-9]+)\.\.\.'),
            convert(int, 'master_port'),
            update(event_type='spark_master_not_ready')),

    case('AbstractSparkRunner'),

        rule(
            capture(r'Starting Spark process: (?P<process>.*)'),
            update(event_type='spark_process_starting')),

        rule(
            capture(r'Process (?P<process>[^ ]) has just received (?P<signal>.*)'),
            update(event_type='spark_received_signal')),

        rule(
            capture(r'(?P<process>[^ ]*) threw exception in state (?P<state>[^:]*):'),
            update(event_type='spark_process_exception')),

    case('JobTrackerManager'),

        rule(
            capture(r'Failed to retrieve jobtracker locations at CL.(?P<consistency_level>[^ ]*) \((?P<error>[^)]*)\)'),
            update(event_type='jobtracker_location_failure')),

    case('SliceQueryFilter'),

        rule(
            capture(r'Scanned over (?P<tombstoned_cells>[0-9]*) tombstones in (?P<keyspace>[^.]*).(?P<table>[^ ]*); query aborted \(see tombstone_failure_threshold\)'),
            convert(int, 'live_cells', 'tombstoned_cells', 'requested_columns'),
            convert(split(', '), 'deletion_info'),
            update(event_type='tombstone_warning')),

        rule(
            capture(r'Read (?P<live_cells>[0-9]*) live and (?P<tombstoned_cells>[0-9]*) tombstoned cells in (?P<keyspace>[^.]*).(?P<table>[^ ]*) \(see tombstone_warn_threshold\). (?P<requested_columns>[0-9]*) columns was requested, slices=\[(?P<slice_start>[^-]*)-(?P<slice_end>[^\]]*)\], delInfo=\{(?P<deletion_info>[^}]*)\}'),
            convert(int, 'live_cells', 'tombstoned_cells', 'requested_columns'),
            convert(split(', '), 'deletion_info'),
            update(event_type='tombstone_warning')),

    case('BatchStatement'),

        rule(
            capture(r'Batch of prepared statements for \[(?P<keyspace>[^.]*).(?P<table>[^\]]*)\] is of size (?P<batch_size>[0-9]*), exceeding specified threshold of (?P<batch_warn_threshold>[0-9]*) by (?P<threshold_exceeded_by>[0-9]*).'),
            convert(int, 'batch_size', 'batch_warn_threshold', 'threshold_excess'),
            update(event_type='batch_size_warning')),
    
    case('MeteredFlusher'),

        rule(
            capture(r"Flushing high-traffic column family CFS\(Keyspace='(?P<keyspace>[^']*)', ColumnFamily='(?P<table>[^']*)'\) \(estimated (?P<estimated_bytes>[0-9]*) bytes\)"),
            convert(int, 'estimated_bytes'),
            update(event_type='flush_metered')),
        
    case('Validator', 'AntiEntropyService'),

        rule(
            capture(r'\[repair #(?P<session_id>[^\]]*)\] Sending completed merkle tree to (?P<node>[^ ]*) for \(?(?P<keyspace>[^,]*)[/,](?P<table>[^)]*)\)?'),
            update(event_type='repair_merkle_sent')), 

    case('HintedHandOffManager'),

        rule(
            capture(r'Finished hinted handoff of (?P<rows>[0-9]*) rows to endpoint (?P<endpoint>.*)'),
            convert(int, 'rows'),
            update(event_type='hinted_handoff_end')),

        rule(
            capture(r'Started hinted handoff for host: (?P<host_id>[^ ]*) with IP: (?P<endpoint>.*)'),
            update(event_type='hinted_handoff_begin')),

        rule(
            capture(r'Timed out replaying hints to (?P<endpoint>.*); aborting \((?P<hints_delivered>[0-9]*) delivered\)'),
            convert(int, 'hints_delivered'),
            update(event_type='hinted_handoff_timeout')),

    case('PluginManager'),

        rule(
            capture(r'Plugin activated: (?P<plugin_class>.*)'),
            update(event_type='plugin_activated')),

        rule(
            capture(r'Registered plugin (?P<plugin_class>.*)'),
            update(event_type='plugin_registered')),

        rule(
            capture(r'Deactivating plugin: (?P<plugin_class>.*)'),
            update(event_type='plugin_deactivating')),

        rule(
            capture(r'Activating plugin: (?P<plugin_class>.*)'),
            update(event_type='plugin_activating')),

        rule(
            capture(r'All plugins are stopped.'),
            update(event_type='plugin_all_stopped')),

    case('AutoSavingCache'),

        rule(
            capture(r'Saved (?P<cache_type>[^ ]*) \((?P<cache_items>[0-9]*) items\) in (?P<save_duration>[0-9]*) ms'),
            convert(int, 'cache_items', 'save_duration'),
            update(event_type='cache_saved')),

        rule(
            capture(r'reading saved cache (?P<cache_file>.*)'),
            update(event_type='cache_read')),

    case('MigrationTask'),

        rule(
            capture(r"Can't send migration request: node (?P<endpoint>[^ ]*) is down."),
            update(event_type='migration_request_failure')),

    case('MigrationManager'),

        rule(
            capture(r"Drop Keyspace '(?P<keyspace>[^']*)'"),
            update(event_type='migration_drop_keyspace')),

        rule(
            capture(r"Update ColumnFamily '(?P<keyspace>[^/]*)/(?P<table>[^']*)' From org.apache.cassandra.config.CFMetaData@(?P<old_hash>[^\]]*)\[(?P<old_metadata>.*)\] To org.apache.cassandra.config.CFMetaData@(?P<new_hash>[^\]]*)\[(?P<new_metadata>.*)\]"),
            update(event_type='migration_update_table')),

        rule(
            capture(r"Create new ColumnFamily: org.apache.cassandra.config.CFMetaData@(?P<hash>[^\]]*)\[(?P<metadata>.*)\]"),
            update(event_type='migration_create_table')),

        rule(
            capture(r"Create new Keyspace: KSMetaData\{(?P<metadata>.*)\}"),
            update(event_type='migration_create_keyspace')),

    case('DefsTables'),

        rule(
            capture(r'Loading org.apache.cassandra.config.CFMetaData@(?P<hash>[^\]]*)\[(?P<metadata>.*)\]'),
            update(event_type='migration_load_table_metadata')),

    case('StorageService'),

        rule(
            capture(r'Node (?P<endpoint>[^ ]*) state jump to normal'),
            update(event_type='node_state_normal')),

        rule(
            capture(r'Repair session (?P<session_id>[^\]]*) for range \((?P<range_begin>[^,]*),(?P<range_end>[^\]]*)\] finished'),
            convert(int, 'range_begin', 'range_end'),
            update(event_type='repair_session_finished')),

        rule(
            capture(r'Repair session (?P<session_id>[^\]]*) for range \((?P<range_begin>[^,]*),(?P<range_end>[^\]]*)\] failed with error (?P<error>)'),
            convert(int, 'range_begin', 'range_end'),
            update(event_type='repair_session_failure')),

        rule(
            capture(r'Repair session failed:'),
            update(event_type='repair_session_failure')),

        rule(
            capture(r'Starting up server gossip'),
            update(event_type='startup_gossip_starting')),

        rule(
            capture(r'Loading persisted ring state'),
            update(event_type='startup_loading_ring_state')),

        rule(
            capture(r'Thrift API version: (?P<version>.*)'),
            update(event_type='startup_thrift_api_version')),

        rule(
            capture(r'CQL supported versions: (?P<supported_versions>[^ ]*) \(default: (?P<default_version>[^)]*)\)'),
            convert(split(','), 'supported_versions'),
            update(event_type='startup_cql_version')),

        rule(
            capture(r'Cassandra version: (?P<version>.*)'),
            update(event_type='startup_cassandra_version')),

        rule(
            capture(r'Using saved tokens \[(?P<tokens>[^\]]*)\]'),
            convert(split(','), 'tokens'),
            update(event_type='startup_using_saved_tokens')),

        rule(
            capture(r'setstreamthroughput: throttle set to (?P<stream_throughput>[0-9]*)'),
            convert(int, 'stream_throughput'),
            update(event_type='node_set_stream_throughput')),

    case('MessagingService'),

        rule(
            capture(r'(?P<messages_dropped>[0-9]*) (?P<message_type>[^ ]*) messages dropped in last 5000ms'),
            convert(int, 'messages_dropped'),
            update(event_type='messages_dropped')))

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
    default(event_type='unknown'))

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
