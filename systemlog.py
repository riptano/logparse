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
            update(event_type='startup_jvm')),

        rule(
            capture(r'Heap size: (?P<heap_used>[0-9]*)/(?P<total_heap>[0-9]*)'), 
            convert(int, 'heap_used', 'total_heap'), 
            update(event_type='startup_heap_size')),

        rule(
            capture(r'Classpath: (?P<classpath>.*)'),
            convert(split(':'), 'classpath'),
            update(event_type='startup_classpath')),

#(?P<memory_type>.*) memory: init = [0-9]*\([0-9]*K\) used = [0-9]*\([0-9]*K\) committed = [0-9]*\([0-9]*K\) max = [0-9-]*\([0-9-]*K\)
#CMS Old Gen Heap memory: init = 3456106496(3375104K) used = 0(0K) committed = 3456106496(3375104K) max = 3456106496(3375104K)
#Code Cache Non-heap memory: init = 2555904(2496K) used = 7276480(7105K) committed = 7340032(7168K) max = 251658240(245760K)
#Compressed Class Space Non-heap memory: init = 0(0K) used = 3162984(3088K) committed = 3325952(3248K) max = 1073741824(1048576K)
#Metaspace Non-heap memory: init = 0(0K) used = 25621304(25020K) committed = 26017792(25408K) max = -1(-1K)
#Par Eden Space Heap memory: init = 671088640(655360K) used = 671088640(655360K) committed = 671088640(655360K) max = 671088640(655360K)
#Par Survivor Space Heap memory: init = 83886080(81920K) used = 22923352(22386K) committed = 83886080(81920K) max = 83886080(81920K)

#Hostname: jblangston-rmbp.local
#JMX is not enabled to receive remote connections. Please see cassandra-env.sh for more info.
#completed pre-loading (8 keys) key cache.
#No gossip backlog; proceeding
#Waiting for gossip to settle before accepting client requests...
#Cassandra shutting down...

#DseConfig
#Load of settings is done.	
#(?P<feature>.*) (?P<enabled>(is|are|is not|are not)) enabled
#CQL slow log is enabled	 	28
#Resource level latency tracking is not enabled	 	28
#Spark cluster info tables are not enabled	 	28
#User level latency tracking is not enabled	 	28
#Cluster summary stats are not enabled	 	28
#Database summary stats are not enabled	 	28
#CQL system info tables are not enabled	 	28
#Histogram data tables are not enabled	 	28

#DseSearchConfig
#(?P<feature>.*) (?P<enabled>(is|are|is not|are not)) enabled
#Solr latency snapshots are not enabled	 	14
#Solr update handler metrics are not enabled	 	14
#Solr request handler metrics are not enabled	 	14
#Solr node health tracking is not enabled	 	14
#Solr index statistics reporting is not enabled	 	14
#Solr slow sub-query log is not enabled	 	14
#Solr cache statistics reporting is not enabled	 	14
#Solr indexing error log is not enabled	

    case('DseDaemon'), 

        rule(
            capture(r'(?P<component>[A-Za-z ]*) versions?: (?P<version>.*)'),
            update(event_type='startup_component_version')),

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

    case('CompactionTask'), 

        rule(
            capture(r'Compacting \[(?P<input_sstables>[^\]]*)\]'),
            convert(sstables, 'input_sstables'),
            update(event_type='compaction_begin')), 

        rule(
            capture(
                r'Compacted (?P<sstable_count>[0-9]*) sstables to \[(?P<output_sstable>[^,]*),\].  (?P<input_bytes>[0-9,]*) bytes to (?P<output_bytes>[0-9,]*) \(~(?P<percent_of_original>[0-9]*)% of original\) in (?P<duration>[0-9,]*)ms = (?P<rate>[0-9.]*)MB/s.  (?P<total_partitions>[0-9,]*) total (partitions|rows), (?P<unique_partitions>[0-9,]*) unique.  (Row|Partition) merge counts were \{(?P<partition_merge_counts>[^}]*)\}',
                r'Compacted (?P<sstable_count>[0-9]*) sstables to \[(?P<output_sstable>[^,]*),\].  (?P<input_bytes>[0-9,]*) bytes to (?P<output_bytes>[0-9,]*) \(~(?P<percent_of_original>[0-9]*)% of original\) in (?P<duration>[0-9,]*)ms = (?P<rate>[0-9.]*)MB/s.  (?P<total_partitions>[0-9,]*) total (partitions|rows) merged to (?P<unique_partitions>[0-9,]*).  (Row|Partition) merge counts were \{(?P<partition_merge_counts>[^}]*)\}'),
            convert(int_with_commas, 'sstable_count', 'input_bytes', 'output_bytes', 'percent_of_original', 'duration', 'total_partitions', 'unique_partitions'),
            update(event_type='compaction_end')),

#Compacted 17 sstables to [].  4,352,295 bytes to 0 (~0% of original) in 176ms = 0.000000MB/s.  47 total partitions merged to 0.  Partition merge counts were {2:5, 3:8, 4:2, 5:1, }

#SSTableDeletingTask
#Unable to delete /mnt/cassandra/data/exchangecf/useractivityuserhourlysnapshot/exchangecf-useractivityuserhourlysnapshot-jb-6-Data.db (it will be removed on server restart; we'll also retry after GC)

    case('CompactionController'), 

        rule(
            capture(r'Compacting large (partition|row) (?P<keyspace>[^/]*)/(?P<table>[^:]*):(?P<partition_key>[0-9]*) \((?P<partition_size>[0-9]*) bytes\) incrementally'),
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
            capture(r'\[repair #(?P<session_id>[^\]]*)\] Forwarding streaming repair of (?P<ranges>[0-9]) ranges to (?P<forwarded_endpoint>[^ ]*) \(to be streamed with (?P<target_endpoint>[^)]*)\)'),
            update(event_type='stream_forwarding')),

#[repair #1afc26e0-1450-11e5-a3ed-3f8e9486b005] Forwarding streaming repair of 942 ranges to /10.1.0.21 (to be streamed with /10.1.0.15)
#Forwarding streaming repair of 11 ranges to /10.1.40.18 (to be streamed with /10.1.0.21)

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

#OutboundTcpConnection
#Handshaking version with /10.1.0.14
#Cannot handshake version with /10.1.40.19

#Gossiper
#InetAddress /10.1.40.13 is now UP
#InetAddress /10.1.40.19 is now DOWN
#Node /10.1.40.21 has restarted, now UP

    case('SSTableReader'),

        rule(
            capture(r'Opening (?P<sstable_name>[^ ]*) \((?P<bytes>[0-9]*) bytes\)'),
            convert(int, 'bytes'),
            update(event_type='startup_sstable_open')),

    case('StatusLogger'),

        rule(
            capture(r'Pool Name *Active *Pending *Completed *Blocked *All Time Blocked'),
            update(event_type='pool_header')),

        rule(
            capture(r'(?P<pool_name>[A-Za-z_]+) +(?P<active>[0-9]+) +(?P<pending>[0-9]+) +(?P<completed>[0-9]+) +(?P<blocked>[0-9]+) +(?P<all_time_blocked>[0-9]+)'),
            convert(int, 'active', 'pending', 'completed', 'blocked', 'all_time_blocked'),
            update(event_type='pool_status')),

        rule(
            capture(r'Cache Type *Size *Capacity *KeysToSave *Provider'),
            update(event_type='cache_header')),

        rule(
            capture(r'(?P<cache_type>[A-Za-z]*Cache(?! Type)) *(?P<size>[0-9]*) *(?P<capacity>[0-9]*) *(?P<keys_to_save>[^ ]*) *(?P<provider>[A-Za-z_.$]*)'),
            convert(int, 'size', 'capacity'),
            update(event_type='cache_status')),

        rule(
            capture(r'ColumnFamily *Memtable ops,data'),
            update(event_type='memtable_header')),


        rule(
            capture(r'(?P<keyspace>[^.]*)\.(?P<table>[^ ]*) *(?P<ops>[0-9]*),(?P<data>[0-9]*)'),
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

#Submitting index build of [tenantactivity.errorcodeIndex] for data in SSTableReader(path='/mnt/cassandra/data/exchangecf/tenantactivity/exchangecf-tenantactivity-jb-3572-Data.db'), SSTableReader(path='/mnt/cassandra/data/exchangecf/tenantactivity/exchangecf-tenantactivity-jb-3574-Data.db'), SSTableReader(path='/mnt/cassandra/data/exchangecf/tenantactivity/exchangecf-tenantactivity-jb-3575-Data.db'), SSTableReader(path='/mnt/cassandra/data/exchangecf/tenantactivity/exchangecf-tenantactivity-jb-3576-Data.db'), SSTableReader(path='/mnt/cassandra/data/exchangecf/tenantactivity/exchangecf-tenantactivity-jb-3577-Data.db'), SSTableReader(path='/mnt/cassandra/data/exchangecf/tenantactivity/exchangecf-tenantactivity-jb-3578-Data.db'), SSTableReader(path='/mnt/cassandra/data/exchangecf/tenantactivity/exchangecf-tenantactivity-jb-3579-Data.db')
#Index build of [tenantactivity.errorcodeIndex] complete

#ShardRouter
#Updating shards state due to endpoint /127.0.0.1 changing state SCHEMA=54d5afa5-2326-38c5-bb1b-2ac1072857a6

#QueryProcessor
#Column definitions for zendesk.users changed, invalidating related prepared statements
#Keyspace zendesk was dropped, invalidating related prepared statements	
#Table zendesk.users was dropped, invalidating related prepared statements	

    case('SolrCoreResourceManager'),

        rule(
            capture(r"Wrote resource '(?P<resource>[^']*)' for core '(?P<keyspace>[^.]*)\.(?P<table>[^']*)'"),
            update(event_type='solr_write_resource')),

        rule(
            capture(r'Trying to load resource (?P<resource>[^ ]*) for core (?P<keyspace>[^.]*).(?P<table>[^ ]*) by querying from local node with (?P<consistency_level>.*)'),
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

#Wrote resource 'schema.xml' for core 'zendesk.users'
#Trying to load resource schema.xml for core zendesk.users by querying from local node with CL QUORUM
#Successfully loaded resource schema.xml for core zendesk.users
#No resource schema.xml.bak found for core zendesk.users on any live node.
#Creating core: zendesk.tickets

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

#SolrDispatchFilter
#[admin] webapp=null path=/admin/cores params={name=zendesk.organizations&action=CREATE} status=0 QTime=25658 
#Error request params: name=zendesk.ticket_audits&action=CREATE
#user.dir=/opt/dse-4.7.0
#SolrDispatchFilter.init() done

    case('ExternalLogger'),

        rule(
            capture(r'(?P<source>[^:]*): (?P<message>.*)'),
            update(event_type='spark_external_logger')),

#SparkMaster: Adding host 10.1.40.10 (Analytics)	
#SparkMaster: Ignoring remote host 10.1.0.20 (Cassandra)	
#SparkMaster: Found host with 0.0.0.0 as rpc_address, using listen_address (/10.1.40.20) to contact it instead. If this is incorrect you should avoid the use of 0.0.0.0 server side.
#SparkWorker: Killing process!
#SparkWorker: Asked to kill executor app-20150627020010-0342/5
#SparkWorker: Launch command: "/usr/lib/jvm/jdk1.7.0_75//bin/java" "-cp" "a.jar:b.jar" "-XX:MaxPermSize=128m" "-Djava.library.path=:/usr/share/dse/hadoop/native/Linux-amd64-64/lib" "-XX:MaxPermSize=256M" "-Djava.io.tmpdir=/mnt/spark/rdd" "-Dspark.cassandra.connection.factory=com.datastax.bdp.spark.DseCassandraConnectionFactory" "-Dspark.kryoserializer.buffer.mb=10" "-Dspark.driver.port=32991" "-Dcassandra.config.loader=com.datastax.bdp.config.DseConfigurationLoader" "-Dspark.cassandra.auth.conf.factory=com.datastax.bdp.spark.DseAuthConfFactory" "-Djava.system.class.loader=com.datastax.bdp.loader.DseClientClassLoader" "-Dlog4j.configuration=file:///etc/dse/spark/log4j-executor.properties" "-Dspark.cassandra.connection.host=10.1.40.10" "-Xms66560M" "-Xmx66560M" "org.apache.spark.executor.CoarseGrainedExecutorBackend" "akka.tcp://sparkDriver@10.1.40.10:32991/user/CoarseGrainedScheduler" "5" "10.1.40.10" "7" "akka.tcp://sparkWorker@10.1.40.10:53306/user/Worker" "app-20150627043009-0345"
#SparkWorker: Runner thread for executor app-20150627043009-0345/5 interrupted
#SparkWorker: Executor app-20150625220827-0264/5 finished with state KILLED exitStatus 1	

#SparkWorkerRunner
#Spark Master not ready yet at 10.1.40.10:7077...
#Started Spark Worker, connected to master 10.1.40.10:7077

#JobTrackerManager
#Failed to retrieve jobtracker locations at CL.QUORUM (Operation timed out - received only 12 responses.)

    case('SliceQueryFilter'),

        rule(
            capture(r'Read (?P<live_cells>[0-9]*) live and (?P<tombstoned_cells>[0-9]*) tombstoned cells in (?P<keyspace>[^.]*).(?P<table>[^ ]*) \(see tombstone_warn_threshold\). (?P<requested_columns>[0-9]*) columns was requested, slices=\[(?P<slice_start>[^-]*)-(?P<slice_end>[^\]]*)\], delInfo=\{(?P<deletion_info>[^}]*)\}'),
            convert(int, 'live_cells', 'tombstoned_cells', 'requested_columns'),
            convert(split(', '), 'deletion_info'),
            update(event_type='tombstone_warning')),

#Scanned over 100000 tombstones in exchangecf.useractivity; query aborted (see tombstone_failure_threshold)

#BatchStatement
#Batch of prepared statements for [exchangecf.maventenanterrors] is of size 277799, exceeding specified threshold of 65536 by 212263.
    
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

#Timed out replaying hints to /10.1.40.11; aborting (31085 delivered)

#PluginManager
#Plugin activated: com.datastax.bdp.plugin.SparkPlugin
#Registered plugin com.datastax.bdp.plugin.JobTrackerManagerPlugin
#Deactivating plugin: com.datastax.bdp.leases.PeriodicTaskOwnershipPlugin
#Activating plugin: com.datastax.bdp.plugin.SparkPlugin
#All plugins are stopped.

#AutoSavingCache
#Saved KeyCache (1267480 items) in 1869 ms
#reading saved cache /mnt/cassandra/saved_caches/dse_system-job_trackers-KeyCache-b.db

#MigrationTask
#Can't send migration request: node /10.1.0.15 is down.

#MigrationManager
#Drop Keyspace 'zendesk'	
#Update ColumnFamily 'zendesk/tickets' From org.apache.cassandra.config.CFMetaData@19aacb04[ ... ] To org.apache.cassandra.config.CFMetaData@42550147[ ... ]
#Create new ColumnFamily: org.apache.cassandra.config.CFMetaData@6bad1d87[ ... ]
#Create new Keyspace: KSMetaData{ ... }

#DefsTables
#Loading org.apache.cassandra.config.CFMetaData@655eef60[ ... ]

    case('StorageService'),

        rule(
            capture(r'Node (?P<endpoint>[^ ]*) state jump to normal'),
            update(event_type='node_state_normal')),

        rule(
            capture(r'Repair session (?P<session_id>[^\]]*) for range \((?P<range_begin>[^,]*),(?P<range_end>[^\]]*)\] failed with error (?P<error>)'),
            convert(int, 'range_begin', 'range_end'),
            update(event_type='repair_session_failure')),

        rule(
            capture(r'setstreamthroughput: throttle set to (?P<stream_throughput>[0-9]*)'),
            convert(int, 'stream_throughput'),
            update(event_type='node_set_stream_throughput')),

#Repair session failed:
#Repair session 5f673720-0a41-11e5-aa12-ffcc6d9f0e80 for range (2215001505538808661,2237421997010099173] finished
#Starting up server gossip	 	14
#Loading persisted ring state	 	14
#Thrift API version: 19.39.0	 	14
#Using saved tokens [80372383360720788]	 	14
#Node /127.0.0.1 state jump to normal	 	14
#CQL supported versions: 2.0.0,3.2.0 (default: 3.2.0)	 	14
#Cassandra version: 2.1.5.469	


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
