.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

Virtual Tables
--------------

Apache Cassandra 4.0 implements virtual tables (`CASSANDRA-7622
<https://issues.apache.org/jira/browse/CASSANDRA-7622>`_).

Definition
^^^^^^^^^^

A virtual table is a table that is backed by an API instead of data explicitly managed and stored as SSTables. Apache Cassandra 4.0 implements a virtual keyspace interface for virtual tables. Virtual tables are specific to each node. 

Objective
^^^^^^^^^

A virtual table could have several uses including:

- Expose metrics through CQL
- Expose YAML configuration information
 
How  are Virtual Tables different from regular tables?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Virtual tables and virtual keyspaces are quite different from regular tables and keyspaces respectively such as:

- Virtual tables are read-only, but it is likely to change
- Virtual tables are not replicated
- Virtual tables are local only and non distributed
- Virtual tables have no associated SSTables
- Consistency level of the queries sent virtual tables are ignored
- Virtual tables are managed by Cassandra and a user cannot run  DDL to create new virtual tables or DML to modify existing virtual       tables
- Virtual tables are created in special keyspaces and not just any keyspace
- All existing virtual tables use LocalPartitioner. Since a virtual table is not replicated the partitioner sorts in order of partition   keys instead of by their hash.
- Making advanced queries with ALLOW FILTERING and aggregation functions may be used with virtual tables even though in normal tables we   dont recommend it

Virtual Keyspaces
^^^^^^^^^^^^^^^^^

Apache Cassandra 4.0 has added two new keyspaces for virtual tables: ``system_virtual_schema`` and ``system_views``. Run the following command to list the keyspaces.

::

 cqlsh> DESC KEYSPACES;
 system_schema  system       system_distributed  system_virtual_schema
 system_auth      system_traces       system_views

The ``system_virtual_schema keyspace`` contains schema information on virtual tables. The ``system_views`` keyspace contains the actual virtual tables. 

Virtual Table Limitations
^^^^^^^^^^^^^^^^^^^^^^^^^

Virtual tables and virtual keyspaces have some limitations initially though some of these could change such as:

- Cannot alter or drop virtual keyspaces or tables
- Cannot truncate virtual tables
- Expiring columns are not supported by virtual tables
- Conditional updates are not supported by virtual tables
- Cannot create tables in virtual keyspaces
- Cannot perform any operations against virtual keyspace
- Secondary indexes are not supported on virtual tables
- Cannot create functions in virtual keyspaces
- Cannot create types in virtual keyspaces
- Materialized views are not supported on virtual tables
- Virtual tables don't support DELETE statements
- Cannot CREATE TRIGGER against a virtual table
- Conditional BATCH statements cannot include mutations for virtual tables
- Cannot include a virtual table statement in a logged batch
- Mutations for virtual and regular tables cannot exist in the same batch
- Conditional BATCH statements cannot include mutations for virtual tables
- Cannot create aggregates in virtual keyspaces

Listing and Describing Virtual Tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Virtual tables in a virtual keyspace may be listed with ``DESC TABLES``.  The ``system_views`` virtual keyspace tables include the following:

::

 cqlsh> USE system_views;
 cqlsh:system_views> DESC TABLES;
 coordinator_scans   clients             tombstones_scanned  internode_inbound
 disk_usage          sstable_tasks       live_scanned        caches           
 local_writes        max_partition_size  local_reads       
 coordinator_writes  internode_outbound  thread_pools      
 local_scans         coordinator_reads   settings  

Some of the salient virtual tables in system_views virtual keyspace are described in Table 1.

Table 1 : Virtual Tables in system_views

+------------------+---------------------------------------------------+
|Virtual Table     | Description                                       | 
+------------------+---------------------------------------------------+
| clients          |Lists information about all connected clients.     |           
+------------------+---------------------------------------------------+
| disk_usage       |Disk usage including disk_space, keyspace_name,    |
|                  |and table_name by system keyspaces.                |
+------------------+---------------------------------------------------+
| local_writes     |A table metric for local writes                    |
|                  |including count, keyspace_name,                    | 
|                  |max, median, per_second, and                       |
|                  |table_name.                                        |                                                                         
+------------------+---------------------------------------------------+
| caches           |Displays the general cache information including   |
|                  |cache name, capacity_bytes, entry_count, hit_count,| 
|                  |hit_ratio double, recent_hit_rate_per_second,      |
|                  |recent_request_rate_per_second, request_count, and | 
|                  |size_bytes.                                        |                                                                         
+------------------+---------------------------------------------------+
| local_reads      |A table metric for  local reads information.       |                                                                   
+------------------+---------------------------------------------------+
| sstable_tasks    |Lists currently running tasks such as compactions  |
|                  |and upgrades on SSTables.                          |
+------------------+---------------------------------------------------+
|internode_inbound |Lists information about the inbound                | 
|                  |internode messaging.                               |                
+------------------+---------------------------------------------------+
| thread_pools     |Lists metrics for each thread pool.                |                                                                        
+------------------+---------------------------------------------------+
| settings         |Displays configuration settings in cassandra.yaml. |                                                                         
+------------------+---------------------------------------------------+
|max_partition_size|A table metric for maximum partition size.         |                                                                       
+------------------+---------------------------------------------------+
|internode_outbound|Information about the outbound internode messaging.|
|                  |                                                   |                                          
+------------------+---------------------------------------------------+
 
We shall discuss some of the virtual tables in more detail next.

Clients Virtual Table
*********************

The ``clients`` virtual table lists all active connections (connected clients) including their ip address, port, connection stage, driver name, driver version, hostname, protocol version, request count, ssl enabled, ssl protocol and user name.  

::

 cqlsh:system_views> select * from system_views.clients;
  address   | port  | connection_stage | driver_name | driver_version | hostname  | protocol_version | request_count | ssl_cipher_suite | ssl_enabled | ssl_protocol | username
 -----------+-------+------------------+-------------+----------------+-----------+------------------+---------------+------------------+-------------+--------------+-----------
  127.0.0.1 | 50628 |            ready |        null |           null | localhost |                4 |            55 |             null |       False |         null | anonymous
  127.0.0.1 | 50630 |            ready |        null |           null | localhost |                4 |            70 |             null |       False |         null | anonymous

 (2 rows)

Some examples of how ``clients`` can be used are:

- To find applications using old incompatible versions of   drivers before upgrading and with nodetool   enableoldprotocolversions and nodetool   disableoldprotocolversions during upgrades.
- To identify clients sending too many requests.
- To find if SSL is enabled during the migration to and from   ssl.


The virtual tables may be described with ``DESCRIBE`` statement. The DDL listed however cannot be run to create a virtual table. As an example describe the ``system_views.clients`` virtual table.

::

  cqlsh:system_views> DESC TABLE system_views.clients;
 CREATE TABLE system_views.clients (
    address inet,
    connection_stage text,
    driver_name text,
    driver_version text,
    hostname text,
    port int,
    protocol_version int,
    request_count bigint,
    ssl_cipher_suite text,
    ssl_enabled boolean,
    ssl_protocol text,
    username text,
    PRIMARY KEY (address, port)) WITH CLUSTERING ORDER BY (port ASC)
    AND compaction = {'class': 'None'}
    AND compression = {};

Caches Virtual Table
********************
The ``caches`` virtual table lists information about the  caches. The four caches presently created are chunks, counters, keys and rows. A query on the ``caches`` virtual table returns the following details.

::

 cqlsh:system_views> SELECT * FROM system_views.caches;
 name     | capacity_bytes | entry_count | hit_count | hit_ratio | recent_hit_rate_per_second | recent_request_rate_per_second | request_count | size_bytes
 ---------+----------------+-------------+-----------+-----------+----------------------------+--------------------------------+---------------+------------
   chunks |      229638144 |          29 |       166 |      0.83 |                          5 |                              6 |           200 |     475136
 counters |       26214400 |           0 |         0 |       NaN |                          0 |                              0 |             0 |          0
     keys |       52428800 |          14 |       124 |  0.873239 |                          4 |                              4 |           142 |       1248
     rows |              0 |           0 |         0 |       NaN |                          0 |                              0 |             0 |          0

 (4 rows)

Settings Virtual Table
**********************
The ``settings table`` is rather useful and lists all the current configuration settings from the ``cassandra.yaml``.  The encryption options are overridden to hide the sensitive truststore information or passwords.  The configuration settings however cannot be set using DML  on the virtual table presently. 
::

 cqlsh:system_views> SELECT * FROM system_views.settings;

 name                                                                   | value
 -----------------------------------------------------------------------+-------------------- 
   allocate_tokens_for_keyspace                                         |        null                                                                                          
   audit_logging_options_audit_logs_dir                                 |  
 /home/ec2-user/cassandra/logs/audit/                                                                                                       
                                          audit_logging_options_enabled |                                                                                                                                         
 false
                              audit_logging_options_excluded_categories |                                                                                                                                              
                               audit_logging_options_excluded_keyspaces |                                                                                                    
 system,system_schema,system_virtual_schema
                                   audit_logging_options_excluded_users |                                                                                                                                              
                              audit_logging_options_included_categories |                                                                                                                                              
                               audit_logging_options_included_keyspaces |                                                                                                                                              
                                   audit_logging_options_included_users |                                                                                                                                              
                                           audit_logging_options_logger |                                                                                                                                
 BinAuditLogger
                                                          authenticator |                                                                                                                         
 AllowAllAuthenticator
                                                             authorizer |                                                                                                                            
 AllowAllAuthorizer
                                                         auto_bootstrap |                                                                                                                                          
 true
                                                          auto_snapshot |                                                                                                                                          
 true
                                              automatic_sstable_upgrade |                                                                                                                                         
 false
                                                  back_pressure_enabled |                                                                                                                                         
 false
                                                 back_pressure_strategy |                                                           
 org.apache.cassandra.net.RateBasedBackPressure{high_ratio=0.9, factor=5, flow=FAST}
                                                      broadcast_address |                                                                                                                                          
 null
                                                  broadcast_rpc_address |                                                                                                                                          
 null
                                                           cluster_name |                                                                                                                                  
 Test Cluster
                                          column_index_cache_size_in_kb |                                                                                                                                             
 2
                                                column_index_size_in_kb |                                                                                                                                            
 64
                                                  commit_failure_policy |                                                                                                                                          
 stop
                                                  commitlog_compression |                                                                                                                                          
 null
                                                    commitlog_directory |                                                                                                       
 /home/ec2-user/cassandra/data/commitlog
                              commitlog_max_compression_buffers_in_pool |                                                                                                                                             
 3
                                          commitlog_periodic_queue_size |                                                                                                                                            
 -1
                                           commitlog_segment_size_in_mb |                                                                                                                                            
 32
                                                         commitlog_sync |                                                                                                                                      
 periodic
                                      commitlog_sync_batch_window_in_ms |                                                                                                                                           
 NaN
                                      commitlog_sync_group_window_in_ms |                                                                                                                                           
 NaN
                                            commitlog_sync_period_in_ms |                                                                                                                                         
 10000
                                            commitlog_total_space_in_mb |                                                                                                                                          
 2556
                        compaction_large_partition_warning_threshold_mb |                                                                                                                                           
 100
                                       compaction_throughput_mb_per_sec |                                                                                                                                            
 16
                                                  concurrent_compactors |                                                                                                                                             
 2
                                              concurrent_counter_writes |                                                                                                                                            
 32
                                  concurrent_materialized_view_builders |                                                                                                                                             
 1
                                    concurrent_materialized_view_writes |                                                                                                                                            
 32
                                                       concurrent_reads |                                                                                                                                            
 32
                                                  concurrent_replicates |                                                                                                                                          
 null
                                                 concurrent_validations |                                                                                                                                    
 2147483647
                                                      concurrent_writes |                                                                                                                                            
 32
                                      credentials_update_interval_in_ms |                                                                                                                                            
 -1
                                             credentials_validity_in_ms |                                                                                                                                          
 2000
                                                     cross_node_timeout |                                                                                                                                         
 false
                                                  data_file_directories |                                                                                                          
 [/home/ec2-user/cassandra/data/data]
                                              diagnostic_events_enabled |                                                                                                                                         
 false
                                                       disk_access_mode |                                                                                                                                          
 mmap
                                                    disk_failure_policy |                                                                                                                                          
 stop
                                              enable_materialized_views |                                                                                                                                         
 false
                                                    enable_sasi_indexes |                                                                                                                                         
 false
                                 enable_scripted_user_defined_functions |                                                                                                                                         
 false
                                           enable_transient_replication |                                                                                                                                         
 false
                                          enable_user_defined_functions |                                                                                                                                         
 false
                                  enable_user_defined_functions_threads |                                                                                                                                          
 true
                                                        endpoint_snitch |                                                                                                                                  
 SimpleSnitch
                                                    file_cache_round_up |                                                                                                                                         
 false
                                                  file_cache_size_in_mb |                                                                                                                                           
 251
                                             ...
                                             ...
                                                 gc_log_threshold_in_ms |                                                                                                                                           
 200
                                                gc_warn_threshold_in_ms |                                                                                                                                          
 1000
                                    hinted_handoff_disabled_datacenters |                                                                                                                                            
 []
                                                 hinted_handoff_enabled |                                                                                                                                          
 true
                                          hinted_handoff_throttle_in_kb |                                                                                                                                          
 1024
                                                      hints_compression |                                                                                                                                          
 null
                                                        hints_directory |                                                                                                           
 /home/ec2-user/cassandra/data/hints
                                               hints_flush_period_in_ms |                                                                                                                                         
 10000
                                                ideal_consistency_level |                                                                                                                                          
 null
                                                    incremental_backups |                                                                                                                                         
 false
                                                          initial_token |                                                                                                                                          
 null
                   inter_dc_stream_throughput_outbound_megabits_per_sec |                                                                                                                                           
 200
                                                   inter_dc_tcp_nodelay |                                                                                                                                         
 false
                  ...
                  ...
                                           internode_authenticator |                                                                  
 null
                                             internode_compression |                                                                    
 dc
                                                    listen_address |                                                             
 localhost
                                                  listen_interface |                                                                  
 null
                                       listen_on_broadcast_address |                                                                 
 false
                         max_concurrent_automatic_sstable_upgrades |                                                                     
 1
                                             max_hint_window_in_ms |                                                              
 10800000
                                          memtable_allocation_type |                                                          
 heap_buffers
                                    min_free_space_per_drive_in_mb |                                                                    
 50
                            native_transport_allow_older_protocols |                                                                  
 true
                          native_transport_flush_in_batches_legacy |                                                                 
 false
                           native_transport_frame_block_size_in_kb |                                                                    32
                               native_transport_idle_timeout_in_ms |                                                                     0
                       native_transport_max_concurrent_connections |                                                                    -1
                native_transport_max_concurrent_connections_per_ip |                                                                    -1
                 native_transport_max_concurrent_requests_in_bytes |                                                             105277030
          native_transport_max_concurrent_requests_in_bytes_per_ip |                                                              26319257
                             native_transport_max_frame_size_in_mb |                                                                   256
                                      native_transport_max_threads |                                                                   128
                                             native_transport_port |                                                                  9042
                                         native_transport_port_ssl |                                                                  null
                                                network_authorizer |                                             AllowAllNetworkAuthorizer
                                                        num_tokens |                                                                   256
                                                       ...
                                                       ...
                                 prepared_statements_cache_size_mb |                                                                  null
                                       range_request_timeout_in_ms |                                                                 10000
                                        read_request_timeout_in_ms |                                                                  5000
                                 repair_command_pool_full_strategy |                                                                 queue
                                          repair_command_pool_size |                                                            2147483647
                                     repair_session_max_tree_depth |                                                                    20
                                        repair_session_space_in_mb |                                                                    62
                repaired_data_tracking_for_partition_reads_enabled |                                                                 false
                    repaired_data_tracking_for_range_reads_enabled |                                                                 false
                       report_unconfirmed_repaired_data_mismatches |                                                                 false
                                             request_timeout_in_ms |                                                                 10000
                                                      role_manager |                                                  CassandraRoleManager
                                                       rpc_address |                                                             localhost
                                                     rpc_interface |                                                                  null
                                            ...
                                            ...
                               server_encryption_options_algorithm |                                                                  null
                                server_encryption_options_protocol |                                                                   TLS
                                      slow_query_log_timeout_in_ms |                                                                   500
                                                  ssl_storage_port |                                                                  7001
                                            start_native_transport |                                                                  true
                                                      storage_port |                                                                  7000
                                            stream_entire_sstables |                                                                  true
               transparent_data_encryption_options_chunk_length_kb |                                                                    64
                        transparent_data_encryption_options_cipher |                                                  AES/CBC/PKCS5Padding
                       transparent_data_encryption_options_enabled |                                                                 false
                     transparent_data_encryption_options_iv_length |                                                                    
 16
 (224 rows)


The ``settings`` table can be really useful if yaml file has been changed since startup and dont know running configuration, or to find if they have been modified via jmx/nodetool or virtual tables.


Thread Pools Virtual Table
**************************

The ``thread_pools`` table lists information about all thread pools. Thread pool information includes active tasks, active tasks limit, blocked tasks, blocked tasks all time,  completed tasks, and pending tasks. A query on the ``thread_pools`` returns following details.

::

 cqlsh:system_views> select * from system_views.thread_pools;

 name                         | active_tasks | active_tasks_limit | blocked_tasks | blocked_tasks_all_time | completed_tasks | pending_tasks
 ------------------------------+--------------+--------------------+---------------+------------------------+-----------------+---------------
             AntiEntropyStage |            0 |                  1 |             0 |                      0 |               0 |             0
         CacheCleanupExecutor |            0 |                  1 |             0 |                      0 |               0 |             0
           CompactionExecutor |            0 |                  2 |             0 |                      0 |             881 |             0
         CounterMutationStage |            0 |                 32 |             0 |                      0 |               0 |             0
                  GossipStage |            0 |                  1 |             0 |                      0 |               0 |             0
              HintsDispatcher |            0 |                  2 |             0 |                      0 |               0 |             0
        InternalResponseStage |            0 |                  2 |             0 |                      0 |               0 |             0
          MemtableFlushWriter |            0 |                  2 |             0 |                      0 |               1 |             0
            MemtablePostFlush |            0 |                  1 |             0 |                      0 |               2 |             0
        MemtableReclaimMemory |            0 |                  1 |             0 |                      0 |               1 |             0
               MigrationStage |            0 |                  1 |             0 |                      0 |               0 |             0
                    MiscStage |            0 |                  1 |             0 |                      0 |               0 |             0
                MutationStage |            0 |                 32 |             0 |                      0 |               0 |             0
    Native-Transport-Requests |            1 |                128 |             0 |                      0 |             130 |             0
       PendingRangeCalculator |            0 |                  1 |             0 |                      0 |               1 |             0
 PerDiskMemtableFlushWriter_0 |            0 |                  2 |             0 |                      0 |               1 |             0
                    ReadStage |            0 |                 32 |             0 |                      0 |              13 |             0
                  Repair-Task |            0 |         2147483647 |             0 |                      0 |               0 |             0
         RequestResponseStage |            0 |                  2 |             0 |                      0 |               0 |             0
                      Sampler |            0 |                  1 |             0 |                      0 |               0 |             0
     SecondaryIndexManagement |            0 |                  1 |             0 |                      0 |               0 |             0
           ValidationExecutor |            0 |         2147483647 |             0 |                      0 |               0 |             0
            ViewBuildExecutor |            0 |                  1 |             0 |                      0 |               0 |             0
            ViewMutationStage |            0 |                 32 |             0 |                      0 |               0 |             0

(24 rows)

Internode Inbound Messaging Virtual Table
*****************************************

The ``internode_inbound``  virtual table is for the internode inbound messaging. Initially no internode inbound messaging may get listed. In addition to the address, port, datacenter and rack information includes  corrupt frames recovered, corrupt frames unrecovered, error bytes, error count, expired bytes, expired count, processed bytes, processed count, received bytes, received count, scheduled bytes, scheduled count, throttled count, throttled nanos, using bytes, using reserve bytes. A query on the ``internode_inbound`` returns following details.

::

 cqlsh:system_views> SELECT * FROM system_views.internode_inbound;
 address | port | dc | rack | corrupt_frames_recovered | corrupt_frames_unrecovered | 
 error_bytes | error_count | expired_bytes | expired_count | processed_bytes | 
 processed_count | received_bytes | received_count | scheduled_bytes | scheduled_count | throttled_count | throttled_nanos | using_bytes | using_reserve_bytes
 ---------+------+----+------+--------------------------+----------------------------+- 
 ----------
 (0 rows)

SSTables Tasks Virtual Table
****************************

The ``sstable_tasks`` could be used to get information about running tasks. It lists following columns.   

::

 cqlsh:system_views> SELECT * FROM system_views.sstable_tasks;
 keyspace_name | table_name | task_id | kind | progress | total | unit
 ---------------+------------+---------+------+----------+-------+------

 
As an example a query of ``total-progress`` gives the remaining time for a task.

Other Virtual Tables
********************

Some examples of using other virtual tables are as follows.

::

  cqlsh> SELECT * FROM disk_usage WHERE mebibytes > 1 ALLOW FILTERING;

  keyspace_name | table_name | mebibytes
  ---------------+------------+-----------
     keyspace1 |  standard1 |       288
    tlp_stress |   keyvalue |      3211

  cqlsh> SELECT * FROM local_read_latency WHERE per_second > 1 ALLOW FILTERING;

  keyspace_name | table_name | p50th_ms | p99th_ms | count    | max_ms  | per_second
  ---------------+------------+----------+----------+----------+---------+------------
    tlp_stress |   keyvalue |    0.043 |    0.152 | 49785158 | 186.563 |  11418.356
 

The system_virtual_schema keyspace
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``system_virtual_schema`` keyspace has three tables: ``keyspaces``,  ``columns`` and  ``tables`` for the virtual keyspace definitions, virtual table definitions, and virtual column definitions  respectively. It is used by Cassandra internally and a user would not need to access it directly. 
