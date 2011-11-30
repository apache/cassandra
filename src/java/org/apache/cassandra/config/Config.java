package org.apache.cassandra.config;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.util.List;


public class Config
{
    public String cluster_name = "Test Cluster";
    public String authenticator;
    public String authority;
    
    /* Hashing strategy Random or OPHF */
    public String partitioner;
    
    public Boolean auto_bootstrap = true;
    public Boolean hinted_handoff_enabled = true;
    public Integer max_hint_window_in_ms = Integer.MAX_VALUE;
    
    public SeedProviderDef seed_provider;
    public DiskAccessMode disk_access_mode = DiskAccessMode.auto;
    
    /* Address where to run the job tracker */
    public String job_tracker_host;
    
    /* Job Jar Location */
    public String job_jar_file_location;
    
    /* initial token in the ring */
    public String initial_token;
    
    public Long rpc_timeout_in_ms = new Long(2000);

    public Integer phi_convict_threshold = 8;
    
    public Integer concurrent_reads = 8;
    public Integer concurrent_writes = 32;
    public Integer concurrent_replicates = 32;
    
    public Integer memtable_flush_writers = null; // will get set to the length of data dirs in DatabaseDescriptor
    public Integer memtable_total_space_in_mb;

    public Integer sliced_buffer_size_in_kb = 64;
    
    public Integer storage_port = 7000;
    public Integer ssl_storage_port = 7001;
    public String listen_address;
    public String broadcast_address;
    
    public String rpc_address;
    public Integer rpc_port = 9160;
    public String rpc_server_type = "sync";
    public Boolean rpc_keepalive = true;
    public Integer rpc_min_threads = null;
    public Integer rpc_max_threads = null;
    public Integer rpc_send_buff_size_in_bytes;
    public Integer rpc_recv_buff_size_in_bytes;

    public Integer thrift_max_message_length_in_mb = 16;
    public Integer thrift_framed_transport_size_in_mb = 15;
    public Boolean snapshot_before_compaction = false;

    /* if the size of columns or super-columns are more than this, indexing will kick in */
    public Integer column_index_size_in_kb = 64;
    public Integer in_memory_compaction_limit_in_mb = 256;
    public Integer concurrent_compactors = Runtime.getRuntime().availableProcessors();
    public Integer compaction_throughput_mb_per_sec = 16;
    public Boolean multithreaded_compaction = false;

    public Integer stream_throughput_outbound_megabits_per_sec;

    public String[] data_file_directories;

    public String saved_caches_directory;

    // Commit Log
    public String commitlog_directory;
    public Integer commitlog_total_space_in_mb = 4096;
    public CommitLogSync commitlog_sync;
    public Double commitlog_sync_batch_window_in_ms;
    public Integer commitlog_sync_period_in_ms;
    
    public String endpoint_snitch;
    public Boolean dynamic_snitch = true;
    public Integer dynamic_snitch_update_interval_in_ms = 100;
    public Integer dynamic_snitch_reset_interval_in_ms = 600000;
    public Double dynamic_snitch_badness_threshold = 0.1;

    public String request_scheduler;
    public RequestSchedulerId request_scheduler_id;
    public RequestSchedulerOptions request_scheduler_options;

    public EncryptionOptions encryption_options = new EncryptionOptions();

    public Integer index_interval = 128;

    public Double flush_largest_memtables_at = 1.0;
    public Double reduce_cache_sizes_at = 1.0;
    public double reduce_cache_capacity_to = 0.6;
    public int hinted_handoff_throttle_delay_in_ms = 0;
    public boolean compaction_preheat_key_cache = true;

    public boolean incremental_backups = false;
    public int memtable_flush_queue_size = 4;

    public static enum CommitLogSync {
        periodic,
        batch
    }
    
    public static enum DiskAccessMode {
        auto,
        mmap,
        mmap_index_only,
        standard,
    }
    
    public static enum RequestSchedulerId
    {
        keyspace
    }
}
