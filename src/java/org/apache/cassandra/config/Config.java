package org.apache.cassandra.config;
import java.util.List;


public class Config {
    public String cluster_name = "Test Cluster";
    public String authenticator;
    
    /* Hashing strategy Random or OPHF */
    public String partitioner;
    
    public Boolean auto_bootstrap = false;
    public Boolean hinted_handoff_enabled = true;
    
    public String[] seeds;
    public DiskAccessMode disk_access_mode = DiskAccessMode.auto;
    
    /* Address where to run the job tracker */
    public String job_tracker_host;
    
    /* Job Jar Location */
    public String job_jar_file_location;
    
    /* time to wait before garbage collecting tombstones (deletion markers) */
    public Integer gc_grace_seconds = 10 * 24 * 3600; // 10 days
    
    /* initial token in the ring */
    public String initial_token;
    
    public Long rpc_timeout_in_ms = new Long(2000);

    public Integer phi_convict_threshold = 8;
    
    public Integer concurrent_reads = 8;
    public Integer concurrent_writes = 32;
    
    
    public Double flush_data_buffer_size_in_mb = new Double(32);
    public Double flush_index_buffer_size_in_mb = new Double(8);
    
    public Integer sliced_buffer_size_in_kb = 64;
    
    public Integer storage_port = 7000;
    public String listen_address;
    
    public String rpc_address;
    public Integer rpc_port = 9160;
    public Boolean thrift_framed_transport = false;
    public Boolean snapshot_before_compaction = false;
    
    public Integer binary_memtable_throughput_in_mb = 256;
    /* Number of minutes to keep a memtable in memory */
    public Integer memtable_flush_after_mins = 60 * 60 * 1000;
    /* Size of the memtable in memory before it is dumped */
    public Integer memtable_throughput_in_mb = 64;
    /* Number of objects in millions in the memtable before it is dumped */
    public Double memtable_operations_in_millions = 0.1;
    
    /* if the size of columns or super-columns are more than this, indexing will kick in */
    public Integer column_index_size_in_kb = 64;
    public Long row_warning_threshold_in_mb = new Long(512);
    
    public String[] data_file_directories;
    
    // Commit Log
    public String commitlog_directory;
    public Integer commitlog_rotation_threshold_in_mb;
    public CommitLogSync commitlog_sync;
    public Double commitlog_sync_batch_window_in_ms;
    public Integer commitlog_sync_period_in_ms;
    
    public String endpoint_snitch;
    
    public List<Keyspace> keyspaces;
    
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
    
}
