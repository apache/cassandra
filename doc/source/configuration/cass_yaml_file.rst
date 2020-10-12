.. _cassandra-yaml:

cassandra.yaml file configuration 
=================================

``cluster_name``
----------------
The name of the cluster. This is mainly used to prevent machines in
one logical cluster from joining another.

*Default Value:* 'Test Cluster'

``num_tokens``
--------------

This defines the number of tokens randomly assigned to this node on the ring
The more tokens, relative to other nodes, the larger the proportion of data
that this node will store. We recommend all nodes to have the same number
of tokens assuming they have equal hardware capability.

If you leave this unspecified, Cassandra will use the default of 1 token for legacy compatibility,
and will use the initial_token as described below.

Specifying initial_token will override this setting on the node's initial start,
on subsequent starts, this setting will apply even if initial token is set.

We recommend setting ``allocate_tokens_for_local_replication_factor`` in conjunction with this setting to ensure even allocation.

*Default Value:* 256

``allocate_tokens_for_keyspace``
--------------------------------
*This option is commented out by default.*

Triggers automatic allocation of num_tokens tokens for this node. The allocation
algorithm attempts to choose tokens in a way that optimizes replicated load over
the nodes in the datacenter for the replica factor.

The load assigned to each node will be close to proportional to its number of
vnodes.

Only supported with the Murmur3Partitioner.

Replica factor is determined via the replication strategy used by the specified
keyspace.

We recommend using the ``allocate_tokens_for_local_replication_factor`` setting instead for operational simplicity.

*Default Value:* KEYSPACE

``allocate_tokens_for_local_replication_factor``
------------------------------------------------
*This option is commented out by default.*

Tokens will be allocated based on this replication factor, regardless of keyspace or datacenter.

*Default Value:* 3

``initial_token``
-----------------
*This option is commented out by default.*

initial_token allows you to specify tokens manually.  While you can use it with
vnodes (num_tokens > 1, above) -- in which case you should provide a 
comma-separated list -- it's primarily used when adding nodes to legacy clusters 
that do not have vnodes enabled.

``hinted_handoff_enabled``
--------------------------

May either be "true" or "false" to enable globally

*Default Value:* true

``hinted_handoff_disabled_datacenters``
---------------------------------------
*This option is commented out by default.*

When hinted_handoff_enabled is true, a black list of data centers that will not
perform hinted handoff

*Default Value (complex option)*::

    #    - DC1
    #    - DC2

``max_hint_window_in_ms``
-------------------------
This defines the maximum amount of time a dead host will have hints
generated.  After it has been dead this long, new hints for it will not be
created until it has been seen alive and gone down again.

*Default Value:* 10800000 # 3 hours

``hinted_handoff_throttle_in_kb``
---------------------------------

Maximum throttle in KBs per second, per delivery thread.  This will be
reduced proportionally to the number of nodes in the cluster.  (If there
are two nodes in the cluster, each delivery thread will use the maximum
rate; if there are three, each will throttle to half of the maximum,
since we expect two nodes to be delivering hints simultaneously.)

*Default Value:* 1024

``max_hints_delivery_threads``
------------------------------

Number of threads with which to deliver hints;
Consider increasing this number when you have multi-dc deployments, since
cross-dc handoff tends to be slower

*Default Value:* 2

``hints_directory``
-------------------
*This option is commented out by default.*

Directory where Cassandra should store hints.
If not set, the default directory is $CASSANDRA_HOME/data/hints.

*Default Value:*  /var/lib/cassandra/hints

``hints_flush_period_in_ms``
----------------------------

How often hints should be flushed from the internal buffers to disk.
Will *not* trigger fsync.

*Default Value:* 10000

``max_hints_file_size_in_mb``
-----------------------------

Maximum size for a single hints file, in megabytes.

*Default Value:* 128

``hints_compression``
---------------------
*This option is commented out by default.*

Compression to apply to the hint files. If omitted, hints files
will be written uncompressed. LZ4, Snappy, and Deflate compressors
are supported.

*Default Value (complex option)*::

    #   - class_name: LZ4Compressor
    #     parameters:
    #         -

``batchlog_replay_throttle_in_kb``
----------------------------------
Maximum throttle in KBs per second, total. This will be
reduced proportionally to the number of nodes in the cluster.

*Default Value:* 1024

``authenticator``
-----------------

Authentication backend, implementing IAuthenticator; used to identify users
Out of the box, Cassandra provides org.apache.cassandra.auth.{AllowAllAuthenticator,
PasswordAuthenticator}.

- AllowAllAuthenticator performs no checks - set it to disable authentication.
- PasswordAuthenticator relies on username/password pairs to authenticate
  users. It keeps usernames and hashed passwords in system_auth.roles table.
  Please increase system_auth keyspace replication factor if you use this authenticator.
  If using PasswordAuthenticator, CassandraRoleManager must also be used (see below)

*Default Value:* AllowAllAuthenticator

``authorizer``
--------------

Authorization backend, implementing IAuthorizer; used to limit access/provide permissions
Out of the box, Cassandra provides org.apache.cassandra.auth.{AllowAllAuthorizer,
CassandraAuthorizer}.

- AllowAllAuthorizer allows any action to any user - set it to disable authorization.
- CassandraAuthorizer stores permissions in system_auth.role_permissions table. Please
  increase system_auth keyspace replication factor if you use this authorizer.

*Default Value:* AllowAllAuthorizer

``role_manager``
----------------

Part of the Authentication & Authorization backend, implementing IRoleManager; used
to maintain grants and memberships between roles.
Out of the box, Cassandra provides org.apache.cassandra.auth.CassandraRoleManager,
which stores role information in the system_auth keyspace. Most functions of the
IRoleManager require an authenticated login, so unless the configured IAuthenticator
actually implements authentication, most of this functionality will be unavailable.

- CassandraRoleManager stores role data in the system_auth keyspace. Please
  increase system_auth keyspace replication factor if you use this role manager.

*Default Value:* CassandraRoleManager

``network_authorizer``
----------------------

Network authorization backend, implementing INetworkAuthorizer; used to restrict user
access to certain DCs
Out of the box, Cassandra provides org.apache.cassandra.auth.{AllowAllNetworkAuthorizer,
CassandraNetworkAuthorizer}.

- AllowAllNetworkAuthorizer allows access to any DC to any user - set it to disable authorization.
- CassandraNetworkAuthorizer stores permissions in system_auth.network_permissions table. Please
  increase system_auth keyspace replication factor if you use this authorizer.

*Default Value:* AllowAllNetworkAuthorizer

``roles_validity_in_ms``
------------------------

Validity period for roles cache (fetching granted roles can be an expensive
operation depending on the role manager, CassandraRoleManager is one example)
Granted roles are cached for authenticated sessions in AuthenticatedUser and
after the period specified here, become eligible for (async) reload.
Defaults to 2000, set to 0 to disable caching entirely.
Will be disabled automatically for AllowAllAuthenticator.

*Default Value:* 2000

``roles_update_interval_in_ms``
-------------------------------
*This option is commented out by default.*

Refresh interval for roles cache (if enabled).
After this interval, cache entries become eligible for refresh. Upon next
access, an async reload is scheduled and the old value returned until it
completes. If roles_validity_in_ms is non-zero, then this must be
also.
Defaults to the same value as roles_validity_in_ms.

*Default Value:* 2000

``permissions_validity_in_ms``
------------------------------

Validity period for permissions cache (fetching permissions can be an
expensive operation depending on the authorizer, CassandraAuthorizer is
one example). Defaults to 2000, set to 0 to disable.
Will be disabled automatically for AllowAllAuthorizer.

*Default Value:* 2000

``permissions_update_interval_in_ms``
-------------------------------------
*This option is commented out by default.*

Refresh interval for permissions cache (if enabled).
After this interval, cache entries become eligible for refresh. Upon next
access, an async reload is scheduled and the old value returned until it
completes. If permissions_validity_in_ms is non-zero, then this must be
also.
Defaults to the same value as permissions_validity_in_ms.

*Default Value:* 2000

``credentials_validity_in_ms``
------------------------------

Validity period for credentials cache. This cache is tightly coupled to
the provided PasswordAuthenticator implementation of IAuthenticator. If
another IAuthenticator implementation is configured, this cache will not
be automatically used and so the following settings will have no effect.
Please note, credentials are cached in their encrypted form, so while
activating this cache may reduce the number of queries made to the
underlying table, it may not  bring a significant reduction in the
latency of individual authentication attempts.
Defaults to 2000, set to 0 to disable credentials caching.

*Default Value:* 2000

``credentials_update_interval_in_ms``
-------------------------------------
*This option is commented out by default.*

Refresh interval for credentials cache (if enabled).
After this interval, cache entries become eligible for refresh. Upon next
access, an async reload is scheduled and the old value returned until it
completes. If credentials_validity_in_ms is non-zero, then this must be
also.
Defaults to the same value as credentials_validity_in_ms.

*Default Value:* 2000

``partitioner``
---------------

The partitioner is responsible for distributing groups of rows (by
partition key) across nodes in the cluster. The partitioner can NOT be
changed without reloading all data.  If you are adding nodes or upgrading,
you should set this to the same partitioner that you are currently using.

The default partitioner is the Murmur3Partitioner. Older partitioners
such as the RandomPartitioner, ByteOrderedPartitioner, and
OrderPreservingPartitioner have been included for backward compatibility only.
For new clusters, you should NOT change this value.


*Default Value:* org.apache.cassandra.dht.Murmur3Partitioner

``data_file_directories``
-------------------------
*This option is commented out by default.*

Directories where Cassandra should store data on disk. If multiple
directories are specified, Cassandra will spread data evenly across 
them by partitioning the token ranges.
If not set, the default directory is $CASSANDRA_HOME/data/data.

*Default Value (complex option)*::

    #     - /var/lib/cassandra/data

``commitlog_directory``
-----------------------
*This option is commented out by default.*
commit log.  when running on magnetic HDD, this should be a
separate spindle than the data directories.
If not set, the default directory is $CASSANDRA_HOME/data/commitlog.

*Default Value:*  /var/lib/cassandra/commitlog

``cdc_enabled``
---------------

Enable / disable CDC functionality on a per-node basis. This modifies the logic used
for write path allocation rejection (standard: never reject. cdc: reject Mutation
containing a CDC-enabled table if at space limit in cdc_raw_directory).

*Default Value:* false

``cdc_raw_directory``
---------------------
*This option is commented out by default.*

CommitLogSegments are moved to this directory on flush if cdc_enabled: true and the
segment contains mutations for a CDC-enabled table. This should be placed on a
separate spindle than the data directories. If not set, the default directory is
$CASSANDRA_HOME/data/cdc_raw.

*Default Value:*  /var/lib/cassandra/cdc_raw

``disk_failure_policy``
-----------------------

Policy for data disk failures:

die
  shut down gossip and client transports and kill the JVM for any fs errors or
  single-sstable errors, so the node can be replaced.

stop_paranoid
  shut down gossip and client transports even for single-sstable errors,
  kill the JVM for errors during startup.

stop
  shut down gossip and client transports, leaving the node effectively dead, but
  can still be inspected via JMX, kill the JVM for errors during startup.

best_effort
   stop using the failed disk and respond to requests based on
   remaining available sstables.  This means you WILL see obsolete
   data at CL.ONE!

ignore
   ignore fatal errors and let requests fail, as in pre-1.2 Cassandra

*Default Value:* stop

``commit_failure_policy``
-------------------------

Policy for commit disk failures:

die
  shut down the node and kill the JVM, so the node can be replaced.

stop
  shut down the node, leaving the node effectively dead, but
  can still be inspected via JMX.

stop_commit
  shutdown the commit log, letting writes collect but
  continuing to service reads, as in pre-2.0.5 Cassandra

ignore
  ignore fatal errors and let the batches fail

*Default Value:* stop

``prepared_statements_cache_size_mb``
-------------------------------------

Maximum size of the native protocol prepared statement cache

Valid values are either "auto" (omitting the value) or a value greater 0.

Note that specifying a too large value will result in long running GCs and possbily
out-of-memory errors. Keep the value at a small fraction of the heap.

If you constantly see "prepared statements discarded in the last minute because
cache limit reached" messages, the first step is to investigate the root cause
of these messages and check whether prepared statements are used correctly -
i.e. use bind markers for variable parts.

Do only change the default value, if you really have more prepared statements than
fit in the cache. In most cases it is not neccessary to change this value.
Constantly re-preparing statements is a performance penalty.

Default value ("auto") is 1/256th of the heap or 10MB, whichever is greater

``key_cache_size_in_mb``
------------------------

Maximum size of the key cache in memory.

Each key cache hit saves 1 seek and each row cache hit saves 2 seeks at the
minimum, sometimes more. The key cache is fairly tiny for the amount of
time it saves, so it's worthwhile to use it at large numbers.
The row cache saves even more time, but must contain the entire row,
so it is extremely space-intensive. It's best to only use the
row cache if you have hot rows or static rows.

NOTE: if you reduce the size, you may not get you hottest keys loaded on startup.

Default value is empty to make it "auto" (min(5% of Heap (in MB), 100MB)). Set to 0 to disable key cache.

``key_cache_save_period``
-------------------------

Duration in seconds after which Cassandra should
save the key cache. Caches are saved to saved_caches_directory as
specified in this configuration file.

Saved caches greatly improve cold-start speeds, and is relatively cheap in
terms of I/O for the key cache. Row cache saving is much more expensive and
has limited use.

Default is 14400 or 4 hours.

*Default Value:* 14400

``key_cache_keys_to_save``
--------------------------
*This option is commented out by default.*

Number of keys from the key cache to save
Disabled by default, meaning all keys are going to be saved

*Default Value:* 100

``row_cache_class_name``
------------------------
*This option is commented out by default.*

Row cache implementation class name. Available implementations:

org.apache.cassandra.cache.OHCProvider
  Fully off-heap row cache implementation (default).

org.apache.cassandra.cache.SerializingCacheProvider
  This is the row cache implementation availabile
  in previous releases of Cassandra.

*Default Value:* org.apache.cassandra.cache.OHCProvider

``row_cache_size_in_mb``
------------------------

Maximum size of the row cache in memory.
Please note that OHC cache implementation requires some additional off-heap memory to manage
the map structures and some in-flight memory during operations before/after cache entries can be
accounted against the cache capacity. This overhead is usually small compared to the whole capacity.
Do not specify more memory that the system can afford in the worst usual situation and leave some
headroom for OS block level cache. Do never allow your system to swap.

Default value is 0, to disable row caching.

*Default Value:* 0

``row_cache_save_period``
-------------------------

Duration in seconds after which Cassandra should save the row cache.
Caches are saved to saved_caches_directory as specified in this configuration file.

Saved caches greatly improve cold-start speeds, and is relatively cheap in
terms of I/O for the key cache. Row cache saving is much more expensive and
has limited use.

Default is 0 to disable saving the row cache.

*Default Value:* 0

``row_cache_keys_to_save``
--------------------------
*This option is commented out by default.*

Number of keys from the row cache to save.
Specify 0 (which is the default), meaning all keys are going to be saved

*Default Value:* 100

``counter_cache_size_in_mb``
----------------------------

Maximum size of the counter cache in memory.

Counter cache helps to reduce counter locks' contention for hot counter cells.
In case of RF = 1 a counter cache hit will cause Cassandra to skip the read before
write entirely. With RF > 1 a counter cache hit will still help to reduce the duration
of the lock hold, helping with hot counter cell updates, but will not allow skipping
the read entirely. Only the local (clock, count) tuple of a counter cell is kept
in memory, not the whole counter, so it's relatively cheap.

NOTE: if you reduce the size, you may not get you hottest keys loaded on startup.

Default value is empty to make it "auto" (min(2.5% of Heap (in MB), 50MB)). Set to 0 to disable counter cache.
NOTE: if you perform counter deletes and rely on low gcgs, you should disable the counter cache.

``counter_cache_save_period``
-----------------------------

Duration in seconds after which Cassandra should
save the counter cache (keys only). Caches are saved to saved_caches_directory as
specified in this configuration file.

Default is 7200 or 2 hours.

*Default Value:* 7200

``counter_cache_keys_to_save``
------------------------------
*This option is commented out by default.*

Number of keys from the counter cache to save
Disabled by default, meaning all keys are going to be saved

*Default Value:* 100

``saved_caches_directory``
--------------------------
*This option is commented out by default.*

saved caches
If not set, the default directory is $CASSANDRA_HOME/data/saved_caches.

*Default Value:*  /var/lib/cassandra/saved_caches

``commitlog_sync_batch_window_in_ms``
-------------------------------------
*This option is commented out by default.*

commitlog_sync may be either "periodic", "group", or "batch." 

When in batch mode, Cassandra won't ack writes until the commit log
has been flushed to disk.  Each incoming write will trigger the flush task.
commitlog_sync_batch_window_in_ms is a deprecated value. Previously it had
almost no value, and is being removed.


*Default Value:* 2

``commitlog_sync_group_window_in_ms``
-------------------------------------
*This option is commented out by default.*

group mode is similar to batch mode, where Cassandra will not ack writes
until the commit log has been flushed to disk. The difference is group
mode will wait up to commitlog_sync_group_window_in_ms between flushes.


*Default Value:* 1000

``commitlog_sync``
------------------

the default option is "periodic" where writes may be acked immediately
and the CommitLog is simply synced every commitlog_sync_period_in_ms
milliseconds.

*Default Value:* periodic

``commitlog_sync_period_in_ms``
-------------------------------

*Default Value:* 10000

``periodic_commitlog_sync_lag_block_in_ms``
-------------------------------------------
*This option is commented out by default.*

When in periodic commitlog mode, the number of milliseconds to block writes
while waiting for a slow disk flush to complete.

``commitlog_segment_size_in_mb``
--------------------------------

The size of the individual commitlog file segments.  A commitlog
segment may be archived, deleted, or recycled once all the data
in it (potentially from each columnfamily in the system) has been
flushed to sstables.

The default size is 32, which is almost always fine, but if you are
archiving commitlog segments (see commitlog_archiving.properties),
then you probably want a finer granularity of archiving; 8 or 16 MB
is reasonable.
Max mutation size is also configurable via max_mutation_size_in_kb setting in
cassandra.yaml. The default is half the size commitlog_segment_size_in_mb * 1024.
This should be positive and less than 2048.

NOTE: If max_mutation_size_in_kb is set explicitly then commitlog_segment_size_in_mb must
be set to at least twice the size of max_mutation_size_in_kb / 1024


*Default Value:* 32

``commitlog_compression``
-------------------------
*This option is commented out by default.*

Compression to apply to the commit log. If omitted, the commit log
will be written uncompressed.  LZ4, Snappy, and Deflate compressors
are supported.

*Default Value (complex option)*::

    #   - class_name: LZ4Compressor
    #     parameters:
    #         -

``table``
---------
*This option is commented out by default.*
Compression to apply to SSTables as they flush for compressed tables.
Note that tables without compression enabled do not respect this flag.

As high ratio compressors like LZ4HC, Zstd, and Deflate can potentially
block flushes for too long, the default is to flush with a known fast
compressor in those cases. Options are:

none : Flush without compressing blocks but while still doing checksums.
fast : Flush with a fast compressor. If the table is already using a
       fast compressor that compressor is used.

*Default Value:* Always flush with the same compressor that the table uses. This

``flush_compression``
---------------------
*This option is commented out by default.*
       was the pre 4.0 behavior.


*Default Value:* fast

``seed_provider``
-----------------

any class that implements the SeedProvider interface and has a
constructor that takes a Map<String, String> of parameters will do.

*Default Value (complex option)*::

        # Addresses of hosts that are deemed contact points. 
        # Cassandra nodes use this list of hosts to find each other and learn
        # the topology of the ring.  You must change this if you are running
        # multiple nodes!
        - class_name: org.apache.cassandra.locator.SimpleSeedProvider
          parameters:
              # seeds is actually a comma-delimited list of addresses.
              # Ex: "<ip1>,<ip2>,<ip3>"
              - seeds: "127.0.0.1:7000"

``concurrent_reads``
--------------------
For workloads with more data than can fit in memory, Cassandra's
bottleneck will be reads that need to fetch data from
disk. "concurrent_reads" should be set to (16 * number_of_drives) in
order to allow the operations to enqueue low enough in the stack
that the OS and drives can reorder them. Same applies to
"concurrent_counter_writes", since counter writes read the current
values before incrementing and writing them back.

On the other hand, since writes are almost never IO bound, the ideal
number of "concurrent_writes" is dependent on the number of cores in
your system; (8 * number_of_cores) is a good rule of thumb.

*Default Value:* 32

``concurrent_writes``
---------------------

*Default Value:* 32

``concurrent_counter_writes``
-----------------------------

*Default Value:* 32

``concurrent_materialized_view_writes``
---------------------------------------

For materialized view writes, as there is a read involved, so this should
be limited by the less of concurrent reads or concurrent writes.

*Default Value:* 32

``file_cache_size_in_mb``
-------------------------
*This option is commented out by default.*

Maximum memory to use for sstable chunk cache and buffer pooling.
32MB of this are reserved for pooling buffers, the rest is used as an
cache that holds uncompressed sstable chunks.
Defaults to the smaller of 1/4 of heap or 512MB. This pool is allocated off-heap,
so is in addition to the memory allocated for heap. The cache also has on-heap
overhead which is roughly 128 bytes per chunk (i.e. 0.2% of the reserved size
if the default 64k chunk size is used).
Memory is only allocated when needed.

*Default Value:* 512

``buffer_pool_use_heap_if_exhausted``
-------------------------------------
*This option is commented out by default.*

Flag indicating whether to allocate on or off heap when the sstable buffer
pool is exhausted, that is when it has exceeded the maximum memory
file_cache_size_in_mb, beyond which it will not cache buffers but allocate on request.


*Default Value:* true

``disk_optimization_strategy``
------------------------------
*This option is commented out by default.*

The strategy for optimizing disk read
Possible values are:
ssd (for solid state disks, the default)
spinning (for spinning disks)

*Default Value:* ssd

``memtable_heap_space_in_mb``
-----------------------------
*This option is commented out by default.*

Total permitted memory to use for memtables. Cassandra will stop
accepting writes when the limit is exceeded until a flush completes,
and will trigger a flush based on memtable_cleanup_threshold
If omitted, Cassandra will set both to 1/4 the size of the heap.

*Default Value:* 2048

``memtable_offheap_space_in_mb``
--------------------------------
*This option is commented out by default.*

*Default Value:* 2048

``memtable_cleanup_threshold``
------------------------------
*This option is commented out by default.*

memtable_cleanup_threshold is deprecated. The default calculation
is the only reasonable choice. See the comments on  memtable_flush_writers
for more information.

Ratio of occupied non-flushing memtable size to total permitted size
that will trigger a flush of the largest memtable. Larger mct will
mean larger flushes and hence less compaction, but also less concurrent
flush activity which can make it difficult to keep your disks fed
under heavy write load.

memtable_cleanup_threshold defaults to 1 / (memtable_flush_writers + 1)

*Default Value:* 0.11

``memtable_allocation_type``
----------------------------

Specify the way Cassandra allocates and manages memtable memory.
Options are:

heap_buffers
  on heap nio buffers

offheap_buffers
  off heap (direct) nio buffers

offheap_objects
   off heap objects

*Default Value:* heap_buffers

``repair_session_space_in_mb``
------------------------------
*This option is commented out by default.*

Limit memory usage for Merkle tree calculations during repairs. The default
is 1/16th of the available heap. The main tradeoff is that smaller trees
have less resolution, which can lead to over-streaming data. If you see heap
pressure during repairs, consider lowering this, but you cannot go below
one megabyte. If you see lots of over-streaming, consider raising
this or using subrange repair.

For more details see https://issues.apache.org/jira/browse/CASSANDRA-14096.


``commitlog_total_space_in_mb``
-------------------------------
*This option is commented out by default.*

Total space to use for commit logs on disk.

If space gets above this value, Cassandra will flush every dirty CF
in the oldest segment and remove it.  So a small total commitlog space
will tend to cause more flush activity on less-active columnfamilies.

The default value is the smaller of 8192, and 1/4 of the total space
of the commitlog volume.


*Default Value:* 8192

``memtable_flush_writers``
--------------------------
*This option is commented out by default.*

This sets the number of memtable flush writer threads per disk
as well as the total number of memtables that can be flushed concurrently.
These are generally a combination of compute and IO bound.

Memtable flushing is more CPU efficient than memtable ingest and a single thread
can keep up with the ingest rate of a whole server on a single fast disk
until it temporarily becomes IO bound under contention typically with compaction.
At that point you need multiple flush threads. At some point in the future
it may become CPU bound all the time.

You can tell if flushing is falling behind using the MemtablePool.BlockedOnAllocation
metric which should be 0, but will be non-zero if threads are blocked waiting on flushing
to free memory.

memtable_flush_writers defaults to two for a single data directory.
This means that two  memtables can be flushed concurrently to the single data directory.
If you have multiple data directories the default is one memtable flushing at a time
but the flush will use a thread per data directory so you will get two or more writers.

Two is generally enough to flush on a fast disk [array] mounted as a single data directory.
Adding more flush writers will result in smaller more frequent flushes that introduce more
compaction overhead.

There is a direct tradeoff between number of memtables that can be flushed concurrently
and flush size and frequency. More is not better you just need enough flush writers
to never stall waiting for flushing to free memory.


*Default Value:* 2

``cdc_total_space_in_mb``
-------------------------
*This option is commented out by default.*

Total space to use for change-data-capture logs on disk.

If space gets above this value, Cassandra will throw WriteTimeoutException
on Mutations including tables with CDC enabled. A CDCCompactor is responsible
for parsing the raw CDC logs and deleting them when parsing is completed.

The default value is the min of 4096 mb and 1/8th of the total space
of the drive where cdc_raw_directory resides.

*Default Value:* 4096

``cdc_free_space_check_interval_ms``
------------------------------------
*This option is commented out by default.*

When we hit our cdc_raw limit and the CDCCompactor is either running behind
or experiencing backpressure, we check at the following interval to see if any
new space for cdc-tracked tables has been made available. Default to 250ms

*Default Value:* 250

``index_summary_capacity_in_mb``
--------------------------------

A fixed memory pool size in MB for for SSTable index summaries. If left
empty, this will default to 5% of the heap size. If the memory usage of
all index summaries exceeds this limit, SSTables with low read rates will
shrink their index summaries in order to meet this limit.  However, this
is a best-effort process. In extreme conditions Cassandra may need to use
more than this amount of memory.

``index_summary_resize_interval_in_minutes``
--------------------------------------------

How frequently index summaries should be resampled.  This is done
periodically to redistribute memory from the fixed-size pool to sstables
proportional their recent read rates.  Setting to -1 will disable this
process, leaving existing index summaries at their current sampling level.

*Default Value:* 60

``trickle_fsync``
-----------------

Whether to, when doing sequential writing, fsync() at intervals in
order to force the operating system to flush the dirty
buffers. Enable this to avoid sudden dirty buffer flushing from
impacting read latencies. Almost always a good idea on SSDs; not
necessarily on platters.

*Default Value:* false

``trickle_fsync_interval_in_kb``
--------------------------------

*Default Value:* 10240

``storage_port``
----------------

TCP port, for commands and data
For security reasons, you should not expose this port to the internet.  Firewall it if needed.

*Default Value:* 7000

``ssl_storage_port``
--------------------

SSL port, for legacy encrypted communication. This property is unused unless enabled in
server_encryption_options (see below). As of cassandra 4.0, this property is deprecated
as a single port can be used for either/both secure and insecure connections.
For security reasons, you should not expose this port to the internet. Firewall it if needed.

*Default Value:* 7001

``listen_address``
------------------

Address or interface to bind to and tell other Cassandra nodes to connect to.
You _must_ change this if you want multiple nodes to be able to communicate!

Set listen_address OR listen_interface, not both.

Leaving it blank leaves it up to InetAddress.getLocalHost(). This
will always do the Right Thing _if_ the node is properly configured
(hostname, name resolution, etc), and the Right Thing is to use the
address associated with the hostname (it might not be).

Setting listen_address to 0.0.0.0 is always wrong.


*Default Value:* localhost

``listen_interface``
--------------------
*This option is commented out by default.*

Set listen_address OR listen_interface, not both. Interfaces must correspond
to a single address, IP aliasing is not supported.

*Default Value:* eth0

``listen_interface_prefer_ipv6``
--------------------------------
*This option is commented out by default.*

If you choose to specify the interface by name and the interface has an ipv4 and an ipv6 address
you can specify which should be chosen using listen_interface_prefer_ipv6. If false the first ipv4
address will be used. If true the first ipv6 address will be used. Defaults to false preferring
ipv4. If there is only one address it will be selected regardless of ipv4/ipv6.

*Default Value:* false

``broadcast_address``
---------------------
*This option is commented out by default.*

Address to broadcast to other Cassandra nodes
Leaving this blank will set it to the same value as listen_address

*Default Value:* 1.2.3.4

``listen_on_broadcast_address``
-------------------------------
*This option is commented out by default.*

When using multiple physical network interfaces, set this
to true to listen on broadcast_address in addition to
the listen_address, allowing nodes to communicate in both
interfaces.
Ignore this property if the network configuration automatically
routes  between the public and private networks such as EC2.

*Default Value:* false

``internode_authenticator``
---------------------------
*This option is commented out by default.*

Internode authentication backend, implementing IInternodeAuthenticator;
used to allow/disallow connections from peer nodes.

*Default Value:* org.apache.cassandra.auth.AllowAllInternodeAuthenticator

``start_native_transport``
--------------------------

Whether to start the native transport server.
The address on which the native transport is bound is defined by rpc_address.

*Default Value:* true

``native_transport_port``
-------------------------
port for the CQL native transport to listen for clients on
For security reasons, you should not expose this port to the internet.  Firewall it if needed.

*Default Value:* 9042

``native_transport_port_ssl``
-----------------------------
*This option is commented out by default.*
Enabling native transport encryption in client_encryption_options allows you to either use
encryption for the standard port or to use a dedicated, additional port along with the unencrypted
standard native_transport_port.
Enabling client encryption and keeping native_transport_port_ssl disabled will use encryption
for native_transport_port. Setting native_transport_port_ssl to a different value
from native_transport_port will use encryption for native_transport_port_ssl while
keeping native_transport_port unencrypted.

*Default Value:* 9142

``native_transport_max_threads``
--------------------------------
*This option is commented out by default.*
The maximum threads for handling requests (note that idle threads are stopped
after 30 seconds so there is not corresponding minimum setting).

*Default Value:* 128

``native_transport_max_frame_size_in_mb``
-----------------------------------------
*This option is commented out by default.*

The maximum size of allowed frame. Frame (requests) larger than this will
be rejected as invalid. The default is 256MB. If you're changing this parameter,
you may want to adjust max_value_size_in_mb accordingly. This should be positive and less than 2048.

*Default Value:* 256

``native_transport_frame_block_size_in_kb``
-------------------------------------------
*This option is commented out by default.*

If checksumming is enabled as a protocol option, denotes the size of the chunks into which frame
are bodies will be broken and checksummed.

*Default Value:* 32

``native_transport_max_concurrent_connections``
-----------------------------------------------
*This option is commented out by default.*

The maximum number of concurrent client connections.
The default is -1, which means unlimited.

*Default Value:* -1

``native_transport_max_concurrent_connections_per_ip``
------------------------------------------------------
*This option is commented out by default.*

The maximum number of concurrent client connections per source ip.
The default is -1, which means unlimited.

*Default Value:* -1

``native_transport_allow_older_protocols``
------------------------------------------

Controls whether Cassandra honors older, yet currently supported, protocol versions.
The default is true, which means all supported protocols will be honored.

*Default Value:* true

``native_transport_idle_timeout_in_ms``
---------------------------------------
*This option is commented out by default.*

Controls when idle client connections are closed. Idle connections are ones that had neither reads
nor writes for a time period.

Clients may implement heartbeats by sending OPTIONS native protocol message after a timeout, which
will reset idle timeout timer on the server side. To close idle client connections, corresponding
values for heartbeat intervals have to be set on the client side.

Idle connection timeouts are disabled by default.

*Default Value:* 60000

``rpc_address``
---------------

The address or interface to bind the native transport server to.

Set rpc_address OR rpc_interface, not both.

Leaving rpc_address blank has the same effect as on listen_address
(i.e. it will be based on the configured hostname of the node).

Note that unlike listen_address, you can specify 0.0.0.0, but you must also
set broadcast_rpc_address to a value other than 0.0.0.0.

For security reasons, you should not expose this port to the internet.  Firewall it if needed.

*Default Value:* localhost

``rpc_interface``
-----------------
*This option is commented out by default.*

Set rpc_address OR rpc_interface, not both. Interfaces must correspond
to a single address, IP aliasing is not supported.

*Default Value:* eth1

``rpc_interface_prefer_ipv6``
-----------------------------
*This option is commented out by default.*

If you choose to specify the interface by name and the interface has an ipv4 and an ipv6 address
you can specify which should be chosen using rpc_interface_prefer_ipv6. If false the first ipv4
address will be used. If true the first ipv6 address will be used. Defaults to false preferring
ipv4. If there is only one address it will be selected regardless of ipv4/ipv6.

*Default Value:* false

``broadcast_rpc_address``
-------------------------
*This option is commented out by default.*

RPC address to broadcast to drivers and other Cassandra nodes. This cannot
be set to 0.0.0.0. If left blank, this will be set to the value of
rpc_address. If rpc_address is set to 0.0.0.0, broadcast_rpc_address must
be set.

*Default Value:* 1.2.3.4

``rpc_keepalive``
-----------------

enable or disable keepalive on rpc/native connections

*Default Value:* true

``internode_send_buff_size_in_bytes``
-------------------------------------
*This option is commented out by default.*

Uncomment to set socket buffer size for internode communication
Note that when setting this, the buffer size is limited by net.core.wmem_max
and when not setting it it is defined by net.ipv4.tcp_wmem
See also:
/proc/sys/net/core/wmem_max
/proc/sys/net/core/rmem_max
/proc/sys/net/ipv4/tcp_wmem
/proc/sys/net/ipv4/tcp_wmem
and 'man tcp'

``internode_recv_buff_size_in_bytes``
-------------------------------------
*This option is commented out by default.*

Uncomment to set socket buffer size for internode communication
Note that when setting this, the buffer size is limited by net.core.wmem_max
and when not setting it it is defined by net.ipv4.tcp_wmem

``incremental_backups``
-----------------------

Set to true to have Cassandra create a hard link to each sstable
flushed or streamed locally in a backups/ subdirectory of the
keyspace data.  Removing these links is the operator's
responsibility.

*Default Value:* false

``snapshot_before_compaction``
------------------------------

Whether or not to take a snapshot before each compaction.  Be
careful using this option, since Cassandra won't clean up the
snapshots for you.  Mostly useful if you're paranoid when there
is a data format change.

*Default Value:* false

``auto_snapshot``
-----------------

Whether or not a snapshot is taken of the data before keyspace truncation
or dropping of column families. The STRONGLY advised default of true 
should be used to provide data safety. If you set this flag to false, you will
lose data on truncation or drop.

*Default Value:* true

``column_index_size_in_kb``
---------------------------

Granularity of the collation index of rows within a partition.
Increase if your rows are large, or if you have a very large
number of rows per partition.  The competing goals are these:

- a smaller granularity means more index entries are generated
  and looking up rows withing the partition by collation column
  is faster
- but, Cassandra will keep the collation index in memory for hot
  rows (as part of the key cache), so a larger granularity means
  you can cache more hot rows

*Default Value:* 64

``column_index_cache_size_in_kb``
---------------------------------

Per sstable indexed key cache entries (the collation index in memory
mentioned above) exceeding this size will not be held on heap.
This means that only partition information is held on heap and the
index entries are read from disk.

Note that this size refers to the size of the
serialized index information and not the size of the partition.

*Default Value:* 2

``concurrent_compactors``
-------------------------
*This option is commented out by default.*

Number of simultaneous compactions to allow, NOT including
validation "compactions" for anti-entropy repair.  Simultaneous
compactions can help preserve read performance in a mixed read/write
workload, by mitigating the tendency of small sstables to accumulate
during a single long running compactions. The default is usually
fine and if you experience problems with compaction running too
slowly or too fast, you should look at
compaction_throughput_mb_per_sec first.

concurrent_compactors defaults to the smaller of (number of disks,
number of cores), with a minimum of 2 and a maximum of 8.

If your data directories are backed by SSD, you should increase this
to the number of cores.

*Default Value:* 1

``concurrent_validations``
--------------------------
*This option is commented out by default.*

Number of simultaneous repair validations to allow. Default is unbounded
Values less than one are interpreted as unbounded (the default)

*Default Value:* 0

``concurrent_materialized_view_builders``
-----------------------------------------

Number of simultaneous materialized view builder tasks to allow.

*Default Value:* 1

``compaction_throughput_mb_per_sec``
------------------------------------

Throttles compaction to the given total throughput across the entire
system. The faster you insert data, the faster you need to compact in
order to keep the sstable count down, but in general, setting this to
16 to 32 times the rate you are inserting data is more than sufficient.
Setting this to 0 disables throttling. Note that this account for all types
of compaction, including validation compaction.

*Default Value:* 16

``sstable_preemptive_open_interval_in_mb``
------------------------------------------

When compacting, the replacement sstable(s) can be opened before they
are completely written, and used in place of the prior sstables for
any range that has been written. This helps to smoothly transfer reads 
between the sstables, reducing page cache churn and keeping hot rows hot

*Default Value:* 50

``stream_entire_sstables``
--------------------------
*This option is commented out by default.*

When enabled, permits Cassandra to zero-copy stream entire eligible
SSTables between nodes, including every component.
This speeds up the network transfer significantly subject to
throttling specified by stream_throughput_outbound_megabits_per_sec.
Enabling this will reduce the GC pressure on sending and receiving node.
When unset, the default is enabled. While this feature tries to keep the
disks balanced, it cannot guarantee it. This feature will be automatically
disabled if internode encryption is enabled. Currently this can be used with
Leveled Compaction. Once CASSANDRA-14586 is fixed other compaction strategies
will benefit as well when used in combination with CASSANDRA-6696.

*Default Value:* true

``stream_throughput_outbound_megabits_per_sec``
-----------------------------------------------
*This option is commented out by default.*

Throttles all outbound streaming file transfers on this node to the
given total throughput in Mbps. This is necessary because Cassandra does
mostly sequential IO when streaming data during bootstrap or repair, which
can lead to saturating the network connection and degrading rpc performance.
When unset, the default is 200 Mbps or 25 MB/s.

*Default Value:* 200

``inter_dc_stream_throughput_outbound_megabits_per_sec``
--------------------------------------------------------
*This option is commented out by default.*

Throttles all streaming file transfer between the datacenters,
this setting allows users to throttle inter dc stream throughput in addition
to throttling all network stream traffic as configured with
stream_throughput_outbound_megabits_per_sec
When unset, the default is 200 Mbps or 25 MB/s

*Default Value:* 200

``read_request_timeout_in_ms``
------------------------------

How long the coordinator should wait for read operations to complete.
Lowest acceptable value is 10 ms.

*Default Value:* 5000

``range_request_timeout_in_ms``
-------------------------------
How long the coordinator should wait for seq or index scans to complete.
Lowest acceptable value is 10 ms.

*Default Value:* 10000

``write_request_timeout_in_ms``
-------------------------------
How long the coordinator should wait for writes to complete.
Lowest acceptable value is 10 ms.

*Default Value:* 2000

``counter_write_request_timeout_in_ms``
---------------------------------------
How long the coordinator should wait for counter writes to complete.
Lowest acceptable value is 10 ms.

*Default Value:* 5000

``cas_contention_timeout_in_ms``
--------------------------------
How long a coordinator should continue to retry a CAS operation
that contends with other proposals for the same row.
Lowest acceptable value is 10 ms.

*Default Value:* 1000

``truncate_request_timeout_in_ms``
----------------------------------
How long the coordinator should wait for truncates to complete
(This can be much longer, because unless auto_snapshot is disabled
we need to flush first so we can snapshot before removing the data.)
Lowest acceptable value is 10 ms.

*Default Value:* 60000

``request_timeout_in_ms``
-------------------------
The default timeout for other, miscellaneous operations.
Lowest acceptable value is 10 ms.

*Default Value:* 10000

``internode_application_send_queue_capacity_in_bytes``
------------------------------------------------------
*This option is commented out by default.*

Defensive settings for protecting Cassandra from true network partitions.
See (CASSANDRA-14358) for details.

The amount of time to wait for internode tcp connections to establish.
internode_tcp_connect_timeout_in_ms = 2000

The amount of time unacknowledged data is allowed on a connection before we throw out the connection
Note this is only supported on Linux + epoll, and it appears to behave oddly above a setting of 30000
(it takes much longer than 30s) as of Linux 4.12. If you want something that high set this to 0
which picks up the OS default and configure the net.ipv4.tcp_retries2 sysctl to be ~8.
internode_tcp_user_timeout_in_ms = 30000

The maximum continuous period a connection may be unwritable in application space
internode_application_timeout_in_ms = 30000

Global, per-endpoint and per-connection limits imposed on messages queued for delivery to other nodes
and waiting to be processed on arrival from other nodes in the cluster.  These limits are applied to the on-wire
size of the message being sent or received.

The basic per-link limit is consumed in isolation before any endpoint or global limit is imposed.
Each node-pair has three links: urgent, small and large.  So any given node may have a maximum of
N*3*(internode_application_send_queue_capacity_in_bytes+internode_application_receive_queue_capacity_in_bytes)
messages queued without any coordination between them although in practice, with token-aware routing, only RF*tokens
nodes should need to communicate with significant bandwidth.

The per-endpoint limit is imposed on all messages exceeding the per-link limit, simultaneously with the global limit,
on all links to or from a single node in the cluster.
The global limit is imposed on all messages exceeding the per-link limit, simultaneously with the per-endpoint limit,
on all links to or from any node in the cluster.


*Default Value:* 4194304                       #4MiB

``internode_application_send_queue_reserve_endpoint_capacity_in_bytes``
-----------------------------------------------------------------------
*This option is commented out by default.*

*Default Value:* 134217728    #128MiB

``internode_application_send_queue_reserve_global_capacity_in_bytes``
---------------------------------------------------------------------
*This option is commented out by default.*

*Default Value:* 536870912      #512MiB

``internode_application_receive_queue_capacity_in_bytes``
---------------------------------------------------------
*This option is commented out by default.*

*Default Value:* 4194304                    #4MiB

``internode_application_receive_queue_reserve_endpoint_capacity_in_bytes``
--------------------------------------------------------------------------
*This option is commented out by default.*

*Default Value:* 134217728 #128MiB

``internode_application_receive_queue_reserve_global_capacity_in_bytes``
------------------------------------------------------------------------
*This option is commented out by default.*

*Default Value:* 536870912   #512MiB

``slow_query_log_timeout_in_ms``
--------------------------------


How long before a node logs slow queries. Select queries that take longer than
this timeout to execute, will generate an aggregated log message, so that slow queries
can be identified. Set this value to zero to disable slow query logging.

*Default Value:* 500

``cross_node_timeout``
----------------------
*This option is commented out by default.*

Enable operation timeout information exchange between nodes to accurately
measure request timeouts.  If disabled, replicas will assume that requests
were forwarded to them instantly by the coordinator, which means that
under overload conditions we will waste that much extra time processing 
already-timed-out requests.

Warning: It is generally assumed that users have setup NTP on their clusters, and that clocks are modestly in sync, 
since this is a requirement for general correctness of last write wins.

*Default Value:* true

``streaming_keep_alive_period_in_secs``
---------------------------------------
*This option is commented out by default.*

Set keep-alive period for streaming
This node will send a keep-alive message periodically with this period.
If the node does not receive a keep-alive message from the peer for
2 keep-alive cycles the stream session times out and fail
Default value is 300s (5 minutes), which means stalled stream
times out in 10 minutes by default

*Default Value:* 300

``streaming_connections_per_host``
----------------------------------
*This option is commented out by default.*

Limit number of connections per host for streaming
Increase this when you notice that joins are CPU-bound rather that network
bound (for example a few nodes with big files).

*Default Value:* 1

``phi_convict_threshold``
-------------------------
*This option is commented out by default.*


phi value that must be reached for a host to be marked down.
most users should never need to adjust this.

*Default Value:* 8

``endpoint_snitch``
-------------------

endpoint_snitch -- Set this to a class that implements
IEndpointSnitch.  The snitch has two functions:

- it teaches Cassandra enough about your network topology to route
  requests efficiently
- it allows Cassandra to spread replicas around your cluster to avoid
  correlated failures. It does this by grouping machines into
  "datacenters" and "racks."  Cassandra will do its best not to have
  more than one replica on the same "rack" (which may not actually
  be a physical location)

CASSANDRA WILL NOT ALLOW YOU TO SWITCH TO AN INCOMPATIBLE SNITCH
ONCE DATA IS INSERTED INTO THE CLUSTER.  This would cause data loss.
This means that if you start with the default SimpleSnitch, which
locates every node on "rack1" in "datacenter1", your only options
if you need to add another datacenter are GossipingPropertyFileSnitch
(and the older PFS).  From there, if you want to migrate to an
incompatible snitch like Ec2Snitch you can do it by adding new nodes
under Ec2Snitch (which will locate them in a new "datacenter") and
decommissioning the old ones.

Out of the box, Cassandra provides:

SimpleSnitch:
   Treats Strategy order as proximity. This can improve cache
   locality when disabling read repair.  Only appropriate for
   single-datacenter deployments.

GossipingPropertyFileSnitch
   This should be your go-to snitch for production use.  The rack
   and datacenter for the local node are defined in
   cassandra-rackdc.properties and propagated to other nodes via
   gossip.  If cassandra-topology.properties exists, it is used as a
   fallback, allowing migration from the PropertyFileSnitch.

PropertyFileSnitch:
   Proximity is determined by rack and data center, which are
   explicitly configured in cassandra-topology.properties.

Ec2Snitch:
   Appropriate for EC2 deployments in a single Region. Loads Region
   and Availability Zone information from the EC2 API. The Region is
   treated as the datacenter, and the Availability Zone as the rack.
   Only private IPs are used, so this will not work across multiple
   Regions.

Ec2MultiRegionSnitch:
   Uses public IPs as broadcast_address to allow cross-region
   connectivity.  (Thus, you should set seed addresses to the public
   IP as well.) You will need to open the storage_port or
   ssl_storage_port on the public IP firewall.  (For intra-Region
   traffic, Cassandra will switch to the private IP after
   establishing a connection.)

RackInferringSnitch:
   Proximity is determined by rack and data center, which are
   assumed to correspond to the 3rd and 2nd octet of each node's IP
   address, respectively.  Unless this happens to match your
   deployment conventions, this is best used as an example of
   writing a custom Snitch class and is provided in that spirit.

You can use a custom Snitch by setting this to the full class name
of the snitch, which will be assumed to be on your classpath.

*Default Value:* SimpleSnitch

``dynamic_snitch_update_interval_in_ms``
----------------------------------------

controls how often to perform the more expensive part of host score
calculation

*Default Value:* 100 

``dynamic_snitch_reset_interval_in_ms``
---------------------------------------
controls how often to reset all host scores, allowing a bad host to
possibly recover

*Default Value:* 600000

``dynamic_snitch_badness_threshold``
------------------------------------
if set greater than zero, this will allow
'pinning' of replicas to hosts in order to increase cache capacity.
The badness threshold will control how much worse the pinned host has to be
before the dynamic snitch will prefer other replicas over it.  This is
expressed as a double which represents a percentage.  Thus, a value of
0.2 means Cassandra would continue to prefer the static snitch values
until the pinned host was 20% worse than the fastest.

*Default Value:* 0.1

``server_encryption_options``
-----------------------------

Enable or disable inter-node encryption
JVM and netty defaults for supported SSL socket protocols and cipher suites can
be replaced using custom encryption options. This is not recommended
unless you have policies in place that dictate certain settings, or
need to disable vulnerable ciphers or protocols in case the JVM cannot
be updated.
FIPS compliant settings can be configured at JVM level and should not
involve changing encryption settings here:
https://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/FIPS.html

*NOTE* No custom encryption options are enabled at the moment
The available internode options are : all, none, dc, rack
If set to dc cassandra will encrypt the traffic between the DCs
If set to rack cassandra will encrypt the traffic between the racks

The passwords used in these options must match the passwords used when generating
the keystore and truststore.  For instructions on generating these files, see:
http://download.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html#CreateKeystore


*Default Value (complex option)*::

        # set to true for allowing secure incoming connections
        enabled: false
        # If enabled and optional are both set to true, encrypted and unencrypted connections are handled on the storage_port
        optional: false
        # if enabled, will open up an encrypted listening socket on ssl_storage_port. Should be used
        # during upgrade to 4.0; otherwise, set to false.
        enable_legacy_ssl_storage_port: false
        # on outbound connections, determine which type of peers to securely connect to. 'enabled' must be set to true.
        internode_encryption: none
        keystore: conf/.keystore
        keystore_password: cassandra
        truststore: conf/.truststore
        truststore_password: cassandra
        # More advanced defaults below:
        # protocol: TLS
        # store_type: JKS
        # cipher_suites: [TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA,TLS_DHE_RSA_WITH_AES_128_CBC_SHA,TLS_DHE_RSA_WITH_AES_256_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA]
        # require_client_auth: false
        # require_endpoint_verification: false

``client_encryption_options``
-----------------------------
enable or disable client-to-server encryption.

*Default Value (complex option)*::

        enabled: false
        # If enabled and optional is set to true encrypted and unencrypted connections are handled.
        optional: false
        keystore: conf/.keystore
        keystore_password: cassandra
        # require_client_auth: false
        # Set trustore and truststore_password if require_client_auth is true
        # truststore: conf/.truststore
        # truststore_password: cassandra
        # More advanced defaults below:
        # protocol: TLS
        # store_type: JKS
        # cipher_suites: [TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA,TLS_DHE_RSA_WITH_AES_128_CBC_SHA,TLS_DHE_RSA_WITH_AES_256_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA]

``internode_compression``
-------------------------
internode_compression controls whether traffic between nodes is
compressed.
Can be:

all
  all traffic is compressed

dc
  traffic between different datacenters is compressed

none
  nothing is compressed.

*Default Value:* dc

``inter_dc_tcp_nodelay``
------------------------

Enable or disable tcp_nodelay for inter-dc communication.
Disabling it will result in larger (but fewer) network packets being sent,
reducing overhead from the TCP protocol itself, at the cost of increasing
latency if you block for cross-datacenter responses.

*Default Value:* false

``tracetype_query_ttl``
-----------------------

TTL for different trace types used during logging of the repair process.

*Default Value:* 86400

``tracetype_repair_ttl``
------------------------

*Default Value:* 604800

``enable_user_defined_functions``
---------------------------------

If unset, all GC Pauses greater than gc_log_threshold_in_ms will log at
INFO level
UDFs (user defined functions) are disabled by default.
As of Cassandra 3.0 there is a sandbox in place that should prevent execution of evil code.

*Default Value:* false

``enable_scripted_user_defined_functions``
------------------------------------------

Enables scripted UDFs (JavaScript UDFs).
Java UDFs are always enabled, if enable_user_defined_functions is true.
Enable this option to be able to use UDFs with "language javascript" or any custom JSR-223 provider.
This option has no effect, if enable_user_defined_functions is false.

*Default Value:* false

``windows_timer_interval``
--------------------------

The default Windows kernel timer and scheduling resolution is 15.6ms for power conservation.
Lowering this value on Windows can provide much tighter latency and better throughput, however
some virtualized environments may see a negative performance impact from changing this setting
below their system default. The sysinternals 'clockres' tool can confirm your system's default
setting.

*Default Value:* 1

``transparent_data_encryption_options``
---------------------------------------


Enables encrypting data at-rest (on disk). Different key providers can be plugged in, but the default reads from
a JCE-style keystore. A single keystore can hold multiple keys, but the one referenced by
the "key_alias" is the only key that will be used for encrypt opertaions; previously used keys
can still (and should!) be in the keystore and will be used on decrypt operations
(to handle the case of key rotation).

It is strongly recommended to download and install Java Cryptography Extension (JCE)
Unlimited Strength Jurisdiction Policy Files for your version of the JDK.
(current link: http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html)

Currently, only the following file types are supported for transparent data encryption, although
more are coming in future cassandra releases: commitlog, hints

*Default Value (complex option)*::

        enabled: false
        chunk_length_kb: 64
        cipher: AES/CBC/PKCS5Padding
        key_alias: testing:1
        # CBC IV length for AES needs to be 16 bytes (which is also the default size)
        # iv_length: 16
        key_provider:
          - class_name: org.apache.cassandra.security.JKSKeyProvider
            parameters:
              - keystore: conf/.keystore
                keystore_password: cassandra
                store_type: JCEKS
                key_password: cassandra

``tombstone_warn_threshold``
----------------------------

####################
SAFETY THRESHOLDS #
####################

When executing a scan, within or across a partition, we need to keep the
tombstones seen in memory so we can return them to the coordinator, which
will use them to make sure other replicas also know about the deleted rows.
With workloads that generate a lot of tombstones, this can cause performance
problems and even exaust the server heap.
(http://www.datastax.com/dev/blog/cassandra-anti-patterns-queues-and-queue-like-datasets)
Adjust the thresholds here if you understand the dangers and want to
scan more tombstones anyway.  These thresholds may also be adjusted at runtime
using the StorageService mbean.

*Default Value:* 1000

``tombstone_failure_threshold``
-------------------------------

*Default Value:* 100000

``batch_size_warn_threshold_in_kb``
-----------------------------------

Log WARN on any multiple-partition batch size exceeding this value. 5kb per batch by default.
Caution should be taken on increasing the size of this threshold as it can lead to node instability.

*Default Value:* 5

``batch_size_fail_threshold_in_kb``
-----------------------------------

Fail any multiple-partition batch exceeding this value. 50kb (10x warn threshold) by default.

*Default Value:* 50

``unlogged_batch_across_partitions_warn_threshold``
---------------------------------------------------

Log WARN on any batches not of type LOGGED than span across more partitions than this limit

*Default Value:* 10

``compaction_large_partition_warning_threshold_mb``
---------------------------------------------------

Log a warning when compacting partitions larger than this value

*Default Value:* 100

``gc_log_threshold_in_ms``
--------------------------
*This option is commented out by default.*

GC Pauses greater than 200 ms will be logged at INFO level
This threshold can be adjusted to minimize logging if necessary

*Default Value:* 200

``gc_warn_threshold_in_ms``
---------------------------
*This option is commented out by default.*

GC Pauses greater than gc_warn_threshold_in_ms will be logged at WARN level
Adjust the threshold based on your application throughput requirement. Setting to 0
will deactivate the feature.

*Default Value:* 1000

``max_value_size_in_mb``
------------------------
*This option is commented out by default.*

Maximum size of any value in SSTables. Safety measure to detect SSTable corruption
early. Any value size larger than this threshold will result into marking an SSTable
as corrupted. This should be positive and less than 2048.

*Default Value:* 256

``back_pressure_enabled``
-------------------------

Back-pressure settings #
If enabled, the coordinator will apply the back-pressure strategy specified below to each mutation
sent to replicas, with the aim of reducing pressure on overloaded replicas.

*Default Value:* false

``back_pressure_strategy``
--------------------------
The back-pressure strategy applied.
The default implementation, RateBasedBackPressure, takes three arguments:
high ratio, factor, and flow type, and uses the ratio between incoming mutation responses and outgoing mutation requests.
If below high ratio, outgoing mutations are rate limited according to the incoming rate decreased by the given factor;
if above high ratio, the rate limiting is increased by the given factor;
such factor is usually best configured between 1 and 10, use larger values for a faster recovery
at the expense of potentially more dropped mutations;
the rate limiting is applied according to the flow type: if FAST, it's rate limited at the speed of the fastest replica,
if SLOW at the speed of the slowest one.
New strategies can be added. Implementors need to implement org.apache.cassandra.net.BackpressureStrategy and
provide a public constructor accepting a Map<String, Object>.

``otc_coalescing_strategy``
---------------------------
*This option is commented out by default.*

Coalescing Strategies #
Coalescing multiples messages turns out to significantly boost message processing throughput (think doubling or more).
On bare metal, the floor for packet processing throughput is high enough that many applications won't notice, but in
virtualized environments, the point at which an application can be bound by network packet processing can be
surprisingly low compared to the throughput of task processing that is possible inside a VM. It's not that bare metal
doesn't benefit from coalescing messages, it's that the number of packets a bare metal network interface can process
is sufficient for many applications such that no load starvation is experienced even without coalescing.
There are other benefits to coalescing network messages that are harder to isolate with a simple metric like messages
per second. By coalescing multiple tasks together, a network thread can process multiple messages for the cost of one
trip to read from a socket, and all the task submission work can be done at the same time reducing context switching
and increasing cache friendliness of network message processing.
See CASSANDRA-8692 for details.

Strategy to use for coalescing messages in OutboundTcpConnection.
Can be fixed, movingaverage, timehorizon, disabled (default).
You can also specify a subclass of CoalescingStrategies.CoalescingStrategy by name.

*Default Value:* DISABLED

``otc_coalescing_window_us``
----------------------------
*This option is commented out by default.*

How many microseconds to wait for coalescing. For fixed strategy this is the amount of time after the first
message is received before it will be sent with any accompanying messages. For moving average this is the
maximum amount of time that will be waited as well as the interval at which messages must arrive on average
for coalescing to be enabled.

*Default Value:* 200

``otc_coalescing_enough_coalesced_messages``
--------------------------------------------
*This option is commented out by default.*

Do not try to coalesce messages if we already got that many messages. This should be more than 2 and less than 128.

*Default Value:* 8

``otc_backlog_expiration_interval_ms``
--------------------------------------
*This option is commented out by default.*

How many milliseconds to wait between two expiration runs on the backlog (queue) of the OutboundTcpConnection.
Expiration is done if messages are piling up in the backlog. Droppable messages are expired to free the memory
taken by expired messages. The interval should be between 0 and 1000, and in most installations the default value
will be appropriate. A smaller value could potentially expire messages slightly sooner at the expense of more CPU
time and queue contention while iterating the backlog of messages.
An interval of 0 disables any wait time, which is the behavior of former Cassandra versions.


*Default Value:* 200

``ideal_consistency_level``
---------------------------
*This option is commented out by default.*

Track a metric per keyspace indicating whether replication achieved the ideal consistency
level for writes without timing out. This is different from the consistency level requested by
each write which may be lower in order to facilitate availability.

*Default Value:* EACH_QUORUM

``automatic_sstable_upgrade``
-----------------------------
*This option is commented out by default.*

Automatically upgrade sstables after upgrade - if there is no ordinary compaction to do, the
oldest non-upgraded sstable will get upgraded to the latest version

*Default Value:* false

``max_concurrent_automatic_sstable_upgrades``
---------------------------------------------
*This option is commented out by default.*
Limit the number of concurrent sstable upgrades

*Default Value:* 1

``audit_logging_options``
-------------------------

Audit logging - Logs every incoming CQL command request, authentication to a node. See the docs
on audit_logging for full details about the various configuration options.

``full_query_logging_options``
------------------------------
*This option is commented out by default.*


default options for full query logging - these can be overridden from command line when executing
nodetool enablefullquerylog

``corrupted_tombstone_strategy``
--------------------------------
*This option is commented out by default.*

validate tombstones on reads and compaction
can be either "disabled", "warn" or "exception"

*Default Value:* disabled

``diagnostic_events_enabled``
-----------------------------

Diagnostic Events #
If enabled, diagnostic events can be helpful for troubleshooting operational issues. Emitted events contain details
on internal state and temporal relationships across events, accessible by clients via JMX.

*Default Value:* false

``native_transport_flush_in_batches_legacy``
--------------------------------------------
*This option is commented out by default.*

Use native transport TCP message coalescing. If on upgrade to 4.0 you found your throughput decreasing, and in
particular you run an old kernel or have very fewer client connections, this option might be worth evaluating.

*Default Value:* false

``repaired_data_tracking_for_range_reads_enabled``
--------------------------------------------------

Enable tracking of repaired state of data during reads and comparison between replicas
Mismatches between the repaired sets of replicas can be characterized as either confirmed
or unconfirmed. In this context, unconfirmed indicates that the presence of pending repair
sessions, unrepaired partition tombstones, or some other condition means that the disparity
cannot be considered conclusive. Confirmed mismatches should be a trigger for investigation
as they may be indicative of corruption or data loss.
There are separate flags for range vs partition reads as single partition reads are only tracked
when CL > 1 and a digest mismatch occurs. Currently, range queries don't use digests so if
enabled for range reads, all range reads will include repaired data tracking. As this adds
some overhead, operators may wish to disable it whilst still enabling it for partition reads

*Default Value:* false

``repaired_data_tracking_for_partition_reads_enabled``
------------------------------------------------------

*Default Value:* false

``report_unconfirmed_repaired_data_mismatches``
-----------------------------------------------
If false, only confirmed mismatches will be reported. If true, a separate metric for unconfirmed
mismatches will also be recorded. This is to avoid potential signal:noise issues are unconfirmed
mismatches are less actionable than confirmed ones.

*Default Value:* false

``enable_materialized_views``
-----------------------------

########################
EXPERIMENTAL FEATURES #
########################

Enables materialized view creation on this node.
Materialized views are considered experimental and are not recommended for production use.

*Default Value:* false

``enable_sasi_indexes``
-----------------------

Enables SASI index creation on this node.
SASI indexes are considered experimental and are not recommended for production use.

*Default Value:* false

``enable_transient_replication``
--------------------------------

Enables creation of transiently replicated keyspaces on this node.
Transient replication is experimental and is not recommended for production use.

*Default Value:* false
