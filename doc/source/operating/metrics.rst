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

.. highlight:: none

.. _monitoring-metrics:

Monitoring
----------

Metrics in Cassandra are managed using the `Dropwizard Metrics <http://metrics.dropwizard.io>`__ library. These metrics
can be queried via JMX or pushed to external monitoring systems using a number of `built in
<http://metrics.dropwizard.io/3.1.0/getting-started/#other-reporting>`__ and `third party
<http://metrics.dropwizard.io/3.1.0/manual/third-party/>`__ reporter plugins.

Metrics are collected for a single node. It's up to the operator to use an external monitoring system to aggregate them.

Metric Types
^^^^^^^^^^^^
All metrics reported by cassandra fit into one of the following types.

``Gauge``
    An instantaneous measurement of a value.

``Counter``
    A gauge for an ``AtomicLong`` instance. Typically this is consumed by monitoring the change since the last call to
    see if there is a large increase compared to the norm.

``Histogram``
    Measures the statistical distribution of values in a stream of data.

    In addition to minimum, maximum, mean, etc., it also measures median, 75th, 90th, 95th, 98th, 99th, and 99.9th
    percentiles.

``Timer``
    Measures both the rate that a particular piece of code is called and the histogram of its duration.

``Latency``
    Special type that tracks latency (in microseconds) with a ``Timer`` plus a ``Counter`` that tracks the total latency
    accrued since starting. The former is useful if you track the change in total latency since the last check. Each
    metric name of this type will have 'Latency' and 'TotalLatency' appended to it.

``Meter``
    A meter metric which measures mean throughput and one-, five-, and fifteen-minute exponentially-weighted moving
    average throughputs.

.. _table-metrics:

Table Metrics
^^^^^^^^^^^^^

Each table in Cassandra has metrics responsible for tracking its state and performance.

The metric names are all appended with the specific ``Keyspace`` and ``Table`` name.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.Table.<MetricName>.<Keyspace>.<Table>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=Table,keyspace=<Keyspace>,scope=<Table>,name=<MetricName>``

.. NOTE::
    There is a special table called '``all``' without a keyspace. This represents the aggregation of metrics across
    **all** tables and keyspaces on the node.


=============================================== ============== ===========
Name                                            Type           Description
=============================================== ============== ===========
AdditionalWrites                                Counter        Total number of additional writes sent to the replicas (other than the intial contacted ones).
AllMemtablesLiveDataSize                        Gauge<Long>    Total amount of live data stored in the memtables (2i and pending flush memtables included) that resides off-heap, excluding any data structure overhead.
AllMemtablesOffHeapDataSize                     Gauge<Long>    Total amount of data stored in the memtables (2i and pending flush memtables included) that resides **off**-heap.
AllMemtablesOffHeapSize (Deprecated)            Gauge<Long>    Total amount of data stored in the memtables (2i and pending flush memtables included) that resides **off**-heap.
AllMemtablesOnHeapDataSize                      Gauge<Long>    Total amount of data stored in the memtables (2i and pending flush memtables included) that resides **on**-heap.
AllMemtablesOnHeapSize (Deprecated)             Gauge<Long>    Total amount of data stored in the memtables (2i and pending flush memtables included) that resides **on**-heap.
AnticompactionTime                              Timer          Time spent anticompacting before a consistent repair.
BloomFilterDiskSpaceUsed                        Gauge<Long>    Disk space used by bloom filter (in bytes).
BloomFilterFalsePositives                       Gauge<Long>    Number of false positives on table's bloom filter.
BloomFilterFalseRatio                           Gauge<Double>  False positive ratio of table's bloom filter.
BloomFilterOffHeapMemoryUsed                    Gauge<Long>    Off-heap memory used by bloom filter.
BytesAnticompacted                              Counter        How many bytes we anticompacted.
BytesFlushed                                    Counter        Total number of bytes flushed since server [re]start.
BytesMutatedAnticompaction                      Counter        How many bytes we avoided anticompacting because the sstable was fully contained in the repaired range.
BytesPendingRepair                              Gauge<Long>    Size of table data isolated for an ongoing incremental repair
BytesRepaired                                   Gauge<Long>    Size of table data repaired on disk
BytesUnrepaired                                 Gauge<Long>    Size of table data unrepaired on disk
BytesValidated                                  Histogram      Histogram over the amount of bytes read during validation.
CasCommit                                       Latency        Latency of paxos commit round.
CasPrepare                                      Latency        Latency of paxos prepare round.
CasPropose                                      Latency        Latency of paxos propose round.
ColUpdateTimeDeltaHistogram                     Histogram      Histogram of column update time delta on this table.
CompactionBytesWritten                          Counter        Total number of bytes written by compaction since server [re]start.
CompressionMetadataOffHeapMemoryUsed            Gauge<Long>    Off-heap memory used by compression meta data.
CompressionRatio                                Gauge<Double>  Current compression ratio for all SSTables.
CoordinatorReadLatency                          Timer          Coordinator read latency for this table.
CoordinatorScanLatency                          Timer          Coordinator range scan latency for this table.
CoordinatorWriteLatency                         Timer          Coordinator write latency for this table.
DroppedMutations                                Counter        Number of dropped mutations on this table.
EstimatedColumnCountHistogram                   Gauge<long[]>  Histogram of estimated number of columns.
EstimatedPartitionCount                         Gauge<Long>    Approximate number of keys in table.
EstimatedPartitionSizeHistogram                 Gauge<long[]>  Histogram of estimated partition size (in bytes).
IndexSummaryOffHeapMemoryUsed                   Gauge<Long>    Off-heap memory used by index summary.
KeyCacheHitRate                                 Gauge<Double>  Key cache hit rate for this table.
LiveDiskSpaceUsed                               Counter        Disk space used by SSTables belonging to this table (in bytes).
LiveSSTableCount                                Gauge<Integer> Number of SSTables on disk for this table.
LiveScannedHistogram                            Histogram      Histogram of live cells scanned in queries on this table.
MaxPartitionSize                                Gauge<Long>    Size of the largest compacted partition (in bytes).
MeanPartitionSize                               Gauge<Long>    Size of the average compacted partition (in bytes).
MemtableColumnsCount                            Gauge<Long>    Total number of columns present in the memtable.
MemtableLiveDataSize                            Gauge<Long>    Total amount of live data stored in the memtable, excluding any data structure overhead.
MemtableOffHeapDataSize                         Gauge<Long>    Total amount of data stored in the memtable that resides **off**-heap, including column related overhead and partitions overwritten.
MemtableOffHeapSize (Deprecated)                Gauge<Long>    Total amount of data stored in the memtable that resides **off**-heap, including column related overhead and partitions overwritten.
MemtableOnHeapDataSize                          Gauge<Long>    Total amount of data stored in the memtable that resides **on**-heap, including column related overhead and partitions overwritten.
MemtableOnHeapSize (Deprecated)                 Gauge<Long>    Total amount of data stored in the memtable that resides **on**-heap, including column related overhead and partitions overwritten.
MemtableSwitchCount                             Counter        Number of times flush has resulted in the memtable being switched out.
MinPartitionSize                                Gauge<Long>    Size of the smallest compacted partition (in bytes).
MutatedAnticompactionGauge                      Gauge<Double>  Ratio of bytes mutated vs total bytes repaired.
PartitionsValidated                             Histogram      Histogram over the number of partitions read during validation.
PendingCompactions                              Gauge<Integer> Estimate of number of pending compactions for this table.
PendingFlushes                                  Counter        Estimated number of flush tasks pending for this table.
PercentRepaired                                 Gauge<Double>  Percent of table data that is repaired on disk.
RangeLatency                                    Latency        Local range scan latency for this table.
ReadLatency                                     Latency        Local read latency for this table.
ReadRepairRequests                              Meter          Throughput for mutations generated by read-repair.
RepairJobsCompleted                             Counter        Total number of completed repairs as coordinator on this table.
RepairJobsStarted                               Counter        Total number of started repairs as coordinator on this table.
RepairSyncTime                                  Timer          Time spent doing streaming during repair.
RepairedDataTrackingOverreadRows                Histogram      Histogram of the amount of the additonal rows of the repaired data read.
RepairedDataTrackingOverreadTime                Timer          Time spent reading the additional rows of the repaired data.
ReplicaFilteringProtectionRequests              Meter          Throughput for row completion requests during replica filtering protection.
ReplicaFilteringProtectionRowsCachedPerQuery    Histogram      Histogram of the number of rows cached per query when replica filtering protection is engaged.
RowCacheHit                                     Counter        Number of table row cache hits.
RowCacheHitOutOfRange                           Counter        Number of table row cache hits that do not satisfy the query filter, thus went to disk.
RowCacheMiss                                    Counter        Number of table row cache misses.
SSTablesPerReadHistogram                        Histogram      Histogram of the number of sstable data files accessed per single partition read. SSTables skipped due to Bloom Filters, min-max key or partition index lookup are not taken into acoount.
ShortReadProtectionRequests                     Meter          Throughput for requests to get extra rows during short read protection.
SpeculativeFailedRetries                        Counter        Number of speculative retries that failed to prevent a timeout
SpeculativeInsufficientReplicas                 Counter        Number of speculative retries that couldn't be attempted due to lack of replicas
SpeculativeRetries                              Counter        Number of times speculative retries were sent for this table.
SpeculativeSampleLatencyNanos                   Gauge<Long>    Number of nanoseconds to wait before speculation is attempted. Value may be statically configured or updated periodically based on coordinator latency.
TombstoneScannedHistogram                       Histogram      Histogram of tombstones scanned in queries on this table.
TotalDiskSpaceUsed                              Counter        Total disk space used by SSTables belonging to this table, including obsolete ones waiting to be GC'd.
TrueSnapshotsSize                               Gauge<Long>    Disk space used by snapshots of this table including all SSTable components.
ValidationTime                                  Timer          Time spent doing validation compaction during repair.
ViewLockAcquireTime                             Timer          Time taken acquiring a partition lock for materialized view updates on this table.
ViewReadTime                                    Timer          Time taken during the local read of a materialized view update.
WaitingOnFreeMemtableSpace                      Histogram      Histogram of time spent waiting for free memtable space, either on- or off-heap.
WriteLatency                                    Latency        Local write latency for this table.
=============================================== ============== ===========

Keyspace Metrics
^^^^^^^^^^^^^^^^
Each keyspace in Cassandra has metrics responsible for tracking its state and performance.

Most of these metrics are the same as the ``Table Metrics`` above, only they are aggregated at the Keyspace level. Only the keyspace specific metrics are specified in the table below.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.keyspace.<MetricName>.<Keyspace>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=Keyspace,keyspace=<Keyspace>,name=<MetricName>``


======================================= ============== ===========
Name                                    Type           Description
======================================= ============== ===========
IdealCLWriteLatency                     Latency        Coordinator latency of writes at the configured ideal consistency level. No values are recorded if ideal consistency level is not configured
RepairPrepareTime                       Timer          Total time spent preparing for repair.
RepairTime                              Timer          Total time spent as repair coordinator.
WriteFailedIdealCL                      Counter        Number of writes that failed to achieve the configured ideal consistency level or 0 if none is configured
======================================= ============== ===========

ThreadPool Metrics
^^^^^^^^^^^^^^^^^^

Cassandra splits work of a particular type into its own thread pool.  This provides back-pressure and asynchrony for
requests on a node.  It's important to monitor the state of these thread pools since they can tell you how saturated a
node is.

The metric names are all appended with the specific ``ThreadPool`` name.  The thread pools are also categorized under a
specific type.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.ThreadPools.<MetricName>.<Path>.<ThreadPoolName>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=ThreadPools,path=<Path>,scope=<ThreadPoolName>,name=<MetricName>``

===================== ============== ===========
Name                  Type           Description
===================== ============== ===========
ActiveTasks           Gauge<Integer> Number of tasks being actively worked on by this pool.
CompletedTasks        Counter        Number of tasks completed.
CurrentlyBlockedTask  Counter        Number of tasks that are currently blocked due to queue saturation but on retry will become unblocked.
MaxPoolSize           Gauge<Integer> The maximum number of threads in this pool.
MaxTasksQueued        Gauge<Integer> The maximum number of tasks queued before a task get blocked.
PendingTasks          Gauge<Integer> Number of queued tasks queued up on this pool.
TotalBlockedTasks     Counter        Number of tasks that were blocked due to queue saturation.
===================== ============== ===========

The following thread pools can be monitored.

============================ ============== ===========
Name                         Type           Description
============================ ============== ===========
AntiEntropyStage             internal       Builds merkle tree for repairs
CacheCleanupExecutor         internal       Cache maintenance performed on this thread pool
CompactionExecutor           internal       Compactions are run on these threads
CounterMutationStage         request        Responsible for counter writes
GossipStage                  internal       Handles gossip requests
HintsDispatcher              internal       Performs hinted handoff
InternalResponseStage        internal       Responsible for intra-cluster callbacks
MemtableFlushWriter          internal       Writes memtables to disk
MemtablePostFlush            internal       Cleans up commit log after memtable is written to disk
MemtableReclaimMemory        internal       Memtable recycling
MigrationStage               internal       Runs schema migrations
MiscStage                    internal       Misceleneous tasks run here
MutationStage                request        Responsible for all other writes
Native-Transport-Requests    transport      Handles client CQL requests
PendingRangeCalculator       internal       Calculates token range
PerDiskMemtableFlushWriter_0 internal       Responsible for writing a spec (there is one of these per disk 0-N)
ReadRepairStage              request        ReadRepair happens on this thread pool
ReadStage                    request        Local reads run on this thread pool
RequestResponseStage         request        Coordinator requests to the cluster run on this thread pool
Sampler                      internal       Responsible for re-sampling the index summaries of SStables
SecondaryIndexManagement     internal       Performs updates to secondary indexes
ValidationExecutor           internal       Performs validation compaction or scrubbing
ViewBuildExecutor            internal       Performs materialized views initial build
ViewMutationStage            request        Responsible for materialized view writes
============================ ============== ===========

.. |nbsp| unicode:: 0xA0 .. nonbreaking space

Client Request Metrics
^^^^^^^^^^^^^^^^^^^^^^

Client requests have their own set of metrics that encapsulate the work happening at coordinator level.

Different types of client requests are broken down by ``RequestType``.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.ClientRequest.<MetricName>.<RequestType>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=ClientRequest,scope=<RequestType>,name=<MetricName>``


:RequestType: CASRead
:Description: Metrics related to transactional read requests.
:Metrics:
    ===================== ============== =============================================================
    Name                  Type           Description
    ===================== ============== =============================================================
    ConditionNotMet       Counter        Number of transaction preconditions did not match current values.
    ContentionHistogram   Histogram      How many contended reads were encountered
    Failures              Counter        Number of transaction failures encountered.
    Timeouts              Counter        Number of timeouts encountered.
    Unavailables          Counter        Number of unavailable exceptions encountered.
    UnfinishedCommit      Counter        Number of transactions that were committed on read.
    |nbsp|                Latency        Transaction read latency.
    ===================== ============== =============================================================

:RequestType: CASWrite
:Description: Metrics related to transactional write requests.
:Metrics:
    ===================== ============== =============================================================
    Name                  Type           Description
    ===================== ============== =============================================================
    ConditionNotMet       Counter        Number of transaction preconditions did not match current values.
    ContentionHistogram   Histogram      How many contended writes were encountered
    Failures              Counter        Number of transaction failures encountered.
    MutationSizeHistogram Histogram      Total size in bytes of the requests mutations.
    Timeouts              Counter        Number of timeouts encountered.
    UnfinishedCommit      Counter        Number of transactions that were committed on write.
    |nbsp|                Latency        Transaction write latency.
    ===================== ============== =============================================================


:RequestType: Read
:Description: Metrics related to standard read requests.
:Metrics:
    ===================== ============== =============================================================
    Name                  Type           Description
    ===================== ============== =============================================================
    Failures              Counter        Number of read failures encountered.
    Timeouts              Counter        Number of timeouts encountered.
    Unavailables          Counter        Number of unavailable exceptions encountered.
    |nbsp|                Latency        Read latency.
    ===================== ============== =============================================================

:RequestType: RangeSlice
:Description: Metrics related to token range read requests.
:Metrics:
    ===================== ============== =============================================================
    Name                  Type           Description
    ===================== ============== =============================================================
    Failures              Counter        Number of range query failures encountered.
    Timeouts              Counter        Number of timeouts encountered.
    Unavailables          Counter        Number of unavailable exceptions encountered.
    |nbsp|                Latency        Range query latency.
    ===================== ============== =============================================================

:RequestType: Write
:Description: Metrics related to regular write requests.
:Metrics:
    ===================== ============== =============================================================
    Name                  Type           Description
    ===================== ============== =============================================================
    Failures              Counter        Number of write failures encountered.
    MutationSizeHistogram Histogram      Total size in bytes of the requests mutations.
    Timeouts              Counter        Number of timeouts encountered.
    Unavailables          Counter        Number of unavailable exceptions encountered.
    |nbsp|                Latency        Write latency.
    ===================== ============== =============================================================


:RequestType: ViewWrite
:Description: Metrics related to materialized view write wrtes.
:Metrics:
    ===================== ============== =============================================================
    Failures              Counter        Number of transaction failures encountered.
    Timeouts              Counter        Number of timeouts encountered.
    Unavailables          Counter        Number of unavailable exceptions encountered.
    ViewPendingMutations  Gauge<Long>    ViewReplicasAttempted - ViewReplicasSuccess.
    ViewReplicasAttempted Counter        Total number of attempted view replica writes.
    ViewReplicasSuccess   Counter        Total number of succeded view replica writes.
    ViewWriteLatency      Timer          Time between when mutation is applied to base table and when CL.ONE is achieved on view.
    ===================== ============== =============================================================

Cache Metrics
^^^^^^^^^^^^^

Cassandra caches have metrics to track the effectivness of the caches. Though the ``Table Metrics`` might be more useful.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.Cache.<MetricName>.<CacheName>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=Cache,scope=<CacheName>,name=<MetricName>``

========================== ============== ===========
Name                       Type           Description
========================== ============== ===========
Capacity                   Gauge<Long>    Cache capacity in bytes.
Entries                    Gauge<Integer> Total number of cache entries.
FifteenMinuteCacheHitRate  Gauge<Double>  15m cache hit rate.
FiveMinuteCacheHitRate     Gauge<Double>  5m cache hit rate.
HitRate                    Gauge<Double>  All time cache hit rate.
Hits                       Meter          Total number of cache hits.
MissLatency                Timer          Latency of misses.
Misses                     Meter          Total number of cache misses.
OneMinuteCacheHitRate      Gauge<Double>  1m cache hit rate.
Requests                   Gauge<Long>    Total number of cache requests.
Size                       Gauge<Long>    Total size of occupied cache, in bytes.
========================== ============== ===========

The following caches are covered:

============================ ===========
Name                         Description
============================ ===========
ChunkCache                   In process uncompressed page cache.
CounterCache                 Keeps hot counters in memory for performance.
KeyCache                     Cache for partition to sstable offsets.
RowCache                     Cache for rows kept in memory.
============================ ===========

.. NOTE::
    Misses and MissLatency are only defined for the ChunkCache

CQL Metrics
^^^^^^^^^^^

Metrics specific to CQL prepared statement caching.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.CQL.<MetricName>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=CQL,name=<MetricName>``

========================== ============== ===========
Name                       Type           Description
========================== ============== ===========
PreparedStatementsCount    Gauge<Integer> Number of cached prepared statements.
PreparedStatementsEvicted  Counter        Number of prepared statements evicted from the prepared statement cache
PreparedStatementsExecuted Counter        Number of prepared statements executed.
PreparedStatementsRatio    Gauge<Double>  Percentage of statements that are prepared vs unprepared.
RegularStatementsExecuted  Counter        Number of **non** prepared statements executed.
========================== ============== ===========

.. _dropped-metrics:

DroppedMessage Metrics
^^^^^^^^^^^^^^^^^^^^^^

Metrics specific to tracking dropped messages for different types of requests.
Dropped writes are stored and retried by ``Hinted Handoff``

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.DroppedMessage.<MetricName>.<Type>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=DroppedMessage,scope=<Type>,name=<MetricName>``

========================== ============== ===========
Name                       Type           Description
========================== ============== ===========
CrossNodeDroppedLatency    Timer          The dropped latency across nodes.
Dropped                    Meter          Number of dropped messages.
InternalDroppedLatency     Timer          The dropped latency within node.
========================== ============== ===========

The different types of messages tracked are:

============================ ===========
Name                         Description
============================ ===========
BATCH_REMOVE                 Batchlog cleanup (after succesfully applied)
BATCH_STORE                  Batchlog write
COUNTER_MUTATION             Counter writes
HINT                         Hint replay
MUTATION                     Regular writes
PAGED_SLICE                  Paged read
RANGE_SLICE                  Token range read
READ                         Regular reads
READ_REPAIR                  Read repair
REQUEST_RESPONSE             RPC Callbacks
_TRACE                       Tracing writes
============================ ===========

Streaming Metrics
^^^^^^^^^^^^^^^^^

Metrics reported during ``Streaming`` operations, such as repair, bootstrap, rebuild.

These metrics are specific to a peer endpoint, with the source node being the node you are pulling the metrics from.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.Streaming.<MetricName>.<PeerIP>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=Streaming,scope=<PeerIP>,name=<MetricName>``

========================== ============== ===========
Name                       Type           Description
========================== ============== ===========
IncomingBytes              Counter        Number of bytes streamed to this node from the peer.
IncomingProcessTime        Timer          The time spent on processing the incoming stream message from the peer.
OutgoingBytes              Counter        Number of bytes streamed to the peer endpoint from this node.
========================== ============== ===========


Compaction Metrics
^^^^^^^^^^^^^^^^^^

Metrics specific to ``Compaction`` work.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.Compaction.<MetricName>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=Compaction,name=<MetricName>``

========================== ======================================== ===============================================
Name                       Type                                     Description
========================== ======================================== ===============================================
BytesCompacted             Counter                                  Total number of bytes compacted since server [re]start.
CompletedTasks             Gauge<Long>                              Number of completed compactions since server [re]start.
PendingTasks               Gauge<Integer>                           Estimated number of compactions remaining to perform.
PendingTasksByTableName    Gauge<Map<String, Map<String, Integer>>> Estimated number of compactions remaining to perform, grouped by keyspace and then table name. This info is also kept in ``Table Metrics``.
TotalCompactionsCompleted  Meter                                    Throughput of completed compactions since server [re]start.
========================== ======================================== ===============================================

CommitLog Metrics
^^^^^^^^^^^^^^^^^

Metrics specific to the ``CommitLog``

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.CommitLog.<MetricName>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=CommitLog,name=<MetricName>``

========================== ============== ===========
Name                       Type           Description
========================== ============== ===========
CompletedTasks             Gauge<Long>    Total number of commit log messages written since [re]start.
OverSizedMutations         Meter          Throughput for mutations that exceed limit.
PendingTasks               Gauge<Long>    Number of commit log messages written but yet to be fsync'd.
TotalCommitLogSize         Gauge<Long>    Current size, in bytes, used by all the commit log segments.
WaitingOnCommit            Timer          Time spent waiting on CL fsync; for Periodic this is only occurs when the sync is lagging its sync interval.
WaitingOnSegmentAllocation Timer          Time spent waiting for a CommitLogSegment to be allocated - under normal conditions this should be zero.
========================== ============== ===========

Storage Metrics
^^^^^^^^^^^^^^^

Metrics specific to the storage engine.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.Storage.<MetricName>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=Storage,name=<MetricName>``

========================== ============== ===========
Name                       Type           Description
========================== ============== ===========
Exceptions                 Counter        Number of internal exceptions caught. Under normal exceptions this should be zero.
Load                       Counter        Size, in bytes, of the on disk data size this node manages.
TotalHints                 Counter        Number of hint messages written to this node since [re]start. Includes one entry for each host to be hinted per hint.
TotalHintsInProgress       Counter        Number of hints attemping to be sent currently.
========================== ============== ===========

.. _hintsservice-metrics:

HintsService Metrics
^^^^^^^^^^^^^^^^^^^^

Metrics specific to the Hints delivery service.  There are also some metrics related to hints tracked in ``Storage Metrics``

These metrics include the peer endpoint **in the metric name**

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.HintsService.<MetricName>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=HintsService,name=<MetricName>``

=========================== ============== ===========
Name                        Type           Description
=========================== ============== ===========
Hint_delays                 Histogram      Histogram of hint delivery delays (in milliseconds)
Hint_delays-<PeerIP>        Histogram      Histogram of hint delivery delays (in milliseconds) per peer
HintsFailed                 Meter          A meter of the hints that failed deliver
HintsSucceeded              Meter          A meter of the hints successfully delivered
HintsTimedOut               Meter          A meter of the hints that timed out
=========================== ============== ===========

SSTable Index Metrics
^^^^^^^^^^^^^^^^^^^^^

Metrics specific to the SSTable index metadata.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.Index.<MetricName>.RowIndexEntry``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=Index scope=RowIndexEntry,name=<MetricName>``

=========================== ============== ===========
Name                        Type           Description
=========================== ============== ===========
IndexInfoCount              Histogram      Histogram of the number of on-heap index entries managed across all SSTables.
IndexInfoGets               Histogram      Histogram of the number index seeks performed per SSTable.
IndexedEntrySize            Histogram      Histogram of the on-heap size, in bytes, of the index across all SSTables.
=========================== ============== ===========

BufferPool Metrics
^^^^^^^^^^^^^^^^^^

Metrics specific to the internal recycled buffer pool Cassandra manages.  This pool is meant to keep allocations and GC
lower by recycling on and off heap buffers.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.BufferPool.<MetricName>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=BufferPool,name=<MetricName>``

=========================== ============== ===========
Name                        Type           Description
=========================== ============== ===========
Misses                      Meter          The rate of misses in the pool. The higher this is the more allocations incurred.
Size                        Gauge<Long>    Size, in bytes, of the managed buffer pool
=========================== ============== ===========

Client Metrics
^^^^^^^^^^^^^^

Metrics specifc to client managment.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.Client.<MetricName>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=Client,name=<MetricName>``

============================== ================================ ===========
Name                           Type                             Description
============================== ================================ ===========
ClientsByProtocolVersion       Gauge<List<Map<String, String>>> List of up to last 100 connections including protocol version. Can be reset with clearConnectionHistory operation in org.apache.cassandra.db:StorageService mbean.
ConnectedNativeClients         Gauge<Integer>                   Number of clients connected to this nodes native protocol server
ConnectedNativeClientsByUser   Gauge<Map<String, Int>           Number of connnective native clients by username
Connections                    Gauge<List<Map<String, String>>  List of all connections and their state information
RequestsSize                   Gauge<Long>                      How many concurrent bytes used in currently processing requests
RequestsSizeByIpDistribution   Histogram                        How many concurrent bytes used in currently processing requests by different ips
============================== ================================ ===========

Batch Metrics
^^^^^^^^^^^^^

Metrics specific to batch statements.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.Batch.<MetricName>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=Batch,name=<MetricName>``

=========================== ============== ===========
Name                        Type           Description
=========================== ============== ===========
PartitionsPerCounterBatch   Histogram      Distribution of the number of partitions processed per counter batch
PartitionsPerLoggedBatch    Histogram      Distribution of the number of partitions processed per logged batch
PartitionsPerUnloggedBatch  Histogram      Distribution of the number of partitions processed per unlogged batch
=========================== ============== ===========

Read Repair Metrics
^^^^^^^^^^^^^^^^^^^

Metrics specific to read repair operations.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.ReadRepair.<MetricName>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=ReadRepair,name=<MetricName>``

=========================== ============== ===========
Name                        Type           Description
=========================== ============== ===========
ReconcileRead               Meter          The rate of read-only read repairs, which do not mutate the replicas
RepairedBlocking            Meter          The rate of blocking read repairs
SpeculatedRead              Meter          The rate of speculated reads during read repairs
SpeculatedWrite             Meter          The rate of speculated writes during read repairs
=========================== ============== ===========

Messaging Metrics
^^^^^^^^^^^^^^^^^

Metrics for internode messaging.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.Messaging.<MetricName>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=Messaging,name=<MetricName>``

=========================== ============== ===========
Name                        Type           Description
=========================== ============== ===========
<DC>-Latency                Timer          Latency of all internode messageing between this node and the datacenters.
<VERB>-WaitLatency          Timer          Latency between the message creation time and the time being executed by VERB
CrossNodeLatency            Timer          Latency of all internode messaging between this node and the peers
=========================== ============== ===========

Internode Inbound Connection Metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Metrics specific to inbound connections of internode messaging.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.InboundConnection.<MetricName>.<IP>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=InboundConnection,scope=<IP>,name=<MetricName>``

=========================== ============== ===========
Name                        Type           Description
=========================== ============== ===========
CorruptFramesRecovered      Guage<Long>    Estimated number of corrupted frames recovered
CorruptFramesUnrecovered    Guage<Long>    Estimated number of corrupted frames unrecovered
ErrorBytes                  Guage<Long>    Estimated number of error bytes
ErrorCount                  Guage<Long>    Estimated number of error count
ExpiredBytes                Guage<Long>    Estimated number of expired bytes
ExpiredCount                Guage<Long>    Estimated number of expired count
ScheduledBytes              Guage<Long>    Estimated number of bytes that are pending execution
ScheduledCount              Guage<Long>    Estimated number of message that are pending execution
ProcessedBytes              Guage<Long>    Estimated number of bytes processed
ProcessedCount              Guage<Long>    Estimated number of messages processed
ReceivedBytes               Guage<Long>    Estimated number of bytes received
ReceivedCount               Guage<Long>    Estimated number of messages received
ThrottledCount              Guage<Long>    Estimated number of messages throttled
ThrottledNanos              Guage<Long>    Estimated duration of throttling in nanoseconds
=========================== ============== ===========

Internode Outbound Connection Metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Metrics specific to outbound connections of internode messaging.

Reported name format:

**Metric Name**
    ``org.apache.cassandra.metrics.Connection.<MetricName>.<IP>``

**JMX MBean**
    ``org.apache.cassandra.metrics:type=Connection,scope=<IP>,name=<MetricName>``

======================================================= ============== ===========
Name                                                    Type           Description
======================================================= ============== ===========
[Large|Small|Urgent]MessagePendingTasks                 Guage<Long>    Estimated number of pending (Large|Small|Urgent) messages queued
[Large|Small|Urgent]MessagePendingBytes                 Guage<Long>    Estimated number of bytes of pending (Large|Small|Urgent) messages queued
[Large|Small|Urgent]MessageCompletedTasks               Guage<Long>    Estimated number of (Large|Small|Urgent) messages sent
[Large|Small|Urgent]MessageCompletedBytes               Guage<Long>    Estimated number of bytes of (Large|Small|Urgent) messages sent
[Large|Small|Urgent]MessageDroppedTasks                 Guage<Long>    Estimated number of dropped (Large|Small|Urgent) messages
[Large|Small|Urgent]MessageDroppedTasksDueToOverload    Guage<Long>    Estimated number of dropped (Large|Small|Urgent) messages due to overload
[Large|Small|Urgent]MessageDroppedBytesDueToOverload    Guage<Long>    Estimated number of bytes of dropped (Large|Small|Urgent) messages due to overload
[Large|Small|Urgent]MessageDroppedTasksDueToTimeout     Guage<Long>    Estimated number of dropped (Large|Small|Urgent) messages due to timeout
[Large|Small|Urgent]MessageDroppedBytesDueToTimeout     Guage<Long>    Estimated number of bytes of dropped (Large|Small|Urgent) messages due to timeout
[Large|Small|Urgent]MessageDroppedTasksDueToError       Guage<Long>    Estimated number of dropped (Large|Small|Urgent) messages due to error
[Large|Small|Urgent]MessageDroppedBytesDueToError       Guage<Long>    Estimated number of bytes of dropped (Large|Small|Urgent) messages due to error
======================================================= ============== ===========

JVM Metrics
^^^^^^^^^^^

JVM metrics such as memory and garbage collection statistics can either be accessed by connecting to the JVM using JMX or can be exported using `Metric Reporters`_.

BufferPool
++++++++++

**Metric Name**
    ``jvm.buffers.<direct|mapped>.<MetricName>``

**JMX MBean**
    ``java.nio:type=BufferPool,name=<direct|mapped>``

========================== ============== ===========
Name                       Type           Description
========================== ============== ===========
Capacity                   Gauge<Long>    Estimated total capacity of the buffers in this pool
Count                      Gauge<Long>    Estimated number of buffers in the pool
Used                       Gauge<Long>    Estimated memory that the Java virtual machine is using for this buffer pool
========================== ============== ===========

FileDescriptorRatio
+++++++++++++++++++

**Metric Name**
    ``jvm.fd.<MetricName>``

**JMX MBean**
    ``java.lang:type=OperatingSystem,name=<OpenFileDescriptorCount|MaxFileDescriptorCount>``

========================== ============== ===========
Name                       Type           Description
========================== ============== ===========
Usage                      Ratio          Ratio of used to total file descriptors
========================== ============== ===========

GarbageCollector
++++++++++++++++

**Metric Name**
    ``jvm.gc.<gc_type>.<MetricName>``

**JMX MBean**
    ``java.lang:type=GarbageCollector,name=<gc_type>``

========================== ============== ===========
Name                       Type           Description
========================== ============== ===========
Count                      Gauge<Long>    Total number of collections that have occurred
Time                       Gauge<Long>    Approximate accumulated collection elapsed time in milliseconds
========================== ============== ===========

Memory
++++++

**Metric Name**
    ``jvm.memory.<heap/non-heap/total>.<MetricName>``

**JMX MBean**
    ``java.lang:type=Memory``

========================== ============== ===========
Committed                  Gauge<Long>    Amount of memory in bytes that is committed for the JVM to use
Init                       Gauge<Long>    Amount of memory in bytes that the JVM initially requests from the OS
Max                        Gauge<Long>    Maximum amount of memory in bytes that can be used for memory management
Usage                      Ratio          Ratio of used to maximum memory
Used                       Gauge<Long>    Amount of used memory in bytes
========================== ============== ===========

MemoryPool
++++++++++

**Metric Name**
    ``jvm.memory.pools.<memory_pool>.<MetricName>``

**JMX MBean**
    ``java.lang:type=MemoryPool,name=<memory_pool>``

========================== ============== ===========
Committed                  Gauge<Long>    Amount of memory in bytes that is committed for the JVM to use
Init                       Gauge<Long>    Amount of memory in bytes that the JVM initially requests from the OS
Max                        Gauge<Long>    Maximum amount of memory in bytes that can be used for memory management
Usage                      Ratio          Ratio of used to maximum memory
Used                       Gauge<Long>    Amount of used memory in bytes
========================== ============== ===========

JMX
^^^

Any JMX based client can access metrics from cassandra.

If you wish to access JMX metrics over http it's possible to download `Mx4jTool <http://mx4j.sourceforge.net/>`__ and
place ``mx4j-tools.jar`` into the classpath.  On startup you will see in the log::

    HttpAdaptor version 3.0.2 started on port 8081

To choose a different port (8081 is the default) or a different listen address (0.0.0.0 is not the default) edit
``conf/cassandra-env.sh`` and uncomment::

    #MX4J_ADDRESS="-Dmx4jaddress=0.0.0.0"

    #MX4J_PORT="-Dmx4jport=8081"


Metric Reporters
^^^^^^^^^^^^^^^^

As mentioned at the top of this section on monitoring the Cassandra metrics can be exported to a number of monitoring
system a number of `built in <http://metrics.dropwizard.io/3.1.0/getting-started/#other-reporting>`__ and `third party
<http://metrics.dropwizard.io/3.1.0/manual/third-party/>`__ reporter plugins.

The configuration of these plugins is managed by the `metrics reporter config project
<https://github.com/addthis/metrics-reporter-config>`__. There is a sample configuration file located at
``conf/metrics-reporter-config-sample.yaml``.

Once configured, you simply start cassandra with the flag
``-Dcassandra.metricsReporterConfigFile=metrics-reporter-config.yaml``. The specified .yaml file plus any 3rd party
reporter jars must all be in Cassandra's classpath.
