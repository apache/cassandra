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

.. _use-nodetool:

Use Nodetool
============

Cassandra's ``nodetool`` allows you to narrow problems from the cluster down
to a particular node and gives a lot of insight into the state of the Cassandra
process itself. There are dozens of useful commands (see ``nodetool help``
for all the commands), but briefly some of the most useful for troubleshooting:

.. _nodetool-status:

Cluster Status
--------------

You can use ``nodetool status`` to assess status of the cluster::

    $ nodetool status <optional keyspace>

    Datacenter: dc1
    =======================
    Status=Up/Down
    |/ State=Normal/Leaving/Joining/Moving
    --  Address    Load       Tokens       Owns (effective)  Host ID                               Rack
    UN  127.0.1.1  4.69 GiB   1            100.0%            35ea8c9f-b7a2-40a7-b9c5-0ee8b91fdd0e  r1
    UN  127.0.1.2  4.71 GiB   1            100.0%            752e278f-b7c5-4f58-974b-9328455af73f  r2
    UN  127.0.1.3  4.69 GiB   1            100.0%            9dc1a293-2cc0-40fa-a6fd-9e6054da04a7  r3

In this case we can see that we have three nodes in one datacenter with about
4.6GB of data each and they are all "up". The up/down status of a node is
independently determined by every node in the cluster, so you may have to run
``nodetool status`` on multiple nodes in a cluster to see the full view.

You can use ``nodetool status`` plus a little grep to see which nodes are
down::

    $ nodetool status | grep -v '^UN'
    Datacenter: dc1
    ===============
    Status=Up/Down
    |/ State=Normal/Leaving/Joining/Moving
    --  Address    Load       Tokens       Owns (effective)  Host ID                               Rack
    Datacenter: dc2
    ===============
    Status=Up/Down
    |/ State=Normal/Leaving/Joining/Moving
    --  Address    Load       Tokens       Owns (effective)  Host ID                               Rack
    DN  127.0.0.5  105.73 KiB  1            33.3%             df303ac7-61de-46e9-ac79-6e630115fd75  r1

In this case there are two datacenters and there is one node down in datacenter
``dc2`` and rack ``r1``. This may indicate an issue on ``127.0.0.5``
warranting investigation.

.. _nodetool-proxyhistograms:

Coordinator Query Latency
-------------------------
You can view latency distributions of coordinator read and write latency
to help narrow down latency issues using ``nodetool proxyhistograms``::

    $ nodetool proxyhistograms
    Percentile       Read Latency      Write Latency      Range Latency   CAS Read Latency  CAS Write Latency View Write Latency
                         (micros)           (micros)           (micros)           (micros)           (micros)           (micros)
    50%                    454.83             219.34               0.00               0.00               0.00               0.00
    75%                    545.79             263.21               0.00               0.00               0.00               0.00
    95%                    654.95             315.85               0.00               0.00               0.00               0.00
    98%                    785.94             379.02               0.00               0.00               0.00               0.00
    99%                   3379.39            2346.80               0.00               0.00               0.00               0.00
    Min                     42.51             105.78               0.00               0.00               0.00               0.00
    Max                  25109.16           43388.63               0.00               0.00               0.00               0.00

Here you can see the full latency distribution of reads, writes, range requests
(e.g. ``select * from keyspace.table``), CAS read (compare phase of CAS) and
CAS write (set phase of compare and set). These can be useful for narrowing
down high level latency problems, for example in this case if a client had a
20 millisecond timeout on their reads they might experience the occasional
timeout from this node but less than 1% (since the 99% read latency is 3.3
milliseconds < 20 milliseconds).

.. _nodetool-tablehistograms:

Local Query Latency
-------------------

If you know which table is having latency/error issues, you can use
``nodetool tablehistograms`` to get a better idea of what is happening
locally on a node::

    $ nodetool tablehistograms keyspace table
    Percentile  SSTables     Write Latency      Read Latency    Partition Size        Cell Count
                                  (micros)          (micros)           (bytes)
    50%             0.00             73.46            182.79             17084               103
    75%             1.00             88.15            315.85             17084               103
    95%             2.00            126.93            545.79             17084               103
    98%             2.00            152.32            654.95             17084               103
    99%             2.00            182.79            785.94             17084               103
    Min             0.00             42.51             24.60             14238                87
    Max             2.00          12108.97          17436.92             17084               103

This shows you percentile breakdowns particularly critical metrics.

The first column contains how many sstables were read per logical read. A very
high number here indicates that you may have chosen the wrong compaction
strategy, e.g. ``SizeTieredCompactionStrategy`` typically has many more reads
per read than ``LeveledCompactionStrategy`` does for update heavy workloads.

The second column shows you a latency breakdown of *local* write latency. In
this case we see that while the p50 is quite good at 73 microseconds, the
maximum latency is quite slow at 12 milliseconds. High write max latencies
often indicate a slow commitlog volume (slow to fsync) or large writes
that quickly saturate commitlog segments.

The third column shows you a latency breakdown of *local* read latency. We can
see that local Cassandra reads are (as expected) slower than local writes, and
the read speed correlates highly with the number of sstables read per read.

The fourth and fifth columns show distributions of partition size and column
count per partition. These are useful for determining if the table has on
average skinny or wide partitions and can help you isolate bad data patterns.
For example if you have a single cell that is 2 megabytes, that is probably
going to cause some heap pressure when it's read.

.. _nodetool-tpstats:

Threadpool State
----------------

You can use ``nodetool tpstats`` to view the current outstanding requests on
a particular node. This is useful for trying to find out which resource
(read threads, write threads, compaction, request response threads) the
Cassandra process lacks. For example::

    $ nodetool tpstats
    Pool Name                         Active   Pending      Completed   Blocked  All time blocked
    ReadStage                              2         0             12         0                 0
    MiscStage                              0         0              0         0                 0
    CompactionExecutor                     0         0           1940         0                 0
    MutationStage                          0         0              0         0                 0
    GossipStage                            0         0          10293         0                 0
    Repair-Task                            0         0              0         0                 0
    RequestResponseStage                   0         0             16         0                 0
    ReadRepairStage                        0         0              0         0                 0
    CounterMutationStage                   0         0              0         0                 0
    MemtablePostFlush                      0         0             83         0                 0
    ValidationExecutor                     0         0              0         0                 0
    MemtableFlushWriter                    0         0             30         0                 0
    ViewMutationStage                      0         0              0         0                 0
    CacheCleanupExecutor                   0         0              0         0                 0
    MemtableReclaimMemory                  0         0             30         0                 0
    PendingRangeCalculator                 0         0             11         0                 0
    SecondaryIndexManagement               0         0              0         0                 0
    HintsDispatcher                        0         0              0         0                 0
    Native-Transport-Requests              0         0            192         0                 0
    MigrationStage                         0         0             14         0                 0
    PerDiskMemtableFlushWriter_0           0         0             30         0                 0
    Sampler                                0         0              0         0                 0
    ViewBuildExecutor                      0         0              0         0                 0
    InternalResponseStage                  0         0              0         0                 0
    AntiEntropyStage                       0         0              0         0                 0

    Message type           Dropped                  Latency waiting in queue (micros)
                                                 50%               95%               99%               Max
    READ                         0               N/A               N/A               N/A               N/A
    RANGE_SLICE                  0              0.00              0.00              0.00              0.00
    _TRACE                       0               N/A               N/A               N/A               N/A
    HINT                         0               N/A               N/A               N/A               N/A
    MUTATION                     0               N/A               N/A               N/A               N/A
    COUNTER_MUTATION             0               N/A               N/A               N/A               N/A
    BATCH_STORE                  0               N/A               N/A               N/A               N/A
    BATCH_REMOVE                 0               N/A               N/A               N/A               N/A
    REQUEST_RESPONSE             0              0.00              0.00              0.00              0.00
    PAGED_RANGE                  0               N/A               N/A               N/A               N/A
    READ_REPAIR                  0               N/A               N/A               N/A               N/A

This command shows you all kinds of interesting statistics. The first section
shows a detailed breakdown of threadpools for each Cassandra stage, including
how many threads are current executing (Active) and how many are waiting to
run (Pending). Typically if you see pending executions in a particular
threadpool that indicates a problem localized to that type of operation. For
example if the ``RequestResponseState`` queue is backing up, that means
that the coordinators are waiting on a lot of downstream replica requests and
may indicate a lack of token awareness, or very high consistency levels being
used on read requests (for example reading at ``ALL`` ties up RF
``RequestResponseState`` threads whereas ``LOCAL_ONE`` only uses a single
thread in the ``ReadStage`` threadpool). On the other hand if you see a lot of
pending compactions that may indicate that your compaction threads cannot keep
up with the volume of writes and you may need to tune either the compaction
strategy or the ``concurrent_compactors`` or ``compaction_throughput`` options.

The second section shows drops (errors) and latency distributions for all the
major request types. Drops are cumulative since process start, but if you
have any that indicate a serious problem as the default timeouts to qualify as
a drop are quite high (~5-10 seconds). Dropped messages often warrants further
investigation.

.. _nodetool-compactionstats:

Compaction State
----------------

As Cassandra is a LSM datastore, Cassandra sometimes has to compact sstables
together, which can have adverse effects on performance. In particular,
compaction uses a reasonable quantity of CPU resources, invalidates large
quantities of the OS `page cache <https://en.wikipedia.org/wiki/Page_cache>`_,
and can put a lot of load on your disk drives. There are great
:ref:`os tools <os-iostat>` to determine if this is the case, but often it's a
good idea to check if compactions are even running using
``nodetool compactionstats``::

    $ nodetool compactionstats
    pending tasks: 2
    - keyspace.table: 2

    id                                   compaction type keyspace table completed total    unit  progress
    2062b290-7f3a-11e8-9358-cd941b956e60 Compaction      keyspace table 21848273  97867583 bytes 22.32%
    Active compaction remaining time :   0h00m04s

In this case there is a single compaction running on the ``keyspace.table``
table, has completed 21.8 megabytes of 97 and Cassandra estimates (based on
the configured compaction throughput) that this will take 4 seconds. You can
also pass ``-H`` to get the units in a human readable format.

Generally each running compaction can consume a single core, but the more
you do in parallel the faster data compacts. Compaction is crucial to ensuring
good read performance so having the right balance of concurrent compactions
such that compactions complete quickly but don't take too many resources
away from query threads is very important for performance. If you notice
compaction unable to keep up, try tuning Cassandra's ``concurrent_compactors``
or ``compaction_throughput`` options.
