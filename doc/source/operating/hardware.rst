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

Hardware Choices
----------------

Like most databases, Cassandra throughput improves with more CPU cores, more RAM, and faster disks. While Cassandra can
be made to run on small servers for testing or development environments (including Raspberry Pis), a minimal production
server requires at least 2 cores, and at least 8GB of RAM. Typical production servers have 8 or more cores and at least
32GB of RAM.

CPU
^^^
Cassandra is highly concurrent, handling many simultaneous requests (both read and write) using multiple threads running
on as many CPU cores as possible. The Cassandra write path tends to be heavily optimized (writing to the commitlog and
then inserting the data into the memtable), so writes, in particular, tend to be CPU bound. Consequently, adding
additional CPU cores often increases throughput of both reads and writes.

Memory
^^^^^^
Cassandra runs within a Java VM, which will pre-allocate a fixed size heap (java's Xmx system parameter). In addition to
the heap, Cassandra will use significant amounts of RAM offheap for compression metadata, bloom filters, row, key, and
counter caches, and an in process page cache. Finally, Cassandra will take advantage of the operating system's page
cache, storing recently accessed portions files in RAM for rapid re-use.

For optimal performance, operators should benchmark and tune their clusters based on their individual workload. However,
basic guidelines suggest:

-  ECC RAM should always be used, as Cassandra has few internal safeguards to protect against bit level corruption
-  The Cassandra heap should be no less than 2GB, and no more than 50% of your system RAM
-  Heaps smaller than 12GB should consider ParNew/ConcurrentMarkSweep garbage collection
-  Heaps larger than 12GB should consider G1GC

Disks
^^^^^
Cassandra persists data to disk for two very different purposes. The first is to the commitlog when a new write is made
so that it can be replayed after a crash or system shutdown. The second is to the data directory when thresholds are
exceeded and memtables are flushed to disk as SSTables.

Commitlogs receive every write made to a Cassandra node and have the potential to block client operations, but they are
only ever read on node start-up. SSTable (data file) writes on the other hand occur asynchronously, but are read to
satisfy client look-ups. SSTables are also periodically merged and rewritten in a process called compaction.  The data
held in the commitlog directory is data that has not been permanently saved to the SSTable data directories - it will be
periodically purged once it is flushed to the SSTable data files.

Cassandra performs very well on both spinning hard drives and solid state disks. In both cases, Cassandra's sorted
immutable SSTables allow for linear reads, few seeks, and few overwrites, maximizing throughput for HDDs and lifespan of
SSDs by avoiding write amplification. However, when using spinning disks, it's important that the commitlog
(``commitlog_directory``) be on one physical disk (not simply a partition, but a physical disk), and the data files
(``data_file_directories``) be set to a separate physical disk. By separating the commitlog from the data directory,
writes can benefit from sequential appends to the commitlog without having to seek around the platter as reads request
data from various SSTables on disk.

In most cases, Cassandra is designed to provide redundancy via multiple independent, inexpensive servers. For this
reason, using NFS or a SAN for data directories is an antipattern and should typically be avoided.  Similarly, servers
with multiple disks are often better served by using RAID0 or JBOD than RAID1 or RAID5 - replication provided by
Cassandra obsoletes the need for replication at the disk layer, so it's typically recommended that operators take
advantage of the additional throughput of RAID0 rather than protecting against failures with RAID1 or RAID5.

Common Cloud Choices
^^^^^^^^^^^^^^^^^^^^

Many large users of Cassandra run in various clouds, including AWS, Azure, and GCE - Cassandra will happily run in any
of these environments. Users should choose similar hardware to what would be needed in physical space. In EC2, popular
options include:

- m1.xlarge instances, which provide 1.6TB of local ephemeral spinning storage and sufficient RAM to run moderate
  workloads
- i2 instances, which provide both a high RAM:CPU ratio and local ephemeral SSDs
- m4.2xlarge / c4.4xlarge instances, which provide modern CPUs, enhanced networking and work well with EBS GP2 (SSD)
  storage

Generally, disk and network performance increases with instance size and generation, so newer generations of instances
and larger instance types within each family often perform better than their smaller or older alternatives.
