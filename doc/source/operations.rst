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

Operating Cassandra
===================

Replication Strategies
----------------------

.. todo:: todo

Snitches
--------

.. todo:: todo

Adding, replacing, moving and removing nodes
--------------------------------------------

.. todo:: todo

Repair
------

.. todo:: todo

Read repair
-----------

.. todo:: todo

Hints
-----

.. todo:: todo

Compaction
----------

Size Tiered
^^^^^^^^^^^

.. todo:: todo

Leveled
^^^^^^^

.. todo:: todo

TimeWindow
^^^^^^^^^^
.. todo:: todo

DateTiered
^^^^^^^^^^
.. todo:: todo

Tombstones and Garbage Collection (GC) Grace
--------------------------------------------

Why Tombstones
^^^^^^^^^^^^^^

When a delete request is received by Cassandra it does not actually remove the data from the underlying store. Instead
it writes a special piece of data known as a tombstone. The Tombstone represents the delete and causes all values which
occurred before the tombstone to not appear in queries to the database. This approach is used instead of removing values
because of the distributed nature of Cassandra.

Deletes without tombstones
~~~~~~~~~~~~~~~~~~~~~~~~~~

Imagine a three node cluster which has the value [A] replicated to every node.::

    [A], [A], [A]

If one of the nodes fails and and our delete operation only removes existing values we can end up with a cluster that
looks like::

    [], [], [A]

Then a repair operation would replace the value of [A] back onto the two
nodes which are missing the value.::

    [A], [A], [A]

This would cause our data to be resurrected even though it had been
deleted.

Deletes with Tombstones
~~~~~~~~~~~~~~~~~~~~~~~

Starting again with a three node cluster which has the value [A] replicated to every node.::

    [A], [A], [A]

If instead of removing data we add a tombstone record, our single node failure situation will look like this.::

    [A, Tombstone[A]], [A, Tombstone[A]], [A]

Now when we issue a repair the Tombstone will be copied to the replica, rather than the deleted data being
resurrected.::

    [A, Tombstone[A]], [A, Tombstone[A]], [A, Tombstone[A]]

Our repair operation will correctly put the state of the system to what we expect with the record [A] marked as deleted
on all nodes. This does mean we will end up accruing Tombstones which will permanently accumulate disk space. To avoid
keeping tombstones forever we have a parameter known as ``gc_grace_seconds`` for every table in Cassandra.

The gc_grace_seconds parameter and Tombstone Removal
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The table level ``gc_grace_seconds`` parameter controls how long Cassandra will retain tombstones through compaction
events before finally removing them. This duration should directly reflect the amount of time a user expects to allow
before recovering a failed node. After ``gc_grace_seconds`` has expired the tombstone can be removed meaning there will
no longer be any record that a certain piece of data was deleted. This means if a node remains down or disconnected for
longer than ``gc_grace_seconds`` it's deleted data will be repaired back to the other nodes and re-appear in the
cluster. This is basically the same as in the "Deletes without Tombstones" section. Note that tombstones will not be
removed until a compaction event even if ``gc_grace_seconds`` has elapsed.

The default value for ``gc_grace_seconds`` is 864000 which is equivalent to 10 days. This can be set when creating or
altering a table using ``WITH gc_grace_seconds``.


Bloom Filters
-------------

In the read path, Cassandra merges data on disk (in SSTables) with data in RAM (in memtables). To avoid checking every
SSTable data file for the partition being requested, Cassandra employs a data structure known as a bloom filter.

Bloom filters are a probabilistic data structure that allows Cassandra to determine one of two possible states: - The
data definitely does not exist in the given file, or - The data probably exists in the given file.

While bloom filters can not guarantee that the data exists in a given SSTable, bloom filters can be made more accurate
by allowing them to consume more RAM. Operators have the opportunity to tune this behavior per table by adjusting the
the ``bloom_filter_fp_chance`` to a float between 0 and 1.

The default value for ``bloom_filter_fp_chance`` is 0.1 for tables using LeveledCompactionStrategy and 0.01 for all
other cases.

Bloom filters are stored in RAM, but are stored offheap, so operators should not consider bloom filters when selecting
the maximum heap size.  As accuracy improves (as the ``bloom_filter_fp_chance`` gets closer to 0), memory usage
increases non-linearly - the bloom filter for ``bloom_filter_fp_chance = 0.01`` will require about three times as much
memory as the same table with ``bloom_filter_fp_chance = 0.1``.

Typical values for ``bloom_filter_fp_chance`` are usually between 0.01 (1%) to 0.1 (10%) false-positive chance, where
Cassandra may scan an SSTable for a row, only to find that it does not exist on the disk. The parameter should be tuned
by use case:

- Users with more RAM and slower disks may benefit from setting the ``bloom_filter_fp_chance`` to a numerically lower
  number (such as 0.01) to avoid excess IO operations
- Users with less RAM, more dense nodes, or very fast disks may tolerate a higher ``bloom_filter_fp_chance`` in order to
  save RAM at the expense of excess IO operations
- In workloads that rarely read, or that only perform reads by scanning the entire data set (such as analytics
  workloads), setting the ``bloom_filter_fp_chance`` to a much higher number is acceptable.

Changing
^^^^^^^^

The bloom filter false positive chance is visible in the ``DESCRIBE TABLE`` output as the field
``bloom_filter_fp_chance``. Operators can change the value with an ``ALTER TABLE`` statement:
::

    ALTER TABLE keyspace.table WITH bloom_filter_fp_chance=0.01

Operators should be aware, however, that this change is not immediate: the bloom filter is calculated when the file is
written, and persisted on disk as the Filter component of the SSTable. Upon issuing an ``ALTER TABLE`` statement, new
files on disk will be written with the new ``bloom_filter_fp_chance``, but existing sstables will not be modified until
they are compacted - if an operator needs a change to ``bloom_filter_fp_chance`` to take effect, they can trigger an
SSTable rewrite using ``nodetool scrub`` or ``nodetool upgradesstables -a``, both of which will rebuild the sstables on
disk, regenerating the bloom filters in the progress.


Compression
-----------

Cassandra offers operators the ability to configure compression on a per-table basis. Compression reduces the size of
data on disk by compressing the SSTable in user-configurable compression ``chunk_length_in_kb``. Because Cassandra
SSTables are immutable, the CPU cost of compressing is only necessary when the SSTable is written - subsequent updates
to data will land in different SSTables, so Cassandra will not need to decompress, overwrite, and recompress data when
UPDATE commands are issued. On reads, Cassandra will locate the relevant compressed chunks on disk, decompress the full
chunk, and then proceed with the remainder of the read path (merging data from disks and memtables, read repair, and so
on).

Configuring Compression
^^^^^^^^^^^^^^^^^^^^^^^

Compression is configured on a per-table basis as an optional argument to ``CREATE TABLE`` or ``ALTER TABLE``. By
default, three options are relevant:

- ``class`` specifies the compression class - Cassandra provides three classes (``LZ4Compressor``,
  ``SnappyCompressor``, and ``DeflateCompressor`` ). The default is ``SnappyCompressor``.
- ``chunk_length_in_kb`` specifies the number of kilobytes of data per compression chunk. The default is 64KB.
- ``crc_check_chance`` determines how likely Cassandra is to verify the checksum on each compression chunk during
  reads. The default is 1.0.

Users can set compression using the following syntax:

::

    CREATE TABLE keyspace.table (id int PRIMARY KEY) WITH compression = {'class': 'LZ4Compressor'};

Or

::

    ALTER TABLE keyspace.table WITH compression = {'class': 'SnappyCompressor', 'chunk_length_in_kb': 128, 'crc_check_chance': 0.5};

Once enabled, compression can be disabled with ``ALTER TABLE`` setting ``enabled`` to ``false``:

::

    ALTER TABLE keyspace.table WITH compression = {'enabled':'false'};

Operators should be aware, however, that changing compression is not immediate. The data is compressed when the SSTable
is written, and as SSTables are immutable, the compression will not be modified until the table is compacted. Upon
issuing a change to the compression options via ``ALTER TABLE``, the existing SSTables will not be modified until they
are compacted - if an operator needs compression changes to take effect immediately, the operator can trigger an SSTable
rewrite using ``nodetool scrub`` or ``nodetool upgradesstables -a``, both of which will rebuild the SSTables on disk,
re-compressing the data in the process.

Benefits and Uses
^^^^^^^^^^^^^^^^^

Compression's primary benefit is that it reduces the amount of data written to disk. Not only does the reduced size save
in storage requirements, it often increases read and write throughput, as the CPU overhead of compressing data is faster
than the time it would take to read or write the larger volume of uncompressed data from disk.

Compression is most useful in tables comprised of many rows, where the rows are similar in nature. Tables containing
similar text columns (such as repeated JSON blobs) often compress very well.

Operational Impact
^^^^^^^^^^^^^^^^^^

- Compression metadata is stored offheap and scales with data on disk.  This often requires 1-3GB of offheap RAM per
  terabyte of data on disk, though the exact usage varies with ``chunk_length_in_kb`` and compression ratios.

- Streaming operations involve compressing and decompressing data on compressed tables - in some code paths (such as
  non-vnode bootstrap), the CPU overhead of compression can be a limiting factor.

- The compression path checksums data to ensure correctness - while the traditional Cassandra read path does not have a
  way to ensure correctness of data on disk, compressed tables allow the user to set ``crc_check_chance`` (a float from
  0.0 to 1.0) to allow Cassandra to probabilistically validate chunks on read to verify bits on disk are not corrupt.

Advanced Use
^^^^^^^^^^^^

Advanced users can provide their own compression class by implementing the interface at
``org.apache.cassandra.io.compress.ICompressor``.

Backups
-------

.. todo:: todo

Monitoring
----------

JMX
^^^
.. todo:: todo

Metric Reporters
^^^^^^^^^^^^^^^^
.. todo:: todo

Security
--------

Roles
^^^^^

.. todo:: todo

JMX access
^^^^^^^^^^

.. todo:: todo

Nodetool (and other tooling)
----------------------------

.. todo:: Try to autogenerate this from Nodetool’s help.

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
