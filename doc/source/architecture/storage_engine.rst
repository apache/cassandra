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

Storage Engine
--------------

.. _commit-log:

CommitLog
^^^^^^^^^

Commitlogs are an append only log of all mutations local to a Cassandra node. Any data written to Cassandra will first be written to a commit log before being written to a memtable. This provides durability in the case of unexpected shutdown. On startup, any mutations in the commit log will be applied to memtables.

All mutations write optimized by storing in commitlog segments, reducing the number of seeks needed to write to disk. Commitlog Segments are limited by the "commitlog_segment_size_in_mb" option, once the size is reached, a new commitlog segment is created. Commitlog segments can be archived, deleted, or recycled once all its data has been flushed to SSTables.  Commitlog segments are truncated when Cassandra has written data older than a certain point to the SSTables. Running "nodetool drain" before stopping Cassandra will write everything in the memtables to SSTables and remove the need to sync with the commitlogs on startup.

- ``commitlog_segment_size_in_mb``: The default size is 32, which is almost always fine, but if you are archiving commitlog segments (see commitlog_archiving.properties), then you probably want a finer granularity of archiving; 8 or 16 MB is reasonable. Max mutation size is also configurable via max_mutation_size_in_kb setting in cassandra.yaml. The default is half the size commitlog_segment_size_in_mb * 1024.

***NOTE: If max_mutation_size_in_kb is set explicitly then commitlog_segment_size_in_mb must be set to at least twice the size of max_mutation_size_in_kb / 1024***

*Default Value:* 32

Commitlogs are an append only log of all mutations local to a Cassandra node. Any data written to Cassandra will first be written to a commit log before being written to a memtable. This provides durability in the case of unexpected shutdown. On startup, any mutations in the commit log will be applied.

- ``commitlog_sync``: may be either “periodic” or “batch.”

  - ``batch``: In batch mode, Cassandra won’t ack writes until the commit log has been fsynced to disk. It will wait "commitlog_sync_batch_window_in_ms" milliseconds between fsyncs. This window should be kept short because the writer threads will be unable to do extra work while waiting. You may need to increase concurrent_writes for the same reason.

    - ``commitlog_sync_batch_window_in_ms``: Time to wait between "batch" fsyncs
    *Default Value:* 2

  - ``periodic``: In periodic mode, writes are immediately ack'ed, and the CommitLog is simply synced every "commitlog_sync_period_in_ms" milliseconds.

    - ``commitlog_sync_period_in_ms``: Time to wait between "periodic" fsyncs
    *Default Value:* 10000

*Default Value:* batch

*** NOTE: In the event of an unexpected shutdown, Cassandra can lose up to the sync period or more if the sync is delayed. If using "batch" mode, it is recommended to store commitlogs in a separate, dedicated device.**


- ``commitlog_directory``: This option is commented out by default When running on magnetic HDD, this should be a separate spindle than the data directories. If not set, the default directory is $CASSANDRA_HOME/data/commitlog.

*Default Value:* /var/lib/cassandra/commitlog

- ``commitlog_compression``: Compression to apply to the commitlog. If omitted, the commit log will be written uncompressed. LZ4, Snappy, Deflate and Zstd compressors are supported.

(Default Value: (complex option)::

    #   - class_name: LZ4Compressor
    #     parameters:
    #         -

- ``commitlog_total_space_in_mb``: Total space to use for commit logs on disk.

If space gets above this value, Cassandra will flush every dirty CF in the oldest segment and remove it. So a small total commitlog space will tend to cause more flush activity on less-active columnfamilies.

The default value is the smaller of 8192, and 1/4 of the total space of the commitlog volume.

*Default Value:* 8192

.. _memtables:

Memtables
^^^^^^^^^

Memtables are in-memory structures where Cassandra buffers writes.  In general, there is one active memtable per table.
Eventually, memtables are flushed onto disk and become immutable `SSTables`_.  This can be triggered in several
ways:

- The memory usage of the memtables exceeds the configured threshold  (see ``memtable_cleanup_threshold``)
- The :ref:`commit-log` approaches its maximum size, and forces memtable flushes in order to allow commitlog segments to
  be freed

Memtables may be stored entirely on-heap or partially off-heap, depending on ``memtable_allocation_type``.

SSTables
^^^^^^^^

SSTables are the immutable data files that Cassandra uses for persisting data on disk.

As SSTables are flushed to disk from :ref:`memtables` or are streamed from other nodes, Cassandra triggers compactions
which combine multiple SSTables into one.  Once the new SSTable has been written, the old SSTables can be removed.

Each SSTable is comprised of multiple components stored in separate files:

``Data.db``
  The actual data, i.e. the contents of rows.

``Index.db``
  An index from partition keys to positions in the ``Data.db`` file.  For wide partitions, this may also include an
  index to rows within a partition.

``Summary.db``
  A sampling of (by default) every 128th entry in the ``Index.db`` file.

``Filter.db``
  A Bloom Filter of the partition keys in the SSTable.

``CompressionInfo.db``
  Metadata about the offsets and lengths of compression chunks in the ``Data.db`` file.

``Statistics.db``
  Stores metadata about the SSTable, including information about timestamps, tombstones, clustering keys, compaction,
  repair, compression, TTLs, and more.

``Digest.crc32``
  A CRC-32 digest of the ``Data.db`` file.

``TOC.txt``
  A plain text list of the component files for the SSTable.

Within the ``Data.db`` file, rows are organized by partition.  These partitions are sorted in token order (i.e. by a
hash of the partition key when the default partitioner, ``Murmur3Partition``, is used).  Within a partition, rows are
stored in the order of their clustering keys.

SSTables can be optionally compressed using block-based compression.

SSTable Versions
^^^^^^^^^^^^^^^^

This section was created using the following
`gist <https://gist.github.com/shyamsalimkumar/49a61e5bc6f403d20c55>`_
which utilized this original
`source <http://www.bajb.net/2013/03/cassandra-sstable-format-version-numbers/>`_.

The version numbers, to date are:

Version 0
~~~~~~~~~

* b (0.7.0): added version to sstable filenames
* c (0.7.0): bloom filter component computes hashes over raw key bytes instead of strings
* d (0.7.0): row size in data component becomes a long instead of int
* e (0.7.0): stores undecorated keys in data and index components
* f (0.7.0): switched bloom filter implementations in data component
* g (0.8): tracks flushed-at context in metadata component

Version 1
~~~~~~~~~

* h (1.0): tracks max client timestamp in metadata component
* hb (1.0.3): records compression ration in metadata component
* hc (1.0.4): records partitioner in metadata component
* hd (1.0.10): includes row tombstones in maxtimestamp
* he (1.1.3): includes ancestors generation in metadata component
* hf (1.1.6): marker that replay position corresponds to 1.1.5+ millis-based id (see CASSANDRA-4782)
* ia (1.2.0):

  * column indexes are promoted to the index file
  * records estimated histogram of deletion times in tombstones
  * bloom filter (keys and columns) upgraded to Murmur3
* ib (1.2.1): tracks min client timestamp in metadata component
* ic (1.2.5): omits per-row bloom filter of column names

Version 2
~~~~~~~~~

* ja (2.0.0):

  * super columns are serialized as composites (note that there is no real format change, this is mostly a marker to know if we should expect super columns or not. We do need a major version bump however, because we should not allow streaming of super columns into this new format)
  * tracks max local deletiontime in sstable metadata
  * records bloom_filter_fp_chance in metadata component
  * remove data size and column count from data file (CASSANDRA-4180)
  * tracks max/min column values (according to comparator)
* jb (2.0.1):

  * switch from crc32 to adler32 for compression checksums
  * checksum the compressed data
* ka (2.1.0):

  * new Statistics.db file format
  * index summaries can be downsampled and the sampling level is persisted
  * switch uncompressed checksums to adler32
  * tracks presense of legacy (local and remote) counter shards
* la (2.2.0): new file name format
* lb (2.2.7): commit log lower bound included

Version 3
~~~~~~~~~

* ma (3.0.0):

  * swap bf hash order
  * store rows natively
* mb (3.0.7, 3.7): commit log lower bound included
* mc (3.0.8, 3.9): commit log intervals included

Example Code
~~~~~~~~~~~~

The following example is useful for finding all sstables that do not match the "ib" SSTable version

.. code-block:: bash

    find /var/lib/cassandra/data/ -type f | grep -v -- -ib- | grep -v "/snapshots"
