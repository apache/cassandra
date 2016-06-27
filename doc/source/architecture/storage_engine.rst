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

.. todo:: todo

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
