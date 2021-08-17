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

Compression
-----------

Cassandra offers operators the ability to configure compression on a per-table basis. Compression reduces the size of
data on disk by compressing the SSTable in user-configurable compression ``chunk_length_in_kb``. As Cassandra SSTables
are immutable, the CPU cost of compressing is only necessary when the SSTable is written - subsequent updates
to data will land in different SSTables, so Cassandra will not need to decompress, overwrite, and recompress data when
UPDATE commands are issued. On reads, Cassandra will locate the relevant compressed chunks on disk, decompress the full
chunk, and then proceed with the remainder of the read path (merging data from disks and memtables, read repair, and so
on).

Compression algorithms typically trade off between the following three areas:

- **Compression speed**: How fast does the compression algorithm compress data. This is critical in the flush and
  compaction paths because data must be compressed before it is written to disk.
- **Decompression speed**: How fast does the compression algorithm de-compress data. This is critical in the read
  and compaction paths as data must be read off disk in a full chunk and decompressed before it can be returned.
- **Ratio**: By what ratio is the uncompressed data reduced by. Cassandra typically measures this as the size of data
  on disk relative to the uncompressed size. For example a ratio of ``0.5`` means that the data on disk is 50% the size
  of the uncompressed data. Cassandra exposes this ratio per table as the ``SSTable Compression Ratio`` field of
  ``nodetool tablestats``.

Cassandra offers five compression algorithms by default that make different tradeoffs in these areas. While
benchmarking compression algorithms depends on many factors (algorithm parameters such as compression level,
the compressibility of the input data, underlying processor class, etc ...), the following table should help you pick
a starting point based on your application's requirements with an extremely rough grading of the different choices
by their performance in these areas (A is relatively good, F is relatively bad):

+---------------------------------------------+-----------------------+-------------+---------------+-------+-------------+
| Compression Algorithm                       | Cassandra Class       | Compression | Decompression | Ratio | C* Version  |
+=============================================+=======================+=============+===============+=======+=============+
| `LZ4 <https://lz4.github.io/lz4/>`_         | ``LZ4Compressor``     |          A+ |            A+ |    C+ | ``>=1.2.2`` |
+---------------------------------------------+-----------------------+-------------+---------------+-------+-------------+
| `LZ4HC <https://lz4.github.io/lz4/>`_       | ``LZ4Compressor``     |          C+ |            A+ |    B+ | ``>= 3.6``  |
+---------------------------------------------+-----------------------+-------------+---------------+-------+-------------+
| `Zstd <https://facebook.github.io/zstd/>`_  | ``ZstdCompressor``    |          A- |            A- |    A+ | ``>= 4.0``  |
+---------------------------------------------+-----------------------+-------------+---------------+-------+-------------+
| `Snappy <http://google.github.io/snappy/>`_ | ``SnappyCompressor``  |          A- |            A  |     C | ``>= 1.0``  |
+---------------------------------------------+-----------------------+-------------+---------------+-------+-------------+
| `Deflate (zlib) <https://zlib.net>`_        | ``DeflateCompressor`` |          C  |            C  |     A | ``>= 1.0``  |
+---------------------------------------------+-----------------------+-------------+---------------+-------+-------------+

Generally speaking for a performance critical (latency or throughput) application ``LZ4`` is the right choice as it
gets excellent ratio per CPU cycle spent. This is why it is the default choice in Cassandra.

For storage critical applications (disk footprint), however, ``Zstd`` may be a better choice as it can get significant
additional ratio to ``LZ4``.

``Snappy`` is kept for backwards compatibility and ``LZ4`` will typically be preferable.

``Deflate`` is kept for backwards compatibility and ``Zstd`` will typically be preferable.

Configuring Compression
^^^^^^^^^^^^^^^^^^^^^^^

Compression is configured on a per-table basis as an optional argument to ``CREATE TABLE`` or ``ALTER TABLE``. Three
options are available for all compressors:

- ``class`` (default: ``LZ4Compressor``): specifies the compression class to use. The two "fast"
  compressors are ``LZ4Compressor`` and ``SnappyCompressor`` and the two "good" ratio compressors are ``ZstdCompressor``
  and ``DeflateCompressor``.
- ``chunk_length_in_kb`` (default: ``16KiB``): specifies the number of kilobytes of data per compression chunk. The main
  tradeoff here is that larger chunk sizes give compression algorithms more context and improve their ratio, but
  require reads to deserialize and read more off disk.
- ``crc_check_chance`` (default: ``1.0``): determines how likely Cassandra is to verify the checksum on each compression
  chunk during reads to protect against data corruption. Unless you have profiles indicating this is a performance
  problem it is highly encouraged not to turn this off as it is Cassandra's only protection against bitrot.

The ``LZ4Compressor`` supports the following additional options:

- ``lz4_compressor_type`` (default ``fast``): specifies if we should use the ``high`` (a.k.a ``LZ4HC``) ratio version
  or the ``fast`` (a.k.a ``LZ4``) version of ``LZ4``. The ``high`` mode supports a configurable level, which can allow
  operators to tune the performance <-> ratio tradeoff via the ``lz4_high_compressor_level`` option. Note that in
  ``4.0`` and above it may be preferable to use the ``Zstd`` compressor.
- ``lz4_high_compressor_level`` (default ``9``): A number between ``1`` and ``17`` inclusive that represents how much
  CPU time to spend trying to get more compression ratio. Generally lower levels are "faster" but they get less ratio
  and higher levels are slower but get more compression ratio.

The ``ZstdCompressor`` supports the following options in addition:

- ``compression_level`` (default ``3``): A number between ``-131072`` and ``22`` inclusive that represents how much CPU
  time to spend trying to get more compression ratio. The lower the level, the faster the speed (at the cost of ratio).
  Values from 20 to 22 are called "ultra levels" and should be used with caution, as they require more memory.
  The default of ``3`` is a good choice for competing with ``Deflate`` ratios and ``1`` is a good choice for competing
  with ``LZ4``.


Users can set compression using the following syntax:

::

    CREATE TABLE keyspace.table (id int PRIMARY KEY) WITH compression = {'class': 'LZ4Compressor'};

Or

::

    ALTER TABLE keyspace.table WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 64, 'crc_check_chance': 0.5};

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
similar text columns (such as repeated JSON blobs) often compress very well. Tables containing data that has already
been compressed or random data (e.g. benchmark datasets) do not typically compress well.

Operational Impact
^^^^^^^^^^^^^^^^^^

- Compression metadata is stored off-heap and scales with data on disk.  This often requires 1-3GB of off-heap RAM per
  terabyte of data on disk, though the exact usage varies with ``chunk_length_in_kb`` and compression ratios.

- Streaming operations involve compressing and decompressing data on compressed tables - in some code paths (such as
  non-vnode bootstrap), the CPU overhead of compression can be a limiting factor.

- To prevent slow compressors (``Zstd``, ``Deflate``, ``LZ4HC``) from blocking flushes for too long, all three
  flush with the default fast ``LZ4`` compressor and then rely on normal compaction to re-compress the data into the
  desired compression strategy. See `CASSANDRA-15379 <https://issues.apache.org/jira/browse/CASSANDRA-15379>` for more
  details.

- The compression path checksums data to ensure correctness - while the traditional Cassandra read path does not have a
  way to ensure correctness of data on disk, compressed tables allow the user to set ``crc_check_chance`` (a float from
  0.0 to 1.0) to allow Cassandra to probabilistically validate chunks on read to verify bits on disk are not corrupt.

Advanced Use
^^^^^^^^^^^^

Advanced users can provide their own compression class by implementing the interface at
``org.apache.cassandra.io.compress.ICompressor``.
