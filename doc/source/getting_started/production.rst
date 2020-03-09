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

Production Recommendations
----------------------------

The ``cassandra.yaml`` and ``jvm.options`` files have a number of notes and recommendations for production usage.  This page
expands on some of the notes in these files with additional information.

Tokens
^^^^^^^

Using more than 1 token (referred to as vnodes) allows for more flexible expansion and more streaming peers when
bootstrapping new nodes into the cluster.  This can limit the negative impact of streaming (I/O and CPU overhead)
as well as allow for incremental cluster expansion.

As a tradeoff, more tokens will lead to sharing data with more peers, which can result in decreased availability.  To learn more about this we
recommend reading `this paper <https://github.com/jolynch/python_performance_toolkit/raw/master/notebooks/cassandra_availability/whitepaper/cassandra-availability-virtual.pdf>`_.

The number of tokens can be changed using the following setting:

``num_tokens: 16``


Here are the most common token counts with a brief explanation of when and why you would use each one.

+-------------+---------------------------------------------------------------------------------------------------+
| Token Count | Description                                                                                       |
+=============+===================================================================================================+
| 1           | Maximum availablility, maximum cluster size, fewest peers,                                        |
|             | but inflexible expansion.  Must always                                                            |
|             | double size of cluster to expand and remain balanced.                                             |
+-------------+---------------------------------------------------------------------------------------------------+
| 4           | A healthy mix of elasticity and availability.  Recommended for clusters which will eventually     |
|             | reach over 30 nodes.  Requires adding approximately 20% more nodes to remain balanced.            |
|             | Shrinking a cluster may result in cluster imbalance.                                              |
+-------------+---------------------------------------------------------------------------------------------------+
| 16          | Best for heavily elastic clusters which expand and shrink regularly, but may have issues          |
|             | availability with larger clusters.  Not recommended for clusters over 50 nodes.                   |
+-------------+---------------------------------------------------------------------------------------------------+


In addition to setting the token count, it's extremely important that ``allocate_tokens_for_local_replication_factor`` be
set as well, to ensure even token allocation.

.. _read-ahead:

Read Ahead
^^^^^^^^^^^

Read ahead is an operating system feature that attempts to keep as much data loaded in the page cache as possible.  The
goal is to decrease latency by using additional throughput on reads where the latency penalty is high due to seek times
on spinning disks.  By leveraging read ahead, the OS can pull additional data into memory without the cost of additional
seeks.  This works well when available RAM is greater than the size of the hot dataset, but can be problematic when the
hot dataset is much larger than available RAM.  The benefit of read ahead decreases as the size of your hot dataset gets
bigger in proportion to available memory.

With small partitions (usually tables with no partition key, but not limited to this case) and solid state drives, read
ahead can increase disk usage without any of the latency benefits, and in some cases can result in up to
a 5x latency and throughput performance penalty.  Read heavy, key/value tables with small (under 1KB) rows are especially
prone to this problem.

We recommend the following read ahead settings:

+----------------+-------------------------+
| Hardware       | Initial Recommendation  |
+================+=========================+
|Spinning Disks  | 64KB                    |
+----------------+-------------------------+
|SSD             | 4KB                     |
+----------------+-------------------------+

Read ahead can be adjusted on Linux systems by using the `blockdev` tool.

For example, we can set read ahead of ``/dev/sda1` to 4KB by doing the following::

    blockdev --setra 8 /dev/sda1

**Note**: blockdev accepts the number of 512 byte sectors to read ahead.  The argument of 8 above is equivilent to 4KB.

Since each system is different, use the above recommendations as a starting point and tuning based on your SLA and
throughput requirements.  To understand how read ahead impacts disk resource usage we recommend carefully reading through the
:ref:`troubleshooting <use-os-tools>` portion of the documentation.


Compression
^^^^^^^^^^^^

Compressed data is stored by compressing fixed size byte buffers and writing the data to disk.  The buffer size is
determined by the  ``chunk_length_in_kb`` element in the compression map of the schema settings.

The default setting is 16KB starting with Cassandra 4.0.

Since the entire compressed buffer must be read off disk, using too high of a compression chunk length can lead to
significant overhead when reading small records.  Combined with the default read ahead setting this can result in massive
read amplification for certain workloads.

LZ4Compressor is the default and recommended compression algorithm.

There is additional information on this topic on `The Last Pickle Blog <https://thelastpickle.com/blog/2018/08/08/compression_performance.html>`_.

Compaction
^^^^^^^^^^^^

There are different :ref:`compaction <compaction>` strategies available for different workloads.
We recommend reading up on the different strategies to understand which is the best for your environment.  Different tables
may (and frequently do) use different compaction strategies on the same cluster.

Encryption
^^^^^^^^^^^

It is significantly easier to set up peer to peer encryption and client server encryption when setting up your production
cluster as opposed to setting it up once the cluster is already serving production traffic.  If you are planning on using network encryption
eventually (in any form), we recommend setting it up now.  Changing these configurations down the line is not impossible,
but mistakes can result in downtime or data loss.

Ensure Keyspaces are Created with NetworkTopologyStrategy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Production clusters should never use SimpleStrategy.  Production keyspaces should use the NetworkTopologyStrategy (NTS).

For example::

    create KEYSPACE mykeyspace WITH replication =
    {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};

NetworkTopologyStrategy allows Cassandra to take advantage of multiple racks and data centers.

Configure Racks and Snitch
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Correctly configuring or changing racks after a cluster has been provisioned is an unsupported process**.  Migrating from
a single rack to multiple racks is also unsupported and can result in data loss.

Using ``GossipingPropertyFileSnitch`` is the most flexible solution for on premise or mixed cloud environments.  ``Ec2Snitch``
is reliable for AWS EC2 only environments.







