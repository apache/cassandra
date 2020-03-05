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

Improved Streaming  
---------------------  

Apache Cassandra 4.0 has made several improvements to streaming.  Streaming is the process used by nodes of a cluster to exchange data in the form of SSTables.  Streaming of SSTables is performed for several operations, such as:

-          SSTable Repair
-          Host Replacement
-          Range movements
-          Bootstrapping
-          Rebuild
-          Cluster expansion

Streaming based on Netty
^^^^^^^^^^^^^^^^^^^^^^^^

Streaming in Cassandra 4.0 is based on Non-blocking Input/Output (NIO) with Netty (`CASSANDRA-12229
<https://issues.apache.org/jira/browse/CASSANDRA-12229>`_). It replaces the single-threaded (or sequential), synchronous, blocking model of streaming messages and transfer of files. Netty supports non-blocking, asynchronous, multi-threaded streaming with which multiple connections are opened simultaneously.  Non-blocking implies that threads are not blocked as they don’t wait for a response for a sent request. A response could be returned in a different thread. With asynchronous, connections and threads are decoupled and do not have a 1:1 relation. Several more connections than threads may be opened.    

Zero Copy Streaming
^^^^^^^^^^^^^^^^^^^^ 

Pre-4.0, during streaming Cassandra reifies the SSTables into objects. This creates unnecessary garbage and slows down the whole streaming process as some SSTables can be transferred as a whole file rather than individual partitions. Cassandra 4.0 has added support for streaming entire SSTables when possible (`CASSANDRA-14556
<https://issues.apache.org/jira/browse/CASSANDRA-14556>`_) for faster Streaming using ZeroCopy APIs. If enabled, Cassandra will use ZeroCopy for eligible SSTables significantly speeding up transfers and increasing throughput.  A zero-copy path avoids bringing data into user-space on both sending and receiving side. Any streaming related operations will notice corresponding improvement. Zero copy streaming is hardware bound; only limited by the hardware limitations (Network and Disk IO ).

High Availability
*****************
In benchmark tests Zero Copy Streaming is 5x faster than partitions based streaming. Faster streaming provides the benefit of improved availability. A cluster’s recovery mainly depends on the streaming speed, Cassandra clusters with failed nodes will be able to recover much more quickly (5x faster). If a node fails, SSTables need to be streamed to a replacement node. During the replacement operation, the new Cassandra node streams SSTables from the neighboring nodes that hold copies of the data belonging to this new node’s token range. Depending on the amount of data stored, this process can require substantial network bandwidth, taking some time to complete. The longer these range movement operations take, the more the cluster availability is lost. Failure of multiple nodes would reduce high availability greatly. The faster the new node completes streaming its data, the faster it can serve traffic, increasing the availability of the cluster.

Enabling Zero Copy Streaming
***************************** 
Zero copy streaming is enabled by setting the following setting in ``cassandra.yaml``.

::

 stream_entire_sstables: true

By default zero copy streaming is enabled. 

SSTables Eligible for Zero Copy Streaming
*****************************************
Zero copy streaming is used if all partitions within the SSTable need to be transmitted. This is common when using ``LeveledCompactionStrategy`` or when partitioning SSTables by token range has been enabled. All partition keys in the SSTables are iterated over to determine the eligibility for Zero Copy streaming.

Benefits of Zero Copy Streaming
******************************** 
When enabled, it permits Cassandra to zero-copy stream entire eligible SSTables between nodes, including every component. This speeds up the network transfer significantly subject to throttling specified by ``stream_throughput_outbound_megabits_per_sec``. 
 
Enabling this will reduce the GC pressure on sending and receiving node. While this feature tries to keep the disks balanced, it cannot guarantee it. This feature will be automatically disabled if internode encryption is enabled. Currently this can be used with Leveled Compaction.   

Configuring for Zero Copy Streaming
************************************ 
Throttling would reduce the streaming speed. The ``stream_throughput_outbound_megabits_per_sec`` throttles all outbound streaming file transfers on a node to the given total throughput in Mbps. When unset, the default is 200 Mbps or 25 MB/s.

::

 stream_throughput_outbound_megabits_per_sec: 200

To run any Zero Copy streaming benchmark the ``stream_throughput_outbound_megabits_per_sec`` must be set to a really high value otherwise, throttling will be significant and the benchmark results will not be meaningful.
 
The ``inter_dc_stream_throughput_outbound_megabits_per_sec`` throttles all streaming file transfer between the datacenters, this setting allows users to throttle inter dc stream throughput in addition to throttling all network stream traffic as configured with ``stream_throughput_outbound_megabits_per_sec``. When unset, the default is 200 Mbps or 25 MB/s.

::

 inter_dc_stream_throughput_outbound_megabits_per_sec: 200

SSTable Components Streamed with Zero Copy Streaming
***************************************************** 
Zero Copy Streaming streams entire SSTables.  SSTables are made up of multiple components in separate files. SSTable components streamed are listed in Table 1.

Table 1. SSTable Components

+------------------+---------------------------------------------------+
|SSTable Component | Description                                       | 
+------------------+---------------------------------------------------+
| Data.db          |The base data for an SSTable: the remaining        |
|                  |components can be regenerated based on the data    |
|                  |component.                                         |                                 
+------------------+---------------------------------------------------+
| Index.db         |Index of the row keys with pointers to their       |
|                  |positions in the data file.                        |                                                                          
+------------------+---------------------------------------------------+
| Filter.db        |Serialized bloom filter for the row keys in the    |
|                  |SSTable.                                           |                                                                          
+------------------+---------------------------------------------------+
|CompressionInfo.db|File to hold information about uncompressed        |
|                  |data length, chunk offsets etc.                    |                                                     
+------------------+---------------------------------------------------+
| Statistics.db    |Statistical metadata about the content of the      |
|                  |SSTable.                                           |                                                                          
+------------------+---------------------------------------------------+
| Digest.crc32     |Holds CRC32 checksum of the data file              | 
|                  |size_bytes.                                        |                                                                         
+------------------+---------------------------------------------------+
| CRC.db           |Holds the CRC32 for chunks in an uncompressed file.|                                                                         
+------------------+---------------------------------------------------+
| Summary.db       |Holds SSTable Index Summary                        |
|                  |(sampling of Index component)                      |                                                                          
+------------------+---------------------------------------------------+
| TOC.txt          |Table of contents, stores the list of all          |
|                  |components for the SSTable.                        |                                                                         
+------------------+---------------------------------------------------+
 
Custom component, used by e.g. custom compaction strategy may also be included.

Repair Streaming Preview
^^^^^^^^^^^^^^^^^^^^^^^^

Repair with ``nodetool repair`` involves streaming of repaired SSTables and a repair preview has been added to provide an estimate of the amount of repair streaming that would need to be performed. Repair preview (`CASSANDRA-13257
<https://issues.apache.org/jira/browse/CASSANDRA-13257>`_) is invoke with ``nodetool repair --preview`` using option:

::

-prv, --preview

It determines ranges and amount of data to be streamed, but doesn't actually perform repair.

Parallelizing of Streaming of Keyspaces
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ 
The streaming of the different keyspaces for bootstrap and rebuild has been parallelized in Cassandra 4.0 (`CASSANDRA-4663
<https://issues.apache.org/jira/browse/CASSANDRA-4663>`_).

Unique nodes for Streaming in Multi-DC deployment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Range Streamer picks unique nodes to stream data from when number of replicas in each DC is three or more (`CASSANDRA-4650
<https://issues.apache.org/jira/browse/CASSANDRA-4650>`_). What the optimization does is to even out the streaming load across the cluster. Without the optimization, some node can be picked up to stream more data than others. This patch allows to select dedicated node to stream only one range.

This will increase the performance of bootstrapping a node and will also put less pressure on nodes serving the data. This does not affect if N < 3 in each DC as then it streams data from only 2 nodes.

Stream Operation Types 
^^^^^^^^^^^^^ 

It is important to know the type or purpose of a certain stream. Version 4.0 (`CASSANDRA-13064
<https://issues.apache.org/jira/browse/CASSANDRA-13064>`_) adds an ``enum`` to distinguish between the different types  of streams.  Stream types are available both in a stream request and a stream task. The different stream types are:

- Restore replica count
- Unbootstrap
- Relocation
- Bootstrap
- Rebuild
- Bulk Load
- Repair

Disallow Decommission when number of Replicas will drop below configured RF
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
`CASSANDRA-12510
<https://issues.apache.org/jira/browse/CASSANDRA-12510>`_ guards against decommission that will drop # of replicas below configured replication factor (RF), and adds the ``--force`` option that allows decommission to continue if intentional; force decommission of this node even when it reduces the number of replicas to below configured RF.
