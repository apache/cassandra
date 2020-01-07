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

Hints
=====

Hints are a type of repair during a write operation. At times a write or an update cannot be replicated to all nodes satisfying the replication factor because a replica node is unavailable. Under such a condition the mutation (a write or update) is stored temporarily on the coordinator node in its filesystem. 

Hints are metadata associated with a mutation (a write or update) indicating that the mutation is not placed on a replica node (the target node) it is meant to be placed on because the node is temporarily unavailable, or is unresponsive.  Hints are used to implement the eventual consistency guarantee that all updates are eventually received by all replicas and all replicas are eventually made consistent.    When the replica node becomes available the hints are replayed on the node.

As a primer on how replicas are placed in a cluster, Apache Cassandra replicates data to provide fault tolerance, high availability and durability. Cassandra partitions data across the cluster using consistent hashing in which a hash function is used on the partition keys to generate consistently ordered hash values (or tokens).  An abstract ring represents the complete hash value range (token range) of the keys stored with each node in the cluster being assigned a certain subset range of hash values (range of tokens) it can store.  The list of nodes responsible for a particular key is called its preference list.  The preference list may include virtual nodes as a virtual node is also a node albeit an abstract node and not a physical node.  Virtual nodes may need to be skipped to create a preference list in which the first N (N being the replication factor) nodes taken clockwise in the consistent hashing ring are all distinct physical nodes. All nodes in a cluster know which node/s should be in the preference list for a given key.  The node that receives a request for a write operation (key/value data) forwards the request to the replica node that is in the preference list for the key.  The node becomes a coordinator node and coordinates the reads and writes.   

Why are hints needed?
=====================

Hints reduce the inconsistency window caused by temporary node unavailability.

Consider that an update or mutation is to be made using the following configuration:

- Consistency level : 2
- Replication factor: 3
- Replication strategy: SimpleStrategy
- Number of nodes in cluster: 5

The update or mutation is sent to a node (node A) in the cluster, and is meant to be forwarded to three other nodes, the replica nodes B, C and D.  The node that receives the request is the proxy node and becomes the coordinator of the request.  Under normal operation the update gets sent to the three replica nodes and the coordinator receives the response from the three nodes satisfying the consistency level.  But suppose node B is down and unavailable.  The update is sent to nodes C and D and a response returned to the coordinator, again satisfying the consistency level of 2.   But that is not the end of the request. Because the replica mutation is meant for replica node B also, a hint is stored by the coordinator node in the local filesystem   indicating that the update or mutation is also to be replicated on node B.  The coordinator node waits for 3 hours by default (as set with max_hint_window_in_ms). If node B becomes available within 3 hours the coordinator sends the hint to node B and the hint is replayed on node B, eventually making all replicas consistent. Such a transfer of an update using hints is called a hinted handoff.  Hinted handoff is used to ensure that read and write operations are not failed and the consistency, availability and durability guarantees are not compromised.  We still need to satisfy the consistency level, because hints & hinted handoffs are not used to satisfy the write consistency level unless the consistency level is ANY.  If the replica node for which a hint is generated does not become available within 3 hours, or the max_hint_window_in_ms, the hint is deleted and a full or read repair becomes necessary.

Hints for Timed Out Write Requests
==================================

Hints are also stored for write requests that are timed out. The ``write_request_timeout_in_ms`` setting in ``cassandra.yaml`` configures the timeout for write requests.

::

write_request_timeout_in_ms: 2000

The coordinator waits for the configured amount of time for write requests to complete and throws a ``TimeOutException``.  The coordinator node also generates a hint for the timed out request. Lowest acceptable value for ``write_request_timeout_in_ms`` is 10 ms.

What data is stored in a hint?
==============================

Hints are stored in flat files in the coordinator node’s ``$CASSANDRA_HOME/data/hints`` directory. A hint includes a hint id, the target replica node on which the mutation is meant to be stored, the serialized mutation (stored as a blob) that couldn’t be delivered to the replica node, and the Cassandra version used to serialize the mutation. By default hints are compressed using ``LZ4Compress``. Multiple hints are appended to the same hints file.
 
Replaying Hints
===============

Hints are streamed in bulk, a segment at a time, to the target replica node and the target node replays them locally. After the target node has replayed a segment it deletes the segment and receives the next segment.

Configuring Hints
=================

Hints are enabled by default. The ``cassandra.yaml`` configuration file provides several settings for configuring hints as discussed in Table 1.

Table 1. Settings for Hints

+----------------------+-------------------------------------------+-----------------+
|Setting               | Description                               |Default Value    |
+----------------------+-------------------------------------------+-----------------+
|hinted_handoff_enabled|Enables/Disables hinted handoffs           | true            |
|                      |                                           |                 | 
|                      |                                           |                 |
|                      |                                           |                 |
|                      |                                           |                 |                                                        
+----------------------+-------------------------------------------+-----------------+
|hinted_handoff_       |A list of data centers that do not perform |                 |
| disabled_datacenters | hinted handoffs even when hinted_handoff_ |                 | 
|                      | enabled is set to true.                   |                 |
|                      | Example:                                  |                 |
|                      | hinted_handoff_disabled_datacenters:      |                 |
|                      |                 - DC1                     |                 |
|                      |                 - DC2|                    |                 |                                                                                          
+----------------------+-------------------------------------------+-----------------+
|max_hint_window_in_ms |Defines the maximum amount of time (ms)    |10800000 #3 hours|
|                      |a node shall have hints generated after it |                 |
|                      |has failed.                                |                 |                                                                         
+----------------------+-------------------------------------------+-----------------+
|hinted_handoff_       |Maximum throttle in KBs per second, per    |                 |
| throttle_in_kb       | delivery thread. This will be reduced     | 1024            |
|                      | proportionally to the number of nodes in  |                 | 
|                      | the cluster.                              |                 |
|                      |(If there are two nodes in the cluster,    |                 |
|                      |each delivery thread will use the maximum  |                 |
|                      |rate; if there are 3, each will throttle   |                 |
|                      |to half of the maximum,since it is expected|                 |
|                      |for two nodes to be delivering hints       |                 |
|                      |simultaneously.)                           |                 |
+----------------------+-------------------------------------------+-----------------+
|max_hints_delivery_   | Number of threads with which to deliver   |     2           |
| threads              |hints; Consider increasing this number when|                 |
|                      |  you have multi-dc deployments, since     |                 |
|                      |  cross-dc handoff tends to be slower      |                 |
+----------------------+-------------------------------------------+-----------------+
|hints_directory       |Directory where Cassandra stores hints.    |$CASSANDRA_HOME/ |
|                      |                                           |data/hints       |
+----------------------+-------------------------------------------+-----------------+
|hints_flush_period_in_|How often hints should be flushed from the |  10000          |
| ms                   | internal buffers to disk. Will *not*      |                 |
|                      | trigger fsync.                            |                 |
+----------------------+-------------------------------------------+-----------------+
|max_hints_file_size_  |Maximum size for a single hints file, in   |   128           |
| in_mb                |megabytes.                                 |                 |
+----------------------+-------------------------------------------+-----------------+
|hints_compression     |Compression to apply to the hint files.    |    LZ4          | 
|                      |  If omitted, hints files will be written  |                 |
|                      |  uncompressed. LZ4, Snappy, and Deflate   |                 |
|                      |  compressors are supported.               |                 |
+----------------------+-------------------------------------------+-----------------+
 
Changing Max Hint Window at Runtime
===================================

Cassandra 4.0 has added support for changing ``max_hint_window_in_ms`` at runtime 
(`CASSANDRA-11720
<https://issues.apache.org/jira/browse/CASSANDRA-11720>`_). The ``max_hint_window_in_ms`` configuration property in ``cassandra.yaml`` may be modified at runtime followed by a rolling restart. The default value of ``max_hint_window_in_ms`` is 3 hours.

::

  max_hint_window_in_ms: 10800000 # 3 hours

The need to be able to modify ``max_hint_window_in_ms`` at runtime is explained with the following example.  A larger node (in terms of data it holds) goes down. And it will take slightly more than ``max_hint_window_in_ms`` to fix it. The disk space to store some additional hints id available.

Added Histogram for Delay to deliver Hints
==========================================

Version 4.0 adds histograms available to understand how long it takes to deliver hints is useful for operators to better identify problems (`CASSANDRA-13234
<https://issues.apache.org/jira/browse/CASSANDRA-13234>`_).
 
Using nodetool for Configuring hints
====================================

The nodetool provides several commands for configuring hints or getting hints related information. The nodetool commands override the corresponding settings if any in ``cassandra.yaml``. These commands are discussed in Table 2.

Table 2. Nodetool Commands for Hints

+----------------------------+-------------------------------------------+
|Command                     | Description                               | 
+----------------------------+-------------------------------------------+
|nodetool disablehandoff     |Disables storing hinted handoffs           |                                                                       
+----------------------------+-------------------------------------------+
|nodetool disablehintsfordc  |Disables hints for a data center           |                                                                                               
+----------------------------+-------------------------------------------+
|nodetool enablehandoff      |Re-enables future hints storing on the     |
|                            | current node                              |                                              
+----------------------------+-------------------------------------------+
|nodetool enablehintsfordc   |Enables hints for a data center that was   |
|                            |  previously disabled                      | 
+----------------------------+-------------------------------------------+
|nodetool getmaxhintwindow   |Prints the max hint window in ms.          |
|                            |  A new nodetool command in Cassandra 4.0. |
+----------------------------+-------------------------------------------+
|nodetool handoffwindow      |Prints current hinted handoff window       |
+----------------------------+-------------------------------------------+
|nodetool pausehandoff       |Pauses hints delivery process              |                                                                     
+----------------------------+-------------------------------------------+
|nodetool resumehandoff      |Resumes hints delivery process             |                                                                                              
+----------------------------+-------------------------------------------+
|nodetool                    |Sets hinted handoff throttle in kb         |
| sethintedhandoffthrottlekb | per second, per delivery thread           |                                                             
+----------------------------+-------------------------------------------+
|nodetool setmaxhintwindow   |Sets the specified max hint window in ms   | 
+----------------------------+-------------------------------------------+
|nodetool statushandoff      |Status of storing future hints on the      |
|                            |  current node                             |
+----------------------------+-------------------------------------------+
|nodetool truncatehints      |Truncates all hints on the local node, or  |
|                            | truncates hints for the endpoint(s)       |
|                            | specified.                                |
+----------------------------+-------------------------------------------+

Hints is not an alternative to performing a full repair or read repair but is only a stopgap measure.
