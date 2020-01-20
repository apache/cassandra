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

Hints are a data repair technique applied during write operations. When
replica nodes are unavailable to accept a mutation, either due to failure or
more commonly routine maintenance, coordinators attempting to write to those
replicas store temporary hints on their local filesystem for later application
to the unavailable replica. Hints are an important way to help reduce the
duration of data inconsistency between replicas as they replay quickly after
unavailable nodes return to the ring, however they are best effort and do not
guarantee eventual consistency like :ref:`anti-entropy repair <repair>` does.

Hints are needed because of how Apache Cassandra replicates data to provide
fault tolerance, high availability and durability. Cassandra partitions data
across the cluster using consistent hashing, and then replicates keys to
multiple nodes along the hash ring. To guarantee availability, all replicas of
a key can accept mutations without consensus, but this means it is possible for
some replicas to accept a mutation while others do not. When this happens an
inconsistency is introduced, and hints are one way Cassandra can fix the
inconsistency.

Hints contain metadata associated with a mutation (insert or update) indicating
that the coordinator has not confirmed the mutation has been successfully
applied to a replica node (the target node). All hints crucially contain the
mutation timestamp, which allow them to be safely applied in an idempotent
manner, including in the presence of other concurrent mutations. Hints are one
of the three ways, in addition to read-repair and full anti-entropy repair,
Cassandra implements the eventual consistency guarantee that
all updates are eventually received by all replicas and all replicas are
eventually made consistent.


Hinted Handoff
--------------

Hinted handoff is the process by which Cassandra applies hints to unavailable
nodes. Consider a mutation is to be made at ``Consistency Level``
``LOCAL_QUORUM`` against a keyspace with ``Replication Factor`` of ``3``.

Normally the client sends the mutation to a single coordinator, who then sends
the mutation to all three replicas, and when two of the three replicas
acknowledge the mutation the coordinator responds successfully to the client.
If a replica node is unavailable, however, the coordinator stores a hint
locally to the filesystem for later application. These local hints will be
retained for up to ``max_hint_window_in_ms`` which defaults to ``3`` hours.
After hint expiry the destination replica will be permanently out of sync until
either read-repair or full anti-entropy repair propagates the mutation. If the
unavailable node does return to the cluster before the expiry, the coordinator
node applies any pending hinted mutations against the replica to ensure that
eventual consistency is maintained. An example of hinted handoff:

.. figure:: images/hints.svg
    :alt: Hinted Handoff Example

    Hinted Handoff in Action

Hints for Timed Out Write Requests
----------------------------------

Hints are also stored for write requests that time out. The
``write_request_timeout_in_ms`` setting in ``cassandra.yaml`` configures the
timeout for write requests.

::

  write_request_timeout_in_ms: 2000

The coordinator waits for the configured amount of time for write requests to
complete, at which point it will time out and generate a hint for the timed out
request. The lowest acceptable value for ``write_request_timeout_in_ms`` is 10 ms.

Where are Hints Stored?
-----------------------

Hints are stored in flat files in the coordinator node’s
``$CASSANDRA_HOME/data/hints`` directory. A hint includes a hint id, the target
replica node on which the mutation is meant to be stored, the serialized
mutation (stored as a blob) that couldn’t be delivered to the replica node, and
the Cassandra version used to serialize the mutation. By default hints are
compressed using ``LZ4Compressor``. Multiple hints are appended to the same hints
file.

Application of Hints
--------------------

Hints are streamed in bulk, a segment at a time, to the target replica node and
the target node replays them locally. After the target node has replayed a
segment it deletes the segment and receives the next segment. This continues
until all hints are drained.

Configuring Hints
-----------------

Hints are enabled by default as they are critical for data consistency. The
``cassandra.yaml`` configuration file provides several settings for configuring
hints as discussed in Table 1.

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
|hinted_handoff        |A list of data centers that do not perform |                 |
|_disabled_datacenters |hinted handoffs even when hinted_handoff_  |                 | 
|                      |enabled is set to true.                    |                 |
|                      |Example:                                   |                 |
|                      |hinted_handoff_disabled_datacenters:       |                 |
|                      |                 - DC1                     |                 |
|                      |                 - DC2|                    |                 |                                                   
+----------------------+-------------------------------------------+-----------------+
|max_hint_window_in_ms |Defines the maximum amount of time (ms)    |10800000 #3 hours|
|                      |a node shall have hints generated after it |                 |
|                      |has failed.                                |                 |                                                   
+----------------------+-------------------------------------------+-----------------+
|hinted_handoff        |Maximum throttle in KBs per second, per    |                 |
|_throttle_in_kb       |delivery thread. This will be reduced      | 1024            |
|                      |proportionally to the number of nodes in   |                 | 
|                      |the cluster.                               |                 |
|                      |(If there are two nodes in the cluster,    |                 |
|                      |each delivery thread will use the maximum  |                 |
|                      |rate; if there are 3, each will throttle   |                 |
|                      |to half of the maximum,since it is expected|                 |
|                      |for two nodes to be delivering hints       |                 |
|                      |simultaneously.)                           |                 |
+----------------------+-------------------------------------------+-----------------+
|max_hints_delivery    |Number of threads with which to deliver    |     2           |
|_threads              |hints; Consider increasing this number when|                 |
|                      |you have multi-dc deployments, since       |                 |
|                      |cross-dc handoff tends to be slower        |                 |
+----------------------+-------------------------------------------+-----------------+
|hints_directory       |Directory where Cassandra stores hints.    |$CASSANDRA_HOME/ |
|                      |                                           |data/hints       |
+----------------------+-------------------------------------------+-----------------+
|hints_flush_period_in |How often hints should be flushed from the |  10000          |
|_ms                   |internal buffers to disk. Will *not*       |                 |
|                      |trigger fsync.                             |                 |
+----------------------+-------------------------------------------+-----------------+
|max_hints_file_size   |Maximum size for a single hints file, in   |   128           |
|_in_mb                |megabytes.                                 |                 |
+----------------------+-------------------------------------------+-----------------+
|hints_compression     |Compression to apply to the hint files.    |  LZ4Compress    | 
|                      |If omitted, hints files will be written    |                 |
|                      |uncompressed. LZ4, Snappy, and Deflate     |                 |
|                      |compressors are supported.                 |                 |
+----------------------+-------------------------------------------+-----------------+
 
Changing Max Hint Window at Runtime
-----------------------------------

Cassandra 4.0 adds support for changing ``max_hint_window_in_ms`` at runtime
(`CASSANDRA-11720 <https://issues.apache.org/jira/browse/CASSANDRA-11720>`_).
The ``max_hint_window_in_ms`` configuration property in ``cassandra.yaml`` may
be modified at runtime followed by a rolling restart. The default value of
``max_hint_window_in_ms`` is 3 hours.

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
|                            |current node                               |                                              
+----------------------------+-------------------------------------------+
|nodetool enablehintsfordc   |Enables hints for a data center that was   |
|                            |previously disabled                        | 
+----------------------------+-------------------------------------------+
|nodetool getmaxhintwindow   |Prints the max hint window in ms.          |
|                            |A new nodetool command in Cassandra 4.0.   |
+----------------------------+-------------------------------------------+
|nodetool handoffwindow      |Prints current hinted handoff window       |
+----------------------------+-------------------------------------------+
|nodetool pausehandoff       |Pauses hints delivery process              |                                                               
+----------------------------+-------------------------------------------+
|nodetool resumehandoff      |Resumes hints delivery process             |                                                               
+----------------------------+-------------------------------------------+
|nodetool                    |Sets hinted handoff throttle in kb         |
|sethintedhandoffthrottlekb  |per second, per delivery thread            |                                                             
+----------------------------+-------------------------------------------+
|nodetool setmaxhintwindow   |Sets the specified max hint window in ms   | 
+----------------------------+-------------------------------------------+
|nodetool statushandoff      |Status of storing future hints on the      |
|                            |current node                               |
+----------------------------+-------------------------------------------+
|nodetool truncatehints      |Truncates all hints on the local node, or  |
|                            |truncates hints for the endpoint(s)        |
|                            |specified.                                 |
+----------------------------+-------------------------------------------+

Hints, like read-repair, are not an alternative to performing full repair, but
do help reduce the duration of inconsistency between replicas.
