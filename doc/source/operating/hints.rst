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

.. _hints:

Hints
=====

Hinting is a data repair technique applied during write operations. When
replica nodes are unavailable to accept a mutation, either due to failure or
more commonly routine maintenance, coordinators attempting to write to those
replicas store temporary hints on their local filesystem for later application
to the unavailable replica. Hints are an important way to help reduce the
duration of data inconsistency. Coordinators replay hints quickly after
unavailable replica nodes return to the ring. Hints are best effort, however,
and do not guarantee eventual consistency like :ref:`anti-entropy repair
<repair>` does.

Hints are useful because of how Apache Cassandra replicates data to provide
fault tolerance, high availability and durability. Cassandra :ref:`partitions
data across the cluster <consistent-hashing-token-ring>` using consistent
hashing, and then replicates keys to multiple nodes along the hash ring. To
guarantee availability, all replicas of a key can accept mutations without
consensus, but this means it is possible for some replicas to accept a mutation
while others do not. When this happens an inconsistency is introduced.

Hints are one of the three ways, in addition to read-repair and
full/incremental anti-entropy repair, that Cassandra implements the eventual
consistency guarantee that all updates are eventually received by all replicas.
Hints, like read-repair, are best effort and not an alternative to performing
full repair, but they do help reduce the duration of inconsistency between
replicas in practice.

Hinted Handoff
--------------

Hinted handoff is the process by which Cassandra applies hints to unavailable
nodes.

For example, consider a mutation is to be made at ``Consistency Level``
``LOCAL_QUORUM`` against a keyspace with ``Replication Factor`` of ``3``.
Normally the client sends the mutation to a single coordinator, who then sends
the mutation to all three replicas, and when two of the three replicas
acknowledge the mutation the coordinator responds successfully to the client.
If a replica node is unavailable, however, the coordinator stores a hint
locally to the filesystem for later application. New hints will be retained for
up to ``max_hint_window_in_ms`` of downtime (defaults to ``3 hours``).  If the
unavailable replica does return to the cluster before the window expires, the
coordinator applies any pending hinted mutations against the replica to ensure
that eventual consistency is maintained.

.. figure:: images/hints.svg
    :alt: Hinted Handoff Example

    Hinted Handoff in Action

* (``t0``): The write is sent by the client, and the coordinator sends it
  to the three replicas. Unfortunately ``replica_2`` is restarting and cannot
  receive the mutation.
* (``t1``): The client receives a quorum acknowledgement from the coordinator.
  At this point the client believe the write to be durable and visible to reads
  (which it is).
* (``t2``): After the write timeout (default ``2s``), the coordinator decides
  that ``replica_2`` is unavailable and stores a hint to its local disk.
* (``t3``): Later, when ``replica_2`` starts back up it sends a gossip message
  to all nodes, including the coordinator.
* (``t4``): The coordinator replays hints including the missed mutation
  against ``replica_2``.

If the node does not return in time, the destination replica will be
permanently out of sync until either read-repair or full/incremental
anti-entropy repair propagates the mutation.

Application of Hints
^^^^^^^^^^^^^^^^^^^^

Hints are streamed in bulk, a segment at a time, to the target replica node and
the target node replays them locally. After the target node has replayed a
segment it deletes the segment and receives the next segment. This continues
until all hints are drained.

Storage of Hints on Disk
^^^^^^^^^^^^^^^^^^^^^^^^

Hints are stored in flat files in the coordinator node’s
``$CASSANDRA_HOME/data/hints`` directory. A hint includes a hint id, the target
replica node on which the mutation is meant to be stored, the serialized
mutation (stored as a blob) that couldn't be delivered to the replica node, the
mutation timestamp, and the Cassandra version used to serialize the mutation.
By default hints are compressed using ``LZ4Compressor``. Multiple hints are
appended to the same hints file.

Since hints contain the original unmodified mutation timestamp, hint application
is idempotent and cannot overwrite a future mutation.

Hints for Timed Out Write Requests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Hints are also stored for write requests that time out. The
``write_request_timeout_in_ms`` setting in ``cassandra.yaml`` configures the
timeout for write requests.

::

  write_request_timeout_in_ms: 2000

The coordinator waits for the configured amount of time for write requests to
complete, at which point it will time out and generate a hint for the timed out
request. The lowest acceptable value for ``write_request_timeout_in_ms`` is 10 ms.


Configuring Hints
-----------------

Hints are enabled by default as they are critical for data consistency. The
``cassandra.yaml`` configuration file provides several settings for configuring
hints:

Table 1. Settings for Hints

+--------------------------------------------+-------------------------------------------+-------------------------------+
|Setting                                     | Description                               |Default Value                  |
+--------------------------------------------+-------------------------------------------+-------------------------------+
|``hinted_handoff_enabled``                  |Enables/Disables hinted handoffs           | ``true``                      |
|                                            |                                           |                               |
|                                            |                                           |                               |
|                                            |                                           |                               |
|                                            |                                           |                               |
+--------------------------------------------+-------------------------------------------+-------------------------------+
|``hinted_handoff_disabled_datacenters``     |A list of data centers that do not perform | ``unset``                     |
|                                            |hinted handoffs even when handoff is       |                               |
|                                            |otherwise enabled.                         |                               |
|                                            |Example:                                   |                               |
|                                            |                                           |                               |
|                                            | .. code-block:: yaml                      |                               |
|                                            |                                           |                               |
|                                            |     hinted_handoff_disabled_datacenters:  |                               |
|                                            |       - DC1                               |                               |
|                                            |       - DC2                               |                               |
+--------------------------------------------+-------------------------------------------+-------------------------------+
|``max_hint_window_in_ms``                   |Defines the maximum amount of time (ms)    | ``10800000`` # 3 hours        |
|                                            |a node shall have hints generated after it |                               |
|                                            |has failed.                                |                               |
+--------------------------------------------+-------------------------------------------+-------------------------------+
|``hinted_handoff_throttle_in_kb``           |Maximum throttle in KBs per second, per    |                               |
|                                            |delivery thread. This will be reduced      | ``1024``                      |
|                                            |proportionally to the number of nodes in   |                               |
|                                            |the cluster.                               |                               |
|                                            |(If there are two nodes in the cluster,    |                               |
|                                            |each delivery thread will use the maximum  |                               |
|                                            |rate; if there are 3, each will throttle   |                               |
|                                            |to half of the maximum,since it is expected|                               |
|                                            |for two nodes to be delivering hints       |                               |
|                                            |simultaneously.)                           |                               |
+--------------------------------------------+-------------------------------------------+-------------------------------+
|``max_hints_delivery_threads``              |Number of threads with which to deliver    | ``2``                         |
|                                            |hints; Consider increasing this number when|                               |
|                                            |you have multi-dc deployments, since       |                               |
|                                            |cross-dc handoff tends to be slower        |                               |
+--------------------------------------------+-------------------------------------------+-------------------------------+
|``hints_directory``                         |Directory where Cassandra stores hints.    |``$CASSANDRA_HOME/data/hints`` |
|                                            |                                           |                               |
+--------------------------------------------+-------------------------------------------+-------------------------------+
|``hints_flush_period_in_ms``                |How often hints should be flushed from the | ``10000``                     |
|                                            |internal buffers to disk. Will *not*       |                               |
|                                            |trigger fsync.                             |                               |
+--------------------------------------------+-------------------------------------------+-------------------------------+
|``max_hints_file_size_in_mb``               |Maximum size for a single hints file, in   | ``128``                       |
|                                            |megabytes.                                 |                               |
+--------------------------------------------+-------------------------------------------+-------------------------------+
|``hints_compression``                       |Compression to apply to the hint files.    | ``LZ4Compressor``             |
|                                            |If omitted, hints files will be written    |                               |
|                                            |uncompressed. LZ4, Snappy, and Deflate     |                               |
|                                            |compressors are supported.                 |                               |
+--------------------------------------------+-------------------------------------------+-------------------------------+

Configuring Hints at Runtime with ``nodetool``
----------------------------------------------

``nodetool`` provides several commands for configuring hints or getting hints
related information. The nodetool commands override the corresponding
settings if any in ``cassandra.yaml`` for the node running the command.

Table 2. Nodetool Commands for Hints

+--------------------------------+-------------------------------------------+
|Command                         | Description                               |
+--------------------------------+-------------------------------------------+
|``nodetool disablehandoff``     |Disables storing and delivering hints      |
+--------------------------------+-------------------------------------------+
|``nodetool disablehintsfordc``  |Disables storing and delivering hints to a |
|                                |data center                                |
+--------------------------------+-------------------------------------------+
|``nodetool enablehandoff``      |Re-enables future hints storing and        |
|                                |delivery on the current node               |
+--------------------------------+-------------------------------------------+
|``nodetool enablehintsfordc``   |Enables hints for a data center that was   |
|                                |previously disabled                        |
+--------------------------------+-------------------------------------------+
|``nodetool getmaxhintwindow``   |Prints the max hint window in ms. New in   |
|                                |Cassandra 4.0.                             |
+--------------------------------+-------------------------------------------+
|``nodetool handoffwindow``      |Prints current hinted handoff window       |
+--------------------------------+-------------------------------------------+
|``nodetool pausehandoff``       |Pauses hints delivery process              |
+--------------------------------+-------------------------------------------+
|``nodetool resumehandoff``      |Resumes hints delivery process             |
+--------------------------------+-------------------------------------------+
|``nodetool                      |Sets hinted handoff throttle in kb         |
|sethintedhandoffthrottlekb``    |per second, per delivery thread            |
+--------------------------------+-------------------------------------------+
|``nodetool setmaxhintwindow``   |Sets the specified max hint window in ms   |
+--------------------------------+-------------------------------------------+
|``nodetool statushandoff``      |Status of storing future hints on the      |
|                                |current node                               |
+--------------------------------+-------------------------------------------+
|``nodetool truncatehints``      |Truncates all hints on the local node, or  |
|                                |truncates hints for the endpoint(s)        |
|                                |specified.                                 |
+--------------------------------+-------------------------------------------+

Make Hints Play Faster at Runtime
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The default of ``1024 kbps`` handoff throttle is conservative for most modern
networks, and it is entirely possible that in a simple node restart you may
accumulate many gigabytes hints that may take hours to play back. For example if
you are ingesting ``100 Mbps`` of data per node, a single 10 minute long
restart will create ``10 minutes * (100 megabit / second) ~= 7 GiB`` of data
which at ``(1024 KiB / second)`` would take ``7.5 GiB / (1024 KiB / second) =
2.03 hours`` to play back. The exact math depends on the load balancing strategy
(round robin is better than token aware), number of tokens per node (more
tokens is better than fewer), and naturally the cluster's write rate, but
regardless you may find yourself wanting to increase this throttle at runtime.

If you find yourself in such a situation, you may consider raising
the ``hinted_handoff_throttle`` dynamically via the
``nodetool sethintedhandoffthrottlekb`` command.

Allow a Node to be Down Longer at Runtime
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sometimes a node may be down for more than the normal ``max_hint_window_in_ms``,
(default of three hours), but the hardware and data itself will still be
accessible.  In such a case you may consider raising the
``max_hint_window_in_ms`` dynamically via the ``nodetool setmaxhintwindow``
command added in Cassandra 4.0 (`CASSANDRA-11720 <https://issues.apache.org/jira/browse/CASSANDRA-11720>`_).
This will instruct Cassandra to continue holding hints for the down
endpoint for a longer amount of time.

This command should be applied on all nodes in the cluster that may be holding
hints. If needed, the setting can be applied permanently by setting the
``max_hint_window_in_ms`` setting in ``cassandra.yaml`` followed by a rolling
restart.

Monitoring Hint Delivery
------------------------

Cassandra 4.0 adds histograms available to understand how long it takes to deliver
hints which is useful for operators to better identify problems (`CASSANDRA-13234
<https://issues.apache.org/jira/browse/CASSANDRA-13234>`_).

There are also metrics available for tracking :ref:`Hinted Handoff <handoff-metrics>`
and :ref:`Hints Service <hintsservice-metrics>` metrics.
