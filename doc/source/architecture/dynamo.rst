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

Dynamo
------

.. _gossip:

Gossip
^^^^^^

.. todo:: todo

Failure Detection
^^^^^^^^^^^^^^^^^

.. todo:: todo

Token Ring/Ranges
^^^^^^^^^^^^^^^^^

.. todo:: todo

.. _replication-strategy:

Replication
^^^^^^^^^^^

The replication strategy of a keyspace determines which nodes are replicas for a given token range. The two main
replication strategies are :ref:`simple-strategy` and :ref:`network-topology-strategy`.

.. _simple-strategy:

SimpleStrategy
~~~~~~~~~~~~~~

SimpleStrategy allows a single integer ``replication_factor`` to be defined. This determines the number of nodes that
should contain a copy of each row.  For example, if ``replication_factor`` is 3, then three different nodes should store
a copy of each row.

SimpleStrategy treats all nodes identically, ignoring any configured datacenters or racks.  To determine the replicas
for a token range, Cassandra iterates through the tokens in the ring, starting with the token range of interest.  For
each token, it checks whether the owning node has been added to the set of replicas, and if it has not, it is added to
the set.  This process continues until ``replication_factor`` distinct nodes have been added to the set of replicas.

.. _network-topology-strategy:

NetworkTopologyStrategy
~~~~~~~~~~~~~~~~~~~~~~~

NetworkTopologyStrategy allows a replication factor to be specified for each datacenter in the cluster.  Even if your
cluster only uses a single datacenter, NetworkTopologyStrategy should be prefered over SimpleStrategy to make it easier
to add new physical or virtual datacenters to the cluster later.

In addition to allowing the replication factor to be specified per-DC, NetworkTopologyStrategy also attempts to choose
replicas within a datacenter from different racks.  If the number of racks is greater than or equal to the replication
factor for the DC, each replica will be chosen from a different rack.  Otherwise, each rack will hold at least one
replica, but some racks may hold more than one. Note that this rack-aware behavior has some potentially `surprising
implications <https://issues.apache.org/jira/browse/CASSANDRA-3810>`_.  For example, if there are not an even number of
nodes in each rack, the data load on the smallest rack may be much higher.  Similarly, if a single node is bootstrapped
into a new rack, it will be considered a replica for the entire ring.  For this reason, many operators choose to
configure all nodes on a single "rack".

.. _transient-replication:

Transient Replication
~~~~~~~~~~~~~~~~~~~~~

Transient replication allows you to configure a subset of replicas to only replicate data that hasn't been incrementally
repaired. This allows you to decouple data redundancy from availability. For instance, if you have a keyspace replicated
at rf 3, and alter it to rf 5 with 2 transient replicas, you go from being able to tolerate one failed replica to being
able to tolerate two, without corresponding increase in storage usage. This is because 3 nodes will replicate all the data
for a given token range, and the other 2 will only replicate data that hasn't been incrementally repaired.

To use transient replication, you first need to enable it in ``cassandra.yaml``. Once enabled, both SimpleStrategy and
NetworkTopologyStrategy can be configured to transiently replicate data. You configure it by specifying replication factor
as ``<total_replicas>/<transient_replicas`` Both SimpleStrategy and NetworkTopologyStrategy support configuring transient
replication.

Transiently replicated keyspaces only support tables created with read_repair set to NONE and monotonic reads are not currently supported.
You also can't use LWT, logged batches, and counters in 4.0. You will possibly never be able to use materialized views
with transiently replicated keyspaces and probably never be able to use 2i with them.

Transient replication is an experimental feature that may not be ready for production use. The expected audienced is experienced
users of Cassandra capable of fully validating a deployment of their particular application. That means being able check
that operations like reads, writes, decommission, remove, rebuild, repair, and replace all work with your queries, data,
configuration, operational practices, and availability requirements.

It is anticipated that 4.next will support monotonic reads with transient replication as well as LWT, logged batches, and
counters.


Tunable Consistency
^^^^^^^^^^^^^^^^^^^

Cassandra supports a per-operation tradeoff between consistency and availability through *Consistency Levels*.
Essentially, an operation's consistency level specifies how many of the replicas need to respond to the coordinator in
order to consider the operation a success.

The following consistency levels are available:

``ONE``
  Only a single replica must respond.

``TWO``
  Two replicas must respond.

``THREE``
  Three replicas must respond.

``QUORUM``
  A majority (n/2 + 1) of the replicas must respond.

``ALL``
  All of the replicas must respond.

``LOCAL_QUORUM``
  A majority of the replicas in the local datacenter (whichever datacenter the coordinator is in) must respond.

``EACH_QUORUM``
  A majority of the replicas in each datacenter must respond.

``LOCAL_ONE``
  Only a single replica must respond.  In a multi-datacenter cluster, this also gaurantees that read requests are not
  sent to replicas in a remote datacenter.

``ANY``
  A single replica may respond, or the coordinator may store a hint. If a hint is stored, the coordinator will later
  attempt to replay the hint and deliver the mutation to the replicas.  This consistency level is only accepted for
  write operations.

Write operations are always sent to all replicas, regardless of consistency level. The consistency level simply
controls how many responses the coordinator waits for before responding to the client.

For read operations, the coordinator generally only issues read commands to enough replicas to satisfy the consistency
level, with one exception. Speculative retry may issue a redundant read request to an extra replica if the other replicas
have not responded within a specified time window.

Picking Consistency Levels
~~~~~~~~~~~~~~~~~~~~~~~~~~

It is common to pick read and write consistency levels that are high enough to overlap, resulting in "strong"
consistency.  This is typically expressed as ``W + R > RF``, where ``W`` is the write consistency level, ``R`` is the
read consistency level, and ``RF`` is the replication factor.  For example, if ``RF = 3``, a ``QUORUM`` request will
require responses from at least two of the three replicas.  If ``QUORUM`` is used for both writes and reads, at least
one of the replicas is guaranteed to participate in *both* the write and the read request, which in turn guarantees that
the latest write will be read. In a multi-datacenter environment, ``LOCAL_QUORUM`` can be used to provide a weaker but
still useful guarantee: reads are guaranteed to see the latest write from within the same datacenter.

If this type of strong consistency isn't required, lower consistency levels like ``ONE`` may be used to improve
throughput, latency, and availability.
