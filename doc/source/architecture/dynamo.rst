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
======

Apache Cassandra relies on a number of techniques from Amazon's `Dynamo
<http://courses.cse.tamu.edu/caverlee/csce438/readings/dynamo-paper.pdf>`_
distributed storage key-value system. Each node in the Dynamo system has three
main components:

- Request coordination over a partitioned dataset
- Ring membership and failure detection
- A local persistence (storage) engine

Cassandra primarily draws from the first two clustering components,
while using a storage engine based on a Log Structured Merge Tree
(`LSM <http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.44.2782&rep=rep1&type=pdf>`_).
In particular, Cassandra relies on Dynamo style:

- Dataset partitioning using consistent hashing
- Multi-master replication using versioned data and tunable consistency
- Distributed cluster membership and failure detection via a gossip protocol
- Incremental scale-out on commodity hardware

Cassandra was designed this way to meet large-scale (PiB+) business-critical
storage requirements. In particular, as applications demanded full global
replication of petabyte scale datasets along with always available low-latency
reads and writes, it became imperative to design a new kind of database model
as the relational database systems of the time struggled to meet the new
requirements of global scale applications.

Dataset Partitioning: Consistent Hashing
----------------------------------------

Cassandra achieves horizontal scalability by
`partitioning <https://en.wikipedia.org/wiki/Partition_(database)>`_
all data stored in the system using a hash function. Each partition is replicated
to multiple physical nodes, often across failure domains such as racks and even
datacenters. As every replica can independently accept mutations to every key
that it owns, every key must be versioned. Unlike in the original Dynamo paper
where deterministic versions and vector clocks were used to reconcile concurrent
updates to a key, Cassandra uses a simpler last write wins model where every
mutation is timestamped (including deletes) and then the latest version of data
is the "winning" value. Formally speaking, Cassandra uses a Last-Write-Wins Element-Set
conflict-free replicated data type for each CQL row (a.k.a `LWW-Element-Set CRDT
<https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type#LWW-Element-Set_(Last-Write-Wins-Element-Set)>`_)
to resolve conflicting mutations on replica sets.

 .. _consistent-hashing-token-ring:

Consistent Hashing using a Token Ring
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Cassandra partitions data over storage nodes using a special form of hashing
called `consistent hashing <https://en.wikipedia.org/wiki/Consistent_hashing>`_.
In naive data hashing, you typically allocate keys to buckets by taking a hash
of the key modulo the number of buckets. For example, if you want to distribute
data to 100 nodes using naive hashing you might assign every node to a bucket
between 0 and 100, hash the input key modulo 100, and store the data on the
associated bucket. In this naive scheme, however, adding a single node might
invalidate almost all of the mappings.

Cassandra instead maps every node to one or more tokens on a continuous hash
ring, and defines ownership by hashing a key onto the ring and then "walking"
the ring in one direction, similar to the `Chord
<https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf>`_
algorithm. The main difference of consistent hashing to naive data hashing is
that when the number of nodes (buckets) to hash into changes, consistent
hashing only has to move a small fraction of the keys.

For example, if we have an eight node cluster with evenly spaced tokens, and
a replication factor (RF) of 3, then to find the owning nodes for a key we
first hash that key to generate a token (which is just the hash of the key),
and then we "walk" the ring in a clockwise fashion until we encounter three
distinct nodes, at which point we have found all the replicas of that key.
This example of an eight node cluster with `RF=3` can be visualized as follows:

.. figure:: images/ring.svg
   :scale: 75 %
   :alt: Dynamo Ring

You can see that in a Dynamo like system, ranges of keys, also known as **token
ranges**, map to the same physical set of nodes. In this example, all keys that
fall in the token range excluding token 1 and including token 2 (`range(t1, t2]`)
are stored on nodes 2, 3 and 4.

Multiple Tokens per Physical Node (a.k.a. `vnodes`)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Simple single token consistent hashing works well if you have many physical
nodes to spread data over, but with evenly spaced tokens and a small number of
physical nodes, incremental scaling (adding just a few nodes of capacity) is
difficult because there are no token selections for new nodes that can leave
the ring balanced. Cassandra seeks to avoid token imbalance because uneven
token ranges lead to uneven request load. For example, in the previous example
there is no way to add a ninth token without causing imbalance; instead we
would have to insert ``8`` tokens in the midpoints of the existing ranges.

The Dynamo paper advocates for the use of "virtual nodes" to solve this
imbalance problem. Virtual nodes solve the problem by assigning multiple
tokens in the token ring to each physical node. By allowing a single physical
node to take multiple positions in the ring, we can make small clusters look
larger and therefore even with a single physical node addition we can make it
look like we added many more nodes, effectively taking many smaller pieces of
data from more ring neighbors when we add even a single node.

Cassandra introduces some nomenclature to handle these concepts:

- **Token**: A single position on the `dynamo` style hash ring.
- **Endpoint**: A single physical IP and port on the network.
- **Host ID**: A unique identifier for a single "physical" node, usually
  present at one `Endpoint` and containing one or more `Tokens`.
- **Virtual Node** (or **vnode**): A `Token` on the hash ring owned by the same
  physical node, one with the same `Host ID`.

The mapping of **Tokens** to **Endpoints** gives rise to the **Token Map**
where Cassandra keeps track of what ring positions map to which physical
endpoints.  For example, in the following figure we can represent an eight node
cluster using only four physical nodes by assigning two tokens to every node:

.. figure:: images/vnodes.svg
   :scale: 75 %
   :alt: Virtual Tokens Ring


Multiple tokens per physical node provide the following benefits:

1. When a new node is added it accepts approximately equal amounts of data from
   other nodes in the ring, resulting in equal distribution of data across the
   cluster.
2. When a node is decommissioned, it loses data roughly equally to other members
   of the ring, again keeping equal distribution of data across the cluster.
3. If a node becomes unavailable, query load (especially token aware query load),
   is evenly distributed across many other nodes.

Multiple tokens, however, can also have disadvantages:

1. Every token introduces up to ``2 * (RF - 1)`` additional neighbors on the
   token ring, which means that there are more combinations of node failures
   where we lose availability for a portion of the token ring. The more tokens
   you have, `the higher the probability of an outage
   <https://jolynch.github.io/pdf/cassandra-availability-virtual.pdf>`_.
2. Cluster-wide maintenance operations are often slowed. For example, as the
   number of tokens per node is increased, the number of discrete repair
   operations the cluster must do also increases.
3. Performance of operations that span token ranges could be affected.

Note that in Cassandra ``2.x``, the only token allocation algorithm available
was picking random tokens, which meant that to keep balance the default number
of tokens per node had to be quite high, at ``256``. This had the effect of
coupling many physical endpoints together, increasing the risk of
unavailability. That is why in ``3.x +`` the new deterministic token allocator
was added which intelligently picks tokens such that the ring is optimally
balanced while requiring a much lower number of tokens per physical node.


Multi-master Replication: Versioned Data and Tunable Consistency
----------------------------------------------------------------

Cassandra replicates every partition of data to many nodes across the cluster
to maintain high availability and durability. When a mutation occurs, the
coordinator hashes the partition key to determine the token range the data
belongs to and then replicates the mutation to the replicas of that data
according to the :ref:`Replication Strategy <replication-strategy>`.

All replication strategies have the notion of a **replication factor** (``RF``),
which indicates to Cassandra how many copies of the partition should exist.
For example with a ``RF=3`` keyspace, the data will be written to three
distinct **replicas**. Replicas are always chosen such that they are distinct
physical nodes which is achieved by skipping virtual nodes if needed.
Replication strategies may also choose to skip nodes present in the same failure
domain such as racks or datacenters so that Cassandra clusters can tolerate
failures of whole racks and even datacenters of nodes.

.. _replication-strategy:

Replication Strategy
^^^^^^^^^^^^^^^^^^^^

Cassandra supports pluggable **replication strategies**, which determine which
physical nodes act as replicas for a given token range. Every keyspace of
data has its own replication strategy. All production deployments should use
the :ref:`network-topology-strategy` while the :ref:`simple-strategy` replication
strategy is useful only for testing clusters where you do not yet know the
datacenter layout of the cluster.

.. _network-topology-strategy:

``NetworkTopologyStrategy``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

``NetworkTopologyStrategy`` allows a replication factor to be specified for each
datacenter in the cluster. Even if your cluster only uses a single datacenter,
``NetworkTopologyStrategy`` should be preferred over ``SimpleStrategy`` to make it
easier to add new physical or virtual datacenters to the cluster later.

In addition to allowing the replication factor to be specified individually by
datacenter, ``NetworkTopologyStrategy`` also attempts to choose replicas within a
datacenter from different racks as specified by the :ref:`Snitch <snitch>`. If
the number of racks is greater than or equal to the replication factor for the
datacenter, each replica is guaranteed to be chosen from a different rack.
Otherwise, each rack will hold at least one replica, but some racks may hold
more than one. Note that this rack-aware behavior has some potentially
`surprising implications
<https://issues.apache.org/jira/browse/CASSANDRA-3810>`_.  For example, if
there are not an even number of nodes in each rack, the data load on the
smallest rack may be much higher.  Similarly, if a single node is bootstrapped
into a brand new rack, it will be considered a replica for the entire ring.
For this reason, many operators choose to configure all nodes in a single
availability zone or similar failure domain as a single "rack".

.. _simple-strategy:

``SimpleStrategy``
~~~~~~~~~~~~~~~~~~

``SimpleStrategy`` allows a single integer ``replication_factor`` to be defined. This determines the number of nodes that
should contain a copy of each row.  For example, if ``replication_factor`` is 3, then three different nodes should store
a copy of each row.

``SimpleStrategy`` treats all nodes identically, ignoring any configured datacenters or racks.  To determine the replicas
for a token range, Cassandra iterates through the tokens in the ring, starting with the token range of interest.  For
each token, it checks whether the owning node has been added to the set of replicas, and if it has not, it is added to
the set.  This process continues until ``replication_factor`` distinct nodes have been added to the set of replicas.

.. _transient-replication:

Transient Replication
~~~~~~~~~~~~~~~~~~~~~

Transient replication is an experimental feature in Cassandra 4.0 not present
in the original Dynamo paper. It allows you to configure a subset of replicas
to only replicate data that hasn't been incrementally repaired. This allows you
to decouple data redundancy from availability. For instance, if you have a
keyspace replicated at rf 3, and alter it to rf 5 with 2 transient replicas,
you go from being able to tolerate one failed replica to being able to tolerate
two, without corresponding increase in storage usage. This is because 3 nodes
will replicate all the data for a given token range, and the other 2 will only
replicate data that hasn't been incrementally repaired.

To use transient replication, you first need to enable it in
``cassandra.yaml``. Once enabled, both ``SimpleStrategy`` and
``NetworkTopologyStrategy`` can be configured to transiently replicate data.
You configure it by specifying replication factor as
``<total_replicas>/<transient_replicas`` Both ``SimpleStrategy`` and
``NetworkTopologyStrategy`` support configuring transient replication.

Transiently replicated keyspaces only support tables created with read_repair
set to ``NONE`` and monotonic reads are not currently supported.  You also
can't use ``LWT``, logged batches, or counters in 4.0. You will possibly never be
able to use materialized views with transiently replicated keyspaces and
probably never be able to use secondary indices with them.

Transient replication is an experimental feature that may not be ready for
production use. The expected audience is experienced users of Cassandra
capable of fully validating a deployment of their particular application. That
means being able check that operations like reads, writes, decommission,
remove, rebuild, repair, and replace all work with your queries, data,
configuration, operational practices, and availability requirements.

It is anticipated that ``4.next`` will support monotonic reads with transient
replication as well as LWT, logged batches, and counters.

Data Versioning
^^^^^^^^^^^^^^^

Cassandra uses mutation timestamp versioning to guarantee eventual consistency of
data. Specifically all mutations that enter the system do so with a timestamp
provided either from a client clock or, absent a client provided timestamp,
from the coordinator node's clock. Updates resolve according to the conflict
resolution rule of last write wins. Cassandra's correctness does depend on
these clocks, so make sure a proper time synchronization process is running
such as NTP.

Cassandra applies separate mutation timestamps to every column of every row
within a CQL partition. Rows are guaranteed to be unique by primary key, and
each column in a row resolve concurrent mutations according to last-write-wins
conflict resolution. This means that updates to different primary keys within a
partition can actually resolve without conflict! Furthermore the CQL collection
types such as maps and sets use this same conflict free mechanism, meaning
that concurrent updates to maps and sets are guaranteed to resolve as well.

Replica Synchronization
~~~~~~~~~~~~~~~~~~~~~~~

As replicas in Cassandra can accept mutations independently, it is possible
for some replicas to have newer data than others. Cassandra has many best-effort
techniques to drive convergence of replicas including
`Replica read repair <read-repair>` in the read path and
`Hinted handoff <hints>` in the write path.

These techniques are only best-effort, however, and to guarantee eventual
consistency Cassandra implements `anti-entropy repair <repair>` where replicas
calculate hierarchical hash-trees over their datasets called `Merkle Trees
<https://en.wikipedia.org/wiki/Merkle_tree>`_ that can then be compared across
replicas to identify mismatched data. Like the original Dynamo paper Cassandra
supports "full" repairs where replicas hash their entire dataset, create Merkle
trees, send them to each other and sync any ranges that don't match.

Unlike the original Dynamo paper, Cassandra also implements sub-range repair
and incremental repair. Sub-range repair allows Cassandra to increase the
resolution of the hash trees (potentially down to the single partition level)
by creating a larger number of trees that span only a portion of the data
range.  Incremental repair allows Cassandra to only repair the partitions that
have changed since the last repair.

Tunable Consistency
^^^^^^^^^^^^^^^^^^^

Cassandra supports a per-operation tradeoff between consistency and
availability through **Consistency Levels**. Cassandra's consistency levels
are a version of Dynamo's ``R + W > N`` consistency mechanism where operators
could configure the number of nodes that must participate in reads (``R``)
and writes (``W``) to be larger than the replication factor (``N``). In
Cassandra, you instead choose from a menu of common consistency levels which
allow the operator to pick ``R`` and ``W`` behavior without knowing the
replication factor. Generally writes will be visible to subsequent reads when
the read consistency level contains enough nodes to guarantee a quorum intersection
with the write consistency level.

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

Write operations **are always sent to all replicas**, regardless of consistency
level. The consistency level simply controls how many responses the coordinator
waits for before responding to the client.

For read operations, the coordinator generally only issues read commands to
enough replicas to satisfy the consistency level. The one exception to this is
when speculative retry may issue a redundant read request to an extra replica
if the original replicas have not responded within a specified time window.

Picking Consistency Levels
~~~~~~~~~~~~~~~~~~~~~~~~~~

It is common to pick read and write consistency levels such that the replica
sets overlap, resulting in all acknowledged writes being visible to subsequent
reads. This is typically expressed in the same terms Dynamo does, in that ``W +
R > RF``, where ``W`` is the write consistency level, ``R`` is the read
consistency level, and ``RF`` is the replication factor.  For example, if ``RF
= 3``, a ``QUORUM`` request will require responses from at least ``2/3``
replicas.  If ``QUORUM`` is used for both writes and reads, at least one of the
replicas is guaranteed to participate in *both* the write and the read request,
which in turn guarantees that the quorums will overlap and the write will be
visible to the read.

In a multi-datacenter environment, ``LOCAL_QUORUM`` can be used to provide a
weaker but still useful guarantee: reads are guaranteed to see the latest write
from within the same datacenter. This is often sufficient as clients homed to
a single datacenter will read their own writes.

If this type of strong consistency isn't required, lower consistency levels
like ``LOCAL_ONE`` or ``ONE`` may be used to improve throughput, latency, and
availability. With replication spanning multiple datacenters, ``LOCAL_ONE`` is
typically less available than ``ONE`` but is faster as a rule. Indeed ``ONE``
will succeed if a single replica is available in any datacenter.

Distributed Cluster Membership and Failure Detection
----------------------------------------------------

The replication protocols and dataset partitioning rely on knowing which nodes
are alive and dead in the cluster so that write and read operations can be
optimally routed. In Cassandra liveness information is shared in a distributed
fashion through a failure detection mechanism based on a gossip protocol.

.. _gossip:

Gossip
^^^^^^

Gossip is how Cassandra propagates basic cluster bootstrapping information such
as endpoint membership and internode network protocol versions. In Cassandra's
gossip system, nodes exchange state information not only about themselves but
also about other nodes they know about. This information is versioned with a
vector clock of ``(generation, version)`` tuples, where the generation is a
monotonic timestamp and version is a logical clock the increments roughly every
second. These logical clocks allow Cassandra gossip to ignore old versions of
cluster state just by inspecting the logical clocks presented with gossip
messages.

Every node in the Cassandra cluster runs the gossip task independently and
periodically. Every second, every node in the cluster:

1. Updates the local node's heartbeat state (the version) and constructs the
   node's local view of the cluster gossip endpoint state.
2. Picks a random other node in the cluster to exchange gossip endpoint state
   with.
3. Probabilistically attempts to gossip with any unreachable nodes (if one exists)
4. Gossips with a seed node if that didn't happen in step 2.

When an operator first bootstraps a Cassandra cluster they designate certain
nodes as "seed" nodes. Any node can be a seed node and the only difference
between seed and non-seed nodes is seed nodes are allowed to bootstrap into the
ring without seeing any other seed nodes. Furthermore, once a cluster is
bootstrapped, seed nodes become "hotspots" for gossip due to step 4 above.

As non-seed nodes must be able to contact at least one seed node in order to
bootstrap into the cluster, it is common to include multiple seed nodes, often
one for each rack or datacenter. Seed nodes are often chosen using existing
off-the-shelf service discovery mechanisms.

.. note::
   Nodes do not have to agree on the seed nodes, and indeed once a cluster is
   bootstrapped, newly launched nodes can be configured to use any existing
   nodes as "seeds". The only advantage to picking the same nodes as seeds
   is it increases their usefullness as gossip hotspots.

Currently, gossip also propagates token metadata and schema *version*
information. This information forms the control plane for scheduling data
movements and schema pulls. For example, if a node sees a mismatch in schema
version in gossip state, it will schedule a schema sync task with the other
nodes. As token information propagates via gossip it is also the control plane
for teaching nodes which endpoints own what data.

Ring Membership and Failure Detection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Gossip forms the basis of ring membership, but the **failure detector**
ultimately makes decisions about if nodes are ``UP`` or ``DOWN``. Every node in
Cassandra runs a variant of the `Phi Accrual Failure Detector
<https://www.computer.org/csdl/proceedings-article/srds/2004/22390066/12OmNvT2phv>`_,
in which every node is constantly making an independent decision of if their
peer nodes are available or not. This decision is primarily based on received
heartbeat state. For example, if a node does not see an increasing heartbeat
from a node for a certain amount of time, the failure detector "convicts" that
node, at which point Cassandra will stop routing reads to it (writes will
typically be written to hints). If/when the node starts heartbeating again,
Cassandra will try to reach out and connect, and if it can open communication
channels it will mark that node as available.

.. note::
   UP and DOWN state are local node decisions and are not propagated with
   gossip. Heartbeat state is propagated with gossip, but nodes will not
   consider each other as "UP" until they can successfully message each other
   over an actual network channel.

Cassandra will never remove a node from gossip state without explicit
instruction from an operator via a decommission operation or a new node
bootstrapping with a ``replace_address_first_boot`` option. This choice is
intentional to allow Cassandra nodes to temporarily fail without causing data
to needlessly re-balance. This also helps to prevent simultaneous range
movements, where multiple replicas of a token range are moving at the same
time, which can violate monotonic consistency and can even cause data loss.

Incremental Scale-out on Commodity Hardware
--------------------------------------------

Cassandra scales-out to meet the requirements of growth in data size and
request rates. Scaling-out means adding additional nodes to the ring, and
every additional node brings linear improvements in compute and storage. In
contrast, scaling-up implies adding more capacity to the existing database
nodes. Cassandra is also capable of scale-up, and in certain environments it
may be preferable depending on the deployment. Cassandra gives operators the
flexibility to chose either scale-out or scale-up.

One key aspect of Dynamo that Cassandra follows is to attempt to run on
commodity hardware, and many engineering choices are made under this
assumption. For example, Cassandra assumes nodes can fail at any time,
auto-tunes to make the best use of CPU and memory resources available and makes
heavy use of advanced compression and caching techniques to get the most
storage out of limited memory and storage capabilities.

Simple Query Model
^^^^^^^^^^^^^^^^^^

Cassandra, like Dynamo, chooses not to provide cross-partition transactions
that are common in SQL Relational Database Management Systems (RDBMS). This
both gives the programmer a simpler read and write API, and allows Cassandra to
more easily scale horizontally since multi-partition transactions spanning
multiple nodes are notoriously difficult to implement and typically very
latent.

Instead, Cassanda chooses to offer fast, consistent, latency at any scale for
single partition operations, allowing retrieval of entire partitions or only
subsets of partitions based on primary key filters. Furthermore, Cassandra does
support single partition compare and swap functionality via the lightweight
transaction CQL API.

Simple Interface for Storing Records
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Cassandra, in a slight departure from Dynamo, chooses a storage interface that
is more sophisticated then "simple key value" stores but significantly less
complex than SQL relational data models.  Cassandra presents a wide-column
store interface, where partitions of data contain multiple rows, each of which
contains a flexible set of individually typed columns. Every row is uniquely
identified by the partition key and one or more clustering keys, and every row
can have as many columns as needed.

This allows users to flexibly add new columns to existing datasets as new
requirements surface. Schema changes involve only metadata changes and run
fully concurrently with live workloads. Therefore, users can safely add columns
to existing Cassandra databases while remaining confident that query
performance will not degrade.
