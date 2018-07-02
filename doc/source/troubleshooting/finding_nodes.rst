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

Find The Misbehaving Nodes
==========================

The first step to troubleshooting a Cassandra issue is to use error messages,
metrics and monitoring information to identify if the issue lies with the
clients or the server and if it does lie with the server find the problematic
nodes in the Cassandra cluster. The goal is to determine if this is a systemic
issue (e.g. a query pattern that affects the entire cluster) or isolated to a
subset of nodes (e.g. neighbors holding a shared token range or even a single
node with bad hardware).

There are many sources of information that help determine where the problem
lies. Some of the most common are mentioned below.

Client Logs and Errors
----------------------
Clients of the cluster often leave the best breadcrumbs to follow. Perhaps
client latencies or error rates have increased in a particular datacenter
(likely eliminating other datacenter's nodes), or clients are receiving a
particular kind of error code indicating a particular kind of problem.
Troubleshooters can often rule out many failure modes just by reading the error
messages. In fact, many Cassandra error messages include the last coordinator
contacted to help operators find nodes to start with.

Some common errors (likely culprit in parenthesis) assuming the client has
similar error names as the Datastax :ref:`drivers <client-drivers>`:

* ``SyntaxError`` (**client**). This and other ``QueryValidationException``
  indicate that the client sent a malformed request. These are rarely server
  issues and usually indicate bad queries.
* ``UnavailableException`` (**server**): This means that the Cassandra
  coordinator node has rejected the query as it believes that insufficent
  replica nodes are available.  If many coordinators are throwing this error it
  likely means that there really are (typically) multiple nodes down in the
  cluster and you can identify them using :ref:`nodetool status
  <nodetool-status>` If only a single coordinator is throwing this error it may
  mean that node has been partitioned from the rest.
* ``OperationTimedOutException`` (**server**): This is the most frequent
  timeout message raised when clients set timeouts and means that the query
  took longer than the supplied timeout. This is a *client side* timeout
  meaning that it took longer than the client specified timeout. The error
  message will include the coordinator node that was last tried which is
  usually a good starting point. This error usually indicates either
  aggressive client timeout values or latent server coordinators/replicas.
* ``ReadTimeoutException`` or ``WriteTimeoutException`` (**server**): These
  are raised when clients do not specify lower timeouts and there is a
  *coordinator* timeouts based on the values supplied in the ``cassandra.yaml``
  configuration file. They usually indicate a serious server side problem as
  the default values are usually multiple seconds.

Metrics
-------

If you have Cassandra :ref:`metrics <monitoring-metrics>` reporting to a
centralized location such as `Graphite <https://graphiteapp.org/>`_ or
`Grafana <https://grafana.com/>`_ you can typically use those to narrow down
the problem. At this stage narrowing down the issue to a particular
datacenter, rack, or even group of nodes is the main goal. Some helpful metrics
to look at are:

Errors
^^^^^^
Cassandra refers to internode messaging errors as "drops", and provided a
number of :ref:`Dropped Message Metrics <dropped-metrics>` to help narrow
down errors. If particular nodes are dropping messages actively, they are
likely related to the issue.

Latency
^^^^^^^
For timeouts or latency related issues you can start with :ref:`Table
Metrics <table-metrics>` by comparing Coordinator level metrics e.g.
``CoordinatorReadLatency`` or ``CoordinatorWriteLatency`` with their associated
replica metrics e.g.  ``ReadLatency`` or ``WriteLatency``.  Issues usually show
up on the ``99th`` percentile before they show up on the ``50th`` percentile or
the ``mean``.  While ``maximum`` coordinator latencies are not typically very
helpful due to the exponentially decaying reservoir used internally to produce
metrics, ``maximum`` replica latencies that correlate with increased ``99th``
percentiles on coordinators can help narrow down the problem.

There are usually three main possibilities:

1. Coordinator latencies are high on all nodes, but only a few node's local
   read latencies are high. This points to slow replica nodes and the
   coordinator's are just side-effects. This usually happens when clients are
   not token aware.
2. Coordinator latencies and replica latencies increase at the
   same time on the a few nodes. If clients are token aware this is almost
   always what happens and points to slow replicas of a subset of token
   ranges (only part of the ring).
3. Coordinator and local latencies are high on many nodes. This usually
   indicates either a tipping point in the cluster capacity (too many writes or
   reads per second), or a new query pattern.

It's important to remember that depending on the client's load balancing
behavior and consistency levels coordinator and replica metrics may or may
not correlate. In particular if you use ``TokenAware`` policies the same
node's coordinator and replica latencies will often increase together, but if
you just use normal ``DCAwareRoundRobin`` coordinator latencies can increase
with unrelated replica node's latencies. For example:

* ``TokenAware`` + ``LOCAL_ONE``: should always have coordinator and replica
  latencies on the same node rise together
* ``TokenAware`` + ``LOCAL_QUORUM``: should always have coordinator and
  multiple replica latencies rise together in the same datacenter.
* ``TokenAware`` + ``QUORUM``: replica latencies in other datacenters can
  affect coordinator latencies.
* ``DCAwareRoundRobin`` + ``LOCAL_ONE``: coordinator latencies and unrelated
  replica node's latencies will rise together.
* ``DCAwareRoundRobin`` + ``LOCAL_QUORUM``: different coordinator and replica
  latencies will rise together with little correlation.

Query Rates
^^^^^^^^^^^
Sometimes the :ref:`Table <table-metrics>` query rate metrics can help
narrow down load issues as  "small" increase in coordinator queries per second
(QPS) may correlate with a very large increase in replica level QPS. This most
often happens with ``BATCH`` writes, where a client may send a single ``BATCH``
query that might contain 50 statements in it, which if you have 9 copies (RF=3,
three datacenters) means that every coordinator ``BATCH`` write turns into 450
replica writes! This is why keeping ``BATCH``'s to the same partition is so
critical, otherwise you can exhaust significant CPU capacitity with a "single"
query.


Next Step: Investigate the Node(s)
----------------------------------

Once you have narrowed down the problem as much as possible (datacenter, rack
, node), login to one of the nodes using SSH and proceed to debug using
:ref:`logs <reading-logs>`, :ref:`nodetool <use-nodetool>`, and
:ref:`os tools <use-os-tools>`. If you are not able to login you may still
have access to :ref:`logs <reading-logs>` and :ref:`nodetool <use-nodetool>`
remotely.
