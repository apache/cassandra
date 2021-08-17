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

.. _guarantees:

Guarantees
==============
Apache Cassandra is a highly scalable and reliable database.  Cassandra is used in web based applications that serve large number of clients and the quantity of data processed is web-scale  (Petabyte) large.  Cassandra   makes some guarantees about its scalability, availability and reliability. To fully understand the inherent limitations of a storage system in an environment in which a certain level of network partition failure is to be expected and taken into account when designing the system it is important to first briefly  introduce the CAP theorem.

What is CAP?
^^^^^^^^^^^^^
According to the CAP theorem it is not possible for a distributed data store to provide more than two of the following guarantees simultaneously.

- Consistency: Consistency implies that every read receives the most recent write or errors out
- Availability: Availability implies that every request receives a response. It is not guaranteed that the response contains the most recent write or data.
- Partition tolerance: Partition tolerance refers to the tolerance of a storage system to failure of a network partition.  Even if some of the messages are dropped or delayed the system continues to operate.

CAP theorem implies that when using a network partition, with the inherent risk of partition failure, one has to choose between consistency and availability and both cannot be guaranteed at the same time. CAP theorem is illustrated in Figure 1.

.. figure:: Figure_1_guarantees.jpg

Figure 1. CAP Theorem

High availability is a priority in web based applications and to this objective Cassandra chooses Availability and Partition Tolerance from the CAP guarantees, compromising on data Consistency to some extent.

Cassandra makes the following guarantees.

- High Scalability
- High Availability
- Durability
- Eventual Consistency of writes to a single table
- Lightweight transactions with linearizable consistency
- Batched writes across multiple tables are guaranteed to succeed completely or not at all
- Secondary indexes are guaranteed to be consistent with their local replicas data

High Scalability
^^^^^^^^^^^^^^^^^
Cassandra is a highly scalable storage system in which nodes may be added/removed as needed. Using gossip-based protocol a unified and consistent membership  list is kept at each node.

High Availability
^^^^^^^^^^^^^^^^^^^
Cassandra guarantees high availability of data by  implementing a fault-tolerant storage system. Failure detection in a node is detected using a gossip-based protocol.

Durability
^^^^^^^^^^^^
Cassandra guarantees data durability by using replicas. Replicas are multiple copies of a data stored on different nodes in a cluster. In a multi-datacenter environment the replicas may be stored on different datacenters. If one replica is lost due to unrecoverable  node/datacenter failure the data is not completely lost as replicas are still available.

Eventual Consistency
^^^^^^^^^^^^^^^^^^^^^^
Meeting the requirements of performance, reliability, scalability and high availability in production Cassandra is an eventually consistent storage system. Eventually consistent implies that all updates reach all replicas eventually. Divergent versions of the same data may exist temporarily but they are eventually reconciled to a consistent state. Eventual consistency is a tradeoff to achieve high availability and it involves some read and write latencies.

Lightweight transactions with linearizable consistency
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Data must be read and written in a sequential order. Paxos consensus protocol is used to implement lightweight transactions. Paxos protocol implements lightweight transactions that are able to handle concurrent operations using linearizable consistency. Linearizable consistency is sequential consistency with real-time constraints and it ensures transaction isolation with compare and set (CAS) transaction. With CAS replica data is compared and data that is found to be out of date is set to the most consistent value. Reads with linearizable consistency allow reading the current state of the data, which may possibly be uncommitted, without making a new addition or update.

Batched Writes
^^^^^^^^^^^^^^^

The guarantee for batched writes across multiple tables is that they will eventually succeed, or none will.  Batch data is first written to batchlog system data, and when the batch data has been successfully stored in the cluster the batchlog data is removed.  The batch is replicated to another node to ensure the full batch completes in the event the coordinator node fails.

Secondary Indexes
^^^^^^^^^^^^^^^^^^
A secondary index is an index on a column and is used to query a table that is normally not queryable. Secondary indexes when built are guaranteed to be consistent with their local replicas.
