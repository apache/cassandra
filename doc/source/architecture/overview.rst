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

Overview
--------

# Architecture overview

This document is meant to present a high level overview of the architecture of Apache Cassandra. More in depth information is available throughout the rest of the documentation. This section breaks up and describes the subsections of the architecture titled dynamo, storage engine, and guarantee. For each section it first provides a clear definition of the title to provide the context necessary to understand the subsections.  Then it connects how each subsection fits into the greater architecture.

## Dynamo

Dynamo is a set of techniques that lead to a highly available key value structured storage system. These principles underlie the motivation for the architectural decisions made during the development and maintenance of Cassandra.

These architectural goals at a high level are: 

Incremental scalability, meaning nodes can be added as needed without affecting the experience of the end user.

Symmetry as in no node should have any more or less responsibilities from other nodes. 

Decentralization, which extends symmetry, meaning nodes should communicate in a peer to peer design completely lacking in central authority. 

Heterogeneity, meaning the system can automatically exploit the benefits of the infrastructure it runs on. For example,  a system based on Dynamo knows to store more data on hardware that is optimized for storage.

### Implementation of Dynamo

Apache Cassandra is designed to distribute data typically across several physically separate locations. This is achieved through storing data on clusters of nodes. The topology of a cluster is described by data centers and racks. A rack contains many nodes and a data center can have many racks. By default Cassandra is configured with a single data center and a single rack. By defining the cluster’s topology Cassandra is able to use this information to store the data optimally as well as query efficiently. Nodes understand where other nodes are placed in the network in relation to each other by the snitch. The snitch gives Cassandra the awareness of its clusters topology which allows for these optimizations. 

Nodes communicate with each other to exchange information using the gossip protocol. Gossip is a peer to peer based protocol that defines how nodes interact over the network. Information exchange in this context is necessary to maintain the health of the system, to detect failure of individual nodes, and to auto replicate data when necessary. Nodes keep track of each others state information and monitor each others health. This supports decentralization, which leads to no single point of failure. Having no single point of failure leads to high availability of data. 

Data distribution over nodes is represented abstractly as a ring of tokens. This ring represents a cluster of nodes where each token is an integer value. Each node takes ownership of these ranges by being assigned a token and owning the range below and including that token up to the token owned by the previous node. It can be viewed as a set of numbers (tokens) displayed in a ring, where each node owns a range of these tokens.

Each row of data has a partition key in Cassandra used to identify a partition. A partitioner is a hash function to identify a partition when given a partition key. This will query the data associated with that particular partition token.

Part of how high availability in Cassandra is achieved is by replication of data. Cassandra's model of consistency is tunable consistency which means a particular amount of replica nodes must be updated for the operation to be considered written. This allows a tradeoff between consistency (all replicas must be updated is absolute consistency) and availability (if zero replicas need to be written to then the data is always available to be read). It also leads to Cassandra being always writeable. 

Cassandra in general has two replication strategies, SimpleStrategy and NetworkTopologyStrategy.

SimpleStrategy uses the replication factor, simply an integer, to determine the number of nodes to update rows to. So where the datacenters and nodes are physically configured in the cluster doesn’t play a role in replication.

NetworkTopologyStrategy uses a replication factor per datacenter and intelligently determines the best nodes to choose as replicas from each rack.


## Storage Engine

.. todo:: todo

## Guarantees

.. todo:: todo
