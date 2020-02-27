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

.. _overview:

Overview
========

Apache Cassandra is an open source, distributed, NoSQL database. It presents
a partitioned wide column storage model with eventually consistent semantics.

Apache Cassandra was initially designed at `Facebook
<https://www.cs.cornell.edu/projects/ladis2009/papers/lakshman-ladis2009.pdf>`_
using a staged event-driven architecture (`SEDA
<http://www.sosp.org/2001/papers/welsh.pdf>`_) to implement a combination of
Amazon’s `Dynamo
<http://courses.cse.tamu.edu/caverlee/csce438/readings/dynamo-paper.pdf>`_
distributed storage and replication techniques combined with Google's `Bigtable
<https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf>`_
data and storage engine model. Dynamo and Bigtable were both developed to meet
emerging requirements for scalable, reliable and highly available storage
systems, but each had areas that could be improved.

Cassandra was designed as a best in class combination of both systems to meet
emerging large scale, both in data footprint and query volume, storage
requirements. As applications began to require full global replication and
always available low-latency reads and writes, it became imperative to design a
new kind of database model as the relational database systems of the time
struggled to meet the new requirements of global scale applications.

Systems like Cassandra are designed for these challenges and seek the
following design objectives:

- Full multi-master database replication
- Global availability at low latency
- Scaling out on commodity hardware
- Linear throughput increase with each additional processor
- Online load balancing and cluster growth
- Partitioned key-oriented queries
- Flexible schema

Features
--------

Cassandra provides the Cassandra Query Language (CQL), an SQL-like language,
to create and update database schema and access data. CQL allows users to
organize data within a cluster of Cassandra nodes using:

- **Keyspace**: defines how a dataset is replicated, for example in which
  datacenters and how many copies. Keyspaces contain tables.
- **Table**: defines the typed schema for a collection of partitions. Cassandra
  tables have flexible addition of new columns to tables with zero downtime.
  Tables contain partitions, which contain partitions, which contain columns.
- **Partition**: defines the mandatory part of the primary key all rows in
  Cassandra must have. All performant queries supply the partition key in
  the query.
- **Row**: contains a collection of columns identified by a unique primary key
  made up of the partition key and optionally additional clustering keys.
- **Column**: A single datum with a type which belong to a row.

CQL supports numerous advanced features over a partitioned dataset such as:

- Single partition lightweight transactions with atomic compare and set
  semantics.
- User-defined types, functions and aggregates
- Collection types including sets, maps, and lists.
- Local secondary indices
- (Experimental) materialized views

Cassandra explicitly chooses not to implement operations that require cross
partition coordination as they are typically slow and hard to provide highly
available global semantics. For example Cassandra does not support:

- Cross partition transactions
- Distributed joins
- Foreign keys or referential integrity.

Operating
---------

Apache Cassandra configuration settings are configured in the ``cassandra.yaml``
file that can be edited by hand or with the aid of configuration management tools.
Some settings can be manipulated live using an online interface, but others
require a restart of the database to take effect.

Cassandra provides tools for managing a cluster. The ``nodetool`` command
interacts with Cassandra's live control interface, allowing runtime manipulation
of many settings from ``cassandra.yaml``. The ``auditlogviewer`` is used
to view the audit logs. The  ``fqltool`` is used to view, replay and compare
full query logs.  The ``auditlogviewer`` and ``fqltool`` are new tools in
Apache Cassandra 4.0.

In addition, Cassandra supports out of the box atomic snapshot functionality,
which presents a point in time snapshot of Cassandra's data for easy
integration with many backup tools. Cassandra also supports incremental backups
where data can be backed up as it is written.

Apache Cassandra 4.0 has added several new features including virtual tables.
transient replication, audit logging, full query logging, and support for Java
11. Two of these features are experimental: transient replication and Java 11
support.
