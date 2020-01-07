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
==========


Apache Cassandra is an open source, distributed, NoSQL database based on the key/value data storage model. Cassandra is based on a query-driven data model.  Data is stored in tables in rows and columns.  Cassandra is designed to meet the requirements of web-scale storage in which reliability, scalability and high availability have the highest priority. Apache Cassandra is eventually consistent, which implies that updates are eventually received by all replicas. Cassandra has provision to replicate data across the nodes in a cluster to provide data durability and reliability.

Cassandra supports lightweight transactions with compare and set but does not support ACID (Atomicity, Consistency, Isolation, Durability) transaction properties. Cassandra does not support joins, foreign keys and has no concept of referential integrity.

Apache Cassandra data definition supports keyspace as a top-level database object that contains tables  and other database objects such as materialized views, user-defined types, functions and aggregates. Replication is managed at the keyspace level at a given data center.  Data definition language (DDL) is provided for keyspaces and tables.

Cassandra supports the Cassandra Query Language (CQL), an SQL-like language that provides data definition syntax for all the database objects including keyspaces, tables, materialized views, indexes and snapshots. CQL statements are run from the cqlsh interface.

Apache Cassandra configuration settings are configured in cassandra.yaml file that may be edited in a vi editor. Cassandra cluster must be restarted after modifying ``cassandra.yaml``.

Cassandra supports backups using snapshots. Automatic Incremental backups may also be used.
Apache Cassandra 4.0 has added several new features including virtual tables. transient replication, audit logging, full query logging, and support for Java 11. Two of these features are experimental: transient replication and Java 11 support.

Cassandra provides tools for managing a cluster. The Nodetool is used to manage the cluster as a whole. The ``nodetool`` command line options override the configuration settings in ``cassandra.yaml``.  The ``auditlogviewer`` is used to view the audit logs. The  ``fqltool`` is used to view, replay and compare full query logs.  The ``auditlogviewer`` and ``fqltool`` are new tools in Apache Cassandra 4.0.
