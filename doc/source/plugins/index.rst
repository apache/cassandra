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

Third-Party Plugins
===================

Available third-party plugins for Apache Cassandra

CAPI-Rowcache
-------------

The Coherent Accelerator Process Interface (CAPI) is a general term for the infrastructure of attaching a Coherent accelerator to an IBM POWER system. A key innovation in IBM POWER8’s open architecture is the CAPI. It provides a high bandwidth, low latency path between external devices, the POWER8 core, and the system’s open memory architecture. IBM Data Engine for NoSQL is an integrated platform for large and fast growing NoSQL data stores. It builds on the CAPI capability of POWER8 systems and provides super-fast access to large flash storage capacity and addresses the challenges associated with typical x86 server based scale-out deployments.

The official page for the `CAPI-Rowcache plugin <https://github.com/ppc64le/capi-rowcache>`__ contains further details how to build/run/download the plugin.


Stratio’s Cassandra Lucene Index
--------------------------------

Stratio’s Lucene index is a Cassandra secondary index implementation based on `Apache Lucene <http://lucene.apache.org/>`__. It extends Cassandra’s functionality to provide near real-time distributed search engine capabilities such as with ElasticSearch or `Apache Solr <http://lucene.apache.org/solr/>`__, including full text search capabilities, free multivariable, geospatial and bitemporal search, relevance queries and sorting based on column value, relevance or distance. Each node indexes its own data, so high availability and scalability is guaranteed.

The official Github repository `Cassandra Lucene Index <http://www.github.com/stratio/cassandra-lucene-index>`__ contains everything you need to build/run/configure the plugin.