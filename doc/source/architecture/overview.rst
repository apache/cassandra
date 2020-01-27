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
========

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

Cassandra was designed as a best in class combination of both systems
to meet the emerging large scale (PiB+) storage requirements web based
applications demand. As applications began to require full global replication
and always available low-latency reads and writes, it became imperative to
design a new kind of database model as the relational database systems of the
time struggled to meet the new requirements of global scale applications.

Systems like Cassandra are designed for these challenges and seek the
following design objectives:

- Full multi-master database replication
- Global availability at low latency
- Scaling out on commodity hardware rather than scaling up
- Linear throughput increase with each additional processor
- Partitiond key-oriented queries
- Flexible schema
- Online load balancing and cluster growth
