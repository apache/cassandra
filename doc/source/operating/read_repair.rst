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

Read repair
-----------

Read repair improves consistency in a Cassandra cluster with every read request. 

In a read, the coordinator node send a data request to one replica node and digest requests to other nodes for consistency level greater than one. If all nodes returns consistent data, coordinator returns it to the client.

In read repair, cassandra sends digests request to all replica nodes not involved directly in read. Cassandra compares data received from all the replicas and writes the most recent version to out-of-date replicas. 

Types of read repair
--------------------
.. todo:: todo

* Foreground

* Background
