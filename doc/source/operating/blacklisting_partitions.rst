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


Blacklisting Partitions (CASSANDRA-12106)
-----------------------------------------

This feature allows partition keys to be blacklisted i.e. Cassandra would not process following operations on those
blacklisted partitions.

- Point READs

Response would be InvalidQueryException.

It is important to note that, this would not have any effect on range reads or write operations.

How to blacklist a partition key
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
``system_distributed.blacklisted_partitions`` table can be used to blacklist partitions. Essentially, you will have to
insert a new row into the table with the following details:

- Keyspace name
- Table name
- Partition key

The way partition key needs to be mentioned is exactly similar to how ``nodetool getendpoints`` works.
Following are several examples for blacklisting partition keys in keyspace, say ks, and table, say table1 for different
possibilities of table1 partition key:
- For a single column partition key, say Id,
  Id is a simple type - insert into system_distributed.blacklisted_partitions (keyspace_name ,columnfamily_name ,partition_key) VALUES ('ks','table1','1');
  Id is a blob        - insert into system_distributed.blacklisted_partitions (keyspace_name ,columnfamily_name ,partition_key) VALUES ('ks','table1','12345f');
  Id has a colon      - insert into system_distributed.blacklisted_partitions (keyspace_name ,columnfamily_name ,partition_key) VALUES ('ks','table1','1\:2');

- For a composite column partition key, say (Key1, Key2),
  insert into system_distributed.blacklisted_partitions (keyspace_name ,columnfamily_name, partition_key) VALUES ('ks','table1','k11:k21');

BlacklistedPartitions Cache
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Cassandra internally maintains an on-heap cache that loads partition keys from ``system_distributed.blacklisted_partitions``.
Any partition key mentioned against a non-existent keyspace name and table name will not be cached.

Cache gets refreshed in following ways
- During Cassandra start up
- Scheduled cache refresh at a default interval of 10 minutes (can be overridden using ``blacklisted_partitions_cache_refresh_period_in_sec`` yaml property)
- Using nodetool refreshblacklistedpartitionscache

There is no bound on how much on-heap memory this cache can occupy - so be cautious on how many keys you would want to blacklist.
``blacklisted_partitions_cache_size_warn_threshold_in_mb`` yaml property can be used to be notified (via warning logs) if cache size exceeds the set threshold.