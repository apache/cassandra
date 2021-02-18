<!---
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Storage-Attached Indexing

## Overview
Storage-attached indexes are a new column-based secondary indexing apparatus for DSE.

The project was inspired by OSS SASI (SSTable-Attached Secondary Indexes) and retains some of its high-level
architectural character (and even some actual code), but makes significant improvements in a number of areas:

- The on-disk/SSTable index formats for both string and numeric data have been completely replaced. Strings are indexed
  on disk using our proprietary on-disk byte-ordered trie data structure, while numeric types are indexed using Lucene's
  balanced kd-tree.
- While indexes continue to be managed at the column level from the user's perspective, the storage design at the column
  index level is row-based, with related offset and token information stored only once at the SSTable level. This
  drastically reduces our on-disk footprint when several columns are indexed on the same table.
- The query path is synchronous and index searches run on IO threads.
- Tracing, metrics, virtual table-based metadata, RLAC, and snapshot-based backup/restore are supported out of the box.

Many similarities with standard secondary indexes remain:

- The full set of C* consistency levels is supported for both reads and writes.
- Index updates are synchronous with mutations and do not require any kind of read-before-write.
- Queries are implemented on the back of C* range reads.
- Paging is supported.
- Only token ordering of results is supported.
- Index builds are visible to operators as compactions and are executed on compaction threads.
- All DML and DDL statements are CQL-based.
- Single-node management operations are available via nodetool. (ex. stop & rebuild_index)

## Quick Start

The following short tutorial will get you up-and-running with storage-attached indexing.

### Build and Start DSE

1.) Make sure you've created the following directories and given yourself permissions on them:

`/var/log/cassandra`

`/var/lib/cassandra`

2.) From the bdp root directory, run the following commands:

`./gradlew jar`

`bin/dse cassandra`

3.) When the node stabilizes, open up `cqlsh` from the bdp root directory.

`bin/cqlsh`

### Create a Simple Data Model

1.) Run the following DDL statements to create a table and two indexes:

`CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy' , 'Cassandra': '1'};`

`USE test;`

`CREATE TABLE person (id int, name text, age int, PRIMARY KEY (id));`

`CREATE CUSTOM INDEX ON person (name) USING 'StorageAttachedIndex' WITH OPTIONS = {'case_sensitive': false};`

`CREATE CUSTOM INDEX ON person (age) USING 'StorageAttachedIndex';`

2.) Add some data.

`INSERT INTO person (id, name, age) VALUES (1, 'John', 21);`

`INSERT INTO person (id, name, age) VALUES (2, 'john', 50);`

`INSERT INTO person (id, name, age) VALUES (3, 'Boris', 43);`

`INSERT INTO person (id, name, age) VALUES (4, 'Caleb', 34);`

### Make Some Queries

1.) Query for everyone named "John", ignoring case.

`SELECT * FROM person WHERE name = 'John';`

```
 id | age | name
----+-----+------
  1 |  21 | John
  2 |  50 | john
```

2.) Query for everyone between the ages of 18 and 25.

`SELECT * FROM person WHERE age >= 18 AND age <= 35;`

```
 id | age | name
----+-----+-------
  1 |  21 |  John
  4 |  34 | Caleb
```

## Contributors

- Marc Selwan
- Caleb Rackliffe
- Zhao Yang
- Jason Rutherglen
- Maciej Zasada
- Andrew de la Peña
- Mike Adamson
- Zahir Patni
- Tomek Lasica
- Berenguer Blasi
- Rocco Varela
