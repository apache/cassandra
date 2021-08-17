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

sstablelevelreset
-----------------

If LeveledCompactionStrategy is set, this script can be used to reset level to 0 on a given set of sstables. This is useful if you want to, for example, change the minimum sstable size, and therefore restart the compaction process using this new configuration.

See http://cassandra.apache.org/doc/latest/operating/compaction.html#leveled-compaction-strategy for information on how levels are used in this compaction strategy.

Cassandra must be stopped before this tool is executed, or unexpected results will occur. Note: the script does not verify that Cassandra is stopped.

ref: https://issues.apache.org/jira/browse/CASSANDRA-5271

Usage
^^^^^

sstablelevelreset --really-reset <keyspace> <table>

The really-reset flag is required, to ensure this intrusive command is not run accidentally.

Table not found
^^^^^^^^^^^^^^^

If the keyspace and/or table is not in the schema (e.g., if you misspelled the table name), the script will return an error.

Example:: 

    ColumnFamily not found: keyspace/evenlog.

Table has no sstables
^^^^^^^^^^^^^^^^^^^^^

Example::

    Found no sstables, did you give the correct keyspace/table?


Table already at level 0
^^^^^^^^^^^^^^^^^^^^^^^^

The script will not set the level if it is already set to 0.

Example::

    Skipped /var/lib/cassandra/data/keyspace/eventlog-65c429e08c5a11e8939edf4f403979ef/mc-1-big-Data.db since it is already on level 0

Table levels reduced to 0
^^^^^^^^^^^^^^^^^^^^^^^^^

If the level is not already 0, then this will reset it to 0.

Example::

    sstablemetadata /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-8-big-Data.db | grep -i level
    SSTable Level: 1

    sstablelevelreset --really-reset keyspace eventlog
    Changing level from 1 to 0 on /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-8-big-Data.db

    sstablemetadata /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-8-big-Data.db | grep -i level
    SSTable Level: 0







