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

sstableexpiredblockers
----------------------

During compaction, entire sstables can be dropped if they contain only expired tombstones, and if it is guaranteed that the data is not newer than the data in other sstables. An expired sstable can be blocked from getting dropped if its newest timestamp is newer than the oldest data in another sstable.

This tool is used to list all sstables that are blocking other sstables from getting dropped (by having older data than the newest tombstone in an expired sstable) so a user can figure out why certain sstables are still on disk.

ref: https://issues.apache.org/jira/browse/CASSANDRA-10015

Cassandra must be stopped before this tool is executed, or unexpected results will occur. Note: the script does not verify that Cassandra is stopped.

Usage
^^^^^

sstableexpiredblockers <keyspace> <table>

Output blocked sstables
^^^^^^^^^^^^^^^^^^^^^^^

If the sstables exist for the table, but no tables have older data than the newest tombstone in an expired sstable, the script will return nothing.

Otherwise, the script will return `<sstable> blocks <#> expired sstables from getting dropped` followed by a list of the blocked sstables.

Example::

    sstableexpiredblockers keyspace1 standard1

    [BigTableReader(path='/var/lib/cassandra/data/keyspace1/standard1-0665ae80b2d711e886c66d2c86545d91/mc-2-big-Data.db') (minTS = 5, maxTS = 5, maxLDT = 2147483647)],  blocks 1 expired sstables from getting dropped: [BigTableReader(path='/var/lib/cassandra/data/keyspace1/standard1-0665ae80b2d711e886c66d2c86545d91/mc-3-big-Data.db') (minTS = 1536349775157606, maxTS = 1536349780311159, maxLDT = 1536349780)],

    [BigTableReader(path='/var/lib/cassandra/data/keyspace1/standard1-0665ae80b2d711e886c66d2c86545d91/mc-1-big-Data.db') (minTS = 1, maxTS = 10, maxLDT = 2147483647)],  blocks 1 expired sstables from getting dropped: [BigTableReader(path='/var/lib/cassandra/data/keyspace1/standard1-0665ae80b2d711e886c66d2c86545d91/mc-3-big-Data.db') (minTS = 1536349775157606, maxTS = 1536349780311159, maxLDT = 1536349780)],


