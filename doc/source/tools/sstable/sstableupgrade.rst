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

sstableupgrade
--------------

Upgrade the sstables in the given table (or snapshot) to the current version of Cassandra. This process is typically done after a Cassandra version upgrade. This operation will rewrite the sstables in the specified table to match the currently installed version of Cassandra. The sstableupgrade command can also be used to downgrade sstables to a previous version.

The snapshot option will only upgrade the specified snapshot. Upgrading snapshots is required before attempting to restore a snapshot taken in a major version older than the major version Cassandra is currently running. This will replace the files in the given snapshot as well as break any hard links to live sstables.

Cassandra must be stopped before this tool is executed, or unexpected results will occur. Note: the script does not verify that Cassandra is stopped.

Usage
^^^^^
sstableupgrade <options> <keyspace> <table> [snapshot_name]

===================================                   ================================================================================
--debug                                               display stack traces
-h,--help                                             display this help message
-k,--keep-source                                      do not delete the source sstables
===================================                   ================================================================================

Rewrite tables to the current Cassandra version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Start with a set of sstables in one version of Cassandra::

    ls -al /tmp/cassandra/data/keyspace1/standard1-9695b790a63211e8a6fb091830ac5256/
    ...
    -rw-r--r--   1 user  wheel      348 Aug 22 13:45 keyspace1-standard1-ka-1-CRC.db
    -rw-r--r--   1 user  wheel  5620000 Aug 22 13:45 keyspace1-standard1-ka-1-Data.db
    -rw-r--r--   1 user  wheel       10 Aug 22 13:45 keyspace1-standard1-ka-1-Digest.sha1
    -rw-r--r--   1 user  wheel    25016 Aug 22 13:45 keyspace1-standard1-ka-1-Filter.db
    -rw-r--r--   1 user  wheel   480000 Aug 22 13:45 keyspace1-standard1-ka-1-Index.db
    -rw-r--r--   1 user  wheel     9895 Aug 22 13:45 keyspace1-standard1-ka-1-Statistics.db
    -rw-r--r--   1 user  wheel     3562 Aug 22 13:45 keyspace1-standard1-ka-1-Summary.db
    -rw-r--r--   1 user  wheel       79 Aug 22 13:45 keyspace1-standard1-ka-1-TOC.txt

After upgrading the Cassandra version, upgrade the sstables::

    sstableupgrade keyspace1 standard1
    Found 1 sstables that need upgrading.
    Upgrading BigTableReader(path='/var/lib/cassandra/data/keyspace1/standard1-9695b790a63211e8a6fb091830ac5256/keyspace1-standard1-ka-1-Data.db')
    Upgrade of BigTableReader(path='/var/lib/cassandra/data/keyspace1/standard1-9695b790a63211e8a6fb091830ac5256/keyspace1-standard1-ka-1-Data.db') complete.

    ls -al /tmp/cassandra/data/keyspace1/standard1-9695b790a63211e8a6fb091830ac5256/
    ...
    drwxr-xr-x   2 user  wheel       64 Aug 22 13:48 backups
    -rw-r--r--   1 user  wheel      292 Aug 22 13:48 mc-2-big-CRC.db
    -rw-r--r--   1 user  wheel  4599475 Aug 22 13:48 mc-2-big-Data.db
    -rw-r--r--   1 user  wheel       10 Aug 22 13:48 mc-2-big-Digest.crc32
    -rw-r--r--   1 user  wheel    25256 Aug 22 13:48 mc-2-big-Filter.db
    -rw-r--r--   1 user  wheel   330807 Aug 22 13:48 mc-2-big-Index.db
    -rw-r--r--   1 user  wheel    10312 Aug 22 13:48 mc-2-big-Statistics.db
    -rw-r--r--   1 user  wheel     3506 Aug 22 13:48 mc-2-big-Summary.db
    -rw-r--r--   1 user  wheel       80 Aug 22 13:48 mc-2-big-TOC.txt

Rewrite tables to the current Cassandra version, and keep tables in old version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Again, starting with a set of sstables in one version::

    ls -al /tmp/cassandra/data/keyspace1/standard1-db532690a63411e8b4ae091830ac5256/
    ...
    -rw-r--r--   1 user  wheel      348 Aug 22 13:58 keyspace1-standard1-ka-1-CRC.db
    -rw-r--r--   1 user  wheel  5620000 Aug 22 13:58 keyspace1-standard1-ka-1-Data.db
    -rw-r--r--   1 user  wheel       10 Aug 22 13:58 keyspace1-standard1-ka-1-Digest.sha1
    -rw-r--r--   1 user  wheel    25016 Aug 22 13:58 keyspace1-standard1-ka-1-Filter.db
    -rw-r--r--   1 user  wheel   480000 Aug 22 13:58 keyspace1-standard1-ka-1-Index.db
    -rw-r--r--   1 user  wheel     9895 Aug 22 13:58 keyspace1-standard1-ka-1-Statistics.db
    -rw-r--r--   1 user  wheel     3562 Aug 22 13:58 keyspace1-standard1-ka-1-Summary.db
    -rw-r--r--   1 user  wheel       79 Aug 22 13:58 keyspace1-standard1-ka-1-TOC.txt

After upgrading the Cassandra version, upgrade the sstables, retaining the original sstables::

    sstableupgrade keyspace1 standard1 -k
    Found 1 sstables that need upgrading.
    Upgrading BigTableReader(path='/var/lib/cassandra/data/keyspace1/standard1-db532690a63411e8b4ae091830ac5256/keyspace1-standard1-ka-1-Data.db')
    Upgrade of BigTableReader(path='/var/lib/cassandra/data/keyspace1/standard1-db532690a63411e8b4ae091830ac5256/keyspace1-standard1-ka-1-Data.db') complete.

    ls -al /tmp/cassandra/data/keyspace1/standard1-db532690a63411e8b4ae091830ac5256/
    ...
    drwxr-xr-x   2 user  wheel       64 Aug 22 14:00 backups
    -rw-r--r--@  1 user  wheel      348 Aug 22 13:58 keyspace1-standard1-ka-1-CRC.db
    -rw-r--r--@  1 user  wheel  5620000 Aug 22 13:58 keyspace1-standard1-ka-1-Data.db
    -rw-r--r--@  1 user  wheel       10 Aug 22 13:58 keyspace1-standard1-ka-1-Digest.sha1
    -rw-r--r--@  1 user  wheel    25016 Aug 22 13:58 keyspace1-standard1-ka-1-Filter.db
    -rw-r--r--@  1 user  wheel   480000 Aug 22 13:58 keyspace1-standard1-ka-1-Index.db
    -rw-r--r--@  1 user  wheel     9895 Aug 22 13:58 keyspace1-standard1-ka-1-Statistics.db
    -rw-r--r--@  1 user  wheel     3562 Aug 22 13:58 keyspace1-standard1-ka-1-Summary.db
    -rw-r--r--@  1 user  wheel       79 Aug 22 13:58 keyspace1-standard1-ka-1-TOC.txt
    -rw-r--r--   1 user  wheel      292 Aug 22 14:01 mc-2-big-CRC.db
    -rw-r--r--   1 user  wheel  4596370 Aug 22 14:01 mc-2-big-Data.db
    -rw-r--r--   1 user  wheel       10 Aug 22 14:01 mc-2-big-Digest.crc32
    -rw-r--r--   1 user  wheel    25256 Aug 22 14:01 mc-2-big-Filter.db
    -rw-r--r--   1 user  wheel   330801 Aug 22 14:01 mc-2-big-Index.db
    -rw-r--r--   1 user  wheel    10312 Aug 22 14:01 mc-2-big-Statistics.db
    -rw-r--r--   1 user  wheel     3506 Aug 22 14:01 mc-2-big-Summary.db
    -rw-r--r--   1 user  wheel       80 Aug 22 14:01 mc-2-big-TOC.txt


Rewrite a snapshot to the current Cassandra version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Find the snapshot name::

    nodetool listsnapshots

    Snapshot Details:
    Snapshot name       Keyspace name                Column family name           True size          Size on disk
    ...
    1534962986979       keyspace1                    standard1                    5.85 MB            5.85 MB

Then rewrite the snapshot::

    sstableupgrade keyspace1 standard1 1534962986979
    Found 1 sstables that need upgrading.
    Upgrading BigTableReader(path='/var/lib/cassandra/data/keyspace1/standard1-5850e9f0a63711e8a5c5091830ac5256/snapshots/1534962986979/keyspace1-standard1-ka-1-Data.db')
    Upgrade of BigTableReader(path='/var/lib/cassandra/data/keyspace1/standard1-5850e9f0a63711e8a5c5091830ac5256/snapshots/1534962986979/keyspace1-standard1-ka-1-Data.db') complete.





