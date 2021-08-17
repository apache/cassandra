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

sstableverify
-------------

Check sstable(s) for errors or corruption, for the provided table.

ref: https://issues.apache.org/jira/browse/CASSANDRA-5791

Cassandra must be stopped before this tool is executed, or unexpected results will occur. Note: the script does not verify that Cassandra is stopped.

Usage
^^^^^
sstableverify <options> <keyspace> <table>

===================================                   ================================================================================
--debug                                               display stack traces
-e, --extended                                        extended verification
-h, --help                                            display this help message
-v, --verbose                                         verbose output
===================================                   ================================================================================

Basic Verification
^^^^^^^^^^^^^^^^^^

This is the basic verification. It is not a very quick process, and uses memory. You might need to increase your memory settings if you have many sstables.

Example::

    sstableverify keyspace eventlog
    Verifying BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-32-big-Data.db') (7.353MiB)
    Deserializing sstable metadata for BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-32-big-Data.db')
    Checking computed hash of BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-32-big-Data.db')
    Verifying BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-37-big-Data.db') (3.775MiB)
    Deserializing sstable metadata for BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-37-big-Data.db')
    Checking computed hash of BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-37-big-Data.db')

Extended Verification
^^^^^^^^^^^^^^^^^^^^^

During an extended verification, the individual values will be validated for errors or corruption. This of course takes more time.

Example::

    root@DC1C1:/# sstableverify -e keyspace eventlog
    WARN  14:08:06,255 Only 33.096GiB free across all data volumes. Consider adding more capacity to your cluster or removing obsolete snapshots
    Verifying BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-32-big-Data.db') (7.353MiB)
    Deserializing sstable metadata for BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-32-big-Data.db')
    Checking computed hash of BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-32-big-Data.db')
    Extended Verify requested, proceeding to inspect values
    Verify of BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-32-big-Data.db') succeeded. All 33211 rows read successfully
    Verifying BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-37-big-Data.db') (3.775MiB)
    Deserializing sstable metadata for BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-37-big-Data.db')
    Checking computed hash of BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-37-big-Data.db')
    Extended Verify requested, proceeding to inspect values
    Verify of BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-37-big-Data.db') succeeded. All 17068 rows read successfully

Corrupted File
^^^^^^^^^^^^^^

Corrupted files are listed if they are detected by the script.

Example::

    sstableverify keyspace eventlog
    Verifying BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-40-big-Data.db') (7.416MiB)
    Deserializing sstable metadata for BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-40-big-Data.db')
    Checking computed hash of BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-40-big-Data.db')
    Error verifying BigTableReader(path='/var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-40-big-Data.db'): Corrupted: /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-40-big-Data.db

A similar (but less verbose) tool will show the suggested actions::

    nodetool verify keyspace eventlog
    error: Invalid SSTable /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-40-big-Data.db, please force repair



