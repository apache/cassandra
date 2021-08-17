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

sstableutil
-----------

List sstable files for the provided table.

ref: https://issues.apache.org/jira/browse/CASSANDRA-7066

Cassandra must be stopped before this tool is executed, or unexpected results will occur. Note: the script does not verify that Cassandra is stopped.

Usage
^^^^^
sstableutil <options> <keyspace> <table>

===================================                   ================================================================================
-c, --cleanup                                         clean up any outstanding transactions
-d, --debug                                           display stack traces
-h, --help                                            display this help message
-o, --oplog                                           include operation logs
-t, --type <arg>                                      all (list all files, final or temporary), tmp (list temporary files only), 
                                                      final (list final files only),
-v, --verbose                                         verbose output
===================================                   ================================================================================

List all sstables
^^^^^^^^^^^^^^^^^

The basic command lists the sstables associated with a given keyspace/table.

Example::

    sstableutil keyspace eventlog
    Listing files...
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-32-big-CRC.db
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-32-big-Data.db
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-32-big-Digest.crc32
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-32-big-Filter.db
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-32-big-Index.db
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-32-big-Statistics.db
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-32-big-Summary.db
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-32-big-TOC.txt
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-37-big-CRC.db
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-37-big-Data.db
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-37-big-Digest.crc32
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-37-big-Filter.db
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-37-big-Index.db
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-37-big-Statistics.db
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-37-big-Summary.db
    /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-37-big-TOC.txt

List only temporary sstables 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Using the -t option followed by `tmp` will list all temporary sstables, in the format above. Temporary sstables were used in pre-3.0 versions of Cassandra.

List only final sstables
^^^^^^^^^^^^^^^^^^^^^^^^

Using the -t option followed by `final` will list all final sstables, in the format above. In recent versions of Cassandra, this is the same output as not using the -t option.

Include transaction logs
^^^^^^^^^^^^^^^^^^^^^^^^

Using the -o option will include transaction logs in the listing, in the format above.

Clean up sstables
^^^^^^^^^^^^^^^^^

Using the -c option removes any transactions left over from incomplete writes or compactions.

From the 3.0 upgrade notes:

New transaction log files have been introduced to replace the compactions_in_progress system table, temporary file markers (tmp and tmplink) and sstable ancestors. Therefore, compaction metadata no longer contains ancestors. Transaction log files list sstable descriptors involved in compactions and other operations such as flushing and streaming. Use the sstableutil tool to list any sstable files currently involved in operations not yet completed, which previously would have been marked as temporary. A transaction log file contains one sstable per line, with the prefix "add:" or "remove:". They also contain a special line "commit", only inserted at the end when the transaction is committed. On startup we use these files to cleanup any partial transactions that were in progress when the process exited. If the commit line is found, we keep new sstables (those with the "add" prefix) and delete the old sstables (those with the "remove" prefix), vice-versa if the commit line is missing. Should you lose or delete these log files, both old and new sstable files will be kept as live files, which will result in duplicated sstables. These files are protected by incremental checksums so you should not manually edit them. When restoring a full backup or moving sstable files, you should clean-up any left over transactions and their temporary files first. 



