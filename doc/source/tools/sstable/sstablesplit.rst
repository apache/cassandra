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

sstablesplit
------------

Big sstable files can take up a lot of disk space. The sstablesplit tool can be used to split those large files into smaller files. It can be thought of as a type of anticompaction. 

ref: https://issues.apache.org/jira/browse/CASSANDRA-4766

Cassandra must be stopped before this tool is executed, or unexpected results will occur. Note: the script does not verify that Cassandra is stopped.

Usage
^^^^^
sstablesplit <options> <filename>

===================================                   ================================================================================
--debug                                               display stack traces
-h, --help                                            display this help message
--no-snapshot                                         don't snapshot the sstables before splitting
-s, --size <size>                                     maximum size in MB for the output sstables (default: 50)
===================================                   ================================================================================

This command should be run with Cassandra stopped. Note: the script does not verify that Cassandra is stopped.

Split a File
^^^^^^^^^^^^

Split a large sstable into smaller sstables. By default, unless the option --no-snapshot is added, a snapshot will be done of the original sstable and placed in the snapshots folder.

Example::

    sstablesplit /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-8-big-Data.db
    
    Pre-split sstables snapshotted into snapshot pre-split-1533144514795

Split Multiple Files
^^^^^^^^^^^^^^^^^^^^

Wildcards can be used in the filename portion of the command to split multiple files.

Example::

    sstablesplit --size 1 /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-1*

Attempt to Split a Small File
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the file is already smaller than the split size provided, the sstable will not be split.

Example::

    sstablesplit /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-8-big-Data.db
    Skipping /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-8-big-Data.db: it's size (1.442 MB) is less than the split size (50 MB)
    No sstables needed splitting.

Split a File into Specified Size
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The default size used for splitting is 50MB. Specify another size with the --size option. The size is in megabytes (MB). Specify only the number, not the units. For example --size 50 is correct, but --size 50MB is not.

Example::

    sstablesplit --size 1 /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-9-big-Data.db
    Pre-split sstables snapshotted into snapshot pre-split-1533144996008


Split Without Snapshot
^^^^^^^^^^^^^^^^^^^^^^

By default, sstablesplit will create a snapshot before splitting. If a snapshot is not needed, use the --no-snapshot option to skip it.

Example::

    sstablesplit --size 1 --no-snapshot /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753/mc-11-big-Data.db

Note: There is no output, but you can see the results in your file system.



