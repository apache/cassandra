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

sstableofflinerelevel
---------------------

When using LeveledCompactionStrategy, sstables can get stuck at L0 on a recently bootstrapped node, and compactions may never catch up. This tool is used to bump sstables into the highest level possible.

ref: https://issues.apache.org/jira/browse/CASSANDRA-8301

The way this is done is: sstables are storted by their last token. Given an original leveling like this (note that [ ] indicates token boundaries, not sstable size on disk; all sstables are the same size)::

    L3 [][][][][][][][][][][]
    L2 [    ][    ][    ][  ]
    L1 [          ][        ]
    L0 [                    ]

Will look like this after being dropped to L0 and sorted by last token (and, to illustrate overlap, the overlapping ones are put on a new line)::

    [][][]
    [    ][][][]
        [    ]
    [          ]
    ...

Then, we start iterating from the smallest last-token and adding all sstables that do not cause an overlap to a level. We will reconstruct the original leveling top-down. Whenever we add an sstable to the level, we remove it from the sorted list. Once we reach the end of the sorted list, we have a full level, and can start over with the level below.

If we end up with more levels than expected, we put all levels exceeding the expected in L0, for example, original L0 files will most likely be put in a level of its own since they most often overlap many other sstables.

Cassandra must be stopped before this tool is executed, or unexpected results will occur. Note: the script does not verify that Cassandra is stopped.

Usage
^^^^^

sstableofflinerelevel [--dry-run] <keyspace> <table>

Doing a dry run
^^^^^^^^^^^^^^^

Use the --dry-run option to see the current level distribution and predicted level after the change.

Example::

    sstableofflinerelevel --dry-run keyspace eventlog
    For sstables in /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753:
    Current leveling:
    L0=2
    Potential leveling:
    L0=1
    L1=1

Running a relevel
^^^^^^^^^^^^^^^^^

Example::

    sstableofflinerelevel keyspace eventlog
    For sstables in /var/lib/cassandra/data/keyspace/eventlog-6365332094dd11e88f324f9c503e4753:
    Current leveling:
    L0=2
    New leveling:
    L0=1
    L1=1

Keyspace or table not found
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If an invalid keyspace and/or table is provided, an exception will be thrown.

Example::

    sstableofflinerelevel --dry-run keyspace evenlog

    Exception in thread "main" java.lang.IllegalArgumentException: Unknown keyspace/columnFamily keyspace1.evenlog
        at org.apache.cassandra.tools.SSTableOfflineRelevel.main(SSTableOfflineRelevel.java:96)







