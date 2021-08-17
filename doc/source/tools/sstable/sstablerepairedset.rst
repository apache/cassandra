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

sstablerepairedset
------------------

Repairs can take a very long time in some environments, for large sizes of data. Use this tool to set the repairedAt status on a given set of sstables, so that repairs can be run on only un-repaired sstables if desired.

Note that running a repair (e.g., via nodetool repair) doesn't set the status of this metadata. Only setting the status of this metadata via this tool does.

ref: https://issues.apache.org/jira/browse/CASSANDRA-5351

Cassandra must be stopped before this tool is executed, or unexpected results will occur. Note: the script does not verify that Cassandra is stopped.

Usage
^^^^^
sstablerepairedset --really-set <options> [-f <sstable-list> | <sstables>]

===================================                   ================================================================================
--really-set                                          required if you want to really set the status
--is-repaired                                         set the repairedAt status to the last modified time
--is-unrepaired                                       set the repairedAt status to 0
-f                                                    use a file containing a list of sstables as the input
===================================                   ================================================================================

Set a lot of sstables to unrepaired status
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are many ways to do this programmatically. This way would likely include variables for the keyspace and table.

Example::

    find /var/lib/cassandra/data/keyspace1/standard1-d936bd20a17c11e8bc92a55ed562cd82/* -name "*Data.db" -print0 | xargs -0 -I % sstablerepairedset --really-set --is-unrepaired %

Set one to many sstables to repaired status
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set the repairedAt status after a repair to mark the sstables as repaired. Again, using variables for the keyspace and table names is a good choice.

Example::

    nodetool repair keyspace1 standard1
    find /var/lib/cassandra/data/keyspace1/standard1-d936bd20a17c11e8bc92a55ed562cd82/* -name "*Data.db" -print0 | xargs -0 -I % sstablerepairedset --really-set --is-repaired %

Print metadata showing repaired status
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

sstablemetadata can be used to view the status set or unset using this command.

Example:

    sstablerepairedset --really-set --is-repaired /var/lib/cassandra/data/keyspace1/standard1-d936bd20a17c11e8bc92a55ed562cd82/mc-1-big-Data.db
    sstablemetadata /var/lib/cassandra/data/keyspace1/standard1-d936bd20a17c11e8bc92a55ed562cd82/mc-1-big-Data.db | grep "Repaired at"
    Repaired at: 1534443974000

    sstablerepairedset --really-set --is-unrepaired /var/lib/cassandra/data/keyspace1/standard1-d936bd20a17c11e8bc92a55ed562cd82/mc-1-big-Data.db
    sstablemetadata /var/lib/cassandra/data/keyspace1/standard1-d936bd20a17c11e8bc92a55ed562cd82/mc-1-big-Data.db | grep "Repaired at"
    Repaired at: 0

Using command in a script
^^^^^^^^^^^^^^^^^^^^^^^^^

If you know you ran repair 2 weeks ago, you can do something like the following::

    sstablerepairset --is-repaired -f <(find /var/lib/cassandra/data/.../ -iname "*Data.db*" -mtime +14)

