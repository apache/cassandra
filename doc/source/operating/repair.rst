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

.. _repair:

Repair
------

Cassandra is designed to remain available if one of it's nodes is down or unreachable. However, when a node is down or
unreachable, it needs to eventually discover the writes it missed. Hints attempt to inform a node of missed writes, but
are a best effort, and aren't guaranteed to inform a node of 100% of the writes it missed. These inconsistencies can
eventually result in data loss as nodes are replaced or tombstones expire.

These inconsistencies are fixed with the repair process. Repair synchronizes the data between nodes by comparing their
respective datasets for their common token ranges, and streaming the differences for any out of sync sections between
the nodes. It compares the data with merkle trees, which are a hierarchy of hashes.

Incremental and Full Repairs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are 2 types of repairs: full repairs, and incremental repairs. Full repairs operate over all of the data in the
token range being repaired. Incremental repairs only repair data that's been written since the previous incremental repair.

Incremental repairs are the default repair type, and if run regularly, can significantly reduce the time and io cost of
performing a repair. However, it's important to understand that once an incremental repair marks data as repaired, it won't
try to repair it again. This is fine for syncing up missed writes, but it doesn't protect against things like disk corruption,
data loss by operator error, or bugs in Cassandra. For this reason, full repairs should still be run occasionally.

Usage and Best Practices
^^^^^^^^^^^^^^^^^^^^^^^^

Since repair can result in a lot of disk and network io, it's not run automatically by Cassandra. It is run by the operator
via nodetool.

Incremental repair is the default and is run with the following command:

::

    nodetool repair

A full repair can be run with the following command:

::

    nodetool repair --full

Additionally, repair can be run on a single keyspace:

::

    nodetool repair [options] <keyspace_name>

Or even on specific tables:

::

    nodetool repair [options] <keyspace_name> <table1> <table2>


The repair command only repairs token ranges on the node being repaired, it doesn't repair the whole cluster. By default, repair
will operate on all token ranges replicated by the node you're running repair on, which will cause duplicate work if you run it
on every node. The ``-pr`` flag will only repair the "primary" ranges on a node, so you can repair your entire cluster by running
``nodetool repair -pr`` on each node in a single datacenter.

The specific frequency of repair that's right for your cluster, of course, depends on several factors. However, if you're
just starting out and looking for somewhere to start, running an incremental repair every 1-3 days, and a full repair every
1-3 weeks is probably reasonable. If you don't want to run incremental repairs, a full repair every 5 days is a good place
to start.

At a minimum, repair should be run often enough that the gc grace period never expires on unrepaired data. Otherwise, deleted
data could reappear. With a default gc grace period of 10 days, repairing every node in your cluster at least once every 7 days
will prevent this, while providing enough slack to allow for delays.

Other Options
^^^^^^^^^^^^^

``-pr, --partitioner-range``
    Restricts repair to the 'primary' token ranges of the node being repaired. A primary range is just a token range for
    which a node is the first replica in the ring.

``-prv, --preview``
    Estimates the amount of streaming that would occur for the given repair command. This builds the merkle trees, and prints
    the expected streaming activity, but does not actually do any streaming. By default, incremental repairs are estimated,
    add the ``--full`` flag to estimate a full repair.

``-vd, --validate``
    Verifies that the repaired data is the same across all nodes. Similiar to ``--preview``, this builds and compares merkle
    trees of repaired data, but doesn't do any streaming. This is useful for troubleshooting. If this shows that the repaired
    data is out of sync, a full repair should be run.

.. seealso::
    :ref:`nodetool repair docs <nodetool_repair>`

Full Repair Example
^^^^^^^^^^^^^^^^^^^^
Full repair is typically needed to redistribute data after increasing the replication factor of a keyspace or after adding a node to the cluster. Full repair involves streaming SSTables. To demonstrate full repair start with a three node cluster.

::

 [ec2-user@ip-10-0-2-238 ~]$ nodetool status
 Datacenter: us-east-1
 =====================
 Status=Up/Down
 |/ State=Normal/Leaving/Joining/Moving
 --  Address   Load        Tokens  Owns  Host ID                              Rack
 UN  10.0.1.115  547 KiB     256    ?  b64cb32a-b32a-46b4-9eeb-e123fa8fc287  us-east-1b
 UN  10.0.3.206  617.91 KiB  256    ?  74863177-684b-45f4-99f7-d1006625dc9e  us-east-1d
 UN  10.0.2.238  670.26 KiB  256    ?  4dcdadd2-41f9-4f34-9892-1f20868b27c7  us-east-1c

Create a keyspace with replication factor 3:

::

 cqlsh> DROP KEYSPACE cqlkeyspace;
 cqlsh> CREATE KEYSPACE CQLKeyspace
   ... WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

Add a table to the keyspace:

::

 cqlsh> use cqlkeyspace;
 cqlsh:cqlkeyspace> CREATE TABLE t (
            ...   id int,
            ...   k int,
            ...   v text,
            ...   PRIMARY KEY (id)
            ... );

Add table data:

::

 cqlsh:cqlkeyspace> INSERT INTO t (id, k, v) VALUES (0, 0, 'val0');
 cqlsh:cqlkeyspace> INSERT INTO t (id, k, v) VALUES (1, 1, 'val1');
 cqlsh:cqlkeyspace> INSERT INTO t (id, k, v) VALUES (2, 2, 'val2');

A query lists the data added:

::

 cqlsh:cqlkeyspace> SELECT * FROM t;

 id | k | v
 ----+---+------
  1 | 1 | val1
  0 | 0 | val0
  2 | 2 | val2
 (3 rows)

Make the following changes to a three node cluster:

1.       Increase the replication factor from 3 to 4.
2.       Add a 4th node to the cluster

When the replication factor is increased the following message gets output indicating that a full repair is needed as per (`CASSANDRA-13079
<https://issues.apache.org/jira/browse/CASSANDRA-13079>`_):

::

 cqlsh:cqlkeyspace> ALTER KEYSPACE CQLKeyspace
            ... WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 4};
 Warnings :
 When increasing replication factor you need to run a full (-full) repair to distribute the
 data.

Perform a full repair on the keyspace ``cqlkeyspace`` table ``t`` with following command:

::

 nodetool repair -full cqlkeyspace t

Full repair completes in about a second as indicated by the output:

::

[ec2-user@ip-10-0-2-238 ~]$ nodetool repair -full cqlkeyspace t
[2019-08-17 03:06:21,445] Starting repair command #1 (fd576da0-c09b-11e9-b00c-1520e8c38f00), repairing keyspace cqlkeyspace with repair options (parallelism: parallel, primary range: false, incremental: false, job threads: 1, ColumnFamilies: [t], dataCenters: [], hosts: [], previewKind: NONE, # of ranges: 1024, pull repair: false, force repair: false, optimise streams: false)
[2019-08-17 03:06:23,059] Repair session fd8e5c20-c09b-11e9-b00c-1520e8c38f00 for range [(-8792657144775336505,-8786320730900698730], (-5454146041421260303,-5439402053041523135], (4288357893651763201,4324309707046452322], ... , (4350676211955643098,4351706629422088296]] finished (progress: 0%)
[2019-08-17 03:06:23,077] Repair completed successfully
[2019-08-17 03:06:23,077] Repair command #1 finished in 1 second
[ec2-user@ip-10-0-2-238 ~]$

The ``nodetool  tpstats`` command should list a repair having been completed as ``Repair-Task`` > ``Completed`` column value of 1:

::

 [ec2-user@ip-10-0-2-238 ~]$ nodetool tpstats
 Pool Name Active   Pending Completed   Blocked  All time blocked
 ReadStage  0           0           99       0              0
 …
 Repair-Task 0       0           1        0              0
 RequestResponseStage                  0        0        2078        0               0
