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
