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

.. _compaction:

Compaction
----------

Strategies
^^^^^^^^^^

Picking the right compaction strategy for your workload will ensure the best performance for both querying and for compaction itself.

:ref:`Size Tiered Compaction Strategy <stcs>`
    The default compaction strategy.  Useful as a fallback when other strategies don't fit the workload.  Most useful for
    non pure time series workloads with spinning disks, or when the I/O from :ref:`LCS <lcs>` is too high.


:ref:`Leveled Compaction Strategy <lcs>`
    Leveled Compaction Strategy (LCS) is optimized for read heavy workloads, or workloads with lots of updates and deletes.  It is not a good choice for immutable time series data.


:ref:`Time Window Compaction Strategy <twcs>`
    Time Window Compaction Strategy is designed for TTL'ed, mostly immutable time series data.



Types of compaction
^^^^^^^^^^^^^^^^^^^

The concept of compaction is used for different kinds of operations in Cassandra, the common thing about these
operations is that it takes one or more sstables and output new sstables. The types of compactions are;

Minor compaction
    triggered automatically in Cassandra.
Major compaction
    a user executes a compaction over all sstables on the node.
User defined compaction
    a user triggers a compaction on a given set of sstables.
Scrub
    try to fix any broken sstables. This can actually remove valid data if that data is corrupted, if that happens you
    will need to run a full repair on the node.
Upgradesstables
    upgrade sstables to the latest version. Run this after upgrading to a new major version.
Cleanup
    remove any ranges this node does not own anymore, typically triggered on neighbouring nodes after a node has been
    bootstrapped since that node will take ownership of some ranges from those nodes.
Secondary index rebuild
    rebuild the secondary indexes on the node.
Anticompaction
    after repair the ranges that were actually repaired are split out of the sstables that existed when repair started.
Sub range compaction
    It is possible to only compact a given sub range - this could be useful if you know a token that has been
    misbehaving - either gathering many updates or many deletes. (``nodetool compact -st x -et y``) will pick
    all sstables containing the range between x and y and issue a compaction for those sstables. For STCS this will
    most likely include all sstables but with LCS it can issue the compaction for a subset of the sstables. With LCS
    the resulting sstable will end up in L0.

When is a minor compaction triggered?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#  When an sstable is added to the node through flushing/streaming etc.
#  When autocompaction is enabled after being disabled (``nodetool enableautocompaction``)
#  When compaction adds new sstables.
#  A check for new minor compactions every 5 minutes.

Merging sstables
^^^^^^^^^^^^^^^^

Compaction is about merging sstables, since partitions in sstables are sorted based on the hash of the partition key it
is possible to efficiently merge separate sstables. Content of each partition is also sorted so each partition can be
merged efficiently.

Tombstones and Garbage Collection (GC) Grace
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Why Tombstones
~~~~~~~~~~~~~~

When a delete request is received by Cassandra it does not actually remove the data from the underlying store. Instead
it writes a special piece of data known as a tombstone. The Tombstone represents the delete and causes all values which
occurred before the tombstone to not appear in queries to the database. This approach is used instead of removing values
because of the distributed nature of Cassandra.

Deletes without tombstones
~~~~~~~~~~~~~~~~~~~~~~~~~~

Imagine a three node cluster which has the value [A] replicated to every node.::

    [A], [A], [A]

If one of the nodes fails and and our delete operation only removes existing values we can end up with a cluster that
looks like::

    [], [], [A]

Then a repair operation would replace the value of [A] back onto the two
nodes which are missing the value.::

    [A], [A], [A]

This would cause our data to be resurrected even though it had been
deleted.

Deletes with Tombstones
~~~~~~~~~~~~~~~~~~~~~~~

Starting again with a three node cluster which has the value [A] replicated to every node.::

    [A], [A], [A]

If instead of removing data we add a tombstone record, our single node failure situation will look like this.::

    [A, Tombstone[A]], [A, Tombstone[A]], [A]

Now when we issue a repair the Tombstone will be copied to the replica, rather than the deleted data being
resurrected.::

    [A, Tombstone[A]], [A, Tombstone[A]], [A, Tombstone[A]]

Our repair operation will correctly put the state of the system to what we expect with the record [A] marked as deleted
on all nodes. This does mean we will end up accruing Tombstones which will permanently accumulate disk space. To avoid
keeping tombstones forever we have a parameter known as ``gc_grace_seconds`` for every table in Cassandra.

The gc_grace_seconds parameter and Tombstone Removal
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The table level ``gc_grace_seconds`` parameter controls how long Cassandra will retain tombstones through compaction
events before finally removing them. This duration should directly reflect the amount of time a user expects to allow
before recovering a failed node. After ``gc_grace_seconds`` has expired the tombstone may be removed (meaning there will
no longer be any record that a certain piece of data was deleted), but as a tombstone can live in one sstable and the
data it covers in another, a compaction must also include both sstable for a tombstone to be removed. More precisely, to
be able to drop an actual tombstone the following needs to be true;

- The tombstone must be older than ``gc_grace_seconds``
- If partition X contains the tombstone, the sstable containing the partition plus all sstables containing data older
  than the tombstone containing X must be included in the same compaction. We don't need to care if the partition is in
  an sstable if we can guarantee that all data in that sstable is newer than the tombstone. If the tombstone is older
  than the data it cannot shadow that data.
- If the option ``only_purge_repaired_tombstones`` is enabled, tombstones are only removed if the data has also been
  repaired.

If a node remains down or disconnected for longer than ``gc_grace_seconds`` it's deleted data will be repaired back to
the other nodes and re-appear in the cluster. This is basically the same as in the "Deletes without Tombstones" section.
Note that tombstones will not be removed until a compaction event even if ``gc_grace_seconds`` has elapsed.

The default value for ``gc_grace_seconds`` is 864000 which is equivalent to 10 days. This can be set when creating or
altering a table using ``WITH gc_grace_seconds``.

TTL
^^^

Data in Cassandra can have an additional property called time to live - this is used to automatically drop data that has
expired once the time is reached. Once the TTL has expired the data is converted to a tombstone which stays around for
at least ``gc_grace_seconds``. Note that if you mix data with TTL and data without TTL (or just different length of the
TTL) Cassandra will have a hard time dropping the tombstones created since the partition might span many sstables and
not all are compacted at once.

Fully expired sstables
^^^^^^^^^^^^^^^^^^^^^^

If an sstable contains only tombstones and it is guaranteed that that sstable is not shadowing data in any other sstable
compaction can drop that sstable. If you see sstables with only tombstones (note that TTL:ed data is considered
tombstones once the time to live has expired) but it is not being dropped by compaction, it is likely that other
sstables contain older data. There is a tool called ``sstableexpiredblockers`` that will list which sstables are
droppable and which are blocking them from being dropped. This is especially useful for time series compaction with
``TimeWindowCompactionStrategy`` (and the deprecated ``DateTieredCompactionStrategy``). With ``TimeWindowCompactionStrategy``
it is possible to remove the guarantee (not check for shadowing data) by enabling ``unsafe_aggressive_sstable_expiration``.

Repaired/unrepaired data
^^^^^^^^^^^^^^^^^^^^^^^^

With incremental repairs Cassandra must keep track of what data is repaired and what data is unrepaired. With
anticompaction repaired data is split out into repaired and unrepaired sstables. To avoid mixing up the data again
separate compaction strategy instances are run on the two sets of data, each instance only knowing about either the
repaired or the unrepaired sstables. This means that if you only run incremental repair once and then never again, you
might have very old data in the repaired sstables that block compaction from dropping tombstones in the unrepaired
(probably newer) sstables.

Data directories
^^^^^^^^^^^^^^^^

Since tombstones and data can live in different sstables it is important to realize that losing an sstable might lead to
data becoming live again - the most common way of losing sstables is to have a hard drive break down. To avoid making
data live tombstones and actual data are always in the same data directory. This way, if a disk is lost, all versions of
a partition are lost and no data can get undeleted. To achieve this a compaction strategy instance per data directory is
run in addition to the compaction strategy instances containing repaired/unrepaired data, this means that if you have 4
data directories there will be 8 compaction strategy instances running. This has a few more benefits than just avoiding
data getting undeleted:

- It is possible to run more compactions in parallel - leveled compaction will have several totally separate levelings
  and each one can run compactions independently from the others.
- Users can backup and restore a single data directory.
- Note though that currently all data directories are considered equal, so if you have a tiny disk and a big disk
  backing two data directories, the big one will be limited the by the small one. One work around to this is to create
  more data directories backed by the big disk.

Single sstable tombstone compaction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When an sstable is written a histogram with the tombstone expiry times is created and this is used to try to find
sstables with very many tombstones and run single sstable compaction on that sstable in hope of being able to drop
tombstones in that sstable. Before starting this it is also checked how likely it is that any tombstones will actually
will be able to be dropped how much this sstable overlaps with other sstables. To avoid most of these checks the
compaction option ``unchecked_tombstone_compaction`` can be enabled.

.. _compaction-options:

Common options
^^^^^^^^^^^^^^

There is a number of common options for all the compaction strategies;

``enabled`` (default: true)
    Whether minor compactions should run. Note that you can have 'enabled': true as a compaction option and then do
    'nodetool enableautocompaction' to start running compactions.
``tombstone_threshold`` (default: 0.2)
    How much of the sstable should be tombstones for us to consider doing a single sstable compaction of that sstable.
``tombstone_compaction_interval`` (default: 86400s (1 day))
    Since it might not be possible to drop any tombstones when doing a single sstable compaction we need to make sure
    that one sstable is not constantly getting recompacted - this option states how often we should try for a given
    sstable. 
``log_all`` (default: false)
    New detailed compaction logging, see :ref:`below <detailed-compaction-logging>`.
``unchecked_tombstone_compaction`` (default: false)
    The single sstable compaction has quite strict checks for whether it should be started, this option disables those
    checks and for some usecases this might be needed.  Note that this does not change anything for the actual
    compaction, tombstones are only dropped if it is safe to do so - it might just rewrite an sstable without being able
    to drop any tombstones.
``only_purge_repaired_tombstone`` (default: false)
    Option to enable the extra safety of making sure that tombstones are only dropped if the data has been repaired.
``min_threshold`` (default: 4)
    Lower limit of number of sstables before a compaction is triggered. Not used for ``LeveledCompactionStrategy``.
``max_threshold`` (default: 32)
    Upper limit of number of sstables before a compaction is triggered. Not used for ``LeveledCompactionStrategy``.

Further, see the section on each strategy for specific additional options.

Compaction nodetool commands
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :ref:`nodetool <nodetool>` utility provides a number of commands related to compaction:

``enableautocompaction``
    Enable compaction.
``disableautocompaction``
    Disable compaction.
``setcompactionthroughput``
    How fast compaction should run at most - defaults to 16MB/s, but note that it is likely not possible to reach this
    throughput.
``compactionstats``
    Statistics about current and pending compactions.
``compactionhistory``
    List details about the last compactions.
``setcompactionthreshold``
    Set the min/max sstable count for when to trigger compaction, defaults to 4/32.

Switching the compaction strategy and options using JMX
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is possible to switch compaction strategies and its options on just a single node using JMX, this is a great way to
experiment with settings without affecting the whole cluster. The mbean is::

    org.apache.cassandra.db:type=ColumnFamilies,keyspace=<keyspace_name>,columnfamily=<table_name>

and the attribute to change is ``CompactionParameters`` or ``CompactionParametersJson`` if you use jconsole or jmc. The
syntax for the json version is the same as you would use in an :ref:`ALTER TABLE <alter-table-statement>` statement -
for example::

    { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 123, 'fanout_size': 10}

The setting is kept until someone executes an :ref:`ALTER TABLE <alter-table-statement>` that touches the compaction
settings or restarts the node.

.. _detailed-compaction-logging:

More detailed compaction logging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Enable with the compaction option ``log_all`` and a more detailed compaction log file will be produced in your log
directory.





