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



.. _LCS:

Leveled Compaction Strategy
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The idea of ``LeveledCompactionStrategy`` (LCS) is that all sstables are put into different levels where we guarantee
that no overlapping sstables are in the same level. By overlapping we mean that the first/last token of a single sstable
are never overlapping with other sstables. This means that for a SELECT we will only have to look for the partition key
in a single sstable per level. Each level is 10x the size of the previous one and each sstable is 160MB by default. L0
is where sstables are streamed/flushed - no overlap guarantees are given here.

When picking compaction candidates we have to make sure that the compaction does not create overlap in the target level.
This is done by always including all overlapping sstables in the next level. For example if we select an sstable in L3,
we need to guarantee that we pick all overlapping sstables in L4 and make sure that no currently ongoing compactions
will create overlap if we start that compaction. We can start many parallel compactions in a level if we guarantee that
we wont create overlap. For L0 -> L1 compactions we almost always need to include all L1 sstables since most L0 sstables
cover the full range. We also can't compact all L0 sstables with all L1 sstables in a single compaction since that can
use too much memory.

When deciding which level to compact LCS checks the higher levels first (with LCS, a "higher" level is one with a higher
number, L0 being the lowest one) and if the level is behind a compaction will be started in that level.

Major compaction
~~~~~~~~~~~~~~~~

It is possible to do a major compaction with LCS - it will currently start by filling out L1 and then once L1 is full,
it continues with L2 etc. This is sub optimal and will change to create all the sstables in a high level instead,
CASSANDRA-11817.

Bootstrapping
~~~~~~~~~~~~~

During bootstrap sstables are streamed from other nodes. The level of the remote sstable is kept to avoid many
compactions after the bootstrap is done. During bootstrap the new node also takes writes while it is streaming the data
from a remote node - these writes are flushed to L0 like all other writes and to avoid those sstables blocking the
remote sstables from going to the correct level, we only do STCS in L0 until the bootstrap is done.

STCS in L0
~~~~~~~~~~

If LCS gets very many L0 sstables reads are going to hit all (or most) of the L0 sstables since they are likely to be
overlapping. To more quickly remedy this LCS does STCS compactions in L0 if there are more than 32 sstables there. This
should improve read performance more quickly compared to letting LCS do its L0 -> L1 compactions. If you keep getting
too many sstables in L0 it is likely that LCS is not the best fit for your workload and STCS could work out better.

Starved sstables
~~~~~~~~~~~~~~~~

If a node ends up with a leveling where there are a few very high level sstables that are not getting compacted they
might make it impossible for lower levels to drop tombstones etc. For example, if there are sstables in L6 but there is
only enough data to actually get a L4 on the node the left over sstables in L6 will get starved and not compacted.  This
can happen if a user changes sstable\_size\_in\_mb from 5MB to 160MB for example. To avoid this LCS tries to include
those starved high level sstables in other compactions if there has been 25 compaction rounds where the highest level
has not been involved.

.. _lcs-options:

LCS options
~~~~~~~~~~~

``sstable_size_in_mb`` (default: 160MB)
    The target compressed (if using compression) sstable size - the sstables can end up being larger if there are very
    large partitions on the node.

``fanout_size`` (default: 10)
    The target size of levels increases by this fanout_size multiplier. You can reduce the space amplification by tuning
    this option.

LCS also support the ``cassandra.disable_stcs_in_l0`` startup option (``-Dcassandra.disable_stcs_in_l0=true``) to avoid
doing STCS in L0.


