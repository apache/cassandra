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


.. _TWCS:

Time Window CompactionStrategy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``TimeWindowCompactionStrategy`` (TWCS) is designed specifically for workloads where it's beneficial to have data on
disk grouped by the timestamp of the data, a common goal when the workload is time-series in nature or when all data is
written with a TTL. In an expiring/TTL workload, the contents of an entire SSTable likely expire at approximately the
same time, allowing them to be dropped completely, and space reclaimed much more reliably than when using
``SizeTieredCompactionStrategy`` or ``LeveledCompactionStrategy``. The basic concept is that
``TimeWindowCompactionStrategy`` will create 1 sstable per file for a given window, where a window is simply calculated
as the combination of two primary options:

``compaction_window_unit`` (default: DAYS)
    A Java TimeUnit (MINUTES, HOURS, or DAYS).
``compaction_window_size`` (default: 1)
    The number of units that make up a window.
``unsafe_aggressive_sstable_expiration`` (default: false)
    Expired sstables will be dropped without checking its data is shadowing other sstables. This is a potentially
    risky option that can lead to data loss or deleted data re-appearing, going beyond what
    `unchecked_tombstone_compaction` does for single  sstable compaction. Due to the risk the jvm must also be
    started with `-Dcassandra.unsafe_aggressive_sstable_expiration=true`.

Taken together, the operator can specify windows of virtually any size, and `TimeWindowCompactionStrategy` will work to
create a single sstable for writes within that window. For efficiency during writing, the newest window will be
compacted using `SizeTieredCompactionStrategy`.

Ideally, operators should select a ``compaction_window_unit`` and ``compaction_window_size`` pair that produces
approximately 20-30 windows - if writing with a 90 day TTL, for example, a 3 Day window would be a reasonable choice
(``'compaction_window_unit':'DAYS','compaction_window_size':3``).

TimeWindowCompactionStrategy Operational Concerns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The primary motivation for TWCS is to separate data on disk by timestamp and to allow fully expired SSTables to drop
more efficiently. One potential way this optimal behavior can be subverted is if data is written to SSTables out of
order, with new data and old data in the same SSTable. Out of order data can appear in two ways:

- If the user mixes old data and new data in the traditional write path, the data will be comingled in the memtables
  and flushed into the same SSTable, where it will remain comingled.
- If the user's read requests for old data cause read repairs that pull old data into the current memtable, that data
  will be comingled and flushed into the same SSTable.

While TWCS tries to minimize the impact of comingled data, users should attempt to avoid this behavior.  Specifically,
users should avoid queries that explicitly set the timestamp via CQL ``USING TIMESTAMP``. Additionally, users should run
frequent repairs (which streams data in such a way that it does not become comingled).

Changing TimeWindowCompactionStrategy Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Operators wishing to enable ``TimeWindowCompactionStrategy`` on existing data should consider running a major compaction
first, placing all existing data into a single (old) window. Subsequent newer writes will then create typical SSTables
as expected.

Operators wishing to change ``compaction_window_unit`` or ``compaction_window_size`` can do so, but may trigger
additional compactions as adjacent windows are joined together. If the window size is decrease d (for example, from 24
hours to 12 hours), then the existing SSTables will not be modified - TWCS can not split existing SSTables into multiple
windows.

