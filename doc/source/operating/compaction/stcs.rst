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


.. _STCS:

Leveled Compaction Strategy
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The basic idea of ``SizeTieredCompactionStrategy`` (STCS) is to merge sstables of approximately the same size. All
sstables are put in different buckets depending on their size. An sstable is added to the bucket if size of the sstable
is within ``bucket_low`` and ``bucket_high`` of the current average size of the sstables already in the bucket. This
will create several buckets and the most interesting of those buckets will be compacted. The most interesting one is
decided by figuring out which bucket's sstables takes the most reads.

Major compaction
~~~~~~~~~~~~~~~~

When running a major compaction with STCS you will end up with two sstables per data directory (one for repaired data
and one for unrepaired data). There is also an option (-s) to do a major compaction that splits the output into several
sstables. The sizes of the sstables are approximately 50%, 25%, 12.5%... of the total size.

.. _stcs-options:

STCS options
~~~~~~~~~~~~

``min_sstable_size`` (default: 50MB)
    Sstables smaller than this are put in the same bucket.
``bucket_low`` (default: 0.5)
    How much smaller than the average size of a bucket a sstable should be before not being included in the bucket. That
    is, if ``bucket_low * avg_bucket_size < sstable_size`` (and the ``bucket_high`` condition holds, see below), then
    the sstable is added to the bucket.
``bucket_high`` (default: 1.5)
    How much bigger than the average size of a bucket a sstable should be before not being included in the bucket. That
    is, if ``sstable_size < bucket_high * avg_bucket_size`` (and the ``bucket_low`` condition holds, see above), then
    the sstable is added to the bucket.

Defragmentation
~~~~~~~~~~~~~~~

Defragmentation is done when many sstables are touched during a read.  The result of the read is put in to the memtable
so that the next read will not have to touch as many sstables. This can cause writes on a read-only-cluster.


