<!---
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Compaction process optimization

This is a refactoring of the sstable content iteration mechanisms to strip much of the avoidable overhead. The main idea
is the restructuring of the iteration into one stream of cells mixed with column, row and partition headers, which
avoids having to recreate the `Row`s and `UnfilteredRowIterator`s together with all the infrastructure needed to merge
them.

The data are exposed via a stateful `SSTableCursor` object, which starts uninitialized and can be `advance()`-d to
obtain items of the stream. The cursor exposes the state of iteration, including:

- the current partition key
- the current clustering key
- the current column
- the current cell (which may include a cell path)

together with an indication of the "level" in the sstable hierarchy that the current position is at. On a valid
position, the latter can be `PARTITION`, `ROW`/`RANGE_TOMBSTONE`, `SIMPLE/COMPLEX_COLUMN` or `COMPLEX_COLUMN_CELL`,
indicating that advancing has ended up, respectively, at the start of a new partition, new row, new range tombstone
marker, new simple column cell, the start of a complex column, or a cell in the complex column. If on an upper level of
the hierarchy, the details about features of the lower levels (e.g. clustering key on `PARTITION`, cell on `ROW`
or `PARTITION`) are invalid, but the information for all higher levels (e.g. partition key on `ROW`) is present.

Cursors' state (i.e. position in the stream) can be compared by comparing the keys in order. More importantly, when we
iterate several cursors in order while merging, we can observe that a cursor positioned on a higher level of the
hierarchy must have a position later than the cursors that are still producing items at a lower level and thus
comparisons can be done by only comparing the level and then (if levels match), the key at the common level. This is
crucial for the efficiency of merging.

Note: we also considered using a "level change" flag instead of stopping on headers (i.e. instead of advancing through
`PARTITION(pk1), ROW(ck1), SIMPLE_COLUMN(c1), ROW(ck2), SIMPLE_COLUMN(c2)` to
list `PARTITION(pk1, ck1, c1), ROW(ck2, c2)`). While it does look like with this we can still efficiently compare via
the level, we need to somehow consume the level advance on matching comparison, which is non-trivial and error-prone.
For example, consider merging:

- `PARTITION(pk1, ck1, c1)`, `ROW(ck3, c2)`
- `PARTITION(pk1, ck2, c3)`

Cell, row and partition deletions are directly reported by methods that return the relevant deletion times. Range
tombstone markers are reported on the rows level with the deletion time they switch to by the `rowLevelDeletion()`
method (i.e. the open time if it's an open bound or a boundary, or LIVE if it's a close bound). The currently active
deletion time is also tracked and reported through the `activeRangeDeletion()` method; note that if the stream is
positioned or a range tombstone marker, it reports the deletion active _before_ it, so that both deletions are
available (useful both for reconstructing range tombstone markers on write, and for merging, where we need to know the
active range deletion before the position on the sources that are positioned later in the stream). The merge cursor
takes care of applying the active deletion (the newest of complex-column, range, row- and partition-level deletion) to
the data it processes to remove any deleted data and tombstones.

There are a couple of further differences with iterators:

- Static rows are listed as the first row in a partition, only if they are not empty &mdash; separating them is only
  useful for reverse iteration which cursors don't aim to support.
- Cursors only iterate on data files, which avoids walking the partition index. This means less resilience to error, but
  in compactions this is not a problem as we abort on error.
- "Shadowable row deletions" (a deprecated feature which is no longer in use) are not reported as such.

Cursors don't currently support all of the functionality of merging over sstable iterators. For details of the
limitations, see the TODO list below.

Beyond the above, the implementation is straight-forward:

- `SSTableCursor` is the main abstraction, a cursor over sstables.
- `SortedStringTableCursor` is the main implementation of `SSTableCursor` which walks over an sstable data file and
  extracts its data stream. To do this it reimplements the functionality of the deserializers for parsing partition, row
  and column headers and relies on an instance of the deserializer to read cells. Supports both BIG and BTI formats
  (which differ only in index and whose data file formats are identical).
- `SSTableCursorMerger` implements merging several `SSTableCursor`s into one. This is implemented via an extracted merge
  core from `MergeIterator` configured to work on cursors.
- `PurgeCursor` implements removal of collectable tombstones.
- `SkipEmptyDataCursor` delays the reporting of headers until content is found, in order to avoid creating empty complex
  columns, rows or partitions in the compacted view.
- `CompactionCursor` sets up a merger over multiple sstable cursors for compaction and implements writing a cursor into
  a new sstable. Note: we currently still create an in-memory row to be able to send it to the serializer for writing.
- `CompactionTask.CompactionOperationCursor` is a cursor counterpart of `CompactionTask.CompactionOperationIterator`.
  The former is chosen if cursors can support the compaction, i.e. if it is known that a secondary index is not in use,
  that the compaction strategy supports cursors (initially we only intend to release this
  for `UnifiedCompactionStrategy`; even afterwards, `TieredCompactionStrategy` would need special support), and that a
  garbage collection compaction is not requested.

Additionally,

- `IteratorFromCursor` converts a cursor into an unfiltered partition iterator for testing and can also be used as a
  reference of the differences.

### Further work

- Writing without going through `Row`, i.e. sending individual cells to the writer instead of the in-memory `Row`
  objects, using a refactoring similar to what is currently done to write rows instead of partitions, should improve
  performance further.

- Secondary indexes currently can't use cursors, because we do not construct the right input for their listeners. Doing
  this may require reconstructing rows for the merge, which is something we definitely do not want to do in the normal
  case. It would probably be better to find the specific needs of SAI and support only them, and leave legacy / custom
  indexes to use iterators (note: since TPC is not going to be developed further, we no longer plan to fully replace the
  iterators with this).

- Garbage collection compaction, i.e. compaction using tombstones from newer non-participating sstables to delete as
  much as possible from the compacted sstables, could be implemented for cursors too.

- If we are going to support all compaction strategies, it may be beneficial to restore levelled compaction's sstable
  concatenation scanner. However, this will only save one comparison per partition, so I doubt it's really worth doing.

## Benchmark results collected during development (most recent results first)

Perhaps most relevant at this time are the differences between `iterateThroughCompactionCursor` vs
`iterateThroughCompactionIterator` (sending the compaction of two sstables to a null writer). Other meaninful
comparisons are `iterateThroughCursor` vs `iterateThroughTableScanner`
(iterating the content of a single sstable without merging) and `iterateThroughMergeCursor` vs
`iterateThroughMergeIterator` (iterating the merge of two sstables, similar to `iterateThroughCompactionCursor`
but without constructing in-memory rows).

```
Benchmark                                                               (compactionMbSecThrottle)  (compactors)  (compression)  (dataBuilder)  (overlapRatio)  (size)  (sstableCount)  Mode  Cnt     Score    Error  Units
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                                     0             1          false        DEFAULT             0.3      10               2  avgt   10   818.816 ± 46.403  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursorWithLimiter                          0             1          false        DEFAULT             0.3      10               2  avgt   10   841.232 ± 41.720  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                                   0             1          false        DEFAULT             0.3      10               2  avgt   10  1731.936 ± 72.224  ms/op
CompactionBreakdownBenchmark.iterateThroughCursor                                               0             1          false        DEFAULT             0.3      10               2  avgt   10   480.876 ± 45.553  ms/op
CompactionBreakdownBenchmark.iterateThroughCursorToIterator                                     0             1          false        DEFAULT             0.3      10               2  avgt   10   602.826 ± 42.933  ms/op
CompactionBreakdownBenchmark.iterateThroughMergeCursor                                          0             1          false        DEFAULT             0.3      10               2  avgt   10   719.327 ± 41.238  ms/op
CompactionBreakdownBenchmark.iterateThroughMergeIterator                                        0             1          false        DEFAULT             0.3      10               2  avgt   10  1406.228 ± 74.427  ms/op
CompactionBreakdownBenchmark.iterateThroughPartitionIndexIterator                               0             1          false        DEFAULT             0.3      10               2  avgt   10   340.876 ± 27.913  ms/op
CompactionBreakdownBenchmark.iterateThroughTableScanner                                         0             1          false        DEFAULT             0.3      10               2  avgt   10  1039.944 ± 89.224  ms/op
```

```
Benchmark                                                      (compactionMbSecThrottle)  (compactors)  (compression)               (dataBuilder)  (overlapRatio)  (size)  (sstableCount)  Mode  Cnt     Score     Error  Units
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                            0             1          false                     DEFAULT             0.3      10               2  avgt   10   812.300 ±  35.196  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                          0             1          false                     DEFAULT             0.3      10               2  avgt   10  1799.127 ± 100.290  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                            0             1          false            BLOB_CLUSTER_KEY             0.3      10               2  avgt   10   874.638 ±  46.639  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                          0             1          false            BLOB_CLUSTER_KEY             0.3      10               2  avgt   10  1813.990 ±  89.474  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                            0             1          false                  BLOB_VALUE             0.3      10               2  avgt   10   850.173 ±  37.608  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                          0             1          false                  BLOB_VALUE             0.3      10               2  avgt   10  1773.747 ±  82.984  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                            0             1          false           MANY_CLUSTER_KEYS             0.3      10               2  avgt   10  1501.582 ±  91.084  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                          0             1          false           MANY_CLUSTER_KEYS             0.3      10               2  avgt   10  2470.640 ±  79.072  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                            0             1          false                 MANY_FIELDS             0.3      10               2  avgt   10  2643.602 ±  85.875  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                          0             1          false                 MANY_FIELDS             0.3      10               2  avgt   10  3095.176 ±  71.408  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                            0             1          false             WIDE_PARTITIONS             0.3      10               2  avgt   10   524.839 ±  19.716  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                          0             1          false             WIDE_PARTITIONS             0.3      10               2  avgt   10   564.349 ±  20.299  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                            0             1          false      COMPLEX_COLUMNS_INSERT             0.3      10               2  avgt   10  1735.086 ±  92.367  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                          0             1          false      COMPLEX_COLUMNS_INSERT             0.3      10               2  avgt   10  2730.369 ±  93.184  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                            0             1          false  COMPLEX_COLUMNS_UPDATE_SET             0.3      10               2  avgt   10  1691.803 ±  83.824  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                          0             1          false  COMPLEX_COLUMNS_UPDATE_SET             0.3      10               2  avgt   10  2671.245 ±  90.731  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                            0             1          false  COMPLEX_COLUMNS_UPDATE_ADD             0.3      10               2  avgt   10  1541.798 ±  96.077  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                          0             1          false  COMPLEX_COLUMNS_UPDATE_ADD             0.3      10               2  avgt   10  2649.346 ± 101.025  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                            0             1          false                  TOMBSTONES             0.3      10               2  avgt   10   971.576 ±  76.193  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                          0             1          false                  TOMBSTONES             0.3      10               2  avgt   10  1908.025 ±  80.601  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                            0             1          false             TOMBSTONES_WIDE             0.3      10               2  avgt   10   594.306 ±  11.245  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                          0             1          false             TOMBSTONES_WIDE             0.3      10               2  avgt   10   701.378 ±  15.859  ms/op
```

```
Benchmark                                            (compaction)  (compactionMbSecThrottle)  (compactors)  (compression)  (dataBuilder)  (overlapRatio)  (size)  (sstableCount)  Mode  Cnt     Score     Error  Units
CompactionBenchmark.compactSSTables  SizeTieredCompactionStrategy                          0             1          false        DEFAULT             0.3      10               2  avgt   10  4151.567 ± 393.045  ms/op
CompactionBenchmark.compactSSTables     UnifiedCompactionStrategy                          0             1          false        DEFAULT             0.3      10               2  avgt   10  3097.753 ±  90.189  ms/op
```

With tombstone and purging support, no row reconstructing in `iterateThroughCompactionCursor`.

```
Benchmark                                                               (compactionMbSecThrottle)  (compactors)  (compression)  (dataBuilder)  (overlapRatio)  (size)  (sstableCount)  Mode  Cnt     Score    Error  Units
CompactionBenchmark.compactSSTables                                                             0             1          false        DEFAULT             0.3      10               2  avgt   10  3275.471 ± 99.962  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                                     0             1          false        DEFAULT             0.3      10               2  avgt   10   726.345 ± 53.326  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursorWithLimiter                          0             1          false        DEFAULT             0.3      10               2  avgt   10   705.488 ± 40.847  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                                   0             1          false        DEFAULT             0.3      10               2  avgt   10  1820.006 ± 92.980  ms/op
CompactionBreakdownBenchmark.iterateThroughCursor                                               0             1          false        DEFAULT             0.3      10               2  avgt   10   463.956 ± 32.461  ms/op
CompactionBreakdownBenchmark.iterateThroughCursorToIterator                                     0             1          false        DEFAULT             0.3      10               2  avgt   10   595.363 ± 42.723  ms/op
CompactionBreakdownBenchmark.iterateThroughMergeCursor                                          0             1          false        DEFAULT             0.3      10               2  avgt   10   699.307 ± 44.041  ms/op
CompactionBreakdownBenchmark.iterateThroughMergeIterator                                        0             1          false        DEFAULT             0.3      10               2  avgt   10  1434.870 ± 84.047  ms/op
CompactionBreakdownBenchmark.iterateThroughPartitionIndexIterator                               0             1          false        DEFAULT             0.3      10               2  avgt   10   347.405 ± 25.364  ms/op
CompactionBreakdownBenchmark.iterateThroughTableScanner                                         0             1          false        DEFAULT             0.3      10               2  avgt   10  1014.317 ± 90.495  ms/op
CompactionBreakdownBenchmark.scannerToCompactionWriter                                          0             1          false        DEFAULT             0.3      10               2  avgt   10  2000.747 ± 76.360  ms/op
```

With tombstone and purging support

```
Benchmark                                                               (compactionMbSecThrottle)  (compactors)  (compression)  (dataBuilder)  (overlapRatio)  (size)  (sstableCount)  Mode  Cnt     Score    Error  Units
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                                     0             1          false        DEFAULT             0.3      10               2  avgt   10   701.806 ± 42.179  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursorWithLimiter                          0             1          false        DEFAULT             0.3      10               2  avgt   10   710.938 ± 41.999  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                                   0             1          false        DEFAULT             0.3      10               2  avgt   10  1777.159 ± 87.460  ms/op
CompactionBreakdownBenchmark.iterateThroughCursor                                               0             1          false        DEFAULT             0.3      10               2  avgt   10   457.567 ± 27.544  ms/op
CompactionBreakdownBenchmark.iterateThroughCursorToIterator                                     0             1          false        DEFAULT             0.3      10               2  avgt   10   592.213 ± 36.001  ms/op
CompactionBreakdownBenchmark.iterateThroughMergeCursor                                          0             1          false        DEFAULT             0.3      10               2  avgt   10   747.161 ± 52.323  ms/op
CompactionBreakdownBenchmark.iterateThroughMergeIterator                                        0             1          false        DEFAULT             0.3      10               2  avgt   10  1500.166 ± 87.268  ms/op
CompactionBreakdownBenchmark.iterateThroughPartitionIndexIterator                               0             1          false        DEFAULT             0.3      10               2  avgt   10   345.971 ± 24.798  ms/op
CompactionBreakdownBenchmark.iterateThroughTableScanner                                         0             1          false        DEFAULT             0.3      10               2  avgt   10  1040.594 ± 82.862  ms/op
CompactionBreakdownBenchmark.scannerToCompactionWriter                                          0             1          false        DEFAULT             0.3      10               2  avgt   10  1968.898 ± 98.314  ms/op
CompactionBenchmark.compactSSTables                                                             0             1          false        DEFAULT             0.3      10               2  avgt   10  3072.202 ± 104.127  ms/op
```

Cell-level stream, deserialized cells, recombined rows on write

```
Benchmark                                                               (compactionMbSecThrottle)  (compactors)  (compression)  (dataBuilder)  (overlapRatio)  (size)  (sstableCount)  Mode  Cnt     Score     Error  Units
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                                     0             1          false        DEFAULT             0.3      10               2  avgt   10   799.025 ±  26.943  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursorWithLimiter                          0             1          false        DEFAULT             0.3      10               2  avgt   10   795.218 ±  17.373  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                                   0             1          false        DEFAULT             0.3      10               2  avgt   10  1996.695 ±  50.663  ms/op
CompactionBreakdownBenchmark.iterateThroughCursor                                               0             1          false        DEFAULT             0.3      10               2  avgt   10   488.837 ±   9.936  ms/op
CompactionBreakdownBenchmark.iterateThroughMergeCursor                                          0             1          false        DEFAULT             0.3      10               2  avgt   10   788.899 ±  20.713  ms/op
CompactionBreakdownBenchmark.iterateThroughMergeIterator                                        0             1          false        DEFAULT             0.3      10               2  avgt   10  1698.324 ± 199.704  ms/op
CompactionBreakdownBenchmark.iterateThroughPartitionIndexIterator                               0             1          false        DEFAULT             0.3      10               2  avgt   10   394.046 ±  11.057  ms/op
CompactionBreakdownBenchmark.iterateThroughTableScanner                                         0             1          false        DEFAULT             0.3      10               2  avgt   10  1250.365 ±  45.029  ms/op
```

With direct write, row stream

```
Benchmark                                                               (compactionMbSecThrottle)  (compactors)  (compression)  (dataBuilder)  (overlapRatio)  (size)  (sstableCount)  Mode  Cnt     Score    Error  Units
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                                     0             1          false        DEFAULT             0.3      10               2  avgt   10   960.035 ± 21.594  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursorWithLimiter                          0             1          false        DEFAULT             0.3      10               2  avgt   10   966.950 ± 43.067  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                                   0             1          false        DEFAULT             0.3      10               2  avgt   10  2062.935 ± 30.653  ms/op
CompactionBenchmark.compactSSTables                                                             0             1          false        DEFAULT             0.3      10               2  avgt   10  3294.247 ± 101.186  ms/op
```

With progress indication and rate limiting, row stream, converted to iterator

```
Benchmark                                                               (compactionMbSecThrottle)  (compactors)  (compression)  (dataBuilder)  (overlapRatio)  (size)  (sstableCount)  Mode  Cnt     Score    Error  Units
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                                     0             1          false        DEFAULT             0.3      10               2  avgt   10   964.176 ± 19.588  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionCursorWithLimiter                          0             1          false        DEFAULT             0.3      10               2  avgt   10  1000.995 ± 28.157  ms/op       Note: has synchronization, uncontended in this test
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                                   0             1          false        DEFAULT             0.3      10               2  avgt   10  2043.687 ± 35.059  ms/op
CompactionBenchmark.compactSSTables                          									0             1          false        DEFAULT             0.3      10               2  avgt   10  3575.346 ± 94.917  ms/op
```

With CompactionCursor, merge through cursor, deserialized rows, no progress/deletions/indexes

```
Benchmark                                                      (compactionMbSecThrottle)  (compactors)  (compression)  (dataBuilder)  (overlapRatio)  (size)  (sstableCount)  Mode  Cnt     Score    Error  Units
CompactionBreakdownBenchmark.iterateThroughCompactionCursor                            0             1          false        DEFAULT             0.3      10               2  avgt   10   944.843 ± 13.718  ms/op
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                          0             1          false        DEFAULT             0.3      10               2  avgt   10  2070.773 ± 43.277  ms/op
CompactionBenchmark.compactSSTables               							           0             1          false        DEFAULT             0.3      10               2  avgt    9  3329.419 ± 81.107  ms/op
```

Initial implementation, row stream, converted to iterator

```
Benchmark                                                          (compactionMbSecThrottle)  (compactors)  (compression)  (dataBuilder)  (overlapRatio)  (size)  (sstableCount)  Mode  Cnt     Score     Error  Units
CompactionBreakdownBenchmark.iterateThroughCompactionIterator                              0             1          false        DEFAULT             0.3      10               2  avgt   10  2064.699 ±  27.296  ms/op
CompactionBreakdownBenchmark.iterateThroughCursor                                          0             1          false        DEFAULT             0.3      10               2  avgt   10   597.398 ±  20.261  ms/op
CompactionBreakdownBenchmark.iterateThroughMergeCursor                                     0             1          false        DEFAULT             0.3      10               2  avgt   10   949.216 ±  13.484  ms/op
CompactionBreakdownBenchmark.iterateThroughMergeIterator                                   0             1          false        DEFAULT             0.3      10               2  avgt   10  1619.175 ±  39.231  ms/op
CompactionBreakdownBenchmark.iterateThroughPartitionIndexIterator                          0             1          false        DEFAULT             0.3      10               2  avgt   10   413.512 ± 100.120  ms/op
CompactionBreakdownBenchmark.iterateThroughTableScanner                                    0             1          false        DEFAULT             0.3      10               2  avgt   10  1167.989 ±  37.205  ms/op
CompactionBreakdownBenchmark.scannerToCompctionWriter                                      0             1          false        DEFAULT             0.3      10               2  avgt    9  2516.367 ±  82.761  ms/op
CompactionBenchmark.compactSSTables                                                        0             1          false        DEFAULT             0.3      10               2  avgt    9  4622.173 ± 157.001  ms/op
```

For information only -- skipping row body

```
Benchmark                                                          (compactionMbSecThrottle)  (compactors)  (compression)  (dataBuilder)  (overlapRatio)  (size)  (sstableCount)  Mode  Cnt    Score    Error  Units
CompactionBreakdownBenchmark.iterateThroughCursor                                          0             1          false        DEFAULT             0.3      10               2  avgt   10  402.963 ± 11.514  ms/op   - 200
CompactionBreakdownBenchmark.iterateThroughMergeCursor                                     0             1          false        DEFAULT             0.3      10               2  avgt   10  668.527 ± 13.703  ms/op   - 300
CompactionBreakdownBenchmark.iterateThroughTableScanner                                    0             1          false        DEFAULT             0.3      10               2  avgt   10  942.351 ± 24.458  ms/op   - 200
```

