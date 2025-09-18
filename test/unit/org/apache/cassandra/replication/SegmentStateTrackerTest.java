/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.replication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.harry.checker.TestHelper;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.Pair;
import org.junit.Assert;
import org.junit.Test;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SegmentStateTrackerTest
{
    @Test
    public void trivialTest()
    {
        long segment = 123L;
        SegmentStateTracker tracker = new SegmentStateTracker(segment);
        TableId tbl = TableId.generate();

        assertTrue(tracker.isClean());
        for (int i = 1; i <= 10; i++)
        {
            tracker.markDirty(tbl, segment, i);
            assertFalse(tracker.isClean());
            if (i % 5 == 0)
            {
                tracker.markClean(tbl, pos(segment, i - 5), pos(segment, i));
                assertTrue(tracker.isClean());
            }
        }
        assertTrue(tracker.isClean());
    }

    @Test
    public void memtableSpannignMultipleSegmentsTest()
    {
        SegmentStateTracker segment1 = new SegmentStateTracker(1);
        SegmentStateTracker segment2 = new SegmentStateTracker(2);
        TableId tbl = TableId.fromLong(1);

        segment1.markDirty(tbl, segment1.segmentId(), 10);
        segment2.markDirty(tbl, segment2.segmentId(), 10);
        segment2.markDirty(tbl, segment2.segmentId(), 20);

        segment1.markClean(tbl, pos(segment1.segmentId(), 10), pos(segment2.segmentId(), 10)); // first flush
        segment2.markClean(tbl, pos(segment1.segmentId(), 10), pos(segment2.segmentId(), 10)); // first flush
        segment2.markClean(tbl, pos(segment2.segmentId(), 10), pos(segment2.segmentId(), 30)); // second flush
        Assert.assertTrue(segment1.isClean());
        Assert.assertTrue(segment2.isClean());
    }

    @Test
    public void fuzzTest()
    {
        int allocationCount = 10_000;
        long seed = 0;
        List<String> ops = null; // new ArrayList<>(); // to enable operation logging
        AtomicLong tableIdGen = new AtomicLong();
        TestHelper.withRandom(seed,
                              rng -> {
                                  // This test tracks state from the perspective of state tracker:
                                  // We make allocations by bumping the pointer, reporting allocations to different tables.
                                  // Table can flush subsequent allocations, and report information back to the segment.
                                  class Table
                                  {
                                      final TableId tableId = TableId.fromLong(tableIdGen.getAndIncrement());

                                      // Track all allocations to a cfid in a Table, and perform contiguous "flushes"
                                      // that perform position bounds to tracker
                                      ArrayList<CommitLogPosition> unflushedAllocations = new ArrayList<>();

                                      // Memtables _always_ report contiguous chunks of metadata, see CFS$Flush, new memtable
                                      // will always be created with commitLogUpperBound of the previous one.
                                      CommitLogPosition lastFlushMax = null;

                                      void addAllocation(CommitLogPosition pos)
                                      {
                                          if (ops != null) ops.add("Allocate " + pos + " in " + tableId);
                                          unflushedAllocations.add(pos);
                                      }

                                      void flush(Collection<SegmentStateTracker> trackers)
                                      {
                                          if (unflushedAllocations.isEmpty())
                                              return;

                                          CommitLogPosition min = lastFlushMax == null ? unflushedAllocations.get(0) : lastFlushMax;
                                          CommitLogPosition max = unflushedAllocations.get(rng.nextInt(unflushedAllocations.size()));
                                          lastFlushMax = max;

                                          if (ops != null) ops.add(String.format("Flush %s [%s, %s]", tableId, min, max));
                                          for (SegmentStateTracker tracker : trackers)
                                              reportFlushed(tracker, min, max);

                                          // TODO (required): use array an copying instead
                                          unflushedAllocations.removeIf(alloc -> alloc.compareTo(min) >= 0 && alloc.compareTo(max) <= 0);
                                      }

                                      boolean hasUnflushed()
                                      {
                                          return !unflushedAllocations.isEmpty();
                                      }

                                      boolean hasUnflushedFor(long segment)
                                      {
                                          for (CommitLogPosition alloc : unflushedAllocations)
                                          {
                                              if (alloc.segmentId == segment)
                                                  return true;
                                          }
                                          return false;
                                      }

                                      List<CommitLogPosition> getUnflushedFor(long segment)
                                      {
                                          List<CommitLogPosition> unflushed = new ArrayList<>();
                                          for (CommitLogPosition alloc : unflushedAllocations)
                                          {
                                              if (alloc.segmentId == segment)
                                                  unflushed.add(alloc);
                                          }
                                          return unflushed;
                                      }

                                      void reportFlushed(SegmentStateTracker tracker, CommitLogPosition minBound, CommitLogPosition maxBound)
                                      {
                                          if (tracker.segmentId() >= minBound.segmentId && tracker.segmentId() <= maxBound.segmentId)
                                              tracker.markClean(tableId, minBound, maxBound);
                                      }
                                  }

                                  int tableCount = 10;
                                  List<Table> tables = new ArrayList<>(tableCount);
                                  for (int i = 0; i < tableCount; i++)
                                      tables.add(new Table());

                                  Map<Long, SegmentStateTracker> segments = new HashMap<>();
                                  Runnable validateAllSegments = () -> {
                                      for (SegmentStateTracker segment : segments.values())
                                      {
                                          boolean segmentIsClean = segment.isClean();
                                          boolean allTablesFlushed = tables.stream().noneMatch(t -> t.hasUnflushedFor(segment.segmentId()));
                                          if (segmentIsClean != allTablesFlushed)
                                              throw new IllegalArgumentException(String.format("Segment is %sclean, but table has %sunflushed allocations:\n%s\n%s",
                                                                                               segmentIsClean ? "" : "not ",
                                                                                               allTablesFlushed ? "" : "no ",
                                                                                               segment,
                                                                                               tables.stream().map(t -> Pair.create(t.tableId, t.getUnflushedFor(segment.segmentId())))
                                                                                                     .filter(t -> !t.right.isEmpty())
                                                                                                     .collect(Collectors.toList())
                                                                                              ));
                                      }
                                  };

                                  SegmentStateTracker currentSegment = null;
                                  int currentSegmentOffset = 0;
                                  for (int i = 0; i < allocationCount; i++)
                                  {
                                      if (i > 0 && i % 50 == 0)
                                      {
                                          for (int j = 0; j < 3; j++)
                                          {
                                              Table table = tables.get(rng.nextInt(tableCount));
                                              table.flush(segments.values());
                                              validateAllSegments.run();
                                          }
                                      }

                                      if (i % 100 == 0)
                                      {
                                          currentSegment = new SegmentStateTracker(segments.size());
                                          currentSegmentOffset = 0;
                                          segments.put(currentSegment.segmentId(), currentSegment);
                                      }

                                      int size = rng.nextInt(100);
                                      Table table = tables.get(rng.nextInt(tableCount));
                                      CommitLogPosition pos = pos(currentSegment.segmentId(), currentSegmentOffset);
                                      table.addAllocation(pos);
                                      currentSegment.markDirty(table.tableId, pos);
                                      currentSegmentOffset += size;
                                      validateAllSegments.run();
                                  }

                                  while (tables.stream().anyMatch(Table::hasUnflushed))
                                  {
                                      Table table = tables.get(rng.nextInt(tableCount));
                                      if (table.hasUnflushed())
                                          table.flush(segments.values());

                                      validateAllSegments.run();
                                  }
                              },
                              e -> {
                                  if (ops != null)
                                  {
                                      System.out.println("History: ");
                                      for (String op : ops)
                                          System.out.println(op);
                                  }
                              });
    }


    public static CommitLogPosition pos(long segmentId, int pos)
    {
        return new CommitLogPosition(segmentId, pos);
    }
}