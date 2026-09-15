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
package org.apache.cassandra.db.partitions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BTreePartitionUpdaterTest
{
    private static TableMetadata metadata;
    private static HeapPool pool;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("ks", "range_merge")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .build();
        pool = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
    }

    @AfterClass
    public static void teardown() throws Exception
    {
        if (pool != null)
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
    }

    @Test
    public void retainedSnapshotsSurviveDisjointAndOverlappingMerges()
    {
        int count = 128;
        PartitionUpdate[] updates = new PartitionUpdate[2 * count];
        for (int i = 0; i < count; ++i)
        {
            updates[i] = update(range(10 * i, true, 10 * i + 6, true, 10, 100));
            updates[count + i] = update(range(10 * i + 2, true, 10 * i + 4, true, 30, 300));
        }

        BTreePartitionData[] snapshots = new BTreePartitionData[updates.length + 1];
        MemtableAllocator allocator = pool.newAllocator("retained-range-snapshots");
        try (OpOrder.Group group = new OpOrder().start())
        {
            mergeAll(updates, snapshots, allocator, group);

            // Read every old version only after all later updates have had a chance to corrupt it.
            for (int i = 0; i <= count; ++i)
                assertSnapshot(snapshots[i], count, i, 0);
            for (int i = 1; i <= count; ++i)
                assertSnapshot(snapshots[count + i], count, count, i);

            // The updater must not mutate its caller's PartitionUpdate either.
            for (int i = 0; i < updates.length; ++i)
            {
                Iterator<RangeTombstone> ranges = updates[i].deletionInfo().rangeIterator(false);
                boolean split = i >= count;
                int start = 10 * (i % count) + (split ? 2 : 0);
                assertRange(range(start, true, start + (split ? 2 : 6), true, split ? 30 : 10, split ? 300 : 100),
                            ranges.next());
                assertFalse(ranges.hasNext());
            }
        }
        finally
        {
            allocator.setDiscarding();
            allocator.setDiscarded();
        }
    }

    private static void mergeAll(PartitionUpdate[] updates, BTreePartitionData[] snapshots,
                                 MemtableAllocator allocator, OpOrder.Group group)
    {
        snapshots[0] = BTreePartitionData.EMPTY;
        for (int i = 0; i < updates.length; ++i)
        {
            BTreePartitionUpdater updater = new BTreePartitionUpdater(allocator, HeapCloner.instance, group, UpdateTransaction.NO_OP);
            snapshots[i + 1] = updater.mergePartitions(snapshots[i], updates[i]);
        }
    }

    private static PartitionUpdate update(RangeTombstone range)
    {
        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, ByteBufferUtil.bytes(0), RegularAndStaticColumns.NONE, 0);
        builder.add(range);
        return builder.build();
    }

    private static void assertSnapshot(BTreePartitionData snapshot, int total, int disjoint, int split)
    {
        List<RangeTombstone> expected = new ArrayList<>();
        for (int i = 0; i < disjoint; ++i)
        {
            int start = 10 * i;
            if (i < split)
            {
                expected.add(range(start, true, start + 2, false, 10, 100));
                expected.add(range(start + 2, true, start + 4, true, 30, 300));
                expected.add(range(start + 4, false, start + 6, true, 10, 100));
            }
            else
            {
                expected.add(range(start, true, start + 6, true, 10, 100));
            }
        }
        DeletionInfo info = snapshot.deletionInfo;
        assertEquals(DeletionTime.LIVE, info.getPartitionDeletion());
        assertEquals(expected.size(), info.rangeCount());
        Iterator<RangeTombstone> forward = info.rangeIterator(false);
        Iterator<RangeTombstone> reverse = info.rangeIterator(true);
        for (int i = 0; i < expected.size(); ++i)
        {
            assertTrue(forward.hasNext());
            assertTrue(reverse.hasNext());
            assertRange(expected.get(i), forward.next());
            assertRange(expected.get(expected.size() - i - 1), reverse.next());
        }
        assertFalse(forward.hasNext());
        assertFalse(reverse.hasNext());
        assertNull(info.rangeCovering(clustering(-1)));
        for (int i = 0; i <= total; ++i)
        {
            for (int offset = 0; offset <= 7; ++offset)
            {
                RangeTombstone actual = info.rangeCovering(clustering(10 * i + offset));
                if (i >= disjoint || offset > 6)
                {
                    assertNull(actual);
                }
                else
                {
                    boolean newer = i < split && offset >= 2 && offset <= 4;
                    assertEquals(DeletionTime.build(newer ? 30 : 10, newer ? 300 : 100), actual.deletionTime());
                    assertTrue(actual.deletionTime().deletes(10));
                    assertEquals(newer, actual.deletionTime().deletes(20));
                    assertFalse(actual.deletionTime().deletes(31));
                }
            }
        }
    }

    private static void assertRange(RangeTombstone expected, RangeTombstone actual)
    {
        assertEquals(0, metadata.comparator.compare(expected.deletedSlice().start(), actual.deletedSlice().start()));
        assertEquals(0, metadata.comparator.compare(expected.deletedSlice().end(), actual.deletedSlice().end()));
        assertEquals(expected.deletionTime(), actual.deletionTime());
    }

    private static Clustering<?> clustering(int value)
    {
        return Clustering.make(ByteBufferUtil.bytes(value));
    }

    private static RangeTombstone range(int start, boolean startInclusive, int end, boolean endInclusive,
                                        long timestamp, long localDeletionTime)
    {
        return new RangeTombstone(Slice.make(ClusteringBound.create(metadata.comparator, true, startInclusive, start),
                                             ClusteringBound.create(metadata.comparator, false, endInclusive, end)),
                                  DeletionTime.build(timestamp, localDeletionTime));
    }
}
