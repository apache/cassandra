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

package org.apache.cassandra.test.microbench;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.BTreePartitionUpdater;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 * Cost of merging one more range tombstone into an unflushed partition that already holds {@code existing}
 * range tombstones, which is what every memtable write carrying a range tombstone pays
 * (BTreePartitionUpdater.mergePartitions -> DeletionInfo.mutableCopy -> RangeTombstoneList.copy/add).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class RangeTombstoneMergeBench
{
    @Param({ "16", "256", "4096", "65536" })
    int existing;

    private TableMetadata metadata;
    private HeapPool pool;
    private MemtableAllocator allocator;
    private OpOrder.Group group;
    private BTreePartitionData partition;
    private PartitionUpdate disjointAfterAll;
    private PartitionUpdate overlappingMiddle;
    private PartitionUpdate overlappingHalf;
    private PartitionUpdate[] sequence;

    @Setup
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("ks", "range_merge_bench")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .build();
        pool = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
        allocator = pool.newAllocator("range-merge-bench");
        group = new OpOrder().start();

        sequence = new PartitionUpdate[existing];
        for (int i = 0; i < existing; i++)
            sequence[i] = update(range(10 * i, 10 * i + 6, 10));
        partition = BTreePartitionData.EMPTY;
        for (PartitionUpdate update : sequence)
            partition = merge(partition, update);

        disjointAfterAll = update(range(10 * existing, 10 * existing + 6, 10));
        int middle = 10 * (existing / 2);
        overlappingMiddle = update(range(middle + 2, middle + 4, 30));
        // A newer tombstone superseding the middle half of the existing intervals.
        overlappingHalf = update(range(10 * (existing / 4) + 2, 10 * (3 * existing / 4) + 4, 30));
    }

    @TearDown
    public void teardown() throws InterruptedException, TimeoutException
    {
        group.close();
        allocator.setDiscarding();
        allocator.setDiscarded();
        pool.shutdownAndWait(1, TimeUnit.MINUTES);
    }

    /** One disjoint tombstone appended after {@code existing} others: the ticket's sorted-append case. */
    @Benchmark
    public BTreePartitionData mergeDisjointIntoExisting()
    {
        return merge(partition, disjointAfterAll);
    }

    /** One tombstone splitting an existing interval in the middle of the partition. */
    @Benchmark
    public BTreePartitionData mergeOverlappingIntoExisting()
    {
        return merge(partition, overlappingMiddle);
    }

    /** One newer tombstone covering half of the existing intervals: the superseding wide-delete case. */
    @Benchmark
    public BTreePartitionData mergeWideOverlapIntoExisting()
    {
        return merge(partition, overlappingHalf);
    }

    /** {@code existing} disjoint tombstones merged one by one into an empty partition. */
    @Benchmark
    public BTreePartitionData mergeSequenceFromEmpty()
    {
        BTreePartitionData current = BTreePartitionData.EMPTY;
        for (PartitionUpdate update : sequence)
            current = merge(current, update);
        return current;
    }

    private BTreePartitionData merge(BTreePartitionData current, PartitionUpdate update)
    {
        return new BTreePartitionUpdater(allocator, HeapCloner.instance, group, UpdateTransaction.NO_OP).mergePartitions(current, update);
    }

    private PartitionUpdate update(RangeTombstone range)
    {
        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, ByteBufferUtil.bytes(0), RegularAndStaticColumns.NONE, 0);
        builder.add(range);
        return builder.build();
    }

    private RangeTombstone range(int start, int end, long timestamp)
    {
        return new RangeTombstone(Slice.make(ClusteringBound.create(metadata.comparator, true, true, start),
                                             ClusteringBound.create(metadata.comparator, false, true, end)),
                                  DeletionTime.build(timestamp, 100));
    }
}
