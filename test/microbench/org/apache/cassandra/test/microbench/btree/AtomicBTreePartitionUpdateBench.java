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

package org.apache.cassandra.test.microbench.btree;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntToLongFunction;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.BulkIterator;
import org.apache.cassandra.utils.memory.ByteBufferCloner;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import static java.lang.Long.min;
import static java.lang.Math.max;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@Threads(4)
@State(Scope.Benchmark)
public class AtomicBTreePartitionUpdateBench
{
    private static final OpOrder NO_ORDER = new OpOrder();
    private static final MutableDeletionInfo NO_DELETION_INFO = new MutableDeletionInfo(DeletionTime.LIVE);
    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
    private static final ByteBuffer zero = Int32Type.instance.decompose(0);
    private static final DecoratedKey decoratedKey = new BufferDecoratedKey(new ByteOrderedPartitioner().getToken(zero), zero);

    static
    {
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
    }

    public enum Distribution { RANDOM, SEQUENTIAL }

    final AtomicInteger uniqueThreadInitialisation = new AtomicInteger();

    @Param({"4"})
    int clusteringCount;

    // a value of -1 indicates to send all inserts to a single row,
    // using the clusterings to build CellPath and write our rows into a Map
    @Param({"-1", "1", "8"})
    int columnCount;

    @Param({"0", "0.5"})
    float insertRowOverlap;

    @Param({"1", "32", "256"})
    int insertRowCount;

    @Param({"8", "256"})
    int valueSize;

    @Param({"256"})
    int rolloverAfterInserts;

    @Param({"RANDOM", "SEQUENTIAL"})
    Distribution distribution;

    @Param({"RANDOM", "SEQUENTIAL"})
    Distribution timestamps;

    @Param({"false"})
    boolean uniquePerTrial;

    // hacky way to pass in the number of threads we're executing concurrently in JMH
    @Param({"4"})
    int threadCount;

    @State(Scope.Benchmark)
    public static class GlobalState
    {
        RegularAndStaticColumns partitionColumns;
        TableMetadata metadata;
        ColumnMetadata[] columns;
        Clustering[] clusterings;
        CellPath[] complexPaths;
        ByteBuffer value;

        @Setup(Level.Trial)
        public void setup(AtomicBTreePartitionUpdateBench bench)
        {
            ColumnMetadata[] partitionKeyColumns = bench.partitionKeyColumns();
            ColumnMetadata[] clusteringColumns = bench.clusteringColumns();
            ColumnMetadata[] regularColumns = bench.regularColumns();
            partitionColumns = RegularAndStaticColumns.builder().addAll(Arrays.asList(regularColumns)).build();
            columns = regularColumns;
            metadata = bench.metadata(partitionKeyColumns, clusteringColumns, columns);
            clusterings = bench.clusterings();
            complexPaths = bench.complexPaths(regularColumns);
            value = ByteBuffer.allocate(bench.valueSize);
        }
    }

    // stateful; cannot be shared between threads
    private static class UpdateGenerator
    {
        final Random random;
        final TableMetadata metadata;
        final RegularAndStaticColumns partitionColumns;
        final boolean isComplex;
        final ColumnMetadata[] columns;
        final Clustering[] clusterings;
        final CellPath[] complexPaths;
        final float insertRowOverlap;
        final Distribution distribution;
        final IntToLongFunction timestamps;
        final ByteBuffer value;

        final IntVisitor insertRowCount;
        final Row[] insertBuffer;
        final ColumnData[] columnBuffer;
        final Cell[] complexBuffer;
        int offset;

        UpdateGenerator(GlobalState global, AtomicBTreePartitionUpdateBench bench, long seed)
        {
            this.random = new Random(seed);
            this.metadata = global.metadata;
            this.partitionColumns = global.partitionColumns;
            this.columns = global.columns.clone();
            this.isComplex = this.columns[0].isComplex();
            this.clusterings = global.clusterings.clone();
            this.complexPaths = global.complexPaths.clone();
            this.value = global.value;
            this.insertRowCount = new IntVisitor(bench.insertRowCount);
            this.insertRowOverlap = bench.insertRowOverlap;
            this.distribution = bench.distribution;
            this.timestamps = bench.timestamps == Distribution.RANDOM ? i -> random.nextLong() : i -> i;
            this.insertBuffer = new Row[bench.insertRowCount * 2];
            this.columnBuffer = new ColumnData[columns.length];
            this.complexBuffer = new Cell[complexPaths.length];
        }

        void reset()
        {
            insertRowCount.randomise(random);
            offset = 0;
        }

        final Function<Clustering, Row> simpleRow = this::simpleRow;
        final Function<CellPath, Cell> complexCell = this::complexCell;

        PartitionUpdate next()
        {
            int rowCount;
            if (!isComplex)
            {
                rowCount = selectSortAndTransform(insertBuffer, clusterings, metadata.comparator, simpleRow);
            }
            else
            {
                rowCount = 1;
                insertBuffer[0] = complexRow();
            }
            try (BulkIterator<Row> iter = BulkIterator.of(insertBuffer))
            {
                Object[] tree = BTree.build(iter, rowCount, UpdateFunction.noOp());
                return PartitionUpdate.unsafeConstruct(metadata, decoratedKey, BTreePartitionData.unsafeConstruct(partitionColumns, tree, DeletionInfo.LIVE, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS), NO_DELETION_INFO, false);
            }
        }

        private <I, O> int selectSortAndTransform(O[] out, I[] in, Comparator<? super I> comparator, Function<I, O> transform)
        {
            int prevRowCount = offset == 0 ? 0 : insertRowCount.cur();
            int rowCount = insertRowCount.next();
            switch (distribution)
            {
                case SEQUENTIAL:
                {
                    for (int i = 0 ; i < rowCount ; ++i)
                        out[i] = transform.apply(in[i + offset]);
                    offset += rowCount * (1f - insertRowOverlap);
                    break;
                }
                case RANDOM:
                {
                    int rowOverlap = (int) (insertRowOverlap * min(rowCount, prevRowCount));
                    shuffle(random, in, 0, rowOverlap, 0, prevRowCount);
                    shuffle(random, in, rowOverlap, rowCount - rowOverlap, prevRowCount, in.length - prevRowCount);
                    Arrays.sort(in, 0, rowCount, comparator);
                    for (int i = 0 ; i < rowCount ; ++i)
                        out[i] = transform.apply(in[i]);
                    ++offset;
                    break;
                }
            }
            return rowCount;
        }

        Row simpleRow(Clustering clustering)
        {
            for (int i = 0 ; i < columns.length ; ++i)
                columnBuffer[i] = cell(columns[i], null);
            return bufferToRow(clustering);
        }

        Row complexRow()
        {
            int mapCount = selectSortAndTransform(complexBuffer, complexPaths, columns[0].cellPathComparator(), complexCell);
            try (BulkIterator<ColumnData> iter = BulkIterator.of(complexBuffer))
            {
                columnBuffer[0] = ComplexColumnData.unsafeConstruct(columns[0], BTree.build(iter, mapCount, UpdateFunction.noOp()), DeletionTime.LIVE);
            }
            return bufferToRow(clusterings[0]);
        }

        Row bufferToRow(Clustering clustering)
        {
            try (BulkIterator<ColumnData> iter = BulkIterator.of(columnBuffer))
            {
                return BTreeRow.create(clustering, LivenessInfo.EMPTY, Row.Deletion.LIVE, BTree.build(iter, columns.length, UpdateFunction.noOp()));
            }
        }

        Cell simpleCell(ColumnMetadata column)
        {
            return cell(column, null);
        }

        Cell complexCell(CellPath path)
        {
            return cell(columns[0], path);
        }

        Cell cell(ColumnMetadata column, CellPath path)
        {
            return new BufferCell(column, timestamps.applyAsLong(offset), Cell.NO_TTL, Cell.NO_DELETION_TIME, value, path);
        }
    }

    private static class Batch
    {
        final AtomicBTreePartition update;
        final PartitionUpdate[] insert;
        // low 20 bits contain the next insert we're performing this generation
        // next 20 bits are inserts we've performed this generation
        // next 24 bits are generation (i.e. number of times we've run this update)
        final AtomicLong state = new AtomicLong();
        final AtomicLong activeThreads = new AtomicLong();
        final MemtableAllocator allocator;
        final Cloner cloner;
        final int waitForActiveThreads;

        /** Signals to replace the reference in {@code update} after this many invocations of {@code allocator.allocate} */
        private int invalidateOn;

        public Batch(int threads, TableMetadata metadata, UpdateGenerator generator, int rolloverAfterInserts)
        {
            waitForActiveThreads = threads;
            allocator = new HeapPool.Allocator(POOL)
            {
                public Cloner cloner(OpOrder.Group opGroup)
                {
                    return new ByteBufferCloner()
                    {
                        @Override
                        public ByteBuffer allocate(int size)
                        {
                            if (invalidateOn > 0 && --invalidateOn == 0)
                                BTreePartitionData.unsafeInvalidate(update);
                            return ByteBuffer.allocate(size);
                        }
                    };
                }
            };
            update = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), decoratedKey, allocator);
            cloner = allocator.cloner(NO_ORDER.getCurrent());

            generator.reset();
            insert = IntStream.range(0, rolloverAfterInserts).mapToObj(i -> generator.next()).toArray(PartitionUpdate[]::new);
        }

        boolean performOne(int ifGeneration, Consumer<Batch> invokeBefore)
        {
            int index;
            ifGeneration &= 0xffffff;
            while (true)
            {
                long cur = state.get();
                int curGeneration = ((int) (cur >>> 40)) & 0xffffff;
                if (curGeneration == ifGeneration)
                {
                    index = ((int) cur) & 0xfffff;
                    if (index == this.insert.length)
                        return false;

                    if (state.compareAndSet(cur, cur + 1)) break;
                    else continue;
                }

                if (ifGeneration < curGeneration)
                    return false;

                // should never really happen
                Thread.yield();
            }

            try
            {
                if (activeThreads.get() < waitForActiveThreads)
                {
                    // try to prevent threads scattering to the four winds and updating different partitions
                    // we don't wait forever, as we can't synchronise with JMH to ensure all threads stop together
                    activeThreads.incrementAndGet();
                    long start = System.nanoTime();
                    while (activeThreads.get() < waitForActiveThreads && TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 50)
                        ThreadLocalRandom.current().nextLong();
                }
                invokeBefore.accept(this);
                update.addAll(insert[index], cloner, NO_ORDER.getCurrent(), UpdateTransaction.NO_OP);
                return true;
            }
            finally
            {
                if (state.addAndGet(0x100000L) == ((((long)ifGeneration) << 40) | (((long)insert.length) << 20) | insert.length))
                {
                    activeThreads.set(0);
                    update.unsafeSetHolder(BTreePartitionData.unsafeGetEmpty());
                    // reset the state and rollover the generation
                    state.set((ifGeneration + 1L) << 40);
                }
            }
        }
    }

    public static class Batches
    {
        Batch[] batches;

        void setup(int threadsForBatch, int threadsForTest, AtomicBTreePartitionUpdateBench bench, GlobalState global)
        {
            UpdateGenerator generator = new UpdateGenerator(global, bench, bench.uniqueThreadInitialisation.incrementAndGet());

            int batchCount = Math.min(256, max(1, (int) (Runtime.getRuntime().maxMemory() / (2 * ((threadsForTest + threadsForBatch - 1) / threadsForBatch) * bench.sizeInBytesOfOneBatch()))));
            batches = IntStream.range(0, batchCount).mapToObj(i -> new Batch(threadsForBatch, global.metadata, generator, bench.rolloverAfterInserts))
                               .toArray(Batch[]::new);
        }

        Batch get(int i)
        {
            return batches[i % batches.length];
        }
    }

    @State(Scope.Thread)
    public static class SingleThreadUpdates extends Batches
    {
        int i;
        int gen;
        @Setup(Level.Trial)
        public void setup(AtomicBTreePartitionUpdateBench bench, GlobalState global)
        {
            super.setup(1, bench.threadCount, bench, global);
        }

        void performOne(Consumer<Batch> invokeFirst)
        {
            while (true)
            {
                if (get(i).performOne(gen, invokeFirst))
                    break;
                if (++i == batches.length) { i = 0; ++gen; }
            }
        }
    }

    @State(Scope.Benchmark)
    public static class ConcurrentUpdates extends Batches
    {
        final AtomicLong state = new AtomicLong();
        @Setup(Level.Trial)
        public void setup(AtomicBTreePartitionUpdateBench bench, GlobalState global)
        {
            super.setup(bench.threadCount, bench.threadCount, bench, global);
        }

        void performOne(Consumer<Batch> invokeFirst)
        {
            while (true)
            {
                long cur = state.get();
                int generation = (int) (cur >>> 32);
                int index = (int) cur;
                if (get(index).performOne(generation, invokeFirst))
                    break;
                state.compareAndSet(cur, (cur & 0xffffffffL) == batches.length ? ((cur & 0xffffffff00000000L) + 0x100000000L) : cur + 1);
            }
        }
    }

    @Benchmark
    public void success(SingleThreadUpdates updates)
    {
        updates.performOne(batch -> {});
    }

    @Benchmark
    public void oneFailure(SingleThreadUpdates updates)
    {
        updates.performOne(batch -> batch.invalidateOn = ThreadLocalRandom.current().nextInt(insertRowCount * max(1, columnCount)));
    }

    @Benchmark
    public void concurrent(ConcurrentUpdates updates)
    {
        updates.performOne(batch -> {});
    }

    private int sizeInBytesOfOneBatch()
    {
        // 50 bytes ~= size of a Cell
        return 50 * max(columnCount, 1) * (insertRowCount + insertRowCount/2) * rolloverAfterInserts;
    }

    private ColumnMetadata[] regularColumns()
    {
        if (columnCount >= 0)
            return regularColumns(BytesType.instance, columnCount);

        AbstractType[] types = new AbstractType[clusteringCount];
        Arrays.fill(types, BytesType.instance);
        return regularColumns(MapType.getInstance(CompositeType.getInstance(types), BytesType.instance, true), 1);
    }

    private ColumnMetadata[] partitionKeyColumns()
    {
        return columns(Int32Type.instance, ColumnMetadata.Kind.PARTITION_KEY, 1, "pk");
    }

    private ColumnMetadata[] clusteringColumns()
    {
        return columns(Int32Type.instance, ColumnMetadata.Kind.CLUSTERING, clusteringCount, "c");
    }

    private TableMetadata metadata(ColumnMetadata[] partitionKeyColumns, ColumnMetadata[] clusteringColumns, ColumnMetadata[] regularColumns)
    {
        List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                                                    .add(partitionKeyColumns)
                                                    .add(clusteringColumns)
                                                    .add(regularColumns)
                                                    .build();

        return TableMetadata.builder("", "")
                            .id(TableId.fromUUID(UUID.randomUUID()))
                            .addColumns(columns)
                            .partitioner(ByteOrderedPartitioner.instance)
                            .build();
    }

    private int uniqueRowCount()
    {
        return (int) (insertRowCount * (1 + (rolloverAfterInserts * (1f - insertRowOverlap))));
    }

    private Clustering[] clusterings()
    {
        Clustering<ByteBuffer> prefix = Clustering.make(IntStream.range(0, clusteringCount - 1).mapToObj(i -> zero).toArray(ByteBuffer[]::new));
        int rowCount = columnCount >= 0 ? uniqueRowCount() : 1;
        return clusterings(rowCount, prefix);
    }

    private CellPath[] complexPaths(ColumnMetadata[] columns)
    {
        if (columnCount >= 0)
            return new CellPath[0];

        Clustering<ByteBuffer> prefix = Clustering.make(IntStream.range(0, clusteringCount - 1).mapToObj(i -> zero).toArray(ByteBuffer[]::new));
        return complexPaths((CompositeType) ((MapType)columns[0].type).getKeysType(), uniqueRowCount(), prefix);
    }

    private static ColumnMetadata[] regularColumns(AbstractType<?> type, int count)
    {
        return columns(type, ColumnMetadata.Kind.REGULAR, count, "v");
    }

    private static ColumnMetadata[] columns(AbstractType<?> type, ColumnMetadata.Kind kind, int count, String prefix)
    {
        return IntStream.range(0, count)
                        .mapToObj(i -> new ColumnMetadata("",
                                                          "",
                                                          new ColumnIdentifier(prefix + i, true),
                                                          type,
                                                          kind != ColumnMetadata.Kind.REGULAR ? i : ColumnMetadata.NO_POSITION,
                                                          kind,
                                                          null))
                        .toArray(ColumnMetadata[]::new);
    }

    private static Clustering[] clusterings(int rowCount, Clustering<ByteBuffer> prefix)
    {
        return IntStream.range(0, rowCount)
                        .mapToObj(i -> {
                            ByteBuffer[] values = Arrays.copyOf(prefix.getBufferArray(), prefix.size() + 1);
                            values[prefix.size()] = Int32Type.instance.decompose(i);
                            return Clustering.make(values);
                        }).toArray(Clustering[]::new);
    }

    private static CellPath[] complexPaths(CompositeType type, int pathCount, Clustering<ByteBuffer> prefix)
    {
        return IntStream.range(0, pathCount)
                        .mapToObj(i -> {
                            Object[] values = Arrays.copyOf(prefix.getRawValues(), prefix.size() + 1);
                            values[prefix.size()] = Int32Type.instance.decompose(i);
                            return CellPath.create(type.decompose(values));
                        }).toArray(CellPath[]::new);
    }

    private static <T> void shuffleAndSort(Random random, T[] data, int size, Comparator<T> comparator)
    {
        shuffle(random, data, size);
        Arrays.sort(data, 0, size, comparator);
    }

    private static void shuffle(Random random, Object[] data, int size)
    {
        shuffle(random, data, 0, size, 0, data.length);
    }

    private static void shuffle(Random random, Object[] data, int trgOffset, int trgSize, int srcOffset, int srcSize)
    {
        for (int i = 0 ; i < trgSize ; ++i)
        {
            int swap = srcOffset + srcSize == 0 ? 0 : random.nextInt(srcSize);
            Object tmp = data[swap];
            data[swap] = data[i + trgOffset];
            data[i + trgOffset] = tmp;
        }
    }
}