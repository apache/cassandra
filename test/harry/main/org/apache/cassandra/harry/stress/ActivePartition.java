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

package org.apache.cassandra.harry.stress;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import accord.utils.Invariants;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.IndexedValueGenerators;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.IndexGenerators;
import org.apache.cassandra.harry.gen.InvertibleGenerator;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.gen.rng.PureRng;
import org.apache.cassandra.harry.gen.rng.SeedableEntropySource;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.stress.distribution.Distribution;
import org.apache.cassandra.harry.util.IteratorsUtil;
import org.apache.cassandra.utils.LazyToString;

import static org.apache.cassandra.harry.SchemaSpec.cumulativeEntropy;
import static org.apache.cassandra.harry.SchemaSpec.forKeys;
import static org.apache.cassandra.harry.dsl.HistoryBuilder.keyComparator;
import static org.apache.cassandra.harry.dsl.IndexedValueGenerators.IndexedPartitionValues;
import static org.apache.cassandra.harry.gen.InvertibleGenerator.fromType;

/**
 *
 */
public final class ActivePartition extends IndexedPartitionValues
{
    /**
     * A helper class to convert between descriptors and indices _for partitions_
     */
    public enum DescriptorIndexBijection
    {
        INSTANCE;

        private final long stream = 0xffeeddccbbaal;
        private final PureRng rng = new PureRng.PCGFast(0xaabbccddeeffl);

        public long toIdx(long pd)
        {
            return rng.sequenceNumber(pd, stream);
        }

        public long toPd(long idx)
        {
            return rng.randomNumber(idx, stream);
        }

    }

    public final long idx;
    public final long pd;

    // TODO: document why two
    private final HistoryBuilder.IndexedBijection<Object[]> cachingPkGen;
    private final HistoryBuilder.IndexedBijection<Object[]> rawGen;
    private final AtomicInteger refCount = new AtomicInteger(0);

    private final LongConsumer cleanup;

    ActivePartition(long pkIdx,
                    long pd,
                    HistoryBuilder.IndexedBijection<Object[]> cachingPkGen,
                    HistoryBuilder.IndexedBijection<Object[]> rawGen,
                    HistoryBuilder.IndexedBijection<Object[]> ckGen,
                    List<HistoryBuilder.IndexedBijection<Object>> regularColumnGens,
                    List<HistoryBuilder.IndexedBijection<Object>> staticColumnGens,
                    List<Comparator<Object>> pkComparators,
                    List<Comparator<Object>> ckComparators,
                    List<Comparator<Object>> regularComparators,
                    List<Comparator<Object>> staticComparators,
                    LongConsumer cleanup)
    {
        super(ckGen, regularColumnGens, staticColumnGens, ckComparators, regularComparators, staticComparators,
              IndexGenerators.uniform(ckGen),
              IndexGenerators.uniform(regularColumnGens),
              IndexGenerators.uniform(regularColumnGens));

        this.cachingPkGen = cachingPkGen;
        this.rawGen = rawGen;
        Invariants.require(DescriptorIndexBijection.INSTANCE.toIdx(pd) == pkIdx);
        Invariants.require(DescriptorIndexBijection.INSTANCE.toPd(pkIdx) == pd);
        this.idx = pkIdx;
        this.pd = pd;

        this.cleanup = v -> {
            cleanup.accept(v);
            ckGen.discard();
        };
    }

    public HistoryBuilder.IndexedBijection<Object[]> pkGen()
    {
        return cachingPkGen;
    }

    private static class ObjectWrapper
    {
        public final Object[] value;

        private ObjectWrapper(Object[] value)
        {
            this.value = value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            ObjectWrapper that = (ObjectWrapper) o;
            return Objects.deepEquals(value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(value);
        }
    }

    public void ref()
    {
        refCount.incrementAndGet();
    }

    public void deref()
    {
        int v = refCount.decrementAndGet();
        Invariants.require(v >= 0);
        if (v == 0)
            cleanup.accept(pd);
    }

    /**
     * It is easiest to generate an operation for a given partition based knowing what values partition may
     * potentially hold.
     *
     * This class is _not_ thread safe
     */
    public static class Partitions extends IndexedValueGenerators
    {
        final SchemaSpec schema;
        final Distribution rowPopulation;
        final VisitGenerator.ColumnPopulation columnPopulation;
        final List<ActivePartition> activePartitions;
        final VisitedPartitions visitedPartitions;
        final Map<Long, ActivePartition> partitionCache = new ConcurrentHashMap<>();
        final AtomicLong nextPartitionIdx = new AtomicLong();
        final RotationStrategy rotationStrategy;

        final long minPartitionIdx;
        final long maxPartitionIdx;
        final long initialLts;

        LongConsumer onRemove;

        public Partitions(SchemaSpec schema,
                          Distribution rowPopulation,
                          VisitGenerator.ColumnPopulation columnPopulation,
                          RotationStrategy rotationStrategy)
        {
            this(schema, rowPopulation, columnPopulation, rotationStrategy, 0, Long.MAX_VALUE, 0);
        }

        public Partitions(SchemaSpec schema,
                          Distribution rowPopulation,
                          VisitGenerator.ColumnPopulation columnPopulation,
                          RotationStrategy rotationStrategy,
                          long minPartitionIdx,
                          long maxPartitionIdx)
        {
            this(schema, rowPopulation, columnPopulation, rotationStrategy, minPartitionIdx, maxPartitionIdx, 0);
        }

        public Partitions(SchemaSpec schema,
                          Distribution rowPopulation,
                          VisitGenerator.ColumnPopulation columnPopulation,
                          RotationStrategy rotationStrategy,
                          long minPartitionIdx,
                          long maxPartitionIdx,
                          long initialLts)
        {
            super(new PartitionKeyGen(schema));
            Invariants.require(minPartitionIdx >= 0, "minPartitionIdx must be non-negative: %d", minPartitionIdx);
            Invariants.require(maxPartitionIdx > minPartitionIdx, "maxPartitionIdx must be greater than minPartitionIdx: %d > %d", maxPartitionIdx, minPartitionIdx);
            this.schema = schema;
            this.rowPopulation = rowPopulation;
            this.columnPopulation = columnPopulation;
            this.activePartitions = new ArrayList<>(rotationStrategy.targetSize());
            this.minPartitionIdx = minPartitionIdx;
            this.maxPartitionIdx = maxPartitionIdx;
            this.initialLts = initialLts;
            this.nextPartitionIdx.set(minPartitionIdx);
            this.visitedPartitions = new VisitedPartitions(minPartitionIdx);
            this.onRemove = (pd_) -> {
                Invariants.nonNull(partitionCache.remove(pd_));
                pkGenInternal().cleanup(pd_);
                long pdIdx = DescriptorIndexBijection.INSTANCE.toIdx(pd_);
                visitedPartitions.add(pdIdx);
            };
            this.rotationStrategy = rotationStrategy;
        }

        /**
         * Populates the active partitions by replaying all partition switches from LTS 0 up to
         * {@code initialLts}, so that the active partitions and visited partitions are in the
         * correct state for resuming from that LTS.
         *
         * When {@code initialLts} is 0, this simply creates the initial set of active partitions.
         *
         * During replay, we track only partition indices without creating full
         * {@link ActivePartition} state. The actual partition objects are only created at the
         * end, once the final set of active partition indices is known.
         */
        public void populate()
        {
            List<Long> activeIdxs = new ArrayList<>(rotationStrategy.targetSize());

            for (int i = 0; i < rotationStrategy.targetSize(); i++)
                activeIdxs.add(advanceNextPartitionIdx());

            for (long lts = 0; lts < initialLts; lts++)
            {
                if (!rotationStrategy.shouldSwitch(lts))
                    continue;

                applyActions(lts,
                             activeIdxs::size,
                             this::advanceNextPartitionIdx,
                             (pos, newIdx) -> { visitedPartitions.add(activeIdxs.get(pos)); activeIdxs.set(pos, newIdx); },
                             (visitedPd) -> activeIdxs.contains(DescriptorIndexBijection.INSTANCE.toIdx(visitedPd)),
                             (pos, visitedIdx) -> { visitedPartitions.add(activeIdxs.get(pos)); activeIdxs.set(pos, visitedIdx); },
                             pos -> DescriptorIndexBijection.INSTANCE.toPd(activeIdxs.get(pos)),
                             action -> {});
            }

            // Now inflate all the active partition objects from the final set of indices
            for (long idx : activeIdxs)
            {
                ActivePartition activePartition = byIdx(idx);
                activePartition.ref();
                partitionCache.put(activePartition.pd, activePartition);
                activePartitions.add(activePartition);
            }
        }

        private long advanceNextPartitionIdx()
        {
            long idx = nextPartitionIdx.getAndIncrement();
            Invariants.require(idx < maxPartitionIdx, "Exhausted partition index space: %d >= %d", idx, maxPartitionIdx);
            return idx;
        }

        private void applyActions(long lts,
                                  IntSupplier activeSize,
                                  LongSupplier createNew,
                                  BiConsumer<Integer, Long> replaceWithNew,
                                  LongPredicate activePd,
                                  BiConsumer<Integer, Long> replaceWithVisited,
                                  java.util.function.IntToLongFunction pdAtPosition,
                                  Consumer<RotationStrategy.PartitionAction> onAction)
        {
            RotationStrategy.PartitionAction[] actions = SeedableEntropySource.computeWithSeed(lts, rotationStrategy::generate);

            for (int i = 0; i < actions.length; i++)
            {
                RotationStrategy.PartitionAction action = actions[i];
                int size = activeSize.getAsInt();
                switch (action)
                {
                    case REPLACE_WITH_NEW:
                    {
                        if (size == 0)
                            continue;
                        int remove = SeedableEntropySource.computeWithSeed(Util.hash(lts, i), rng -> rng.nextInt(size));
                        // Skip eviction with probability proportional to log2 of partition size
                        long candidatePd = pdAtPosition.applyAsLong(remove);
                        int partitionSize = Math.toIntExact(rowPopulation.next(candidatePd));
                        boolean evict = SeedableEntropySource.computeWithSeed(Util.hash(lts, i),
                                                                              rng -> rng.nextInt(Math.max(1, Integer.highestOneBit(partitionSize))) == 0);
                        if (!evict)
                            continue;
                        long newIdx = createNew.getAsLong();
                        replaceWithNew.accept(remove, newIdx);
                        break;
                    }
                    case REPLACE_WITH_VISITED:
                    {
                        if (size == 0)
                            continue;
                        int remove = SeedableEntropySource.computeWithSeed(Util.hash(lts, i), rng -> rng.nextInt(size));
                        // Skip eviction with probability proportional to log2 of partition size
                        long candidatePd = pdAtPosition.applyAsLong(remove);
                        int partitionSize = Math.toIntExact(rowPopulation.next(candidatePd));
                        boolean evict = SeedableEntropySource.computeWithSeed(Util.hash(lts, i),
                                                                              rng -> rng.nextInt(Math.max(1, Integer.highestOneBit(partitionSize))) == 0);
                        if (!evict)
                            continue;
                        long visitedIdx = visitedPartitions.getBySeed(Util.hash(lts, i));
                        long visitedPd = visitedIdx < 0 ? -1 : DescriptorIndexBijection.INSTANCE.toPd(visitedIdx);
                        if (visitedPd < 0 || activePd.test(visitedPd))
                        {
                            // No visited partitions available or picked one is still active; fall back to new
                            long newIdx = createNew.getAsLong();
                            replaceWithNew.accept(remove, newIdx);
                        }
                        else
                        {
                            replaceWithVisited.accept(remove, visitedIdx);
                        }
                        break;
                    }
                }
                onAction.accept(action);
            }
        }

        /**
         * Add a callback to be triggered when partition is phased out.
         *
         * TODO (consider): This might start racing when we allow adding partitions back.
         */
        public void onRemove(LongConsumer consumer)
        {
            LongConsumer prev = this.onRemove;
            this.onRemove = pd -> {
                prev.accept(pd);
                consumer.accept(pd);
            };
        }

        // TODO: biased partition picker
        public ActivePartition pick(EntropySource entropySource)
        {
            ActivePartition picked = activePartitions.get(entropySource.nextInt(activePartitions.size()));
            Invariants.require(picked.refCount.get() > 0);
            Invariants.require(partitionCache.containsKey(picked.pd));
            return picked;
        }

        @Override
        public Generator<Long> pkIdxGen()
        {
            throw new UnsupportedOperationException();
        }

        private PartitionKeyGen pkGenInternal()
        {
            return (PartitionKeyGen) super.pkGen();
        }

        private ActivePartition byIdx(long idx)
        {
            long pd = DescriptorIndexBijection.INSTANCE.toPd(idx);
            ActivePartition partition = createActivePartition(idx, pd, schema, rowPopulation, columnPopulation, (HistoryBuilder.IndexedBijection<Object[]>) pkGen, onRemove);
            pkGenInternal().ensure(pd, partition.rawGen::inflate);
            return partition;
        }

        @Override
        public ActivePartition forPd(long pd)
        {
            return Invariants.nonNull(partitionCache.get(pd));
        }

        public void maybeSwitchPartition(long lts, Consumer<RotationStrategy.PartitionAction> consumer)
        {
            if (!rotationStrategy.shouldSwitch(lts))
                return;

            applyActions(lts,
                         activePartitions::size,
                         () -> {
                             long idx = advanceNextPartitionIdx();
                             ActivePartition ap = byIdx(idx);
                             ap.ref();
                             partitionCache.put(ap.pd, ap);
                             return idx;
                         },
                         (pos, newIdx) -> {
                             ActivePartition next = Invariants.nonNull(partitionCache.get(DescriptorIndexBijection.INSTANCE.toPd(newIdx)));
                             activePartitions.set(pos, next).deref();
                         },
                         partitionCache::containsKey,
                         (pos, visitedIdx) -> {
                             ActivePartition next = byIdx(visitedIdx);
                             next.ref();
                             partitionCache.put(next.pd, next);
                             activePartitions.set(pos, next).deref();
                         },
                         pos -> activePartitions.get(pos).pd,
                         consumer);
        }
    }

    public static class PartitionKeyGen implements HistoryBuilder.IndexedBijection<Object[]>
    {
        final SchemaSpec schema;

        final Map<ObjectWrapper, Long> deflate = new HashMap<>();
        final Map<Long, Object[]> inflate = new HashMap<>();

        public PartitionKeyGen(SchemaSpec schema)
        {
            this.schema = schema;
        }

        public void cleanup(long pd)
        {
            Object[] values = Invariants.nonNull(inflate.remove(pd));
            Invariants.nonNull(deflate.remove(new ObjectWrapper(values)));
        }

        public void ensure(long pd, LongFunction<Object[]> value)
        {
            if (!inflate.containsKey(pd))
            {
                // TODO: need to extract pkgen from inside value descriptors
                Object[] values = value.apply(pd);
                inflate.put(pd, values);
                deflate.put(new ObjectWrapper(values), pd);
            }
        }

        @Override
        public Object[] inflate(long pd)
        {
            return Invariants.nonNull(inflate.get(pd));
        }

        @Override
        public long deflate(Object[] value)
        {
            return Invariants.nonNull(deflate.get(new ObjectWrapper(value)),
                                      "Could not find deflate ", LazyToString.lazy(() -> Arrays.toString(value)));
        }

        @Override
        public int byteSize()
        {
            return 0;
        }

        @Override
        public int compare(long l, long r)
        {
            return 0;
        }

        @Override
        public long idxFor(long pd)
        {
            return DescriptorIndexBijection.INSTANCE.toIdx(pd);
        }

        @Override
        public long descriptorAt(long idx)
        {
            return DescriptorIndexBijection.INSTANCE.toPd(idx);
        }
    }

    /**
     * For _any_ partition, its characteristics are deterministic and depend on its pd. In other words, over the lifetime
     * of partition, its max number of rows (and, therefore, possible values for its rows), _can not_ be changed. However,
     * partition can get rotated in and out from active set at any point in time.
     */
    @SuppressWarnings("unchecked")
    public static ActivePartition createActivePartition(long idx,
                                                        long pd,
                                                        SchemaSpec schema,
                                                        Distribution rowPopulation,
                                                        VisitGenerator.ColumnPopulation population,
                                                        HistoryBuilder.IndexedBijection<Object[]> cachingPkGen,
                                                        LongConsumer cleanup)
    {
        List<Comparator<Object>> pkComparators = new ArrayList<>();
        List<Comparator<Object>> ckComparators = new ArrayList<>();
        List<Comparator<Object>> regularComparators = new ArrayList<>();
        List<Comparator<Object>> staticComparators = new ArrayList<>();

        EntropySource rng = new JdkRandomEntropySource(pd);
        for (int i = 0; i < schema.partitionKeys.size(); i++)
            pkComparators.add((Comparator<Object>) schema.partitionKeys.get(i).type.comparator());
        for (int i = 0; i < schema.clusteringKeys.size(); i++)
            ckComparators.add((Comparator<Object>) schema.clusteringKeys.get(i).type.comparator());
        for (int i = 0; i < schema.regularColumns.size(); i++)
            regularComparators.add((Comparator<Object>) schema.regularColumns.get(i).type.comparator());
        for (int i = 0; i < schema.staticColumns.size(); i++)
            staticComparators.add((Comparator<Object>) schema.staticColumns.get(i).type.comparator());

        Map<ColumnSpec<?>, HistoryBuilder.IndexedBijection<Object>> map = new HashMap<>();
        for (ColumnSpec<?> column : IteratorsUtil.concat(schema.regularColumns, schema.staticColumns))
        {
            int populationPerColumn = Math.toIntExact(population.distribution(column.name).next(pd));
            map.computeIfAbsent(column, (a) -> (HistoryBuilder.IndexedBijection<Object>) fromType(rng, populationPerColumn, column));
        }

        // As of now, we allow only single partition queries, and within the scope of the visit we can select
        // values only from one partition, so we simply create an identity PK bijection to avoid lookups altogether.
        HistoryBuilder.IndexedBijection<Object[]> rawPkGen = new HistoryBuilder.IndexedBijection<Object[]>() {
            private Object[] value = null;

            @Override
            public Object[] inflate(long descriptor) {
                Invariants.require(pd == descriptor, "Partition descriptor mismatch: %d != %d", pd, descriptor);
                return ensureValue();
            }

            private Object[] ensureValue()
            {
                if (value == null)
                    value = SeedableEntropySource.computeWithSeed(pd, forKeys(schema.partitionKeys)::generate);
                return value;
            }
            @Override
            public long deflate(Object[] value) {
                Invariants.require(Arrays.equals(value, ensureValue()), "Partition key mismatch, %s != %s", ensureValue(), value);
                // TODO (required): allow selecting only a subset of PK and CK
                return pd;
            }

            @Override
            public int byteSize() {
                return Long.BYTES;
            }

            @Override
            public int compare(long l, long r) {
                throw new IllegalStateException("Not implemented");
            }

            @Override
            public long idxFor(long pd) {
                return DescriptorIndexBijection.INSTANCE.toIdx(pd);
            }

            @Override
            public long descriptorAt(long idx) {
                return DescriptorIndexBijection.INSTANCE.toPd(idx);
            }
        };

        // TODO (required): at the moment, we generate the values for clusterings by pre-generating a set number of values, which
        //                  doesn't give us enough control over the possible values. What we need to do instead is to allow
        //                  generating a set number of values _per column_. For example, if ck1 has 5 unique value, and ck2 has
        //                  5 unique values, for each ck1 we will have a value of ck2, so the number of possible values grows
        //                  combinatorically.
        int combinations = Math.toIntExact(rowPopulation.next(pd));
        HistoryBuilder.IndexedBijection<Object[]> ckGenerator = new InvertibleGenerator<>(rng,
                                                                                          cumulativeEntropy(schema.clusteringKeys),
                                                                                          combinations,
                                                                                          forKeys(schema.clusteringKeys),
                                                                                          keyComparator(schema.clusteringKeys));

        return new ActivePartition(idx,
                                   pd,
                                   cachingPkGen,
                                   rawPkGen,
                                   ckGenerator,
                                   schema.regularColumns.stream()
                                                        .map(map::get)
                                                        .collect(Collectors.toList()),
                                   schema.staticColumns.stream()
                                                       .map(map::get)
                                                       .collect(Collectors.toList()),
                                   pkComparators,
                                   ckComparators,
                                   regularComparators,
                                   staticComparators,
                                   cleanup);
    }

    /**
     * Tracks which partition indices have been visited. Maintains a contiguous range [minIdx, highIdxWatermark]
     * and a min-heap of visited indices above the watermark. When indices added to the heap become consecutive
     * with the watermark, the watermark is advanced.
     *
     * {@code getBySeed} picks a visited partition index uniformly at random using a deterministic seed,
     * or returns -1 if no partitions have been visited.
     */
    public static class VisitedPartitions
    {
        private final long minIdx;
        private long highIdxWatermark;
        private long[] sorted;
        private int sortedSize;

        public VisitedPartitions(long minIdx)
        {
            Invariants.require(minIdx >= 0, "minIdx must be non-negative: %d", minIdx);
            this.minIdx = minIdx;
            this.highIdxWatermark = -1;
            this.sorted = new long[16];
            this.sortedSize = 0;
        }

        public synchronized void add(long idx)
        {
            Invariants.require(idx >= minIdx, "Partition index %d is below minimum %d", idx, minIdx);

            if (highIdxWatermark >= 0 && idx <= highIdxWatermark)
                return;

            int pos = Arrays.binarySearch(sorted, 0, sortedSize, idx);
            if (pos >= 0)
                return; // already present

            int insertPos = -pos - 1;
            if (sortedSize == sorted.length)
                sorted = Arrays.copyOf(sorted, sorted.length * 2);
            System.arraycopy(sorted, insertPos, sorted, insertPos + 1, sortedSize - insertPos);
            sorted[insertPos] = idx;
            sortedSize++;

            drain();
        }

        private void drain()
        {
            int removed = 0;
            while (removed < sortedSize)
            {
                long top = sorted[removed];

                if (highIdxWatermark == -1)
                {
                    if (top == minIdx)
                    {
                        removed++;
                        highIdxWatermark = minIdx;
                    }
                    else
                    {
                        break;
                    }
                }
                else if (top == highIdxWatermark + 1)
                {
                    removed++;
                    highIdxWatermark = top;
                }
                else if (top <= highIdxWatermark)
                {
                    removed++;
                }
                else
                {
                    break;
                }
            }

            if (removed > 0)
            {
                sortedSize -= removed;
                System.arraycopy(sorted, removed, sorted, 0, sortedSize);
            }
        }

        private synchronized long size()
        {
            long contiguous = highIdxWatermark >= 0 ? (highIdxWatermark - minIdx + 1) : 0;
            return contiguous + sortedSize;
        }

        public synchronized long getBySeed(long seed)
        {
            long total = size();
            if (total == 0)
                return -1;

            long chosen = SeedableEntropySource.computeWithSeed(seed, rng -> rng.nextLong(0, total));

            long contiguous = highIdxWatermark >= 0 ? (highIdxWatermark - minIdx + 1) : 0;
            if (chosen < contiguous)
                return minIdx + chosen;

            int heapIdx = Math.toIntExact(chosen - contiguous);
            return sorted[heapIdx];
        }
    }

    public static class TrackerWrapper implements DataTracker
    {
        private final DataTracker delegate;
        private final Partitions partitions;

        public TrackerWrapper(DataTracker delegate, Partitions partitions)
        {
            this.delegate = delegate;
            this.partitions = partitions;

        }

        @Override
        public void begin(Visit visit)
        {
            delegate.begin(visit);
        }

        @Override
        public void end(Visit visit)
        {
            delegate.end(visit);
            // Referencing happens before handing over to the worker
            for (long pd : visit.visitedPartitions)
                partitions.forPd(pd).deref();
        }
    }
}
