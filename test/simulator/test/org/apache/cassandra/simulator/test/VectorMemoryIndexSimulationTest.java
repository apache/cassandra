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

package org.apache.cassandra.simulator.test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.vector.OnHeapGraph;
import org.apache.cassandra.index.sai.disk.v1.vector.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.disk.v1.vector.VectorPostings;
import org.apache.cassandra.index.sai.memory.VectorMemoryIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.simulator.asm.NemesisFieldKind;
import org.apache.cassandra.simulator.asm.NemesisFieldSelectors;
import org.apache.cassandra.utils.CloseableIterator;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import sun.misc.Unsafe;

/**
 * Simulation test for {@link VectorMemoryIndex} that exercises the real {@code add()} and
 * {@code orderBy()} / {@code orderResultsBy()} code paths under adversarial nemesis scheduling.
 * <p>
 * The nemesis framework injects scheduling perturbation around mutable field accesses in
 * {@link OnHeapGraph}, {@link VectorPostings}, and {@link VectorMemoryIndex}.
 * <p>
 * Because the simulator's {@code InstanceClassLoader} cannot bootstrap full Cassandra infrastructure
 * (DatabaseDescriptor, Keyspace, ColumnFamilyStore), this test constructs a minimal
 * {@link StorageAttachedIndex} via {@code Unsafe.allocateInstance()} with only the fields
 * that {@link VectorMemoryIndex} actually accesses set reflectively.
 * <p>
 * Tests ported from {@link org.apache.cassandra.index.sai.memory.VectorMemoryIndexTest}
 * <ul>
 *   <li>{@link #testConcurrentAddsWithRandomVectors()} — N writers with random vectors, verify no data loss</li>
 *   <li>{@link #testConcurrentAddsWithSharedVectors()} — N writers with shared vectors, verify no data loss</li>
 *   <li>{@link #testConcurrentAddsAndOrderByRandomVectors()} — writers + readers via orderBy() with random vectors</li>
 *   <li>{@link #testConcurrentAddsAndOrderBySharedVectors()} — writers + readers via orderBy() with shared vectors</li>
 *   <li>{@link #testConcurrentAddsAndOrderResultsByRandomVectors()} — writers + readers via orderResultsBy() with random vectors</li>
 *   <li>{@link #testConcurrentAddsAndOrderResultsBySharedVectors()} — writers + readers via orderResultsBy() with shared vectors</li>
 * </ul>
 */
public class VectorMemoryIndexSimulationTest extends SimulationTestBase
{
    private static final int DIMENSIONS = 8;
    private static final int VECTORS_PER_THREAD = 200;
    private static final int NUM_WRITER_THREADS = 4;
    private static final int NUM_READER_THREADS = 4;
    private static final int PRE_SEED_COUNT = 50;
    // Lower than VectorMemoryIndexTest (0.9) because simulation uses fewer vectors per thread
    // and the adversarial scheduling can affect graph construction quality.
    private static final double RECALL_THRESHOLD = 0.8;

    @BeforeClass
    public static void registerNemesisFields()
    {
        // OnHeapGraph mutable fields
        NemesisFieldSelectors.register(OnHeapGraph.class, "hasDeletions", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(OnHeapGraph.class, "postingsMap", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(OnHeapGraph.class, "postingsByOrdinal", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(OnHeapGraph.class, "vectorsByKey", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(OnHeapGraph.class, "vectorValues", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(OnHeapGraph.class, "builder", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(OnHeapGraph.class, "nextOrdinal", NemesisFieldKind.ATOMICX);

        // VectorPostings mutable fields
        NemesisFieldSelectors.register(VectorPostings.class, "ordinal", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(VectorPostings.class, "postings", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(VectorPostings.class, "rowIds", NemesisFieldKind.SIMPLE);

        // NOTE: VectorMemoryIndex.keyBounds is NOT registered because it is written inside a
        // synchronized(this) block (updateKeyBounds). Nemesis pausing inside a held monitor
        // causes unresolvable deadlocks with the simulator's cooperative scheduling.
    }

    /**
     * Verifies that concurrent calls to {@link VectorMemoryIndex#add} with random vectors
     * do not corrupt the graph or lose data.
     */
    @Test
    public void testConcurrentAddsWithRandomVectors()
    {
        SharedState.useSharedVectors = false;
        testConcurrentAddsAreEventuallyConsistent();
    }

    /**
     * Verifies that concurrent calls to {@link VectorMemoryIndex#add} with shared (near-duplicate)
     * vectors do not corrupt the graph or lose data.
     */
    @Test
    public void testConcurrentAddsWithSharedVectors()
    {
        SharedState.useSharedVectors = true;
        testConcurrentAddsAreEventuallyConsistent();
    }

    /**
     * Verifies that concurrent calls to {@link VectorMemoryIndex#add} do not corrupt the graph
     * or lose data. Each thread owns a disjoint range of partition key integers.
     * After all writers complete, the graph must contain all inserted vectors.
     * <p>
     * GraphIndexBuilder.addGraphNode() is designed for concurrent use: insertionsInProgress
     * is a ConcurrentSkipListSet, and PoolingSupport gives each thread its own GraphSearcher
     * and scratch arrays. This test validates the full stack from VectorMemoryIndex.index()
     * through OnHeapGraph.add() through GraphIndexBuilder.addGraphNode().
     * <p>
     * After all writes complete, a full-ring search with limit == totalInserted
     * must return the vast majority of distinct results. Every returned key must
     * have been inserted by a worker thread, and every score must be a valid
     * positive float (a zero or NaN score would indicate graph corruption).
     */
    private void testConcurrentAddsAreEventuallyConsistent()
    {
        int totalInserted = NUM_WRITER_THREADS * VECTORS_PER_THREAD;

        simulate(() -> {
            boolean useShared = SharedState.useSharedVectors;
            VectorMemoryIndex memtableIndex = createVectorMemoryIndex();
            ConcurrentMap<Integer, DecoratedKey> keyMap = new ConcurrentHashMap<>();

            org.apache.cassandra.concurrent.ExecutorPlus executor =
                org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory().pooled("writers", NUM_WRITER_THREADS);

            for (int t = 0; t < NUM_WRITER_THREADS; t++)
            {
                final int threadId = t;
                executor.submit(() -> {
                    for (int i = 0; i < VECTORS_PER_THREAD; i++)
                    {
                        int pk = threadId * VECTORS_PER_THREAD + i;
                        DecoratedKey key = makeKey(pk);
                        ByteBuffer vector = useShared ? makeSharedVector(i) : randomVector();
                        memtableIndex.add(key, Clustering.EMPTY, vector);
                        keyMap.put(pk, key);
                    }
                });
            }

            SharedState.memtableIndex = memtableIndex;
            SharedState.keyMap = keyMap;
            SharedState.totalInserted = totalInserted;
        }, () -> {
            VectorMemoryIndex memtableIndex = (VectorMemoryIndex) SharedState.memtableIndex;
            @SuppressWarnings("unchecked")
            ConcurrentMap<Integer, DecoratedKey> keyMap = (ConcurrentMap<Integer, DecoratedKey>) SharedState.keyMap;
            int total = SharedState.totalInserted;

            if (memtableIndex == null)
                throw new AssertionError("VectorMemoryIndex was not created");
            if (memtableIndex.isEmpty())
                throw new AssertionError("VectorMemoryIndex is empty after concurrent adds");

            StorageAttachedIndex index = getIndex(memtableIndex);
            AbstractBounds<PartitionPosition> fullRing =
                new Range<>(Murmur3Partitioner.instance.getMinimumToken().minKeyBound(),
                            Murmur3Partitioner.instance.getMinimumToken().minKeyBound());

            Expression expression = Expression.create(index);
            expression.add(Operator.ANN, randomVector());

            QueryContext queryContext = createQueryContext(total);
            Set<Integer> foundKeys = new HashSet<>();
            try (CloseableIterator<PrimaryKeyWithScore> results = memtableIndex.orderBy(queryContext, expression, fullRing))
            {
                while (results.hasNext())
                {
                    PrimaryKeyWithScore result = results.next();
                    if (result.primaryKey() == null)
                        throw new AssertionError("Null PrimaryKey in search results after concurrent adds");
                    float score = result.score();
                    if (!Float.isFinite(score))
                        throw new AssertionError("Non-finite score after concurrent adds: " + score);

                    // All vector components are drawn from [0, 1) via ThreadLocalRandom.nextFloat(),
                    // so every term in the dot product is non-negative and the sum is strictly positive.
                    // A score of 0f or below would indicate graph corruption, not a valid similarity result.
                    if (score <= 0f)
                        throw new AssertionError("Non-positive score after concurrent adds: " + score);

                    int pk = Int32Type.instance.compose(result.primaryKey().partitionKey().getKey());
                    if (foundKeys.contains(pk))
                        throw new AssertionError("Duplicate key returned after concurrent adds: " + pk);
                    if (!keyMap.containsKey(pk))
                        throw new AssertionError("Returned key " + pk + " was not inserted by any worker thread");
                    foundKeys.add(pk);
                }
            }

            int expectedMinimum = (int) (total * RECALL_THRESHOLD);
            if (foundKeys.size() < expectedMinimum)
                throw new AssertionError(String.format(
                    "Search returned %d of %d results after concurrent adds (expected at least %d)",
                    foundKeys.size(), total, expectedMinimum));
        }, DEFAULT_ITERATIONS);
    }

    /**
     * Verifies that {@link VectorMemoryIndex#orderBy} never throws while concurrent add() calls
     * with random vectors are in progress.
     */
    @Test
    public void testConcurrentAddsAndOrderByRandomVectors()
    {
        SharedState.useSharedVectors = false;
        testConcurrentAddsAndOrderByNeverThrow();
    }

    /**
     * Verifies that {@link VectorMemoryIndex#orderBy} never throws while concurrent add() calls
     * with shared (near-duplicate) vectors are in progress.
     */
    @Test
    public void testConcurrentAddsAndOrderBySharedVectors()
    {
        SharedState.useSharedVectors = true;
        testConcurrentAddsAndOrderByNeverThrow();
    }

    /**
     * Verifies that {@link VectorMemoryIndex#orderBy} never throws while concurrent add() calls
     * are in progress, and that the index reaches a consistent state once writes settle.
     * <p>
     * Missing results during concurrent writes are expected and correct — a read that
     * races with a write is allowed to miss that write (valid linearization). The only
     * invariant asserted during the write window is safety: no exceptions, no null PKs,
     * no non-finite scores from results that *are* returned.
     * <p>
     * After all writers complete, a final search verifies full consistency at rest.
     */
    private void testConcurrentAddsAndOrderByNeverThrow()
    {
        int totalInserted = NUM_WRITER_THREADS * VECTORS_PER_THREAD;

        simulate(() -> {
            boolean useShared = SharedState.useSharedVectors;
            VectorMemoryIndex memtableIndex = createVectorMemoryIndex();
            ConcurrentMap<Integer, DecoratedKey> keyMap = new ConcurrentHashMap<>();

            // Pre-seed enough rows that orderBy() always has a non-empty graph to search,
            // avoiding the early-return in OnHeapGraph.search() when vectorValues.size() == 0
            // which would prevent readers from exercising any real code paths.
            for (int i = 1; i <= PRE_SEED_COUNT; i++)
            {
                DecoratedKey dk = makeKey(-i);
                memtableIndex.add(dk, Clustering.EMPTY, randomVector());
                keyMap.put(-i, dk);
            }

            org.apache.cassandra.concurrent.ExecutorPlus executor =
                org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory().pooled("mixed", NUM_WRITER_THREADS + NUM_READER_THREADS);

            // Writers: each inserts into a disjoint PK range
            for (int t = 0; t < NUM_WRITER_THREADS; t++)
            {
                final int threadId = t;
                executor.submit(() -> {
                    for (int i = 0; i < VECTORS_PER_THREAD; i++)
                    {
                        int pk = threadId * VECTORS_PER_THREAD + i;
                        DecoratedKey dk = makeKey(pk);
                        ByteBuffer vector = useShared ? makeSharedVector(i) : randomVector();
                        memtableIndex.add(dk, Clustering.EMPTY, vector);
                        keyMap.put(pk, dk);
                    }
                });
            }

            // Readers: call real orderBy() while writers are in progress
            // Safety assertions only. Missing results are a valid linearization
            // of concurrent read/write and are not asserted against here.
            StorageAttachedIndex index = getIndex(memtableIndex);
            for (int t = 0; t < NUM_READER_THREADS; t++)
            {
                executor.submit(() -> {
                    AbstractBounds<PartitionPosition> fullRing =
                        new Range<>(Murmur3Partitioner.instance.getMinimumToken().minKeyBound(),
                                    Murmur3Partitioner.instance.getMinimumToken().minKeyBound());

                    for (int i = 0; i < 10; i++)
                    {
                        Expression expression = Expression.create(index);
                        expression.add(Operator.ANN, randomVector());
                        QueryContext ctx = createQueryContext(totalInserted + PRE_SEED_COUNT);

                        // Must not throw — safety invariant
                        try (CloseableIterator<PrimaryKeyWithScore> results = memtableIndex.orderBy(ctx, expression, fullRing))
                        {
                            while (results.hasNext())
                            {
                                PrimaryKeyWithScore result = results.next();
                                if (result.primaryKey() == null)
                                    throw new AssertionError("Null PrimaryKey during concurrent add() + orderBy()");
                                if (!Float.isFinite(result.score()))
                                    throw new AssertionError("Non-finite score during concurrent add() + orderBy(): " + result.score());
                            }
                        }
                    }
                });
            }

            SharedState.memtableIndex = memtableIndex;
            SharedState.keyMap = keyMap;
            SharedState.totalInserted = totalInserted;
        }, () -> {
            VectorMemoryIndex memtableIndex = (VectorMemoryIndex) SharedState.memtableIndex;
            int total = SharedState.totalInserted;
            StorageAttachedIndex index = getIndex(memtableIndex);

            AbstractBounds<PartitionPosition> fullRing =
                new Range<>(Murmur3Partitioner.instance.getMinimumToken().minKeyBound(),
                            Murmur3Partitioner.instance.getMinimumToken().minKeyBound());

            Expression expression = Expression.create(index);
            expression.add(Operator.ANN, randomVector());
            QueryContext ctx = createQueryContext(total + PRE_SEED_COUNT);

            Set<Integer> foundKeys = new HashSet<>();
            try (CloseableIterator<PrimaryKeyWithScore> results = memtableIndex.orderBy(ctx, expression, fullRing))
            {
                while (results.hasNext())
                {
                    PrimaryKeyWithScore result = results.next();
                    if (result.primaryKey() == null)
                        throw new AssertionError("Null PrimaryKey after writes settled");
                    if (!Float.isFinite(result.score()))
                        throw new AssertionError("Non-finite score after writes settled: " + result.score());

                    // All vector components are drawn from [0, 1) via ThreadLocalRandom.nextFloat(),
                    // so every term in the dot product is non-negative and the sum is strictly positive.
                    // A score of 0f or below would indicate graph corruption, not a valid similarity result.
                    if (result.score() <= 0f)
                        throw new AssertionError("Non-positive score after writes settled: " + result.score());

                    int pk = Int32Type.instance.compose(result.primaryKey().partitionKey().getKey());
                    foundKeys.add(pk);
                }
            }

            // ANN recall is approximate, so we allow a small miss rate rather
            // than asserting exact equality. Pre-seeded keys (negative PKs) are
            // included in the limit so they do not crowd out writer-inserted keys.
            long writerKeysFound = foundKeys.stream().filter(pk -> pk >= 0).count();
            int expectedMinimum = (int) (total * RECALL_THRESHOLD);
            if (writerKeysFound < expectedMinimum)
                throw new AssertionError(String.format(
                    "Only %d of %d writer-inserted keys found after writes settled (expected at least %d)",
                    writerKeysFound, total, expectedMinimum));
        }, DEFAULT_ITERATIONS);
    }

    /**
     * Verifies that {@link VectorMemoryIndex#orderResultsBy} never throws while concurrent
     * add() calls with random vectors are in progress.
     */
    @Test
    public void testConcurrentAddsAndOrderResultsByRandomVectors()
    {
        SharedState.useSharedVectors = false;
        testConcurrentAddsAndOrderResultsByNeverThrow();
    }

    /**
     * Verifies that {@link VectorMemoryIndex#orderResultsBy} never throws while concurrent
     * add() calls with shared (near-duplicate) vectors are in progress.
     */
    @Test
    public void testConcurrentAddsAndOrderResultsBySharedVectors()
    {
        SharedState.useSharedVectors = true;
        testConcurrentAddsAndOrderResultsByNeverThrow();
    }

    /**
     * Verifies that orderResultsBy() never throws while concurrent add() calls are in
     * progress, and that the index reaches a consistent state once writes settle.
     * <p>
     * The materialized key list passed to orderResultsBy() is built from keyMap, which is a
     * ConcurrentHashMap updated by every add() call. A snapshot taken mid-write may be
     * incomplete — this is intentional and mirrors the production path where the source
     * KeyRangeIterator only sees keys committed before the non-ANN index scan ran.
     */
    private void testConcurrentAddsAndOrderResultsByNeverThrow()
    {
        int totalInserted = NUM_WRITER_THREADS * VECTORS_PER_THREAD;

        simulate(() -> {
            boolean useShared = SharedState.useSharedVectors;
            VectorMemoryIndex memtableIndex = createVectorMemoryIndex();
            StorageAttachedIndex index = getIndex(memtableIndex);
            ConcurrentMap<Integer, DecoratedKey> keyMap = new ConcurrentHashMap<>();

            // Pre-seed rows so orderResultsBy() always has a non-empty [minimumKey, maximumKey]
            // window and a non-trivial resultsInRange list on the first reader pass.
            for (int i = 1; i <= PRE_SEED_COUNT; i++)
            {
                DecoratedKey dk = makeKey(-i);
                memtableIndex.add(dk, Clustering.EMPTY, randomVector());
                keyMap.put(-i, dk);
            }

            org.apache.cassandra.concurrent.ExecutorPlus executor =
                org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory().pooled("mixed", NUM_WRITER_THREADS + NUM_READER_THREADS);

            // Writers: each inserts into a disjoint PK range [threadId*VECTORS_PER_THREAD, (threadId+1)*VECTORS_PER_THREAD)
            for (int t = 0; t < NUM_WRITER_THREADS; t++)
            {
                final int threadId = t;
                executor.submit(() -> {
                    for (int i = 0; i < VECTORS_PER_THREAD; i++)
                    {
                        int pk = threadId * VECTORS_PER_THREAD + i;
                        DecoratedKey dk = makeKey(pk);
                        ByteBuffer vector = useShared ? makeSharedVector(i) : randomVector();
                        memtableIndex.add(dk, Clustering.EMPTY, vector);
                        keyMap.put(pk, dk);
                    }
                });
            }

            // Readers: call real orderResultsBy() with a snapshot of current keys
            // Safety assertions only during concurrent writes. Missing results are a valid
            // linearization and are not asserted against here.
            for (int t = 0; t < NUM_READER_THREADS; t++)
            {
                executor.submit(() -> {
                    for (int i = 0; i < 10; i++)
                    {
                        // Snapshot current keys and build sorted PrimaryKey list
                        List<PrimaryKey> snapshotKeys = keyMap.values()
                                                             .stream()
                                                             .map(dk -> index.keyFactory().create(dk))
                                                             .sorted()
                                                             .collect(Collectors.toList());
                        if (snapshotKeys.isEmpty())
                            continue;

                        Expression expression = Expression.create(index);
                        expression.add(Operator.ANN, randomVector());
                        QueryContext ctx = createQueryContext(snapshotKeys.size());

                        // Must not throw
                        try (CloseableIterator<PrimaryKeyWithScore> results = memtableIndex.orderResultsBy(ctx, snapshotKeys, expression))
                        {
                            while (results.hasNext())
                            {
                                PrimaryKeyWithScore result = results.next();
                                if (result.primaryKey() == null)
                                    throw new AssertionError("Null PrimaryKey during concurrent add() + orderResultsBy()");
                                if (!Float.isFinite(result.score()))
                                    throw new AssertionError("Non-finite score during concurrent add() + orderResultsBy(): " + result.score());
                            }
                        }
                    }
                });
            }

            SharedState.memtableIndex = memtableIndex;
            SharedState.keyMap = keyMap;
            SharedState.totalInserted = totalInserted;
        }, () -> {
            VectorMemoryIndex memtableIndex = (VectorMemoryIndex) SharedState.memtableIndex;
            StorageAttachedIndex index = getIndex(memtableIndex);
            @SuppressWarnings("unchecked")
            ConcurrentMap<Integer, DecoratedKey> keyMap = (ConcurrentMap<Integer, DecoratedKey>) SharedState.keyMap;
            int total = SharedState.totalInserted;

            List<PrimaryKey> allKeys = keyMap.values()
                                             .stream()
                                             .map(dk -> index.keyFactory().create(dk))
                                             .sorted()
                                             .collect(Collectors.toList());

            Expression expression = Expression.create(index);
            expression.add(Operator.ANN, randomVector());
            QueryContext ctx = createQueryContext(total + PRE_SEED_COUNT);

            Set<Integer> foundKeys = new HashSet<>();
            try (CloseableIterator<PrimaryKeyWithScore> results = memtableIndex.orderResultsBy(ctx, allKeys, expression))
            {
                while (results.hasNext())
                {
                    PrimaryKeyWithScore result = results.next();
                    if (result.primaryKey() == null)
                        throw new AssertionError("Null PrimaryKey after writes settled in orderResultsBy()");
                    if (!Float.isFinite(result.score()))
                        throw new AssertionError("Non-finite score after writes settled in orderResultsBy(): " + result.score());

                    // All vector components are drawn from [0, 1) via ThreadLocalRandom.nextFloat(),
                    // so every term in the dot product is non-negative and the sum is strictly positive.
                    // A score of 0f or below would indicate graph corruption, not a valid similarity result.
                    if (result.score() <= 0f)
                        throw new AssertionError("Non-positive score after writes settled in orderResultsBy(): " + result.score());

                    int pk = Int32Type.instance.compose(result.primaryKey().partitionKey().getKey());
                    foundKeys.add(pk);
                }
            }

            long writerKeysFound = foundKeys.stream().filter(pk -> pk >= 0).count();
            int expectedMinimum = (int) (total * RECALL_THRESHOLD);
            if (writerKeysFound < expectedMinimum)
                throw new AssertionError(String.format(
                    "orderResultsBy() returned %d of %d writer-inserted keys after writes settled (expected at least %d)",
                    writerKeysFound, total, expectedMinimum));
        }, DEFAULT_ITERATIONS);
    }

    // ---- Infrastructure: create VectorMemoryIndex without DatabaseDescriptor ----

    /**
     * Creates a {@link VectorMemoryIndex} by constructing a minimal {@link StorageAttachedIndex}
     * via {@code Unsafe.allocateInstance()} (bypassing the constructor which requires ColumnFamilyStore
     * and triggers DatabaseDescriptor initialization).
     * <p>
     * Only the fields actually accessed by VectorMemoryIndex methods are set:
     * <ul>
     *   <li>{@code indexTermType} — for decomposeVector(), indexType(), columnMetadata()</li>
     *   <li>{@code indexWriterConfig} — for graph construction and getSimilarityFunction()</li>
     *   <li>{@code primaryKeyFactory} — for creating PrimaryKey instances</li>
     * </ul>
     */
    private static VectorMemoryIndex createVectorMemoryIndex()
    {
        try
        {
            // RangeUtil has a static initializer that calls DatabaseDescriptor.getPartitioner().
            // Set the partitioner field directly to avoid full DD initialization.
            setField(org.apache.cassandra.config.DatabaseDescriptor.class, "partitioner", null, Murmur3Partitioner.instance);

            VectorType<?> vectorType = VectorType.getInstance(FloatType.instance, DIMENSIONS);
            ColumnMetadata column = ColumnMetadata.regularColumn("ks_sim", "tbl_sim", "vec", vectorType, 0);
            IndexTermType termType = IndexTermType.create(column, Collections.singletonList(
                ColumnMetadata.regularColumn("ks_sim", "tbl_sim", "pk", Int32Type.instance, 1)),
                IndexTarget.Type.SIMPLE);

            IndexWriterConfig writerConfig = new IndexWriterConfig(
                IndexWriterConfig.DEFAULT_MAXIMUM_NODE_CONNECTIONS,
                IndexWriterConfig.DEFAULT_CONSTRUCTION_BEAM_WIDTH,
                VectorSimilarityFunction.DOT_PRODUCT,
                null);

            ClusteringComparator emptyComparator = new ClusteringComparator();
            PrimaryKey.Factory keyFactory = new PrimaryKey.Factory(Murmur3Partitioner.instance, emptyComparator);

            // Allocate StorageAttachedIndex without calling its constructor
            Unsafe unsafe = getUnsafe();
            StorageAttachedIndex index = (StorageAttachedIndex) unsafe.allocateInstance(StorageAttachedIndex.class);

            // Set the fields VectorMemoryIndex accesses
            setField(StorageAttachedIndex.class, "indexTermType", index, termType);
            setField(StorageAttachedIndex.class, "indexWriterConfig", index, writerConfig);
            setField(StorageAttachedIndex.class, "primaryKeyFactory", index, keyFactory);

            // hasClustering() calls baseCfs.getComparator().size() — needs a minimal CFS with metadata
            TableMetadata tableMetadata = TableMetadata.builder("ks_sim", "tbl_sim")
                                                       .addPartitionKeyColumn("pk", Int32Type.instance)
                                                       .addRegularColumn("val", vectorType)
                                                       .partitioner(Murmur3Partitioner.instance)
                                                       .caching(CachingParams.CACHE_NOTHING)
                                                       .build();
            // Create a minimal CFS via Unsafe with just the metadata ref set
            ColumnFamilyStore fakeCfs = (ColumnFamilyStore) unsafe.allocateInstance(ColumnFamilyStore.class);
            org.apache.cassandra.schema.TableMetadataRef metadataRef =
                org.apache.cassandra.schema.TableMetadataRef.forOfflineTools(tableMetadata);
            setField(ColumnFamilyStore.class, "metadata", fakeCfs, metadataRef);
            setField(StorageAttachedIndex.class, "baseCfs", index, fakeCfs);

            // validateTermSize needs maxTermSizeGuardrail (non-null).
            // Create a disabled guardrail via Unsafe rather than loading Guardrails (which may trigger DD init)
            org.apache.cassandra.db.guardrails.MaxThreshold fakeGuardrail =
                (org.apache.cassandra.db.guardrails.MaxThreshold) unsafe.allocateInstance(org.apache.cassandra.db.guardrails.MaxThreshold.class);
            setField(StorageAttachedIndex.class, "maxTermSizeGuardrail", index, fakeGuardrail);

            // Proxy Memtable — Mockito cannot be used inside InstanceClassLoader
            Memtable memtable = (Memtable) java.lang.reflect.Proxy.newProxyInstance(
                Memtable.class.getClassLoader(),
                new Class<?>[]{ Memtable.class },
                (proxy, method, args) -> {
                    if ("hashCode".equals(method.getName())) return System.identityHashCode(proxy);
                    if ("toString".equals(method.getName())) return "SimulatedMemtable";
                    if ("equals".equals(method.getName())) return proxy == args[0];
                    if (method.getReturnType() == boolean.class) return false;
                    return null;
                }
            );

            return new VectorMemoryIndex(index, memtable);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to create VectorMemoryIndex for simulation", e);
        }
    }

    private static DecoratedKey makeKey(int pk)
    {
        ByteBuffer key = Int32Type.instance.decompose(pk);
        return Murmur3Partitioner.instance.decorateKey(key);
    }

    private static ByteBuffer randomVector()
    {
        List<Float> rawVector = new ArrayList<>(DIMENSIONS);
        for (int i = 0; i < DIMENSIONS; i++)
            rawVector.add(ThreadLocalRandom.current().nextFloat());
        return VectorType.getInstance(FloatType.instance, DIMENSIONS).getSerializer().serialize(rawVector);
    }

    /**
     * Creates a shared vector where most dimensions are 0.5f and the last dimension
     * varies by index. This tests that the graph handles duplicate/near-duplicate vectors correctly.
     */
    private static ByteBuffer makeSharedVector(int i)
    {
        List<Float> raw = new ArrayList<>(Collections.nCopies(DIMENSIONS - 1, 0.5f));
        raw.add(i / (float) VECTORS_PER_THREAD);
        return VectorType.getInstance(FloatType.instance, DIMENSIONS).getSerializer().serialize(raw);
    }

    private static QueryContext createQueryContext(int limit)
    {
        // Create a minimal ReadCommand for QueryContext's limit() method.
        // TableMetadata and PartitionRangeReadCommand do not require DatabaseDescriptor.
        VectorType<?> vectorType = VectorType.getInstance(FloatType.instance, DIMENSIONS);
        TableMetadata metadata = TableMetadata.builder("ks_sim", "tbl_sim")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addRegularColumn("val", vectorType)
                                              .partitioner(Murmur3Partitioner.instance)
                                              .caching(CachingParams.CACHE_NOTHING)
                                              .build();
        return new QueryContext(
            PartitionRangeReadCommand.create(metadata,
                                             (int) (System.currentTimeMillis() / 1000),
                                             ColumnFilter.all(metadata),
                                             RowFilter.none(),
                                             DataLimits.cqlLimits(limit),
                                             DataRange.allData(metadata.partitioner)),
            TimeUnit.SECONDS.toMillis(60));
    }

    private static StorageAttachedIndex getIndex(VectorMemoryIndex memtableIndex)
    {
        try
        {
            Field f = org.apache.cassandra.index.sai.memory.MemoryIndex.class.getDeclaredField("index");
            f.setAccessible(true);
            return (StorageAttachedIndex) f.get(memtableIndex);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Unsafe getUnsafe()
    {
        try
        {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void setField(Class<?> clazz, String fieldName, Object target, Object value)
    {
        try
        {
            Field f = clazz.getDeclaredField(fieldName);
            f.setAccessible(true);
            f.set(target, value);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to set field " + fieldName + " on " + clazz.getSimpleName(), e);
        }
    }

    /**
     * Static holder for sharing state between the action and check runnables.
     * Fields are volatile because they are written by the test method (outer classloader)
     * and read inside the InstanceClassLoader.
     */
    public static class SharedState
    {
        public static volatile Object memtableIndex;
        public static volatile Object keyMap;
        public static volatile int totalInserted;
        public static volatile boolean useSharedVectors;
    }
}
