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

package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.v1.vector.OnHeapGraph;
import org.apache.cassandra.index.sai.disk.v1.vector.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.apache.cassandra.config.CassandraRelevantProperties.MEMTABLE_SHARD_COUNT;

public class VectorMemoryIndexTest extends SAITester
{
    private static final Injections.Counter indexSearchCounter = Injections.newCounter("IndexSearchCounter")
                                                                           .add(InvokePointBuilder.newInvokePoint()
                                                                                                  .onClass(TrieMemoryIndex.class)
                                                                                                  .onMethod("search"))
                                                                           .build();

    private static final double RECALL_THRESHOLD = 0.9;
    private static final int VECTORS_PER_THREAD = 2000;
    private static final int VECTORS_PER_THREAD_BEYOND_POOL_CAP = 500; // more writers, so fewer vectors each

    private ColumnFamilyStore cfs;
    private StorageAttachedIndex index;
    private VectorMemoryIndex memtableIndex;
    private IPartitioner partitioner;
    private ConcurrentMap<DecoratedKey, Integer> keyMap;
    private Map<Integer, ByteBuffer> rowMap;
    private int dimensionCount;

    @BeforeClass
    public static void setShardCount()
    {
        MEMTABLE_SHARD_COUNT.setInt(8);
    }

    @Before
    public void setup() throws Throwable
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.updateNormalTokens(BootStrapper.getRandomTokens(metadata, 10), FBUtilities.getBroadcastAddressAndPort());

        dimensionCount = getRandom().nextIntBetween(2, 2048);
        index = SAITester.createMockIndex(VectorType.getInstance(FloatType.instance, dimensionCount));
        cfs = index.baseCfs();
        partitioner = cfs.getPartitioner();
        indexSearchCounter.reset();
        keyMap = new ConcurrentHashMap<>();
        rowMap = new HashMap<>();

        Injections.inject(indexSearchCounter);
    }

    @Test
    public void randomQueryTest() throws Exception
    {
        // A non-null memtable tells it to track the mapping from primary key to vector, needed for brute force search
        memtableIndex = new VectorMemoryIndex(index, mockMemtable(1));

        for (int row = 0; row < getRandom().nextIntBetween(1000, 5000); row++)
        {
            int pk = getRandom().nextIntBetween(0, 10000);
            while (rowMap.containsKey(pk))
                pk = getRandom().nextIntBetween(0, 10000);
            var value = randomVector();
            rowMap.put(pk, value);
            addRow(pk, value);
        }

        List<DecoratedKey> keys = new ArrayList<>(keyMap.keySet());
        long actualVectorsReturned = 0;
        long expectedVectorsReturned = 0;

        for (int executionCount = 0; executionCount < 1000; executionCount++)
        {
            Expression expression = generateRandomExpression();
            AbstractBounds<PartitionPosition> keyRange = generateRandomBounds(keys);
            Set<Integer> keysInRange = keys.stream().filter(keyRange::contains)
                                           .map(k -> Int32Type.instance.compose(k.getKey()))
                                           .collect(Collectors.toSet());

            Set<Integer> foundKeys = new HashSet<>();
            int limit = getRandom().nextIntBetween(1, 100);
            ReadCommand command = createRangeRead(limit);
            long expectedResults = Math.min(limit, keysInRange.size());

            try (CloseableIterator<PrimaryKeyWithScore> iterator = memtableIndex.orderBy(new QueryContext(command,
                                                                                                          DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS)),
                                                                                         expression, keyRange))
            {
                PrimaryKeyWithScore lastKey = null;
                while (iterator.hasNext() && foundKeys.size() < expectedResults)
                {
                    PrimaryKeyWithScore primaryKeyWithScore = iterator.next();
                    if (lastKey != null)
                        // This assertion only holds true as long as we query at most the expectedNumResults.
                        // Once we query deeper, we might get a key with a higher score than the last key.
                        // This is a direct consequence of the approximate part of ANN.
                        // Note that PrimaryKeyWithScore is flipped to descending order, so we use >= here.
                        assertTrue("Returned keys are not ordered by score", primaryKeyWithScore.compareTo(lastKey) >= 0);
                    lastKey = primaryKeyWithScore;
                    int key = Int32Type.instance.compose(primaryKeyWithScore.primaryKey().partitionKey().getKey());
                    assertFalse(foundKeys.contains(key));

                    assertTrue(keyRange.contains(primaryKeyWithScore.primaryKey().partitionKey()));
                    assertTrue(rowMap.containsKey(key));
                    foundKeys.add(key);
                }
                // Note that we weight each result evenly instead of each query evenly.
                actualVectorsReturned += foundKeys.size();
                expectedVectorsReturned += expectedResults;
                if (foundKeys.size() < expectedResults)
                    assertTrue("Expected at least " + expectedResults + " results but got " + foundKeys.size(),
                               foundKeys.size() >= expectedResults * RECALL_THRESHOLD);
            }
        }

        assertTrue("Expected at least " + expectedVectorsReturned + " results but got " + actualVectorsReturned,
                   actualVectorsReturned >= expectedVectorsReturned * RECALL_THRESHOLD);
    }

    /**
     * Verifies that expectedNodesVisited() always returns a value within its documented
     * bounds: at least min(limit, graphSize) and at most graphSize.
     * <p>
     * This is a pure arithmetic test with no index infrastructure required. It exercises
     * the boundary conditions that matter for the brute-force/ANN threshold decision in
     * maxBruteForceRows(): if the formula underflows its lower bound, small queries will
     * incorrectly use ANN; if it overflows its upper bound, the result is nonsensical.
     */
    @Test
    public void testExpectedNodesVisitedRespectsBounds()
    {
        int[] graphSizes       = { 1, 2, 10, 100, 1000, 10000 };
        int[] limits           = { 1, 2, 5, 10, 50, 100 };
        double[] permittedFractions = { 0.01, 0.1, 0.5, 1.0, 2.0 };

        for (int graphSize : graphSizes)
        {
            for (int limit : limits)
            {
                for (double fraction : permittedFractions)
                {
                    int permitted = Math.max(1, (int) (graphSize * fraction));
                    int result = VectorMemoryIndex.expectedNodesVisited(limit, permitted, graphSize);
                    int lowerBound = Math.min(limit, graphSize);

                    assertTrue(String.format("expectedNodesVisited(%d, %d, %d) = %d is below lower bound %d", limit, permitted, graphSize, result, lowerBound),
                            result >= lowerBound);

                    assertTrue(String.format("expectedNodesVisited(%d, %d, %d) = %d exceeds graphSize %d", limit, permitted, graphSize, result, graphSize),
                            result <= graphSize);
                }
            }
        }
    }

    @Test
    public void testConcurrentAddsWithRandomVectors() throws Exception
    {
        testConcurrentAddsAreEventuallyConsistent(Runtime.getRuntime().availableProcessors(), VECTORS_PER_THREAD, (threadId, i) -> randomVectorFromThreadLocal());
    }

    @Test
    public void testConcurrentAddsWithSharedVectors() throws Exception
    {
        testConcurrentAddsAreEventuallyConsistent(Runtime.getRuntime().availableProcessors(), VECTORS_PER_THREAD, (threadId, i) -> makeSharedVector(i));
    }

    /**
     * More writers than jvector's GraphIndexBuilder can serve at once must wait, not fail the insert. The other
     * concurrent tests use exactly availableProcessors writers, within jvector's limit, so their memtable reports that
     * it bounds writers and OnHeapGraph skips its semaphore; this test's memtable cannot make that promise, so
     * OnHeapGraph bounds the writers itself.
     */
    @Test
    public void testConcurrentAddsExceedingJVectorPoolCap() throws Exception
    {
        int numThreads = 2 * OnHeapGraph.MAX_CONCURRENT_GRAPH_INSERTS;
        testConcurrentAddsAreEventuallyConsistent(numThreads, VECTORS_PER_THREAD_BEYOND_POOL_CAP, (threadId, i) -> randomVectorFromThreadLocal());
    }

    /**
     * Verifies that concurrent calls to add() do not corrupt the graph or lose data.
     * <p>
     * GraphIndexBuilder.addGraphNode() is designed for concurrent use: insertionsInProgress
     * is a ConcurrentSkipListSet, and PoolingSupport gives each thread its own GraphSearcher
     * and scratch arrays. This test validates the full stack from VectorMemoryIndex.index()
     * through OnHeapGraph.add() through GraphIndexBuilder.addGraphNode().
     * <p>
     * After all writers complete, a full-ring search must return the vast majority of
     * inserted keys with valid scores, confirming no data was lost or corrupted.
     */
    private void testConcurrentAddsAreEventuallyConsistent(int numThreads, int vectorsPerThread, BiFunction<Integer, Integer, ByteBuffer> vectorFactory) throws Exception
    {
        memtableIndex = new VectorMemoryIndex(index, mockMemtable(numThreads));

        int totalInserted = numThreads * vectorsPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        // CyclicBarrier ensures all threads begin inserting simultaneously,
        // maximizing contention on GraphIndexBuilder and ConcurrentVectorValues.
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < numThreads; t++)
        {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try
                {
                    barrier.await();
                    for (int i = 0; i < vectorsPerThread; i++)
                    {
                        int pk = threadId * vectorsPerThread + i;
                        addRow(pk, vectorFactory.apply(threadId, i));
                    }
                }
                catch (BrokenBarrierException | InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }));
        }

        executor.shutdown();
        assertTrue("Timed out waiting for concurrent adds", executor.awaitTermination(60, TimeUnit.SECONDS));

        // Rethrow any exception from worker threads — assertion failures inside a
        // Runnable are otherwise silently swallowed by the ExecutorService.
        for (Future<?> f : futures)
        {
            try
            {
                f.get();
            }
            catch (ExecutionException e)
            {
                fail("Worker thread threw during concurrent add(): " + e.getCause());
            }
        }

        // After all writes complete, a full-ring search with limit == totalInserted
        // must return the vast majority of distinct results. Every returned key must
        // have been inserted by a worker thread, and every score must be a valid
        // positive float (a zero or NaN score would indicate graph corruption).
        AbstractBounds<PartitionPosition> fullRing = new Range<>(partitioner.getMinimumToken().minKeyBound(), partitioner.getMinimumToken().minKeyBound());
        Expression expression = generateRandomExpression();
        ReadCommand command = createRangeRead(totalInserted);

        QueryContext queryContext = new QueryContext(command, DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));
        Set<Integer> foundKeys = new HashSet<>();

        try (CloseableIterator<PrimaryKeyWithScore> iterator = memtableIndex.orderBy(queryContext, expression, fullRing))
        {
            while (iterator.hasNext())
            {
                PrimaryKeyWithScore result = iterator.next();
                assertNotNull("Null PrimaryKey in search results after concurrent adds", result.primaryKey());
                float score = result.score();
                assertTrue("Non-finite score after concurrent adds: " + score, Float.isFinite(score));

                // All vector components are drawn from [0, 1) via ThreadLocalRandom.nextFloat(),
                // so every term in the dot product is non-negative and the sum is strictly positive.
                // A score of 0f or below would indicate graph corruption, not a valid similarity result.
                assertTrue("Non-positive score after concurrent adds: " + score, score > 0f);

                int pk = Int32Type.instance.compose(result.primaryKey().partitionKey().getKey());
                assertFalse("Duplicate key returned after concurrent adds: " + pk, foundKeys.contains(pk));
                assertTrue("Returned key " + pk + " was not inserted by any worker thread", keyMap.containsKey(result.primaryKey().partitionKey()));
                foundKeys.add(pk);
            }
        }

        assertTrue("Search returned " + foundKeys.size() + " of " + totalInserted + " results after concurrent adds (expected at least " + (int) (totalInserted * RECALL_THRESHOLD) + ')',
                   foundKeys.size() >= totalInserted * RECALL_THRESHOLD);
    }

    @Test
    public void testConcurrentAddsAndOrderByRandomVectors() throws Exception
    {
        testConcurrentAddsAndOrderByNeverThrow((threadId, i) -> randomVectorFromThreadLocal());
    }

    @Test
    public void testConcurrentAddsAndOrderBySharedVectors() throws Exception
    {
        testConcurrentAddsAndOrderByNeverThrow((threadId, i) -> makeSharedVector(i));
    }

    /**
     * Verifies that orderBy() never throws an exception while concurrent add() calls
     * are in progress, and that the index reaches a consistent state once writes settle.
     * <p>
     * Missing results during concurrent writes are expected and correct — a read that
     * races with a write is allowed to miss that write (valid linearization). The only
     * invariant asserted during the write window is safety: no exceptions, no null PKs,
     * no non-finite scores from results that *are* returned.
     * <p>
     * Readers block on writersFinished after the barrier release and perform one final
     * search pass after all writers have joined, confirming full consistency at rest.
     */
    public void testConcurrentAddsAndOrderByNeverThrow(BiFunction<Integer, Integer, ByteBuffer> vectorFactory) throws Exception
    {
        int numWriterThreads = Runtime.getRuntime().availableProcessors();
        int numReaderThreads = Runtime.getRuntime().availableProcessors();
        memtableIndex = new VectorMemoryIndex(index, mockMemtable(numWriterThreads));
        int totalInserted = numWriterThreads * VECTORS_PER_THREAD;

        // Pre-seed enough rows that orderBy() always has a non-empty graph to search,
        // avoiding the early-return in OnHeapGraph.search() when vectorValues.size() == 0
        // which would prevent readers from exercising any real code paths.
        int preSeedCount = 50;
        for (int i = 1; i <= preSeedCount; i++)
            addRow(-i, randomVector()); // negative PKs, disjoint from writer range [0, totalInserted)

        // each reader blocks here until every writer has completed,
        // then performs one final search to verify post-settlement consistency.
        CountDownLatch writersFinished = new CountDownLatch(numWriterThreads);

        // phase1Executed: confirms at least one reader searched during the concurrent
        // write window. If this latch is never counted down, the write window was too
        // short and the concurrent safety assertions in Phase 1 were never exercised.
        AtomicBoolean phase1Executed = new AtomicBoolean(false);

        CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(numWriterThreads + numReaderThreads);
        CyclicBarrier barrier = new CyclicBarrier(numWriterThreads + numReaderThreads);

        // Writers: each inserts into a disjoint PK range
        for (int t = 0; t < numWriterThreads; t++)
        {
            final int threadId = t;
            executor.submit(() -> {
                try
                {
                    barrier.await();
                    for (int i = 0; i < VECTORS_PER_THREAD; i++)
                    {
                        int pk = threadId * VECTORS_PER_THREAD + i;
                        addRow(pk, vectorFactory.apply(threadId, i));
                    }
                }
                catch (Throwable e)
                {
                    errors.add(e);
                }
                finally
                {
                    writersFinished.countDown();
                }
            });
        }

        // Readers: issue one search while writers are running (safety only), then
        // block on writersFinished and issue one final search for correctness.
        for (int t = 0; t < numReaderThreads; t++)
        {
            executor.submit(() -> {
                try
                {
                    barrier.await();

                    AbstractBounds<PartitionPosition> fullRing = new Range<>(partitioner.getMinimumToken().minKeyBound(), partitioner.getMinimumToken().minKeyBound());
                    ReadCommand command = createRangeRead(totalInserted + preSeedCount);
                    QueryContext queryContext = new QueryContext(command, DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));

                    // Build query vectors inline — getRandom() is not thread-safe from
                    // worker threads, so we use ThreadLocalRandom directly.
                    ByteBuffer queryBuf = randomVectorFromThreadLocal();
                    Expression concurrentExpression = Expression.create(index);
                    concurrentExpression.add(Operator.ANN, queryBuf);

                    // --- Phase 1: one search while writers are still running ---
                    // Safety assertions only. Missing results are a valid linearization
                    // of concurrent read/write and are not asserted against here.
                    try (CloseableIterator<PrimaryKeyWithScore> it = memtableIndex.orderBy(queryContext, concurrentExpression, fullRing))
                    {
                        while (it.hasNext())
                        {
                            PrimaryKeyWithScore result = it.next();
                            assertNotNull("Null PrimaryKey during concurrent add() + orderBy()", result.primaryKey());
                            assertTrue("Non-finite score during concurrent add() + orderBy(): " + result.score(), Float.isFinite(result.score()));
                        }
                    }
                    phase1Executed.set(true);

                    // --- Phase 2: block until all writers finish, then verify consistency ---
                    writersFinished.await();

                    ByteBuffer settledQueryBuf = randomVectorFromThreadLocal();
                    Expression settledExpression = Expression.create(index);
                    settledExpression.add(Operator.ANN, settledQueryBuf);

                    Set<Integer> foundAfterSettle = new HashSet<>();
                    try (CloseableIterator<PrimaryKeyWithScore> it = memtableIndex.orderBy(queryContext, settledExpression, fullRing))
                    {
                        while (it.hasNext())
                        {
                            PrimaryKeyWithScore result = it.next();
                            assertNotNull("Null PrimaryKey after writes settled", result.primaryKey());
                            assertTrue("Non-finite score after writes settled: " + result.score(), Float.isFinite(result.score()));

                            // All vector components are drawn from [0, 1) via ThreadLocalRandom.nextFloat(),
                            // so every term in the dot product is non-negative and the sum is strictly positive.
                            // A score of 0f or below would indicate graph corruption, not a valid similarity result.
                            assertTrue("Non-positive score after writes settled: " + result.score(), result.score() > 0f);

                            int pk = Int32Type.instance.compose(result.primaryKey().partitionKey().getKey());
                            foundAfterSettle.add(pk);
                        }
                    }

                    // ANN recall is approximate, so we allow a small miss rate rather
                    // than asserting exact equality. Pre-seeded keys (negative PKs) are
                    // included in the limit so they do not crowd out writer-inserted keys.
                    int expectedMinimum = (int) (totalInserted * RECALL_THRESHOLD);
                    long writerKeysFound = foundAfterSettle.stream().filter(pk -> pk >= 0).count();
                    assertTrue("Only " + writerKeysFound + " of " + totalInserted + " writer-inserted keys found after writes settled" + " (expected at least " + expectedMinimum + ')',
                               writerKeysFound >= expectedMinimum);
                }
                catch (Throwable e)
                {
                    errors.add(e);
                }
            });
        }

        executor.shutdown();
        assertTrue("Timed out waiting for concurrent add() + orderBy()", executor.awaitTermination(60, TimeUnit.SECONDS));

        // Verify Phase 1 actually executed — if this fails, increase vectorsPerWriter
        // so the write window is wide enough for readers to search concurrently.
        assertTrue("No reader executed a search during the concurrent write window; increase vectorsPerWriter to widen the write window", phase1Executed.get());

        if (!errors.isEmpty())
        {
            AssertionError failure = new AssertionError("Concurrent add() + orderBy() produced " + errors.size() + " error(s); first: " + errors.get(0));
            errors.forEach(failure::addSuppressed);
            throw failure;
        }
    }

    @Test
    public void testConcurrentAddsAndOrderResultsByRandomVectors() throws Exception
    {
        testConcurrentAddsAndOrderResultsByNeverThrow((threadId, i) -> randomVectorFromThreadLocal());
    }

    @Test
    public void testConcurrentAddsAndOrderResultsBySharedVectors() throws Exception
    {
        testConcurrentAddsAndOrderResultsByNeverThrow((threadId, i) -> makeSharedVector(i));
    }

    /**
     * Verifies that orderResultsBy() never throws while concurrent add() calls are in
     * progress, and that the index reaches a consistent state once writes settle.
     * <p>
     * The materialized key list passed to orderResultsBy() is built from keyMap, which is a
     * ConcurrentHashMap updated by every addRow() call. A snapshot taken mid-write may be
     * incomplete — this is intentional and mirrors the production path where the source
     * KeyRangeIterator only sees keys committed before the non-ANN index scan ran.
     */
    private void testConcurrentAddsAndOrderResultsByNeverThrow(BiFunction<Integer, Integer, ByteBuffer> vectorFactory) throws Exception
    {
        int numWriterThreads = Runtime.getRuntime().availableProcessors();
        int numReaderThreads = Runtime.getRuntime().availableProcessors();
        memtableIndex = new VectorMemoryIndex(index, mockMemtable(numWriterThreads));
        int totalInserted = numWriterThreads * VECTORS_PER_THREAD;

        // Pre-seed rows so orderResultsBy() always has a non-empty [minimumKey, maximumKey]
        // window and a non-trivial resultsInRange list on the first reader pass.
        int preSeedCount = 50;
        for (int i = 1; i <= preSeedCount; i++)
            addRow(-i, randomVector()); // negative PKs, disjoint from writer range [0, totalInserted)

        CountDownLatch writersFinished = new CountDownLatch(numWriterThreads);
        AtomicBoolean phase1Executed = new AtomicBoolean(false);
        CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(numWriterThreads + numReaderThreads);
        CyclicBarrier barrier = new CyclicBarrier(numWriterThreads + numReaderThreads);

        // Writers: each inserts into a disjoint PK range [threadId*vectorsPerWriter, (threadId+1)*vectorsPerWriter)
        for (int t = 0; t < numWriterThreads; t++)
        {
            final int threadId = t;
            executor.submit(() -> {
                try
                {
                    barrier.await();
                    for (int i = 0; i < VECTORS_PER_THREAD; i++)
                    {
                        int pk = threadId * VECTORS_PER_THREAD + i;
                        addRow(pk, vectorFactory.apply(threadId, i));
                    }
                }
                catch (Throwable e)
                {
                    errors.add(e);
                }
                finally
                {
                    writersFinished.countDown();
                }
            });
        }

        for (int t = 0; t < numReaderThreads; t++)
        {
            executor.submit(() -> {
                try
                {
                    barrier.await();

                    ReadCommand command = createRangeRead(totalInserted + preSeedCount);
                    QueryContext queryContext = new QueryContext(command, DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));

                    // --- Phase 1: search during concurrent writes ---
                    // Snapshot the keys visible so far; the list may be incomplete, which is
                    // a valid linearization. We only assert safety here, not completeness.
                    List<PrimaryKey> snapshotKeys = buildSortedPrimaryKeySnapshot();

                    ByteBuffer queryBuf = randomVectorFromThreadLocal();
                    Expression concurrentExpression = Expression.create(index);
                    concurrentExpression.add(Operator.ANN, queryBuf);

                    if (!snapshotKeys.isEmpty())
                    {
                        try (CloseableIterator<PrimaryKeyWithScore> it = memtableIndex.orderResultsBy(queryContext, snapshotKeys, concurrentExpression))
                        {
                            while (it.hasNext())
                            {
                                PrimaryKeyWithScore result = it.next();
                                assertNotNull("Null PrimaryKey during concurrent add() + orderResultsBy()", result.primaryKey());
                                assertTrue("Non-finite score during concurrent add() + orderResultsBy(): " + result.score(), Float.isFinite(result.score()));
                            }
                        }
                        phase1Executed.set(true);
                    }

                    // --- Phase 2: wait for all writers, then verify correctness ---
                    writersFinished.await();

                    List<PrimaryKey> allKeys = buildSortedPrimaryKeySnapshot();
                    ByteBuffer settledQueryBuf = randomVectorFromThreadLocal();
                    Expression settledExpression = Expression.create(index);
                    settledExpression.add(Operator.ANN, settledQueryBuf);

                    Set<Integer> foundAfterSettle = new HashSet<>();
                    try (CloseableIterator<PrimaryKeyWithScore> it = memtableIndex.orderResultsBy(queryContext, allKeys, settledExpression))
                    {
                        while (it.hasNext())
                        {
                            PrimaryKeyWithScore result = it.next();
                            assertNotNull("Null PrimaryKey after writes settled in orderResultsBy()", result.primaryKey());
                            assertTrue("Non-finite score after writes settled in orderResultsBy(): " + result.score(), Float.isFinite(result.score()));

                            // All vector components are drawn from [0, 1) via ThreadLocalRandom.nextFloat(),
                            // so every term in the dot product is non-negative and the sum is strictly positive.
                            // A score of 0f or below would indicate graph corruption, not a valid similarity result.
                            assertTrue("Non-positive score after writes settled in orderResultsBy(): " + result.score(), result.score() > 0f);

                            int pk = Int32Type.instance.compose(result.primaryKey().partitionKey().getKey());
                            foundAfterSettle.add(pk);
                        }
                    }

                    long writerKeysFound = foundAfterSettle.stream().filter(pk -> pk >= 0).count();
                    int expectedMinimum = (int) (totalInserted * RECALL_THRESHOLD);
                    assertTrue("orderResultsBy() returned " + writerKeysFound + " of " + totalInserted + " writer-inserted keys after writes settled (expected at least " + expectedMinimum + ')',
                               writerKeysFound >= expectedMinimum);
                }
                catch (Throwable e)
                {
                    errors.add(e);
                }
            });
        }

        executor.shutdown();
        assertTrue("Timed out waiting for concurrent add() + orderResultsBy()", executor.awaitTermination(60, TimeUnit.SECONDS));
        assertTrue("No reader executed a Phase 1 search during the concurrent write window; increase vectorsPerWriter to widen the write window", phase1Executed.get());

        if (!errors.isEmpty())
        {
            AssertionError failure = new AssertionError("Concurrent add() + orderResultsBy() produced " + errors.size() + " error(s); first: " + errors.get(0));
            errors.forEach(failure::addSuppressed);
            throw failure;
        }
    }

    private PartitionRangeReadCommand createRangeRead(int limit)
    {
        return PartitionRangeReadCommand.create(cfs.metadata(),
                                                FBUtilities.nowInSeconds(),
                                                ColumnFilter.all(cfs.metadata()),
                                                RowFilter.none(),
                                                DataLimits.cqlLimits(limit),
                                                DataRange.allData(cfs.metadata().partitioner));
    }

    private List<PrimaryKey> buildSortedPrimaryKeySnapshot()
    {
        return keyMap.keySet()
                     .stream()
                     .map(dk -> index.hasClustering() ? index.keyFactory().create(dk, Clustering.EMPTY) : index.keyFactory().create(dk))
                     .sorted()
                     .collect(Collectors.toList());
    }

    private ByteBuffer makeSharedVector(int i)
    {
        List<Float> raw = new ArrayList<>(Collections.nCopies(dimensionCount - 1, 0.5f));
        raw.add(i / (float) VECTORS_PER_THREAD);
        return VectorType.getInstance(FloatType.instance, dimensionCount).getSerializer().serialize(raw);
    }

    private Expression generateRandomExpression()
    {
        Expression expression = Expression.create(index);
        expression.add(Operator.ANN, randomVector());
        return expression;
    }

    private ByteBuffer randomVector()
    {
        return randomVector(() -> getRandom().nextFloat());
    }

    private ByteBuffer randomVectorFromThreadLocal()
    {
        return randomVector(() -> ThreadLocalRandom.current().nextFloat());
    }

    private ByteBuffer randomVector(Supplier<Float> supplier)
    {
        List<Float> rawVector = new ArrayList<>(dimensionCount);
        for (int i = 0; i < dimensionCount; i++)
            rawVector.add(supplier.get());
        return VectorType.getInstance(FloatType.instance, dimensionCount).getSerializer().serialize(rawVector);
    }

    private AbstractBounds<PartitionPosition> generateRandomBounds(List<DecoratedKey> keys)
    {
        PartitionPosition leftBound = getRandom().nextBoolean() ? partitioner.getMinimumToken().minKeyBound()
                                                                : keys.get(getRandom().nextIntBetween(0, keys.size() - 1)).getToken().minKeyBound();

        PartitionPosition rightBound = getRandom().nextBoolean() ? partitioner.getMinimumToken().minKeyBound()
                                                                 : keys.get(getRandom().nextIntBetween(0, keys.size() - 1)).getToken().maxKeyBound();

        AbstractBounds<PartitionPosition> keyRange;

        if (leftBound.isMinimum() && rightBound.isMinimum())
            keyRange = new Range<>(leftBound, rightBound);
        else
        {
            if (AbstractBounds.strictlyWrapsAround(leftBound, rightBound))
            {
                PartitionPosition temp = leftBound;
                leftBound = rightBound;
                rightBound = temp;
            }
            if (getRandom().nextBoolean())
                keyRange = new Bounds<>(leftBound, rightBound);
            else if (getRandom().nextBoolean())
                keyRange = new ExcludingBounds<>(leftBound, rightBound);
            else
                keyRange = new IncludingExcludingBounds<>(leftBound, rightBound);
        }
        return keyRange;
    }

    private void addRow(int pk, ByteBuffer value)
    {
        DecoratedKey key = makeKey(cfs.metadata(), pk);
        memtableIndex.add(key, Clustering.EMPTY, value);
        keyMap.put(key, pk);
    }

    /**
     * A memtable that answers {@link Memtable#limitsConcurrentWritesTo} truthfully for the number of writer threads the
     * test will use. With that many writers at or under {@link OnHeapGraph#MAX_CONCURRENT_GRAPH_INSERTS}, OnHeapGraph
     * relies on the memtable and creates no semaphore; with more, it bounds the writers itself.
     */
    private static Memtable mockMemtable(int writers)
    {
        Memtable memtable = Mockito.mock(Memtable.class);
        Mockito.when(memtable.limitsConcurrentWritesTo(Mockito.anyInt())).thenAnswer(invocation -> writers <= (int) invocation.getArgument(0));
        return memtable;
    }

    private DecoratedKey makeKey(TableMetadata table, Integer partitionKey)
    {
        ByteBuffer key = table.partitionKeyType.fromString(partitionKey.toString());
        return table.partitioner.decorateKey(key);
    }
}
