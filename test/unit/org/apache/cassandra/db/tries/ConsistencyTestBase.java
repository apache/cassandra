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

package org.apache.cassandra.db.tries;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;
import java.util.function.Predicate;

import com.google.common.collect.Iterables;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.generateKeys;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;
import static org.junit.Assert.assertTrue;

public abstract class ConsistencyTestBase<C, T extends BaseTrie<C, ?, T>, R extends BaseTrie<C, ?, ?>>
{
    // Note: This should not be run by default with verification to have the higher concurrency of faster writes and reads.

    private static final int COUNT = 30000;
    private static final int PROGRESS_UPDATE = Math.max(1, COUNT / 15);
    private static final int READERS = 8;
    private static final int WALKERS = 2;
    private static final Random rand = new Random();

    /**
     * Force copy every modified cell below the partition/enumeration level. Provides atomicity of mutations within the
     * partition level as well as consistency.
     */
    public final Predicate<InMemoryTrie.NodeFeatures<C>> FORCE_COPY_PARTITION = features -> isPartition(features.content());
    /**
     * Force copy every modified cell below the partition/enumeration level. Provides atomicity of mutations within the
     * partition level as well as consistency.
     */
    public final Predicate<InMemoryTrie.NodeFeatures<TestRangeState>> FORCE_COPY_PARTITION_RANGE_STATE = features -> isPartition(features.content());
    /**
     * Force copy every modified cell below the earliest branching point. Provides atomicity of mutations at any level,
     * but readers/walkers may see inconsistent views of the data, in the sense that older mutations may be missed
     * while newer ones are returned.
     */
    public final static Predicate<InMemoryTrie.NodeFeatures<?>> FORCE_ATOMIC = features -> features.isBranching();
    public final static <Q> Predicate<InMemoryTrie.NodeFeatures<Q>> forceAtomic()
    {
        return (Predicate<InMemoryTrie.NodeFeatures<Q>>) (Predicate<?>) FORCE_ATOMIC;
    }
    /**
     * Do not do any additional copying beyond what is required to build the tries safely for concurrent readers.
     * Mutations may be partially seen by readers, and older mutations may be missed while newer ones are returned.
     */
    public final static Predicate<InMemoryTrie.NodeFeatures<?>> NO_ATOMICITY = features -> false;
    public final static <Q> Predicate<InMemoryTrie.NodeFeatures<Q>> noAtomicity()
    {
        return (Predicate<InMemoryTrie.NodeFeatures<Q>>) (Predicate<?>) NO_ATOMICITY;
    }

    abstract R makeTrie(OpOrder readOrder);

    abstract C value(ByteComparable b, ByteComparable cprefix, ByteComparable c, int add, int seqId);

    abstract C metadata(ByteComparable b);

    abstract String pk(C c);

    abstract String ck(C c);

    abstract int seq(C c);

    abstract int value(C c);

    abstract int updateCount(C c);

    abstract T makeSingleton(ByteComparable b, C content);

    abstract T withRootMetadata(T wrapped, C metadata);

    abstract T merge(Collection<T> tries, Trie.CollectionMergeResolver<C> mergeResolver);

    abstract void apply(R trie,
                        T mutation,
                        InMemoryBaseTrie.UpsertTransformer<C, C> mergeResolver,
                        Predicate<InMemoryTrie.NodeFeatures<C>> forcedCopyChecker) throws TrieSpaceExhaustedException;

    abstract void delete(R trie,
                         ByteComparable deletionPrefix,
                         TestRangeState partitionMarker,
                         RangeTrie<TestRangeState> deletion,
                         InMemoryBaseTrie.UpsertTransformer<C, TestRangeState> mergeResolver,
                         Predicate<InMemoryBaseTrie.NodeFeatures<TestRangeState>> forcedCopyChecker) throws TrieSpaceExhaustedException;

    abstract boolean isPartition(C c);
    boolean isPartition(TestRangeState c)
    {
        if (!(c instanceof TestStateMetadata))
            return false;
        return isPartition(((TestStateMetadata<C>) c).metadata);
    }

    abstract C mergeMetadata(C c1, C c2);
    abstract C deleteMetadata(C existing, int entriesCount);

    // To overridden by deletion branch testing.
    Iterable<Map.Entry<Preencoded, C>> getEntrySet(BaseTrie<C, ?, ?> trie)
    {
        return trie.entrySet();
    }


    abstract void printStats(R trie, Predicate<InMemoryBaseTrie.NodeFeatures<C>> forcedCopyChecker);

    @Test
    public void testConsistentUpdates() throws Exception
    {
        // Check that multi-path updates with below-partition-level copying are safe for concurrent readers,
        // and that content is atomically applied, i.e. that reader see either nothing from the update or all of it,
        // and consistent, i.e. that it is not possible to receive some newer updates while missing
        // older ones. (For example, if the sequence of additions is 3, 1, 5, without this requirement a reader
        // could see an enumeration which lists 3 and 5 but not 1.)
        testUpdateConsistency(3, FORCE_COPY_PARTITION, FORCE_COPY_PARTITION_RANGE_STATE, true, true);
        // Note: using 3 per mutation, so that the first and second update fit in a sparse in-memory trie block.
    }

    @Test
    public void testAtomicUpdates() throws Exception
    {
        // Check that multi-path updates with below-branching-point copying are safe for concurrent readers,
        // and that content is atomically applied, i.e. that reader see either nothing from the update or all of it.
        testUpdateConsistency(3, forceAtomic(), forceAtomic(), true, false);
    }

    @Test
    public void testSafeUpdates() throws Exception
    {
        // Check that multi path updates without additional copying are safe for concurrent readers.
        testUpdateConsistency(3, noAtomicity(), noAtomicity(), false, false);
    }

    @Test
    public void testConsistentSinglePathUpdates() throws Exception
    {
        // Check that single path updates with below-partition-level copying are safe for concurrent readers,
        // and that content is consistent, i.e. that it is not possible to receive some newer updates while missing
        // older ones. (For example, if the sequence of additions is 3, 1, 5, without this requirement a reader
        // could see an enumeration which lists 3 and 5 but not 1.)
        testUpdateConsistency(1, FORCE_COPY_PARTITION, FORCE_COPY_PARTITION_RANGE_STATE, true, true);
    }


    @Test
    public void testAtomicSinglePathUpdates() throws Exception
    {
        // When doing single path updates atomicity comes for free. This only checks that the branching checker is
        // not doing anything funny.
        testUpdateConsistency(1, forceAtomic(), forceAtomic(), true, false);
    }

    @Test
    public void testSafeSinglePathUpdates() throws Exception
    {
        // Check that single path updates without additional copying are safe for concurrent readers.
        testUpdateConsistency(1, noAtomicity(), noAtomicity(), true, false);
    }

    // The generated keys all start with NEXT_COMPONENT, which makes it impossible to test the precise behavior of the
    // partition-level force copying. Strip that byte.
    private static ByteComparable[] skipFirst(ByteComparable[] keys)
    {
        ByteComparable[] result = new ByteComparable[keys.length];
        for (int i = 0; i < keys.length; ++i)
            result[i] = skipFirst(keys[i]);
        return result;
    }

    private static ByteComparable skipFirst(ByteComparable key)
    {
        return v -> {
            var bs = key.asComparableBytes(v);
            int n = bs.next();
            assert n != ByteSource.END_OF_STREAM;
            return bs;
        };
    }

    private static ByteComparable swapTerminator(ByteComparable key, int newTerminator)
    {
        byte[] bytes = key.asByteComparableArray(VERSION);
        bytes[bytes.length - 1] = (byte) newTerminator;
        return ByteComparable.preencoded(VERSION, bytes);
    }

    static class ThreadWithProgressAck extends Thread
    {
        final int threadId;
        final LongUnaryOperator ackWriteProgress;
        final Consumer<LongUnaryOperator> runnable;

        ThreadWithProgressAck(AtomicInteger threadIdx, Consumer<LongUnaryOperator> runnable)
        {
            threadId = threadIdx.getAndIncrement();
            ackWriteProgress = x -> x | (1<<threadId);
            this.runnable = runnable;
        }

        @Override
        public void run()
        {
            runnable.accept(ackWriteProgress);
        }
    }

    public void testUpdateConsistency(int PER_MUTATION,
                                      Predicate<InMemoryTrie.NodeFeatures<C>> forcedCopyChecker,
                                      Predicate<InMemoryTrie.NodeFeatures<TestRangeState>> forcedCopyCheckerRanges,
                                      boolean checkAtomicity,
                                      boolean checkSequence)
    throws Exception
    {
        long seed = rand.nextLong();
        System.out.println("Seed: " + seed);
        rand.setSeed(seed);

        ByteComparable[] ckeys = skipFirst(generateKeys(rand, COUNT));
        ByteComparable[] pkeys = skipFirst(generateKeys(rand, Math.min(100, COUNT / 10)));  // to guarantee repetition

        /*
         * Adds COUNT partitions each with perPartition separate clusterings, where the sum of the values
         * of all clusterings is 0.
         * If the sum for any walk covering whole partitions is non-zero, we have had non-atomic updates.
         */

        OpOrder readOrder = new OpOrder();
        R trie = makeTrie(readOrder);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        List<Thread> threads = new ArrayList<Thread>();
        AtomicBoolean writeCompleted = new AtomicBoolean(false);
        AtomicInteger writeProgress = new AtomicInteger(0);
        AtomicLong writeProgressAck = new AtomicLong(0);
        AtomicInteger threadIdx = new AtomicInteger(0);

        Consumer<LongUnaryOperator> walkTrie = ackWriteProgress ->
        {
            while (!writeCompleted.get())
            {
                try
                {
                    writeProgressAck.getAndUpdate(ackWriteProgress);
                    int min = writeProgress.get();
                    try (OpOrder.Group group = readOrder.start())
                    {
                        Iterable<Map.Entry<Preencoded, C>> entries = getEntrySet(trie);
                        checkEntries("", min, true, checkAtomicity, false, PER_MUTATION, entries);
                    }
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    errors.add(t);
                }
            }
        };

        Consumer<LongUnaryOperator> readTrie = ackWriteProgress ->
        {
            Random r = ThreadLocalRandom.current();
            while (!writeCompleted.get())
            {
                try
                {
                    {
                        writeProgressAck.getAndUpdate(ackWriteProgress);
                        ByteComparable key = pkeys[r.nextInt(pkeys.length)];
                        int min = writeProgress.get() / (pkeys.length * PER_MUTATION) * PER_MUTATION;
                        Iterable<Map.Entry<Preencoded, C>> entries;

                        try (OpOrder.Group group = readOrder.start())
                        {
                            var tail = trie.tailTrie(key);
                            if (tail != null)
                            {
                                entries = getEntrySet(tail);
                                checkEntries(" in tail " + key.byteComparableAsString(VERSION), min, false, checkAtomicity, checkSequence, PER_MUTATION, entries);
                            }
                            else
                                Assert.assertEquals("Trie key not found when there should be data for it", 0, min);
                        }

                        try (OpOrder.Group group = readOrder.start())
                        {
                            entries = getEntrySet(trie.subtrie(key, key));
                            checkEntries(" in branch " + key.byteComparableAsString(VERSION), min, true, checkAtomicity, checkSequence, PER_MUTATION, entries);
                        }
                    }
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    errors.add(t);
                }
            }
        };

        for (int i = 0; i < WALKERS; ++i)
            threads.add(new ThreadWithProgressAck(threadIdx, walkTrie));

        for (int i = 0; i < READERS; ++i)
            threads.add(new ThreadWithProgressAck(threadIdx, readTrie));

        byte[] choices = new byte[COUNT / PER_MUTATION];
        rand.nextBytes(choices);
        threads.add(new Thread()
        {
            public void run()
            {
                final Trie.CollectionMergeResolver<C> mergeResolver = new Trie.CollectionMergeResolver<C>()
                {
                    @Override
                    public C resolve(C c1, C c2)
                    {
                        if (isPartition(c1) && isPartition(c2))
                            return mergeMetadata(c1, c2);
                        throw new AssertionError("Test error, keys should be distinct.");
                    }

                    public C resolve(Collection<C> contents)
                    {
                        return contents.stream().reduce(this::resolve).get();
                    }
                };

                try
                {
                    // Insert the data.
                    int lastUpdate = 0;
                    for (int i = 0; i < COUNT; i += PER_MUTATION)
                    {
                        ByteComparable b = pkeys[(i / PER_MUTATION) % pkeys.length];
                        C partitionMarker = metadata(b);
                        ByteComparable cprefix = null;
                        if ((choices[i / PER_MUTATION] & 1) == 1)
                            cprefix = ckeys[i]; // Also test branching point below the partition level

                        List<T> sources = new ArrayList<>();
                        for (int j = 0; j < PER_MUTATION; ++j)
                        {

                            ByteComparable k = ckeys[i + j];
                            T row = makeSingleton(k,
                                                  value(b, cprefix, k,
                                                        j == 0 ? -PER_MUTATION + 1 : 1,
                                                        (i / PER_MUTATION / pkeys.length) * PER_MUTATION + j));

                            if (cprefix != null)
                                row = row.prefixedBy(cprefix);

                            row = withRootMetadata(row, partitionMarker);
                            row = row.prefixedBy(b);
                            sources.add(row);
                        }

                        final T mutation = merge(sources, mergeResolver);

                        apply(trie, mutation,
                              (existing, update) -> existing == null ? update : mergeResolver.resolve(existing, update),
                              forcedCopyChecker);

                        if (i >= pkeys.length * PER_MUTATION && i - lastUpdate >= PROGRESS_UPDATE)
                        {
                            writeProgress.set(i);
                            lastUpdate = i;
                        }
                    }

                    writeProgress.set(COUNT);
                    printStats(trie, forcedCopyChecker);
                    Thread.sleep(100); // Let the threads check the completed state too.

                    // Make sure we can read everything we have inserted from this thread (if this fails, the problem
                    // is not concurrency).
                    try (OpOrder.Group group = readOrder.start())
                    {
                        Iterable<Map.Entry<Preencoded, C>> entries = getEntrySet(trie);
                        checkEntries("", COUNT, true, checkAtomicity, false, PER_MUTATION, entries);
                    }

                    InMemoryTrie.UpsertTransformer<C, TestRangeState> deleteResolver = (existing, update) ->
                    {
                        if (update instanceof TestStateMetadata)
                        {
                            assert isPartition(existing);
                            return deleteMetadata(existing, PER_MUTATION);
                        }
                        return null;
                    };

                    // Now delete the data in the reverse order of the insertion to satisfy the same constraints.
                    for (int i = COUNT - PER_MUTATION; i >= 0; i -= PER_MUTATION)
                    {
                        if (i < writeProgress.get())
                        {
                            // Reduce the writeProgress so that we can start deleting a batch.
                            writeProgress.set(writeProgress.get() - PROGRESS_UPDATE);
                            // Wait until all reader threads have completed the current pass.
                            writeProgressAck.set(0);
                            while (writeProgressAck.get() + 1 < 1 << threadIdx.get())
                                Thread.yield();
                        }

                        ByteComparable b = pkeys[(i / PER_MUTATION) % pkeys.length];
                        TestRangeState partitionMarker = new TestStateMetadata<>(metadata(b));
                        List<RangeTrie<TestRangeState>> ranges = new ArrayList<>();
                        ByteComparable cprefix = null;
                        if ((choices[i / PER_MUTATION] & 3) == 3)
                        {
                            // Delete the whole branch in one range
                            ranges.add(makeRangeCovering(ckeys[i]));
                        }
                        else
                        {
                            // A range for each entry
                            if ((choices[i / PER_MUTATION] & 1) == 1)
                                cprefix = ckeys[i];
                            for (int j = 0; j < PER_MUTATION; ++j)
                                ranges.add(makeRangeCovering(ckeys[i + j]));
                        }

                        RangeTrie<TestRangeState> deletion = RangeTrie.merge(ranges, Trie.throwingResolver());
                        if (cprefix != null)
                            deletion = deletion.prefixedBy(cprefix);

                        delete(trie, b, partitionMarker, deletion, deleteResolver, forcedCopyCheckerRanges);
                    }

                    writeProgress.set(0);
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    errors.add(t);
                }
                finally
                {
                    writeCompleted.set(true);
                }
            }
        });

        for (Thread t : threads)
            t.start();

        for (Thread t : threads)
            t.join();

        printStats(trie, forcedCopyChecker);

        Assert.assertEquals("Writer did not complete", 0, writeProgress.get());

        assertTrue(Iterables.isEmpty(getEntrySet(trie)));

        if (!errors.isEmpty())
        {
            System.out.println(trie.dump());
            for (byte b : choices)
                switch (b & 3)
                {
                    case 0:
                    case 2:
                        System.out.print(".");
                        break;
                    case 1:
                        System.out.print("-");
                        break;
                    case 3:
                        System.out.print("#");
                        break;
                }
            System.out.println();
            Assert.fail("Got errors:\n" + errors);
        }
    }

    private static RangeTrie<TestRangeState> makeRangeCovering(ByteComparable cprefix)
    {
        return RangeTrie.range(swapTerminator(cprefix, ByteSource.LT_NEXT_COMPONENT),
                               swapTerminator(cprefix, ByteSource.GT_NEXT_COMPONENT),
                               VERSION,
                               TestRangeState.COVERED);
    }

    public void checkEntries(String location,
                             int min,
                             boolean usePk,
                             boolean checkAtomicity,
                             boolean checkConsecutiveIds,
                             int PER_MUTATION,
                             Iterable<Map.Entry<Preencoded, C>> entries)
    {
        long sum = 0;
        int count = 0;
        long idSum = 0;
        long idMax = 0;
        int updateCount = 0;
        for (var en : entries)
        {
            String path = en.getKey().byteComparableAsString(VERSION);
            final C v = en.getValue();
            if (isPartition(v))
            {
                Assert.assertEquals("Partition metadata" + location, (usePk ? pk(v) : ""), path);
                updateCount += updateCount(v);
                continue;
            }
            String valueKey = (usePk ? pk(v) : "") + ck(v);
            Assert.assertEquals(location, valueKey, path);
            ++count;
            sum += value(v);
            int seq = seq(v);
            idSum += seq;
            if (seq > idMax)
                idMax = seq;
        }

        assertTrue("Values" + location + " should be at least " + min + ", got " + count, min <= count);

        if (checkAtomicity)
        {
            // If mutations apply atomically, the row count is always a multiple of the mutation size...
            assertTrue("Values" + location + " should be a multiple of " + PER_MUTATION + ", got " + count, count % PER_MUTATION == 0);
            // ... and the sum of the values is 0 (as the sum for each individual mutation is 0).
            Assert.assertEquals("Value sum" + location, 0, sum);
        }

        if (checkConsecutiveIds)
        {
            // The update count reflected in the partition metadata must match the row count.
            Assert.assertEquals("Update count" + location, count, updateCount);
            // If mutations apply consistently for the partition, for any row we see we have to have seen all rows that
            // were applied before that. In other words, the id sum should be the sum of the integers from 1 to the
            // highest id seen in the partition.
            Assert.assertEquals("Id sum" + location, idMax * (idMax + 1) / 2, idSum);
        }
    }

    static abstract class TestRangeState implements RangeState<TestRangeState>
    {
        static final TestRangeState COVERED = new TestRangeCoveringState();
        static final TestRangeState RANGE_START = new TestRangeBoundary(Direction.FORWARD);
        static final TestRangeState RANGE_END = new TestRangeBoundary(Direction.REVERSE);

        public static TestRangeState combine(TestRangeState existing, TestRangeState incoming)
        {
            // This can only be called for TestRangeBoundary as other types should not end up in any persisted tries.
            TestRangeBoundary be = (TestRangeBoundary) existing;
            TestRangeBoundary bi = (TestRangeBoundary) incoming;
            if (be == null)
                return bi;
            if (be.direction == bi.direction)
                return be;
            return null;    // switch from covered to covered, we should not store anything
        }
    }

    static class TestRangeCoveringState extends TestRangeState
    {
        @Override
        public boolean isBoundary()
        {
            return false;
        }

        @Override
        public TestRangeCoveringState precedingState(Direction direction)
        {
            return this;
        }

        @Override
        public TestRangeState restrict(boolean applicableBefore, boolean applicableAfter)
        {
            throw new AssertionError();
        }

        @Override
        public TestRangeState asBoundary(Direction direction)
        {
            return direction.isForward() ? RANGE_START : RANGE_END;
        }

        @Override
        public String toString()
        {
            return "COVERING";
        }
    }

    static class TestRangeBoundary extends TestRangeState
    {
        final Direction direction;

        TestRangeBoundary(Direction direction)
        {
            this.direction = direction;
        }

        @Override
        public boolean isBoundary()
        {
            return true;
        }

        @Override
        public TestRangeState precedingState(Direction direction)
        {
            return direction == this.direction ? null : COVERED;
        }

        @Override
        public TestRangeState restrict(boolean applicableBefore, boolean applicableAfter)
        {
            if (direction.isForward() && !applicableBefore || !direction.isForward() && !applicableAfter)
                return null;
            return this;
        }

        @Override
        public TestRangeState asBoundary(Direction direction)
        {
            throw new AssertionError();
        }

        @Override
        public String toString()
        {
            return direction.isForward() ? "START" : "END";
        }
    }

    static class TestStateMetadata<C> extends TestRangeState
    {
        final C metadata;

        TestStateMetadata(C metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public boolean isBoundary()
        {
            return true;
        }

        @Override
        public TestRangeState precedingState(Direction direction)
        {
            return null;
        }

        @Override
        public TestRangeState restrict(boolean applicableBefore, boolean applicableAfter)
        {
            return this; // metadata should survive ranges
        }

        @Override
        public TestRangeState asBoundary(Direction direction)
        {
            throw new AssertionError();
        }

        @Override
        public String toString()
        {
            return metadata.toString();
        }
    }
}
