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

package org.apache.cassandra.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.Snapshot;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.concurrent.CassandraThread;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.MonotonicClockTranslation;

import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.DEFAULT_BUCKET_COUNT;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.DEFAULT_STRIPE_COUNT;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.DEFAULT_ZERO_CONSIDERATION;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.HALF_TIME_IN_S;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.MEAN_LIFETIME_IN_S;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.findIndex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class HistogramUpdateBuffersTest
{
    private static final long[] OFFSETS = DecayingEstimatedHistogramReservoir.DEFAULT_WITHOUT_ZERO_BUCKET_OFFSETS;
    /** Fixed, so a failing iteration's seed -- printed with the failure -- reproduces it on its own. */
    private static final long AGGREGATE_BASE_SEED = 0xB0FFE2;

    @Before
    public void before()
    {
        // ant sets the window to zero for every test JVM; do it here too so a run from an IDE behaves the same
        HistogramUpdateBuffers.relaxedFlushWindowNanos = 0;
        // earlier tests share this thread, so start from an empty ring or the count below is not a drain's worth
        HistogramUpdateBuffers.flush();
    }

    /**
     * A metrics read settles for a flush that finished moments ago rather than walking every thread's buffer again,
     * so that a scrape of thousands of histograms drains once instead of once per histogram.
     */
    @Test
    public void testRelaxedFlushSettlesForARecentFlush()
    {
        TickingClock clock = new TickingClock();
        DecayingEstimatedHistogramReservoir reservoir = newReservoir(clock);
        long value = 3;

        HistogramUpdateBuffers.relaxedFlushWindowNanos = TimeUnit.MINUTES.toNanos(10);
        try
        {
            HistogramUpdateBuffers.flush(); // the read below now has a flush recent enough to lean on
            reservoir.update(value);
            assertEquals("a read inside the window must not drain again", 0, sum(reservoir.getSnapshot().getValues()));

            HistogramUpdateBuffers.flush(); // a read that has to be exact asks for a flush of its own
            assertEquals(1, sum(reservoir.getSnapshot().getValues()));
        }
        finally
        {
            HistogramUpdateBuffers.relaxedFlushWindowNanos = 0;
        }
    }

    @Test
    public void testEncodingRoundTrip()
    {
        int[] deltas = { 0, 1, HistogramUpdateBuffers.MAX_DELTA };
        int[] buckets = { 0, 1, 164, DecayingEstimatedHistogramReservoir.MAX_BUCKET_COUNT };
        // the id field owns the sign bit, so cover ids either side of it as well as the extremes
        int[] ids = { 0, 1, 12345, (1 << 19) - 1, 1 << 19, HistogramUpdateBuffers.MAX_ID };

        for (int id : ids)
        {
            for (int bucket : buckets)
            {
                for (int delta : deltas)
                {
                    int entry = HistogramUpdateBuffers.encode(id, bucket, delta);
                    assertEquals(id, HistogramUpdateBuffers.reservoirIdOf(entry));
                    assertEquals(bucket, HistogramUpdateBuffers.bucketOf(entry));
                    assertEquals(delta, HistogramUpdateBuffers.timeDeltaOf(entry));

                    long aggregated = ((long) entry << Integer.SIZE) | 0xFFFFFFFFL;
                    assertEquals(entry, HistogramUpdateBuffers.entryOf(aggregated));
                    assertEquals(id, HistogramUpdateBuffers.idOfAggregated(aggregated));
                }
            }
        }
    }

    /**
     * Two reservoirs whose ids land in the same slot of the id directory, so one has to probe past the other. The
     * slot to clear afterwards is then the probed one rather than {@code hash(id)}, which is what
     * {@code spareSlotPerReservoir} exists for -- aggregating twice on the same scratch is what catches getting
     * that wrong.
     */
    @Test
    public void testAggregateWithCollidingReservoirIds()
    {
        int[] ids = findCollidingReservoirIds();
        assertEquals("the two ids must share a slot", HistogramUpdateBuffers.hash(ids[0]), HistogramUpdateBuffers.hash(ids[1]));

        HistogramUpdateBuffers.Scratch scratch = new HistogramUpdateBuffers.Scratch();
        int[] batch = new int[600];
        for (int i = 0; i < batch.length; i++)
            batch[i] = HistogramUpdateBuffers.encode(ids[i % 2], (i / 2) % 7, i % (HistogramUpdateBuffers.MAX_DELTA + 1));

        assertAggregatesLikeReference(scratch, batch, "colliding ids " + ids[0] + " and " + ids[1]);
        // a second batch over the same scratch: only correct if the probed slot was cleared, not hash(id)
        assertAggregatesLikeReference(scratch, batch, "colliding ids, second aggregate");
    }

    /**
     * Two distinct entries of one reservoir that hash to the same slot of the count table, so the second has to
     * probe past the first instead of being counted as a repeat of it.
     */
    @Test
    public void testAggregateWithCollidingEntries()
    {
        int id = 12345;
        int[] entries = findCollidingEntries(id);
        assertEquals("the two entries must share a slot", HistogramUpdateBuffers.hash(entries[0]), HistogramUpdateBuffers.hash(entries[1]));

        HistogramUpdateBuffers.Scratch scratch = new HistogramUpdateBuffers.Scratch();
        // uneven repeats, so counting the two as one entry cannot accidentally produce the expected totals
        int[] batch = new int[300];
        for (int i = 0; i < batch.length; i++)
            batch[i] = entries[i % 3 == 0 ? 1 : 0];

        assertAggregatesLikeReference(scratch, batch, "colliding entries " + entries[0] + " and " + entries[1]);
        assertAggregatesLikeReference(scratch, batch, "colliding entries, second aggregate");
    }

    /**
     * Random batches against a plain map of counts. Ids are drawn from the whole range, so entries either side of
     * the sign bit are covered, and one scratch serves every iteration so a table left dirty shows up as a wrong
     * count in a later one.
     */
    @Test
    public void testAggregateMatchesReferenceCounting()
    {
        HistogramUpdateBuffers.Scratch scratch = new HistogramUpdateBuffers.Scratch();
        for (int iteration = 0; iteration < 200; iteration++)
        {
            long seed = AGGREGATE_BASE_SEED + iteration;
            Random random = new Random(seed);

            int[] ids = new int[1 + random.nextInt(64)];
            for (int i = 0; i < ids.length; i++)
                ids[i] = random.nextInt(HistogramUpdateBuffers.MAX_ID + 1);
            int buckets = 1 + random.nextInt(40);

            int[] batch = new int[1 + random.nextInt(HistogramUpdateBuffers.CAPACITY)];
            for (int i = 0; i < batch.length; i++)
                batch[i] = HistogramUpdateBuffers.encode(ids[random.nextInt(ids.length)],
                                                         random.nextInt(buckets),
                                                         random.nextInt(HistogramUpdateBuffers.MAX_DELTA + 1));

            assertAggregatesLikeReference(scratch, batch, "seed " + seed);
        }
    }

    /**
     * Checks the whole contract of {@link HistogramUpdateBuffers#aggregate} against a plain map: one (entry, count)
     * pair per distinct entry, one contiguous range per reservoir, and both tables left empty for the next batch.
     */
    private static void assertAggregatesLikeReference(HistogramUpdateBuffers.Scratch scratch, int[] batch, String context)
    {
        Map<Integer, Integer> expectedCounts = new HashMap<>();
        for (int entry : batch)
            expectedCounts.merge(entry, 1, Integer::sum);

        System.arraycopy(batch, 0, scratch.entries, 0, batch.length);
        int distinct = HistogramUpdateBuffers.aggregate(scratch, batch.length);
        assertEquals(context + ": distinct entries", expectedCounts.size(), distinct);

        Map<Integer, Integer> actualCounts = new HashMap<>();
        List<Integer> idRuns = new ArrayList<>();
        int previousId = -1;
        for (int i = 0; i < distinct; i++)
        {
            long pair = scratch.aggregatedEntries[i];
            int entry = HistogramUpdateBuffers.entryOf(pair);
            int id = HistogramUpdateBuffers.reservoirIdOf(entry);
            assertEquals(context + ": id read off the pair at " + i, id, HistogramUpdateBuffers.idOfAggregated(pair));
            assertNull(context + ": entry " + entry + " emitted more than once",
                       actualCounts.put(entry, HistogramUpdateBuffers.countOf(pair)));
            if (id != previousId)
            {
                idRuns.add(id);
                previousId = id;
            }
        }

        assertEquals(context + ": counts per entry", expectedCounts, actualCounts);
        assertEquals(context + ": every reservoir must own exactly one contiguous range",
                     new HashSet<>(idRuns).size(), idRuns.size());

        for (int i = 0; i < scratch.entryToCountTable.length; i++)
            assertEquals(context + ": count table slot " + i + " was left occupied", 0L, scratch.entryToCountTable[i]);
        for (int i = 0; i < scratch.reservoirIds.length; i++)
            assertEquals(context + ": id table slot " + i + " was left occupied", 0, scratch.reservoirIds[i]);
    }

    /** Two distinct reservoir ids sharing a slot; the tables hold fewer slots than there are ids, so a pair exists. */
    private static int[] findCollidingReservoirIds()
    {
        Map<Integer, Integer> bySlot = new HashMap<>();
        for (int id = 0; id <= HistogramUpdateBuffers.MAX_ID; id++)
        {
            Integer previous = bySlot.putIfAbsent(HistogramUpdateBuffers.hash(id), id);
            if (previous != null)
                return new int[]{ previous, id };
        }
        throw new AssertionError("no two reservoir ids share a slot");
    }

    /** Two distinct entries of one reservoir sharing a slot. */
    private static int[] findCollidingEntries(int id)
    {
        Map<Integer, Integer> bySlot = new HashMap<>();
        for (int bucket = 0; bucket <= DecayingEstimatedHistogramReservoir.MAX_BUCKET_COUNT; bucket++)
        {
            for (int delta = 0; delta <= HistogramUpdateBuffers.MAX_DELTA; delta++)
            {
                int entry = HistogramUpdateBuffers.encode(id, bucket, delta);
                Integer previous = bySlot.putIfAbsent(HistogramUpdateBuffers.hash(entry), entry);
                if (previous != null)
                    return new int[]{ previous, entry };
            }
        }
        throw new AssertionError("no two entries of reservoir " + id + " share a slot");
    }

    @Test
    public void testEachReservoirIsAppliedInOneCallPerDrain()
    {
        TickingClock clock = new TickingClock();
        int reservoirCount = 16;
        int updatesEach = HistogramUpdateBuffers.CAPACITY / reservoirCount;
        List<CountingReservoir> reservoirs = new ArrayList<>();
        for (int i = 0; i < reservoirCount; i++)
            reservoirs.add(new CountingReservoir(clock));

        // round-robin, so a reservoir's entries end up scattered the length of the ring -- the arrangement the
        // grouping pass has to undo, and the one a positional compaction would leave in pieces
        for (int round = 0; round < updatesEach; round++)
            for (CountingReservoir reservoir : reservoirs)
                reservoir.update(1 + round % 40);

        HistogramUpdateBuffers.flush();

        for (int i = 0; i < reservoirCount; i++)
            assertEquals("reservoir " + i + " was applied in more than one range", 1, reservoirs.get(i).calls);
    }

    /** Counts how many ranges a flush splits this reservoir into. */
    private static class CountingReservoir extends DecayingEstimatedHistogramReservoir
    {
        int calls;

        CountingReservoir(MonotonicClock clock)
        {
            super(DEFAULT_ZERO_CONSIDERATION, DEFAULT_BUCKET_COUNT, DEFAULT_STRIPE_COUNT, clock);
        }

        @Override
        void applyBufferedUpdates(long[] entries, int from, int to, long ownerThreadId, long baselineSeconds, long[] weights)
        {
            calls++;
            super.applyBufferedUpdates(entries, from, to, ownerThreadId, baselineSeconds, weights);
        }
    }

    /**
     * A {@link CassandraThread} keeps its buffer in a field rather than in the thread local, and releases it from
     * {@code run()}. That release has to both apply what is buffered and deregister the buffer, or a flush would go
     * on visiting dead threads forever.
     */
    @Test
    public void testCassandraThreadReleasesItsBufferOnDeath() throws InterruptedException
    {
        TickingClock clock = new TickingClock();
        DecayingEstimatedHistogramReservoir reservoir = newReservoir(clock);
        int updates = 100;
        long value = 7;
        // the buffer the dying thread owned, rather than a count of all buffers: other tests leave buffers of dead
        // plain threads registered until the GC gets to them, so a global count moves on its own
        AtomicReference<HistogramUpdateBuffers.Buffer> ownBuffer = new AtomicReference<>();

        Thread thread = new CassandraThread(() -> {
            for (int i = 0; i < updates; i++)
                reservoir.update(value);
            ownBuffer.set(((CassandraThread) Thread.currentThread()).getHistogramUpdateBuffer());
        });
        thread.setName("histogram-buffer-cassandra-updater");
        thread.start();
        thread.join(TimeUnit.MINUTES.toMillis(1));
        assertFalse("updater thread did not finish", thread.isAlive());

        assertFalse("buffer of the dead thread is still registered", HistogramUpdateBuffers.isRegistered(ownBuffer.get()));
        // and deregistering must not have discarded what the buffer still held
        assertEquals(updates, reservoir.getSnapshot().getValues()[findIndex(OFFSETS, value)]);
    }

    /**
     * Entries carry only {@link HistogramUpdateBuffers#MAX_DELTA} seconds of range, so a buffer that outlives its
     * window has to re-baseline. That must not disturb the weights: the batches below span many windows, yet every
     * entry must still be weighted by the second it was actually recorded at.
     */
    @Test
    public void testUpdatesSpanningSeveralBaselines()
    {
        TickingClock clock = new TickingClock();
        DecayingEstimatedHistogramReservoir reservoir = newReservoir(clock);

        int batch = 20;
        int step = HistogramUpdateBuffers.MAX_DELTA + 1;
        long value = 5;
        long expectedWeight = 0;
        long expectedCount = 0;
        for (long second = 0; second < HALF_TIME_IN_S; second += step)
        {
            for (int i = 0; i < batch; i++)
                reservoir.update(value);
            expectedWeight += batch * Math.round(Math.exp(second / MEAN_LIFETIME_IN_S));
            expectedCount += batch;
            clock.addSeconds(step);
        }

        assertEquals(expectedCount, reservoir.getSnapshot().getValues()[findIndex(OFFSETS, value)]);
        assertEquals(expectedWeight, decayingBucketValue(reservoir, value));
    }

    @Test
    public void testInterleavedUpdatesOfSeveralReservoirs()
    {
        TickingClock clock = new TickingClock();
        // more reservoirs than one buffer holds updates, all fed from a single thread, so a flush has to split its
        // batch into many per-reservoir ranges
        int reservoirCount = HistogramUpdateBuffers.CAPACITY / 8;
        int updatesPerValue = 30;
        long[] values = { 1, 17, 500, 1_000_000 };

        List<DecayingEstimatedHistogramReservoir> reservoirs = new ArrayList<>();
        for (int i = 0; i < reservoirCount; i++)
            reservoirs.add(newReservoir(clock));

        for (int round = 0; round < updatesPerValue; round++)
        {
            for (long value : values)
            {
                for (DecayingEstimatedHistogramReservoir reservoir : reservoirs)
                    reservoir.update(value);
            }
        }

        for (DecayingEstimatedHistogramReservoir reservoir : reservoirs)
        {
            long[] counts = reservoir.getSnapshot().getValues();
            for (long value : values)
                assertEquals("value " + value, updatesPerValue, counts[findIndex(OFFSETS, value)]);
            assertEquals(updatesPerValue * (long) values.length, sum(counts));
        }
    }

    @Test
    public void testConcurrentUpdates() throws InterruptedException
    {
        TickingClock clock = new TickingClock();
        int threadCount = 8;
        int reservoirCount = 4;
        // enough per thread to wrap the ring several times, so most updates are applied by the owner itself
        int updatesPerThread = HistogramUpdateBuffers.CAPACITY * 5;
        long value = 123;

        List<DecayingEstimatedHistogramReservoir> reservoirs = new ArrayList<>();
        for (int i = 0; i < reservoirCount; i++)
            reservoirs.add(newReservoir(clock));

        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadCount; i++)
        {
            Thread thread = new Thread(() -> {
                awaitUninterruptibly(start);
                for (int u = 0; u < updatesPerThread; u++)
                    reservoirs.get(u % reservoirCount).update(value);
            }, "histogram-buffer-updater-" + i);
            thread.start();
            threads.add(thread);
        }
        start.countDown();
        for (Thread thread : threads)
            thread.join(TimeUnit.MINUTES.toMillis(1));

        long expected = (long) threadCount * updatesPerThread / reservoirCount;
        for (DecayingEstimatedHistogramReservoir reservoir : reservoirs)
        {
            long[] counts = reservoir.getSnapshot().getValues();
            assertEquals(expected, counts[findIndex(OFFSETS, value)]);
            assertEquals(expected, sum(counts));
        }
    }

    @Test
    public void testUpdatesOfADeadThreadAreApplied() throws InterruptedException
    {
        TickingClock clock = new TickingClock();
        DecayingEstimatedHistogramReservoir reservoir = newReservoir(clock);
        int updates = 100;
        long value = 7;

        Thread thread = new Thread(() -> {
            for (int i = 0; i < updates; i++)
                reservoir.update(value);
        }, "histogram-buffer-dying-updater");
        thread.start();
        thread.join(TimeUnit.MINUTES.toMillis(1));

        long[] counts = reservoir.getSnapshot().getValues();
        assertEquals(updates, counts[findIndex(OFFSETS, value)]);
        assertEquals(updates, sum(counts));
    }

    /**
     * A buffered update has to be weighted by the second it was recorded at. Two equally sized batches half a
     * decay period apart must therefore end up with the later one weighing exactly twice as much, even though both
     * are drained by the same flush after the clock has already moved on.
     */
    @Test
    public void testBufferedUpdatesKeepTheirOwnDecayWeight()
    {
        TickingClock clock = new TickingClock();
        DecayingEstimatedHistogramReservoir reservoir = newReservoir(clock);

        int batch = 100;
        long oldValue = 5;
        long recentValue = 500;
        for (int i = 0; i < batch; i++)
            reservoir.update(oldValue);
        clock.addSeconds(HALF_TIME_IN_S);
        for (int i = 0; i < batch; i++)
            reservoir.update(recentValue);

        Snapshot snapshot = reservoir.getSnapshot();
        // the non-decaying buckets ignore the weights altogether
        assertEquals(batch, snapshot.getValues()[findIndex(OFFSETS, oldValue)]);
        assertEquals(batch, snapshot.getValues()[findIndex(OFFSETS, recentValue)]);
        // half a decay period later the newer batch weighs exp(ln 2) == 2 as much as the older one
        assertEquals(batch, decayingBucketValue(reservoir, oldValue));
        assertEquals(2L * batch, decayingBucketValue(reservoir, recentValue));
    }

    private static long decayingBucketValue(DecayingEstimatedHistogramReservoir reservoir, long value)
    {
        int index = findIndex(OFFSETS, value);
        long sum = 0;
        for (int stripe = 0; stripe < reservoir.stripeCount(); stripe++)
            sum += reservoir.stripedBucketValue(reservoir.stripedIndex(index, stripe), true);
        return sum;
    }

    private static DecayingEstimatedHistogramReservoir newReservoir(MonotonicClock clock)
    {
        return new DecayingEstimatedHistogramReservoir(DEFAULT_ZERO_CONSIDERATION, DEFAULT_BUCKET_COUNT, DEFAULT_STRIPE_COUNT, clock);
    }

    private static long sum(long[] values)
    {
        long sum = 0;
        for (long value : values)
            sum += value;
        return sum;
    }

    private static void awaitUninterruptibly(CountDownLatch latch)
    {
        try
        {
            latch.await();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    /** A clock that only moves when the test tells it to, so forward-decay weights are exactly predictable. */
    private static class TickingClock implements MonotonicClock
    {
        private volatile long tick;

        void addSeconds(long seconds)
        {
            tick += TimeUnit.SECONDS.toNanos(seconds);
        }

        @Override
        public long now()
        {
            return tick;
        }

        @Override
        public long error()
        {
            return 0;
        }

        @Override
        public MonotonicClockTranslation translate()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isAfter(long instant)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isAfter(long now, long instant)
        {
            throw new UnsupportedOperationException();
        }
    }
}
