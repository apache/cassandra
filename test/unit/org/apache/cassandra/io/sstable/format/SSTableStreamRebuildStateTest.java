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

package org.apache.cassandra.io.sstable.format;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.apache.cassandra.io.sstable.format.SSTableStreamRebuildState.State;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SSTableStreamRebuildStateTest
{
    @Test
    public void startsNormal()
    {
        SSTableStreamRebuildState s = new SSTableStreamRebuildState();
        assertEquals(State.NORMAL, s.state());
        assertEquals(0, s.zcsStreamCount());
    }

    @Test
    public void multipleStreamsAllowedAndCounted()
    {
        SSTableStreamRebuildState s = new SSTableStreamRebuildState();
        assertTrue(s.tryBeginStreaming());
        assertTrue(s.tryBeginStreaming());
        assertEquals(State.ZCS_STREAMING, s.state());
        assertEquals(2, s.zcsStreamCount());

        s.endStreaming();
        assertEquals(State.ZCS_STREAMING, s.state());
        assertEquals(1, s.zcsStreamCount());

        s.endStreaming();
        assertEquals(State.NORMAL, s.state());
        assertEquals(0, s.zcsStreamCount());
    }

    @Test
    public void rebuildBlockedWhileStreaming()
    {
        SSTableStreamRebuildState s = new SSTableStreamRebuildState();
        assertTrue(s.tryBeginStreaming());
        assertFalse(s.tryBeginRebuild());
        assertEquals(State.ZCS_STREAMING, s.state());
    }

    @Test
    public void streamingBlockedWhileRebuilding()
    {
        SSTableStreamRebuildState s = new SSTableStreamRebuildState();
        assertTrue(s.tryBeginRebuild());
        assertFalse(s.tryBeginStreaming());
        assertEquals(State.REBUILDING, s.state());
        assertEquals(0, s.zcsStreamCount());
    }

    @Test
    public void rebuildExcludesSecondRebuild()
    {
        SSTableStreamRebuildState s = new SSTableStreamRebuildState();
        assertTrue(s.tryBeginRebuild());
        assertFalse(s.tryBeginRebuild());
    }

    @Test
    public void rebuildResetsToNormal()
    {
        SSTableStreamRebuildState s = new SSTableStreamRebuildState();
        assertTrue(s.tryBeginRebuild());
        s.endRebuild();
        assertEquals(State.NORMAL, s.state());
        assertTrue(s.tryBeginStreaming());
    }

    @Test
    public void endIsDefensiveAgainstOverRelease()
    {
        SSTableStreamRebuildState s = new SSTableStreamRebuildState();
        s.endStreaming(); // no-op, must not go negative
        s.endRebuild();   // no-op
        assertEquals(State.NORMAL, s.state());
        assertEquals(0, s.zcsStreamCount());
    }

    /**
     * Under contention many threads race to begin a rebuild of the same sstable. Because a rebuild is exclusive,
     * exactly one attempt must win; every other attempt must observe the in-flight rebuild and fail. Covers the
     * "second concurrent rebuild attempt fails while one is in flight" scenario under real contention rather than
     * the sequential check in {@link #rebuildExcludesSecondRebuild()}.
     */
    @Test
    public void concurrentRebuildAttempts_onlyOneWins() throws Exception
    {
        for (int round = 0; round < 200; round++)
        {
            SSTableStreamRebuildState s = new SSTableStreamRebuildState();
            int threads = 8;
            CyclicBarrier start = new CyclicBarrier(threads);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            try
            {
                AtomicInteger winners = new AtomicInteger();
                Future<?>[] futures = new Future<?>[threads];
                for (int i = 0; i < threads; i++)
                {
                    futures[i] = pool.submit(() -> {
                        awaitBarrier(start);
                        if (s.tryBeginRebuild())
                            winners.incrementAndGet();
                    });
                }
                for (Future<?> f : futures)
                    f.get(30, TimeUnit.SECONDS);

                assertEquals("exactly one rebuild attempt must win the race", 1, winners.get());
                assertEquals(State.REBUILDING, s.state());
                assertFalse("streaming must be blocked while the winning rebuild is in flight", s.tryBeginStreaming());

                s.endRebuild();
                assertEquals(State.NORMAL, s.state());
            }
            finally
            {
                pool.shutdownNow();
            }
        }
    }

    /**
     * Concurrent entire-sstable streams of the same sstable must all be admitted and reference counted correctly,
     * so that no update is lost under contention (the count must equal the number of successful begins, and only
     * the final release returns the sstable to {@code NORMAL}).
     */
    @Test
    public void concurrentStreamsAreRefCountedCorrectly() throws Exception
    {
        for (int round = 0; round < 200; round++)
        {
            SSTableStreamRebuildState s = new SSTableStreamRebuildState();
            int threads = 8;
            CyclicBarrier start = new CyclicBarrier(threads);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            try
            {
                AtomicInteger admitted = new AtomicInteger();
                Future<?>[] futures = new Future<?>[threads];
                for (int i = 0; i < threads; i++)
                {
                    futures[i] = pool.submit(() -> {
                        awaitBarrier(start);
                        if (s.tryBeginStreaming())
                            admitted.incrementAndGet();
                    });
                }
                for (Future<?> f : futures)
                    f.get(30, TimeUnit.SECONDS);

                assertEquals("all concurrent streams must be admitted (streaming is not exclusive)", threads, admitted.get());
                assertEquals(State.ZCS_STREAMING, s.state());
                assertEquals(threads, s.zcsStreamCount());
                assertFalse("a rebuild must be blocked while any stream is in flight", s.tryBeginRebuild());

                for (int i = 0; i < threads; i++)
                    s.endStreaming();

                assertEquals(State.NORMAL, s.state());
                assertEquals(0, s.zcsStreamCount());
            }
            finally
            {
                pool.shutdownNow();
            }
        }
    }

    /**
     * Stress test hammering {@code tryBeginRebuild}/{@code tryBeginStreaming} from many threads to assert the core
     * mutual-exclusion invariant never breaks under contention: a rebuild and any stream are never both active at
     * once, and at most one rebuild is ever active. This complements the sequential state-transition tests, which
     * cannot detect races in the check-and-set logic.
     */
    @Test
    public void mutualExclusionHoldsUnderStress() throws Exception
    {
        SSTableStreamRebuildState s = new SSTableStreamRebuildState();
        int threads = 12;
        int iterationsPerThread = 20_000;

        AtomicInteger activeRebuilds = new AtomicInteger();
        AtomicInteger activeStreams = new AtomicInteger();
        AtomicReference<String> violation = new AtomicReference<>();

        CyclicBarrier start = new CyclicBarrier(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try
        {
            Future<?>[] futures = new Future<?>[threads];
            for (int t = 0; t < threads; t++)
            {
                final int seed = t;
                futures[t] = pool.submit(() -> {
                    awaitBarrier(start);
                    java.util.Random rnd = new java.util.Random(seed);
                    for (int i = 0; i < iterationsPerThread && violation.get() == null; i++)
                    {
                        if (rnd.nextBoolean())
                        {
                            if (s.tryBeginRebuild())
                            {
                                int r = activeRebuilds.incrementAndGet();
                                if (r != 1)
                                    violation.compareAndSet(null, "two rebuilds active simultaneously: " + r);
                                if (activeStreams.get() != 0)
                                    violation.compareAndSet(null, "stream active during rebuild");
                                activeRebuilds.decrementAndGet();
                                s.endRebuild();
                            }
                        }
                        else
                        {
                            if (s.tryBeginStreaming())
                            {
                                activeStreams.incrementAndGet();
                                if (activeRebuilds.get() != 0)
                                    violation.compareAndSet(null, "rebuild active during stream");
                                activeStreams.decrementAndGet();
                                s.endStreaming();
                            }
                        }
                    }
                });
            }
            for (Future<?> f : futures)
                f.get(60, TimeUnit.SECONDS);

            assertNull(violation.get(), violation.get());
            assertEquals("all operations released, must return to NORMAL", State.NORMAL, s.state());
            assertEquals(0, s.zcsStreamCount());
        }
        finally
        {
            pool.shutdownNow();
        }
    }

    private static void awaitBarrier(CyclicBarrier barrier)
    {
        try
        {
            barrier.await(30, TimeUnit.SECONDS);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
