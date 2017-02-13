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
package org.apache.cassandra.utils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.CoalescingStrategies.Clock;
import org.apache.cassandra.utils.CoalescingStrategies.Coalescable;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;
import org.apache.cassandra.utils.CoalescingStrategies.Parker;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.*;

public class CoalescingStrategiesTest
{

    static final ExecutorService ex = Executors.newSingleThreadExecutor();

    private static final Logger logger = LoggerFactory.getLogger(CoalescingStrategiesTest.class);

    static class MockParker implements Parker
    {
        Queue<Long> parks = new ArrayDeque<Long>();
        Semaphore permits = new Semaphore(0);

        Semaphore parked = new Semaphore(0);

        public void park(long nanos)
        {
            parks.offer(nanos);
            parked.release();
            try
            {
                permits.acquire();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    static class SimpleCoalescable implements Coalescable
    {
        final long timestampNanos;

        SimpleCoalescable(long timestampNanos)
        {
            this.timestampNanos = timestampNanos;
        }

        public long timestampNanos()
        {
            return timestampNanos;
        }
    }


    static long toNanos(long micros)
    {
        return TimeUnit.MICROSECONDS.toNanos(micros);
    }

    MockParker parker;

    BlockingQueue<SimpleCoalescable> input;
    List<SimpleCoalescable> output;

    CoalescingStrategy cs;

    Semaphore queueParked = new Semaphore(0);
    Semaphore queueRelease = new Semaphore(0);

    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @SuppressWarnings({ "serial" })
    @Before
    public void setUp() throws Exception
    {
        cs = null;
        CoalescingStrategies.CLOCK = new Clock()
        {
            @Override
            public long nanoTime()
            {
                return 0;
            }
        };

        parker = new MockParker();
        input = new LinkedBlockingQueue<SimpleCoalescable>()
                {
            @Override
            public SimpleCoalescable take() throws InterruptedException
            {
                queueParked.release();
                queueRelease.acquire();
                return super.take();
            }
        };
        output = new ArrayList<>(128);

        clear();
    }

    CoalescingStrategy newStrategy(String name, int window)
    {
        return CoalescingStrategies.newCoalescingStrategy(name, window, parker, logger, "Stupendopotamus");
    }

    void add(long whenMicros)
    {
        input.offer(new SimpleCoalescable(toNanos(whenMicros)));
    }

    void clear()
    {
        output.clear();
        input.clear();
        parker.parks.clear();
        parker.parked.drainPermits();
        parker.permits.drainPermits();
        queueParked.drainPermits();
        queueRelease.drainPermits();
    }

    void release() throws Exception
    {
        queueRelease.release();
        parker.permits.release();
        fut.get();
    }

    Future<?> fut;
    void runBlocker(Semaphore waitFor) throws Exception
    {
        fut = ex.submit(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    cs.coalesce(input, output, 128);
                }
                catch (Exception ex)
                {
                    ex.printStackTrace();
                    throw new RuntimeException(ex);
                }
            }
        });
        waitFor.acquire();
    }

    @Test
    public void testFixedCoalescingStrategy() throws Exception
    {
        cs = newStrategy("FIXED", 200);

        //Test that when a stream of messages continues arriving it keeps sending until all are drained
        //It does this because it is already awake and sending messages
        add(42);
        add(42);
        cs.coalesce(input, output, 128);
        assertEquals( 2, output.size());
        assertNull(parker.parks.poll());

        clear();

        runBlocker(queueParked);
        add(42);
        add(42);
        add(42);
        release();
        assertEquals( 3, output.size());
        assertEquals(toNanos(200), parker.parks.poll().longValue());

    }

    @Test
    public void testFixedCoalescingStrategyEnough() throws Exception
    {
        int oldValue = DatabaseDescriptor.getOtcCoalescingEnoughCoalescedMessages();
        DatabaseDescriptor.setOtcCoalescingEnoughCoalescedMessages(1);
        try {
            cs = newStrategy("FIXED", 200);

            //Test that when a stream of messages continues arriving it keeps sending until all are drained
            //It does this because it is already awake and sending messages
            add(42);
            add(42);
            cs.coalesce(input, output, 128);
            assertEquals(2, output.size());
            assertNull(parker.parks.poll());

            clear();

            runBlocker(queueParked);
            add(42);
            add(42);
            add(42);
            release();
            assertEquals(3, output.size());
            assertNull(parker.parks.poll());
        }
        finally {
            DatabaseDescriptor.setOtcCoalescingEnoughCoalescedMessages(oldValue);
        }

    }

    @Test
    public void testDisabledCoalescingStrateg() throws Exception
    {
        cs = newStrategy("DISABLED", 200);

        add(42);
        add(42);
        cs.coalesce(input, output, 128);
        assertEquals( 2, output.size());
        assertNull(parker.parks.poll());

        clear();

        runBlocker(queueParked);
        add(42);
        add(42);
        release();
        assertEquals( 2, output.size());
        assertNull(parker.parks.poll());
    }

    @Test
    public void parkLoop() throws Exception
   {
        final Thread current = Thread.currentThread();
        final Semaphore helperReady = new Semaphore(0);
        final Semaphore helperGo = new Semaphore(0);

        new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    helperReady.release();
                    helperGo.acquire();
                    Thread.sleep(50);
                    LockSupport.unpark(current);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                    logger.error("Error", e);
                    System.exit(-1);
                }
            }
        }.start();

        long start = System.nanoTime();
        helperGo.release();

        long parkNanos = TimeUnit.MILLISECONDS.toNanos(500);

        CoalescingStrategies.parkLoop(parkNanos);
        long delta = System.nanoTime() - start;

        assertTrue (delta >= (parkNanos - (parkNanos / 16)));
    }

    @Test
    public void testMovingAverageCoalescingStrategy() throws Exception
    {
        cs = newStrategy("org.apache.cassandra.utils.CoalescingStrategies$MovingAverageCoalescingStrategy", 200);


        //Test that things can be pulled out of the queue if it is non-empty
        add(201);
        add(401);
        cs.coalesce(input, output, 128);
        assertEquals( 2, output.size());
        assertNull(parker.parks.poll());

        //Test that blocking on the queue results in everything drained
        clear();

        runBlocker(queueParked);
        add(601);
        add(801);
        release();
        assertEquals( 2, output.size());
        assertNull(parker.parks.poll());

        clear();

        //Test that out of order samples still flow
        runBlocker(queueParked);
        add(0);
        release();
        assertEquals( 1, output.size());
        assertNull(parker.parks.poll());

        clear();

        add(0);
        cs.coalesce(input, output, 128);
        assertEquals( 1, output.size());
        assertNull(parker.parks.poll());

        clear();

        //Test that too high an average doesn't coalesce
        for (long ii = 0; ii < 128; ii++)
            add(ii * 1000);
        cs.coalesce(input, output, 128);
        assertEquals(output.size(), 128);
        assertTrue(parker.parks.isEmpty());

        clear();

        runBlocker(queueParked);
        add(129 * 1000);
        release();
        assertTrue(parker.parks.isEmpty());

        clear();

        //Test that a low enough average coalesces
        cs = newStrategy("MOVINGAVERAGE", 200);
        for (long ii = 0; ii < 128; ii++)
            add(ii * 99);
        cs.coalesce(input, output, 128);
        assertEquals(output.size(), 128);
        assertTrue(parker.parks.isEmpty());

        clear();

        runBlocker(queueParked);
        add(128 * 99);
        add(129 * 99);
        release();
        assertEquals(2, output.size());
        assertEquals(toNanos(198), parker.parks.poll().longValue());
    }

    @Test
    public void testTimeHorizonStrategy() throws Exception
    {
        cs = newStrategy("TIMEHORIZON", 200);

        //Test that things can be pulled out of the queue if it is non-empty
        add(201);
        add(401);
        cs.coalesce(input, output, 128);
        assertEquals( 2, output.size());
        assertNull(parker.parks.poll());

        //Test that blocking on the queue results in everything drained
        clear();

        runBlocker(queueParked);
        add(601);
        add(801);
        release();
        assertEquals( 2, output.size());
        assertNull(parker.parks.poll());

        clear();

        //Test that out of order samples still flow
        runBlocker(queueParked);
        add(0);
        release();
        assertEquals( 1, output.size());
        assertNull(parker.parks.poll());

        clear();

        add(0);
        cs.coalesce(input, output, 128);
        assertEquals( 1, output.size());
        assertNull(parker.parks.poll());

        clear();

        //Test that too high an average doesn't coalesce
        for (long ii = 0; ii < 128; ii++)
            add(ii * 1000);
        cs.coalesce(input, output, 128);
        assertEquals(output.size(), 128);
        assertTrue(parker.parks.isEmpty());

        clear();

        runBlocker(queueParked);
        add(129 * 1000);
        release();
        assertTrue(parker.parks.isEmpty());

        clear();

        //Test that a low enough average coalesces
        cs = newStrategy("TIMEHORIZON", 200);
        primeTimeHorizonAverage(99);

        clear();

        runBlocker(queueParked);
        add(100000 * 99);
        queueRelease.release();
        parker.parked.acquire();
        add(100001 * 99);
        parker.permits.release();
        fut.get();
        assertEquals(2, output.size());
        assertEquals(toNanos(198), parker.parks.poll().longValue());

        clear();

        //Test far future
        add(Integer.MAX_VALUE);
        cs.coalesce(input, output, 128);
        assertEquals(1, output.size());
        assertTrue(parker.parks.isEmpty());

        clear();

        //Distant past
        add(0);
        cs.coalesce(input, output, 128);
        assertEquals(1, output.size());
        assertTrue(parker.parks.isEmpty());
    }

    void primeTimeHorizonAverage(long micros) throws Exception
    {
        for (long ii = 0; ii < 100000; ii++)
        {
            add(ii * micros);
            if (ii % 128 == 0)
            {
                cs.coalesce(input, output, 128);
                output.clear();
            }
        }
    }
}
