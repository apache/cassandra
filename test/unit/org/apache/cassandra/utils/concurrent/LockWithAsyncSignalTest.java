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

package org.apache.cassandra.utils.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class LockWithAsyncSignalTest
{
    @Test
    public void test()
    {
        ExecutorPlus submitters = executorFactory().pooled("test-submitters", 16);
        ExecutorPlus consumers = executorFactory().pooled("test-consumers", 16);
        for (int i = 0 ; i < 10 ; ++i)
            testOne(submitters, consumers, 4, 8, i);
    }

    private static void testOne(ExecutorPlus submitterExecutor, ExecutorPlus consumerExecutor, int submitterCount, int consumerCount, int seconds)
    {
        class Waiting extends AtomicBoolean implements Comparable<Waiting>
        {
            final long ticket;

            Waiting(long ticket)
            {
                this.ticket = ticket;
            }

            @Override
            public int compareTo(Waiting that)
            {
                return Long.compare(this.ticket, that.ticket);
            }
        }
        final LockWithAsyncSignal lock = new LockWithAsyncSignal();
        final List<Future<Object>> submitters = new ArrayList<>();
        final List<Future<Object>> consumers = new ArrayList<>();
        final AtomicBoolean submittersRunning = new AtomicBoolean(true);
        final AtomicBoolean consumersRunning = new AtomicBoolean(true);
        final ConcurrentSkipListMap<Waiting, Boolean> waiting = new ConcurrentSkipListMap<>();
        final AtomicLong nextTicket = new AtomicLong();
        final EstimatedHistogram latency = new EstimatedHistogram();
        for (int i = 0; i < submitterCount + consumerCount ; ++i)
        {
            boolean submitter = i/2 >= consumerCount || ((i & 1) == 0 && i/2 < submitterCount);
            if (submitter)
            {
                submitters.add(submitterExecutor.submit(() -> {
                    final Random rnd = new Random();
                    while (submittersRunning.get())
                    {
                        LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(rnd.nextInt(100)));
                        long start = System.nanoTime();
                        Waiting awaiting = new Waiting(nextTicket.incrementAndGet());
                        waiting.put(awaiting, true);
                        lock.signal();
                        while (!awaiting.get());
                        long end = System.nanoTime();
                        latency.add(NANOSECONDS.toMicros(end - start));
                    }
                    return null;
                }));
            }
            else
            {
                consumers.add(consumerExecutor.submit(() -> {
                    final Random rnd = new Random();
                    while (true)
                    {
                        if (rnd.nextBoolean()) lock.lock();
                        else if (!lock.tryLock()) continue;

                        Waiting waitUntil;
                        try
                        {
                            AtomicBoolean awaiting;
                            while (null != (awaiting = pollFirst(waiting)))
                                awaiting.set(true);

                            lock.await();
                            if (null != (awaiting = pollFirst(waiting)))
                                awaiting.set(true);

                            if (!consumersRunning.get())
                            {
                                lock.signal();
                                return null;
                            }

                            waitUntil = peekLast(waiting);
                            if (!waiting.isEmpty())
                                lock.signal();
                        }
                        finally
                        {
                            lock.unlock();
                        }

                        if (waitUntil != null)
                        {
                            while (!waitUntil.get());
                        }
                    }
                }));
            }
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (true)
        {
            long wait = deadline - System.nanoTime();
            if (wait < 0)
                break;
            LockSupport.parkNanos(wait);
        }
        submittersRunning.set(false);
        lock.signal();
        FBUtilities.waitOnFutures(submitters, 2L, TimeUnit.SECONDS);
        consumersRunning.set(false);
        FBUtilities.waitOnFutures(consumers, 2L, TimeUnit.SECONDS);
    }

    private static <T> T pollFirst(NavigableMap<T, ?> map)
    {
        Map.Entry<T, ?> e = map.pollFirstEntry();
        return e == null ? null : e.getKey();
    }

    private static <T> T peekLast(NavigableMap<T, ?> map)
    {
        Map.Entry<T, ?> e = map.lastEntry();
        return e == null ? null : e.getKey();
    }
}
