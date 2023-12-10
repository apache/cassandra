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

package org.apache.cassandra.fuzz.harry.model;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.harry.clock.ApproximateClock;

public class ApproximateClockTest
{
    @Test
    public void approximateClockTest() throws InterruptedException
    {
        ConcurrentHashMap<Long, Long> m = new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, Long> inverse = new ConcurrentHashMap<>();
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        int duration = 1000;
        int concurrency = 5;
        long maxTicks = timeUnit.toMicros(duration)  / (4 * concurrency);

        ApproximateClock clock = new ApproximateClock(duration, timeUnit);

        ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        final Lock lock = new ReentrantLock();
        AtomicReference<Throwable> throwable = new AtomicReference();
        final Condition signalError = lock.newCondition();
        lock.lock();
        for (int i = 0; i < concurrency; i++)
        {
            executor.submit(() -> {
                try
                {
                    int sleepCnt = 0;

                    while (!executor.isShutdown() && !Thread.currentThread().isInterrupted())
                    {
                        sleepCnt++;
                        if (sleepCnt >= maxTicks)
                        {
                            LockSupport.parkNanos(timeUnit.toNanos(duration));
                            sleepCnt = 0;
                        }

                        if (executor.isShutdown() || Thread.currentThread().isInterrupted())
                            return;

                        long lts = clock.nextLts();

                        // Make sure to test "history" path
                        if (lts % 10000 == 0)
                        {
                            scheduledExecutor.schedule(() -> {
                                try
                                {
                                    long rts = clock.rts(lts);
                                    Assert.assertNull(m.put(lts, rts));
                                    Assert.assertNull(inverse.put(rts, lts));
                                }
                                catch (Throwable t)
                                {
                                    throwable.set(t);
                                    signalError.signalAll();
                                    t.printStackTrace();
                                }
                            }, 2 * duration, timeUnit);
                            continue;
                        }

                        try
                        {
                            long rts = clock.rts(lts);
                            Assert.assertNull(m.put(lts, rts));
                            Assert.assertNull(inverse.put(rts, lts));
                        }
                        catch (Throwable t)
                        {
                            throwable.set(t);
                            signalError.signalAll();
                        }
                    }
                }
                catch (Throwable t)
                {
                    throwable.set(t);
                    signalError.signalAll();
                    t.printStackTrace();
                }
            });
        }
        signalError.await(10, TimeUnit.SECONDS);
        lock.unlock();
        executor.shutdown();
        Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        scheduledExecutor.shutdown();
        Assert.assertTrue(scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS));
        Throwable t = throwable.get();
        if (t != null)
            throw new AssertionError("Caught an exception while executing", t);

        Assert.assertEquals(m.size(), inverse.size());
        Iterator<Map.Entry<Long, Long>> iter = m.entrySet().iterator();

        Map.Entry<Long, Long> previous = iter.next();
        while (iter.hasNext())
        {
            if (previous == null)
            {
                previous = iter.next();
                continue;
            }

            Map.Entry<Long, Long> current = iter.next();
            long lts = current.getKey();
            long rts = current.getValue();
            Assert.assertEquals(String.format("%s and %s sort wrong", previous, current),
                                Long.compare(previous.getKey(), current.getKey()),
                                Long.compare(previous.getValue(), current.getValue()));

            Assert.assertEquals(clock.rts(lts), rts);
            Assert.assertEquals(clock.lts(rts), lts);
            previous = current;
        }
    }

    @Test
    public void approximateClockInvertibilityTest()
    {
        ConcurrentHashMap<Long, Long> m = new ConcurrentHashMap<>();
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        int duration = 100;
        int cycles = 10_000;
        ApproximateClock clock = new ApproximateClock(duration, timeUnit);

        for (long i = 0; i < cycles; i++)
        {
            long lts = clock.nextLts();
            Assert.assertEquals(lts, i);
            long rts = clock.rts(lts);
            Assert.assertNull(m.put(lts, rts));
        }

        for (Map.Entry<Long, Long> entry : m.entrySet())
        {
            Assert.assertEquals(entry.getKey(),
                                Long.valueOf(clock.lts(entry.getValue())));
        }
    }

}
