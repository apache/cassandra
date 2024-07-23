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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.Semaphore;

public class SemaphoreTest extends SimulationTestBase
{
    @Test
    public void semaphoreAcquireUntilTest()
    {
        simulate(arr(() -> {
                     try
                     {
                         Semaphore semaphore = Semaphore.newSemaphore(1);
                         semaphore.acquire(1);
                         long start = Clock.Global.nanoTime();
                         Assert.assertFalse(semaphore.tryAcquire(1, 5000, TimeUnit.MILLISECONDS));
                         long elapsed = TimeUnit.NANOSECONDS.toMillis(Clock.Global.nanoTime() - start);
                         Assert.assertTrue(String.format("Elapsed only %sms, while should have at least 5000", elapsed),
                                           elapsed > 5000);
                     }
                     catch (Throwable t)
                     {
                         throw new RuntimeException(t);
                     }
                 }),
                 () -> {},
                 System.currentTimeMillis());
    }

    @Test
    public void semaphoreAcquireReleaseRepeatabilityTest()
    {
        long seed = System.currentTimeMillis();
        semaphoreTestInternal(seed);
        State.record = false;
        // Verify that subsequent interleavings will be the same
        semaphoreTestInternal(seed);
    }

    protected void semaphoreTestInternal(long seed)
    {
        simulate(arr(() -> {
                     ExecutorPlus executor = ExecutorFactory.Global.executorFactory().pooled("semaphore-test-", 10);
                     Semaphore semaphore = Semaphore.newSemaphore(5);
                     CountDownLatch latch = CountDownLatch.newCountDownLatch(5);

                     for (int i = 0; i < 5; i++)
                     {
                         int thread = i;
                         executor.submit(() -> {
                             for (int j = 0; j < 100; j++)
                             {
                                 int permits = semaphore.permits();
                                 Assert.assertTrue(permits + " should be non negative", permits >= 0);

                                 try
                                 {
                                     semaphore.acquire(1);
                                     State.tick(thread, j);
                                     semaphore.release(1);
                                 }
                                 catch (Throwable e)
                                 {
                                     throw new RuntimeException(e);
                                 }
                             }
                             latch.decrement();
                         });
                     }

                     latch.awaitUninterruptibly();
                     int permits = semaphore.permits();
                     Assert.assertEquals(5, permits);
                 }),
                 () -> {},
                 seed);
    }

    @Shared
    public static class State
    {
        static final List<Tick> ticks = new ArrayList<>();
        static boolean record = true;
        static int i = 0;

        public static void tick(int thread, int iteration)
        {
            if (record)
            {
                Tick tick = new Tick(thread, iteration);
                ticks.add(tick);
            }
            else
            {
                Tick tick = ticks.get(i);
                Assert.assertEquals(tick.thread, thread);
                Assert.assertEquals(tick.iteration, iteration);
                i++;
            }
        }
    }

    @Shared
    public static class Tick
    {
        final int thread;
        final int iteration;

        public Tick(int thread, int iteration)
        {
            this.thread = thread;
            this.iteration = iteration;
        }

        @Override
        public String toString()
        {
            return "Tick{" +
                   "thread=" + thread +
                   ", iteration=" + iteration +
                   '}';
        }
    }
}
