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

package org.apache.cassandra.concurrent;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;

public class SEPExecutorTest
{
    @Test
    public void shutdownTest() throws Throwable
    {
        for (int i = 0; i < 1000; i++)
        {
            shutdownOnce(i);
        }
    }

    private static void shutdownOnce(int run) throws Throwable
    {
        SharedExecutorPool sharedPool = new SharedExecutorPool("SharedPool");
        String MAGIC = "UNREPEATABLE_MAGIC_STRING";
        OutputStream nullOutputStream = new OutputStream() {
            public void write(int b) { }
        };
        PrintStream nullPrintSteam = new PrintStream(nullOutputStream);

        for (int idx = 0; idx < 20; idx++)
        {
            ExecutorService es = sharedPool.newExecutor(FBUtilities.getAvailableProcessors(), Integer.MAX_VALUE, "STAGE", run + MAGIC + idx);
            // Write to black hole
            es.execute(() -> nullPrintSteam.println("TEST" + es));
        }

        // shutdown does not guarantee that threads are actually dead once it exits, only that they will stop promptly afterwards
        sharedPool.shutdownAndWait(1L, TimeUnit.MINUTES);
        for (Thread thread : Thread.getAllStackTraces().keySet())
        {
            if (thread.getName().contains(MAGIC))
            {
                thread.join(100);
                if (thread.isAlive())
                    Assert.fail(thread + " is still running " + Arrays.toString(thread.getStackTrace()));
            }
        }
    }

    @Test
    public void changingMaxWorkersMeetsConcurrencyGoalsTest() throws InterruptedException
    {
        final int numBusyWorkers = 2; // Number of busy worker threads to run and gum things up
        SharedExecutorPool sharedPool = new SharedExecutorPool("ChangingMaxWorkersMeetsConcurrencyGoalsTest");
        final AtomicInteger notifiedMaxPoolSize = new AtomicInteger();

        LocalAwareExecutorService executor = sharedPool.newExecutor(0, notifiedMaxPoolSize::set, 4, "internal", "resizetest");

        AtomicBoolean stayBusy = new AtomicBoolean(true);
        for (int i = 0; i < numBusyWorkers; i++)
        {
            executor.execute(new BusyWork(executor, stayBusy));
        }

        final int previousConcurrency = executor.getMaximumPoolSize();
        try
        {
            assertMaxTaskConcurrency(executor, 1);
            Assert.assertEquals(1, notifiedMaxPoolSize.get());

            assertMaxTaskConcurrency(executor, 2);
            Assert.assertEquals(2, notifiedMaxPoolSize.get());

            assertMaxTaskConcurrency(executor, 1);
            Assert.assertEquals(1, notifiedMaxPoolSize.get());

            assertMaxTaskConcurrency(executor, 3);
            Assert.assertEquals(3, notifiedMaxPoolSize.get());

            executor.setMaximumPoolSize(0);
            Assert.assertEquals(0, notifiedMaxPoolSize.get());

            assertMaxTaskConcurrency(executor, 4);
            Assert.assertEquals(4, notifiedMaxPoolSize.get());
        }
        finally
        {
            stayBusy.set(false);
            executor.setMaximumPoolSize(previousConcurrency);
            executor.shutdownNow();
            Assert.assertTrue(executor.isShutdown());
            Assert.assertTrue(executor.awaitTermination(1L, TimeUnit.MINUTES));
        }
    }

    static class LatchWaiter implements Runnable
    {
        CountDownLatch latch;
        long timeout;
        TimeUnit unit;

        public LatchWaiter(CountDownLatch latch, long timeout, TimeUnit unit)
        {
            this.latch = latch;
            this.timeout = timeout;
            this.unit = unit;
        }

        public void run()
        {
            latch.countDown();
            try
            {
                latch.await(timeout, unit); // block until all the latch waiters have run, now at desired concurrency
            }
            catch (InterruptedException e)
            {
                Assert.fail("interrupted: " + e);
            }
        }
    }

    static class BusyWork implements Runnable
    {
        private ExecutorService executor;
        private AtomicBoolean stayBusy;

        public BusyWork(ExecutorService executor, AtomicBoolean stayBusy)
        {
            this.executor = executor;
            this.stayBusy = stayBusy;
        }

        public void run()
        {
            if (stayBusy.get())
            {
                executor.execute(new BusyWork(executor, stayBusy));
            }
        }
    }

    void assertMaxTaskConcurrency(LocalAwareExecutorService executor, int concurrency) throws InterruptedException
    {
        executor.setMaximumPoolSize(concurrency);

        CountDownLatch concurrencyGoal = new CountDownLatch(concurrency);
        for (int i = 0; i < concurrency; i++)
        {
            executor.execute(new LatchWaiter(concurrencyGoal, 5L, TimeUnit.SECONDS));
        }
        // Will return true if all of the LatchWaiters count down before the timeout
        Assert.assertEquals(true, concurrencyGoal.await(3L, TimeUnit.SECONDS));
    }
}
