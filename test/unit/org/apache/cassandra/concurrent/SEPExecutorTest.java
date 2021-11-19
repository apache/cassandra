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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.concurrent.DebuggableThreadPoolExecutorTest.checkLocalStateIsPropagated;
import static org.assertj.core.api.Assertions.assertThat;

public class SEPExecutorTest
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

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
        try (PrintStream nullPrintSteam = new PrintStream(nullOutputStream))
        {
            for (int idx = 0; idx < 20; idx++)
            {
                ExecutorService es = sharedPool.newExecutor(FBUtilities.getAvailableProcessors(), "STAGE", run + MAGIC + idx);
                // Write to black hole
                es.execute(() -> nullPrintSteam.println("TEST" + es));
            }
        }

        // shutdown does not guarantee that threads are actually dead once it exits, only that they will stop promptly afterwards
        sharedPool.shutdownAndWait(1L, TimeUnit.MINUTES);
        for (Thread thread : Thread.getAllStackTraces().keySet())
        {
            if (thread.getName().contains(MAGIC))
            {
                thread.join(1000);
                if (thread.isAlive())
                    Assert.fail(thread + " is still running " + Arrays.toString(thread.getStackTrace()));
            }
        }
    }

    private static class BusyExecutor
    {
        // Number of busy worker threads to run and gum things up. Chosen to be
        // between the low and high max pool size so the test exercises resizing
        // under a number of different conditions.
        static final int numBusyWorkers = 2;
        final AtomicInteger notifiedMaxPoolSize = new AtomicInteger();

        SharedExecutorPool sharedPool;
        LocalAwareExecutorPlus executor;

        Thread makeBusy;
        AtomicBoolean stayBusy;

        public BusyExecutor(String poolName, String executorName)
        {
            sharedPool = new SharedExecutorPool(poolName);
            executor = sharedPool.newExecutor(0, notifiedMaxPoolSize::set, "internal", executorName);
        }

        public void start()
        {
            // Keep feeding the executor work while resizing
            // so it stays under load.
            stayBusy = new AtomicBoolean(true);
            Semaphore busyWorkerPermits = new Semaphore(numBusyWorkers);
            makeBusy = new Thread(() -> {
                while (stayBusy.get())
                {
                    try
                    {
                        if (busyWorkerPermits.tryAcquire(1, MILLISECONDS)) {
                            executor.execute(new BusyWork(busyWorkerPermits));
                        }
                    }
                    catch (InterruptedException e)
                    {
                        // ignore, will either stop looping if done or retry the lock
                    }
                }
            });

            makeBusy.start();
        }

        public void shutdown() throws TimeoutException, InterruptedException
        {
            stayBusy.set(false);
            makeBusy.join(TimeUnit.SECONDS.toMillis(5));
            Assert.assertFalse("makeBusy thread should have checked stayBusy and exited",
                               makeBusy.isAlive());
            sharedPool.shutdownAndWait(1L, MINUTES);
        }

        public LocalAwareExecutorPlus getExecutor()
        {
            return executor;
        }

        public int getNotifiedMaxPoolSize()
        {
            return notifiedMaxPoolSize.get();
        }
    }

    @Test
    public void changingMaxWorkersMeetsConcurrencyGoalsTest() throws InterruptedException, TimeoutException
    {
        BusyExecutor busyExecutor = new BusyExecutor("ChangingMaxWorkersMeetsConcurrencyGoalsTest", "resizetest");
        LocalAwareExecutorPlus executor = busyExecutor.getExecutor();

        busyExecutor.start();
        try
        {
            for (int repeat = 0; repeat < 1000; repeat++)
            {
                assertMaxTaskConcurrency(executor, 1);
                Assert.assertEquals(1, busyExecutor.getNotifiedMaxPoolSize());

                assertMaxTaskConcurrency(executor, 2);
                Assert.assertEquals(2, busyExecutor.getNotifiedMaxPoolSize());

                assertMaxTaskConcurrency(executor, 1);
                Assert.assertEquals(1, busyExecutor.getNotifiedMaxPoolSize());

                assertMaxTaskConcurrency(executor, 3);
                Assert.assertEquals(3, busyExecutor.getNotifiedMaxPoolSize());

                executor.setMaximumPoolSize(0);
                Assert.assertEquals(0, busyExecutor.getNotifiedMaxPoolSize());

                assertMaxTaskConcurrency(executor, 4);
                Assert.assertEquals(4, busyExecutor.getNotifiedMaxPoolSize());
            }
        }
        finally
        {
            busyExecutor.shutdown();
        }
    }

    @Test
    public void stoppedWorkersProcessTasksWhenConcurrencyIncreases() throws InterruptedException
    {
        BusyExecutor busyExecutor = new BusyExecutor("StoppedWorkersProcessTasksWhenConcurrencyIncreases", "stoptest");
        LocalAwareExecutorPlus executor = busyExecutor.getExecutor();
        busyExecutor.start();
        try
        {
            for (int repeat = 0; repeat < 25; repeat++)
            {
                assertMaxTaskConcurrency(executor, 3);
                Assert.assertEquals(3, busyExecutor.getNotifiedMaxPoolSize());

                executor.setMaximumPoolSize(0);
                Assert.assertEquals(0, busyExecutor.getNotifiedMaxPoolSize());
                Thread.sleep(250);

                assertMaxTaskConcurrency(executor, 4);
                Assert.assertEquals(4, busyExecutor.getNotifiedMaxPoolSize());
            }
        }
        finally
        {
            executor.shutdown();
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
        private final Semaphore busyWorkers;

        public BusyWork(Semaphore busyWorkers)
        {
            this.busyWorkers = busyWorkers;
        }

        public void run()
        {
            busyWorkers.release();
        }
    }

    void assertMaxTaskConcurrency(LocalAwareExecutorPlus executor, int concurrency) throws InterruptedException
    {
        executor.setMaximumPoolSize(concurrency);

        CountDownLatch concurrencyGoal = new CountDownLatch(concurrency);
        for (int i = 0; i < concurrency; i++)
        {
            executor.execute(new LatchWaiter(concurrencyGoal, 5L, TimeUnit.SECONDS));
        }
        // Will return true if all of the LatchWaiters count down before the timeout
        Assert.assertTrue("Test tasks did not hit max concurrency goal", concurrencyGoal.await(3L, TimeUnit.SECONDS));
    }

    @Test
    public void testLocalStatePropagation() throws InterruptedException, TimeoutException
    {
        SharedExecutorPool sharedPool = new SharedExecutorPool("TestPool");
        try
        {
            LocalAwareExecutorPlus executor = sharedPool.newExecutor(1, "TEST", "TEST");
            assertThat(executor).isInstanceOf(LocalAwareExecutorPlus.class);
            checkLocalStateIsPropagated(executor);
        }
        finally
        {
            sharedPool.shutdownAndWait(1, TimeUnit.SECONDS);
        }
    }

}
