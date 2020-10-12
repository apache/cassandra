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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.cassandra.concurrent.*;

import static org.apache.cassandra.Util.spinAssertEquals;
import static org.junit.Assert.*;

public class ThreadPoolMetricsTest
{
    @Test
    public void testJMXEnabledThreadPoolMetricsWithNoBlockedThread()
    {
        JMXEnabledThreadPoolExecutor executor = new JMXEnabledThreadPoolExecutor(2,
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.SECONDS,
                                                                                 new ArrayBlockingQueue<>(2),
                                                                                 new NamedThreadFactory("ThreadPoolMetricsTest-1"),
                                                                                 "internal");
        testMetricsWithNoBlockedThreads(executor, executor.metrics);
    }

    @Test
    public void testJMXEnabledThreadPoolMetricsWithBlockedThread()
    {
        JMXEnabledThreadPoolExecutor executor = new JMXEnabledThreadPoolExecutor(2,
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.SECONDS,
                                                                                 new ArrayBlockingQueue<>(2),
                                                                                 new NamedThreadFactory("ThreadPoolMetricsTest-2"),
                                                                                 "internal");
        testMetricsWithBlockedThreads(executor, executor.metrics);
    }

    @Test
    public void testSEPExecutorMetrics()
    {
        SEPExecutor executor = (SEPExecutor) new SharedExecutorPool("ThreadPoolMetricsTest-2").newExecutor(2,
                                                                                                           "ThreadPoolMetricsTest-3",
                                                                                                           "internal");

        testMetricsWithNoBlockedThreads(executor, executor.metrics);
    }

    public void testMetricsWithBlockedThreads(LocalAwareExecutorService threadPool, ThreadPoolMetrics metrics)
    {
        assertEquals(2, metrics.maxPoolSize.getValue().intValue());

        BlockingTask task1 = new BlockingTask();
        BlockingTask task2 = new BlockingTask();
        BlockingTask task3 = new BlockingTask();
        BlockingTask task4 = new BlockingTask();

        // The ThreadPool has a size of 2 so the 2 first tasks should go into active straight away
        threadPool.execute(task1);
        threadPool.execute(task2);

        spinAssertEquals(2, () -> metrics.activeTasks.getValue().intValue(), 1);

        // There are no threads available any more the 2 next tasks should go into the queue
        threadPool.execute(task3);
        threadPool.execute(task4);

        spinAssertEquals(2, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(2, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(0L, () -> metrics.totalBlocked.getCount(), 1);

        // The queue is full the 2 next task should go into blocked and block the thread
        BlockingTask task5 = new BlockingTask();
        BlockingTask task6 = new BlockingTask();

        AtomicInteger blockedThreads = new AtomicInteger(0);
        new Thread(() ->
        {
            blockedThreads.incrementAndGet();
            threadPool.execute(task5);
            blockedThreads.decrementAndGet();
        }).start();

        spinAssertEquals(1, () -> blockedThreads.get(), 1);
        spinAssertEquals(2, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(2, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(1L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(1L, () -> metrics.totalBlocked.getCount(), 1);

        new Thread(() ->
        {
            blockedThreads.incrementAndGet();
            threadPool.execute(task6);
            blockedThreads.decrementAndGet();
        }).start();

        spinAssertEquals(2, () -> blockedThreads.get(), 1);
        spinAssertEquals(2, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(2, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(2L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(2L, () -> metrics.totalBlocked.getCount(), 1);

        // Allowing first task to complete
        task1.allowToComplete();

        spinAssertEquals(true, () -> task3.isStarted(), 1);
        spinAssertEquals(2, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(1L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(2, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(1L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(2L, () -> metrics.totalBlocked.getCount(), 1);
        spinAssertEquals(1, () -> blockedThreads.get(), 1);

        // Allowing second task to complete
        task2.allowToComplete();

        spinAssertEquals(true, () -> task4.isStarted(), 1);
        spinAssertEquals(2, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(2L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(2, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(2L, () -> metrics.totalBlocked.getCount(), 1);
        spinAssertEquals(0, () -> blockedThreads.get(), 1);

        // Allowing third task to complete
        task3.allowToComplete();

        spinAssertEquals(true, () -> task5.isStarted(), 1);
        spinAssertEquals(2, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(3L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(1, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(2L, () -> metrics.totalBlocked.getCount(), 1);

        // Allowing fourth task to complete
        task4.allowToComplete();

        spinAssertEquals(true, () -> task6.isStarted(), 1);
        spinAssertEquals(2, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(4L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(0, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(2L, () -> metrics.totalBlocked.getCount(), 1);

        // Allowing last tasks to complete
        task5.allowToComplete();
        task6.allowToComplete();

        spinAssertEquals(0, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(6L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(0, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(2L, () -> metrics.totalBlocked.getCount(), 1);
    }

    public void testMetricsWithNoBlockedThreads(LocalAwareExecutorService threadPool, ThreadPoolMetrics metrics)
    {
        BlockingTask task1 = new BlockingTask();
        BlockingTask task2 = new BlockingTask();
        BlockingTask task3 = new BlockingTask();
        BlockingTask task4 = new BlockingTask();

        // The ThreadPool has a size of 2 so the 2 first tasks should go into active straight away
        threadPool.execute(task1);

        spinAssertEquals(1, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(0, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(0L, () -> metrics.totalBlocked.getCount(), 1);

        threadPool.execute(task2);

        spinAssertEquals(2, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(0, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(0L, () -> metrics.totalBlocked.getCount(), 1);

        // There are no threads available any more the 2 next tasks should go into the queue
        threadPool.execute(task3);

        spinAssertEquals(2, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(1, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(0L, () -> metrics.totalBlocked.getCount(), 1);

        threadPool.execute(task4);

        spinAssertEquals(2, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(2, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(0L, () -> metrics.totalBlocked.getCount(), 1);

        // Allowing first task to complete
        task1.allowToComplete();

        spinAssertEquals(true, () -> task3.isStarted(), 1);
        spinAssertEquals(2, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(1L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(1, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(0L, () -> metrics.totalBlocked.getCount(), 1);

        // Allowing second task to complete
        task2.allowToComplete();

        spinAssertEquals(true, () -> task4.isStarted(), 1);
        spinAssertEquals(2, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(2L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(0, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(0L, () -> metrics.totalBlocked.getCount(), 1);

        // Allowing third task to complete
        task3.allowToComplete();

        spinAssertEquals(1, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(3L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(0, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(0L, () -> metrics.totalBlocked.getCount(), 1);

        // Allowing fourth task to complete
        task4.allowToComplete();

        spinAssertEquals(0, () -> metrics.activeTasks.getValue().intValue(), 1);
        spinAssertEquals(4L, () -> metrics.completedTasks.getValue().longValue(), 1);
        spinAssertEquals(0, () -> metrics.pendingTasks.getValue().intValue(), 1);
        spinAssertEquals(0L, () -> metrics.currentBlocked.getCount(), 1);
        spinAssertEquals(0L, () -> metrics.totalBlocked.getCount(), 1);
    }

    private class BlockingTask implements Runnable
    {
        private final CountDownLatch latch = new CountDownLatch(1);

        private volatile boolean started;

        public boolean isStarted()
        {
            return started;
        }

        public void allowToComplete()
        {
            latch.countDown();
        }

        @Override
        public void run()
        {
            started = true;
            try
            {
                latch.await(30, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
    }
}
