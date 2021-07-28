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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.*;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.junit.Assert.*;

public class ThreadPoolMetricsTest
{
    @Test
    public void testJMXEnabledThreadPoolMetricsWithNoBlockedThread()
    {
        ThreadPoolExecutorPlus executor = (ThreadPoolExecutorPlus) executorFactory()
                .withJmxInternal()
                .configurePooled("ThreadPoolMetricsTest-1", 2)
                .withQueueLimit(2)
                .build();
        testMetricsWithNoBlockedThreads(executor, ((ThreadPoolExecutorJMXAdapter)executor.onShutdown()).metrics());
    }

    @Test
    public void testJMXEnabledThreadPoolMetricsWithBlockedThread()
    {
        ThreadPoolExecutorPlus executor = (ThreadPoolExecutorPlus) executorFactory()
                .withJmxInternal()
                .configurePooled("ThreadPoolMetricsTest-2", 2)
                .withQueueLimit(2)
                .build();
        testMetricsWithBlockedThreads(executor, ((ThreadPoolExecutorJMXAdapter)executor.onShutdown()).metrics());
    }

    @Test
    public void testSEPExecutorMetrics()
    {
        SEPExecutor executor = (SEPExecutor) new SharedExecutorPool("ThreadPoolMetricsTest-2").newExecutor(2,
                                                                                                           "ThreadPoolMetricsTest-3",
                                                                                                           "internal");

        testMetricsWithNoBlockedThreads(executor, executor.metrics);
    }

    private static void testMetricsWithBlockedThreads(ExecutorPlus threadPool, ThreadPoolMetrics metrics)
    {
        assertEquals(2, metrics.maxPoolSize.getValue().intValue());

        spinAssertEquals(0, metrics.activeTasks::getValue);
        spinAssertEquals(0L, metrics.completedTasks::getValue);
        spinAssertEquals(0, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(0L, metrics.totalBlocked::getCount);

        BlockingTask task1 = new BlockingTask();
        BlockingTask task2 = new BlockingTask();
        BlockingTask task3 = new BlockingTask();
        BlockingTask task4 = new BlockingTask();

        // The ThreadPool has a size of 2 so the 2 first tasks should go into active straight away
        threadPool.execute(task1);
        threadPool.execute(task2);

        spinAssertEquals(2, metrics.activeTasks::getValue);
        spinAssertEquals(0L, metrics.completedTasks::getValue);
        spinAssertEquals(0, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(0L, metrics.totalBlocked::getCount);

        // There are no threads available any more the 2 next tasks should go into the queue
        threadPool.execute(task3);
        threadPool.execute(task4);

        spinAssertEquals(2, metrics.activeTasks::getValue);
        spinAssertEquals(0L, metrics.completedTasks::getValue);
        spinAssertEquals(2, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(0L, metrics.totalBlocked::getCount);

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

        spinAssertEquals(1, blockedThreads::get);
        spinAssertEquals(2, metrics.activeTasks::getValue);
        spinAssertEquals(0L, metrics.completedTasks::getValue);
        spinAssertEquals(2, metrics.pendingTasks::getValue);
        spinAssertEquals(1L, metrics.currentBlocked::getCount);
        spinAssertEquals(1L, metrics.totalBlocked::getCount);

        new Thread(() ->
        {
            blockedThreads.incrementAndGet();
            threadPool.execute(task6);
            blockedThreads.decrementAndGet();
        }).start();

        spinAssertEquals(2, blockedThreads::get);
        spinAssertEquals(2, metrics.activeTasks::getValue);
        spinAssertEquals(0L, metrics.completedTasks::getValue);
        spinAssertEquals(2, metrics.pendingTasks::getValue);
        spinAssertEquals(2L, metrics.currentBlocked::getCount);
        spinAssertEquals(2L, metrics.totalBlocked::getCount);

        // Allowing first task to complete
        task1.allowToComplete();

        spinAssertEquals(true, task3::isStarted);
        spinAssertEquals(2, metrics.activeTasks::getValue);
        spinAssertEquals(1L, metrics.completedTasks::getValue);
        spinAssertEquals(2, metrics.pendingTasks::getValue);
        spinAssertEquals(1L, metrics.currentBlocked::getCount);
        spinAssertEquals(2L, metrics.totalBlocked::getCount);
        spinAssertEquals(1, blockedThreads::get);

        // Allowing second task to complete
        task2.allowToComplete();

        spinAssertEquals(true, task4::isStarted);
        spinAssertEquals(2, metrics.activeTasks::getValue);
        spinAssertEquals(2L, metrics.completedTasks::getValue);
        spinAssertEquals(2, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(2L, metrics.totalBlocked::getCount);
        spinAssertEquals(0, blockedThreads::get);

        // Allowing third task to complete
        task3.allowToComplete();

        spinAssertEquals(true, () -> task5.isStarted() || task6.isStarted());
        spinAssertEquals(2, metrics.activeTasks::getValue);
        spinAssertEquals(3L, metrics.completedTasks::getValue);
        spinAssertEquals(1, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(2L, metrics.totalBlocked::getCount);

        // Allowing fourth task to complete
        task4.allowToComplete();

        spinAssertEquals(true, () -> task5.isStarted() && task6.isStarted());
        spinAssertEquals(2, metrics.activeTasks::getValue);
        spinAssertEquals(4L, metrics.completedTasks::getValue);
        spinAssertEquals(0, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(2L, metrics.totalBlocked::getCount);

        // Allowing last tasks to complete
        task5.allowToComplete();
        task6.allowToComplete();

        spinAssertEquals(0, metrics.activeTasks::getValue);
        spinAssertEquals(6L, metrics.completedTasks::getValue);
        spinAssertEquals(0, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(2L, metrics.totalBlocked::getCount);
    }

    private static void testMetricsWithNoBlockedThreads(ExecutorPlus threadPool, ThreadPoolMetrics metrics)
    {
        spinAssertEquals(0, metrics.activeTasks::getValue);
        spinAssertEquals(0L, metrics.completedTasks::getValue);
        spinAssertEquals(0, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(0L, metrics.totalBlocked::getCount);

        BlockingTask task1 = new BlockingTask();
        BlockingTask task2 = new BlockingTask();
        BlockingTask task3 = new BlockingTask();
        BlockingTask task4 = new BlockingTask();

        // The ThreadPool has a size of 2 so the 2 first tasks should go into active straight away
        threadPool.execute(task1);

        spinAssertEquals(1, metrics.activeTasks::getValue);
        spinAssertEquals(0L, metrics.completedTasks::getValue);
        spinAssertEquals(0, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(0L, metrics.totalBlocked::getCount);

        threadPool.execute(task2);

        spinAssertEquals(2, metrics.activeTasks::getValue);
        spinAssertEquals(0L, metrics.completedTasks::getValue);
        spinAssertEquals(0, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(0L, metrics.totalBlocked::getCount);

        // There are no threads available any more the 2 next tasks should go into the queue
        threadPool.execute(task3);

        spinAssertEquals(2, metrics.activeTasks::getValue);
        spinAssertEquals(0L, metrics.completedTasks::getValue);
        spinAssertEquals(1, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(0L, metrics.totalBlocked::getCount);

        threadPool.execute(task4);

        spinAssertEquals(2, metrics.activeTasks::getValue);
        spinAssertEquals(0L, metrics.completedTasks::getValue);
        spinAssertEquals(2, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(0L, metrics.totalBlocked::getCount);

        // Allowing first task to complete
        task1.allowToComplete();

        spinAssertEquals(true, task3::isStarted);
        spinAssertEquals(2, metrics.activeTasks::getValue);
        spinAssertEquals(1L, metrics.completedTasks::getValue);
        spinAssertEquals(1, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(0L, metrics.totalBlocked::getCount);

        // Allowing second task to complete
        task2.allowToComplete();

        spinAssertEquals(true, task4::isStarted);
        spinAssertEquals(2, metrics.activeTasks::getValue);
        spinAssertEquals(2L, metrics.completedTasks::getValue);
        spinAssertEquals(0, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(0L, metrics.totalBlocked::getCount);

        // Allowing third task to complete
        task3.allowToComplete();

        spinAssertEquals(1, metrics.activeTasks::getValue);
        spinAssertEquals(3L, metrics.completedTasks::getValue);
        spinAssertEquals(0, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(0L, metrics.totalBlocked::getCount);

        // Allowing fourth task to complete
        task4.allowToComplete();

        spinAssertEquals(0, metrics.activeTasks::getValue);
        spinAssertEquals(4L, metrics.completedTasks::getValue);
        spinAssertEquals(0, metrics.pendingTasks::getValue);
        spinAssertEquals(0L, metrics.currentBlocked::getCount);
        spinAssertEquals(0L, metrics.totalBlocked::getCount);
    }

    private static void spinAssertEquals(Object expected, Supplier<Object> actualSupplier)
    {
        Util.spinAssertEquals(expected, actualSupplier, 1);
    }

    private static final class BlockingTask implements Runnable
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
