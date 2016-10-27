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

package org.apache.cassandra.db.monitoring;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MonitoringTaskTest
{
    private static final long timeout = 100;
    private static final long slowTimeout = 10;

    private static final long MAX_SPIN_TIME_NANOS = TimeUnit.SECONDS.toNanos(5);

    private static final int REPORT_INTERVAL_MS = 600000; // long enough so that it won't check unless told to do so
    private static final int MAX_TIMEDOUT_OPERATIONS = -1; // unlimited

    @BeforeClass
    public static void setup()
    {
        MonitoringTask.instance = MonitoringTask.make(REPORT_INTERVAL_MS, MAX_TIMEDOUT_OPERATIONS);
    }

    @After
    public void cleanUp()
    {
        // these clear the queues of the monitorint task
        MonitoringTask.instance.getSlowOperations();
        MonitoringTask.instance.getFailedOperations();
    }

    private static final class TestMonitor extends MonitorableImpl
    {
        private final String name;

        TestMonitor(String name, long timestamp, boolean isCrossNode, long timeout, long slow)
        {
            this.name = name;
            setMonitoringTime(timestamp, isCrossNode, timeout, slow);
        }

        public String name()
        {
            return name;
        }

        @Override
        public String toString()
        {
            return name();
        }
    }

    private static void waitForOperationsToComplete(Monitorable... operations) throws InterruptedException
    {
        waitForOperationsToComplete(Arrays.asList(operations));
    }

    private static void waitForOperationsToComplete(List<Monitorable> operations) throws InterruptedException
    {
        long timeout = operations.stream().map(Monitorable::timeout).reduce(0L, Long::max);
        Thread.sleep(timeout * 2 + ApproximateTime.precision());

        long start = System.nanoTime();
        while(System.nanoTime() - start <= MAX_SPIN_TIME_NANOS)
        {
            long numInProgress = operations.stream().filter(Monitorable::isInProgress).count();
            if (numInProgress == 0)
                return;
        }
    }

    private static void waitForOperationsToBeReportedAsSlow(Monitorable... operations) throws InterruptedException
    {
        waitForOperationsToBeReportedAsSlow(Arrays.asList(operations));
    }

    private static void waitForOperationsToBeReportedAsSlow(List<Monitorable> operations) throws InterruptedException
    {
        long timeout = operations.stream().map(Monitorable::slowTimeout).reduce(0L, Long::max);
        Thread.sleep(timeout * 2 + ApproximateTime.precision());

        long start = System.nanoTime();
        while(System.nanoTime() - start <= MAX_SPIN_TIME_NANOS)
        {
            long numSlow = operations.stream().filter(Monitorable::isSlow).count();
            if (numSlow == operations.size())
                return;
        }
    }

    @Test
    public void testAbort() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test abort", System.currentTimeMillis(), false, timeout, slowTimeout);
        waitForOperationsToComplete(operation);

        assertTrue(operation.isAborted());
        assertFalse(operation.isCompleted());
        assertEquals(1, MonitoringTask.instance.getFailedOperations().size());
    }

    @Test
    public void testAbortIdemPotent() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test abort", System.currentTimeMillis(), false, timeout, slowTimeout);
        waitForOperationsToComplete(operation);

        assertTrue(operation.abort());

        assertTrue(operation.isAborted());
        assertFalse(operation.isCompleted());
        assertEquals(1, MonitoringTask.instance.getFailedOperations().size());
    }

    @Test
    public void testAbortCrossNode() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test for cross node", System.currentTimeMillis(), true, timeout, slowTimeout);
        waitForOperationsToComplete(operation);

        assertTrue(operation.isAborted());
        assertFalse(operation.isCompleted());
        assertEquals(1, MonitoringTask.instance.getFailedOperations().size());
    }

    @Test
    public void testComplete() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test complete", System.currentTimeMillis(), false, timeout, slowTimeout);
        operation.complete();
        waitForOperationsToComplete(operation);

        assertFalse(operation.isAborted());
        assertTrue(operation.isCompleted());
        assertEquals(0, MonitoringTask.instance.getFailedOperations().size());
    }

    @Test
    public void testCompleteIdemPotent() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test complete", System.currentTimeMillis(), false, timeout, slowTimeout);
        operation.complete();
        waitForOperationsToComplete(operation);

        assertTrue(operation.complete());

        assertFalse(operation.isAborted());
        assertTrue(operation.isCompleted());
        assertEquals(0, MonitoringTask.instance.getFailedOperations().size());
    }

    @Test
    public void testReportSlow() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test report slow", System.currentTimeMillis(), false, timeout, slowTimeout);
        waitForOperationsToBeReportedAsSlow(operation);

        assertTrue(operation.isSlow());
        operation.complete();
        assertFalse(operation.isAborted());
        assertTrue(operation.isCompleted());
        assertEquals(1, MonitoringTask.instance.getSlowOperations().size());
    }

    @Test
    public void testNoReportSlowIfZeroSlowTimeout() throws InterruptedException
    {
        // when the slow timeout is set to zero then operation won't be reported as slow
        Monitorable operation = new TestMonitor("Test report slow disabled", System.currentTimeMillis(), false, timeout, 0);
        waitForOperationsToBeReportedAsSlow(operation);

        assertTrue(operation.isSlow());
        operation.complete();
        assertFalse(operation.isAborted());
        assertTrue(operation.isCompleted());
        assertEquals(0, MonitoringTask.instance.getSlowOperations().size());
    }

    @Test
    public void testReport() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test report", System.currentTimeMillis(), false, timeout, slowTimeout);
        waitForOperationsToComplete(operation);

        assertTrue(operation.isSlow());
        assertTrue(operation.isAborted());
        assertFalse(operation.isCompleted());

        // aborted operations are not logged as slow
        assertFalse(MonitoringTask.instance.logSlowOperations(ApproximateTime.currentTimeMillis()));
        assertEquals(0, MonitoringTask.instance.getSlowOperations().size());

        assertTrue(MonitoringTask.instance.logFailedOperations(ApproximateTime.currentTimeMillis()));
        assertEquals(0, MonitoringTask.instance.getFailedOperations().size());
    }

    @Test
    public void testRealScheduling() throws InterruptedException
    {
        MonitoringTask.instance = MonitoringTask.make(10, -1);
        try
        {
            Monitorable operation1 = new TestMonitor("Test report 1", System.currentTimeMillis(), false, timeout, slowTimeout);
            waitForOperationsToComplete(operation1);

            assertTrue(operation1.isAborted());
            assertFalse(operation1.isCompleted());

            Monitorable operation2 = new TestMonitor("Test report 2", System.currentTimeMillis(), false, timeout, slowTimeout);
            waitForOperationsToBeReportedAsSlow(operation2);

            operation2.complete();
            assertFalse(operation2.isAborted());
            assertTrue(operation2.isCompleted());

            Thread.sleep(ApproximateTime.precision() + 500);
            assertEquals(0, MonitoringTask.instance.getFailedOperations().size());
            assertEquals(0, MonitoringTask.instance.getSlowOperations().size());
        }
        finally
        {
            MonitoringTask.instance = MonitoringTask.make(REPORT_INTERVAL_MS, MAX_TIMEDOUT_OPERATIONS);
        }
    }

    @Test
    public void testMultipleThreads() throws InterruptedException
    {
        final int opCount = 50;
        final ExecutorService executorService = Executors.newFixedThreadPool(20);
        final List<Monitorable> operations = Collections.synchronizedList(new ArrayList<>(opCount));

        for (int i = 0; i < opCount; i++)
        {
            executorService.submit(() ->
                operations.add(new TestMonitor(UUID.randomUUID().toString(), System.currentTimeMillis(), false, timeout, slowTimeout))
            );
        }

        executorService.shutdown();
        assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));
        assertEquals(opCount, operations.size());

        waitForOperationsToComplete(operations);
        assertEquals(opCount, MonitoringTask.instance.getFailedOperations().size());
        assertEquals(0, MonitoringTask.instance.getSlowOperations().size());
    }

    @Test
    public void testZeroMaxTimedoutOperations() throws InterruptedException
    {
        doTestMaxTimedoutOperations(0, 1, 0);
    }

    @Test
    public void testMaxTimedoutOperationsExceeded() throws InterruptedException
    {
        doTestMaxTimedoutOperations(5, 10, 6);
    }

    private static void doTestMaxTimedoutOperations(int maxTimedoutOperations,
                                                    int numThreads,
                                                    int numExpectedOperations) throws InterruptedException
    {
        MonitoringTask.instance = MonitoringTask.make(REPORT_INTERVAL_MS, maxTimedoutOperations);
        try
        {
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
            final CountDownLatch finished = new CountDownLatch(numThreads);

            for (int i = 0; i < numThreads; i++)
            {
                final String operationName = "Operation " + Integer.toString(i+1);
                final int numTimes = i + 1;
                executorService.submit(() -> {
                    try
                    {
                        for (int j = 0; j < numTimes; j++)
                        {
                            Monitorable operation1 = new TestMonitor(operationName,
                                                                     System.currentTimeMillis(),
                                                                     false,
                                                                     timeout,
                                                                     slowTimeout);
                            waitForOperationsToComplete(operation1);

                            Monitorable operation2 = new TestMonitor(operationName,
                                                                     System.currentTimeMillis(),
                                                                     false,
                                                                     timeout,
                                                                     slowTimeout);
                            waitForOperationsToBeReportedAsSlow(operation2);
                            operation2.complete();
                        }
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                        fail("Unexpected exception");
                    }
                    finally
                    {
                        finished.countDown();
                    }
                });
            }

            finished.await();
            assertEquals(0, executorService.shutdownNow().size());

            List<String> failedOperations = MonitoringTask.instance.getFailedOperations();
            assertEquals(numExpectedOperations, failedOperations.size());
            if (numExpectedOperations > 0)
                assertTrue(failedOperations.get(numExpectedOperations - 1).startsWith("..."));
        }
        finally
        {
            MonitoringTask.instance = MonitoringTask.make(REPORT_INTERVAL_MS, MAX_TIMEDOUT_OPERATIONS);
        }
    }

    @Test
    public void testMultipleThreadsSameNameFailed() throws InterruptedException
    {
        final int threadCount = 50;
        final List<Monitorable> operations = new ArrayList<>(threadCount);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch finished = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++)
        {
            executorService.submit(() -> {
                try
                {
                    Monitorable operation = new TestMonitor("Test testMultipleThreadsSameName failed",
                                                            System.currentTimeMillis(),
                                                            false,
                                                            timeout,
                                                            slowTimeout);
                    operations.add(operation);
                }
                finally
                {
                    finished.countDown();
                }
            });
        }

        finished.await();
        assertEquals(0, executorService.shutdownNow().size());

        waitForOperationsToComplete(operations);
        assertEquals(1, MonitoringTask.instance.getFailedOperations().size());
    }

    @Test
    public void testMultipleThreadsSameNameSlow() throws InterruptedException
    {
        final int threadCount = 50;
        final List<Monitorable> operations = new ArrayList<>(threadCount);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch finished = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++)
        {
            executorService.submit(() -> {
                try
                {
                    Monitorable operation = new TestMonitor("Test testMultipleThreadsSameName slow",
                                                            System.currentTimeMillis(),
                                                            false,
                                                            timeout,
                                                            slowTimeout);
                    operations.add(operation);
                }
                finally
                {
                    finished.countDown();
                }
            });
        }

        finished.await();
        assertEquals(0, executorService.shutdownNow().size());

        waitForOperationsToBeReportedAsSlow(operations);
        operations.forEach(o -> o.complete());

        assertEquals(1, MonitoringTask.instance.getSlowOperations().size());
    }

    @Test
    public void testMultipleThreadsNoFailedOps() throws InterruptedException
    {
        final int threadCount = 50;
        final List<Monitorable> operations = new ArrayList<>(threadCount);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch finished = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++)
        {
            executorService.submit(() -> {
                try
                {
                    Monitorable operation = new TestMonitor("Test thread " + Thread.currentThread().getName(),
                                                            System.currentTimeMillis(),
                                                            false,
                                                            timeout,
                                                            slowTimeout);
                    operations.add(operation);
                    operation.complete();
                }
                finally
                {
                    finished.countDown();
                }
            });
        }

        finished.await();
        assertEquals(0, executorService.shutdownNow().size());

        waitForOperationsToComplete(operations);
        assertEquals(0, MonitoringTask.instance.getFailedOperations().size());
    }
}
