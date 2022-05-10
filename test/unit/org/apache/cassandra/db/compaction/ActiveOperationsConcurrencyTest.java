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

package org.apache.cassandra.db.compaction;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.NonThrowingCloseable;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ActiveOperationsConcurrencyTest
{
    private static final Logger logger = LoggerFactory.getLogger(ActiveOperationsConcurrencyTest.class);
    volatile Object blackhole;

    @Test
    public void noConcurrentModificationTest() throws InterruptedException
    {
        DatabaseDescriptor.daemonInitialization();

        int NUM_THREADS = 32;
        AtomicBoolean exit = new AtomicBoolean(false);
        CopyOnWriteArrayList<Object> errors = new CopyOnWriteArrayList<>();

        ActiveOperations testedObject = new ActiveOperations();

        class TestThread extends Thread
        {
            public TestThread(int idx)
            {
                super("test-thread-" + idx);
            }

            @Override
            public void run()
            {
                try
                {
                    while (!exit.get())
                    {
                        switch (ThreadLocalRandom.current().nextInt(4))
                        {
                            case 0: getTableOperationsTest(); break;
                            case 1: onOperationStartTest(); break;
                            case 2: getOperationsForSSTableTest(); break;
                            case 3: isActiveTest(); break;
                        }
                    }
                }
                catch (Throwable e)
                {
                    logger.error("Stopping test due to error ", e);
                    errors.add(e);
                    exit.set(true);
                }
            }

            private void isActiveTest()
            {
                LockSupport.parkNanos(ThreadLocalRandom.current().nextInt(1000000));
                TableOperation operation = mock(TableOperation.class);
                blackhole = testedObject.isActive(operation);
            }

            private void getOperationsForSSTableTest()
            {
                LockSupport.parkNanos(ThreadLocalRandom.current().nextInt(1000000));
                SSTableReader reader = mock(SSTableReader.class);
                blackhole = testedObject.getOperationsForSSTable(reader, OperationType.COMPACTION);
            }

            private void onOperationStartTest()
            {
                LockSupport.parkNanos(ThreadLocalRandom.current().nextInt(1000000));
                AbstractTableOperation.OperationProgress progress = mock(AbstractTableOperation.OperationProgress.class);
                when(progress.total()).thenReturn(ThreadLocalRandom.current().nextLong());
                TableOperation operation = mock(TableOperation.class);
                when(operation.getProgress()).thenReturn(progress);
                try (NonThrowingCloseable release = testedObject.onOperationStart(operation))
                {
                    blackhole = release;
                    LockSupport.parkNanos(ThreadLocalRandom.current().nextInt(1000000));
                }
            }

            private void getTableOperationsTest()
            {
                LockSupport.parkNanos(ThreadLocalRandom.current().nextInt(1000000));
                blackhole = testedObject.getTableOperations();
            }
        }

        Thread[] threads = new Thread[NUM_THREADS];

        IntStream.range(0, NUM_THREADS)
                 .forEach(idx -> threads[idx] = new TestThread(idx));

        for (Thread thread : threads)
        {
            thread.start();
        }

        int NUM_TICKS = 10;
        for (int tick = 0; tick < NUM_TICKS && !exit.get(); tick++)
        {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            logger.info("Tick {}...", tick);
        }
        exit.set(true);

        for (Thread thread : threads)
        {
            thread.join();
        }

        if (!errors.isEmpty())
        {
            errors.forEach(error -> logger.error("Error: ", error));
            fail("Unexpected errors in the test");
        }
    }
}