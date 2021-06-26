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

package org.apache.cassandra.utils.memory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class MemtableCleanerThreadTest
{
    private static final long TIMEOUT_SECONDS = 5;
    private static final long TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS);

    @Mock
    private MemtablePool pool;

    @Mock
    private MemtableCleaner cleaner;

    private MemtableCleanerThread<MemtablePool> cleanerThread;

    @Before
    public void setup()
    {
        MockitoAnnotations.initMocks(this);
    }

    private void startThread()
    {
        cleanerThread = new MemtableCleanerThread<>(pool, cleaner);
        assertNotNull(cleanerThread);
        cleanerThread.start();

        for (int i = 0; i < TIMEOUT_MILLIS && !cleanerThread.isAlive(); i++)
            FBUtilities.sleepQuietly(1);
    }

    private void stopThread() throws InterruptedException
    {
        cleanerThread.shutdownNow();

        assertTrue(cleanerThread.awaitTermination(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
    }

    private void waitForPendingTasks()
    {
        // wait for a bit because the cleaner latch completes before the pending tasks are decremented
        FBUtilities.sleepQuietly(TIMEOUT_MILLIS);

        assertEquals(0, cleanerThread.numPendingTasks());
    }

    @Test
    public void testCleanerInvoked() throws Exception
    {
        CountDownLatch cleanerExecutedLatch = new CountDownLatch(1);
        CompletableFuture<Boolean> fut = new CompletableFuture<>();
        AtomicBoolean needsCleaning = new AtomicBoolean(false);

        when(pool.needsCleaning()).thenAnswer(invocation -> needsCleaning.get());

        when(cleaner.clean()).thenAnswer(invocation -> {
            needsCleaning.set(false);
            cleanerExecutedLatch.countDown();
            return fut;
        });

        // start the thread with needsCleaning returning false, the cleaner should not be invoked
        needsCleaning.set(false);
        startThread();
        assertFalse(cleanerExecutedLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(1, cleanerExecutedLatch.getCount());
        assertEquals(0, cleanerThread.numPendingTasks());

        // now invoke the cleaner
        needsCleaning.set(true);
        cleanerThread.trigger();
        assertTrue(cleanerExecutedLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(0, cleanerExecutedLatch.getCount());
        assertEquals(1, cleanerThread.numPendingTasks());

        // now complete the cleaning task
        needsCleaning.set(false);
        fut.complete(true);
        waitForPendingTasks();

        stopThread();
    }

    @Test
    public void testCleanerError() throws Exception
    {
        AtomicReference<CountDownLatch> cleanerLatch = new AtomicReference<>(new CountDownLatch(1));
        AtomicReference<CompletableFuture<Boolean>> fut = new AtomicReference<>(new CompletableFuture<>());
        AtomicBoolean needsCleaning = new AtomicBoolean(false);
        AtomicInteger numTimeCleanerInvoked = new AtomicInteger(0);

        when(pool.needsCleaning()).thenAnswer(invocation -> needsCleaning.get());

        when(cleaner.clean()).thenAnswer(invocation -> {
            needsCleaning.set(false);
            numTimeCleanerInvoked.incrementAndGet();
            cleanerLatch.get().countDown();
            return fut.get();
        });

        // start the thread with needsCleaning returning true, the cleaner should be invoked
        needsCleaning.set(true);
        startThread();
        assertTrue(cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(0, cleanerLatch.get().getCount());
        assertEquals(1, cleanerThread.numPendingTasks());
        assertEquals(1, numTimeCleanerInvoked.get());

        // complete the cleaning task with an error, no other cleaning task should be invoked
        cleanerLatch.set(new CountDownLatch(1));
        CompletableFuture<Boolean> oldFut = fut.get();
        fut.set(new CompletableFuture<>());
        needsCleaning.set(false);
        oldFut.completeExceptionally(new RuntimeException("Test"));
        assertFalse(cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(1, cleanerLatch.get().getCount());
        assertEquals(1, numTimeCleanerInvoked.get());

        // now trigger cleaning again and verify that a new task is invoked
        cleanerLatch.set(new CountDownLatch(1));
        fut.set(new CompletableFuture<>());
        needsCleaning.set(true);
        cleanerThread.trigger();
        assertTrue(cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(0, cleanerLatch.get().getCount());
        assertEquals(2, numTimeCleanerInvoked.get());

        //  complete the cleaning task with false (nothing should be scheduled)
        cleanerLatch.set(new CountDownLatch(1));
        oldFut = fut.get();
        fut.set(new CompletableFuture<>());
        needsCleaning.set(false);
        oldFut.complete(false);
        assertFalse(cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(1, cleanerLatch.get().getCount());
        assertEquals(2, numTimeCleanerInvoked.get());

        // now trigger cleaning again and verify that a new task is invoked
        cleanerLatch.set(new CountDownLatch(1));
        fut.set(new CompletableFuture<>());
        needsCleaning.set(true);
        cleanerThread.trigger();
        assertTrue(cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(0, cleanerLatch.get().getCount());
        assertEquals(3, numTimeCleanerInvoked.get());

        stopThread();
    }
}