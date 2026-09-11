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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompactionHistoryWriterTest
{
    @BeforeClass
    public static void setup()
    {
        SchemaLoader.prepareServer();
    }

    @Test(timeout = 30000)
    public void testBoundedBacklogRejectsNewestAndPreservesFifo() throws Exception
    {
        CompactionHistoryWriter writer = new CompactionHistoryWriter(1, executorFactory().configureSequential("HistoryBacklogTest"));
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<Integer> writes = new ArrayList<>();
        try
        {
            Future<Void> first = writer.submit(() -> {
                started.countDown();
                await(release);
                writes.add(1);
            });
            assertTrue(started.await(10, TimeUnit.SECONDS));
            Future<Void> second = writer.submit(() -> writes.add(2));
            Future<Void> rejected = writer.submit(() -> writes.add(3));
            assertFailure(rejected, RejectedExecutionException.class);
            assertFalse(first.isDone());
            assertFalse(second.isDone());
            release.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
            assertEquals(Arrays.asList(1, 2), writes);
            writer.submit(() -> writes.add(4)).get(10, TimeUnit.SECONDS);
            assertEquals(Arrays.asList(1, 2, 4), writes);
        }
        finally
        {
            release.countDown();
            writer.shutdownAndWait(10, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testShutdownDrainsAcceptedWritesAndRejectsNewWrites() throws Exception
    {
        CompactionHistoryWriter writer = new CompactionHistoryWriter(1, executorFactory().configureSequential("HistoryShutdownTest"));
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<Integer> writes = new ArrayList<>();
        try
        {
            Future<Void> first = writer.submit(() -> {
                started.countDown();
                await(release);
                writes.add(1);
            });
            assertTrue(started.await(10, TimeUnit.SECONDS));
            Future<Void> second = writer.submit(() -> writes.add(2));
            try
            {
                writer.shutdownAndWait(0, TimeUnit.NANOSECONDS);
                fail("A blocked history mutation must prevent successful drain");
            }
            catch (TimeoutException expected)
            {
                assertFalse(first.isDone());
                assertFalse(second.isDone());
            }
            assertFailure(writer.submit(() -> writes.add(3)), RejectedExecutionException.class);
            release.countDown();
            writer.shutdownAndWait(10, TimeUnit.SECONDS);
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
            assertEquals(Arrays.asList(1, 2), writes);
        }
        finally
        {
            release.countDown();
            writer.shutdownAndWait(10, TimeUnit.SECONDS);
        }
    }

    private static Throwable assertFailure(Future<Void> future, Class<? extends Throwable> type) throws Exception
    {
        try
        {
            future.get(10, TimeUnit.SECONDS);
            throw new AssertionError("Expected exceptional history completion");
        }
        catch (ExecutionException e)
        {
            assertTrue("Unexpected history failure: " + e.getCause(), type.isInstance(e.getCause()));
            return e.getCause();
        }
    }

    private static void await(CountDownLatch latch)
    {
        try
        {
            assertTrue(latch.await(20, TimeUnit.SECONDS));
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
