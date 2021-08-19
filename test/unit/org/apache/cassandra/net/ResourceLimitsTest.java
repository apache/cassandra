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
package org.apache.cassandra.net;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.net.ResourceLimits.*;

import static org.junit.Assert.*;

public class ResourceLimitsTest
{
    @Test
    public void testAllocatesWithinLimits()
    {
        testAllocatesWithinLimits(Basic::new);
        testAllocatesWithinLimits(Concurrent::new);
    }

    private void testAllocatesWithinLimits(LongFunction<Limit> supplier)
    {
        Limit limit = supplier.apply(100);

        assertEquals(100, limit.limit());
        assertEquals(0,   limit.using());
        assertEquals(100, limit.remaining());

        assertTrue(limit.tryAllocate(10));
        assertEquals(10, limit.using());
        assertEquals(90, limit.remaining());

        assertTrue(limit.tryAllocate(30));
        assertEquals(40, limit.using());
        assertEquals(60, limit.remaining());

        assertTrue(limit.tryAllocate(60));
        assertEquals(100, limit.using());
        assertEquals(0, limit.remaining());
    }

    @Test
    public void testFailsToAllocateOverCapacity()
    {
        testFailsToAllocateOverCapacity(Basic::new);
        testFailsToAllocateOverCapacity(Concurrent::new);
    }

    private void testFailsToAllocateOverCapacity(LongFunction<Limit> supplier)
    {
        Limit limit = supplier.apply(100);

        assertEquals(100, limit.limit());
        assertEquals(0,   limit.using());
        assertEquals(100, limit.remaining());

        assertTrue(limit.tryAllocate(10));
        assertEquals(10, limit.using());
        assertEquals(90, limit.remaining());

        assertFalse(limit.tryAllocate(91));
        assertEquals(10, limit.using());
        assertEquals(90, limit.remaining());
    }

    @Test
    public void testRelease()
    {
        testRelease(Basic::new);
        testRelease(Concurrent::new);
    }

    private void testRelease(LongFunction<Limit> supplier)
    {
        Limit limit = supplier.apply(100);

        assertEquals(100, limit.limit());
        assertEquals(0,   limit.using());
        assertEquals(100, limit.remaining());

        assertTrue(limit.tryAllocate(10));
        assertTrue(limit.tryAllocate(30));
        assertTrue(limit.tryAllocate(60));
        assertEquals(100, limit.using());
        assertEquals(0, limit.remaining());

        limit.release(10);
        assertEquals(90, limit.using());
        assertEquals(10, limit.remaining());

        limit.release(30);
        assertEquals(60, limit.using());
        assertEquals(40, limit.remaining());

        limit.release(60);
        assertEquals(0,   limit.using());
        assertEquals(100, limit.remaining());
    }

    @Test
    public void testConcurrentLimit() throws Exception
    {
        int numThreads = 4;
        int numPermitsPerThread = 1_000_000;
        int numPermits = numThreads * numPermitsPerThread;

        CountDownLatch latch = new CountDownLatch(numThreads);
        Limit limit = new Concurrent(numPermits);

        class Worker implements Runnable
        {
            public void run()
            {
                for (int i = 0; i < numPermitsPerThread; i += 10)
                    assertTrue(limit.tryAllocate(10));

                for (int i = 0; i < numPermitsPerThread; i += 10)
                    limit.release(10);

                latch.countDown();
            }
        }

        Executor executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++)
            executor.execute(new Worker());
        latch.await(10, TimeUnit.SECONDS);

        assertEquals(0,          limit.using());
        assertEquals(numPermits, limit.remaining());
    }

    @Test
    public void negativeConcurrentUsingValueKillsJVMTest()
    {
        DatabaseDescriptor.daemonInitialization(); // Prevent NPE for DatabaseDescriptor.getDiskFailurePolicy
        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        try
        {
            Concurrent concurrent = new Concurrent(1);
            try
            {
                concurrent.release(2);
            }
            catch (Throwable tr)
            {
                JVMStabilityInspector.inspectThrowable(tr);
            }
            Assert.assertTrue(killerForTests.wasKilled());
            Assert.assertFalse(killerForTests.wasKilledQuietly()); //only killed quietly on startup failure
        }
        finally
        {
            JVMStabilityInspector.replaceKiller(originalKiller);
        }
    }
}
