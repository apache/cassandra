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

package org.apache.cassandra.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongBinaryOperator;

import org.junit.Assert;
import org.junit.Test;

public class RecomputingSupplierTest
{
    @Test
    public void recomputingSupplierTest() throws Throwable
    {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        ExecutorService testExecutor = Executors.newFixedThreadPool(10);

        AtomicReference<Throwable> thrown = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        final AtomicLong counter = new AtomicLong(0);
        final RecomputingSupplier<Long> supplier = new RecomputingSupplier<>(() -> {
            try
            {
                long v = counter.incrementAndGet();
                LockSupport.parkNanos(1);
                // Make sure that the value still hasn't changed
                Assert.assertEquals(v, counter.get());
                return v;
            }
            catch (Throwable e)
            {
                thrown.set(e);
                latch.countDown();
                throw new RuntimeException(e);
            }
        }, executor);

        for (int i = 0; i < 5; i++)
        {
            testExecutor.submit(() -> {
                try
                {
                    while (!Thread.interrupted() && !testExecutor.isShutdown())
                        supplier.recompute();
                }
                catch (Throwable e)
                {
                    thrown.set(e);
                    latch.countDown();
                }
            });
        }

        AtomicLong lastSeen = new AtomicLong(0);
        for (int i = 0; i < 5; i++)
        {
            testExecutor.submit(() -> {
                while (!Thread.interrupted() && !testExecutor.isShutdown())
                {
                    try
                    {
                        long seenBeforeGet = lastSeen.get();
                        Long v = supplier.get(1, TimeUnit.SECONDS);
                        if (v != null)
                        {
                            lastSeen.accumulateAndGet(v, Math::max);
                            Assert.assertTrue(String.format("Seen %d out of order. Last seen value %d", v, seenBeforeGet),
                                              v >= seenBeforeGet);

                        }

                    }
                    catch (Throwable e)
                    {
                        thrown.set(e);
                        latch.countDown();
                    }
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);

        testExecutor.shutdown();
        testExecutor.awaitTermination(10, TimeUnit.SECONDS);
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        if (thrown.get() != null)
            throw new AssertionError(supplier.toString(), thrown.get());

        Assert.assertTrue(counter.get() > 1); // We actually did some work
        Assert.assertEquals(supplier.get(1, TimeUnit.SECONDS).longValue(), counter.get());
    }

    @Test
    public void throwingSupplier() throws InterruptedException, TimeoutException
    {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);


        final RecomputingSupplier<Long> supplier = new RecomputingSupplier<>(() -> {
            throw new RuntimeException();
        }, executor);

        supplier.recompute();

        try
        {
            supplier.get(10, TimeUnit.SECONDS);
            Assert.fail("Should have thrown");
        }
        catch (ExecutionException t)
        {
            // ignore
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
}
