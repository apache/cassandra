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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * No objects are created currently from SlidingTimeRate in Cassandra 4.0.
 * If you decide to use it, please check CASSANDRA-16713.
 * There still might be a bug, flaky test to be fixed before using it again.
 *
 * Skipping all tests for running now to clean he noise before 4.0 GA release.
 */
public class SlidingTimeRateTest
{
    @Ignore
    @Test
    public void testUpdateAndGet()
    {
        SlidingTimeRate rate = new SlidingTimeRate(new TestTimeSource(), 10, 1, TimeUnit.SECONDS);
        int updates = 100;
        for (int i = 0; i < updates; i++)
        {
            rate.update(1);
        }
        Assert.assertEquals(updates, rate.get(TimeUnit.SECONDS), 0.0);
    }

    @Ignore
    @Test
    public void testUpdateAndGetBetweenWindows()
    {
        TestTimeSource time = new TestTimeSource();
        SlidingTimeRate rate = new SlidingTimeRate(time, 5, 1, TimeUnit.SECONDS);
        int updates = 100;
        for (int i = 0; i < updates; i++)
        {
            rate.update(1);
            time.sleep(100, TimeUnit.MILLISECONDS);
        }
        Assert.assertEquals(10, rate.get(TimeUnit.SECONDS), 0.0);
    }

    @Ignore
    @Test
    public void testUpdateAndGetPastWindowSize()
    {
        TestTimeSource time = new TestTimeSource();
        SlidingTimeRate rate = new SlidingTimeRate(time, 5, 1, TimeUnit.SECONDS);
        int updates = 100;
        for (int i = 0; i < updates; i++)
        {
            rate.update(1);
        }

        time.sleep(6, TimeUnit.SECONDS);

        Assert.assertEquals(0, rate.get(TimeUnit.SECONDS), 0.0);
    }

    @Ignore
    @Test
    public void testUpdateAndGetToPointInTime()
    {
        TestTimeSource time = new TestTimeSource();
        SlidingTimeRate rate = new SlidingTimeRate(time, 5, 1, TimeUnit.SECONDS);
        int updates = 10;
        for (int i = 0; i < updates; i++)
        {
            rate.update(1);
            time.sleep(100, TimeUnit.MILLISECONDS);
        }

        time.sleep(1, TimeUnit.SECONDS);

        Assert.assertEquals(5, rate.get(TimeUnit.SECONDS), 0.0);
        Assert.assertEquals(10, rate.get(1, TimeUnit.SECONDS), 0.0);
    }

    @Ignore
    @Test
    public void testDecay() throws InterruptedException
    {
        TestTimeSource time = new TestTimeSource();
        SlidingTimeRate rate = new SlidingTimeRate(time, 5, 1, TimeUnit.SECONDS);
        int updates = 10;
        for (int i = 0; i < updates; i++)
        {
            rate.update(1);
            time.sleep(100, TimeUnit.MILLISECONDS);
        }
        Assert.assertEquals(10, rate.get(TimeUnit.SECONDS), 0.0);

        time.sleep(1, TimeUnit.SECONDS);

        Assert.assertEquals(5, rate.get(TimeUnit.SECONDS), 0.0);

        time.sleep(2, TimeUnit.SECONDS);

        Assert.assertEquals(2.5, rate.get(TimeUnit.SECONDS), 0.0);
    }

    @Ignore
    @Test
    public void testPruning()
    {
        TestTimeSource time = new TestTimeSource();
        SlidingTimeRate rate = new SlidingTimeRate(time, 5, 1, TimeUnit.SECONDS);

        rate.update(1);
        Assert.assertEquals(1, rate.size());

        time.sleep(6, TimeUnit.SECONDS);

        rate.prune();
        Assert.assertEquals(0, rate.size());
    }

    @Ignore
    @Test
    public void testConcurrentUpdateAndGet() throws InterruptedException
    {
        final ExecutorService executor = Executors.newFixedThreadPool(FBUtilities.getAvailableProcessors());
        final TestTimeSource time = new TestTimeSource();
        final SlidingTimeRate rate = new SlidingTimeRate(time, 5, 1, TimeUnit.SECONDS);
        int updates = 100000;
        for (int i = 0; i < updates; i++)
        {
            executor.submit(() -> {
                time.sleep(1, TimeUnit.MILLISECONDS);
                rate.update(1);
            });
        }

        executor.shutdown();

        Assert.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
        Assert.assertEquals(1000, rate.get(TimeUnit.SECONDS), 100.0);
    }
}
