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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.metrics.Sampler.Sample;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FreeRunningClock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;


public class SamplerTest
{
    Sampler<String> sampler;


    @BeforeClass
    public static void initMessagingService() throws ConfigurationException
    {
        // required so the rejection policy doesnt fail on initializing
        // static MessagingService resources
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void sampleLoadshedding() throws Exception
    {
        // dont need to run this in children tests
        if (sampler != null) return;
        AtomicInteger called = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);
        Sampler<String> waitSampler = new Sampler<String>()
        {
            protected void insert(String item, long value)
            {
                called.incrementAndGet();
                try
                {
                    latch.await(1, TimeUnit.MINUTES);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }

            public boolean isEnabled()
            {
                return true;
            }

            public boolean isActive()
            {
                return true;
            }

            public void beginSampling(int capacity, long durationMillis)
            {
            }

            public List<Sample<String>> finishSampling(int count)
            {
                return null;
            }

            public String toString(String value)
            {
                return "";
            }
        };
        // 1000 queued, 1 in progress, 1 to drop
        for (int i = 0; i < 1002; i++)
        {
            waitSampler.addSample("TEST", 1);
        }
        latch.countDown();
        waitForEmpty(1000);
        Assert.assertEquals(1001, called.get());
        Assert.assertEquals(1, MessagingService.instance().getDroppedMessages().get("_SAMPLE").intValue());
    }

    @Test
    public void testSamplerOutOfOrder() throws TimeoutException
    {
        if(sampler == null) return;
        sampler.beginSampling(10, 1000000);
        insert(sampler);
        waitForEmpty(1000);
        List<Sample<String>> single = sampler.finishSampling(10);
        single = sampler.finishSampling(10);
        Assert.assertEquals(0, single.size());
    }

    @Test(expected=RuntimeException.class)
    public void testWhileRunning()
    {
        if(sampler == null) throw new RuntimeException();
        sampler.clock = new FreeRunningClock();
        try
        {
            sampler.beginSampling(10, 1000000);
        } catch (RuntimeException e)
        {
            Assert.fail(); // shouldnt fail on first call
        }
        // should throw Exception
        sampler.beginSampling(10, 1000000);
    }

    @Test
    public void testRepeatStartAfterTimeout()
    {
        if(sampler == null) return;
        FreeRunningClock clock = new FreeRunningClock();
        sampler.clock = clock;
        try
        {
            sampler.beginSampling(10, 10);
        } catch (RuntimeException e)
        {
            Assert.fail(); // shouldnt fail on first call
        }
        clock.advance(11, TimeUnit.MILLISECONDS);
        sampler.beginSampling(10, 1000000);
    }

    /**
     * checking for exceptions if not thread safe (MinMaxPQ and SS/HL are not)
     */
    @Test
    public void testMultithreadedAccess() throws Exception
    {
        if(sampler == null) return;
        final AtomicBoolean running = new AtomicBoolean(true);
        final CountDownLatch latch = new CountDownLatch(1);

        NamedThreadFactory.createThread(new Runnable()
        {
            public void run()
            {
                try
                {
                    while (running.get())
                    {
                        insert(sampler);
                    }
                } finally
                {
                    latch.countDown();
                }
            }

        }
        , "inserter").start();
        try
        {
            // start/stop in fast iterations
            for(int i = 0; i<100; i++)
            {
                sampler.beginSampling(i, 100000);
                sampler.finishSampling(i);
            }
            // start/stop with pause to let it build up past capacity
            for(int i = 0; i<3; i++)
            {
                sampler.beginSampling(i, 100000);
                Thread.sleep(250);
                sampler.finishSampling(i);
            }

            // with empty results
            running.set(false);
            latch.await(1, TimeUnit.SECONDS);
            waitForEmpty(1000);
            for(int i = 0; i<10; i++)
            {
                sampler.beginSampling(i, 100000);
                Thread.sleep(i);
                sampler.finishSampling(i);
            }
        } finally
        {
            running.set(false);
        }
    }

    public void insert(Sampler<String> sampler)
    {
        for(int i = 1; i <= 10; i++)
        {
            for(int j = 0; j < i; j++)
            {
                String key = "item" + i;
                sampler.addSample(key, 1);
            }
        }
    }

    public void waitForEmpty(int timeoutMs) throws TimeoutException
    {
        int timeout = 0;
        while (Sampler.samplerExecutor.getPendingTaskCount() > 0)
        {
            timeout++;
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            if (timeout * 100 > timeoutMs)
            {
                throw new TimeoutException("sampler executor not cleared within timeout");
            }
        }
    }

    public <T> Map<T, Long> countMap(List<Sample<T>> target)
    {
        Map<T, Long> counts = Maps.newHashMap();
        for(Sample<T> counter : target)
        {
            counts.put(counter.value, counter.count);
        }
        return counts;
    }
}
