package org.apache.cassandra.utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.utils.TopKSampler.SamplerResult;
import org.junit.Test;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.Counter;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;

public class TopKSamplerTest
{

    @Test
    public void testSamplerSingleInsertionsEqualMulti() throws TimeoutException
    {
        TopKSampler<String> sampler = new TopKSampler<String>();
        sampler.beginSampling(10);
        insert(sampler);
        waitForEmpty(1000);
        SamplerResult single = sampler.finishSampling(10);

        TopKSampler<String> sampler2 = new TopKSampler<String>();
        sampler2.beginSampling(10);
        for(int i = 1; i <= 10; i++)
        {
           String key = "item" + i;
           sampler2.addSample(key, MurmurHash.hash64(key), i);
        }
        waitForEmpty(1000);
        Assert.assertEquals(countMap(single.topK), countMap(sampler2.finishSampling(10).topK));
        Assert.assertEquals(sampler2.hll.cardinality(), 10);
        Assert.assertEquals(sampler.hll.cardinality(), sampler2.hll.cardinality());
    }

    @Test
    public void testSamplerOutOfOrder() throws TimeoutException
    {
        TopKSampler<String> sampler = new TopKSampler<String>();
        sampler.beginSampling(10);
        insert(sampler);
        waitForEmpty(1000);
        SamplerResult single = sampler.finishSampling(10);
        single = sampler.finishSampling(10);
    }

    /**
     * checking for exceptions from SS/HLL which are not thread safe
     */
    @Test
    public void testMultithreadedAccess() throws Exception
    {
        final AtomicBoolean running = new AtomicBoolean(true);
        final CountDownLatch latch = new CountDownLatch(1);
        final TopKSampler<String> sampler = new TopKSampler<String>();

        new Thread(new Runnable()
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
        ,"inserter").start();
        try
        {
            // start/stop in fast iterations
            for(int i = 0; i<100; i++)
            {
                sampler.beginSampling(i);
                sampler.finishSampling(i);
            }
            // start/stop with pause to let it build up past capacity
            for(int i = 0; i<3; i++)
            {
                sampler.beginSampling(i);
                Thread.sleep(250);
                sampler.finishSampling(i);
            }

            // with empty results
            running.set(false);
            latch.await(1, TimeUnit.SECONDS);
            waitForEmpty(1000);
            for(int i = 0; i<10; i++)
            {
                sampler.beginSampling(i);
                Thread.sleep(i);
                sampler.finishSampling(i);
            }
        } finally
        {
            running.set(false);
        }
    }

    private void insert(TopKSampler<String> sampler)
    {
        for(int i = 1; i <= 10; i++)
        {
            for(int j = 0; j < i; j++)
            {
                String key = "item" + i;
                sampler.addSample(key, MurmurHash.hash64(key), 1);
            }
        }
    }

    private void waitForEmpty(int timeoutMs) throws TimeoutException
    {
        int timeout = 0;
        while (!TopKSampler.samplerExecutor.getQueue().isEmpty())
        {
            timeout++;
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            if (timeout * 100 > timeoutMs)
            {
                throw new TimeoutException("TRACE executor not cleared within timeout");
            }
        }
    }

    private <T> Map<T, Long> countMap(List<Counter<T>> target)
    {
        Map<T, Long> counts = Maps.newHashMap();
        for(Counter<T> counter : target)
        {
            counts.put(counter.getItem(), counter.getCount());
        }
        return counts;
    }
}
