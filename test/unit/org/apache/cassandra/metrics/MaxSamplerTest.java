package org.apache.cassandra.metrics;

import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.metrics.Sampler.Sample;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MaxSamplerTest extends SamplerTest
{
    @Before
    public void setSampler()
    {
        this.sampler = new MaxSampler<String>()
        {
            public String toString(String value)
            {
                return value;
            }
        };
    }

    @Test
    public void testReturnsMax() throws TimeoutException
    {
        sampler.beginSampling(5, 100000);
        add();
        List<Sample<String>> result = sampler.finishSampling(10);
        for (int i = 9995 ; i < 10000 ; i ++)
        {
            final String key = "test" + i;
            Assert.assertTrue(result.stream().anyMatch(s -> s.value.equals(key)));
        }
    }

    @Test
    public void testSizeEqualsCapacity() throws TimeoutException
    {
        sampler.beginSampling(10, 100000);
        add();
        List<Sample<String>> result = sampler.finishSampling(10);
        for (int i = 9990 ; i < 10000 ; i ++)
        {
            final String key = "test" + i;
            Assert.assertTrue(result.stream().anyMatch(s -> s.value.equals(key)));
        }
    }

    @Test
    public void testCapacityLarger() throws TimeoutException
    {

        sampler.beginSampling(100, 100000);
        add();
        List<Sample<String>> result = sampler.finishSampling(10);
        for (int i = 9990 ; i < 10000 ; i ++)
        {
            final String key = "test" + i;
            Assert.assertTrue(result.stream().anyMatch(s -> s.value.equals(key)));
        }
    }

    private void add() throws TimeoutException
    {
        for (int i = 0 ; i < 10000 ; i ++)
        {
            // dont load shed test data
            if (i % 999 == 0)
                waitForEmpty(1000);
            sampler.addSample("test"+i, i);
        }
        waitForEmpty(1000);
    }
}
