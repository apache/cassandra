package org.apache.cassandra.metrics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.*;

import com.codahale.metrics.Meter;

public class ThreadMetricStateTest
{
    public static long allocRaw = 0;
    public static long cpuRaw = 0;
    Meter testCpu;
    Meter testAlloc;
    ThreadMetricState state;

    class UnderTest extends ThreadMetricState
    {

        public UnderTest(long threadId, Meter cpu, Meter allocs)
        {
            super(threadId, cpu, allocs);
        }

        @Override
        long fetchTotalCPU(long threadId)
        {
            return cpuRaw;
        }

        @Override
        long fetchTotalAlloc(long threadId)
        {
            return allocRaw;
        }
    }

    @Before
    public void setup()
    {
        cpuRaw = 0;
        allocRaw = 0;
        testCpu = new Meter();
        testAlloc = new Meter();
        state = new UnderTest(1, testCpu, testAlloc);
    }

    @Test
    public void testThreadDied()
    {
        Assert.assertEquals(0, testCpu.getCount());
        cpuRaw += 10;
        state.updateCPU();
        Assert.assertEquals(10, testCpu.getCount());
        cpuRaw = -1;
        state.update();
        Assert.assertEquals(10, testCpu.getCount());
    }

    @Test
    public void testCPUDelta()
    {
        Assert.assertEquals(0, testCpu.getCount());
        cpuRaw += 10;
        state.updateCPU();
        Assert.assertEquals(10, testCpu.getCount());
        cpuRaw += 10;
        state.update();
        Assert.assertEquals(20, testCpu.getCount());
    }

    @Test
    public void testCPUDeltaBaselined()
    {
        cpuRaw = 1230;
        state = new UnderTest(1, testCpu, testAlloc);
        Assert.assertEquals(0, testCpu.getCount());
        cpuRaw += 10;
        state.updateCPU();
        Assert.assertEquals(10, testCpu.getCount());
        cpuRaw += 10;
        state.update();
        Assert.assertEquals(20, testCpu.getCount());
    }

    @Test
    public void testAllocDelta()
    {
        Assert.assertEquals(0, testAlloc.getCount());
        allocRaw += 10;
        state.updateAllocs();
        Assert.assertEquals(10, testAlloc.getCount());
        allocRaw += 10;
        state.update();
        state.update();
        Assert.assertEquals(20, testAlloc.getCount());
    }

    @Test
    public void testAllocDeltaBaselined()
    {
        allocRaw = 1230;
        state = new UnderTest(1, testCpu, testAlloc);
        Assert.assertEquals(0, testAlloc.getCount());
        allocRaw += 10;
        state.updateAllocs();
        Assert.assertEquals(10, testAlloc.getCount());
        allocRaw += 10;
        state.update();
        state.update();
        Assert.assertEquals(20, testAlloc.getCount());
    }
}
