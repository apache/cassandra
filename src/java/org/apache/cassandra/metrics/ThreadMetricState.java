package org.apache.cassandra.metrics;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.atomic.AtomicLongArray;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;

public class ThreadMetricState
{
    private final AtomicLongArray state = new AtomicLongArray(2);
    static final ThreadMXBean BEAN = ManagementFactory.getThreadMXBean();
    private static final boolean SUN;

    static {
        boolean success = false;
        try
        {
            Class.forName("com.sun.management.ThreadMXBean");
            success = true;
        }
        catch (ClassNotFoundException ex)
        {
            success = false;
        }
        SUN = success;
    }

    Meter cpu;
    Meter allocs;
    long threadId;

    public ThreadMetricState(long threadId, Meter cpu, Meter allocs)
    {
        this.threadId = threadId;
        this.cpu = cpu;
        this.allocs = allocs;
        baseline();
    }

    public long getCPU()
    {
        return state.get(0);
    }

    public long getAllocations()
    {
        return state.get(1);
    }

    boolean setCPU(long expect, long cpu)
    {
        return state.compareAndSet(0, expect, cpu);
    }

    boolean setAllocations(long expect,long allocs)
    {
        return state.compareAndSet(1, expect, allocs);
    }

    @VisibleForTesting
    long fetchTotalCPU(long threadId)
    {
        return BEAN.getThreadCpuTime(threadId);
    }

    @VisibleForTesting
    long fetchTotalAlloc(long threadId)
    {
        final com.sun.management.ThreadMXBean beanX = (com.sun.management.ThreadMXBean) BEAN;
        return beanX.getThreadAllocatedBytes(threadId);
    }

    public void baseline()
    {
        if (BEAN.isCurrentThreadCpuTimeSupported())
        {
            long ns = fetchTotalCPU(threadId);
            if (ns > 0)
                state.set(0, ns);
        }

        if (SUN)
        {
            long bytes = fetchTotalAlloc(threadId);
            if(bytes > 0)
            {
                state.set(1, bytes);
            }
        }
    }

    public void update()
    {
        updateCPU();
        updateAllocs();
    }

    public void updateCPU()
    {
        boolean success = false;
        long delta = 0;
        while(!success && BEAN.isCurrentThreadCpuTimeSupported())
        {
            long pCpu = getCPU();
            long cpu = fetchTotalCPU(threadId);
            if(cpu > 0)
            {
                delta = cpu - pCpu;
                success = setCPU(pCpu, cpu);
            }
            else
                success = true; // skip setting
        }
        if (delta > 0)
            cpu.mark(delta);
    }

    public void updateAllocs()
    {
        long delta = 0;
        if (SUN)
        {
            final com.sun.management.ThreadMXBean beanX = (com.sun.management.ThreadMXBean) BEAN;
            boolean success = false;
            while(!success && beanX.isThreadAllocatedMemoryEnabled())
            {
                long pAllocs = getAllocations();
                long allocs = fetchTotalAlloc(threadId);
                if(allocs > 0)
                {
                    delta = allocs - pAllocs;
                    success = setAllocations(pAllocs, allocs);
                }
                else
                    success = true; // skip setting
            }
        }
        if (delta > 0)
            allocs.mark(delta);
    }
}
