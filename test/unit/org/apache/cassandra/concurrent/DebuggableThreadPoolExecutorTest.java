package org.apache.cassandra.concurrent;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.utils.WrappedRunnable;

public class DebuggableThreadPoolExecutorTest
{
    @Test
    public void testSerialization() throws InterruptedException
    {
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>(1);
        DebuggableThreadPoolExecutor executor = new DebuggableThreadPoolExecutor(1,
                                                                                 1,
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.MILLISECONDS,
                                                                                 q,
                                                                                 new NamedThreadFactory("TEST"));
        WrappedRunnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws InterruptedException
            {
                Thread.sleep(50);
            }
        };
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++)
        {
            executor.submit(runnable);
        }
        assert q.size() > 0 : q.size();
        while (executor.getCompletedTaskCount() < 10)
            continue;
        long delta = System.currentTimeMillis() - start;
        assert delta >= 9 * 50 : delta;
    }
}
