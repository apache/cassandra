package org.apache.cassandra.db.commitlog;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.ObjectName;

public abstract class AbstractCommitLogExecutorService extends AbstractExecutorService implements ICommitLogExecutorService
{
    protected volatile long completedTaskCount = 0;

    protected static void registerMBean(Object o)
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(o, new ObjectName("org.apache.cassandra.db:type=Commitlog"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the current number of running tasks
     */
    public int getActiveCount()
    {
        return 1;
    }

    /**
     * Get the number of completed tasks
     */
    public long getCompletedTasks()
    {
        return completedTaskCount;
    }

    // cassandra is crash-only so there's no need to implement the shutdown methods

    public boolean isShutdown()
    {
        return false;
    }

    public boolean isTerminated()
    {
        return false;
    }

    public void shutdown()
    {
        throw new UnsupportedOperationException();
    }

    public List<Runnable> shutdownNow()
    {
        throw new UnsupportedOperationException();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }
}
