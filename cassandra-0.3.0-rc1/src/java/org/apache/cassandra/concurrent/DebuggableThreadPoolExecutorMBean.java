package org.apache.cassandra.concurrent;

public interface DebuggableThreadPoolExecutorMBean
{
    public long getPendingTasks();
}
