package org.apache.cassandra.concurrent;

import java.util.concurrent.*;

import org.apache.log4j.Logger;

public class RetryingScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor
{
    protected static Logger logger = Logger.getLogger(RetryingScheduledThreadPoolExecutor.class);

    public RetryingScheduledThreadPoolExecutor(String threadPoolName, int priority)
    {
        this(1, threadPoolName, priority);
    }

    public RetryingScheduledThreadPoolExecutor(int corePoolSize, String threadPoolName, int priority)
    {
        super(corePoolSize, new NamedThreadFactory(threadPoolName, priority));
    }

    public RetryingScheduledThreadPoolExecutor(String threadPoolName)
    {
        this(1, threadPoolName, Thread.NORM_PRIORITY);
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task)
    {
        return new LoggingScheduledFuture<V>(task);
    }

    private class LoggingScheduledFuture<V> implements RunnableScheduledFuture<V>
    {
        private final RunnableScheduledFuture<V> task;

        public LoggingScheduledFuture(RunnableScheduledFuture<V> task)
        {
            this.task = task;
        }

        public boolean isPeriodic()
        {
            return task.isPeriodic();
        }

        public long getDelay(TimeUnit unit)
        {
            return task.getDelay(unit);
        }

        public int compareTo(Delayed o)
        {
            return task.compareTo(o);
        }

        public void run()
        {
            try
            {
                task.run();
            }
            catch (Exception e)
            {
                logger.error("error running scheduled task", e);
            }
        }

        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return task.cancel(mayInterruptIfRunning);
        }

        public boolean isCancelled()
        {
            return task.isCancelled();
        }

        public boolean isDone()
        {
            return task.isDone();
        }

        public V get() throws InterruptedException, ExecutionException
        {
            return task.get();
        }

        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            return task.get(timeout, unit);
        }
    }
}
