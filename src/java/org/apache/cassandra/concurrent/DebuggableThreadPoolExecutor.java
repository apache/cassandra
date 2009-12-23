package org.apache.cassandra.concurrent;

import java.util.concurrent.*;

import org.apache.log4j.Logger;

public class DebuggableThreadPoolExecutor extends ThreadPoolExecutor
{
    protected static Logger logger = Logger.getLogger(JMXEnabledThreadPoolExecutor.class);

    public DebuggableThreadPoolExecutor(String threadPoolName)
    {
        this(1, 1, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(threadPoolName));
    }

    public DebuggableThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory)
    {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);

        if (maximumPoolSize > 1)
        {
            this.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        }
        else
        {
            // preserve task serialization.  this is more complicated than it needs to be,
            // since TPE rejects if queue.offer reports a full queue.
            // the easiest option (since most of TPE.execute deals with private members)
            // appears to be to wrap the given queue class with one whose offer
            // simply delegates to put().  this would be ugly, since it violates both
            // the spirit and letter of queue.offer, but effective.
            // so far, though, all our serialized executors use unbounded queues,
            // so actually implementing this has not been necessary.
            this.setRejectedExecutionHandler(new RejectedExecutionHandler()
            {
                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
                {
                    throw new AssertionError("Blocking serialized executor is not yet implemented");
                }
            });
        }
    }

    public void afterExecute(Runnable r, Throwable t)
    {
        super.afterExecute(r,t);

        // exceptions wrapped by FutureTask
        if (r instanceof FutureTask)
        {
            try
            {
                ((FutureTask) r).get();
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            catch (ExecutionException e)
            {
                logger.error("Error in executor futuretask", e);
            }
        }

        // exceptions for non-FutureTask runnables [i.e., added via execute() instead of submit()]
        if (t != null)
        {
            logger.error("Error in ThreadPoolExecutor", t);
        }
    }
}
