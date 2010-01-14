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
            // clearly strict serialization is not a requirement.  just make the calling thread execute.
            this.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        }
        else
        {
            // preserve task serialization.  this is more complicated than it needs to be,
            // since TPE rejects if queue.offer reports a full queue.  we'll just
            // override this with a handler that retries until it gets in.  ugly, but effective.
            // (there is an extensive analysis of the options here at
            //  http://today.java.net/pub/a/today/2008/10/23/creating-a-notifying-blocking-thread-pool-executor.html)
            this.setRejectedExecutionHandler(new RejectedExecutionHandler()
            {
                public void rejectedExecution(Runnable task, ThreadPoolExecutor executor)
                {
                    BlockingQueue<Runnable> queue = executor.getQueue();
                    while (true)
                    {
                        if (executor.isShutdown())
                            throw new RejectedExecutionException("ThreadPoolExecutor has shut down");
                        try
                        {
                            if (queue.offer(task, 1000, TimeUnit.MILLISECONDS))
                                break;
                        }
                        catch (InterruptedException e)
                        {
                            throw new AssertionError(e);    
                        }
                    }
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
