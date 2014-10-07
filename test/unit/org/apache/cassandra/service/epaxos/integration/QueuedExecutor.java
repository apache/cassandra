package org.apache.cassandra.service.epaxos.integration;

import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;

import com.google.common.base.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.Pair;

public class QueuedExecutor extends AbstractExecutorService
{
    private static final Logger logger = LoggerFactory.getLogger(QueuedExecutor.class);

    // used for labelling log output
    private volatile Integer nextNodeSubmit = null;
    private volatile Integer currentNode = null;
    private Queue<Pair<Integer, Runnable>> queue = new LinkedTransferQueue<>();
    private volatile int executed = 0;
    private volatile int skipAt = 0;

    private final ArrayList<Runnable> preRunCallbacks = new ArrayList<>(10);
    private final ArrayList<Runnable> skippablePostRunCallbacks = new ArrayList<>(10);
    private final ArrayList<Predicate<Throwable>> exceptionHandlers = new ArrayList<>(10);
    private final long threadId;

    public QueuedExecutor()
    {
        threadId = Thread.currentThread().getId();
    }

    private void assertThread()
    {
        assert Thread.currentThread().getId() == threadId;
    }

    private void run(Runnable runnable)
    {
        try
        {
            runnable.run();
        }
        catch (Throwable t)
        {
            boolean ignore = false;
            for (Predicate<Throwable> handler: exceptionHandlers)
            {
                ignore |= handler.apply(t);
            }

            if (ignore)
            {
                logger.warn("ignoring: {}", t);
            }
            else
            {
                throw t;
            }
        }
    }

    private boolean skipCallbacks()
    {
        return skipAt > 0 && queueSize() > skipAt;
    }

    private synchronized void maybeRun(Runnable runnable)
    {
        assertThread();

        boolean wasEmpty = queue.isEmpty();
        queue.add(Pair.create(nextNodeSubmit, runnable));
        nextNodeSubmit = null;
        if (wasEmpty)
        {
            while (!queue.isEmpty())
            {
                Pair<Integer, Runnable> nextTask = queue.peek();  // prevents the next added task thinking it should run
                Integer prevNode = currentNode;
                currentNode = nextTask.left;

                for (Runnable r: preRunCallbacks)
                {
                    r.run();
                }

                run(nextTask.right);

                currentNode = prevNode;
                queue.remove();
                executed++;
                if (!skipCallbacks())
                {
                    for (Runnable r: skippablePostRunCallbacks)
                    {
                        r.run();
                    }
                }
                else
                {
                    logger.debug("skipping callbacks");
                }
            }
        }
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result)
    {
        assertThread();
        maybeRun(task);
        return null;
    }

    @Override
    public Future<?> submit(Runnable task)
    {
        assertThread();
        maybeRun(task);
        return null;
    }

    public int getExecuted()
    {
        assertThread();
        return executed;
    }

    public int queueSize()
    {
        assertThread();
        return queue.size();
    }

    public void addPreRunCallback(Runnable r)
    {
        assertThread();
        preRunCallbacks.add(r);
    }

    public void addSkippablePostRunCallback(Runnable r)
    {
        assertThread();
        skippablePostRunCallbacks.add(r);
    }

    public void addExceptionHandler(Predicate<Throwable> handler)
    {
        exceptionHandlers.add(handler);
    }

    public void setSkipAt(int skipAt)
    {
        this.skipAt = skipAt;
    }

    public Integer getCurrentNode()
    {
        return currentNode;
    }

    public void setNextNodeSubmit(Integer nextNodeSubmit)
    {
        this.nextNodeSubmit = nextNodeSubmit;
    }
}
