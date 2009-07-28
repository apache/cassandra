package org.apache.cassandra.db;

import java.util.concurrent.*;
import java.util.List;
import java.util.Queue;
import java.util.ArrayList;
import java.io.IOException;

public class CommitLogExecutorService extends AbstractExecutorService
{
    Queue<CheaterFutureTask> queue;

    public CommitLogExecutorService()
    {
        queue = new ConcurrentLinkedQueue<CheaterFutureTask>();
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                while (true)
                {
                    process();
                }
            }
        };
        new Thread(runnable).start();
    }

    private ArrayList<CheaterFutureTask> incompleteTasks = new ArrayList<CheaterFutureTask>();
    private ArrayList taskValues = new ArrayList(); // TODO not sure how to generify this
    void process()
    {
        while (queue.isEmpty())
        {
            try
            {
                Thread.sleep(1);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }

        // attempt to do a bunch of LogRecordAdder ops before syncing
        incompleteTasks.clear();
        taskValues.clear();
        while (!queue.isEmpty()
               && queue.peek().getRawCallable() instanceof CommitLog.LogRecordAdder
               && incompleteTasks.size() < 20)
        {
            CheaterFutureTask task = queue.remove();
            incompleteTasks.add(task);
            try
            {
                taskValues.add(task.getRawCallable().call());
            }
            catch (Exception e)
            {
                // it doesn't seem worth bothering future-izing the exception
                // since if a commitlog op throws, we're probably screwed anyway
                throw new RuntimeException(e);
            }
        }

        if (incompleteTasks.size() == 0)
        {
            // no LRAs; just run the task
            queue.remove().run();
        }
        else
        {
            // now sync and set the tasks' values (which allows thread calling get() to proceed)
            try
            {
                CommitLog.open().sync();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            for (int i = 0; i < incompleteTasks.size(); i++)
            {
                incompleteTasks.get(i).set(taskValues.get(i));
            }
        }
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value)
    {
        return newTaskFor(Executors.callable(runnable, value));
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable)
    {
        return new CheaterFutureTask(callable);
    }

    public void execute(Runnable command)
    {
        queue.add((CheaterFutureTask)command);
    }

    public boolean isShutdown()
    {
        return false;
    }

    public boolean isTerminated()
    {
        return false;
    }

    // cassandra is crash-only so there's no need to implement the shutdown methods
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

class CheaterFutureTask<V> extends FutureTask<V>
{
    private Callable rawCallable;

    public CheaterFutureTask(Callable<V> callable)
    {
        super(callable);
        rawCallable = callable;
    }

    public Callable getRawCallable()
    {
        return rawCallable;
    }

    @Override
    public void set(V v)
    {
        super.set(v);
    }
}
