package org.apache.cassandra.db;

import java.util.concurrent.*;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;

public class CommitLogExecutorService extends AbstractExecutorService
{
    BlockingQueue<CheaterFutureTask> queue;

    public CommitLogExecutorService()
    {
        queue = new ArrayBlockingQueue<CheaterFutureTask>(10000);
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                try
                {
                    if (DatabaseDescriptor.isCommitLogSyncEnabled())
                    {
                        while (true)
                        {
                            processWithSyncDelay();
                        }
                    }
                    else
                    {
                        while (true)
                        {
                            process();
                        }
                    }
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
        new Thread(runnable).start();
    }

    private void process() throws InterruptedException
    {
        queue.take().run();
    }

    private ArrayList<CheaterFutureTask> incompleteTasks = new ArrayList<CheaterFutureTask>();
    private ArrayList taskValues = new ArrayList(); // TODO not sure how to generify this
    private void processWithSyncDelay() throws Exception
    {
        CheaterFutureTask firstTask = queue.take();
        if (!(firstTask.getRawCallable() instanceof CommitLog.LogRecordAdder))
        {
            firstTask.run();
            return;
        }

        // attempt to do a bunch of LogRecordAdder ops before syncing
        // (this is a little clunky since there is no blocking peek method,
        //  so we have to break it into firstTask / extra tasks)
        incompleteTasks.clear();
        taskValues.clear();
        long end = System.nanoTime() + 1000 * DatabaseDescriptor.getCommitLogSyncDelay();

        // it doesn't seem worth bothering future-izing the exception
        // since if a commitlog op throws, we're probably screwed anyway
        incompleteTasks.add(firstTask);
        taskValues.add(firstTask.getRawCallable().call());
        while (!queue.isEmpty()
               && queue.peek().getRawCallable() instanceof CommitLog.LogRecordAdder
               && System.nanoTime() < end)
        {
            CheaterFutureTask task = queue.remove();
            incompleteTasks.add(task);
            taskValues.add(task.getRawCallable().call());
        }

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
        try
        {
            queue.put((CheaterFutureTask)command);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
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
