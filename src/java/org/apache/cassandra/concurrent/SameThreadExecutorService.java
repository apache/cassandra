package org.apache.cassandra.concurrent;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SameThreadExecutorService extends AbstractExecutorService
{

    public static SameThreadExecutorService INSTANCE = new SameThreadExecutorService();

    @Override
    public void shutdown()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isShutdown()
    {
        return false;
    }

    @Override
    public boolean isTerminated()
    {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void execute(Runnable command)
    {
        command.run();
    }

    public static void main(String[] args)
    {
        ExecutorService exec = new SameThreadExecutorService();
        Future<?> f = exec.submit(new Runnable()
        {
            @Override
            public void run()
            {
                throw new RuntimeException();
            }
        });
    }

}
