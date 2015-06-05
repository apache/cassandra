package org.apache.cassandra.service.epaxos.integration;

import org.apache.cassandra.concurrent.TracingAwareExecutorService;
import org.apache.cassandra.tracing.TraceState;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

public abstract class AbstractExecutorService implements TracingAwareExecutorService
{

    public class CallableWrapper implements Runnable
    {
        private final Callable callable;

        public CallableWrapper(Callable callable)
        {
            this.callable = callable;
        }

        @Override
        public void run()
        {
            try
            {
                callable.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void execute(Runnable command, TraceState state)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void maybeExecuteImmediately(Runnable command)
    {
        throw new UnsupportedOperationException();
    }

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
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isTerminated()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void execute(Runnable command)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<T> submit(Callable<T> task)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result)
    {
        return null;
    }
}
