/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.concurrent;

import java.util.concurrent.Callable;

import javax.annotation.Nullable;

import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.RunnableFuture;

/**
 * A FutureTask that utilises Cassandra's {@link AsyncFuture}, making it compatible with {@link ExecutorPlus}.
 * Propagates exceptions to the uncaught exception handler.
 */
public class FutureTask<V> extends AsyncFuture<V> implements RunnableFuture<V>
{
    private Callable<? extends V> call;
    private volatile DebuggableTask debuggable;

    public FutureTask(Callable<? extends V> call)
    {
        this(call, call instanceof DebuggableTask ? (DebuggableTask) call : null);
    }

    public FutureTask(Runnable run)
    {
        this(callable(run), run instanceof DebuggableTask ? (DebuggableTask) run : null);
    }

    private FutureTask(Callable<? extends V> call, DebuggableTask debuggable)
    {
        this.call = call;
        this.debuggable = debuggable;
    }

    @Nullable
    DebuggableTask debuggableTask()
    {
        return debuggable;
    }

    V call() throws Exception
    {
        return call.call();
    }

    public void run()
    {
        try
        {
            if (!setUncancellable())
                return;

            trySuccess(call());
        }
        catch (Throwable t)
        {
            tryFailure(t);
        }
        finally
        {
            call = null;
            debuggable = null;
        }
    }

    protected boolean tryFailure(Throwable t)
    {
        ExecutionFailure.handle(t);
        return super.tryFailure(t);
    }

    public static <T> Callable<T> callable(Runnable run)
    {
        return new Callable<T>()
        {
            public T call()
            {
                run.run();
                return null;
            }

            public String toString()
            {
                return run.toString();
            }
        };
    }

    public static <T> Callable<T> callable(Object id, Runnable run)
    {
        return new Callable<T>()
        {
            public T call()
            {
                run.run();
                return null;
            }

            public String toString()
            {
                return id.toString();
            }
        };
    }

    public static <T> Callable<T> callable(Runnable run, T result)
    {
        return new Callable<T>()
        {
            public T call()
            {
                run.run();
                return result;
            }

            public String toString()
            {
                return run + "->" + result;
            }
        };
    }

    public static <T> Callable<T> callable(Object id, Runnable run, T result)
    {
        return new Callable<T>()
        {
            public T call()
            {
                run.run();
                return result;
            }

            public String toString()
            {
                return id.toString();
            }
        };
    }

    @Override
    protected String description()
    {
        Object desc = call;
        return desc == null ? null : call.toString();
    }
}
