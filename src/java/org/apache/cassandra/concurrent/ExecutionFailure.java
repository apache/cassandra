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
import java.util.concurrent.Future;

import org.apache.cassandra.concurrent.DebuggableTask.RunnableDebuggableTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.WithResources;

/**
 * Standardised handling of failures during execution - mostly this involves invoking a thread's
 * {@link java.lang.Thread.UncaughtExceptionHandler} or
 * {@link JVMStabilityInspector#uncaughtException(Thread, Throwable)},
 * with special handling for {@link CompactionInterruptedException}.
 * This class also provides wrappers for {@link WithResources} with {@link Runnable} and {@link Callable}.
 */
public class ExecutionFailure
{
    private static final Logger logger = LoggerFactory.getLogger(ExecutionFailure.class);

    /**
     * Invoke the relevant {@link java.lang.Thread.UncaughtExceptionHandler},
     * ignoring (except for logging) any {@link CompactionInterruptedException}
     */
    public static void handle(Throwable t)
    {
        try
        {
            if (t instanceof CompactionInterruptedException)
            {
                // TODO: should we check to see there aren't nested CompactionInterruptedException?
                logger.info(t.getMessage());
                if (t.getSuppressed() != null && t.getSuppressed().length > 0)
                    logger.warn("Interruption of compaction encountered exceptions:", t);
                else
                    logger.trace("Full interruption stack trace:", t);
            }
            else
            {
                Thread thread = Thread.currentThread();
                Thread.UncaughtExceptionHandler handler = thread.getUncaughtExceptionHandler();
                if (handler == null)
                    handler = JVMStabilityInspector::uncaughtException;
                handler.uncaughtException(thread, t);
            }
        }
        catch (Throwable shouldNeverHappen)
        {
            logger.error("Unexpected error while handling unexpected error", shouldNeverHappen);
        }
    }

    /**
     * See {@link #propagating(WithResources, Runnable)}
     */
    static Runnable propagating(Runnable wrap)
    {
        return wrap instanceof FutureTask<?> ? wrap : propagating(WithResources.none(), wrap);
    }

    /**
     * In the case of plain executions, we want to handle exceptions without the full {@link FutureTask} machinery
     * while still propagating the exception to the encapsulating Future
     */
    static Runnable propagating(WithResources withResources, Runnable wrap)
    {
        return enforceOptions(withResources, wrap, true);
    }

    /**
     * See {@link #suppressing(WithResources, Runnable)}
     */
    static Runnable suppressing(Runnable wrap)
    {
        return wrap instanceof FutureTask<?> ? wrap : suppressing(WithResources.none(), wrap);
    }

    /**
     * In the case of scheduled periodic tasks, we don't want exceptions propagating to cancel the recurring execution.
     */
    static Runnable suppressing(WithResources withResources, Runnable wrap)
    {
        return enforceOptions(withResources, wrap, false);
    }

    /**
     * @see #suppressing(WithResources, Runnable)
     */
    static RunnableDebuggableTask suppressingDebuggable(WithResources withResources, RunnableDebuggableTask debuggable)
    {
        return enforceOptionsDebuggable(withResources, debuggable, false);
    }

    /**
     * Encapsulate the execution, propagating or suppressing any exceptions as requested.
     *
     * note that if {@code wrap} is a {@link java.util.concurrent.Future} its exceptions may not be captured,
     * however the codebase should be using our internal {@link Future} variants which handle exceptions in the
     * desired way.
     */
    private static Runnable enforceOptions(WithResources withResources, Runnable wrap, boolean propagate)
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                try (@SuppressWarnings("unused") Closeable close = withResources.get())
                {
                    wrap.run();
                }
                catch (Throwable t)
                {
                    handle(t);
                    if (propagate)
                        throw t;
                }
            }

            @Override
            public String toString()
            {
                return wrap.toString();
            }
        };
    }

    /**
     * @see #enforceOptions(WithResources, Runnable, boolean)
     */
    private static RunnableDebuggableTask enforceOptionsDebuggable(WithResources withResources, RunnableDebuggableTask debuggable, boolean propagate)
    {
        return new RunnableDebuggableTask()
        {
            @Override
            public void run()
            {
                try (@SuppressWarnings("unused") Closeable close = withResources.get())
                {
                    debuggable.run();
                }
                catch (Throwable t)
                {
                    handle(t);
                    if (propagate)
                        throw t;
                }
            }

            @Override
            public String toString()
            {
                return debuggable.toString();
            }

            @Override
            public long creationTimeNanos()
            {
                return debuggable.creationTimeNanos();
            }

            @Override
            public long startTimeNanos()
            {
                return debuggable.startTimeNanos();
            }

            @Override
            public String description()
            {
                return debuggable.description();
            }
        };
    }

    /**
     * See {@link #enforceOptions(WithResources, Callable)}
     */
    static <V> Callable<V> propagating(Callable<V> wrap)
    {
        return enforceOptions(WithResources.none(), wrap);
    }

    /**
     * In the case of non-recurring scheduled tasks, we want to handle exceptions without the full {@link FutureTask}
     * machinery, while still propagating the exception to the encapsulating Future
     */
    static <V> Callable<V> enforceOptions(WithResources withResources, Callable<V> wrap)
    {
        return new Callable<V>()
        {
            @Override
            public V call() throws Exception
            {
                try (@SuppressWarnings("unused") Closeable close = withResources.get())
                {
                    return wrap.call();
                }
                catch (Throwable t)
                {
                    handle(t);
                    throw t;
                }
            }

            @Override
            public String toString()
            {
                return wrap.toString();
            }
        };
    }
}
