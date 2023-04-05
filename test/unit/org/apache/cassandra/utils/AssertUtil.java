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

package org.apache.cassandra.utils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.concurrent.NamedThreadFactory;

public final class AssertUtil
{
    private AssertUtil()
    {

    }

    /**
     * Launch the input in another thread, throws a assert failure if it takes longer than the defined timeout.
     *
     * An attempt to halt the thread uses an interrupt, but only works if the underline logic respects it.
     *
     * The assert message will contain the stacktrace at the time of the timeout; grouped by common threads.
     */
    public static void assertTimeoutPreemptively(Duration timeout, Executable fn)
    {
        StackTraceElement caller = Thread.currentThread().getStackTrace()[2];
        assertTimeoutPreemptively(caller, timeout, () -> {
            fn.execute();
            return null;
        });
    }

    /**
     * Launch the input in another thread, throws a assert failure if it takes longer than the defined timeout.
     *
     * An attempt to halt the thread uses an interrupt, but only works if the underline logic respects it.
     *
     * The assert message will contain the stacktrace at the time of the timeout; grouped by common threads.
     */
    public static <T> T assertTimeoutPreemptively(Duration timeout, ThrowingSupplier<T> supplier)
    {
        StackTraceElement caller = Thread.currentThread().getStackTrace()[2];
        return assertTimeoutPreemptively(caller, timeout, supplier);
    }

    private static <T> T assertTimeoutPreemptively(StackTraceElement caller, Duration timeout, ThrowingSupplier<T> supplier)
    {

        String[] split = caller.getClassName().split("\\.");
        String simpleClassName = split[split.length - 1];
        ExecutorService executorService = Executors.newSingleThreadExecutor(new NamedThreadFactory("TimeoutTest-" + simpleClassName + "#" + caller.getMethodName()));
        try
        {
            Future<T> future = executorService.submit(() -> {
                try {
                    return supplier.get();
                }
                catch (Throwable throwable) {
                    throw Throwables.throwAsUncheckedException(throwable);
                }
            });

            long timeoutInNanos = timeout.toNanos();
            try
            {
                return future.get(timeoutInNanos, TimeUnit.NANOSECONDS);
            }
            catch (TimeoutException ex)
            {
                future.cancel(true);
                Map<Thread, StackTraceElement[]> threadDump = Thread.getAllStackTraces();
                StringBuilder sb = new StringBuilder("execution timed out after ").append(TimeUnit.NANOSECONDS.toMillis(timeoutInNanos)).append(" ms\n");
                Multimap<List<StackTraceElement>, Thread> groupCommonThreads = HashMultimap.create();
                for (Map.Entry<Thread, StackTraceElement[]> e : threadDump.entrySet())
                    groupCommonThreads.put(Arrays.asList(e.getValue()), e.getKey());

                for (Map.Entry<List<StackTraceElement>, Collection<Thread>> e : groupCommonThreads.asMap().entrySet())
                {
                    sb.append("Threads: ");
                    Joiner.on(", ").appendTo(sb, e.getValue().stream().map(Thread::getName).iterator());
                    sb.append("\n");
                    for (StackTraceElement elem : e.getKey())
                        sb.append("\t").append(elem.getClassName()).append(".").append(elem.getMethodName()).append("[").append(elem.getLineNumber()).append("]\n");
                    sb.append("\n");
                }
                throw new AssertionError(sb.toString());
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw Throwables.throwAsUncheckedException(e);
            }
            catch (ExecutionException ex)
            {
                throw Throwables.throwAsUncheckedException(ex.getCause());
            }
            catch (Throwable ex)
            {
                throw Throwables.throwAsUncheckedException(ex);
            }
        }
        finally
        {
            executorService.shutdownNow();
        }
    }

    public interface ThrowingSupplier<T>
    {
        T get() throws Throwable;
    }

    public interface Executable
    {
        void execute() throws Throwable;
    }
}
