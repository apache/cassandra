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

package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.TraceState;

/**
 * Timer implementation based on the hashed wheel algorithm with 100ms precision, using Netty's
 * {@link HashedWheelTimer} under the hood.
 *
 * @see #onTimeout(Runnable, long, TimeUnit)
 * @see #onTimeout(Runnable, long, TimeUnit, ExecutorLocals)
 */
public class Timer
{
    private static final String THREAD_NAME = "hashed-wheel-timer";
    public static final Timer INSTANCE = new Timer();

    private final HashedWheelTimer timer;

    private Timer()
    {
        this.timer = new HashedWheelTimer(new ThreadFactoryBuilder().setDaemon(true).setNameFormat(THREAD_NAME).build(),
                                          100, TimeUnit.MILLISECONDS);

        this.timer.start();
    }

    /**
     * @see #onTimeout(Runnable, long, TimeUnit, ExecutorLocals).
     */
    public Future<Void> onTimeout(Runnable task, long timeout, TimeUnit unit)
    {
        return onTimeout(task, timeout, unit, null);
    }

    /**
     * Schedules the given {@code task} to be run after the given {@code timeout} with related {@code unit} expires,
     * and returns a {@link Future} that can be used to check for expiration and cancel the timeout. Passed
     * {@code executorLocals} are eventually propagated to the executed task.
     */
    public Future<Void> onTimeout(Runnable task, long timeout, TimeUnit unit, ExecutorLocals executorLocals)
    {
        ClientWarn.State clientWarnState = executorLocals == null ? null : executorLocals.clientWarnState;
        TraceState traceState = executorLocals == null ? null : executorLocals.traceState;
        CompletableFuture<Void> result = new CompletableFuture<>();
        Timeout handle = timer.newTimeout(ignored ->
                                          {
                                              ExecutorLocals.set(ExecutorLocals.create(traceState, clientWarnState));
                                              try
                                              {
                                                  task.run();
                                                  result.complete(null);
                                              }
                                              catch (Throwable ex)
                                              {
                                                  result.completeExceptionally(ex);
                                              }
                                          }, timeout, unit);

        return new Future<Void>()
        {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning)
            {
                return handle.cancel();
            }

            @Override
            public boolean isCancelled()
            {
                return handle.isCancelled();
            }

            @Override
            public boolean isDone()
            {
                return handle.isExpired();
            }

            @Override
            public Void get() throws InterruptedException, ExecutionException
            {
                return result.get();
            }

            @Override
            public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
            {
                return result.get(timeout, unit);
            }
        };
    }

    public void shutdown()
    {
        timer.stop();
    }
}
