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

import java.util.concurrent.Executor;

import com.google.common.util.concurrent.ListenableFuture;

import io.netty.util.internal.PlatformDependent;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A Future that integrates several different (but equivalent) APIs used within Cassandra into a single concept,
 * integrating also with our {@link Awaitable} abstraction, to overall improve coherency and clarity in the codebase.
 */
public interface Future<V> extends io.netty.util.concurrent.Future<V>, ListenableFuture<V>, Awaitable
{
    /**
     * Wait indefinitely for this future to complete, throwing any interrupt
     * @throws InterruptedException if interrupted
     */
    @Override
    Future<V> await() throws InterruptedException;

    /**
     * Wait indefinitely for this future to complete
     */
    @Override
    Future<V> awaitUninterruptibly();

    /**
     * Wait indefinitely for this promise to complete, throwing any interrupt as an UnhandledInterruptedException
     * @throws UncheckedInterruptedException if interrupted
     */
    @Override
    Future<V> awaitThrowUncheckedOnInterrupt();

    default void rethrowIfFailed()
    {
        Throwable cause = this.cause();
        if (cause != null)
        {
            PlatformDependent.throwException(cause);
        }
    }

    /**
     * waits for completion; in case of failure rethrows the original exception without a new wrapping exception
     * so may cause problems for reporting stack traces
     */
    @Override
    default Future<V> sync() throws InterruptedException
    {
        await();
        rethrowIfFailed();
        return this;
    }

    /**
     * waits for completion; in case of failure rethrows the original exception without a new wrapping exception
     * so may cause problems for reporting stack traces
     */
    @Override
    default Future<V> syncUninterruptibly()
    {
        awaitUninterruptibly();
        rethrowIfFailed();
        return this;
    }

    @Deprecated
    @Override
    default boolean await(long l) throws InterruptedException
    {
        return await(l, MILLISECONDS);
    }

    @Deprecated
    @Override
    default boolean awaitUninterruptibly(long l)
    {
        return awaitUninterruptibly(l, MILLISECONDS);
    }

    /**
     * Invoke {@code runnable} on completion, using {@code executor}.
     * Tasks are submitted to their executors in the order they were added to this Future.
     */
    @Override
    default void addListener(Runnable runnable, Executor executor)
    {
        addListener(future -> executor.execute(runnable));
    }

    /**
     * Invoke {@code runnable} on completion. Depending on the implementation and its configuration, this
     * may be executed immediately by the notifying/completing thread, or asynchronously by an executor.
     * Tasks are executed, or submitted to the executor, in the order they were added to this Future.
     */
    default void addListener(Runnable runnable)
    {
        addListener(future -> runnable.run());
    }
}
