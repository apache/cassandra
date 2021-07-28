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

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Extends {@link AsyncFuture} to implement the {@link Promise} interface.
 */
public class AsyncPromise<V> extends AsyncFuture<V> implements Promise<V>
{
    public AsyncPromise() {}

    public AsyncPromise(Executor notifyExecutor)
    {
        super(notifyExecutor);
    }

    AsyncPromise(Executor notifyExecutor, V immediateSuccess)
    {
        super(notifyExecutor, immediateSuccess);
    }

    AsyncPromise(Executor notifyExecutor, Throwable immediateFailure)
    {
        super(notifyExecutor, immediateFailure);
    }

    AsyncPromise(Executor notifyExecutor, FailureHolder initialState)
    {
        super(notifyExecutor, initialState);
    }

    public AsyncPromise(Executor notifyExecutor, GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        super(notifyExecutor, listener);
    }

    AsyncPromise(Executor notifyExecutor, FailureHolder initialState, GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        super(notifyExecutor, initialState, listener);
    }

    public static <V> AsyncPromise<V> uncancellable(EventExecutor executor)
    {
        return new AsyncPromise<>(executor, UNCANCELLABLE);
    }

    public static <V> AsyncPromise<V> uncancellable(EventExecutor executor, GenericFutureListener<? extends Future<? super V>> listener)
    {
        return new AsyncPromise<>(executor, UNCANCELLABLE, listener);
    }

    /**
     * Complete the promise successfully if not already complete
     * @throws IllegalStateException if already set
     */
    @Override
    public Promise<V> setSuccess(V v)
    {
        if (!trySuccess(v))
            throw new IllegalStateException("complete already: " + this);
        return this;
    }

    /**
     * Complete the promise successfully if not already complete
     * @return true iff completed promise
     */
    @Override
    public boolean trySuccess(V v)
    {
        return super.trySuccess(v);
    }

    /**
     * Complete the promise abnormally if not already complete
     * @throws IllegalStateException if already set
     */
    @Override
    public Promise<V> setFailure(Throwable throwable)
    {
        if (!tryFailure(throwable))
            throw new IllegalStateException("complete already: " + this);
        return this;
    }

    /**
     * Complete the promise abnormally if not already complete
     * @return true iff completed promise
     */
    @Override
    public boolean tryFailure(Throwable throwable)
    {
        return super.tryFailure(throwable);
    }

    /**
     * Prevent a future caller from cancelling this promise
     * @return true if the promise is now uncancellable (whether or not we did this)
     */
    @Override
    public boolean setUncancellable()
    {
        return super.setUncancellable();
    }

    /**
     * Prevent a future caller from cancelling this promise
     * @return true iff this invocation set it to uncancellable, whether or not now uncancellable
     */
    @Override
    public boolean setUncancellableExclusive()
    {
        return super.setUncancellableExclusive();
    }

    @Override
    public boolean isUncancellable()
    {
        return super.isUncancellable();
    }

    /**
     * waits for completion; in case of failure rethrows the original exception without a new wrapping exception
     * so may cause problems for reporting stack traces
     */
    @Override
    public Promise<V> sync() throws InterruptedException
    {
        super.sync();
        return this;
    }

    /**
     * waits for completion; in case of failure rethrows the original exception without a new wrapping exception
     * so may cause problems for reporting stack traces
     */
    @Override
    public Promise<V> syncUninterruptibly()
    {
        super.syncUninterruptibly();
        return this;
    }

    @Override
    public Promise<V> addListener(GenericFutureListener<? extends Future<? super V>> listener)
    {
        super.addListener(listener);
        return this;
    }

    @Override
    public Promise<V> addListeners(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>>... listeners)
    {
        super.addListeners(listeners);
        return this;
    }

    @Override
    public Promise<V> removeListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Promise<V> removeListeners(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>>... listeners)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Wait for this promise to complete
     * @throws InterruptedException if interrupted
     */
    @Override
    public AsyncPromise<V> await() throws InterruptedException
    {
        super.await();
        return this;
    }

    /**
     * Wait uninterruptibly for this promise to complete
     */
    @Override
    public AsyncPromise<V> awaitUninterruptibly()
    {
        super.awaitUninterruptibly();
        return this;
    }

    /**
     * Wait for this promise to complete, throwing any interrupt as an UnhandledInterruptedException
     * @throws UncheckedInterruptedException if interrupted
     */
    @Override
    public AsyncPromise<V> awaitThrowUncheckedOnInterrupt() throws UncheckedInterruptedException
    {
        super.awaitThrowUncheckedOnInterrupt();
        return this;
    }
}