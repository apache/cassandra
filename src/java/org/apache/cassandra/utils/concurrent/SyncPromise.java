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
import java.util.function.Consumer;

import com.google.common.util.concurrent.FutureCallback;

import io.netty.util.concurrent.Future; // checkstyle: permit this import
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Extends {@link SyncFuture} to implement the {@link Promise} interface.
 */
public class SyncPromise<V> extends SyncFuture<V> implements Promise<V>
{
    public static class WithExecutor<V> extends SyncPromise<V>
    {
        final Executor notifyExecutor;
        protected WithExecutor(Executor notifyExecutor)
        {
            this.notifyExecutor = notifyExecutor;
        }

        protected WithExecutor(Executor notifyExecutor, FailureHolder initialState)
        {
            super(initialState);
            this.notifyExecutor = notifyExecutor;
        }

        protected WithExecutor(Executor notifyExecutor, GenericFutureListener<? extends Future<? super V>> listener)
        {
            super(listener);
            this.notifyExecutor = notifyExecutor;
        }

        @Override
        public Executor notifyExecutor()
        {
            return notifyExecutor;
        }
    }

    public SyncPromise() {}

    SyncPromise(FailureHolder initialState)
    {
        super(initialState);
    }

    public SyncPromise(GenericFutureListener<? extends Future<? super V>> listener)
    {
        super(listener);
    }

    SyncPromise(FailureHolder initialState, GenericFutureListener<? extends Future<? super V>> listener)
    {
        super(initialState, listener);
    }

    public static <V> SyncPromise<V> withExecutor(Executor executor)
    {
        return new SyncPromise.WithExecutor<>(executor);
    }

    public static <V> SyncPromise<V> uncancellable()
    {
        return new SyncPromise<>(UNCANCELLABLE);
    }

    public static <V> SyncPromise<V> uncancellable(Executor executor)
    {
        return new WithExecutor<>(executor, UNCANCELLABLE);
    }

    public static <V> SyncPromise<V> uncancellable(GenericFutureListener<? extends Future<? super V>> listener)
    {
        return new SyncPromise<>(UNCANCELLABLE, listener);
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
    public SyncPromise<V> addListener(GenericFutureListener<? extends Future<? super V>> listener)
    {
        super.addListener(listener);
        return this;
    }

    @Override
    public SyncPromise<V> addListeners(GenericFutureListener<? extends Future<? super V>>... listeners)
    {
        super.addListeners(listeners);
        return this;
    }

    @Override
    public SyncPromise<V> removeListener(GenericFutureListener<? extends Future<? super V>> listener)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SyncPromise<V> removeListeners(GenericFutureListener<? extends Future<? super V>>... listeners)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SyncPromise<V> addCallback(FutureCallback<? super V> callback)
    {
        super.addCallback(callback);
        return this;
    }

    @Override
    public SyncPromise<V> addCallback(FutureCallback<? super V> callback, Executor executor)
    {
        super.addCallback(callback, executor);
        return this;
    }

    @Override
    public SyncPromise<V> addCallback(Consumer<? super V> onSuccess, Consumer<? super Throwable> onFailure)
    {
        super.addCallback(onSuccess, onFailure);
        return this;
    }

    /**
     * Wait for this promise to complete
     * @throws InterruptedException if interrupted
     */
    @Override
    public SyncPromise<V> await() throws InterruptedException
    {
        super.await();
        return this;
    }

    /**
     * Wait uninterruptibly for this promise to complete
     */
    @Override
    public SyncPromise<V> awaitUninterruptibly()
    {
        super.awaitUninterruptibly();
        return this;
    }

    /**
     * Wait for this promise to complete, throwing any interrupt as an UncheckedInterruptedException
     * @throws UncheckedInterruptedException if interrupted
     */
    @Override
    public SyncPromise<V> awaitThrowUncheckedOnInterrupt() throws UncheckedInterruptedException
    {
        super.awaitThrowUncheckedOnInterrupt();
        return this;
    }
}

