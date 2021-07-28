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
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import com.google.common.util.concurrent.FutureCallback;

import io.netty.util.concurrent.GenericFutureListener;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * A Promise that integrates {@link io.netty.util.concurrent.Promise} with our {@link Future} API
 * to improve clarity and coherence in the codebase.
 */
@Shared(scope = SIMULATION, ancestors = INTERFACES)
public interface Promise<V> extends io.netty.util.concurrent.Promise<V>, Future<V>
{
    public static <V> GenericFutureListener<? extends Future<V>> listener(FutureCallback<V> callback)
    {
        return future -> {
            if (future.isSuccess()) callback.onSuccess(future.getNow());
            else callback.onFailure(future.cause());
        };
    }

    public static <V> GenericFutureListener<? extends Future<V>> listener(ExecutorService executor, FutureCallback<V> callback)
    {
        return future -> executor.execute(() -> {
            if (future.isSuccess()) callback.onSuccess(future.getNow());
            else callback.onFailure(future.cause());
        });
    }

    @Override
    Promise<V> addCallback(FutureCallback<? super V> callback);

    Promise<V> addCallback(FutureCallback<? super V> callback, Executor executor);

    Promise<V> addCallback(Consumer<? super V> onSuccess, Consumer<? super Throwable> onFailure);

    @Override
    Promise<V> addListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> var1);

    @Override
    Promise<V> addListeners(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>>... var1);

    @Override
    Promise<V> removeListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> var1);

    @Override
    Promise<V> removeListeners(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>>... var1);

    /**
     * Complete the promise successfully if not already complete
     * @throws IllegalStateException if already set
     */
    @Override
    Promise<V> setSuccess(V v) throws IllegalStateException;

    /**
     * Complete the promise abnormally if not already complete
     * @throws IllegalStateException if already set
     */
    @Override
    Promise<V> setFailure(Throwable throwable) throws IllegalStateException;

    /**
     * Prevent a future caller from cancelling this promise
     * @return true iff this invocation set it to uncancellable, whether or not now uncancellable
     */
    boolean setUncancellableExclusive();

    /**
     * Wait indefinitely for this promise to complete, throwing any interrupt
     * @throws InterruptedException if interrupted
     */
    @Override
    Promise<V> await() throws InterruptedException;

    /**
     * Wait indefinitely for this promise to complete
     */
    @Override
    Promise<V> awaitUninterruptibly();

    /**
     * Wait indefinitely for this promise to complete, throwing any interrupt as an UncheckedInterruptedException
     * @throws UncheckedInterruptedException if interrupted
     */
    @Override
    Promise<V> awaitThrowUncheckedOnInterrupt();

    /**
     * waits for completion; in case of failure rethrows the original exception without a new wrapping exception
     * so may cause problems for reporting stack traces
     */
    @Override
    Promise<V> sync() throws InterruptedException;

    /**
     * waits for completion; in case of failure rethrows the original exception without a new wrapping exception
     * so may cause problems for reporting stack traces
     */
    @Override
    Promise<V> syncUninterruptibly();
}

