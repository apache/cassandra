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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture; // checkstyle: permit this import

import io.netty.util.concurrent.GenericFutureListener;

/**
 * Our default {@link Future} implementation, with all state being managed without locks (except those used by the JVM).
 *
 * Some implementation comments versus Netty's default promise:
 *  - We permit efficient initial state declaration, avoiding unnecessary CAS or lock acquisitions when mutating
 *    a Promise we are ourselves constructing (and can easily add more; only those we use have been added)
 *  - We guarantee the order of invocation of listeners (and callbacks etc, and with respect to each other)
 *  - We save some space when registering listeners, especially if there is only one listener, as we perform no
 *    extra allocations in this case.
 *  - We implement our invocation list as a concurrent stack, that is cleared on notification
 *  - We handle special values slightly differently.
 *    - We do not use a special value for null, instead using a special value to indicate the result has not been set.
 *      This means that once isSuccess() holds, the result must be a correctly typed object (modulo generics pitfalls).
 *    - All special values are also instances of FailureHolder, which simplifies a number of the logical conditions.
 */
public class AsyncFuture<V> extends AbstractFuture<V>
{
    @SuppressWarnings({ "rawtypes" })
    private static final AtomicReferenceFieldUpdater<AsyncFuture, WaitQueue> waitingUpdater = AtomicReferenceFieldUpdater.newUpdater(AsyncFuture.class, WaitQueue.class, "waiting");
    @SuppressWarnings({ "unused" })
    private volatile WaitQueue waiting;

    public AsyncFuture()
    {
        super();
    }

    protected AsyncFuture(V immediateSuccess)
    {
        super(immediateSuccess);
    }

    protected AsyncFuture(Throwable immediateFailure)
    {
        super(immediateFailure);
    }

    protected AsyncFuture(FailureHolder initialState)
    {
        super(initialState);
    }

    protected AsyncFuture(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        super(listener);
    }

    protected AsyncFuture(FailureHolder initialState, GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        super(initialState, listener);
    }

    /**
     * Shared implementation of various promise completion methods.
     * Updates the result if it is possible to do so, returning success/failure.
     *
     * If the promise is UNSET the new value will succeed;
     *          if it is UNCANCELLABLE it will succeed only if the new value is not CANCELLED
     *          otherwise it will fail, as isDone() is implied
     *
     * If the update succeeds, and the new state implies isDone(), any listeners and waiters will be notified
     */
    boolean trySet(Object v)
    {
        while (true)
        {
            Object current = result;
            if (isDone(current) || (current == UNCANCELLABLE && (v == CANCELLED || v == UNCANCELLABLE)))
                return false;
            if (resultUpdater.compareAndSet(this, current, v))
            {
                if (v != UNCANCELLABLE)
                {
                    ListenerList.notify(listenersUpdater, this);
                    AsyncAwaitable.signalAll(waitingUpdater, this);
                }
                return true;
            }
        }
    }

    /**
     * Logically append {@code newListener} to {@link #listeners}
     * (at this stage it is a stack, so we actually prepend)
     *
     * @param newListener must be either a {@link ListenerList} or {@link GenericFutureListener}
     */
    void appendListener(ListenerList<V> newListener)
    {
        ListenerList.push(listenersUpdater, this, newListener);
        if (isDone())
            ListenerList.notify(listenersUpdater, this);
    }

    /**
     * Support {@link com.google.common.util.concurrent.Futures#transform} natively
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public <T> Future<T> map(Function<? super V, ? extends T> mapper, Executor executor)
    {
        return map(new AsyncFuture<>(), mapper, executor);
    }

    /**
     * Support {@link com.google.common.util.concurrent.Futures#transformAsync(ListenableFuture, AsyncFunction, Executor)} natively
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public <T> Future<T> flatMap(Function<? super V, ? extends Future<T>> flatMapper, @Nullable Executor executor)
    {
        return flatMap(new AsyncFuture<>(), flatMapper, executor);
    }

    /**
     * Wait for this future to complete {@link Awaitable#await()}
     */
    @Override
    public AsyncFuture<V> await() throws InterruptedException
    {
        //noinspection unchecked
        return AsyncAwaitable.await(waitingUpdater, Future::isDone, this);
    }

    @Override
    public boolean awaitUntil(long nanoTimeDeadline) throws InterruptedException
    {
        return AsyncAwaitable.awaitUntil(waitingUpdater, Future::isDone, this, nanoTimeDeadline);
    }
}

