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

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.internal.ThrowableUtil;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

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
@SuppressWarnings("rawtypes")
public class AsyncFuture<V> extends Awaitable.AsyncAwaitable implements Future<V>
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncFuture.class);

    protected static final FailureHolder UNSET = new FailureHolder(null);
    protected static final FailureHolder UNCANCELLABLE = new FailureHolder(null);
    protected static final FailureHolder CANCELLED = new FailureHolder(ThrowableUtil.unknownStackTrace(new CancellationException(), AsyncFuture.class, "cancel(...)"));

    static class FailureHolder
    {
        final Throwable cause;
        FailureHolder(Throwable cause)
        {
            this.cause = cause;
        }
    }

    protected static Throwable cause(Object result)
    {
        return result instanceof FailureHolder ? ((FailureHolder) result).cause : null;
    }
    protected static boolean isSuccess(Object result)
    {
        return !(result instanceof FailureHolder);
    }
    protected static boolean isCancelled(Object result)
    {
        return result == CANCELLED;
    }
    protected static boolean isDone(Object result)
    {
        return result != UNSET && result != UNCANCELLABLE;
    }

    private final @Nullable Executor notifyExecutor;
    private volatile Object result;
    private volatile GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listeners;
    private static final AtomicReferenceFieldUpdater<AsyncFuture, Object> resultUpdater = newUpdater(AsyncFuture.class, Object.class, "result");
    private static final AtomicReferenceFieldUpdater<AsyncFuture, GenericFutureListener> listenersUpdater = newUpdater(AsyncFuture.class, GenericFutureListener.class, "listeners");

    private static final DeferredGenericFutureListener NOTIFYING = future -> {};

    private static interface DeferredGenericFutureListener<F extends Future<?>> extends GenericFutureListener<F> {}

    public AsyncFuture()
    {
        this(null, UNSET);
    }

    public AsyncFuture(Executor notifyExecutor)
    {
        this(notifyExecutor, UNSET);
    }

    protected AsyncFuture(Executor notifyExecutor, V immediateSuccess)
    {
        resultUpdater.lazySet(this, immediateSuccess);
        this.notifyExecutor = notifyExecutor;
    }

    protected AsyncFuture(Executor notifyExecutor, Throwable immediateFailure)
    {
        resultUpdater.lazySet(this, new FailureHolder(immediateFailure));
        this.notifyExecutor = notifyExecutor;
    }

    protected AsyncFuture(Executor notifyExecutor, FailureHolder initialState)
    {
        resultUpdater.lazySet(this, initialState);
        this.notifyExecutor = notifyExecutor;
    }

    protected AsyncFuture(Executor notifyExecutor, GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        this(notifyExecutor);
        this.listeners = listener;
    }

    protected AsyncFuture(Executor notifyExecutor, FailureHolder initialState, GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        this(notifyExecutor, initialState);
        this.listeners = listener;
    }

    protected boolean trySuccess(V v)
    {
        return trySet(v);
    }

    protected boolean tryFailure(Throwable throwable)
    {
        return trySet(new FailureHolder(throwable));
    }

    protected boolean setUncancellable()
    {
        if (trySet(UNCANCELLABLE))
            return true;
        return isUncancellable();
    }

    protected boolean setUncancellableExclusive()
    {
        return trySet(UNCANCELLABLE);
    }

    protected boolean isUncancellable()
    {
        Object result = this.result;
        return result == UNCANCELLABLE || (isDone(result) && !isCancelled(result));
    }

    public boolean cancel(boolean b)
    {
        return trySet(CANCELLED);
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
    private boolean trySet(Object v)
    {
        while (true)
        {
            Object current = result;
            if (isDone(current) || (current == UNCANCELLABLE && v == CANCELLED))
                return false;
            if (resultUpdater.compareAndSet(this, current, v))
            {
                if (v != UNCANCELLABLE)
                {
                    notifyListeners();
                    signal();
                }
                return true;
            }
        }
    }

    @Override
    public boolean isSuccess()
    {
        return isSuccess(result);
    }

    @Override
    public boolean isCancelled()
    {
        return isCancelled(result);
    }

    @Override
    public boolean isDone()
    {
        return isDone(result);
    }

    @Override
    public boolean isCancellable()
    {
        return result == UNSET;
    }

    @Override
    public Throwable cause()
    {
        return cause(result);
    }

    /**
     * if isSuccess(), returns the value, otherwise returns null
     */
    @Override
    public V getNow()
    {
        Object result = this.result;
        if (isSuccess(result))
            return (V) result;
        return null;
    }

    /**
     * Shared implementation of get() after suitable await(); assumes isDone(), and returns
     * either the success result or throws the suitable exception under failure
     */
    protected V getWhenDone() throws ExecutionException
    {
        Object result = this.result;
        if (isSuccess(result))
            return (V) result;
        if (result == CANCELLED)
            throw new CancellationException();
        throw new ExecutionException(((FailureHolder) result).cause);
    }

    @Override
    public V get() throws InterruptedException, ExecutionException
    {
        await();
        return getWhenDone();
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        if (!await(timeout, unit))
            throw new TimeoutException();
        return getWhenDone();
    }

    @Override
    public Future<V> addListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        listenersUpdater.accumulateAndGet(this, listener, AsyncFuture::appendListener);
        if (isDone())
            notifyListeners();
        return this;
    }

    private void notifyListeners()
    {
        if (notifyExecutor != null)
        {
            // TODO: could generify to any executor able to say if already executing within
            if (notifyExecutor instanceof EventExecutor && ((EventExecutor) notifyExecutor).inEventLoop())
                doNotifyListenersExclusive();
            else if (listeners != null) // submit this method, to guarantee we invoke in the submitted order
                notifyExecutor.execute(this::doNotifyListenersExclusive);
        }
        else
        {
            doNotifyListeners();
        }
    }

    private void doNotifyListeners()
    {
        @SuppressWarnings("rawtypes") GenericFutureListener listeners;
        while (true)
        {
            listeners = this.listeners;
            if (listeners == null || listeners instanceof DeferredGenericFutureListener<?>)
                return; // either no listeners, or we are already notifying listeners, so we'll get to the new one when ready

            if (listenersUpdater.compareAndSet(this, listeners, NOTIFYING))
            {
                while (true)
                {
                    invokeListener(listeners, this);
                    if (listenersUpdater.compareAndSet(this, NOTIFYING, null))
                        return;
                    listeners = listenersUpdater.getAndSet(this, NOTIFYING);
                }
            }
        }
    }

    private void doNotifyListenersExclusive()
    {
        if (listeners == null || listeners instanceof DeferredGenericFutureListener<?>)
            return; // either no listeners, or we are already notifying listeners, so we'll get to the new one when ready

        while (true)
        {
            @SuppressWarnings("rawtypes") GenericFutureListener listeners = listenersUpdater.getAndSet(this, NOTIFYING);
            if (listeners != null)
                invokeListener(listeners, this);

            if (listenersUpdater.compareAndSet(this, NOTIFYING, null))
                return;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Future<V> addListeners(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>>... listeners)
    {
        // this could be more efficient if we cared, but we do not
        return addListener(future -> {
            for (GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener : listeners)
                invokeListener((GenericFutureListener<io.netty.util.concurrent.Future<? super V>>)listener, future);
        });
    }

    @Override
    public Future<V> removeListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Future<V> removeListeners(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>>... listeners)
    {
        throw new UnsupportedOperationException();
    }

    private static <F extends Future<?>> GenericFutureListener<F> appendListener(GenericFutureListener<F> prevListener, GenericFutureListener<F> newListener)
    {
        GenericFutureListener<F> result = newListener;

        if (prevListener != null && prevListener != NOTIFYING)
        {
            result = future -> {
                invokeListener(prevListener, future);
                // we will wrap the outer invocation with invokeListener, so no need to do it here too
                newListener.operationComplete(future);
            };
        }

        if (prevListener instanceof DeferredGenericFutureListener<?>)
        {
            GenericFutureListener<F> wrap = result;
            result = (DeferredGenericFutureListener<F>) wrap::operationComplete;
        }

        return result;
    }

    /**
     * Wait for this future to complete {@link Awaitable#await()}
     */
    @Override
    public AsyncFuture<V> await() throws InterruptedException
    {
        super.await();
        return this;
    }

    /**
     * Wait for this future to complete {@link Awaitable#awaitUninterruptibly()}
     */
    @Override
    public AsyncFuture<V> awaitUninterruptibly()
    {
        super.awaitUninterruptibly();
        return this;
    }

    /**
     * Wait for this future to complete {@link Awaitable#awaitThrowUncheckedOnInterrupt()}
     */
    @Override
    public AsyncFuture<V> awaitThrowUncheckedOnInterrupt() throws UncheckedInterruptedException
    {
        super.awaitThrowUncheckedOnInterrupt();
        return this;
    }

    /**
     * {@link AsyncAwaitable#isSignalled()}
     */
    @Override
    protected boolean isSignalled()
    {
        return isDone(result);
    }

    public String toString()
    {
        Object result = this.result;
        if (isSuccess(result))
            return "(success: " + result + ')';
        if (result == UNCANCELLABLE)
            return "(uncancellable)";
        if (result == CANCELLED)
            return "(cancelled)";
        if (isDone(result))
            return "(failure: " + ((FailureHolder) result).cause + ')';
        return "(incomplete)";
    }

    protected static <F extends io.netty.util.concurrent.Future<?>> void invokeListener(GenericFutureListener<F> listener, F future)
    {
        try
        {
            listener.operationComplete(future);
        }
        catch (Throwable t)
        {
            logger.error("Failed to invoke listener {} to {}", listener, future, t);
        }
    }
}
