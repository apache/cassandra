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

package org.apache.cassandra.net;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.ThrowableUtil;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.*;

/**
 * Netty's DefaultPromise uses a mutex to coordinate notifiers AND waiters between the eventLoop and the other threads.
 * Since we register cross-thread listeners, this has the potential to block internode messaging for an unknown
 * number of threads for an unknown period of time, if we are unlucky with the scheduler (which will certainly
 * happen, just with some unknown but low periodicity)
 *
 * At the same time, we manage some other efficiencies:
 *  - We save some space when registering listeners, especially if there is only one listener, as we perform no
 *    extra allocations in this case.
 *  - We permit efficient initial state declaration, avoiding unnecessary CAS or lock acquisitions when mutating
 *    a Promise we are ourselves constructing (and can easily add more; only those we use have been added)
 *
 * We can also make some guarantees about our behaviour here, although we primarily mirror Netty.
 * Specifically, we can guarantee that notifiers are always invoked in the order they are added (which may be true
 * for netty, but was unclear and is not declared).  This is useful for ensuring the correctness of some of our
 * behaviours in OutboundConnection without having to jump through extra hoops.
 *
 * The implementation loosely follows that of Netty's DefaultPromise, with some slight changes; notably that we have
 * no synchronisation on our listeners, instead using a CoW list that is cleared each time we notify listeners.
 *
 * We handle special values slightly differently.  We do not use a special value for null, instead using
 * a special value to indicate the result has not been set yet.  This means that once isSuccess() holds,
 * the result must be a correctly typed object (modulo generics pitfalls).
 * All special values are also instances of FailureHolder, which simplifies a number of the logical conditions.
 *
 * @param <V>
 */
public class AsyncPromise<V> implements Promise<V>
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncPromise.class);

    private final EventExecutor executor;
    private volatile Object result;
    private volatile GenericFutureListener<? extends Future<? super V>> listeners;
    private volatile WaitQueue waiting;
    private static final AtomicReferenceFieldUpdater<AsyncPromise, Object> resultUpdater = newUpdater(AsyncPromise.class, Object.class, "result");
    private static final AtomicReferenceFieldUpdater<AsyncPromise, GenericFutureListener> listenersUpdater = newUpdater(AsyncPromise.class, GenericFutureListener.class, "listeners");
    private static final AtomicReferenceFieldUpdater<AsyncPromise, WaitQueue> waitingUpdater = newUpdater(AsyncPromise.class, WaitQueue.class, "waiting");

    private static final FailureHolder UNSET = new FailureHolder(null);
    private static final FailureHolder UNCANCELLABLE = new FailureHolder(null);
    private static final FailureHolder CANCELLED = new FailureHolder(ThrowableUtil.unknownStackTrace(new CancellationException(), AsyncPromise.class, "cancel(...)"));

    private static final DeferredGenericFutureListener NOTIFYING = future -> {};
    private static interface DeferredGenericFutureListener<F extends Future<?>> extends GenericFutureListener<F> {}

    private static final class FailureHolder
    {
        final Throwable cause;
        private FailureHolder(Throwable cause)
        {
            this.cause = cause;
        }
    }

    public AsyncPromise(EventExecutor executor)
    {
        this(executor, UNSET);
    }

    private AsyncPromise(EventExecutor executor, FailureHolder initialState)
    {
        this.executor = executor;
        this.result = initialState;
    }

    public AsyncPromise(EventExecutor executor, GenericFutureListener<? extends Future<? super V>> listener)
    {
        this(executor);
        this.listeners = listener;
    }

    AsyncPromise(EventExecutor executor, FailureHolder initialState, GenericFutureListener<? extends Future<? super V>> listener)
    {
        this(executor, initialState);
        this.listeners = listener;
    }

    public static <V> AsyncPromise<V> uncancellable(EventExecutor executor)
    {
        return new AsyncPromise<>(executor, UNCANCELLABLE);
    }

    public static <V> AsyncPromise<V> uncancellable(EventExecutor executor, GenericFutureListener<? extends Future<? super V>> listener)
    {
        return new AsyncPromise<>(executor, UNCANCELLABLE);
    }

    public Promise<V> setSuccess(V v)
    {
        if (!trySuccess(v))
            throw new IllegalStateException("complete already: " + this);
        return this;
    }

    public Promise<V> setFailure(Throwable throwable)
    {
        if (!tryFailure(throwable))
            throw new IllegalStateException("complete already: " + this);
        return this;
    }

    public boolean trySuccess(V v)
    {
        return trySet(v);
    }

    public boolean tryFailure(Throwable throwable)
    {
        return trySet(new FailureHolder(throwable));
    }

    public boolean setUncancellable()
    {
        if (trySet(UNCANCELLABLE))
            return true;
        return result == UNCANCELLABLE;
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
                    notifyWaiters();
                }
                return true;
            }
        }
    }

    public boolean isSuccess()
    {
        return isSuccess(result);
    }

    private static boolean isSuccess(Object result)
    {
        return !(result instanceof FailureHolder);
    }

    public boolean isCancelled()
    {
        return isCancelled(result);
    }

    private static boolean isCancelled(Object result)
    {
        return result == CANCELLED;
    }

    public boolean isDone()
    {
        return isDone(result);
    }

    private static boolean isDone(Object result)
    {
        return result != UNSET && result != UNCANCELLABLE;
    }

    public boolean isCancellable()
    {
        Object result = this.result;
        return result == UNSET;
    }

    public Throwable cause()
    {
        Object result = this.result;
        if (result instanceof FailureHolder)
            return ((FailureHolder) result).cause;
        return null;
    }

    /**
     * if isSuccess(), returns the value, otherwise returns null
     */
    @SuppressWarnings("unchecked")
    public V getNow()
    {
        Object result = this.result;
        if (isSuccess(result))
            return (V) result;
        return null;
    }

    public V get() throws InterruptedException, ExecutionException
    {
        await();
        return getWhenDone();
    }

    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        if (!await(timeout, unit))
            throw new TimeoutException();
        return getWhenDone();
    }

    /**
     * Shared implementation of get() after suitable await(); assumes isDone(), and returns
     * either the success result or throws the suitable exception under failure
     */
    @SuppressWarnings("unchecked")
    private V getWhenDone() throws ExecutionException
    {
        Object result = this.result;
        if (isSuccess(result))
            return (V) result;
        if (result == CANCELLED)
            throw new CancellationException();
        throw new ExecutionException(((FailureHolder) result).cause);
    }

    /**
     * waits for completion; in case of failure rethrows the original exception without a new wrapping exception
     * so may cause problems for reporting stack traces
     */
    public Promise<V> sync() throws InterruptedException
    {
        await();
        rethrowIfFailed();
        return this;
    }

    /**
     * waits for completion; in case of failure rethrows the original exception without a new wrapping exception
     * so may cause problems for reporting stack traces
     */
    public Promise<V> syncUninterruptibly()
    {
        awaitUninterruptibly();
        rethrowIfFailed();
        return this;
    }

    private void rethrowIfFailed()
    {
        Throwable cause = this.cause();
        if (cause != null)
        {
            PlatformDependent.throwException(cause);
        }
    }

    public Promise<V> addListener(GenericFutureListener<? extends Future<? super V>> listener)
    {
        listenersUpdater.accumulateAndGet(this, listener, AsyncPromise::appendListener);
        if (isDone())
            notifyListeners();
        return this;
    }

    public Promise<V> addListeners(GenericFutureListener<? extends Future<? super V>> ... listeners)
    {
        // this could be more efficient if we cared, but we do not
        return addListener(future -> {
            for (GenericFutureListener<? extends Future<? super V>> listener : listeners)
                AsyncPromise.invokeListener((GenericFutureListener<Future<? super V>>)listener, future);
        });
    }

    public Promise<V> removeListener(GenericFutureListener<? extends Future<? super V>> listener)
    {
        throw new UnsupportedOperationException();
    }

    public Promise<V> removeListeners(GenericFutureListener<? extends Future<? super V>> ... listeners)
    {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    private void notifyListeners()
    {
        if (!executor.inEventLoop())
        {
            // submit this method, to guarantee we invoke in the submitted order
            executor.execute(this::notifyListeners);
            return;
        }

        if (listeners == null || listeners instanceof DeferredGenericFutureListener<?>)
            return; // either no listeners, or we are already notifying listeners, so we'll get to the new one when ready

        // first run our notifiers
        while (true)
        {
            GenericFutureListener listeners = listenersUpdater.getAndSet(this, NOTIFYING);
            if (listeners != null)
                invokeListener(listeners, this);

            if (listenersUpdater.compareAndSet(this, NOTIFYING, null))
                return;
        }
    }

    private static <F extends Future<?>> void invokeListener(GenericFutureListener<F> listener, F future)
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

    public Promise<V> await() throws InterruptedException
    {
        await(0L, (signal, nanos) -> { signal.await(); return true; } );
        return this;
    }

    public Promise<V> awaitUninterruptibly()
    {
        await(0L, (signal, nanos) -> { signal.awaitUninterruptibly(); return true; } );
        return this;
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException
    {
        return await(unit.toNanos(timeout),
                     (signal, nanos) -> signal.awaitUntil(nanos + System.nanoTime()));
    }

    public boolean await(long timeoutMillis) throws InterruptedException
    {
        return await(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    public boolean awaitUninterruptibly(long timeout, TimeUnit unit)
    {
        return await(unit.toNanos(timeout),
                     (signal, nanos) -> signal.awaitUntilUninterruptibly(nanos + System.nanoTime()));
    }

    public boolean awaitUninterruptibly(long timeoutMillis)
    {
        return awaitUninterruptibly(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    interface Awaiter<T extends Throwable>
    {
        boolean await(WaitQueue.Signal value, long nanos) throws T;
    }

    /**
     * A clean way to implement each variant of await using lambdas; we permit a nanos parameter
     * so that we can implement this without any unnecessary lambda allocations, although not
     * all implementations need the nanos parameter (i.e. those that wait indefinitely)
     */
    private <T extends Throwable> boolean await(long nanos, Awaiter<T> awaiter) throws T
    {
        if (isDone())
            return true;

        WaitQueue.Signal await = registerToWait();
        if (null != await)
            return awaiter.await(await, nanos);

        return true;
    }

    /**
     * Register a signal that will be notified when the promise is completed;
     * if the promise becomes completed before this signal is registered, null is returned
     */
    private WaitQueue.Signal registerToWait()
    {
        WaitQueue waiting = this.waiting;
        if (waiting == null && !waitingUpdater.compareAndSet(this, null, waiting = new WaitQueue()))
            waiting = this.waiting;
        assert waiting != null;

        WaitQueue.Signal signal = waiting.register();
        if (!isDone())
            return signal;
        signal.cancel();
        return null;
    }

    private void notifyWaiters()
    {
        WaitQueue waiting = this.waiting;
        if (waiting != null)
            waiting.signalAll();
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
}
