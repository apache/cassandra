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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture; // checkstyle: permit this import

import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.internal.ThrowableUtil;
import org.apache.cassandra.utils.concurrent.ListenerList.CallbackBiConsumerListener;
import org.apache.cassandra.utils.concurrent.ListenerList.CallbackLambdaListener;
import org.apache.cassandra.utils.concurrent.ListenerList.CallbackListener;
import org.apache.cassandra.utils.concurrent.ListenerList.CallbackListenerWithExecutor;
import org.apache.cassandra.utils.concurrent.ListenerList.GenericFutureListenerList;
import org.apache.cassandra.utils.concurrent.ListenerList.RunnableWithExecutor;
import org.apache.cassandra.utils.concurrent.ListenerList.RunnableWithNotifyExecutor;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import static org.apache.cassandra.utils.concurrent.ListenerList.notifyListener;

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
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractFuture<V> implements Future<V>
{
    protected static final FailureHolder UNSET = new FailureHolder(null);
    protected static final FailureHolder UNCANCELLABLE = new FailureHolder(null);
    protected static final FailureHolder CANCELLED = new FailureHolder(ThrowableUtil.unknownStackTrace(new CancellationException(), AbstractFuture.class, "cancel(...)"));

    static class FailureHolder
    {
        final Throwable cause;
        FailureHolder(Throwable cause)
        {
            this.cause = cause;
        }
    }

    private static Throwable cause(Object result)
    {
        return result instanceof FailureHolder ? ((FailureHolder) result).cause : null;
    }
    static boolean isSuccess(Object result)
    {
        return !(result instanceof FailureHolder);
    }
    static boolean isCancelled(Object result)
    {
        return result == CANCELLED;
    }
    static boolean isDone(Object result)
    {
        return result != UNSET && result != UNCANCELLABLE;
    }

    volatile Object result;
    volatile ListenerList<V> listeners; // either a ListenerList or GenericFutureListener (or null)
    static final AtomicReferenceFieldUpdater<AbstractFuture, Object> resultUpdater = newUpdater(AbstractFuture.class, Object.class, "result");
    static final AtomicReferenceFieldUpdater<AbstractFuture, ListenerList> listenersUpdater = newUpdater(AbstractFuture.class, ListenerList.class, "listeners");

    protected AbstractFuture(FailureHolder initialState)
    {
        // TODO: document visibility of constructor (i.e. must be safe published)
        resultUpdater.lazySet(this, initialState);
    }

    public AbstractFuture()
    {
        this(UNSET);
    }

    protected AbstractFuture(V immediateSuccess)
    {
        resultUpdater.lazySet(this, immediateSuccess);
    }

    protected AbstractFuture(Throwable immediateFailure)
    {
       this(new FailureHolder(immediateFailure));
    }

    protected AbstractFuture(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        this();
        listenersUpdater.lazySet(this, new GenericFutureListenerList(listener));
    }

    protected AbstractFuture(FailureHolder initialState, GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        this(initialState);
        listenersUpdater.lazySet(this, new GenericFutureListenerList(listener));
    }

    public Executor notifyExecutor()
    {
        return null;
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
    abstract boolean trySet(Object v);

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

    /**
     * Logically append {@code newListener} to {@link #listeners}
     * (at this stage it is a stack, so we actually prepend)
     */
    abstract void appendListener(ListenerList<V> newListener);

    /**
     * Support {@link com.google.common.util.concurrent.Futures#addCallback} natively
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public AbstractFuture<V> addCallback(FutureCallback<? super V> callback)
    {
        appendListener(new CallbackListener<>(this, callback));
        return this;
    }

    /**
     * Support {@link com.google.common.util.concurrent.Futures#addCallback} natively
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public AbstractFuture<V> addCallback(BiConsumer<? super V, Throwable> callback)
    {
        appendListener(new CallbackBiConsumerListener<>(this, callback, null));
        return this;
    }

    @Override
    public Future<V> addCallback(BiConsumer<? super V, Throwable> callback, Executor executor)
    {
        appendListener(new CallbackBiConsumerListener<>(this, callback, executor));
        return this;
    }

    /**
     * Support {@link com.google.common.util.concurrent.Futures#addCallback} natively
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public AbstractFuture<V> addCallback(FutureCallback<? super V> callback, Executor executor)
    {
        Preconditions.checkNotNull(executor);
        appendListener(new CallbackListenerWithExecutor<>(this, callback, executor));
        return this;
    }

    /**
     * Support more fluid version of {@link com.google.common.util.concurrent.Futures#addCallback}
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public AbstractFuture<V> addCallback(Consumer<? super V> onSuccess, Consumer<? super Throwable> onFailure)
    {
        appendListener(new CallbackLambdaListener<>(this, onSuccess, onFailure, null));
        return this;
    }

    /**
     * Support {@link com.google.common.util.concurrent.Futures#transformAsync(ListenableFuture, AsyncFunction, Executor)} natively
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public <T> Future<T> map(Function<? super V, ? extends T> mapper)
    {
        return map(mapper, null);
    }

    /**
     * Support more fluid version of {@link com.google.common.util.concurrent.Futures#addCallback}
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public AbstractFuture<V> addCallback(Consumer<? super V> onSuccess, Consumer<? super Throwable> onFailure, Executor executor)
    {
        appendListener(new CallbackLambdaListener<>(this, onSuccess, onFailure, executor));
        return this;
    }

    /**
     * Support {@link com.google.common.util.concurrent.Futures#transform(ListenableFuture, com.google.common.base.Function, Executor)} natively
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    protected <T> Future<T> map(AbstractFuture<T> result, Function<? super V, ? extends T> mapper, @Nullable Executor executor)
    {
        addListener(() -> {
            try
            {
                if (isSuccess()) result.trySet(mapper.apply(getNow()));
                else result.tryFailure(cause());
            }
            catch (Throwable t)
            {
                result.tryFailure(t);
                throw t;
            }
        }, executor);
        return result;
    }

    /**
     * Support {@link com.google.common.util.concurrent.Futures#transformAsync(ListenableFuture, AsyncFunction, Executor)} natively
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    protected <T> Future<T> flatMap(AbstractFuture<T> result, Function<? super V, ? extends Future<T>> flatMapper, @Nullable Executor executor)
    {
        addListener(() -> {
            try
            {
                if (isSuccess()) flatMapper.apply(getNow()).addListener(propagate(result));
                else result.tryFailure(cause());
            }
            catch (Throwable t)
            {
                result.tryFailure(t);
                throw t;
            }
        }, executor);
        return result;
    }

    /**
     * Add a listener to be invoked once this future completes.
     * Listeners are submitted to {@link #notifyExecutor} in the order they are added (or the specified executor
     * in the case of {@link #addListener(Runnable, Executor)}.
     * if {@link #notifyExecutor} is unset, they are invoked in the order they are added.
     * The ordering holds across all variants of this method.
     */
    public Future<V> addListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        appendListener(new GenericFutureListenerList(listener));
        return this;
    }

    /**
     * Add a listener to be invoked once this future completes.
     * Listeners are submitted to their {@code #executor} (or {@link #notifyExecutor}) in the order they are added;
     * if {@link #notifyExecutor} is unset, they are invoked in the order they are added.
     * The ordering holds across all variants of this method.
     */
    public void addListener(Runnable task, @Nullable Executor executor)
    {
        appendListener(new RunnableWithExecutor(task, executor));
    }

    /**
     * Add a listener to be invoked once this future completes.
     * Listeners are submitted to {@link #notifyExecutor} in the order they are added (or the specified executor
     * in the case of {@link #addListener(Runnable, Executor)}.
     * if {@link #notifyExecutor} is unset, they are invoked in the order they are added.
     * The ordering holds across all variants of this method.
     */
    public void addListener(Runnable task)
    {
        appendListener(new RunnableWithNotifyExecutor(task));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Future<V> addListeners(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>>... listeners)
    {
        // this could be more efficient if we cared, but we do not
        return addListener(future -> {
            for (GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener : listeners)
                notifyListener((GenericFutureListener<io.netty.util.concurrent.Future<? super V>>)listener, future);
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

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException
    {
        return Defaults.await(this, timeout, unit);
    }

    @Override
    public boolean awaitThrowUncheckedOnInterrupt(long time, TimeUnit units) throws UncheckedInterruptedException
    {
        return Defaults.awaitThrowUncheckedOnInterrupt(this, time, units);
    }

    @Override
    public boolean awaitUninterruptibly(long timeout, TimeUnit unit)
    {
        return Defaults.awaitUninterruptibly(this, timeout, unit);
    }

    @Override
    public boolean awaitUntilThrowUncheckedOnInterrupt(long nanoTimeDeadline) throws UncheckedInterruptedException
    {
        return Defaults.awaitUntilThrowUncheckedOnInterrupt(this, nanoTimeDeadline);
    }

    @Override
    public boolean awaitUntilUninterruptibly(long nanoTimeDeadline)
    {
        return Defaults.awaitUntilUninterruptibly(this, nanoTimeDeadline);
    }

    /**
     * Wait for this future to complete {@link Awaitable#awaitUninterruptibly()}
     */
    @Override
    public Future<V> awaitUninterruptibly()
    {
        return Defaults.awaitUninterruptibly(this);
    }

    /**
     * Wait for this future to complete {@link Awaitable#awaitThrowUncheckedOnInterrupt()}
     */
    @Override
    public Future<V> awaitThrowUncheckedOnInterrupt() throws UncheckedInterruptedException
    {
        return Defaults.awaitThrowUncheckedOnInterrupt(this);
    }

    public String toString()
    {
        String description = description();
        String state = stateInfo();
        return description == null ? state : (state + ' ' + description);
    }

    private String stateInfo()
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

    protected String description()
    {
        return null;
    }

    /**
     * @return a listener that will propagate to {@code to} the result of the future it is invoked with
     */
    private static <V> GenericFutureListener<? extends Future<V>> propagate(AbstractFuture<? super V> to)
    {
        return from -> {
            if (from.isSuccess()) to.trySuccess(from.getNow());
            else to.tryFailure(from.cause());
        };
    }
}
