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

import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

/**
 * Netty's PromiseCombiner is not threadsafe, and we combine futures from multiple event executors.
 *
 * This class groups a number of Future into a single logical Future, by registering a listener to each that
 * decrements a shared counter; if any of them fail, the FutureCombiner is completed with the first cause,
 * but in all scenario only completes when all underlying future have completed (exceptionally or otherwise)
 *
 * This Future is always uncancellable.
 *
 * We extend AsyncFuture, and simply provide it an uncancellable Promise that will be completed by the listeners
 * registered to the input futures.
 */
public class FutureCombiner<T> extends AsyncFuture<T>
{
    private interface ListenerFactory<T>
    {
        Listener<T> create(int count, Supplier<T> onSuccess, FutureCombiner<T> complete);
    }

    /**
     * Tracks completion; once all futures have completed, invokes {@code Listener#complete#trySuccess(Object)} with {@link Listener#onSuccess}.
     * Never invokes failure on {@link Listener#complete}.
     */
    private static class Listener<T> extends AtomicInteger implements GenericFutureListener<io.netty.util.concurrent.Future<Object>>
    {
        Supplier<T> onSuccess; // non-final so we can release resources immediately when failing fast
        final FutureCombiner<T> complete;

        Listener(int count, Supplier<T> onSuccess, FutureCombiner<T> complete)
        {
            super(count);
            Preconditions.checkNotNull(onSuccess);
            this.onSuccess = onSuccess;
            this.complete = complete;
        }

        @Override
        public void operationComplete(io.netty.util.concurrent.Future<Object> result)
        {
            if (0 == decrementAndGet())
                onCompletion();
        }

        void onCompletion()
        {
            complete.trySuccess(onSuccess.get());
            onSuccess = null;
        }
    }

    /**
     * Tracks completion; once all futures have completed, invokes {@code Listener#complete#trySuccess(Object)} with {@link Listener#onSuccess}.
     * If any future fails, immediately propagates this failure and releases associated resources.
     */
    private static class FailFastListener<T> extends Listener<T>
    {
        FailFastListener(int count, Supplier<T> onSuccess, FutureCombiner<T> complete)
        {
            super(count, onSuccess, complete);
        }

        @Override
        public void operationComplete(io.netty.util.concurrent.Future<Object> result)
        {
            if (!result.isSuccess())
            {
                onSuccess = null;
                complete.tryFailure(result.cause());
            }
            else
            {
                super.operationComplete(result);
            }
        }
    }

    /**
     * Tracks completion; once all futures have completed, invokes {@code Listener#complete#trySuccess(Object)} with {@link Listener#onSuccess}.
     * If any future fails we propagate this failure, but only once all have completed.
     */
    private static class FailSlowListener<T> extends Listener<T>
    {
        private static final AtomicReferenceFieldUpdater<FailSlowListener, Throwable> firstCauseUpdater =
        AtomicReferenceFieldUpdater.newUpdater(FailSlowListener.class, Throwable.class, "firstCause");

        private volatile Throwable firstCause;

        FailSlowListener(int count, Supplier<T> onSuccess, FutureCombiner<T> complete)
        {
            super(count, onSuccess, complete);
        }

        @Override
        void onCompletion()
        {
            if (onSuccess == null)
                complete.tryFailure(firstCause);
            else
                super.onCompletion();
        }

        @Override
        public void operationComplete(io.netty.util.concurrent.Future<Object> result)
        {
            if (!result.isSuccess())
            {
                onSuccess = null;
                firstCauseUpdater.compareAndSet(FailSlowListener.this, null, result.cause());
            }

            super.operationComplete(result);
        }
    }

    private volatile Collection<? extends io.netty.util.concurrent.Future<?>> propagateCancellation;

    private FutureCombiner(Collection<? extends io.netty.util.concurrent.Future<?>> combine, Supplier<T> resultSupplier, ListenerFactory<T> listenerFactory)
    {
        if (combine.isEmpty())
        {
            trySuccess(null);
        }
        else
        {
            Listener<T> listener = listenerFactory.create(combine.size(), resultSupplier, this);
            combine.forEach(f -> {
                if (f.isDone()) listener.operationComplete((io.netty.util.concurrent.Future<Object>) f);
                else f.addListener(listener);
            });
        }
    }

    @Override
    protected boolean setUncancellable()
    {
        if (!super.setUncancellable())
            return false;
        propagateCancellation = null;
        return true;
    }

    @Override
    protected boolean setUncancellableExclusive()
    {
        if (!super.setUncancellableExclusive())
            return false;
        propagateCancellation = null;
        return true;
    }

    @Override
    protected boolean trySuccess(T t)
    {
        if (!super.trySuccess(t))
            return false;
        propagateCancellation = null;
        return true;
    }

    @Override
    protected boolean tryFailure(Throwable throwable)
    {
        if (!super.tryFailure(throwable))
            return false;
        propagateCancellation = null;
        return true;
    }

    @Override
    public boolean cancel(boolean b)
    {
        if (!super.cancel(b))
            return false;
        Collection<? extends io.netty.util.concurrent.Future<?>> propagate = propagateCancellation;
        propagateCancellation = null;
        if (propagate != null)
            propagate.forEach(f -> f.cancel(b));
        return true;
    }

    /**
     * Waits for all of {@code futures} to complete, only propagating failures on completion
     */
    public static FutureCombiner<Void> nettySuccessListener(Collection<? extends io.netty.util.concurrent.Future<?>> futures)
    {
        return new FutureCombiner<Void>(futures, () -> null, FailSlowListener::new)
        {
            @Override
            public Executor notifyExecutor()
            {
                return GlobalEventExecutor.INSTANCE;
            }
        };
    }

    /**
     * Waits only until the first failure, or until all have succeeded.
     * Returns a list of results if successful; an exception if any failed.
     *
     * @param futures futures to wait for completion of
     * @return a Future containing all results of {@code futures}
     */
    public static <V> Future<List<V>> allOf(Collection<? extends io.netty.util.concurrent.Future<? extends V>> futures)
    {
        if (futures.isEmpty())
            return ImmediateFuture.success(Collections.emptyList());

        return new FutureCombiner<>(futures, () -> futures.stream().map(f -> f.getNow()).collect(Collectors.toList()), FailFastListener::new);
    }

    /**
     * Waits for all futures to complete, returning a list containing values of all successful input futures. This
     * emulates Guava's Futures::successfulAsList in that results will be in the same order as inputs and any
     * non-success value (e.g. failure or cancellation) will be replaced by null.
     * @param futures futures to wait for completion of
     * @return a Future containing all successful results of {@code futures} and nulls for non-successful futures
     */
    public static <V> Future<List<V>> successfulOf(List<? extends io.netty.util.concurrent.Future<V>> futures)
    {
        if (futures.isEmpty())
            return ImmediateFuture.success(Collections.emptyList());

        return new FutureCombiner<>(futures,
                                    () -> futures.stream()
                                                 .map(f -> f.isSuccess() ? f.getNow() : null)
                                                 .collect(Collectors.toList()),
                                    Listener::new);
    }
}
