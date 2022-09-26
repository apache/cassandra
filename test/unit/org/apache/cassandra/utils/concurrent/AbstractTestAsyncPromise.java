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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;

import io.netty.util.concurrent.GenericFutureListener;
import org.apache.cassandra.config.DatabaseDescriptor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public abstract class AbstractTestAsyncPromise extends AbstractTestPromise
{
    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    public static <V> Promise<V> cancelSuccess(Promise<V> promise)
    {
        success(promise, Promise::isCancellable, true);
        return cancelShared(promise);
    }

    public static <V> Promise<V> cancelExclusiveSuccess(Promise<V> promise)
    {
        success(promise, Promise::isCancellable, true);
        success(promise, Promise::setUncancellableExclusive, true);
        return cancelShared(promise);
    }

    private static <V> Promise<V> cancelShared(Promise<V> promise)
    {
        success(promise, Promise::setUncancellable, true);
        success(promise, Promise::setUncancellable, true);
        success(promise, Promise::setUncancellableExclusive, false);
        success(promise, p -> p.cancel(true), false);
        success(promise, p -> p.cancel(false), false);
        success(promise, Promise::isCancellable, false);
        success(promise, Promise::isCancelled, false);
        return promise;
    }

    public static <V> Promise<V> cancelFailure(Promise<V> promise)
    {
        success(promise, Promise::isCancellable, false);
        return cancelShared(promise);
    }

    protected <V> void testOneSuccess(Promise<V> promise, boolean tryOrSet, V value, V otherValue)
    {
        List<V> results = new ArrayList<>();
        List<Integer> order = new ArrayList<>();
        class ListenerFactory
        {
            int count = 0;

            public GenericFutureListener<Future<V>> get()
            {
                int id = count++;
                return p -> { results.add(p.getNow()); order.add(id); };
            }
            public GenericFutureListener<Future<V>> getListenerToFailure(Promise<V> promise)
            {
                int id = count++;
                return p -> { Assert.assertTrue(p.cause() instanceof RuntimeException); results.add(promise.getNow()); order.add(id); };
            }
            public GenericFutureListener<Future<V>> getRecursive()
            {
                int id = count++;
                return p -> { promise.addListener(get()); results.add(p.getNow()); order.add(id); };
            }
            public Runnable getRunnable(Future<V> p)
            {
                int id = count++;
                return () -> { results.add(p.getNow()); order.add(id); };
            }
            public Runnable getRecursiveRunnable(Future<V> p)
            {
                int id = count++;
                return () -> { promise.addListener(getRunnable(p)); results.add(p.getNow()); order.add(id); };
            }
            public Consumer<V> getConsumer()
            {
                int id = count++;
                return result -> { results.add(result); order.add(id); };
            }
            public Consumer<V> getRecursiveConsumer()
            {
                int id = count++;
                return result -> { promise.addCallback(getConsumer(), fail -> Assert.fail()); results.add(result); order.add(id); };
            }
            public Function<V, Future<V>> getAsyncFunction()
            {
                int id = count++;
                return result -> { results.add(result); order.add(id); return ImmediateFuture.success(result); };
            }
            public Function<V, V> getFunction()
            {
                int id = count++;
                return result -> { results.add(result); order.add(id); return result; };
            }
            public Function<V, Future<V>> getRecursiveAsyncFunction(Promise<V> promise)
            {
                int id = count++;
                return result -> { promise.flatMap(getAsyncFunction()); results.add(result); order.add(id); return ImmediateFuture.success(result); };
            }
            public Function<V, Future<V>> getAsyncFailingFunction()
            {
                int id = count++;
                return result -> { results.add(result); order.add(id); return ImmediateFuture.failure(new RuntimeException()); };
            }
            public Function<V, V> getFailingFunction()
            {
                int id = count++;
                return result -> { results.add(result); order.add(id); throw new RuntimeException(); };
            }
            public Function<V, Future<V>> getRecursiveAsyncFailingFunction(Promise<V> promise)
            {
                int id = count++;
                return result -> { promise.flatMap(getAsyncFailingFunction()); results.add(result); order.add(id); return ImmediateFuture.failure(new RuntimeException()); };
            }
            public FutureCallback<V> getCallback(Future<V> p)
            {
                int id = count++;
                return new FutureCallback<V>()
                {
                    @Override
                    public void onSuccess(@Nullable Object o)
                    {
                        results.add(p.getNow());
                        order.add(id);
                    }

                    @Override
                    public void onFailure(Throwable throwable)
                    {
                        Assert.fail();
                    }
                };
            }
            public FutureCallback<V> getRecursiveCallback(Future<V> p)
            {
                int id = count++;
                return new FutureCallback<V>()
                {
                    @Override
                    public void onSuccess(@Nullable Object o)
                    {
                        promise.addCallback(getCallback(p));
                        results.add(p.getNow());
                        order.add(id);
                    }

                    @Override
                    public void onFailure(Throwable throwable)
                    {
                        Assert.fail();
                    }
                };
            }
        }
        ListenerFactory listeners = new ListenerFactory();
        Async async = new Async();
        promise.addListener(listeners.get());
        promise.addListener(listeners.getRunnable(promise));
        promise.addListener(listeners.getRunnable(promise), MoreExecutors.directExecutor());
        promise.addListeners(listeners.getRecursive(), listeners.get());
        promise.addListener(listeners.getRecursiveRunnable(promise), MoreExecutors.directExecutor());
        promise.addListener(listeners.getRecursive());
        promise.addCallback(listeners.getCallback(promise));
        promise.addCallback(listeners.getCallback(promise), MoreExecutors.directExecutor());
        promise.addCallback(listeners.getRecursiveCallback(promise));
        promise.addCallback(listeners.getRecursiveCallback(promise), MoreExecutors.directExecutor());
        promise.addCallback(listeners.getConsumer(), fail -> Assert.fail());
        promise.addCallback(listeners.getConsumer(), fail -> Assert.fail(), MoreExecutors.directExecutor());
        promise.addCallback(listeners.getRecursiveConsumer(), fail -> Assert.fail());
        promise.addCallback(listeners.getRecursiveConsumer(), fail -> Assert.fail(), MoreExecutors.directExecutor());
        promise.map(listeners.getFunction()).addListener(listeners.get());
        promise.map(listeners.getFunction(), MoreExecutors.directExecutor()).addListener(listeners.get());
        promise.map(listeners.getFailingFunction()).addListener(listeners.getListenerToFailure(promise));
        promise.map(listeners.getFailingFunction(), MoreExecutors.directExecutor()).addListener(listeners.getListenerToFailure(promise));
        promise.flatMap(listeners.getAsyncFunction()).addListener(listeners.get());
        promise.flatMap(listeners.getAsyncFunction(), MoreExecutors.directExecutor()).addListener(listeners.get());
        promise.flatMap(listeners.getRecursiveAsyncFunction(promise)).addListener(listeners.get());
        promise.flatMap(listeners.getRecursiveAsyncFunction(promise), MoreExecutors.directExecutor()).addListener(listeners.get());
        promise.flatMap(listeners.getAsyncFailingFunction()).addListener(listeners.getListenerToFailure(promise));
        promise.flatMap(listeners.getAsyncFailingFunction(), MoreExecutors.directExecutor()).addListener(listeners.getListenerToFailure(promise));
        promise.flatMap(listeners.getRecursiveAsyncFailingFunction(promise)).addListener(listeners.getListenerToFailure(promise));
        promise.flatMap(listeners.getRecursiveAsyncFailingFunction(promise), MoreExecutors.directExecutor()).addListener(listeners.getListenerToFailure(promise));

        success(promise, Promise::getNow, null);
        success(promise, Promise::isSuccess, false);
        success(promise, Promise::isDone, false);
        success(promise, Promise::isCancelled, false);
        async.success(promise, Promise::get, value);
        async.success(promise, p -> p.get(1L, SECONDS), value);
        async.success(promise, Promise::await, promise);
        async.success(promise, Promise::awaitUninterruptibly, promise);
        async.success(promise, p -> p.await(1L, SECONDS), true);
        async.success(promise, p -> p.await(1000L), true);
        async.success(promise, p -> p.awaitUninterruptibly(1L, SECONDS), true);
        async.success(promise, p -> p.awaitUninterruptibly(1000L), true);
        async.success(promise, Promise::sync, promise);
        async.success(promise, Promise::syncUninterruptibly, promise);
        if (tryOrSet) promise.trySuccess(value);
        else promise.setSuccess(value);
        success(promise, p -> p.cancel(true), false);
        success(promise, p -> p.cancel(false), false);
        failure(promise, p -> p.setSuccess(null), IllegalStateException.class);
        failure(promise, p -> p.setFailure(new NullPointerException()), IllegalStateException.class);
        success(promise, Promise::getNow, value);
        success(promise, p -> p.trySuccess(otherValue), false);
        success(promise, p -> p.tryFailure(new NullPointerException()), false);
        success(promise, Promise::getNow, value);
        success(promise, Promise::cause, null);
        promise.addListener(listeners.get());
        promise.addListener(listeners.getRunnable(promise));
        promise.addListener(listeners.getRunnable(promise), MoreExecutors.directExecutor());
        promise.addListeners(listeners.getRecursive(), listeners.get());
        promise.addListener(listeners.getRecursiveRunnable(promise), MoreExecutors.directExecutor());
        promise.addListener(listeners.getRecursive());
        promise.addCallback(listeners.getCallback(promise));
        promise.addCallback(listeners.getCallback(promise), MoreExecutors.directExecutor());
        promise.addCallback(listeners.getRecursiveCallback(promise));
        promise.addCallback(listeners.getRecursiveCallback(promise), MoreExecutors.directExecutor());
        promise.addCallback(listeners.getConsumer(), fail -> Assert.fail());
        promise.addCallback(listeners.getConsumer(), fail -> Assert.fail(), MoreExecutors.directExecutor());
        promise.addCallback(listeners.getRecursiveConsumer(), fail -> Assert.fail());
        promise.addCallback(listeners.getRecursiveConsumer(), fail -> Assert.fail(), MoreExecutors.directExecutor());
        promise.map(listeners.getFunction()).addListener(listeners.get());
        promise.map(listeners.getFunction(), MoreExecutors.directExecutor()).addListener(listeners.get());
        promise.map(listeners.getFailingFunction()).addListener(listeners.getListenerToFailure(promise));
        promise.map(listeners.getFailingFunction(), MoreExecutors.directExecutor()).addListener(listeners.getListenerToFailure(promise));
        promise.flatMap(listeners.getAsyncFunction()).addListener(listeners.get());
        promise.flatMap(listeners.getAsyncFunction(), MoreExecutors.directExecutor()).addListener(listeners.get());
        promise.flatMap(listeners.getRecursiveAsyncFunction(promise)).addListener(listeners.get());
        promise.flatMap(listeners.getRecursiveAsyncFunction(promise), MoreExecutors.directExecutor()).addListener(listeners.get());
        promise.flatMap(listeners.getAsyncFailingFunction()).addListener(listeners.getListenerToFailure(promise));
        promise.flatMap(listeners.getAsyncFailingFunction(), MoreExecutors.directExecutor()).addListener(listeners.getListenerToFailure(promise));
        promise.flatMap(listeners.getRecursiveAsyncFailingFunction(promise)).addListener(listeners.getListenerToFailure(promise));
        promise.flatMap(listeners.getRecursiveAsyncFailingFunction(promise), MoreExecutors.directExecutor()).addListener(listeners.getListenerToFailure(promise));
        success(promise, Promise::isSuccess, true);
        success(promise, Promise::isDone, true);
        success(promise, Promise::isCancelled, false);
        async.verify();
        Assert.assertEquals(listeners.count, results.size());
        Assert.assertEquals(listeners.count, order.size());
        for (V result : results)
            Assert.assertEquals(value, result);
        for (int i = 0 ; i < order.size() ; ++i)
            Assert.assertEquals(i, order.get(i).intValue());
    }

    protected <V> void testOneFailure(Promise<V> promise, boolean tryOrSet, Throwable cause, V otherValue)
    {
        List<Throwable> results = new ArrayList<>();
        List<Integer> order = new ArrayList<>();
        Async async = new Async();
        class ListenerFactory
        {
            int count = 0;

            public GenericFutureListener<Future<V>> get()
            {
                int id = count++;
                return p -> { results.add(p.cause()); order.add(id); };
            }
            public GenericFutureListener<Future<V>> getRecursive()
            {
                int id = count++;
                return p -> { promise.addListener(get()); results.add(p.cause()); order.add(id); };
            }
            public Runnable getRunnable(Future<V> p)
            {
                int id = count++;
                return () -> { results.add(p.cause()); order.add(id); };
            }
            public Runnable getRecursiveRunnable(Future<V> p)
            {
                int id = count++;
                return () -> { promise.addListener(getRunnable(p)); results.add(p.cause()); order.add(id); };
            }
            public Consumer<Throwable> getConsumer()
            {
                int id = count++;
                return result -> { results.add(result); order.add(id); };
            }
            public Consumer<Throwable> getRecursiveConsumer()
            {
                int id = count++;
                return result -> { promise.addCallback(fail -> Assert.fail(), getConsumer()); results.add(result); order.add(id); };
            }
            public Function<V, Future<V>> getAsyncFunction()
            {
                return result -> { Assert.fail(); return ImmediateFuture.success(result); };
            }
            public Function<V, Future<V>> getRecursiveAsyncFunction()
            {
                return result -> { promise.flatMap(getAsyncFunction()); return ImmediateFuture.success(result); };
            }
            public FutureCallback<V> getCallback(Future<V> p)
            {
                int id = count++;
                return new FutureCallback<V>()
                {
                    @Override
                    public void onSuccess(@Nullable Object o)
                    {
                        Assert.fail();
                    }

                    @Override
                    public void onFailure(Throwable throwable)
                    {
                        results.add(p.cause());
                        order.add(id);
                    }
                };
            }
            public FutureCallback<V> getRecursiveCallback(Future<V> p)
            {
                int id = count++;
                return new FutureCallback<V>()
                {
                    @Override
                    public void onSuccess(@Nullable Object o)
                    {
                        Assert.fail();
                    }

                    @Override
                    public void onFailure(Throwable throwable)
                    {
                        promise.addCallback(getCallback(p));
                        results.add(p.cause());
                        order.add(id);
                    }
                };
            }
        }
        ListenerFactory listeners = new ListenerFactory();
        promise.addListener(listeners.get());
        promise.addListeners(listeners.getRecursive(), listeners.get());
        promise.addListener(listeners.getRecursive());
        promise.addListener(listeners.getRunnable(promise));
        promise.addListener(listeners.getRunnable(promise), MoreExecutors.directExecutor());
        promise.addListener(listeners.getRecursiveRunnable(promise), MoreExecutors.directExecutor());
        promise.addListener(listeners.getRecursive());
        promise.addCallback(listeners.getCallback(promise));
        promise.addCallback(listeners.getRecursiveCallback(promise));
        promise.addCallback(fail -> Assert.fail(), listeners.getConsumer());
        promise.addCallback(fail -> Assert.fail(), listeners.getRecursiveConsumer());
        promise.flatMap(listeners.getAsyncFunction()).addListener(listeners.get());
        promise.flatMap(listeners.getAsyncFunction(), MoreExecutors.directExecutor()).addListener(listeners.get());
        promise.flatMap(listeners.getRecursiveAsyncFunction()).addListener(listeners.get());
        promise.flatMap(listeners.getRecursiveAsyncFunction(), MoreExecutors.directExecutor()).addListener(listeners.get());
        success(promise, Promise::isSuccess, false);
        success(promise, Promise::isDone, false);
        success(promise, Promise::isCancelled, false);
        success(promise, Promise::getNow, null);
        success(promise, Promise::cause, null);
        async.failure(promise, p -> p.get(), ExecutionException.class);
        async.failure(promise, p -> p.get(1L, SECONDS), ExecutionException.class);
        async.success(promise, Promise::await, promise);
        async.success(promise, Promise::awaitThrowUncheckedOnInterrupt, promise);
        async.success(promise, Promise::awaitUninterruptibly, promise);
        async.success(promise, p -> p.await(1L, SECONDS), true);
        async.success(promise, p -> p.await(1000L), true);
        async.success(promise, p -> p.awaitUninterruptibly(1L, SECONDS), true);
        async.success(promise, p -> p.awaitThrowUncheckedOnInterrupt(1L, SECONDS), true);
        async.success(promise, p -> p.awaitUninterruptibly(1000L), true);
        async.success(promise, p -> p.awaitUntil(nanoTime() + SECONDS.toNanos(1L)), true);
        async.success(promise, p -> p.awaitUntilUninterruptibly(nanoTime() + SECONDS.toNanos(1L)), true);
        async.success(promise, p -> p.awaitUntilThrowUncheckedOnInterrupt(nanoTime() + SECONDS.toNanos(1L)), true);
        async.failure(promise, p -> p.sync(), cause);
        async.failure(promise, p -> p.syncUninterruptibly(), cause);
        if (tryOrSet) promise.tryFailure(cause);
        else promise.setFailure(cause);
        success(promise, p -> p.cancel(true), false);
        success(promise, p -> p.cancel(false), false);
        failure(promise, p -> p.setSuccess(null), IllegalStateException.class);
        failure(promise, p -> p.setFailure(new NullPointerException()), IllegalStateException.class);
        success(promise, Promise::cause, cause);
        success(promise, Promise::getNow, null);
        success(promise, p -> p.trySuccess(otherValue), false);
        success(promise, p -> p.tryFailure(new NullPointerException()), false);
        success(promise, Promise::getNow, null);
        success(promise, Promise::cause, cause);
        promise.addListener(listeners.get());
        promise.addListeners(listeners.getRecursive(), listeners.get());
        promise.addListener(listeners.getRecursive());
        promise.addListener(listeners.getRunnable(promise));
        promise.addListener(listeners.getRunnable(promise), MoreExecutors.directExecutor());
        promise.addListener(listeners.getRecursiveRunnable(promise), MoreExecutors.directExecutor());
        promise.addListener(listeners.getRecursive());
        promise.addCallback(listeners.getCallback(promise));
        promise.addCallback(listeners.getRecursiveCallback(promise));
        promise.addCallback(fail -> Assert.fail(), listeners.getConsumer());
        promise.addCallback(fail -> Assert.fail(), listeners.getRecursiveConsumer());
        promise.flatMap(listeners.getAsyncFunction()).addListener(listeners.get());
        promise.flatMap(listeners.getAsyncFunction(), MoreExecutors.directExecutor()).addListener(listeners.get());
        success(promise, Promise::isSuccess, false);
        success(promise, Promise::isDone, true);
        success(promise, Promise::isCancelled, false);
        success(promise, Promise::isCancellable, false);
        async.verify();
        Assert.assertEquals(listeners.count, results.size());
        Assert.assertEquals(listeners.count, order.size());
        for (Throwable result : results)
            Assert.assertEquals(cause, result);
        for (int i = 0 ; i < order.size() ; ++i)
            Assert.assertEquals(i, order.get(i).intValue());
    }

    // TODO: test listeners?
    public <V> void testOneCancellation(Promise<V> promise, boolean interruptIfRunning, V otherValue)
    {
        Async async = new Async();
        success(promise, Promise::isCancellable, true);
        success(promise, Promise::getNow, null);
        success(promise, Promise::cause, null);
        async.failure(promise, p -> p.get(), CancellationException.class);
        async.failure(promise, p -> p.get(1L, SECONDS), CancellationException.class);
        async.success(promise, Promise::await, promise);
        async.success(promise, Promise::awaitThrowUncheckedOnInterrupt, promise);
        async.success(promise, Promise::awaitUninterruptibly, promise);
        async.success(promise, p -> p.await(1L, SECONDS), true);
        async.success(promise, p -> p.await(1000L), true);
        async.success(promise, p -> p.awaitUninterruptibly(1L, SECONDS), true);
        async.success(promise, p -> p.awaitThrowUncheckedOnInterrupt(1L, SECONDS), true);
        async.success(promise, p -> p.awaitUninterruptibly(1000L), true);
        async.success(promise, p -> p.awaitUntil(nanoTime() + SECONDS.toNanos(1L)), true);
        async.success(promise, p -> p.awaitUntilUninterruptibly(nanoTime() + SECONDS.toNanos(1L)), true);
        async.success(promise, p -> p.awaitUntilThrowUncheckedOnInterrupt(nanoTime() + SECONDS.toNanos(1L)), true);
        async.failure(promise, p -> p.sync(), CancellationException.class);
        async.failure(promise, p -> p.syncUninterruptibly(), CancellationException.class);
        promise.cancel(interruptIfRunning);
        failure(promise, p -> p.setFailure(null), IllegalStateException.class);
        failure(promise, p -> p.setFailure(null), IllegalStateException.class);
        Assert.assertTrue(promise.cause() instanceof CancellationException);
        success(promise, Promise::getNow, null);
        success(promise, p -> p.trySuccess(otherValue), false);
        success(promise, Promise::getNow, null);
        Assert.assertTrue(promise.cause() instanceof CancellationException);
        success(promise, Promise::isSuccess, false);
        success(promise, Promise::isDone, true);
        success(promise, Promise::isCancelled, true);
        success(promise, Promise::isCancellable, false);
        async.verify();
    }


    public <V> void testOneTimeout(Promise<V> promise)
    {
        Async async = new Async();
        async.failure(promise, p -> p.get(1L, MILLISECONDS), TimeoutException.class);
        async.success(promise, p -> p.await(1L, MILLISECONDS), false);
        async.success(promise, p -> p.awaitThrowUncheckedOnInterrupt(1L, MILLISECONDS), false);
        async.success(promise, p -> p.await(1L), false);
        async.success(promise, p -> p.awaitUninterruptibly(1L, MILLISECONDS), false);
        async.success(promise, p -> p.awaitThrowUncheckedOnInterrupt(1L, MILLISECONDS), false);
        async.success(promise, p -> p.awaitUninterruptibly(1L), false);
        async.success(promise, p -> p.awaitUntil(nanoTime() + MILLISECONDS.toNanos(1L)), false);
        async.success(promise, p -> p.awaitUntilUninterruptibly(nanoTime() + MILLISECONDS.toNanos(1L)), false);
        async.success(promise, p -> p.awaitUntilThrowUncheckedOnInterrupt(nanoTime() + MILLISECONDS.toNanos(1L)), false);
        Uninterruptibles.sleepUninterruptibly(10L, MILLISECONDS);
        async.verify();
    }

}

