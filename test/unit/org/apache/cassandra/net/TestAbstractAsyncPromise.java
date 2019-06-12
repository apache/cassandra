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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

abstract class TestAbstractAsyncPromise extends TestAbstractPromise
{
    <V> void testOneSuccess(Promise<V> promise, boolean setUncancellable, boolean tryOrSet, V value, V otherValue)
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
            public GenericFutureListener<Future<V>> getRecursive()
            {
                int id = count++;
                return p -> { promise.addListener(get()); results.add(p.getNow()); order.add(id); };
            }
        }
        ListenerFactory listeners = new ListenerFactory();
        Async async = new Async();
        promise.addListener(listeners.get());
        promise.addListeners(listeners.getRecursive(), listeners.get());
        promise.addListener(listeners.getRecursive());
        success(promise, Promise::getNow, null);
        success(promise, Promise::isSuccess, false);
        success(promise, Promise::isDone, false);
        success(promise, Promise::isCancelled, false);
        success(promise, Promise::isCancellable, true);
        if (setUncancellable)
        {
            success(promise, Promise::setUncancellable, true);
            success(promise, Promise::setUncancellable, true);
            success(promise, p -> p.cancel(true), false);
            success(promise, p -> p.cancel(false), false);
        }
        success(promise, Promise::isCancellable, !setUncancellable);
        async.success(promise, Promise::get, value);
        async.success(promise, p -> p.get(1L, TimeUnit.SECONDS), value);
        async.success(promise, Promise::await, promise);
        async.success(promise, Promise::awaitUninterruptibly, promise);
        async.success(promise, p -> p.await(1L, TimeUnit.SECONDS), true);
        async.success(promise, p -> p.await(1000L), true);
        async.success(promise, p -> p.awaitUninterruptibly(1L, TimeUnit.SECONDS), true);
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
        promise.addListeners(listeners.getRecursive(), listeners.get());
        promise.addListener(listeners.getRecursive());
        success(promise, Promise::isSuccess, true);
        success(promise, Promise::isDone, true);
        success(promise, Promise::isCancelled, false);
        success(promise, Promise::isCancellable, false);
        async.verify();
        Assert.assertEquals(listeners.count, results.size());
        Assert.assertEquals(listeners.count, order.size());
        for (V result : results)
            Assert.assertEquals(value, result);
        for (int i = 0 ; i < order.size() ; ++i)
            Assert.assertEquals(i, order.get(i).intValue());
    }

    <V> void testOneFailure(Promise<V> promise, boolean setUncancellable, boolean tryOrSet, Throwable cause, V otherValue)
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
        }
        ListenerFactory listeners = new ListenerFactory();
        promise.addListener(listeners.get());
        promise.addListeners(listeners.getRecursive(), listeners.get());
        promise.addListener(listeners.getRecursive());
        success(promise, Promise::isSuccess, false);
        success(promise, Promise::isDone, false);
        success(promise, Promise::isCancelled, false);
        success(promise, Promise::isCancellable, true);
        if (setUncancellable)
        {
            success(promise, Promise::setUncancellable, true);
            success(promise, Promise::setUncancellable, true);
            success(promise, p -> p.cancel(true), false);
            success(promise, p -> p.cancel(false), false);
        }
        success(promise, Promise::isCancellable, !setUncancellable);
        success(promise, Promise::getNow, null);
        success(promise, Promise::cause, null);
        async.failure(promise, Promise::get, ExecutionException.class);
        async.failure(promise, p -> p.get(1L, TimeUnit.SECONDS), ExecutionException.class);
        async.success(promise, Promise::await, promise);
        async.success(promise, Promise::awaitUninterruptibly, promise);
        async.success(promise, p -> p.await(1L, TimeUnit.SECONDS), true);
        async.success(promise, p -> p.await(1000L), true);
        async.success(promise, p -> p.awaitUninterruptibly(1L, TimeUnit.SECONDS), true);
        async.success(promise, p -> p.awaitUninterruptibly(1000L), true);
        async.failure(promise, Promise::sync, cause);
        async.failure(promise, Promise::syncUninterruptibly, cause);
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

    public <V> void testOneCancellation(Promise<V> promise, boolean interruptIfRunning, V otherValue)
    {
        Async async = new Async();
        success(promise, Promise::isCancellable, true);
        success(promise, Promise::getNow, null);
        success(promise, Promise::cause, null);
        async.failure(promise, Promise::get, CancellationException.class);
        async.failure(promise, p -> p.get(1L, TimeUnit.SECONDS), CancellationException.class);
        async.success(promise, Promise::await, promise);
        async.success(promise, Promise::awaitUninterruptibly, promise);
        async.success(promise, p -> p.await(1L, TimeUnit.SECONDS), true);
        async.success(promise, p -> p.await(1000L), true);
        async.success(promise, p -> p.awaitUninterruptibly(1L, TimeUnit.SECONDS), true);
        async.success(promise, p -> p.awaitUninterruptibly(1000L), true);
        async.failure(promise, Promise::sync, CancellationException.class);
        async.failure(promise, Promise::syncUninterruptibly, CancellationException.class);
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


    public <V> void testOneTimeout(Promise<V> promise, boolean setUncancellable)
    {
        Async async = new Async();
        if (setUncancellable)
            success(promise, Promise::setUncancellable, true);
        success(promise, Promise::isCancellable, !setUncancellable);
        async.failure(promise, p -> p.get(1L, TimeUnit.MILLISECONDS), TimeoutException.class);
        async.success(promise, p -> p.await(1L, TimeUnit.MILLISECONDS), false);
        async.success(promise, p -> p.await(1L), false);
        async.success(promise, p -> p.awaitUninterruptibly(1L, TimeUnit.MILLISECONDS), false);
        async.success(promise, p -> p.awaitUninterruptibly(1L), false);
        Uninterruptibles.sleepUninterruptibly(10L, TimeUnit.MILLISECONDS);
        async.verify();
    }

}
