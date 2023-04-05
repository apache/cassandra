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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.util.concurrent.Future; // checkstyle: permit this import
import io.netty.util.concurrent.GenericFutureListener;

/**
 * A delegating future, that we can extend to provide subtly modified behaviour.
 *
 * See {@link FutureCombiner} and {@link FutureResult}
 */
public class FutureDelegate<V> implements Future<V>
{
    final Future<V> delegate;

    FutureDelegate(Future<V> delegate)
    {
        this.delegate = delegate;
    }

    public boolean isSuccess()
    {
        return delegate.isSuccess();
    }

    public boolean isCancellable()
    {
        return delegate.isCancellable();
    }

    public Throwable cause()
    {
        return delegate.cause();
    }

    public Future<V> sync() throws InterruptedException
    {
        return delegate.sync();
    }

    public Future<V> syncUninterruptibly()
    {
        return delegate.syncUninterruptibly();
    }

    public Future<V> await() throws InterruptedException
    {
        return delegate.await();
    }

    public Future<V> awaitUninterruptibly()
    {
        return delegate.awaitUninterruptibly();
    }

    public boolean await(long l, TimeUnit timeUnit) throws InterruptedException
    {
        return delegate.await(l, timeUnit);
    }

    public boolean await(long l) throws InterruptedException
    {
        return delegate.await(l);
    }

    public boolean awaitUninterruptibly(long l, TimeUnit timeUnit)
    {
        return delegate.awaitUninterruptibly(l, timeUnit);
    }

    public boolean awaitUninterruptibly(long l)
    {
        return delegate.awaitUninterruptibly(l);
    }

    public V getNow()
    {
        return delegate.getNow();
    }

    public boolean cancel(boolean b)
    {
        return delegate.cancel(b);
    }

    public boolean isCancelled()
    {
        return delegate.isCancelled();
    }

    public boolean isDone()
    {
        return delegate.isDone();
    }

    public V get() throws InterruptedException, ExecutionException
    {
        return delegate.get();
    }

    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        return delegate.get(timeout, unit);
    }

    @Override
    public io.netty.util.concurrent.Future<V> addListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> genericFutureListener)
    {
        return delegate.addListener(genericFutureListener);
    }

    @Override
    public io.netty.util.concurrent.Future<V> addListeners(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>>... genericFutureListeners)
    {
        return delegate.addListeners(genericFutureListeners);
    }

    @Override
    public io.netty.util.concurrent.Future<V> removeListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> genericFutureListener)
    {
        return delegate.removeListener(genericFutureListener);
    }

    @Override
    public io.netty.util.concurrent.Future<V> removeListeners(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>>... genericFutureListeners)
    {
        return delegate.removeListeners(genericFutureListeners);
    }
}
