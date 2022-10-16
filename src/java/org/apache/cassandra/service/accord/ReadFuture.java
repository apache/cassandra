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

package org.apache.cassandra.service.accord;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.util.concurrent.FutureCallback;

import accord.api.Data;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

public class ReadFuture implements Future<Data>
{
    private final Future<Data> wrappped;

    public ReadFuture(Future<Data> wrappped)
    {
        this.wrappped = wrappped;
    }

    @Override
    public Future<Data> await() throws InterruptedException
    {
        return wrappped.await();
    }

    @Override
    public Future<Data> awaitUninterruptibly()
    {
        return wrappped.awaitUninterruptibly();
    }

    @Override
    public Future<Data> awaitThrowUncheckedOnInterrupt()
    {
        return wrappped.awaitThrowUncheckedOnInterrupt();
    }

    @Override
    public void rethrowIfFailed()
    {
        wrappped.rethrowIfFailed();
    }

    @Override
    public Future<Data> sync() throws InterruptedException
    {
        return wrappped.sync();
    }

    @Override
    public Future<Data> syncUninterruptibly()
    {
        return wrappped.syncUninterruptibly();
    }

    @Override
    public Future<Data> syncThrowUncheckedOnInterrupt()
    {
        return wrappped.syncThrowUncheckedOnInterrupt();
    }

    @Override
    @Deprecated
    public boolean await(long l) throws InterruptedException
    {
        return wrappped.await(l);
    }

    @Override
    @Deprecated
    public boolean awaitUninterruptibly(long l)
    {
        return wrappped.awaitUninterruptibly(l);
    }

    @Override
    public Future<Data> addCallback(BiConsumer<? super Data, Throwable> callback)
    {
        return wrappped.addCallback(callback);
    }

    @Override
    public Future<Data> addCallback(BiConsumer<? super Data, Throwable> callback, Executor executor)
    {
        return wrappped.addCallback(callback, executor);
    }

    @Override
    public Future<Data> addCallback(FutureCallback<? super Data> callback)
    {
        return wrappped.addCallback(callback);
    }

    @Override
    public Future<Data> addCallback(FutureCallback<? super Data> callback, Executor executor)
    {
        return wrappped.addCallback(callback, executor);
    }

    @Override
    public Future<Data> addCallback(Consumer<? super Data> onSuccess, Consumer<? super Throwable> onFailure)
    {
        return wrappped.addCallback(onSuccess, onFailure);
    }

    @Override
    public Future<Data> addCallback(Consumer<? super Data> onSuccess, Consumer<? super Throwable> onFailure, Executor executor)
    {
        return wrappped.addCallback(onSuccess, onFailure, executor);
    }

    @Override
    public <T> Future<T> map(Function<? super Data, ? extends T> mapper)
    {
        return wrappped.map(mapper);
    }

    @Override
    public <T> Future<T> map(Function<? super Data, ? extends T> mapper, Executor executor)
    {
        return wrappped.map(mapper, executor);
    }

    @Override
    public <T> Future<T> flatMap(Function<? super Data, ? extends Future<T>> flatMapper)
    {
        return wrappped.flatMap(flatMapper);
    }

    @Override
    public <T> Future<T> flatMap(Function<? super Data, ? extends Future<T>> flatMapper, Executor executor)
    {
        return wrappped.flatMap(flatMapper, executor);
    }

    @Override
    public void addListener(Runnable runnable, Executor executor)
    {
        wrappped.addListener(runnable, executor);
    }

    @Override
    public void addListener(Runnable runnable)
    {
        wrappped.addListener(runnable);
    }

    @Override
    public Executor notifyExecutor()
    {
        return wrappped.notifyExecutor();
    }

    @Override
    public Future<Data> addListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super Data>> genericFutureListener)
    {
        return wrappped.addListener(genericFutureListener);
    }

    @Override
    public Future<Data> addListeners(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super Data>>... genericFutureListeners)
    {
        return wrappped.addListeners(genericFutureListeners);
    }

    @Override
    public Future<Data> removeListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super Data>> genericFutureListener)
    {
        return wrappped.removeListener(genericFutureListener);
    }

    @Override
    public Future<Data> removeListeners(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super Data>>... genericFutureListeners)
    {
        return wrappped.removeListeners(genericFutureListeners);
    }

    @Override
    public boolean isSuccess()
    {
        return wrappped.isSuccess();
    }

    @Override
    public boolean isCancellable()
    {
        return wrappped.isCancellable();
    }

    @Override
    public Throwable cause()
    {
        return wrappped.cause();
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException
    {
        return wrappped.await(timeout, unit);
    }

    @Override
    public boolean awaitUninterruptibly(long timeout, TimeUnit unit)
    {
        return wrappped.awaitUninterruptibly(timeout, unit);
    }

    @Override
    public Data getNow()
    {
        return wrappped.getNow();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        return wrappped.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled()
    {
        return wrappped.isCancelled();
    }

    @Override
    public boolean isDone()
    {
        return wrappped.isDone();
    }

    @Override
    public Data get() throws InterruptedException, ExecutionException
    {
        return wrappped.get();
    }

    @Override
    public Data get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        return wrappped.get(timeout, unit);
    }

    @Override
    public boolean awaitUntil(long nanoTimeDeadline) throws InterruptedException
    {
        return wrappped.awaitUntil(nanoTimeDeadline);
    }

    @Override
    public boolean awaitUntilThrowUncheckedOnInterrupt(long nanoTimeDeadline) throws UncheckedInterruptedException
    {
        return wrappped.awaitUntilThrowUncheckedOnInterrupt(nanoTimeDeadline);
    }

    @Override
    public boolean awaitUntilUninterruptibly(long nanoTimeDeadline)
    {
        return wrappped.awaitUntilUninterruptibly(nanoTimeDeadline);
    }

    @Override
    public boolean awaitThrowUncheckedOnInterrupt(long time, TimeUnit units) throws UncheckedInterruptedException
    {
        return wrappped.awaitThrowUncheckedOnInterrupt(time, units);
    }
}
