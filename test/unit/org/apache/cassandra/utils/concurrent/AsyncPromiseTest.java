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

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.utils.WithResources;

public class AsyncPromiseTest extends AbstractTestAsyncPromise
{
    @After
    public void shutdown()
    {
        exec.shutdownNow();
    }

    private static <V> List<Supplier<Promise<V>>> suppliers(AtomicInteger listeners, boolean includeUncancellable)
    {
        List<Supplier<Promise<V>>> cancellable = ImmutableList.of(
            () -> new AsyncPromise<>(),
            () -> new AsyncPromise<>(f -> listeners.incrementAndGet()),
            () -> AsyncPromise.withExecutor(TestInExecutor.INSTANCE));
        List<Supplier<Promise<V>>> uncancellable = ImmutableList.of(
            () -> AsyncPromise.uncancellable(),
            () -> AsyncPromise.uncancellable((GenericFutureListener<? extends Future<? super V>>) f -> listeners.incrementAndGet()),
            () -> AsyncPromise.uncancellable(MoreExecutors.directExecutor()),
            () -> AsyncPromise.uncancellable(TestInExecutor.INSTANCE)
        );

        if (!includeUncancellable)
            return cancellable;

        ImmutableList.Builder<Supplier<Promise<V>>> builder = ImmutableList.builder();
        builder.addAll(cancellable)
               .addAll(cancellable.stream().map(s -> (Supplier<Promise<V>>) () -> cancelSuccess(s.get())).collect(Collectors.toList()))
               .addAll(cancellable.stream().map(s -> (Supplier<Promise<V>>) () -> cancelExclusiveSuccess(s.get())).collect(Collectors.toList()))
               .addAll(uncancellable)
               .addAll(uncancellable.stream().map(s -> (Supplier<Promise<V>>) () -> cancelFailure(s.get())).collect(Collectors.toList()));
        return builder.build();
    }

    @Test
    public void testSuccess()
    {
        final AtomicInteger initialListeners = new AtomicInteger();
        List<Supplier<Promise<Integer>>> suppliers = suppliers(initialListeners, true);
        for (boolean tryOrSet : new boolean[]{ false, true })
            for (Integer v : new Integer[]{ null, 1 })
                for (Supplier<Promise<Integer>> supplier : suppliers)
                    testOneSuccess(supplier.get(), tryOrSet, v, 2);
        Assert.assertEquals(5 * 2 * 2, initialListeners.get());
    }

    @Test
    public void testFailure()
    {
        final AtomicInteger initialListeners = new AtomicInteger();
        List<Supplier<Promise<Integer>>> suppliers = suppliers(initialListeners, true);
        for (boolean tryOrSet : new boolean[] { false, true })
            for (Throwable v : new Throwable[] { null, new NullPointerException() })
                for (Supplier<Promise<Integer>> supplier : suppliers)
                    testOneFailure(supplier.get(), tryOrSet, v, 2);
        Assert.assertEquals(5 * 2 * 2, initialListeners.get());
    }


    @Test
    public void testCancellation()
    {
        final AtomicInteger initialListeners = new AtomicInteger();
        List<Supplier<Promise<Integer>>> suppliers = suppliers(initialListeners, false);
        for (boolean interruptIfRunning : new boolean[] { true, false })
            for (Supplier<Promise<Integer>> supplier : suppliers)
                testOneCancellation(supplier.get(), interruptIfRunning, 2);
        Assert.assertEquals(2, initialListeners.get());
    }


    @Test
    public void testTimeout()
    {
        final AtomicInteger initialListeners = new AtomicInteger();
        List<Supplier<Promise<Integer>>> suppliers = suppliers(initialListeners, true);
        for (Supplier<Promise<Integer>> supplier : suppliers)
            testOneTimeout(supplier.get());
        Assert.assertEquals(0, initialListeners.get());
    }

    private static final class TestInExecutor implements ExecutorPlus
    {
        static final TestInExecutor INSTANCE = new TestInExecutor();
        @Override
        public void shutdown()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isShutdown()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isTerminated()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> org.apache.cassandra.utils.concurrent.Future<T> submit(Callable<T> task)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> org.apache.cassandra.utils.concurrent.Future<T> submit(Runnable task, T result)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.apache.cassandra.utils.concurrent.Future<?> submit(Runnable task)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(WithResources withResources, Runnable task)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> org.apache.cassandra.utils.concurrent.Future<T> submit(WithResources withResources, Callable<T> task)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.apache.cassandra.utils.concurrent.Future<?> submit(WithResources withResources, Runnable task)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> org.apache.cassandra.utils.concurrent.Future<T> submit(WithResources withResources, Runnable task, T result)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean inExecutor()
        {
            return true;
        }

        @Override
        public void execute(Runnable command)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getCorePoolSize()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setCorePoolSize(int newCorePoolSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getMaximumPoolSize()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMaximumPoolSize(int newMaximumPoolSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getActiveTaskCount()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getCompletedTaskCount()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getPendingTaskCount()
        {
            throw new UnsupportedOperationException();
        }
    }

}

