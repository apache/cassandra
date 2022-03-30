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
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;

import net.openhft.chronicle.core.util.ThrowingBiConsumer;
import net.openhft.chronicle.core.util.ThrowingConsumer;
import net.openhft.chronicle.core.util.ThrowingFunction;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public abstract class AbstractTestAwaitable<A extends Awaitable>
{
    protected final ExecutorService exec = Executors.newCachedThreadPool();

    protected void testOneSuccess(A awaitable, Consumer<A> signal)
    {
        Async async = new Async();
        //noinspection Convert2MethodRef
        async.success(awaitable, a -> a.await(), awaitable);
        async.success(awaitable, a -> a.awaitUninterruptibly(), awaitable);
        async.success(awaitable, a -> a.awaitThrowUncheckedOnInterrupt(), awaitable);
        async.success(awaitable, a -> a.await(1L, SECONDS), true);
        async.success(awaitable, a -> a.awaitUninterruptibly(1L, SECONDS), true);
        async.success(awaitable, a -> a.awaitThrowUncheckedOnInterrupt(1L, SECONDS), true);
        async.success(awaitable, a -> a.awaitUntil(Long.MAX_VALUE), true);
        async.success(awaitable, a -> a.awaitUntilUninterruptibly(Long.MAX_VALUE), true);
        async.success(awaitable, a -> a.awaitUntilThrowUncheckedOnInterrupt(Long.MAX_VALUE), true);
        signal.accept(awaitable);
        async.verify();
    }

    public void testOneTimeout(A awaitable)
    {
        Async async = new Async();
        async.success(awaitable, a -> a.await(1L, MILLISECONDS), false);
        async.success(awaitable, a -> a.awaitUninterruptibly(1L, MILLISECONDS), false);
        async.success(awaitable, a -> a.awaitThrowUncheckedOnInterrupt(1L, MILLISECONDS), false);
        async.success(awaitable, a -> a.awaitUntil(nanoTime() + MILLISECONDS.toNanos(1L)), false);
        async.success(awaitable, a -> a.awaitUntilUninterruptibly(nanoTime() + MILLISECONDS.toNanos(1L)), false);
        async.success(awaitable, a -> a.awaitUntilThrowUncheckedOnInterrupt(nanoTime() + MILLISECONDS.toNanos(1L)), false);
        Uninterruptibles.sleepUninterruptibly(10L, MILLISECONDS);
        async.verify();
    }

    public void testOneInterrupt(A awaitable)
    {
        Async async = new Async();
        async.failure(awaitable, a -> { Thread.currentThread().interrupt(); a.await(); }, InterruptedException.class);
        async.failure(awaitable, a -> { Thread.currentThread().interrupt(); a.await(1L, SECONDS); }, InterruptedException.class);
        async.success(awaitable, a -> { Thread.currentThread().interrupt(); return a.awaitUninterruptibly(1L, SECONDS); }, false);
        async.failure(awaitable, a -> { Thread.currentThread().interrupt(); a.awaitThrowUncheckedOnInterrupt(1L, SECONDS); }, UncheckedInterruptedException.class);
        async.failure(awaitable, a -> { Thread.currentThread().interrupt(); a.awaitUntil(nanoTime() + SECONDS.toNanos(1L)); }, InterruptedException.class);
        async.success(awaitable, a -> { Thread.currentThread().interrupt(); return a.awaitUntilUninterruptibly(nanoTime() + SECONDS.toNanos(1L)); }, false);
        async.failure(awaitable, a -> { Thread.currentThread().interrupt(); a.awaitUntilThrowUncheckedOnInterrupt(nanoTime() + SECONDS.toNanos(1L)); }, UncheckedInterruptedException.class);
        Uninterruptibles.sleepUninterruptibly(2L, SECONDS);
        async.verify();
    }

    class Async
    {
        final List<ThrowingBiConsumer<Long, TimeUnit, ?>> waitingOn = new ArrayList<>();
        void verify()
        {
            for (int i = 0 ; i < waitingOn.size() ; ++i)
            {
                try
                {
                    waitingOn.get(i).accept(100L, MILLISECONDS);
                }
                catch (Throwable t)
                {
                    throw new AssertionError("" + i, t);
                }
            }
        }
        void failure(A awaitable, ThrowingConsumer<A, ?> action, Throwable failsWith)
        {
            waitingOn.add(exec.submit(() -> AbstractTestAwaitable.failure(awaitable, action, failsWith))::get);
        }
        void failure(A awaitable, ThrowingConsumer<A, ?> action, Class<? extends Throwable> failsWith)
        {
            waitingOn.add(exec.submit(() -> AbstractTestAwaitable.failure(awaitable, action, failsWith))::get);
        }
        void failure(A awaitable, ThrowingConsumer<A, ?> action, Predicate<Throwable> failsWith)
        {
            waitingOn.add(exec.submit(() -> AbstractTestAwaitable.failure(awaitable, action, failsWith))::get);
        }
        <P extends A, R> void success(P awaitable, ThrowingFunction<P, R, ?> action, R result)
        {
            waitingOn.add(exec.submit(() -> AbstractTestAwaitable.success(awaitable, action, result))::get);
        }
        <P extends A, R> void success(P awaitable, ThrowingFunction<P, R, ?> action, Predicate<R> result)
        {
            waitingOn.add(exec.submit(() -> AbstractTestAwaitable.success(awaitable, action, result))::get);
        }
    }

    private static <A extends Awaitable> void failure(A awaitable, ThrowingConsumer<A, ?> action, Throwable failsWith)
    {
        failure(awaitable, action, t -> Objects.equals(failsWith, t));
    }

    static <A extends Awaitable> void failure(A awaitable, ThrowingConsumer<A, ?> action, Class<? extends Throwable> failsWith)
    {
        failure(awaitable, action, failsWith::isInstance);
    }

    private static <A extends Awaitable> void failure(A awaitable, ThrowingConsumer<A, ?> action, Predicate<Throwable> failsWith)
    {
        Throwable fail = null;
        try
        {
            action.accept(awaitable);
        }
        catch (Throwable t)
        {
            fail = t;
        }
        if (!failsWith.test(fail))
            throw new AssertionError(fail);
    }

    static <A extends Awaitable, R> void success(A awaitable, ThrowingFunction<A, R, ?> action, R result)
    {
        try
        {
            Assert.assertEquals(result, action.apply(awaitable));
        }
        catch (Throwable t)
        {
            throw new AssertionError(t);
        }
    }

    static <A extends Awaitable, R> void success(A awaitable, ThrowingFunction<A, R, ?> action, Predicate<R> result)
    {
        try
        {
            Assert.assertTrue(result.test(action.apply(awaitable)));
        }
        catch (Throwable t)
        {
            throw new AssertionError(t);
        }
    }

}
