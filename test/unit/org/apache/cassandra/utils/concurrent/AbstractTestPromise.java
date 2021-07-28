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

import org.junit.Assert;

import io.netty.util.concurrent.Promise;
import net.openhft.chronicle.core.util.ThrowingBiConsumer;
import net.openhft.chronicle.core.util.ThrowingConsumer;
import net.openhft.chronicle.core.util.ThrowingFunction;

public abstract class AbstractTestPromise
{
    protected final ExecutorService exec = Executors.newCachedThreadPool();

    class Async
    {
        final List<ThrowingBiConsumer<Long, TimeUnit, ?>> waitingOn = new ArrayList<>();
        void verify()
        {
            for (int i = 0 ; i < waitingOn.size() ; ++i)
            {
                try
                {
                    waitingOn.get(i).accept(1L, TimeUnit.SECONDS);
                }
                catch (Throwable t)
                {
                    throw new AssertionError("" + i, t);
                }
            }
        }
        <V> void failure(Promise<V> promise, ThrowingConsumer<Promise<V>, ?> action, Throwable failsWith)
        {
            waitingOn.add(exec.submit(() -> AbstractTestPromise.failure(promise, action, failsWith))::get);
        }
        <V> void failure(Promise<V> promise, ThrowingConsumer<Promise<V>, ?> action, Class<? extends Throwable> failsWith)
        {
            waitingOn.add(exec.submit(() -> AbstractTestPromise.failure(promise, action, failsWith))::get);
        }
        <V> void failure(Promise<V> promise, ThrowingConsumer<Promise<V>, ?> action, Predicate<Throwable> failsWith)
        {
            waitingOn.add(exec.submit(() -> AbstractTestPromise.failure(promise, action, failsWith))::get);
        }
        <P extends Promise<?>, R> void success(P promise, ThrowingFunction<P, R, ?> action, R result)
        {
            waitingOn.add(exec.submit(() -> AbstractTestPromise.success(promise, action, result))::get);
        }
    }

    private static <V> void failure(Promise<V> promise, ThrowingConsumer<Promise<V>, ?> action, Throwable failsWith)
    {
        failure(promise, action, t -> Objects.equals(failsWith, t));
    }

    static <V> void failure(Promise<V> promise, ThrowingConsumer<Promise<V>, ?> action, Class<? extends Throwable> failsWith)
    {
        failure(promise, action, failsWith::isInstance);
    }

    private static <V> void failure(Promise<V> promise, ThrowingConsumer<Promise<V>, ?> action, Predicate<Throwable> failsWith)
    {
        Throwable fail = null;
        try
        {
            action.accept(promise);
        }
        catch (Throwable t)
        {
            fail = t;
        }
        if (!failsWith.test(fail))
            throw new AssertionError(fail);
    }

    static <P extends Promise<?>, R> void success(P promise, ThrowingFunction<P, R, ?> action, R result)
    {
        try
        {
            Assert.assertEquals(result, action.apply(promise));
        }
        catch (Throwable t)
        {
            throw new AssertionError(t);
        }
    }

}
