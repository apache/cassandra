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

package org.apache.cassandra.distributed.api;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Represents a clean way to handoff evaluation of some work to an executor associated
 * with a node's lifetime.
 *
 * There is no transfer of execution to the parallel class hierarchy.
 *
 * Classes, such as Instance, that are themselves instantiated on the correct ClassLoader, utilise this class
 * to ensure the lifetime of any thread evaluating one of its method invocations matches the lifetime of the class itself.
 * Since they are instantiated on the correct ClassLoader, sharing only the interface, there is no serialization necessary.
 */
public interface IIsolatedExecutor
{
    public interface CallableNoExcept<O> extends Callable<O> { public O call(); }
    public interface SerializableCallable<O> extends CallableNoExcept<O>, Serializable { }
    public interface SerializableRunnable extends Runnable, Serializable {}
    public interface SerializableConsumer<O> extends Consumer<O>, Serializable {}
    public interface SerializableBiConsumer<I1, I2> extends BiConsumer<I1, I2>, Serializable {}
    public interface SerializableFunction<I, O> extends Function<I, O>, Serializable {}
    public interface SerializableBiFunction<I1, I2, O> extends BiFunction<I1, I2, O>, Serializable {}
    public interface TriFunction<I1, I2, I3, O>
    {
        O apply(I1 i1, I2 i2, I3 i3);
    }
    public interface SerializableTriFunction<I1, I2, I3, O> extends Serializable, TriFunction<I1, I2, I3, O> { }

    Future<Void> shutdown();

    /**
     * Convert the execution to one performed asynchronously on the IsolatedExecutor, returning a Future of the execution result
     */
    <O> CallableNoExcept<Future<O>> async(CallableNoExcept<O> call);

    /**
     * Convert the execution to one performed synchronously on the IsolatedExecutor
     */
    <O> CallableNoExcept<O> sync(CallableNoExcept<O> call);

    /**
     * Convert the execution to one performed asynchronously on the IsolatedExecutor, returning a Future of the execution result
     */
    CallableNoExcept<Future<?>> async(Runnable run);

    /**
     * Convert the execution to one performed synchronously on the IsolatedExecutor
     */
    Runnable sync(Runnable run);

    /**
     * Convert the execution to one performed asynchronously on the IsolatedExecutor, returning a Future of the execution result
     */
    <I> Function<I, Future<?>> async(Consumer<I> consumer);

    /**
     * Convert the execution to one performed synchronously on the IsolatedExecutor
     */
    <I> Consumer<I> sync(Consumer<I> consumer);

    /**
     * Convert the execution to one performed asynchronously on the IsolatedExecutor, returning a Future of the execution result
     */
    <I1, I2> BiFunction<I1, I2, Future<?>> async(BiConsumer<I1, I2> consumer);

    /**
     * Convert the execution to one performed synchronously on the IsolatedExecutor
     */
    <I1, I2> BiConsumer<I1, I2> sync(BiConsumer<I1, I2> consumer);

    /**
     * Convert the execution to one performed asynchronously on the IsolatedExecutor, returning a Future of the execution result
     */
    <I, O> Function<I, Future<O>> async(Function<I, O> f);

    /**
     * Convert the execution to one performed synchronously on the IsolatedExecutor
     */
    <I, O> Function<I, O> sync(Function<I, O> f);

    /**
     * Convert the execution to one performed asynchronously on the IsolatedExecutor, returning a Future of the execution result
     */
    <I1, I2, O> BiFunction<I1, I2, Future<O>> async(BiFunction<I1, I2, O> f);

    /**
     * Convert the execution to one performed synchronously on the IsolatedExecutor
     */
    <I1, I2, O> BiFunction<I1, I2, O> sync(BiFunction<I1, I2, O> f);

    /**
     * Convert the execution to one performed asynchronously on the IsolatedExecutor, returning a Future of the execution result
     */
    <I1, I2, I3, O> TriFunction<I1, I2, I3, Future<O>> async(TriFunction<I1, I2, I3, O> f);

    /**
     * Convert the execution to one performed synchronously on the IsolatedExecutor
     */
    <I1, I2, I3, O> TriFunction<I1, I2, I3, O> sync(TriFunction<I1, I2, I3, O> f);

}
