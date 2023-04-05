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

package org.apache.cassandra.distributed.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.Throwables;

import static java.util.concurrent.TimeUnit.SECONDS;

public class IsolatedExecutor implements IIsolatedExecutor
{
    final ExecutorService isolatedExecutor;
    private final String name;
    final ClassLoader classLoader;
    private final DynamicFunction<Serializable> transfer;
    private final ShutdownExecutor shutdownExecutor;

    public static final ShutdownExecutor DEFAULT_SHUTDOWN_EXECUTOR = (name, classLoader, shuttingDown, onTermination) -> {
        /* Use a thread pool with a core pool size of zero to terminate the thread as soon as possible
         ** so the instance class loader can be garbage collected.  Uses a custom thread factory
         ** rather than NamedThreadFactory to avoid calling FastThreadLocal.removeAll() in 3.0 and up
         ** as it was observed crashing during test failures and made it harder to find the real cause.
         */
        ThreadFactory threadFactory = (Runnable r) -> {
            Thread t = new Thread(r, name + "_shutdown");
            t.setDaemon(true);
            return t;
        };

        ExecutorService shutdownExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 0, SECONDS,
                                                                  new LinkedBlockingQueue<>(), threadFactory);
        return shutdownExecutor.submit(() -> {
            try
            {
                ExecutorUtils.awaitTermination(60, TimeUnit.SECONDS, shuttingDown);
                return onTermination.call();
            }
            finally
            {
                shutdownExecutor.shutdownNow();
            }
        });
    };

    // retained for backwards compatibility
    @SuppressWarnings("unused")
    public IsolatedExecutor(String name, ClassLoader classLoader, ExecutorFactory executorFactory)
    {
        this(name, classLoader, executorFactory.pooled("isolatedExecutor", Integer.MAX_VALUE), DEFAULT_SHUTDOWN_EXECUTOR);
    }

    // retained for backwards compatibility
    @SuppressWarnings("unused")
    public IsolatedExecutor(String name, ClassLoader classLoader, ExecutorService executorService)
    {
        this(name, classLoader, executorService, DEFAULT_SHUTDOWN_EXECUTOR);
    }

    IsolatedExecutor(String name, ClassLoader classLoader, ExecutorService executorService, ShutdownExecutor shutdownExecutor)
    {
        this.name = name;
        this.isolatedExecutor = executorService;
        this.classLoader = classLoader;
        this.transfer = transferTo(classLoader);
        this.shutdownExecutor = shutdownExecutor;
    }

    protected IsolatedExecutor(IsolatedExecutor from, ExecutorService executor)
    {
        this.name = from.name;
        this.isolatedExecutor = executor;
        this.classLoader = from.classLoader;
        this.transfer = from.transfer;
        this.shutdownExecutor = from.shutdownExecutor;
    }

    public IIsolatedExecutor with(ExecutorService executor)
    {
        return new IsolatedExecutor(this, executor);
    }

    public Future<Void> shutdown()
    {
        isolatedExecutor.shutdownNow();
        return shutdownExecutor.shutdown(name, classLoader, isolatedExecutor, () -> {

            // Shutdown logging last - this is not ideal as the logging subsystem is initialized
            // outsize of this class, however doing it this way provides access to the full
            // logging system while termination is taking place.
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            loggerContext.stop();

            FastThreadLocal.destroy();

            // Close the instance class loader after shutting down the isolatedExecutor and logging
            // in case error handling triggers loading additional classes
            ((URLClassLoader) classLoader).close();
            return null;
        });
    }

    public <O> Supplier<Future<O>> supplyAsync(SerializableSupplier<O> call) { return () -> isolatedExecutor.submit(call::get); }
    public <O> Supplier<O> supplySync(SerializableSupplier<O> call) { return () -> waitOn(supplyAsync(call).get()); }

    public <O> CallableNoExcept<Future<O>> async(CallableNoExcept<O> call) { return () -> isolatedExecutor.submit(call); }
    public <O> CallableNoExcept<O> sync(CallableNoExcept<O> call) { return () -> waitOn(async(call).call()); }

    public CallableNoExcept<Future<?>> async(Runnable run) { return () -> isolatedExecutor.submit(run); }
    public Runnable sync(Runnable run) { return () -> waitOn(async(run).call()); }

    public <I> Function<I, Future<?>> async(Consumer<I> consumer) { return (a) -> isolatedExecutor.submit(() -> consumer.accept(a)); }
    public <I> Consumer<I> sync(Consumer<I> consumer) { return (a) -> waitOn(async(consumer).apply(a)); }

    public <I1, I2> BiFunction<I1, I2, Future<?>> async(BiConsumer<I1, I2> consumer) { return (a, b) -> isolatedExecutor.submit(() -> consumer.accept(a, b)); }
    public <I1, I2> BiConsumer<I1, I2> sync(BiConsumer<I1, I2> consumer) { return (a, b) -> waitOn(async(consumer).apply(a, b)); }

    public <I1, I2, I3> TriFunction<I1, I2, I3, Future<?>> async(TriConsumer<I1, I2, I3> consumer) { return (a, b, c) -> isolatedExecutor.submit(() -> consumer.accept(a, b, c)); }
    public <I1, I2, I3> TriConsumer<I1, I2, I3> sync(TriConsumer<I1, I2, I3> consumer) { return (a, b, c) -> waitOn(async(consumer).apply(a, b, c)); }

    public <I, O> Function<I, Future<O>> async(Function<I, O> f) { return (a) -> isolatedExecutor.submit(() -> f.apply(a)); }
    public <I, O> Function<I, O> sync(Function<I, O> f) { return (a) -> waitOn(async(f).apply(a)); }

    public <I1, I2, O> BiFunction<I1, I2, Future<O>> async(BiFunction<I1, I2, O> f) { return (a, b) -> isolatedExecutor.submit(() -> f.apply(a, b)); }
    public <I1, I2, O> BiFunction<I1, I2, O> sync(BiFunction<I1, I2, O> f) { return (a, b) -> waitOn(async(f).apply(a, b)); }

    public <I1, I2, I3, O> TriFunction<I1, I2, I3, Future<O>> async(TriFunction<I1, I2, I3, O> f) { return (a, b, c) -> isolatedExecutor.submit(() -> f.apply(a, b, c)); }
    public <I1, I2, I3, O> TriFunction<I1, I2, I3, O> sync(TriFunction<I1, I2, I3, O> f) { return (a, b, c) -> waitOn(async(f).apply(a, b, c)); }

    public <I1, I2, I3, I4, O> QuadFunction<I1, I2, I3, I4, Future<O>> async(QuadFunction<I1, I2, I3, I4, O> f) { return (a, b, c, d) -> isolatedExecutor.submit(() -> f.apply(a, b, c, d)); }
    public <I1, I2, I3, I4, O> QuadFunction<I1, I2, I3, I4, O> sync(QuadFunction<I1, I2, I3, I4, O> f) { return (a, b, c, d) -> waitOn(async(f).apply(a, b, c, d)); }

    public <I1, I2, I3, I4, I5, O> QuintFunction<I1, I2, I3, I4, I5, Future<O>> async(QuintFunction<I1, I2, I3, I4, I5, O> f) { return (a, b, c, d, e) -> isolatedExecutor.submit(() -> f.apply(a, b, c, d, e)); }
    public <I1, I2, I3, I4, I5, O> QuintFunction<I1, I2, I3, I4, I5, O> sync(QuintFunction<I1, I2, I3, I4, I5, O> f) { return (a, b, c, d,e ) -> waitOn(async(f).apply(a, b, c, d, e)); }

    public Executor executor()
    {
        return isolatedExecutor;
    }

    public <T extends Serializable> T transfer(T in)
    {
        return transfer.apply(in);
    }

    public static <T extends Serializable> T transferAdhoc(T object, ClassLoader classLoader)
    {
        try
        {
            return transferOneObjectAdhoc(object, classLoader, lookupDeserializeOneObject(classLoader));
        }
        catch (IllegalAccessException | InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static <T extends Serializable> T transferAdhocPropagate(T object, ClassLoader classLoader) throws InvocationTargetException, IllegalAccessException
    {
        return transferOneObjectAdhoc(object, classLoader, lookupDeserializeOneObject(classLoader));
    }

    private static final SerializableFunction<byte[], Object> DESERIALIZE_ONE_OBJECT = IsolatedExecutor::deserializeOneObject;

    public static DynamicFunction<Serializable> transferTo(ClassLoader classLoader)
    {
        SerializableFunction<byte[], Object> deserializeOneObject = transferAdhoc(DESERIALIZE_ONE_OBJECT, classLoader);
        return new DynamicFunction<Serializable>()
        {
            public <T extends Serializable> T apply(T in)
            {
                return (T) deserializeOneObject.apply(serializeOneObject(in));
            }
        };
    }

    private static <T extends Serializable> T transferOneObjectAdhoc(T object, ClassLoader classLoader, Method deserializeOnInstance) throws IllegalAccessException, InvocationTargetException
    {
        byte[] bytes = serializeOneObject(object);
        Object onInstance = deserializeOnInstance.invoke(null, bytes);
        if (onInstance.getClass().getClassLoader() != classLoader)
            throw new IllegalStateException(onInstance + " seemingly from wrong class loader: " + onInstance.getClass().getClassLoader() + ", but expected " + classLoader);

        return (T) onInstance;
    }

    private static Method lookupDeserializeOneObject(ClassLoader classLoader)
    {
        try
        {
            return classLoader.loadClass(IsolatedExecutor.class.getName()).getDeclaredMethod("deserializeOneObject", byte[].class);
        }
        catch (ClassNotFoundException | NoSuchMethodException e)
        {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unused") // called through method invocation
    public static Object deserializeOneObject(byte[] bytes)
    {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bais))
        {
            return ois.readObject();
        }
        catch (IOException | ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static byte[] serializeOneObject(Object object)
    {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos))
        {
            oos.writeObject(object);
            oos.close();
            return baos.toByteArray();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static <T> T waitOn(Future<T> f)
    {
        try
        {
            return f.get();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw Throwables.throwAsUncheckedException(e);
        }
        catch (ExecutionException e)
        {
            throw Throwables.throwAsUncheckedException(e.getCause());
        }
    }

}
