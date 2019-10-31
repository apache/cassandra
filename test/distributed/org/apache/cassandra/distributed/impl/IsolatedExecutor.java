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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.utils.ExecutorUtils;

public class IsolatedExecutor implements IIsolatedExecutor
{
    final ExecutorService isolatedExecutor;
    private final String name;
    private final ClassLoader classLoader;
    private final Method deserializeOnInstance;

    IsolatedExecutor(String name, ClassLoader classLoader)
    {
        this.name = name;
        this.isolatedExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("isolatedExecutor", Thread.NORM_PRIORITY, classLoader, new ThreadGroup(name)));
        this.classLoader = classLoader;
        this.deserializeOnInstance = lookupDeserializeOneObject(classLoader);
    }

    public Future<Void> shutdown()
    {
        isolatedExecutor.shutdown();

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
        ExecutorService shutdownExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 0, TimeUnit.SECONDS,
                                                                  new LinkedBlockingQueue<>(), threadFactory);
        return shutdownExecutor.submit(() -> {
            try
            {
                ExecutorUtils.awaitTermination(60, TimeUnit.SECONDS, isolatedExecutor);

                // Shutdown logging last - this is not ideal as the logging subsystem is initialized
                // outsize of this class, however doing it this way provides access to the full
                // logging system while termination is taking place.
                LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
                loggerContext.stop();

                // Close the instance class loader after shutting down the isolatedExecutor and logging
                // in case error handling triggers loading additional classes
                ((URLClassLoader) classLoader).close();
            }
            finally
            {
                shutdownExecutor.shutdownNow();
            }
            return null;
        });
    }

    public <O> CallableNoExcept<Future<O>> async(CallableNoExcept<O> call) { return () -> isolatedExecutor.submit(call); }
    public <O> CallableNoExcept<O> sync(CallableNoExcept<O> call) { return () -> waitOn(async(call).call()); }

    public CallableNoExcept<Future<?>> async(Runnable run) { return () -> isolatedExecutor.submit(run); }
    public Runnable sync(Runnable run) { return () -> waitOn(async(run).call()); }

    public <I> Function<I, Future<?>> async(Consumer<I> consumer) { return (a) -> isolatedExecutor.submit(() -> consumer.accept(a)); }
    public <I> Consumer<I> sync(Consumer<I> consumer) { return (a) -> waitOn(async(consumer).apply(a)); }

    public <I1, I2> BiFunction<I1, I2, Future<?>> async(BiConsumer<I1, I2> consumer) { return (a, b) -> isolatedExecutor.submit(() -> consumer.accept(a, b)); }
    public <I1, I2> BiConsumer<I1, I2> sync(BiConsumer<I1, I2> consumer) { return (a, b) -> waitOn(async(consumer).apply(a, b)); }

    public <I, O> Function<I, Future<O>> async(Function<I, O> f) { return (a) -> isolatedExecutor.submit(() -> f.apply(a)); }
    public <I, O> Function<I, O> sync(Function<I, O> f) { return (a) -> waitOn(async(f).apply(a)); }

    public <I1, I2, O> BiFunction<I1, I2, Future<O>> async(BiFunction<I1, I2, O> f) { return (a, b) -> isolatedExecutor.submit(() -> f.apply(a, b)); }
    public <I1, I2, O> BiFunction<I1, I2, O> sync(BiFunction<I1, I2, O> f) { return (a, b) -> waitOn(async(f).apply(a, b)); }

    public <I1, I2, I3, O> TriFunction<I1, I2, I3, Future<O>> async(TriFunction<I1, I2, I3, O> f) { return (a, b, c) -> isolatedExecutor.submit(() -> f.apply(a, b, c)); }
    public <I1, I2, I3, O> TriFunction<I1, I2, I3, O> sync(TriFunction<I1, I2, I3, O> f) { return (a, b, c) -> waitOn(async(f).apply(a, b, c)); }

    public <E extends Serializable> E transfer(E object)
    {
        return (E) transferOneObject(object, classLoader, deserializeOnInstance);
    }

    static <E extends Serializable> E transferAdhoc(E object, ClassLoader classLoader)
    {
        return transferOneObject(object, classLoader, lookupDeserializeOneObject(classLoader));
    }

    private static <E extends Serializable> E transferOneObject(E object, ClassLoader classLoader, Method deserializeOnInstance)
    {
        byte[] bytes = serializeOneObject(object);
        try
        {
            Object onInstance = deserializeOnInstance.invoke(null, bytes);
            if (onInstance.getClass().getClassLoader() != classLoader)
                throw new IllegalStateException(onInstance + " seemingly from wrong class loader: " + onInstance.getClass().getClassLoader() + ", but expected " + classLoader);

            return (E) onInstance;
        }
        catch (IllegalAccessException | InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
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
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause());
        }
    }

    public interface ThrowingRunnable
    {
        public void run() throws Throwable;

        public static Runnable toRunnable(ThrowingRunnable runnable)
        {
            return () -> {
                try
                {
                    runnable.run();
                }
                catch (Throwable throwable)
                {
                    throw new RuntimeException(throwable);
                }
            };
        }
    }
}
