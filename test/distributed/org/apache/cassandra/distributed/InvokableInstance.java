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

package org.apache.cassandra.distributed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.utils.Throwables;

public abstract class InvokableInstance
{
    protected final ExecutorService isolatedExecutor;
    private final ClassLoader classLoader;
    private final Method deserializeOnInstance;

    public InvokableInstance(String name, ClassLoader classLoader)
    {
        this.isolatedExecutor = Executors.newCachedThreadPool(new NamedThreadFactory(name, Thread.NORM_PRIORITY, classLoader, new ThreadGroup(name)));
        this.classLoader = classLoader;
        try
        {
            this.deserializeOnInstance = classLoader.loadClass(InvokableInstance.class.getName()).getDeclaredMethod("deserializeOneObject", byte[].class);
        }
        catch (ClassNotFoundException | NoSuchMethodException e)
        {
            throw new RuntimeException(e);
        }
    }

    public interface CallableNoExcept<T> extends Callable<T> { public T call(); }
    public interface SerializableCallable<T> extends CallableNoExcept<T>, Serializable { }
    public <T> CallableNoExcept<T> callsOnInstance(SerializableCallable<T> call) { return invokesOnExecutor((SerializableCallable<T>) transferOneObject(call), isolatedExecutor); }
    public <T> T callOnInstance(SerializableCallable<T> call) { return callsOnInstance(call).call(); }

    public interface SerializableRunnable extends Runnable, Serializable {}
    public Runnable runsOnInstance(SerializableRunnable run) { return invokesOnExecutor((SerializableRunnable) transferOneObject(run), isolatedExecutor); }
    public void runOnInstance(SerializableRunnable run) { runsOnInstance(run).run(); }

    public interface SerializableConsumer<T> extends Consumer<T>, Serializable {}
    public <T> Consumer<T> acceptsOnInstance(SerializableConsumer<T> consumer) { return invokesOnExecutor((SerializableConsumer<T>) transferOneObject(consumer), isolatedExecutor); }

    public interface SerializableBiConsumer<T1, T2> extends BiConsumer<T1, T2>, Serializable {}
    public <T1, T2> BiConsumer<T1, T2> acceptsOnInstance(SerializableBiConsumer<T1, T2> consumer) { return invokesOnExecutor((SerializableBiConsumer<T1, T2>) transferOneObject(consumer), isolatedExecutor); }

    public interface SerializableFunction<I, O> extends Function<I, O>, Serializable {}
    public <I, O> Function<I, O> appliesOnInstance(SerializableFunction<I, O> f) { return invokesOnExecutor((SerializableFunction<I, O>) transferOneObject(f), isolatedExecutor); }

    public interface SerializableBiFunction<I1, I2, O> extends BiFunction<I1, I2, O>, Serializable {}
    public <I1, I2, O> BiFunction<I1, I2, O> appliesOnInstance(SerializableBiFunction<I1, I2, O> f) { return invokesOnExecutor((SerializableBiFunction<I1, I2, O>) transferOneObject(f), isolatedExecutor); }

    public interface TriFunction<I1, I2, I3, O>
    {
        O apply(I1 i1, I2 i2, I3 i3);
    }
    public interface SerializableTriFunction<I1, I2, I3, O> extends Serializable, TriFunction<I1, I2, I3, O> { }

    public <I1, I2, I3, O> TriFunction<I1, I2, I3, O> appliesOnInstance(SerializableTriFunction<I1, I2, I3, O> f) { return invokesOnExecutor((SerializableTriFunction<I1, I2, I3, O>) transferOneObject(f), isolatedExecutor); }

    public interface InstanceFunction<I, O> extends SerializableBiFunction<Instance, I, O> {}

    // E must be a functional interface, and lambda must be implemented by a lambda function
    public <E extends Serializable> E invokesOnInstance(E lambda)
    {
        return (E) transferOneObject(lambda);
    }

    public Object transferOneObject(Object object)
    {
        byte[] bytes = serializeOneObject(object);
        try
        {
            Object onInstance = deserializeOnInstance.invoke(null, bytes);
            if (onInstance.getClass().getClassLoader() != classLoader)
                throw new IllegalStateException(onInstance + " seemingly from wrong class loader: " + onInstance.getClass().getClassLoader() + ", but expected " + classLoader);

            return onInstance;
        }
        catch (IllegalAccessException | InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
    }

    private byte[] serializeOneObject(Object object)
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

    @SuppressWarnings("unused") // called through method invocation
    public static Object deserializeOneObject(byte[] bytes)
    {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bais);)
        {
            return ois.readObject();
        }
        catch (IOException | ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static <V> CallableNoExcept<V> invokesOnExecutor(SerializableCallable<V> callable, ExecutorService invokeOn)
    {
        return () -> {
            try
            {
                return invokeOn.submit(callable).get();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            catch (ExecutionException e)
            {
                Throwables.maybeFail(e.getCause());
                throw new AssertionError();
            }
        };
    }

    private static Runnable invokesOnExecutor(SerializableRunnable runnable, ExecutorService invokeOn)
    {
        return () -> {
            try
            {
                invokeOn.submit(runnable).get();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            catch (ExecutionException e)
            {
                Throwables.maybeFail(e.getCause());
                throw new AssertionError();
            }
        };
    }

    private static <A> Consumer<A> invokesOnExecutor(SerializableConsumer<A> consumer, ExecutorService invokeOn)
    {
        return (a) -> invokesOnExecutor(() -> consumer.accept(a), invokeOn).run();
    }

    private static <A, B> BiConsumer<A, B> invokesOnExecutor(SerializableBiConsumer<A, B> consumer, ExecutorService invokeOn)
    {
        return (a, b) -> invokesOnExecutor(() -> consumer.accept(a, b), invokeOn).run();
    }

    private static <A, B> Function<A, B> invokesOnExecutor(SerializableFunction<A, B> f, ExecutorService invokeOn)
    {
        return (a) -> invokesOnExecutor(() -> f.apply(a), invokeOn).call();
    }

    private static <A, B, C> BiFunction<A, B, C> invokesOnExecutor(SerializableBiFunction<A, B, C> f, ExecutorService invokeOn)
    {
        return (a, b) -> invokesOnExecutor(() -> f.apply(a, b), invokeOn).call();
    }

    private static <A, B, C, D> SerializableTriFunction<A, B, C, D> invokesOnExecutor(SerializableTriFunction<A, B, C, D> f, ExecutorService invokeOn)
    {
        return (a, b, c) -> invokesOnExecutor(() -> f.apply(a, b, c), invokeOn).call();
    }

    void shutdown()
    {
        isolatedExecutor.shutdownNow();
    }

}
