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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * An extension of {@link NonBlockingHashMap} where all values are wrapped by {@link Future}.
 * <p>
 * The main purpose of this class is to provide the functionality of concurrent hash map which may perform operations like
 * {@link ConcurrentHashMap#computeIfAbsent(Object, Function)} and {@link ConcurrentHashMap#computeIfPresent(Object, BiFunction)}
 * with synchronization scope reduced to the single key - that is, when dealing with a single key, unlike
 * {@link ConcurrentHashMap} we do not lock the whole map for the time the mapping function is running. This may help
 * to avoid the case when we want to load/unload a value for a key K1 while loading/unloading a value for a key K2. Such
 * scenario is forbidden in case of {@link ConcurrentHashMap} and leads to a deadlock. On the other hand, {@link NonBlockingHashMap}
 * does not guarantee at-most-once semantics of running the mapping function for a single key.
 *
 * @param <K>
 * @param <V>
 */
public class LoadingMap<K, V>
{
    private final NonBlockingHashMap<K, Future<V>> internalMap = new NonBlockingHashMap<>();

    /**
     * Returns a promise for the given key or null if there is nothing associated with the key.
     * <p/>if the promise is not fulfilled, it means that there a loading process associated with the key
     * <p/>if the promise is fulfilled with {@code null} value, it means that there is an unloading process associated with the key
     * <p/>if the promise is fulfilled with a failure, it means that a loading process associated with the key failed
     * but the exception was not propagated yet (a failed promise is eventually removed from the map)
     */
    @VisibleForTesting
    Future<V> get(K key)
    {
        return internalMap.get(key);
    }

    /**
     * Get a value for a given key. Returns a non-null object only if there is a successfully initialized value associated,
     * with the provided key. It returns {@code null} if there is no value for the key, or the value is being initialized
     * or removed. It does not throw if the last attempt to initialize the value failed.
     */
    public V getIfReady(K key)
    {
        Future<V> future = internalMap.get(key);
        return future != null ? future.getNow() : null;
    }

    /**
     * If the value for the given key is missing, execute a load function to obtain a value and put it into the map.
     * It is guaranteed that the loading and unloading a value for a single key are executed serially. It is also
     * guaranteed that the load function is executed exactly once to load a value into the map (regardless of the concurrent attempts).
     * <p/>
     * In case there is a concurrent attempt to load a value for this key, this attempt waits until the concurrent attempt
     * is done and returns its result (if succeeded). If the concurrent attempt fails, this attempt is retried. However,
     * if this attempt fails, it is not retried and the exception is rethrown. In case there is a concurrent attempt
     * to unload a value for this key, this attempt waits until the concurrent attempt is done and retries loading.
     * <p/>
     * When the mapping function returns {@code null}, {@link NullPointerException} is thrown. When the mapping function
     * throws exception, it is rethrown by this method. In both cases nothing gets added to the map.
     * <p/>
     * It is allowed to nest loading for a different key, though nested loading for the same key results in a deadlock.
     */
    public V blockingLoadIfAbsent(K key, Supplier<? extends V> loadFunction) throws RuntimeException
    {
        while (true)
        {
            Future<V> future = internalMap.get(key);
            boolean attemptedInThisThread = false;
            if (future == null)
            {
                AsyncPromise<V> newEntry = new AsyncPromise<>();
                future = internalMap.putIfAbsent(key, newEntry);
                if (future == null)
                {
                    // We managed to create an entry for the value. Now initialize it.
                    attemptedInThisThread = true;
                    future = newEntry;
                    try
                    {
                        V v = loadFunction.get();
                        if (v == null)
                            throw new NullPointerException("The mapping function returned null");
                        else
                            newEntry.setSuccess(v);
                    }
                    catch (Throwable t)
                    {
                        newEntry.setFailure(t);
                        // Remove future so that construction can be retried later
                        internalMap.remove(key, future);
                    }
                }

                // Else some other thread beat us to it, but we now have the reference to the future which we can wait for.
            }

            V v = future.awaitUninterruptibly().getNow();

            if (v != null) // implies success
                return v;

            if (attemptedInThisThread)
                // Rethrow if the failing attempt was initiated by us (failed and attemptedInThisThread)
                future.rethrowIfFailed();

            // Retry in other cases, that is, if blockingUnloadIfPresent was called in the meantime
            // (success and getNow == null) hoping that unloading gets finished soon, and if the concurrent attempt
            // to load entry fails
            Thread.yield();
        }
    }

    /**
     * If a value for the given key is present, unload function is run and the value is removed from the map.
     * Similarly to {@link #blockingLoadIfAbsent(Object, Supplier)} at-most-once semantics is guaranteed for unload
     * function.
     * <p/>
     * When unload function fails, the value is removed from the map anyway and the failure is rethrown.
     * <p/>
     * When the key was not found, the method returns {@code null}.
     *
     * @throws UnloadExecutionException when the unloading failed to complete - this is checked exception because
     *                                  the value is removed from the map regardless of the result of unloading;
     *                                  therefore if the unloading failed, the caller is responsible for handling that
     */
    public V blockingUnloadIfPresent(K key, Consumer<? super V> unloadFunction) throws UnloadExecutionException
    {
        Promise<V> droppedFuture = new AsyncPromise<V>().setSuccess(null);

        Future<V> existingFuture;
        do
        {
            existingFuture = internalMap.get(key);
            if (existingFuture == null || existingFuture.isDone() && existingFuture.getNow() == null)
                return null;
        } while (!internalMap.replace(key, existingFuture, droppedFuture));

        V v = existingFuture.awaitUninterruptibly().getNow();
        try
        {
            if (v == null)
                // which means that either the value failed to load or a concurrent attempt to unload already did the work
                return null;

            unloadFunction.accept(v);
            return v;
        }
        catch (Throwable t)
        {
            throw new UnloadExecutionException(v, t);
        }
        finally
        {
            Future<V> future = internalMap.remove(key);
            assert future == droppedFuture;
        }
    }

    /**
     * Thrown when unloading a value failed. It encapsulates the value which was failed to unload.
     */
    public static class UnloadExecutionException extends ExecutionException
    {
        private final Object value;

        public UnloadExecutionException(Object value, Throwable cause)
        {
            super(cause);
            this.value = value;
        }

        public <T> T value()
        {
            return (T) value;
        }
    }
}
