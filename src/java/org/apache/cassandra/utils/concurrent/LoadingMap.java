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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * An extension of {@link NonBlockingHashMap} where all values are wrapped by {@link CompletableFuture}.
 * <p>
 * The main purpose of this class is to provide the functionality of concurrent hash map which may perform operations
 * like {@link ConcurrentHashMap#compute(Object, BiFunction)} with synchronization scope reduced to the single key -
 * that is, when dealing with a single key, unlike {@link ConcurrentHashMap} the whole map is not locked for the time
 * the mapping function is running. This may help to avoid the case when loading/unloading a value for a key K1 while
 * loading/unloading a value for a key K2. Such scenario is forbidden in case of {@link ConcurrentHashMap} and leads to
 * a deadlock. On the other hand, {@link NonBlockingHashMap} does not guarantee at-most-once semantics of running the
 * mapping function for a single key.
 */
public class LoadingMap<K, V>
{
    // The map of futures lets us synchronize on per key basis rather than synchronizing the whole map.
    // It works in the way that when there is an ongoing computation (update) on a key, the other thread
    // trying to access that key recevies an incomplete future and needs to wait until the computation is done.
    // This way we can achieve serial execution for each key while different keys can be processed concurrently.
    // It also ensures exactly-once semantics for the update operation.
    private final Map<K, CompletableFuture<V>> internalMap;

    public LoadingMap()
    {
        this.internalMap = new NonBlockingHashMap<>();
    }

    public LoadingMap(int initialSize)
    {
        this.internalMap = new NonBlockingHashMap<>(initialSize);
    }

    /**
     * Recomputes the given object in the map in a thread-safe way.
     * The remapping function is applied for the entry of the provided key with the following rules:
     * - if entry exists, it is passed to the remapping function
     * - if entry does not exist, null is passed to the remapping function
     * - if the remapping function returns non-null value, the entry is added or replaced
     * - if the remapping function returns null value, the entry is removed
     * <p>
     * The remapping function is guaranteed to be applied exactly once.
     * <p>
     * The method blocks until the update is applied. The method waits for the ongoing updates for the same key, but
     * it does not wait for any updates for other keys.
     */
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction)
    {
        CompletableFuture<V> newEntry = new CompletableFuture<>();
        CompletableFuture<V> previousEntry = replaceEntry(key, newEntry, false, false);
        return updateOrRemoveEntry(key, remappingFunction, previousEntry, newEntry);
    }

    /**
     * Similar to {@link #compute(Object, BiFunction)} but the mapping function is applied only if there is no existing
     * entry in the map. Thus, the mapping function will be applied at-most-once.
     */
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction)
    {
        CompletableFuture<V> newEntry = new CompletableFuture<>();
        CompletableFuture<V> previousEntry = replaceEntry(key, newEntry, false, true);
        if (previousEntry != null)
            return previousEntry.join();

        return updateOrRemoveEntry(key, (k, v) -> mappingFunction.apply(k), previousEntry, newEntry);
    }

    /**
     * Similar to {@link #compute(Object, BiFunction)} but the remapping function is applied only if there is existing
     * item in the map. Thus, the remapping function will be applied at-most-once and the value parameter will never be
     * {@code null}.
     */
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction)
    {
        CompletableFuture<V> newEntry = new CompletableFuture<>();
        CompletableFuture<V> previousEntry = replaceEntry(key, newEntry, true, false);
        if (previousEntry == null)
            return null;

        return updateOrRemoveEntry(key, remappingFunction, previousEntry, newEntry);
    }

    /**
     * Safely replaces the future entry in the internal map, reattempting if the existing entry resolves to {@code null},
     *
     * @param key           key for which the entry is to be replaced
     * @param newEntry      new entry to be put into the map
     * @param skipIfMissing if set, the entry will be replaced only if there is an existing entry in the map
     *                      (which resolves to a non-null value); otherwise, the method returns {@code null}
     * @param skipIfExists  if set, the entry will be put into the map only if there is no existing entry (which resolves
     *                      to a non-null value); otherwise, the method returns the existing entry
     * @return the existing entry or {@code null} if there was no entry in the map
     */
    private CompletableFuture<V> replaceEntry(K key, CompletableFuture<V> newEntry, boolean skipIfMissing, boolean skipIfExists)
    {
        CompletableFuture<V> previousEntry;
        V previousValue;
        do
        {
            previousEntry = internalMap.get(key);

            if (previousEntry == null)
            {
                if (skipIfMissing || internalMap.putIfAbsent(key, newEntry) == null)
                    // skip-if-missing: break fast if we are aiming to remove the entry - if it does not exist, there is nothing to do
                    // put-if-abset: there were no entry for the provided key, so we put a promise there and break
                    return null;
            }
            else
            {
                previousValue = previousEntry.join();

                if (previousValue != null)
                {
                    if (skipIfExists || internalMap.replace(key, previousEntry, newEntry))
                        // skip-if-exist: break fast if we are aiming to compute a new entry only if it is missing
                        // replace: there was a legitmate entry with a non-null value - we replace it with a promise and break
                        return previousEntry;
                }

                // otherwise, if previousValue == null, some other thread deleted the entry in the meantime; we need
                // to try again because yet another thread might have attempted to do something for that key
            }
        } while (true);
    }

    /**
     * Applies the transformation on entry in a safe way. If the transformation throws an exception, the previous state
     * is recovered.
     *
     * @param key               key for which we process entries
     * @param remappingFunction remapping function which gets the key, the current entry value and is expected to return
     *                          a new value or null if the entry is to be removed
     * @param previousEntry     previous entry, which is no longer in the map but its value is already resolved and non-null
     * @param newEntry          new entry, which is already in the map and is a non-completed promise
     * @return the resolved value of the new entry
     */
    private V updateOrRemoveEntry(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, CompletableFuture<V> previousEntry, CompletableFuture<V> newEntry)
    {
        V previousValue = previousEntry != null ? previousEntry.join() : null;

        try
        {
            // apply the provided remapping function
            V newValue = remappingFunction.apply(key, previousValue);
            if (newValue == null)
            {
                // null result means we should remove the entry
                CompletableFuture<V> removedEntry = internalMap.remove(key);
                newEntry.complete(null);
                assert removedEntry == newEntry;
                return null;
            }
            else
            {
                // non-null result means we should complete the new entry promise with the returned value
                newEntry.complete(newValue);
                return newValue;
            }
        }
        catch (RuntimeException ex)
        {
            // in case of exception (which may happen only in remapping function), we simply revert the change and
            // rethrow the exception
            if (previousEntry != null)
            {
                // if the entry existed before, the new entry promise is simply completed with the old value
                newEntry.complete(previousValue);
            }
            else
            {
                // if the entry did not exist before, the new entry is removed and promise is completed with null, which
                // tells other threads waiting for the promise completion to try again
                CompletableFuture<V> f = internalMap.remove(key);
                assert f == newEntry;
                newEntry.complete(null);
            }

            throw ex;
        }
    }

    @VisibleForTesting
    Future<V> getUnsafe(K key)
    {
        return internalMap.get(key);
    }

    public V getIfReady(K key)
    {
        CompletableFuture<V> f = internalMap.get(key);
        return f != null ? f.getNow(null) : null;
    }

    public V get(K key)
    {
        while (true)
        {
            CompletableFuture<V> entry = internalMap.get(key);
            if (entry == null)
                // value not found
                return null;

            V value = entry.join();
            if (value != null)
                return value;

            // we need to retry because info == null means that the entry got removed
            Thread.yield();
        }
    }

    public Stream<V> valuesStream()
    {
        return internalMap.keySet().stream().map(this::get).filter(Objects::nonNull);
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
     * <p/>
     * Note that this is just a special case of {@link #computeIfAbsent(Object, Function)} which would just throw
     * {@link NullPointerException} if the mapping function returns {@code null}. This method is there mostly to ensure
     * parity with OSS implementation.
     */
    public V blockingLoadIfAbsent(K key, Supplier<? extends V> loadFunction) throws RuntimeException
    {
        return computeIfAbsent(key, k -> {
            V result = loadFunction.get();
            if (result == null)
                throw new NullPointerException("The mapping function returned null");
            return result;
        });
    }

    /**
     * If a value for the given key is present, unload function is run and the value is removed from the map.
     * Similarly to {@link #blockingLoadIfAbsent(Object, Supplier)} at-most-once semantics is guaranteed for unload
     * function.
     * <p/>
     * When unload function fails, the value is removed from the map anyway and the failure is rethrown.
     * <p/>
     * When the key was not found, the method returns {@code null}.
     * <p>
     * Note that this has slightly different semantics than {@link #computeIfPresent(Object, BiFunction)} where
     * the mapping function returns {@code null} - in particular, the value is removed from the map regardless the
     * mapping function succeedes or not. The value removed from the map (if existed) is returned (unlike in case of
     * {@link #computeIfPresent(Object, BiFunction)} which always return new value - {@code null} in case of removing).
     * If the mapping function fails, the value associated with the provided key (if existed) is encapsulated in the
     * exception.
     *
     * @throws UnloadExecutionException when the unloading failed to complete - this is checked exception because
     *                                  the value is removed from the map regardless of the result of unloading;
     *                                  therefore if the unloading failed, the caller is responsible for handling that
     */
    public V blockingUnloadIfPresent(K key, Consumer<? super V> unloadFunction) throws UnloadExecutionException
    {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<V> value = new AtomicReference<>();
        computeIfPresent(key, (k, v) -> {
            try
            {
                value.set(v);
                unloadFunction.accept(v);
            }
            catch (Throwable t)
            {
                failure.set(t);
            }
            return null;
        });
        if (failure.get() == null)
            return value.get();
        else
            throw new UnloadExecutionException(value.get(), failure.get());
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
