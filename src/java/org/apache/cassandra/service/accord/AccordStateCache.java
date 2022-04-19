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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import accord.api.Read;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * Cache for AccordCommand and AccordCommandsForKey, available memory is shared between the two object types.
 *
 * Supports dynamic object sizes. After each acquire/free cycle, the cacheable objects size is recomputed to
 * account for data added/removed during txn processing if it's modified flag is set
 */
public class AccordStateCache
{
    public interface AccordState<K, V extends AccordState<K, V>>
    {
        Node<K, V> createNode();
        K key();
        boolean hasModifications();
        void clearModifiedFlag();
    }

    static abstract class Node<K, V extends AccordStateCache.AccordState<K, V>>
    {
        // just for measuring empty size on heap
        private static class MeasurableState implements AccordState<Object, MeasurableState>
        {
            @Override
            public Node<Object, MeasurableState> createNode()
            {
                return new Node<>(this) { @Override long sizeInBytes(MeasurableState value) { return 0; } };
            }
            @Override public Object key() { return null; }
            @Override public boolean hasModifications() { return false; }
            @Override public void clearModifiedFlag() { } }
        static final long EMPTY_SIZE = ObjectSizes.measure(new MeasurableState().createNode());

        final V value;
        private Node<?, ?> prev;
        private Node<?, ?> next;
        private int references = 0;
        private long lastQueriedSize = 0;

        Node(V value)
        {
            this.value = value;
        }

        abstract long sizeInBytes(V value);

        long size()
        {
            long result = EMPTY_SIZE + sizeInBytes(value);
            lastQueriedSize = result;
            return result;
        }

        long sizeDelta()
        {
            long prevSize = lastQueriedSize;
            return size() - prevSize;
        }

        K key()
        {
            return value.key();
        }
    }

    static class Stats
    {
        private long queries;
        private long hits;
        private long misses;
    }

    public final Map<Object, Node<?, ?>> active = new HashMap<>();
    private final Map<Object, Node<?, ?>> cache = new HashMap<>();
    private final Set<Instance<?, ?>> instances = new HashSet<>();

    private final Map<Object, Future<?>> loadFutures = new HashMap<>();
    private final Map<Object, Future<?>> saveFutures = new HashMap<>();

    private final Map<Object, Read.ReadFuture> readFutures = new HashMap<>();
    private final Map<Object, Future<?>> writeFutures = new HashMap<>();

    Node<?, ?> head;
    Node<?, ?> tail;
    private long maxSizeInBytes;
    private long bytesCached = 0;
    private final Stats stats = new Stats();

    public AccordStateCache(long maxSizeInBytes)
    {
        this.maxSizeInBytes = maxSizeInBytes;
    }

    public void setMaxSize(long size)
    {
        maxSizeInBytes = size;
        maybeEvict();
    }

    private void unlink(Node<?, ?> node)
    {
        Node<?, ?> prev = node.prev;
        Node<?, ?> next = node.next;

        if (prev == null)
        {
            Preconditions.checkState(head == node);
            head = next;
        }
        else
        {
            prev.next = next;
        }

        if (next == null)
        {
            Preconditions.checkState(tail == node);
            tail = prev;
        }
        else
        {
            next.prev = prev;
        }

        node.prev = null;
        node.next = null;
    }

    private void push(Node<?, ?> node)
    {
        if (head != null)
        {
            node.prev = null;
            node.next = head;
            head.prev = node;
            head = node;
        }
        else
        {
            head = node;
            tail = node;
        }
    }

    private void updateSize(Node<?, ?> node)
    {
        bytesCached += node.sizeDelta();
    }

    private void maybeEvict()
    {
        if (bytesCached <= maxSizeInBytes)
            return;

        Node<?, ?> current = tail;
        while (current != null && bytesCached > maxSizeInBytes)
        {
            // don't evict if there's an outstanding save future. If an item is evicted then reloaded
            // before it's mutation is applied, out of date info will be loaded
            Future<?> future = getFutureInternal(saveFutures, current.key());
            if (future != null && !future.isDone())
            {
                current = current.prev;
                continue;
            }
            Node<?, ?> evict = current;
            current = current.prev;

            unlink(evict);
            cache.remove(evict.key());
            bytesCached -= evict.size();
        }
    }

    private <K, V extends AccordStateCache.AccordState<K, V>> V getOrCreateInternal(K key, Function<K, V> factory, Stats instanceStats)
    {
        stats.queries++;
        instanceStats.queries++;

        Node<K, V> node = (Node<K, V>) active.get(key);
        if (node != null)
        {
            stats.hits++;
            instanceStats.hits++;
            node.references++;
            return node.value;
        }

        node = (Node<K, V>) cache.remove(key);

        if (node == null)
        {
            stats.misses++;
            instanceStats.misses++;
            V value = factory.apply(key);
            node = value.createNode();
            updateSize(node);
        }
        else
        {
            stats.hits++;
            instanceStats.hits++;
            unlink(node);
        }

        Preconditions.checkState(node.references == 0);
        maybeEvict();

        node.references++;
        active.put(key, node);

        return node.value;
    }

    private <K, V extends AccordStateCache.AccordState<K, V>> void releaseInternal(V value)
    {
        K key = value.key();
        maybeClearFuture(key);
        Node<K, V> node = (Node<K, V>) active.get(key);
        Preconditions.checkState(node != null && node.references > 0);
        Preconditions.checkState(node.value == value);
        if (--node.references == 0)
        {
            active.remove(key);
            cache.put(key, node);
            push(node);
        }

        if (value.hasModifications())
        {
            value.clearModifiedFlag();
            updateSize(node);
        }
        maybeEvict();
    }

    private static <K, F extends Future<?>> F getFutureInternal(Map<Object, F> futuresMap, K key)
    {
        F r = futuresMap.get(key);
        if (r == null)
            return null;

        if (!r.isDone())
            return r;

        futuresMap.remove(key);
        return null;
    }

    private static <K, F extends Future<?>> void setFutureInternal(Map<Object, F> futuresMap, K key, F future)
    {
        Preconditions.checkState(!futuresMap.containsKey(key));
        futuresMap.put(key, future);
    }

    private <K> void maybeClearFuture(K key)
    {
        // will clear if it's done
        getFutureInternal(loadFutures, key);
        getFutureInternal(saveFutures, key);
        getFutureInternal(readFutures, key);
        getFutureInternal(writeFutures, key);
    }

    public class Instance<K, V extends AccordStateCache.AccordState<K, V>>
    {
        private final Class<K> keyClass;
        private final Class<V> valClass;
        private final Function<K, V> factory;
        private final Stats stats = new Stats();

        public Instance(Class<K> keyClass, Class<V> valClass, Function<K, V> factory)
        {
            this.keyClass = keyClass;
            this.valClass = valClass;
            this.factory = factory;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Instance<?, ?> instance = (Instance<?, ?>) o;
            return keyClass.equals(instance.keyClass) && valClass.equals(instance.valClass);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keyClass, valClass);
        }

        public V getOrCreate(K key)
        {
            return getOrCreateInternal(key, factory, stats);
        }

        public void release(V value)
        {
            releaseInternal(value);
        }

        public Future<?> getLoadFuture(K key)
        {
            return getFutureInternal(loadFutures, key);
        }

        public void setLoadFuture(K key, Future<?> future)
        {
            setFutureInternal(loadFutures, key, future);
        }

        public Future<?> getSaveFuture(K key)
        {
            return getFutureInternal(saveFutures, key);
        }

        public void setSaveFuture(K key, Future<?> future)
        {
            setFutureInternal(saveFutures, key, future);
        }

        public Read.ReadFuture getReadFuture(K key)
        {
            return getFutureInternal(readFutures, key);
        }

        public void setReadFuture(K key, Read.ReadFuture future)
        {
            setFutureInternal(readFutures, key, future);
        }

        public Future<?> getWriteFuture(K key)
        {
            return getFutureInternal(writeFutures, key);
        }

        public void setWriteFuture(K key, Future<?> future)
        {
            setFutureInternal(writeFutures, key, future);
        }

        public void clearWriteFuture(K key)
        {
            // will clear if it's done
            getWriteFuture(key);
        }

        public long cacheQueries()
        {
            return stats.queries;
        }

        public long cacheHits()
        {
            return stats.hits;
        }

        public long cacheMisses()
        {
            return stats.misses;
        }
    }

    public <K, V extends AccordStateCache.AccordState<K, V>> Instance<K, V> instance(Class<K> keyClass,
                                                                                     Class<V> valClass,
                                                                                     Function<K, V> factory)
    {
        Instance<K, V> instance = new Instance<>(keyClass, valClass, factory);
        if (!instances.add(instance))
            throw new IllegalArgumentException(String.format("Cache instances for types %s -> %s already exists",
                                                             keyClass.getName(), valClass.getName()));
        return instance;
    }

    @VisibleForTesting
    int numActiveEntries()
    {
        return active.size();
    }

    @VisibleForTesting
    int numCachedEntries()
    {
        return cache.size();
    }

    @VisibleForTesting
    long bytesCached()
    {
        return bytesCached;
    }

    @VisibleForTesting
    boolean keyIsActive(Object key)
    {
        return active.containsKey(key);
    }

    @VisibleForTesting
    boolean keyIsCached(Object key)
    {
        return cache.containsKey(key);
    }

    @VisibleForTesting
    int references(Object key)
    {
        Node<?, ?> node = active.get(key);
        return node != null ? node.references : 0;
    }

    public long cacheQueries()
    {
        return stats.queries;
    }

    public long cacheHits()
    {
        return stats.hits;
    }

    public long cacheMisses()
    {
        return stats.misses;
    }
}
