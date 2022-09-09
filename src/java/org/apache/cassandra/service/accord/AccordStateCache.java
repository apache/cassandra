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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Read;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

/**
 * Cache for AccordCommand and AccordCommandsForKey, available memory is shared between the two object types.
 *
 * Supports dynamic object sizes. After each acquire/free cycle, the cacheable objects size is recomputed to
 * account for data added/removed during txn processing if it's modified flag is set
 *
 * TODO: explain how items move to and from the active pool and are evicted
 */
public class AccordStateCache
{
    private static final Logger logger = LoggerFactory.getLogger(AccordStateCache.class);

    private static class WriteOnlyGroup<K, V extends AccordState<K>>
    {
        private boolean locked = false;
        private List<AccordState.WriteOnly<K, V>> items = new ArrayList<>();

        @Override
        public String toString()
        {
            return "WriteOnlyGroup{" +
                   "locked=" + locked +
                   ", items=" + items +
                   '}';
        }

        void lock()
        {
            locked = true;
        }

        void add(AccordState.WriteOnly<K, V> item)
        {
            items.add(item);
        }

        void purge()
        {
            if (locked)
                return;

            while (!items.isEmpty())
            {
                AccordState.WriteOnly<K, V> item = items.get(0);

                // we can't remove items out of order, so if we encounter a write is still pending, we stop
                if (item.future() == null || !item.future().isDone())
                    break;

                items.remove(0);
            }
        }

        boolean isEmpty()
        {
            return items.isEmpty();
        }
    }

    static class Node<K, V extends AccordState<K>>
    {
        static final long EMPTY_SIZE = ObjectSizes.measure(new AccordStateCache.Node<>(null));

        final V value;
        private Node<?, ?> prev;
        private Node<?, ?> next;
        private int references = 0;
        private long lastQueriedSize = 0;

        Node(V value)
        {
            this.value = value;
        }

        long size()
        {
            long result = EMPTY_SIZE + value.estimatedSizeOnHeap();
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

    private static class NamedMap<K, V> extends HashMap<K, V>
    {
        final String name;

        public NamedMap(String name)
        {
            this.name = name;
        }
    }

    public final Map<Object, Node<?, ?>> active = new HashMap<>();
    private final Map<Object, Node<?, ?>> cache = new HashMap<>();
    private final Map<Object, WriteOnlyGroup<?, ?>> pendingWriteOnly = new HashMap<>();
    private final Set<Instance<?, ?>> instances = new HashSet<>();

    private final NamedMap<Object, Future<?>> loadFutures = new NamedMap<>("loadFutures");
    private final NamedMap<Object, Future<?>> saveFutures = new NamedMap<>("saveFutures");

    private final NamedMap<Object, Read.ReadFuture> readFutures = new NamedMap<>("readFutures");
    private final NamedMap<Object, Future<?>> writeFutures = new NamedMap<>("writeFutures");

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

    // don't evict if there's an outstanding save future. If an item is evicted then reloaded
    // before it's mutation is applied, out of date info will be loaded
    private boolean canEvict(Object key)
    {
        Future<?> future = getFuture(saveFutures, key);
        return future == null || future.isDone();
    }

    private void maybeEvict()
    {
        if (bytesCached <= maxSizeInBytes)
            return;

        Node<?, ?> current = tail;
        while (current != null && bytesCached > maxSizeInBytes)
        {
            Node<?, ?> evict = current;
            current = current.prev;
            if (!canEvict(evict.key()))
                continue;

            logger.trace("Evicting {} {}", evict.value.getClass().getSimpleName(), evict.key());
            unlink(evict);
            cache.remove(evict.key());
            bytesCached -= evict.size();
        }
    }

    private static <K, F extends Future<?>> F getFuture(NamedMap<Object, F> futuresMap, K key)
    {
        F r = futuresMap.get(key);
        if (r == null)
            return null;

        if (!r.isDone())
            return r;

        if (logger.isTraceEnabled())
            logger.trace("Clearing future for {} from {}: {}", key, futuresMap.name, r);
        futuresMap.remove(key);
        return null;
    }

    private static <K, F extends Future<?>> void setFuture(Map<Object, F> futuresMap, K key, F future)
    {
        Preconditions.checkState(!futuresMap.containsKey(key));
        futuresMap.put(key, future);
    }

    private static <K> void mergeFuture(Map<Object, Future<?>> futuresMap, K key, Future<?> future)
    {
        Future<?> existing = futuresMap.get(key);
        if (existing != null && !existing.isDone())
        {
            logger.trace("Merging future {} with existing {}", future, existing);
            future = FutureCombiner.allOf(ImmutableList.of(existing, future));
        }

        futuresMap.put(key, future);
    }

    private <K> void maybeClearFuture(K key)
    {
        // will clear if it's done
        getFuture(loadFutures, key);
        getFuture(saveFutures, key);
        getFuture(readFutures, key);
        getFuture(writeFutures, key);
    }

    public class Instance<K, V extends AccordState<K>>
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

        private V getOrCreate(K key, boolean createIfAbsent)
        {
            stats.queries++;
            AccordStateCache.this.stats.queries++;

            Node<K, V> node = (Node<K, V>) active.get(key);
            if (node != null)
            {
                stats.hits++;
                AccordStateCache.this.stats.hits++;
                node.references++;
                return node.value;
            }

            node = (Node<K, V>) cache.remove(key);

            if (node == null)
            {
                stats.misses++;
                AccordStateCache.this.stats.misses++;
                if (!createIfAbsent)
                    return null;
                V value = factory.apply(key);
                node = new Node<>(value);
                updateSize(node);
            }
            else
            {
                stats.hits++;
                AccordStateCache.this.stats.hits++;
                unlink(node);
            }

            Preconditions.checkState(node.references == 0);
            maybeEvict();

            node.references++;
            active.put(key, node);

            return node.value;
        }

        public V getOrCreate(K key)
        {
            return getOrCreate(key, true);
        }

        public V getOrNull(K key)
        {
            return getOrCreate(key, false);
        }

        public void release(V value)
        {
            K key = value.key();
            logger.trace("Releasing resources for {}: {}", key, value);
            maybeClearFuture(key);
            Node<K, V> node = (Node<K, V>) active.get(key);
            Preconditions.checkState(node != null && node.references > 0);
            Preconditions.checkState(node.value == value);
            if (--node.references == 0)
            {
                logger.trace("Moving {} from active pool to cache", key);
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

        @VisibleForTesting
        boolean canEvict(K key)
        {
            return AccordStateCache.this.canEvict(key);
        }

        @VisibleForTesting
        boolean writeOnlyGroupIsLocked(K key)
        {
            WriteOnlyGroup<K, V> group = (WriteOnlyGroup<K, V>) pendingWriteOnly.get(key);
            return group != null && group.locked;
        }

        @VisibleForTesting
        int pendingWriteOnlyOperations(K key)
        {
            WriteOnlyGroup<K, V> group = (WriteOnlyGroup<K, V>) pendingWriteOnly.get(key);
            return group != null ? group.items.size() : 0;
        }

        public void lockWriteOnlyGroupIfExists(K key)
        {
            WriteOnlyGroup<K, V> group = (WriteOnlyGroup<K, V>) pendingWriteOnly.get(key);
            if (group == null)
                return;

            logger.trace("Locking write only group for {} ({})", key, group);
            group.purge();
            if (!group.isEmpty())
                group.lock();
        }

        public void applyAndRemoveWriteOnlyGroup(V instance)
        {
            WriteOnlyGroup<K, V> group = (WriteOnlyGroup<K, V>) pendingWriteOnly.remove(instance.key());
            if (group == null)
                return;

            logger.trace("Applying and removing write only group for {} ({})", instance.key(), group);
            for (AccordState.WriteOnly<K, V> writeOnly : group.items)
            {
                writeOnly.applyChanges(instance);
                if (!writeOnly.future().isDone())
                    mergeFuture(saveFutures, instance.key(), writeOnly.future());
            }
        }

        public void addWriteOnly(AccordState.WriteOnly<K, V> writeOnly)
        {
            K key = writeOnly.key();
            Preconditions.checkArgument(writeOnly.future() != null);
            WriteOnlyGroup<K, V> group = (WriteOnlyGroup<K, V>) pendingWriteOnly.computeIfAbsent(key, k -> new WriteOnlyGroup<>());

            // if a load future exists for the key we're creating a write group for, we need to lock
            // the group so the loading instance gets changes applied when it finishes loading
            if (getLoadFuture(key) != null)
                group.lock();

            group.add(writeOnly);
        }

        public void purgeWriteOnly(K key)
        {
            WriteOnlyGroup<?, ?> items = pendingWriteOnly.get(key);
            if (items == null)
                return;

            items.purge();
            if (items.isEmpty())
                pendingWriteOnly.remove(key);
        }

        public Future<?> getLoadFuture(K key)
        {
            return getFuture(loadFutures, key);
        }

        public void setLoadFuture(K key, Future<?> future)
        {
            setFuture(loadFutures, key, future);
        }

        public Future<?> getSaveFuture(K key)
        {
            return getFuture(saveFutures, key);
        }

        public void addSaveFuture(K key, Future<?> future)
        {
            logger.trace("Adding save future for {}: {}", key, future);
            mergeFuture(saveFutures, key, future);
        }

        public Read.ReadFuture getReadFuture(K key)
        {
            return getFuture(readFutures, key);
        }

        public void setReadFuture(K key, Read.ReadFuture future)
        {
            setFuture(readFutures, key, future);
        }

        public Future<?> getWriteFuture(K key)
        {
            return getFuture(writeFutures, key);
        }

        public void setWriteFuture(K key, Future<?> future)
        {
            setFuture(writeFutures, key, future);
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

    public <K, V extends AccordState<K>> Instance<K, V> instance(Class<K> keyClass, Class<V> valClass, Function<K, V> factory)
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
