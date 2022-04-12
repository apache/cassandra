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

import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * Cache for AccordCommand and AccordCommandsForKey, available memory is shared between them
 *
 * Supports dynamic object sizes
 *  after each acquire/free cycle, the cacheable objects size is recomputed to account for data added/removed during
 *      txn processing
 *
 * Nice to have
 *  cached objects could be asked to reduce their size (unload things unlikely to be used) instead of
 *      immediately evicting
 *  track cache hits, misses, and key contention
 */
public class AccordStateCache
{
    public interface AccordState<K, V extends AccordState<K, V>>
    {
        Node<K, V> createNode();
        K key();
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
            @Override
            public Object key() { return null; }
        }
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

    public final Map<Object, Node<?, ?>> active = new HashMap<>();
    private final Map<Object, Node<?, ?>> cache = new HashMap<>();
    private final Set<Instance<?, ?>> instances = new HashSet<>();

    private final Map<Object, Future<?>> loadFutures = new HashMap<>();
    // TODO: add guards to prevent command changes during command execution/apply

    Node<?, ?> head;
    Node<?, ?> tail;
    private long maxSizeInBytes;
    private long bytesCached = 0;

    public AccordStateCache(long maxSizeInBytes)
    {
        this.maxSizeInBytes = maxSizeInBytes;
    }

    public void maxSizeInBytes(long size)
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

    private Node<?, ?> pop()
    {
        if (tail == null)
            return null;

        Node<?, ?> node = tail;
        if (node == head)
        {
            Preconditions.checkState(node.prev == null);
            head = null;
            tail = null;
        }
        else
        {
            Preconditions.checkState(node.prev != null);
            tail = node.prev;
        }

        if (cache.remove(node.key()) == null)
            throw new IllegalStateException("Popped node was not in cache");

        node.prev = null;
        node.next = null;
        return node;
    }

    private void updateSize(Node<?, ?> node)
    {
        bytesCached += node.sizeDelta();
    }

    private void maybeEvict()
    {
        if (bytesCached <= maxSizeInBytes)
            return;

        while (bytesCached > maxSizeInBytes)
        {
            Node<?, ?> node = pop();
            if (node == null)
                return;

            bytesCached -= node.size();
        }
    }

    /**
     * Should we block and load, or return uninitialized objects to support blind writes?
     * @param key
     * @return
     */
    private <K, V extends AccordStateCache.AccordState<K, V>> V getOrCreateInternal(K key, Function<K, V> factory)
    {
        Node<K, V> node = (Node<K, V>) active.get(key);
        if (node != null)
        {
            node.references++;
            return node.value;
        }

        node = (Node<K, V>) cache.remove(key);

        if (node == null)
        {
            V value = factory.apply(key);
            node = value.createNode();
            updateSize(node);
        }
        else
        {
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
        maybeClearReadFuture(key);
        Node<K, V> node = (Node<K, V>) active.get(key);
        Preconditions.checkState(node != null && node.references > 0);
        Preconditions.checkState(node.value == value);
        if (--node.references == 0)
        {
            active.remove(key);
            cache.put(key, node);
            push(node);
        }

        updateSize(node);
        maybeEvict();
    }

    private static <K> Future<?> getFutureInternal(Map<Object, Future<?>> futuresMap, K key)
    {
        Future<?> r = futuresMap.get(key);
        if (r == null)
            return null;

        if (!r.isDone())
            return r;

        futuresMap.remove(key);
        return null;
    }

    private static <K> void setFutureInternal(Map<Object, Future<?>> futuresMap, K key, Future<?> future)
    {
        Preconditions.checkState(!futuresMap.containsKey(key));
        futuresMap.put(key, future);
    }

    private <K> void maybeClearReadFuture(K key)
    {
        // will clear if it's done
        getFutureInternal(loadFutures, key);
    }

    public class Instance<K, V extends AccordStateCache.AccordState<K, V>>
    {
        private final Class<K> keyClass;
        private final Class<V> valClass;
        private final Function<K, V> factory;

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
            return getOrCreateInternal(key, factory);
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
}
