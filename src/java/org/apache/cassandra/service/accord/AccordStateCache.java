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

/**
 * Cache for AccordCommand and AccordCommandsForKey
 *
 * Caching
 *  caches up to a configurable amount of resources. Implements an intrusive LRU cache, where the cacheable objects
 *      implement the linked list node structure used by
 *
 * Supports dynamic object sizes
 *  after each acquire/free cycle, the cacheable objects size is recomputed to account for data added/removed during
 *      txn processing
 *
 * Locking
 *  should handle locking of objects
 *  should handle batch locking where all are locked, or none are (maybe?) If something is contended, we should avoid
 *
 * Loading
 *  should return futures or something so loading doesn't
 *      (a) block the thread or
 *      (b) hold up operations with common keys that are ready to execute
 *
 * Nice to have
 *  commands and commands for keys could share a common resource pool, but appear as 2 separate stores
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
        private boolean active = false;
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
    private <K, V extends AccordStateCache.AccordState<K, V>> V acquireInternal(K key, Function<K, V> factory)
    {
        if (active.containsKey(key))
            return null;

        Node<K, V> node = (Node<K, V>) cache.remove(key);

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

        Preconditions.checkState(!node.active);
        maybeEvict();

        node.active = true;
        active.put(key, node);

        return node.value;
    }

    private <K, V extends AccordStateCache.AccordState<K, V>> void releaseInternal(V value)
    {
        K key = value.key();
        Node<K, V> node = (Node<K, V>) active.remove(key);
        Preconditions.checkState(node != null && node.active);
        Preconditions.checkState(node.value == value);
        node.active = false;
        cache.put(key, node);
        push(node);

        updateSize(node);
        maybeEvict();
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

        /**
         * Should we block and load, or return uninitialized objects to support blind writes?
         * @param key
         * @return
         */
        public V acquire(K key)
        {
            return acquireInternal(key, factory);
        }

        public void release(V value)
        {
            releaseInternal(value);
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
}
