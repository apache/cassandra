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
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
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
 * @param <K>
 * @param <V>
 */
public class AccordStateCache<K, V>
{
    public interface Weigher<T>
    {
        long weigh(T obj);
    }

    // TODO: consider making intrusive
    static class Node<K, V>
    {
        static final long EMPTY_SIZE = ObjectSizes.measure(new Node(null, null));
        final K key;
        final V value;
        private Node<K, V> prev;
        private Node<K, V> next;
        private boolean active = false;
        private long lastQueriedSize = 0;

        private Node(K key, V value)
        {
            this.key = key;
            this.value = value;
        }

        long size(Weigher<K> keyWeigher, Weigher<V> valueWeigher)
        {
            long result = EMPTY_SIZE + keyWeigher.weigh(key) + valueWeigher.weigh(value);
            lastQueriedSize = result;
            return result;
        }

        long sizeDelta(Weigher<K> keyWeigher, Weigher<V> valueWeigher)
        {
            long prevSize = lastQueriedSize;
            return size(keyWeigher, valueWeigher) - prevSize;
        }
    }

    public final Map<K, Node<K, V>> active = new HashMap<>();
    private final Map<K, Node<K, V>> cache = new HashMap<>();

    Node<K, V> head;
    Node<K, V> tail;
    private final Function<K, V> factory;
    private final Weigher<K> keyWeigher;
    private final Weigher<V> valueWeigher;
    private final long maxSizeInBytes;
    private long bytesCached = 0;

    public AccordStateCache(Function<K, V> factory, Weigher<K> keyWeigher, Weigher<V> valueWeigher, long maxSizeInBytes)
    {
        this.factory = factory;
        this.keyWeigher = keyWeigher;
        this.valueWeigher = valueWeigher;
        this.maxSizeInBytes = maxSizeInBytes;
    }

    private void unlink(Node<K, V> node)
    {
        Node<K, V> prev = node.prev;
        Node<K, V> next = node.next;

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

    private void push(Node<K, V> node)
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

    private Node<K, V> pop()
    {
        if (tail == null)
            return null;

        Node<K, V> node = tail;
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

        if (cache.remove(node.key) == null)
            throw new IllegalStateException("Popped node was not in cache");

        node.prev = null;
        node.next = null;
        return node;
    }

    private void updateSize(Node<K, V> node)
    {
        bytesCached += node.sizeDelta(keyWeigher, valueWeigher);
    }

    private void maybeEvict()
    {
        if (bytesCached <= maxSizeInBytes)
            return;

        while (bytesCached > maxSizeInBytes)
        {
            Node<K, V> node = pop();
            if (node == null)
                return;

            bytesCached -= node.size(keyWeigher, valueWeigher);
        }
    }

    /**
     * Should we block and load, or return uninitialized objects to support blind writes?
     * @param key
     * @return
     */
    public V acquire(K key)
    {
        if (active.containsKey(key))
            return null;

        Node<K, V> node = cache.remove(key);

        if (node == null)
        {
            node = new Node<>(key, factory.apply(key));
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

    public void release(K key, V value)
    {
        Node<K, V> node = active.remove(key);
        Preconditions.checkState(node != null && node.active);
        Preconditions.checkState(node.value == value);
        node.active = false;
        cache.put(key, node);
        push(node);

        updateSize(node);
        maybeEvict();
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
    boolean keyIsActive(K key)
    {
        return active.containsKey(key);
    }

    @VisibleForTesting
    boolean keyIsCached(K key)
    {
        return cache.containsKey(key);
    }
}
