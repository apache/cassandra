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
package org.apache.cassandra.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * A variant of BiMap that permits concurrent access, and expects uniqueness of values in both domain and range.
 * We synchronize on _modifications only_, and use ConcurrentHashMap so that readers can lookup safely. This does mean there
 * could be races to lookup the inverse, but we aren't too worried about that.
 *
 * @param <K>
 * @param <V>
 */
public class ConcurrentBiMap<K, V> implements Map<K, V>
{
    protected final Map<K, V> forwardMap;
    protected final Map<V, K> reverseMap;

    public ConcurrentBiMap()
    {
        this(new ConcurrentHashMap<K, V>(16, 0.5f, 1), new ConcurrentHashMap<V, K>(16, 0.5f, 1));
    }

    protected ConcurrentBiMap(Map<K, V> forwardMap, Map<V, K> reverseMap)
    {
        this.forwardMap = forwardMap;
        this.reverseMap = reverseMap;
    }

    public Map<V, K> inverse()
    {
        return Collections.unmodifiableMap(reverseMap);
    }

    public void clear()
    {
        forwardMap.clear();
        reverseMap.clear();
    }

    public boolean containsKey(Object key)
    {
        return forwardMap.containsKey(key);
    }

    public boolean containsValue(Object value)
    {
        return reverseMap.containsKey(value);
    }

    public Set<Entry<K, V>> entrySet()
    {
        return forwardMap.entrySet();
    }

    public V get(Object key)
    {
        return forwardMap.get(key);
    }

    public boolean isEmpty()
    {
        return forwardMap.isEmpty();
    }

    public Set<K> keySet()
    {
        return forwardMap.keySet();
    }

    public synchronized V put(K key, V value)
    {
        K oldKey = reverseMap.get(value);
        if (oldKey != null && !key.equals(oldKey))
            throw new IllegalArgumentException(value + " is already bound in reverseMap to " + oldKey);
        V oldVal = forwardMap.put(key, value);
        if (oldVal != null && !Objects.equals(reverseMap.remove(oldVal), key))
            throw new IllegalStateException(); // for the prior mapping to be correct, we MUST get back the key from the reverseMap
        reverseMap.put(value, key);
        return oldVal;
    }

    public synchronized void putAll(Map<? extends K, ? extends V> m)
    {
        for (Entry<? extends K, ? extends V> entry : m.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    public synchronized V remove(Object key)
    {
        V oldVal = forwardMap.remove(key);
        if (oldVal == null)
            return null;
        Object oldKey = reverseMap.remove(oldVal);
        if (oldKey == null || !oldKey.equals(key))
            throw new IllegalStateException(); // for the prior mapping to be correct, we MUST get back the key from the reverseMap
        return oldVal;
    }

    public int size()
    {
        return forwardMap.size();
    }

    public Collection<V> values()
    {
        return reverseMap.keySet();
    }
}
