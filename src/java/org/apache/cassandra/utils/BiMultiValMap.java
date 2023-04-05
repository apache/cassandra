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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 *
 * A variant of BiMap which does not enforce uniqueness of values. This means the inverse
 * is a Multimap.  (But the "forward" view is not a multimap; keys may only each have one value.)
 *
 * @param <K>
 * @param <V>
 */
public class BiMultiValMap<K, V> implements Map<K, V>
{
    protected final Map<K, V> forwardMap;
    protected final Multimap<V, K> reverseMap;

    public BiMultiValMap()
    {
        this.forwardMap = new HashMap<K, V>();
        this.reverseMap = HashMultimap.<V, K>create();
    }

    protected BiMultiValMap(Map<K, V> forwardMap, Multimap<V, K> reverseMap)
    {
        this.forwardMap = forwardMap;
        this.reverseMap = reverseMap;
    }

    public BiMultiValMap(BiMultiValMap<K, V> map)
    {
        this();
        forwardMap.putAll(map);
        reverseMap.putAll(map.inverse());
    }

    public Multimap<V, K> inverse()
    {
        return Multimaps.unmodifiableMultimap(reverseMap);
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

    public Set<Map.Entry<K, V>> entrySet()
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

    public V put(K key, V value)
    {
        V oldVal = forwardMap.put(key, value);
        if (oldVal != null)
            reverseMap.remove(oldVal, key);
        reverseMap.put(value, key);
        return oldVal;
    }

    public void putAll(Map<? extends K, ? extends V> m)
    {
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    public V remove(Object key)
    {
        V oldVal = forwardMap.remove(key);
        reverseMap.remove(oldVal, key);
        return oldVal;
    }

    public Collection<K> removeValue(V value)
    {
        Collection<K> keys = reverseMap.removeAll(value);
        for (K key : keys)
            forwardMap.remove(key);
        return keys;
    }

    public int size()
    {
        return forwardMap.size();
    }

    public Collection<V> values()
    {
        return reverseMap.keys();
    }

    public Collection<V> valueSet()
    {
        return reverseMap.keySet();
    }
}
