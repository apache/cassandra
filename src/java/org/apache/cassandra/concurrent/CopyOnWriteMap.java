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

package org.apache.cassandra.concurrent;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;

public class CopyOnWriteMap<K, V> extends AbstractMap<K, V>
{
    private volatile Map<K, V> map = ImmutableMap.of();

    // Read only Access
    @Override
    public Set<Entry<K, V>> entrySet()
    {
        return map.entrySet();
    }

    @Override
    public int size()
    {
        return map.size();
    }

    @Override
    public boolean containsValue(Object value)
    {
        return map.containsValue(value);
    }

    @Override
    public boolean containsKey(Object key)
    {
        return map.containsKey(key);
    }

    @Override
    public V get(Object key)
    {
        return map.get(key);
    }

    @Override
    public V getOrDefault(Object key, V defaultValue)
    {
        return map.getOrDefault(key, defaultValue);
    }

    @Override
    public Set<K> keySet()
    {
        return map.keySet();
    }

    @Override
    public Collection<V> values()
    {
        return map.values();
    }

    @Override
    public boolean equals(Object o)
    {
        return map.equals(o);
    }

    @Override
    public int hashCode()
    {
        return map.hashCode();
    }

    @Override
    public String toString()
    {
        return map.toString();
    }

    // Write Access; must guard with synchronized
    @Override
    public synchronized V remove(Object key)
    {
        Objects.requireNonNull(key);
        if (!containsKey(key))
            return null;
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        V current = null;
        for (Entry<K, V> e : map.entrySet())
        {
            if (Objects.equals(key, e.getKey()))
            {
                current = e.getValue();
                continue;
            }
            builder.put(e.getKey(), e.getValue());
        }
        map = builder.build();
        return current;
    }

    @Override
    public synchronized void clear()
    {
        map = ImmutableMap.of();
    }

    public synchronized Map<K, V> getAndClear()
    {
        Map<K, V> backup = map;
        map = ImmutableMap.of();
        return backup;
    }

    @Override
    public synchronized V put(K key, V value)
    {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        V previous = null;
        for (Entry<K, V> e : map.entrySet())
        {
            if (Objects.equals(key, e.getKey()))
            {
                previous = e.getValue();
                continue;
            }
            builder.put(e.getKey(), e.getValue());
        }
        builder.put(key, value);
        map = builder.build();
        return previous;
    }

    @Override
    public synchronized void putAll(Map<? extends K, ? extends V> m)
    {
        Objects.requireNonNull(m);
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        for (Entry<K, V> e : map.entrySet())
        {
            if (m.containsKey(e.getKey())) continue;
            builder.put(e.getKey(), e.getValue());
        }
        builder.putAll(m);
        map = builder.build();
    }

    @Override
    public synchronized void replaceAll(BiFunction<? super K, ? super V, ? extends V> function)
    {
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        for (Entry<K, V> e : map.entrySet())
        {
            V v = function.apply(e.getKey(), e.getValue());
            builder.put(e.getKey(), v);
        }
        this.map = builder.build();
    }

    // below methods can delegate to the interface, but must get the lock first

    @Override
    public synchronized V putIfAbsent(K key, V value)
    {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        return super.putIfAbsent(key, value);
    }

    @Override
    public synchronized V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction)
    {
        Objects.requireNonNull(key);
        Objects.requireNonNull(mappingFunction);
        return super.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public synchronized boolean remove(Object key, Object value)
    {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        return super.remove(key, value);
    }

    @Override
    public synchronized boolean replace(K key, V oldValue, V newValue)
    {
        Objects.requireNonNull(key);
        Objects.requireNonNull(oldValue);
        Objects.requireNonNull(newValue);
        return super.replace(key, oldValue, newValue);
    }

    @Override
    public synchronized V replace(K key, V value)
    {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        return super.replace(key, value);
    }

    @Override
    public synchronized V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction)
    {
        Objects.requireNonNull(key);
        return super.computeIfPresent(key, remappingFunction);
    }

    @Override
    public synchronized V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction)
    {
        Objects.requireNonNull(key);
        return super.compute(key, remappingFunction);
    }

    @Override
    public synchronized V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction)
    {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        return super.merge(key, value, remappingFunction);
    }

}
