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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

public abstract class AbstractBiMultiValMap<K, V> implements Map<K, V>
{
    protected abstract Map<K, V> forwardDelegate();
    protected abstract Multimap<V, K> reverseDelegate();

    public Multimap<V, K> inverse()
    {
        return Multimaps.unmodifiableMultimap(reverseDelegate());
    }

    public void clear()
    {
        forwardDelegate().clear();
        reverseDelegate().clear();
    }

    public boolean containsKey(Object key)
    {
        return forwardDelegate().containsKey(key);
    }

    public boolean containsValue(Object value)
    {
        return reverseDelegate().containsKey(value);
    }

    public Set<Entry<K, V>> entrySet()
    {
        return forwardDelegate().entrySet();
    }

    public V get(Object key)
    {
        return forwardDelegate().get(key);
    }

    public boolean isEmpty()
    {
        return forwardDelegate().isEmpty();
    }

    public Set<K> keySet()
    {
        return forwardDelegate().keySet();
    }

    public V put(K key, V value)
    {
        V oldVal = forwardDelegate().put(key, value);
        if (oldVal != null)
            reverseDelegate().remove(oldVal, key);
        reverseDelegate().put(value, key);
        return oldVal;
    }

    public void putAll(Map<? extends K, ? extends V> m)
    {
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    public V remove(Object key)
    {
        V oldVal = forwardDelegate().remove(key);
        reverseDelegate().remove(oldVal, key);
        return oldVal;
    }

    public Collection<K> removeValue(V value)
    {
        Collection<K> keys = reverseDelegate().removeAll(value);
        for (K key : keys)
            forwardDelegate().remove(key);
        return keys;
    }

    public int size()
    {
        return forwardDelegate().size();
    }

    public Collection<V> values()
    {
        return reverseDelegate().keys();
    }

    public Collection<V> valueSet()
    {
        return reverseDelegate().keySet();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof AbstractBiMultiValMap)) return false;
        AbstractBiMultiValMap<?, ?> that = (AbstractBiMultiValMap<?, ?>) o;
        return forwardDelegate().equals(that.forwardDelegate()) && reverseDelegate().equals(that.reverseDelegate());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(forwardDelegate(), reverseDelegate());
    }
}
