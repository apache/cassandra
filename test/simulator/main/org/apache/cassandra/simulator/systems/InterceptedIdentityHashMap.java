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

package org.apache.cassandra.simulator.systems;

import java.util.AbstractSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Iterators;

import static org.apache.cassandra.simulator.systems.InterceptorOfSystemMethods.Global.identityHashCode;

/**
 * A class that behaves like IdentityHashMap but uses our deterministically generated
 * {@link InterceptorOfSystemMethods.Global#identityHashCode(Object)}
 *
 * This is used in case we iterate over the contents of any such collection (which we do)
 */
@SuppressWarnings("unused")
public class InterceptedIdentityHashMap<K, V> extends IdentityHashMap<K, V>
{
    static class Key<K>
    {
        final int hash;
        final K key;

        Key(K key)
        {
            this.key = key;
            this.hash = identityHashCode(key);
        }

        K key()
        {
            return key;
        }

        @Override
        public int hashCode()
        {
            return hash;
        }

        @Override
        public boolean equals(Object that)
        {
            return that instanceof Key && key == ((Key<?>)that).key;
        }
    }

    final HashMap<Key<K>, V> wrapped = new HashMap<>();
    public InterceptedIdentityHashMap() {}
    public InterceptedIdentityHashMap(int sizeToIgnore) {}

    @Override
    public int size()
    {
        return wrapped.size();
    }

    @Override
    public boolean isEmpty()
    {
        return wrapped.isEmpty();
    }

    @Override
    public boolean containsValue(Object value)
    {
        return wrapped.containsValue(value);
    }

    @Override
    public boolean containsKey(Object o)
    {
        return wrapped.containsKey(new Key<>(o));
    }

    @Override
    public V put(K key, V value)
    {
        return wrapped.put(new Key<>(key), value);
    }

    @Override
    public V get(Object key)
    {
        return wrapped.get(new Key<>(key));
    }

    @Override
    public V remove(Object key)
    {
        return wrapped.remove(new Key<>(key));
    }

    @Override
    public Set<K> keySet()
    {
        return new AbstractSet<K>()
        {
            @Override
            public Iterator<K> iterator()
            {
                return Iterators.transform(wrapped.keySet().iterator(), Key::key);
            }

            @Override
            public boolean contains(Object o)
            {
                return containsKey(new Key<>(o));
            }

            @Override
            public int size()
            {
                return wrapped.size();
            }
        };
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        return new AbstractSet<Entry<K, V>>()
        {
            @Override
            public Iterator<Entry<K, V>> iterator()
            {
                return Iterators.transform(wrapped.entrySet().iterator(), e -> new SimpleEntry<>(e.getKey().key, e.getValue()));
            }

            @Override
            public int size()
            {
                return wrapped.size();
            }
        };
    }
}
