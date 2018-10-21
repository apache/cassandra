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

package org.apache.cassandra.locator;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

@VisibleForTesting
public abstract class ReplicaMultimap<K, C extends ReplicaCollection<?>>
{
    final Map<K, C> map;
    ReplicaMultimap(Map<K, C> map)
    {
        this.map = map;
    }

    public abstract C get(K key);

    public static abstract class Builder
            <K, B extends ReplicaCollection.Builder<?>>

    {
        protected abstract B newBuilder(K key);

        final Map<K, B> map;
        Builder()
        {
            this.map = new HashMap<>();
        }

        public B get(K key)
        {
            Preconditions.checkNotNull(key);
            return map.computeIfAbsent(key, k -> newBuilder(key));
        }

        public B getIfPresent(K key)
        {
            Preconditions.checkNotNull(key);
            return map.get(key);
        }

        public void put(K key, Replica replica)
        {
            Preconditions.checkNotNull(key);
            Preconditions.checkNotNull(replica);
            get(key).add(replica);
        }
    }

    public Iterable<Replica> flattenValues()
    {
        return Iterables.concat(map.values());
    }

    public Iterable<Map.Entry<K, Replica>> flattenEntries()
    {
        return () -> {
            Stream<Map.Entry<K, Replica>> s = map.entrySet()
                                                 .stream()
                                                 .flatMap(entry -> entry.getValue()
                                                                        .stream()
                                                                        .map(replica -> (Map.Entry<K, Replica>)new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), replica)));
            return s.iterator();
        };
    }

    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    public boolean containsKey(Object key)
    {
        return map.containsKey(key);
    }

    public Set<K> keySet()
    {
        return map.keySet();
    }

    public Set<Map.Entry<K, C>> entrySet()
    {
        return map.entrySet();
    }

    public Map<K, C> asMap()
    {
        return map;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaMultimap<?, ?> that = (ReplicaMultimap<?, ?>) o;
        return Objects.equals(map, that.map);
    }

    public int hashCode()
    {
        return map.hashCode();
    }

    @Override
    public String toString()
    {
        return map.toString();
    }
}
