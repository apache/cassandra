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

package org.apache.cassandra.tcm.ownership;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.schema.ReplicationParams;

public abstract class ReplicationMap<T> implements Iterable<Map.Entry<ReplicationParams, T>>
{
    private final Map<ReplicationParams, T> map;

    protected ReplicationMap()
    {
        this(Collections.emptyMap());
    }

    protected ReplicationMap(Map<ReplicationParams, T> map)
    {
        this.map = map;
    }

    protected abstract T defaultValue();
    protected abstract T localOnly();

    public T get(ReplicationParams params)
    {
        if (params.isLocal())
            return localOnly();
        return map.getOrDefault(params, defaultValue());
    }

    public int size()
    {
        return map.size();
    }

    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    public void forEach(BiConsumer<ReplicationParams, T> consumer)
    {
        for (Map.Entry<ReplicationParams, T> entry : this)
            consumer.accept(entry.getKey(), entry.getValue());
    }

    public ImmutableMap<ReplicationParams, T> asMap()
    {
        return ImmutableMap.copyOf(map);
    }

    public Set<ReplicationParams> keys()
    {
        return map.keySet();
    }

    public Iterator<Map.Entry<ReplicationParams, T>> iterator()
    {
        return map.entrySet().iterator();
    }

    public Stream<Map.Entry<ReplicationParams, T>> stream()
    {
        return StreamSupport.stream(spliterator(), false);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicationMap<?> that = (ReplicationMap<?>) o;
        return map.equals(that.map);
    }

    public int hashCode()
    {
        return Objects.hash(map);
    }
}
