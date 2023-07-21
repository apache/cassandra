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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.checkerframework.checker.mustcall.qual.CreatesMustCallFor;
import org.checkerframework.checker.mustcall.qual.InheritableMustCall;
import org.checkerframework.checker.mustcall.qual.MustCallAlias;
import org.checkerframework.checker.mustcall.qual.NotOwning;
import org.checkerframework.checker.mustcall.qual.Owning;

@InheritableMustCall({ "removeAll" })
public class ResourcesMap<K, V>
{
    private final Map<K, V> map;

    public ResourcesMap()
    {
        this.map = new HashMap<>();
    }

    public @NotOwning V get(K key)
    {
        return map.get(key);
    }

    public @Owning V remove(K key)
    {
        return map.remove(key);
    }

    @CreatesMustCallFor
    public @NotOwning V put(K key, @Owning V value)
    {
        return map.put(key, value);
    }

    public void forEach(Consumer<@MustCallAlias V> consumer)
    {
        map.values().forEach(consumer);
    }

    public <T> void removeAll(T initial, OwningBiFunction<T, V> consumer)
    {
        Iterator<V> iterator = map.values().iterator();
        T acc = initial;
        while (iterator.hasNext())
        {
            V value = iterator.next();
            try
            {
                iterator.remove();
            }
            finally
            {
                acc = consumer.apply(acc, value);
            }
        }
    }

    public <T> void removeAll(OwningConsumer<V> consumer)
    {
        Iterator<V> iterator = map.values().iterator();
        while (iterator.hasNext())
        {
            V value = iterator.next();
            try
            {
                iterator.remove();
            }
            finally
            {
                consumer.accept(value);
            }
        }
    }

    public interface OwningConsumer<V> extends Consumer<V>
    {
        @Override
        void accept(@Owning V v);
    }

    public interface OwningBiFunction<T, V> extends BiFunction<T, V, T>
    {
        @Override
        T apply(T t, @Owning V v);
    }
}
