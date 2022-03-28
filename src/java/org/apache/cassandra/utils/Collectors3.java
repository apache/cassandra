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

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Some extra Collector implementations.
 * <p>
 * Named Collectors3 just in case Guava ever makes a Collectors2
 */
public class Collectors3
{
    private static final Collector.Characteristics[] LIST_CHARACTERISTICS = new Collector.Characteristics[]{};

    public static <T> Collector<T, ?, ImmutableList<T>> toImmutableList()
    {
        return Collector.of(ImmutableList.Builder<T>::new,
                            ImmutableList.Builder<T>::add,
                            (l, r) -> l.addAll(r.build()),
                            ImmutableList.Builder<T>::build,
                            LIST_CHARACTERISTICS);
    }

    private static final Collector.Characteristics[] SET_CHARACTERISTICS = new Collector.Characteristics[]{ Collector.Characteristics.UNORDERED };

    public static <T> Collector<T, ?, ImmutableSet<T>> toImmutableSet()
    {
        return Collector.of(ImmutableSet.Builder<T>::new,
                            ImmutableSet.Builder<T>::add,
                            (l, r) -> l.addAll(r.build()),
                            ImmutableSet.Builder<T>::build,
                            SET_CHARACTERISTICS);
    }

    private static final Collector.Characteristics[] MAP_CHARACTERISTICS = new Collector.Characteristics[]{ Collector.Characteristics.UNORDERED };

    public static <K, V> Collector<Map.Entry<K, V>, ?, ImmutableMap<K, V>> toImmutableMap()
    {
        return Collector.of(ImmutableMap.Builder<K, V>::new,
                            ImmutableMap.Builder<K, V>::put,
                            (l, r) -> l.putAll(r.build()),
                            ImmutableMap.Builder<K, V>::build,
                            MAP_CHARACTERISTICS);
    }

    public static <T, K, V> Collector<T, ?, ImmutableMap<K, V>> toImmutableMap(Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends V> valueMapper)
    {
        return Collector.of(ImmutableMap.Builder<K, V>::new,
                            (b, t) -> b.put(keyMapper.apply(t), valueMapper.apply(t)),
                            (l, r) -> l.putAll(r.build()),
                            ImmutableMap.Builder::build,
                            MAP_CHARACTERISTICS);
    }

}
