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

import java.util.Objects;

import com.google.common.collect.ImmutableMap;

public class ImmutableUtils
{
    public static <K, V> ImmutableMap<K, V> without(ImmutableMap<K, V> map, K keyToRemove)
    {
        if (map.containsKey(keyToRemove))
        {
            ImmutableMap.Builder<K, V> builder = ImmutableMap.builderWithExpectedSize(map.size() - 1);
            map.forEach((k, v) -> {
                if (!Objects.equals(k, keyToRemove))
                    builder.put(k, v);
            });
            return builder.build();
        }
        return map;
    }

    public static <K, V> ImmutableMap<K, V> withAddedOrUpdated(ImmutableMap<K, V> map, K keyToAdd, V valueToAdd)
    {
        V currentValue = map.get(keyToAdd);
        if (Objects.equals(currentValue, valueToAdd))
            return map;

        ImmutableMap.Builder<K, V> builder;
        if (currentValue != null)
        {
            builder = ImmutableMap.builderWithExpectedSize(map.size());
            map.forEach((k, v) -> {
                if (Objects.equals(k, keyToAdd))
                    builder.put(keyToAdd, valueToAdd);
                else
                    builder.put(k, v);
            });
        }
        else
        {
            builder = ImmutableMap.builderWithExpectedSize(map.size() + 1);
            builder.putAll(map);
            builder.put(keyToAdd, valueToAdd);
        }
        return builder.build();
    }
}
