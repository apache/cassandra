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
import java.util.Objects;

public class MutableEntry<K, V> implements Map.Entry<K, V>
{
    private final K k;
    private V v;

    public MutableEntry(K k, V v)
    {
        this.k = k;
        this.v = v;
    }

    @Override
    public K getKey()
    {
        return k;
    }

    @Override
    public V getValue()
    {
        return v;
    }

    @Override
    public V setValue(V value)
    {
        V previous = v;
        v = Objects.requireNonNull(value);
        return previous;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || !(o instanceof Map.Entry)) return false;
        Map.Entry<?, ?> that = (Map.Entry<?, ?>) o;
        return Objects.equals(k, that.getKey()) && Objects.equals(v, that.getValue());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(k, v);
    }

    @Override
    public String toString()
    {
        return k + "=" + v;
    }
}
