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

package org.apache.cassandra.service.accord.db;

import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Preconditions;

import org.apache.cassandra.service.accord.api.AccordKey;

public abstract class AbstractKeyIndexed<T>
{
    final NavigableMap<AccordKey, T> items;

    @Override
    public String toString()
    {
        return items.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractKeyIndexed<?> that = (AbstractKeyIndexed<?>) o;
        return items.equals(that.items);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(items);
    }

    public AbstractKeyIndexed(List<T> items, Function<T, AccordKey> keyFunction)
    {
        this.items = new TreeMap<>();
        for (int i=0, mi=items.size(); i<mi; i++)
        {
            T item = items.get(i);
            AccordKey key = keyFunction.apply(item);
            // TODO: support multiple reads/writes per key
            Preconditions.checkArgument(!this.items.containsKey(key));
            this.items.put(key, item);
        }
    }

    public AbstractKeyIndexed(NavigableMap<AccordKey, T> items)
    {
        this.items = items;
    }

    void forEachIntersecting(AccordKey key, Consumer<T> consumer)
    {
        T item = items.get(key);
        if (item == null)
            return;

        consumer.accept(item);
    }
}
