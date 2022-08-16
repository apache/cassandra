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
import java.util.Objects;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.api.KeyRange;
import accord.topology.KeyRanges;
import accord.txn.Keys;
import org.apache.cassandra.service.accord.api.AccordKey;

public abstract class AbstractKeyIndexed<T extends AccordKey>
{
    private final Keys keys;
    final List<T> items;

    private static <T extends AccordKey> Keys extractKeys(List<T> items)
    {
        Key[] keys = new Key[items.size()];
        for (int i=0, mi=items.size(); i<mi; i++)
        {
            keys[i] = items.get(i);
            Preconditions.checkState(i == 0 || keys[i].compareTo(keys[i-1]) > 0);
        }
        return new Keys(keys);
    }

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
        return keys.equals(that.keys) && items.equals(that.items);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keys, items);
    }

    public AbstractKeyIndexed(List<T> items)
    {
        this(extractKeys(items), items);
    }

    public AbstractKeyIndexed(Keys keys, List<T> items)
    {
        Preconditions.checkArgument(keys.size() == items.size());
        this.keys = keys;
        this.items = items;
    }

    void forEachIntersecting(KeyRanges ranges, Consumer<T> consumer)
    {
        for (int k=0, mk=ranges.size(); k<mk; k++)
        {
            KeyRange<?> range = ranges.get(k);
            int lowIdx = range.lowKeyIndex(keys);
            if (lowIdx < -keys.size())
                return;
            if (lowIdx < 0)
                continue;
            for (int i = lowIdx, limit = range.higherKeyIndex(keys) ; i < limit ; ++i)
                consumer.accept(items.get(i));
        }
    }

    private int findFirst(Key key)
    {
        if (keys.isEmpty()) return -1;

        int i = keys.search(0, keys.size(), key,
                            (k, r) -> ((Key) r).compareTo((Key) k) < 0 ? -1 : 1);

        int minIdx = -1 - i;

        return minIdx < keys.size() ? minIdx : i;
    }

    void forEachIntersecting(AccordKey key, Consumer<T> consumer)
    {
        // there may be multiple items for a given key, so start with the first
        int idx = findFirst(key);
        if (idx < 0)
            return;

        for (;idx<keys.size(); idx++)
        {
            T item = items.get(idx);
            if (AccordKey.compare(key, item) != 0)
                return;
            consumer.accept(item);
        }
    }
}
