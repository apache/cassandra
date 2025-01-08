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

package accord.utils;

import java.util.AbstractList;
import java.util.List;
import java.util.RandomAccess;
import java.util.Set;
import java.util.Spliterator;

public interface SortedList<T extends Comparable<? super T>> extends List<T>, Set<T>, RandomAccess
{
    int findNext(int i, Comparable<? super T> find);
    int find(Comparable<? super T> find);
    int find(int from, int to, Comparable<? super T> find);

    default boolean contains(Comparable<? super T> find)
    {
        return find != null && find(find) >= 0;
    }

    default int indexOf(Comparable<? super T> find)
    {
        return find == null ? -1 : Math.max(-1, find(find));
    }

    @Override
    default boolean contains(Object o)
    {
        return o != null && find((Comparable<? super T>) o) >= 0;
    }

    @Override
    default int indexOf(Object o)
    {
        return o == null ? -1 : Math.max(-1, find((Comparable<? super T>) o));
    }

    @Override
    default Spliterator<T> spliterator()
    {
        return List.super.spliterator();
    }

    @Override
    default int lastIndexOf(Object o)
    {
        return indexOf(o);
    }

    default <V> List<V> select(V[] selectFrom, List<T> select)
    {
        return new AbstractList<>()
        {
            @Override
            public V get(int index) { return selectFrom[find(select.get(index))]; }
            @Override
            public int size() { return select.size(); }
        };
    }

}
