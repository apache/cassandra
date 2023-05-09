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

import java.util.Comparator;

/**
 * Converts any comparator to a comparator that never treats distinct objects as equal,
 * even if the original comparator considers them equal.
 * For all other items, the order of the original comparator is preserved.
 * Allows to store duplicate items in sorted sets.
 */
public class UniqueComparator<T> implements Comparator<T>
{
    private final Comparator<T> comparator;

    public UniqueComparator(Comparator<T> comparator)
    {
        this.comparator = comparator;
    }

    @Override
    public int compare(T o1, T o2)
    {
        int result = comparator.compare(o1, o2);
        if (result == 0 && o1 != o2)
        {
            // If the wrapped comparator considers the items equal,
            // but they are not actually the same object, distinguish them
            return System.identityHashCode(o1) - System.identityHashCode(o2);
        }
        return result;
    }
}
