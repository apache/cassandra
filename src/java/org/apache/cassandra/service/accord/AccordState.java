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

package org.apache.cassandra.service.accord;

import java.util.function.Function;

import org.apache.cassandra.service.accord.store.StoredNavigableMap;
import org.apache.cassandra.service.accord.store.StoredSet;
import org.apache.cassandra.utils.concurrent.Future;

public interface AccordState<K, V extends AccordState<K, V>>
{
    K key();

    boolean hasModifications();

    void clearModifiedFlag();

    long estimatedSizeOnHeap();

    interface WriteOnly<K, V extends AccordState<K, V>> extends AccordState<K, V>
    {
        void future(Future<?> future);

        Future<?> future();

        /**
         * Apply the write only changes to the full instance
         */
        void applyChanges(V instance);

        static <T, K extends Comparable<?>, V> void applyMapChanges(T from, T to, Function<T, StoredNavigableMap<K, V>> getMap)
        {
            StoredNavigableMap<K, V> fromMap = getMap.apply(from);

            if (!fromMap.hasModifications())
                return;

            StoredNavigableMap<K, V> toMap = getMap.apply(to);
            fromMap.forEachAddition(toMap::blindPut);
            fromMap.forEachDeletion(toMap::blindRemove);
        }

        static <T, V extends Comparable<?>> void applySetChanges(T from, T to, Function<T, StoredSet<V, ?>> getSet)
        {
            StoredSet<V, ?> fromSet = getSet.apply(from);

            if (!fromSet.hasModifications())
                return;

            StoredSet<V, ?> toSet = getSet.apply(to);
            fromSet.forEachAddition(toSet::blindAdd);
            fromSet.forEachDeletion(toSet::blindRemove);
        }
    }
}
