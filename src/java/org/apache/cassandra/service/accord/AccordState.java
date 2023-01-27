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

import java.util.function.BiConsumer;
import java.util.function.Function;

import accord.utils.async.AsyncResult;
import org.apache.cassandra.service.accord.store.StoredNavigableMap;
import org.apache.cassandra.service.accord.store.StoredSet;

public interface AccordState<K>
{
    enum ReadWrite { FULL, WRITE_ONLY, READ_ONLY }

    K key();

    boolean hasModifications();

    void clearModifiedFlag();

    boolean isEmpty();

    boolean isLoaded();

    long estimatedSizeOnHeap();

    default ReadWrite rw()
    {
        return ReadWrite.FULL;
    }

    default boolean isFullInstance()
    {
        return rw() == ReadWrite.FULL;
    }

    default boolean isWriteOnlyInstance()
    {
        return rw() == ReadWrite.WRITE_ONLY;
    }

    default boolean isReadOnlyInstance()
    {
        return rw() == ReadWrite.READ_ONLY;
    }

    interface WriteOnly<K, V extends AccordState<K>> extends AccordState<K>
    {
        @Override
        default ReadWrite rw()
        {
            return ReadWrite.WRITE_ONLY;
        }

        void asyncResult(AsyncResult<Void> notifier);

        AsyncResult<Void> asyncResult();

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
            fromMap.forEachDeletion((BiConsumer<K, V>) toMap::blindRemove);
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
