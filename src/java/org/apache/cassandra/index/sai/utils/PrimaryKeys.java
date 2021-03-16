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
package org.apache.cassandra.index.sai.utils;

import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Stream;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * A sorted set of {@link PrimaryKey}s.
 *
 * The primary keys are sorted first by token, then by partition key value, and then by clustering.
 */
public interface PrimaryKeys extends Iterable<PrimaryKey>
{
    // from https://github.com/gaul/java-collection-overhead
    long SET_ENTRY_OVERHEAD = 36;
    long MAP_ENTRY_OVERHEAD = 36;

    /**
     * Returns a new empty {@link PrimaryKey} sorted set using the specified clustering comparator.
     *
     * @param clusteringComparator a clustering comparator
     * @return a new empty primary key set
     */
    static PrimaryKeys create(ClusteringComparator clusteringComparator)
    {
        return clusteringComparator.size() == 0 ? new Skinny() : new Wide(clusteringComparator);
    }

    /**
     * Adds the specified {@link PrimaryKey}.
     *
     * @param key a primary key
     */
    default long add(PrimaryKey key)
    {
        return add(key.partitionKey(), key.clustering());
    }

    /**
     * Adds the primary key defined by the specified partition key and clustering.
     *
     * @param key a partition key
     * @param clustering a clustering key
     */
    long add(DecoratedKey key, Clustering clustering);

    /**
     * Returns all the partition keys.
     *
     * @return all the partition keys
     */
    SortedSet<DecoratedKey> partitionKeys();

    /**
     * Returns the number of primary keys.
     *
     * @return the number of primary keys
     */
    int size();

    /**
     * Returns if this primary key set is empty.
     *
     * @return {@code true} if this is empty, {@code false} otherwise
     */
    boolean isEmpty();

    Stream<PrimaryKey> stream();

    long unsharedHeapSize();

    @Override
    @SuppressWarnings("NullableProblems")
    default Iterator<PrimaryKey> iterator()
    {
        return stream().iterator();
    }

    /**
     * A {@link PrimaryKeys} implementation for tables without a defined clustering key,
     * in which case the clustering key is always {@link Clustering#EMPTY}.
     */
    @ThreadSafe
    class Skinny implements PrimaryKeys
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new Skinny());

        public final ConcurrentSkipListSet<DecoratedKey> keys;

        private Skinny()
        {
            this.keys = new ConcurrentSkipListSet<>();
        }

        @Override
        public long add(DecoratedKey key, Clustering clustering)
        {
            assert clustering.isEmpty() : "Expected Clustering.EMPTY but got " + clustering;
            return keys.add(key) ? SET_ENTRY_OVERHEAD : 0;
        }

        @Override
        public SortedSet<DecoratedKey> partitionKeys()
        {
            return keys;
        }

        @Override
        public int size()
        {
            return keys.size();
        }

        @Override
        public boolean isEmpty()
        {
            return keys.isEmpty();
        }

        @Override
        public Stream<PrimaryKey> stream()
        {
            return keys.stream().map(PrimaryKey.Skinny::new);
        }

        @Override
        public long unsharedHeapSize()
        {
            return EMPTY_SIZE;
        }
    }

    /**
     * A {@link PrimaryKeys} implementation for tables with a defined clustering key.
     */
    @ThreadSafe
    class Wide implements PrimaryKeys
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new Wide(null));

        private final ClusteringComparator clusteringComparator;
        private final ConcurrentSkipListMap<DecoratedKey, ConcurrentSkipListSet<Clustering>> keys;

        private Wide(ClusteringComparator clusteringComparator)
        {
            this.clusteringComparator = clusteringComparator;
            this.keys = new ConcurrentSkipListMap<>();
        }

        @Override
        public long add(DecoratedKey key, Clustering clustering)
        {
            long onHeapOverhead = 0;
            ConcurrentSkipListSet<Clustering> keys = this.keys.get(key);

            if (keys == null)
            {
                ConcurrentSkipListSet<Clustering> newKeys = new ConcurrentSkipListSet<>(clusteringComparator);
                keys = this.keys.putIfAbsent(key, newKeys);
                if (keys == null)
                {
                    onHeapOverhead += (ObjectSizes.measure(newKeys) + MAP_ENTRY_OVERHEAD);
                    keys = newKeys;
                }
            }

            return keys.add(clustering) ? onHeapOverhead + SET_ENTRY_OVERHEAD : onHeapOverhead;
        }

        @Override
        public SortedSet<DecoratedKey> partitionKeys()
        {
            return keys.keySet();
        }

        @Override
        public int size()
        {
            return keys.values().stream().mapToInt(Set::size).sum();
        }

        @Override
        public boolean isEmpty()
        {
            return keys.isEmpty();
        }

        @Override
        public Stream<PrimaryKey> stream()
        {
            return keys.entrySet().stream().flatMap(e -> e.getValue().stream().map(c -> PrimaryKey.of(e.getKey(), c)));
        }

        @Override
        public long unsharedHeapSize()
        {
            return EMPTY_SIZE;
        }
    }
}
