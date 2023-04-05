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

package org.apache.cassandra.locator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A collection like class for Replica objects. Represents both a well defined order on the contained Replica objects,
 * and efficient methods for accessing the contained Replicas, directly and as a projection onto their endpoints and ranges.
 */
public interface ReplicaCollection<C extends ReplicaCollection<C>> extends Iterable<Replica>
{
    /**
     * @return a Set of the endpoints of the contained Replicas.
     * Iteration order is maintained where there is a 1:1 relationship between endpoint and Replica
     * Typically this collection offers O(1) access methods, and this is true for all but ReplicaList.
     */
    public abstract Set<InetAddressAndPort> endpoints();

    /**
     * @param i a value in the range [0..size())
     * @return the i'th Replica, in our iteration order
     */
    public abstract Replica get(int i);

    /**
     * @return the number of Replica contained
     */
    public abstract int size();

    /**
     * @return true iff size() == 0
     */
    public abstract boolean isEmpty();

    /**
     * @return true iff a Replica in this collection is equal to the provided Replica.
     * Typically this method is expected to take O(1) time, and this is true for all but ReplicaList.
     */
    public abstract boolean contains(Replica replica);

    /**
     * @return the number of replicas that match the predicate
     */
    public abstract int count(Predicate<? super Replica> predicate);

    /**
     * @return a *eagerly constructed* copy of this collection containing the Replica that match the provided predicate.
     * An effort will be made to either return ourself, or a subList, where possible.
     * It is guaranteed that no changes to any upstream Builder will affect the state of the result.
     */
    public abstract C filter(Predicate<? super Replica> predicate);

    /**
     * @return a *eagerly constructed* copy of this collection containing the Replica that match the provided predicate.
     * An effort will be made to either return ourself, or a subList, where possible.
     * It is guaranteed that no changes to any upstream Builder will affect the state of the result.
     * Only the first maxSize items will be returned.
     */
    public abstract C filter(Predicate<? super Replica> predicate, int maxSize);

    /**
     * @return a *lazily constructed* Iterable over this collection, containing the Replica that match the provided predicate.
     */
    public abstract Iterable<Replica> filterLazily(Predicate<? super Replica> predicate);

    /**
     * @return a *lazily constructed* Iterable over this collection, containing the Replica that match the provided predicate.
     * Only the first maxSize matching items will be returned.
     */
    public abstract Iterable<Replica> filterLazily(Predicate<? super Replica> predicate, int maxSize);

    /**
     * @return an *eagerly constructed* copy of this collection containing the Replica at positions [start..end);
     * An effort will be made to either return ourself, or a subList, where possible.
     * It is guaranteed that no changes to any upstream Builder will affect the state of the result.
     */
    public abstract C subList(int start, int end);

    /**
     * @return an *eagerly constructed* copy of this collection containing the Replica re-ordered according to this comparator
     * It is guaranteed that no changes to any upstream Builder will affect the state of the result.
     */
    public abstract C sorted(Comparator<? super Replica> comparator);

    public abstract Iterator<Replica> iterator();
    public abstract Stream<Replica> stream();

    public abstract boolean equals(Object o);
    public abstract int hashCode();
    public abstract String toString();

    /**
     * A mutable (append-only) extension of a ReplicaCollection.
     * All methods besides add() will return an immutable snapshot of the collection, or the matching items.
     */
    public interface Builder<C extends ReplicaCollection<C>> extends ReplicaCollection<C>
    {
        /**
         * @return an Immutable clone that assumes this Builder will never be modified again,
         * so its contents can be reused.
         *
         * This Builder should enforce that it is no longer modified.
         */
        public C build();

        /**
         * @return an Immutable clone that assumes this Builder will be modified again
         */
        public C snapshot();

        /**
         * Passed to add() and addAll() as ignoreConflicts parameter. The meaning of conflict varies by collection type
         * (for Endpoints, it is a duplicate InetAddressAndPort; for RangesAtEndpoint it is a duplicate Range).
         */
        enum Conflict
        {
            /** fail on addition of any such conflict */
            NONE,
            /** fail on addition of any such conflict where the contents differ (first occurrence and position wins) */
            DUPLICATE,
            /** ignore all conflicts (the first occurrence and position wins) */
            ALL
        }

        /**
         * @param replica add this replica to the end of the collection
         * @param ignoreConflict conflicts to ignore, see {@link Conflict}
         */
        Builder<C> add(Replica replica, Conflict ignoreConflict);

        default public Builder<C> add(Replica replica)
        {
            return add(replica, Conflict.NONE);
        }

        default public Builder<C> addAll(Iterable<Replica> replicas, Conflict ignoreConflicts)
        {
            for (Replica replica : replicas)
                add(replica, ignoreConflicts);
            return this;
        }

        default public Builder<C> addAll(Iterable<Replica> replicas)
        {
            return addAll(replicas, Conflict.NONE);
        }
    }

}
