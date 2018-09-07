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

import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * A collection like class for Replica objects. Since the Replica class contains inetaddress, range, and
 * transient replication status, basic contains and remove methods can be ambiguous. Replicas forces you
 * to be explicit about what you're checking the container for, or removing from it.
 */
public abstract class AbstractReplicaCollection<C extends AbstractReplicaCollection<C>> implements ReplicaCollection<C>
{
    protected static final List<Replica> EMPTY_LIST = new ArrayList<>(); // since immutable, can safely return this to avoid megamorphic callsites

    public static <C extends ReplicaCollection<C>, B extends Builder<C, ?, B>> Collector<Replica, B, C> collector(Set<Collector.Characteristics> characteristics, Supplier<B> supplier)
    {
        return new Collector<Replica, B, C>()
        {
            private final BiConsumer<B, Replica> accumulator = Builder::add;
            private final BinaryOperator<B> combiner = (a, b) -> { a.addAll(b.mutable); return a; };
            private final Function<B, C> finisher = Builder::build;
            public Supplier<B> supplier() { return supplier; }
            public BiConsumer<B, Replica> accumulator() { return accumulator; }
            public BinaryOperator<B> combiner() { return combiner; }
            public Function<B, C> finisher() { return finisher; }
            public Set<Characteristics> characteristics() { return characteristics; }
        };
    }

    protected final List<Replica> list;
    protected final boolean isSnapshot;
    protected AbstractReplicaCollection(List<Replica> list, boolean isSnapshot)
    {
        this.list = list;
        this.isSnapshot = isSnapshot;
    }

    // if subList == null, should return self (or a clone thereof)
    protected abstract C snapshot(List<Replica> subList);
    protected abstract C self();
    /**
     * construct a new Mutable of our own type, so that we can concatenate
     * TODO: this isn't terribly pretty, but we need sometimes to select / merge two Endpoints of unknown type;
     */
    public abstract Mutable<C> newMutable(int initialCapacity);

    public C snapshot()
    {
        return isSnapshot ? self()
                          : snapshot(list.isEmpty() ? EMPTY_LIST
                                                    : new ArrayList<>(list));
    }

    /** see {@link ReplicaCollection#subList(int, int)}*/
    public final C subList(int start, int end)
    {
        List<Replica> subList;
        if (isSnapshot)
        {
            if (start == 0 && end == size()) return self();
            else if (start == end) subList = EMPTY_LIST;
            else subList = list.subList(start, end);
        }
        else
        {
            if (start == end) subList = EMPTY_LIST;
            else subList = new ArrayList<>(list.subList(start, end)); // TODO: we could take a subList here, but comodification checks stop us
        }
        return snapshot(subList);
    }

    /** see {@link ReplicaCollection#count(Predicate)}*/
    public int count(Predicate<Replica> predicate)
    {
        int count = 0;
        for (int i = 0 ; i < list.size() ; ++i)
            if (predicate.test(list.get(i)))
                ++count;
        return count;
    }

    /** see {@link ReplicaCollection#filter(Predicate)}*/
    public final C filter(Predicate<Replica> predicate)
    {
        return filter(predicate, Integer.MAX_VALUE);
    }

    /** see {@link ReplicaCollection#filter(Predicate, int)}*/
    public final C filter(Predicate<Replica> predicate, int limit)
    {
        if (isEmpty())
            return snapshot();

        List<Replica> copy = null;
        int beginRun = -1, endRun = -1;
        int i = 0;
        for (; i < list.size() ; ++i)
        {
            Replica replica = list.get(i);
            if (predicate.test(replica))
            {
                if (copy != null)
                    copy.add(replica);
                else if (beginRun < 0)
                    beginRun = i;
                else if (endRun > 0)
                {
                    copy = new ArrayList<>(Math.min(limit, (list.size() - i) + (endRun - beginRun)));
                    for (int j = beginRun ; j < endRun ; ++j)
                        copy.add(list.get(j));
                    copy.add(list.get(i));
                }
                if (--limit == 0)
                {
                    ++i;
                    break;
                }
            }
            else if (beginRun >= 0 && endRun < 0)
                endRun = i;
        }

        if (beginRun < 0)
            beginRun = endRun = 0;
        if (endRun < 0)
            endRun = i;
        if (copy == null)
            return subList(beginRun, endRun);
        return snapshot(copy);
    }

    /** see {@link ReplicaCollection#sorted(Comparator)}*/
   public final C sorted(Comparator<Replica> comparator)
    {
        List<Replica> copy = new ArrayList<>(list);
        copy.sort(comparator);
        return snapshot(copy);
    }

    public final Replica get(int i)
    {
        return list.get(i);
    }

    public final int size()
    {
        return list.size();
    }

    public final boolean isEmpty()
    {
        return list.isEmpty();
    }

    public final Iterator<Replica> iterator()
    {
        return list.iterator();
    }

    public final Stream<Replica> stream() { return list.stream(); }

    /**
     *  <p>
     *  It's not clear whether {@link AbstractReplicaCollection} should implement the order sensitive {@link Object#equals(Object) equals}
     *  of {@link java.util.List} or the order oblivious {@link Object#equals(Object) equals} of {@link java.util.Set}. We never rely on equality
     *  in the database so rather then leave in a potentially surprising implementation we have it throw {@link UnsupportedOperationException}.
     *  </p>
     *  <p>
     *  Don't implement this and pick one behavior over the other. If you want equality you can static import {@link com.google.common.collect.Iterables#elementsEqual(Iterable, Iterable)}
     *  and use that to get order sensitive equals.
     *  </p>
     */
    public final boolean equals(Object o)
    {
        throw new UnsupportedOperationException("AbstractReplicaCollection equals unsupported");
    }

    /**
     *  <p>
     *  It's not clear whether {@link AbstractReplicaCollection} should implement the order sensitive {@link Object#hashCode() hashCode}
     *  of {@link java.util.List} or the order oblivious {@link Object#hashCode() equals} of {@link java.util.Set}. We never rely on hashCode
     *  in the database so rather then leave in a potentially surprising implementation we have it throw {@link UnsupportedOperationException}.
     *  </p>
     *  <p>
     *  Don't implement this and pick one behavior over the other.
     *  </p>
     */
    public final int hashCode()
    {
        throw new UnsupportedOperationException("AbstractReplicaCollection hashCode unsupported");
    }

    @Override
    public final String toString()
    {
        return list.toString();
    }

    static <C extends AbstractReplicaCollection<C>> C concat(C replicas, C extraReplicas, Mutable.Conflict ignoreConflicts)
    {
        if (extraReplicas.isEmpty())
            return replicas;
        if (replicas.isEmpty())
            return extraReplicas;
        Mutable<C> mutable = replicas.newMutable(replicas.size() + extraReplicas.size());
        mutable.addAll(replicas, Mutable.Conflict.NONE);
        mutable.addAll(extraReplicas, ignoreConflicts);
        return mutable.asSnapshot();
    }

}
