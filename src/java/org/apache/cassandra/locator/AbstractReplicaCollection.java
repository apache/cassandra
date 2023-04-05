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

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * A collection like class for Replica objects. Since the Replica class contains inetaddress, range, and
 * transient replication status, basic contains and remove methods can be ambiguous. Replicas forces you
 * to be explicit about what you're checking the container for, or removing from it.
 *
 * TODO: there's nothing about this collection that's unique to Replicas, and the implementation
 *       could make a useful general purpose immutable list<->set
 */
public abstract class AbstractReplicaCollection<C extends AbstractReplicaCollection<C>> implements ReplicaCollection<C>
{
    protected static final ReplicaList EMPTY_LIST = new ReplicaList(); // since immutable, can safely return this to avoid megamorphic callsites

    public static <C extends ReplicaCollection<C>, B extends Builder<C>> Collector<Replica, B, C> collector(Set<Collector.Characteristics> characteristics, Supplier<B> supplier)
    {
        return new Collector<Replica, B, C>()
        {
            private final BiConsumer<B, Replica> accumulator = Builder::add;
            private final BinaryOperator<B> combiner = (a, b) -> { a.addAll(b); return a; };
            private final Function<B, C> finisher = Builder::build;
            public Supplier<B> supplier() { return supplier; }
            public BiConsumer<B, Replica> accumulator() { return accumulator; }
            public BinaryOperator<B> combiner() { return combiner; }
            public Function<B, C> finisher() { return finisher; }
            public Set<Characteristics> characteristics() { return characteristics; }
        };
    }

    /**
     * A simple list with no comodification checks and immutability by default (only append permitted, and only one initial copy)
     * this permits us to reduce the amount of garbage generated, by not wrapping iterators or unnecessarily copying
     * and reduces the amount of indirection necessary, as well as ensuring monomorphic callsites
     *
     * TODO flatten into AbstractReplicaCollection?
     */
    protected final static class ReplicaList implements Iterable<Replica>
    {
        private static final Replica[] EMPTY = new Replica[0];
        Replica[] contents;
        int begin, size;

        public ReplicaList() { this(0); }
        public ReplicaList(int capacity) { contents = capacity == 0 ? EMPTY : new Replica[capacity]; }
        public ReplicaList(Replica[] contents, int begin, int size) { this.contents = contents; this.begin = begin; this.size = size; }

        public boolean isSubList(ReplicaList subList)
        {
            return subList.contents == contents;
        }

        public Replica get(int index)
        {
            if (index > size)
                throw new IndexOutOfBoundsException();
            return contents[begin + index];
        }

        public void add(Replica replica)
        {
            // can only add to full array - if we have sliced it, we must be a snapshot
            if (begin != 0)
                throw new IllegalStateException();

            if (size == contents.length)
            {
                int newSize;
                if (size < 3) newSize = 3;
                else if (size < 9) newSize = 9;
                else newSize = size * 2;
                contents = Arrays.copyOf(contents, newSize);
            }
            contents[size++] = replica;
        }

        public int size()
        {
            return size;
        }

        public boolean isEmpty()
        {
            return size == 0;
        }

        public ReplicaList subList(int begin, int end)
        {
            if (end > size || begin > end) throw new IndexOutOfBoundsException();
            return new ReplicaList(contents, this.begin + begin, end - begin);
        }

        public ReplicaList sorted(Comparator<? super Replica> comparator)
        {
            Replica[] copy = Arrays.copyOfRange(contents, begin, begin + size);
            Arrays.sort(copy, comparator);
            return new ReplicaList(copy, 0, copy.length);
        }

        public Stream<Replica> stream()
        {
            return Arrays.stream(contents, begin, begin + size);
        }

        @Override
        public void forEach(Consumer<? super Replica> forEach)
        {
            for (int i = begin, end = begin + size ; i < end ; ++i)
                forEach.accept(contents[i]);
        }

        /** see {@link ReplicaCollection#count(Predicate)}*/
        public int count(Predicate<? super Replica> test)
        {
            int count = 0;
            for (int i = begin, end = i + size ; i < end ; ++i)
                if (test.test(contents[i]))
                    ++count;
            return count;
        }

        public final boolean anyMatch(Predicate<? super Replica> predicate)
        {
            for (int i = begin, end = i + size ; i < end ; ++i)
                if (predicate.test(contents[i]))
                    return true;
            return false;
        }

        @Override
        public Spliterator<Replica> spliterator()
        {
            return Arrays.spliterator(contents, begin, begin + size);
        }

        // we implement our own iterator, because it is trivial to do so, and in monomorphic call sites
        // will compile down to almost optimal indexed for loop
        @Override
        public Iterator<Replica> iterator()
        {
            return new Iterator<Replica>()
            {
                final int end = begin + size;
                int i = begin;
                @Override
                public boolean hasNext()
                {
                    return i < end;
                }

                @Override
                public Replica next()
                {
                    if (!hasNext()) throw new IllegalStateException();
                    return contents[i++];
                }
            };
        }

        // we implement our own iterator, because it is trivial to do so, and in monomorphic call sites
        // will compile down to almost optimal indexed for loop
        public <K> Iterator<K> transformIterator(Function<? super Replica, ? extends K> function)
        {
            return new Iterator<K>()
            {
                final int end = begin + size;
                int i = begin;
                @Override
                public boolean hasNext()
                {
                    return i < end;
                }

                @Override
                public K next()
                {
                    return function.apply(contents[i++]);
                }
            };
        }

        // we implement our own iterator, because it is trivial to do so, and in monomorphic call sites
        // will compile down to almost optimal indexed for loop
        // in this case, especially, it is impactful versus Iterables.limit(Iterables.filter())
        private Iterator<Replica> filterIterator(Predicate<? super Replica> predicate, int limit)
        {
            return new Iterator<Replica>()
            {
                final int end = begin + size;
                int next = begin;
                int count = 0;
                { updateNext(); }
                void updateNext()
                {
                    if (count == limit) next = end;
                    while (next < end && !predicate.test(contents[next]))
                        ++next;
                    ++count;
                }
                @Override
                public boolean hasNext()
                {
                    return next < end;
                }

                @Override
                public Replica next()
                {
                    if (!hasNext()) throw new IllegalStateException();
                    Replica result = contents[next++];
                    updateNext();
                    return result;
                }
            };
        }

        protected <T> void forEach(Function<? super Replica, T> function, Consumer<? super T> action)
        {
            for (int i = begin, end = begin + size ; i < end ; ++i)
                action.accept(function.apply(contents[i]));
        }

        @VisibleForTesting
        public boolean equals(Object to)
        {
            if (to == null || to.getClass() != ReplicaList.class)
                return false;
            ReplicaList that = (ReplicaList) to;
            if (this.size != that.size) return false;
            return Iterables.elementsEqual(this, that);
        }
    }

    /**
     * A simple map that ensures the underlying list's iteration order is maintained, and can be shared with
     * subLists (either produced via subList, or via filter that naturally produced a subList).
     * This permits us to reduce the amount of garbage generated, by not unnecessarily copying,
     * reduces the amount of indirection necessary, as well as ensuring monomorphic callsites.
     * The underlying map is also more efficient, particularly for such small collections as we typically produce.
     */
    protected static class ReplicaMap<K> extends AbstractMap<K, Replica>
    {
        private final Function<Replica, K> toKey;
        private final ReplicaList list;
        // we maintain a map of key -> index in our list; this lets us share with subLists (or between Builder and snapshots)
        // since we only need to corroborate that the list index we find is within the bounds of our list
        // (if not, it's a shared map, and the key only occurs in one of our ancestors)
        private final ObjectIntHashMap<K> map;
        private Set<K> keySet;
        private Set<Entry<K, Replica>> entrySet;

        abstract class AbstractImmutableSet<T> extends AbstractSet<T>
        {
            @Override
            public boolean removeAll(Collection<?> c) { throw new UnsupportedOperationException(); }
            @Override
            public boolean remove(Object o) { throw new UnsupportedOperationException(); }
            @Override
            public int size() { return list.size(); }
        }

        class KeySet extends AbstractImmutableSet<K>
        {
            @Override
            public boolean contains(Object o) { return containsKey(o); }
            @Override
            public Iterator<K> iterator() { return list.transformIterator(toKey); }

            @Override
            public void forEach(Consumer<? super K> action)
            {
                list.forEach(toKey, action);
            }
        }

        class EntrySet extends AbstractImmutableSet<Entry<K, Replica>>
        {
            @Override
            public boolean contains(Object o)
            {
                Preconditions.checkNotNull(o);
                if (!(o instanceof Entry<?, ?>)) return false;
                return Objects.equals(get(((Entry) o).getKey()), ((Entry) o).getValue());
            }

            @Override
            public Iterator<Entry<K, Replica>> iterator()
            {
                return list.transformIterator(r -> new SimpleImmutableEntry<>(toKey.apply(r), r));
            }
        }

        public ReplicaMap(ReplicaList list, Function<Replica, K> toKey)
        {
            // 8*0.65 => RF=5; 16*0.65 ==> RF=10
            // use list capacity if empty, otherwise use actual list size
            this.toKey = toKey;
            this.map = new ObjectIntHashMap<>(list.size == 0 ? list.contents.length : list.size, 0.65f);
            this.list = list;
            for (int i = list.begin ; i < list.begin + list.size ; ++i)
            {
                boolean inserted = internalPutIfAbsent(list.contents[i], i);
                assert inserted;
            }
        }

        public ReplicaMap(ReplicaList list, Function<Replica, K> toKey, ObjectIntHashMap<K> map)
        {
            this.toKey = toKey;
            this.list = list;
            this.map = map;
        }

        // to be used only by subclasses of AbstractReplicaCollection
        boolean internalPutIfAbsent(Replica replica, int index)
        {
            K key = toKey.apply(replica);
            int otherIndex = map.put(key, index + 1);
            if (otherIndex == 0)
                return true;
            map.put(key, otherIndex);
            return false;
        }

        @Override
        public boolean containsKey(Object key)
        {
            Preconditions.checkNotNull(key);
            return get(key) != null;
        }

        public Replica get(Object key)
        {
            Preconditions.checkNotNull(key);
            int index = map.get((K)key) - 1;
            // since this map can be shared between sublists (or snapshots of mutables)
            // we have to first corroborate that the index we've found is actually within our list's bounds
            if (index < list.begin || index >= list.begin + list.size)
                return null;
            return list.contents[index];
        }

        @Override
        public Replica remove(Object key)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<K> keySet()
        {
            Set<K> ks = keySet;
            if (ks == null)
                keySet = ks = new KeySet();
            return ks;
        }

        @Override
        public Set<Entry<K, Replica>> entrySet()
        {
            Set<Entry<K, Replica>> es = entrySet;
            if (es == null)
                entrySet = es = new EntrySet();
            return es;
        }

        public int size()
        {
            return list.size();
        }

        ReplicaMap<K> forSubList(ReplicaList subList)
        {
            assert subList.contents == list.contents;
            return new ReplicaMap<>(subList, toKey, map);
        }
    }

    static class AsList<T> extends AbstractList<T> implements RandomAccess
    {
        final Function<Replica, T> view;
        final ReplicaList list;

        AsList(Function<Replica, T> view, ReplicaList list)
        {
            this.view = view;
            this.list = list;
        }

        public final T get(int index)
        {
            return view.apply(list.get(index));
        }

        public final int size()
        {
            return list.size;
        }

        @Override
        public final void forEach(Consumer<? super T> forEach)
        {
            list.forEach(view, forEach);
        }
    }


    protected final ReplicaList list;
    AbstractReplicaCollection(ReplicaList list)
    {
        this.list = list;
    }

    /**
     * construct a new Builder of our own type, so that we can concatenate
     * TODO: this isn't terribly pretty, but we need sometimes to select / merge two Endpoints of unknown type;
     */
    public abstract Builder<C> newBuilder(int initialCapacity);

    // return a new "sub-collection" with some sub-selection of the contents of this collection
    abstract C snapshot(ReplicaList newList);
    // return this object, if it is an immutable snapshot, otherwise returns a copy with these properties
    public abstract C snapshot();

    /** see {@link ReplicaCollection#subList(int, int)}*/
    public final C subList(int start, int end)
    {
        if (start == 0 && end == size())
            return snapshot();

        ReplicaList subList;
        if (start == end) subList = EMPTY_LIST;
        else subList = list.subList(start, end);

        return snapshot(subList);
    }

    public final <T> List<T> asList(Function<Replica, T> view)
    {
        return new AsList<>(view, list);
    }

    /** see {@link ReplicaCollection#count(Predicate)}*/
    public final int count(Predicate<? super Replica> test)
    {
        return list.count(test);
    }

    public final boolean anyMatch(Predicate<? super Replica> test)
    {
        return list.anyMatch(test);
    }

    /** see {@link ReplicaCollection#filter(Predicate)}*/
    public final C filter(Predicate<? super Replica> predicate)
    {
        return filter(predicate, Integer.MAX_VALUE);
    }

    /** see {@link ReplicaCollection#filter(Predicate, int)}*/
    public final C filter(Predicate<? super Replica> predicate, int limit)
    {
        if (isEmpty())
            return snapshot();

        ReplicaList copy = null;
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
                    copy = new ReplicaList(Math.min(limit, (list.size() - i) + (endRun - beginRun)));
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

    /** see {@link ReplicaCollection#filterLazily(Predicate)}*/
    public final Iterable<Replica> filterLazily(Predicate<? super Replica> predicate)
    {
        return filterLazily(predicate, Integer.MAX_VALUE);
    }

    /** see {@link ReplicaCollection#filterLazily(Predicate,int)}*/
    public final Iterable<Replica> filterLazily(Predicate<? super Replica> predicate, int limit)
    {
        return () -> list.filterIterator(predicate, limit);
    }

    /** see {@link ReplicaCollection#sorted(Comparator)}*/
    public final C sorted(Comparator<? super Replica> comparator)
    {
        return snapshot(list.sorted(comparator));
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

    public final void forEach(Consumer<? super Replica> forEach)
    {
        list.forEach(forEach);
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
        return Iterables.toString(list);
    }

    static <C extends AbstractReplicaCollection<C>> C concat(C replicas, C extraReplicas, Builder.Conflict ignoreConflicts)
    {
        if (extraReplicas.isEmpty())
            return replicas;
        if (replicas.isEmpty())
            return extraReplicas;
        Builder<C> builder = replicas.newBuilder(replicas.size() + extraReplicas.size());
        builder.addAll(replicas, Builder.Conflict.NONE);
        builder.addAll(extraReplicas, ignoreConflicts);
        return builder.build();
    }

}
