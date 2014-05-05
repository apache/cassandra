/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.btree;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.SortedSet;

public class BTreeSet<V> implements NavigableSet<V>
{
    protected final Comparator<V> comparator;
    protected final Object[] tree;

    public BTreeSet(Object[] tree, Comparator<V> comparator)
    {
        this.tree = tree;
        this.comparator = comparator;
    }

    public BTreeSet<V> update(Collection<V> updateWith, boolean isSorted)
    {
        return new BTreeSet<>(BTree.update(tree, comparator, updateWith, isSorted, UpdateFunction.NoOp.<V>instance()), comparator);
    }

    @Override
    public Comparator<? super V> comparator()
    {
        return comparator;
    }

    protected Cursor<V, V> slice(boolean forwards, boolean permitInversion)
    {
        return BTree.slice(tree, forwards);
    }

    @Override
    public int size()
    {
        return slice(true, false).count();
    }

    @Override
    public boolean isEmpty()
    {
        return slice(true, false).hasNext();
    }

    @Override
    public Iterator<V> iterator()
    {
        return slice(true, true);
    }

    @Override
    public Iterator<V> descendingIterator()
    {
        return slice(false, true);
    }

    @Override
    public Object[] toArray()
    {
        return toArray(new Object[0]);
    }

    @Override
    public <T> T[] toArray(T[] a)
    {
        int size = size();
        if (a.length < size)
            a = Arrays.copyOf(a, size);
        int i = 0;
        for (V v : this)
            a[i++] = (T) v;
        return a;
    }

    @Override
    public NavigableSet<V> subSet(V fromElement, boolean fromInclusive, V toElement, boolean toInclusive)
    {
        return new BTreeRange<>(tree, comparator, fromElement, fromInclusive, toElement, toInclusive);
    }

    @Override
    public NavigableSet<V> headSet(V toElement, boolean inclusive)
    {
        return new BTreeRange<>(tree, comparator, null, true, toElement, inclusive);
    }

    @Override
    public NavigableSet<V> tailSet(V fromElement, boolean inclusive)
    {
        return new BTreeRange<>(tree, comparator, fromElement, inclusive, null, true);
    }

    @Override
    public SortedSet<V> subSet(V fromElement, V toElement)
    {
        return subSet(fromElement, true, toElement, false);
    }

    @Override
    public SortedSet<V> headSet(V toElement)
    {
        return headSet(toElement, false);
    }

    @Override
    public SortedSet<V> tailSet(V fromElement)
    {
        return tailSet(fromElement, true);
    }

    @Override
    public V first()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V last()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends V> c)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V pollFirst()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V pollLast()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(V v)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V lower(V v)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V floor(V v)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V ceiling(V v)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V higher(V v)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(Object o)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<V> descendingSet()
    {
        return new BTreeRange<>(this.tree, this.comparator).descendingSet();
    }

    public static class BTreeRange<V> extends BTreeSet<V> implements NavigableSet<V>
    {

        protected final V lowerBound, upperBound;
        protected final boolean inclusiveLowerBound, inclusiveUpperBound;

        BTreeRange(Object[] tree, Comparator<V> comparator)
        {
            this(tree, comparator, null, true, null, true);
        }

        BTreeRange(BTreeRange<V> from)
        {
            this(from.tree, from.comparator, from.lowerBound, from.inclusiveLowerBound, from.upperBound, from.inclusiveUpperBound);
        }

        BTreeRange(Object[] tree, Comparator<V> comparator, V lowerBound, boolean inclusiveLowerBound, V upperBound, boolean inclusiveUpperBound)
        {
            super(tree, comparator);
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.inclusiveLowerBound = inclusiveLowerBound;
            this.inclusiveUpperBound = inclusiveUpperBound;
        }

        // narrowing range constructor - makes this the intersection of the two ranges over the same tree b
        BTreeRange(BTreeRange<V> a, BTreeRange<V> b)
        {
            super(a.tree, a.comparator);
            assert a.tree == b.tree;
            final BTreeRange<V> lb, ub;

            if (a.lowerBound == null)
            {
                lb = b;
            }
            else if (b.lowerBound == null)
            {
                lb = a;
            }
            else
            {
                int c = comparator.compare(a.lowerBound, b.lowerBound);
                if (c < 0)
                    lb = b;
                else if (c > 0)
                    lb = a;
                else if (!a.inclusiveLowerBound)
                    lb = a;
                else
                    lb = b;
            }

            if (a.upperBound == null)
            {
                ub = b;
            }
            else if (b.upperBound == null)
            {
                ub = a;
            }
            else
            {
                int c = comparator.compare(b.upperBound, a.upperBound);
                if (c < 0)
                    ub = b;
                else if (c > 0)
                    ub = a;
                else if (!a.inclusiveUpperBound)
                    ub = a;
                else
                    ub = b;
            }

            lowerBound = lb.lowerBound;
            inclusiveLowerBound = lb.inclusiveLowerBound;
            upperBound = ub.upperBound;
            inclusiveUpperBound = ub.inclusiveUpperBound;
        }

        @Override
        protected Cursor<V, V> slice(boolean forwards, boolean permitInversion)
        {
            return BTree.slice(tree, comparator, lowerBound, inclusiveLowerBound, upperBound, inclusiveUpperBound, forwards);
        }

        @Override
        public NavigableSet<V> subSet(V fromElement, boolean fromInclusive, V toElement, boolean toInclusive)
        {
            return new BTreeRange<>(this, new BTreeRange<>(tree, comparator, fromElement, fromInclusive, toElement, toInclusive));
        }

        @Override
        public NavigableSet<V> headSet(V toElement, boolean inclusive)
        {
            return new BTreeRange<>(this, new BTreeRange<>(tree, comparator, lowerBound, true, toElement, inclusive));
        }

        @Override
        public NavigableSet<V> tailSet(V fromElement, boolean inclusive)
        {
            return new BTreeRange<>(this, new BTreeRange<>(tree, comparator, fromElement, inclusive, null, true));
        }

        @Override
        public NavigableSet<V> descendingSet()
        {
            return new BTreeDescRange<>(this);
        }
    }

    public static class BTreeDescRange<V> extends BTreeRange<V>
    {
        BTreeDescRange(BTreeRange<V> from)
        {
            super(from.tree, from.comparator, from.lowerBound, from.inclusiveLowerBound, from.upperBound, from.inclusiveUpperBound);
        }

        @Override
        protected Cursor<V, V> slice(boolean forwards, boolean permitInversion)
        {
            return super.slice(permitInversion ? !forwards : forwards, false);
        }

        @Override
        public NavigableSet<V> subSet(V fromElement, boolean fromInclusive, V toElement, boolean toInclusive)
        {
            return super.subSet(toElement, toInclusive, fromElement, fromInclusive).descendingSet();
        }

        @Override
        public NavigableSet<V> headSet(V toElement, boolean inclusive)
        {
            return super.tailSet(toElement, inclusive).descendingSet();
        }

        @Override
        public NavigableSet<V> tailSet(V fromElement, boolean inclusive)
        {
            return super.headSet(fromElement, inclusive).descendingSet();
        }

        @Override
        public NavigableSet<V> descendingSet()
        {
            return new BTreeRange<>(this);
        }
    }
}
