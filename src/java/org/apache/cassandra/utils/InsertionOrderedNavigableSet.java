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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedSet;

import com.google.common.base.Preconditions;

/**
 * A {@link NavigableSet} that enforces in-order insertion of elements. This is helpful when we 
 * have an already-ordered collection with no duplicates and want constant time insertion.
 * <p>
 * Note: Not all methods of {@link NavigableSet} are implemented.
 *
 * @param <E> the type of elements maintained by this set
 */
public class InsertionOrderedNavigableSet<E> implements NavigableSet<E>
{
    private final ArrayList<E> elements;
    private final Comparator<? super E> comparator;

    public InsertionOrderedNavigableSet(Comparator<? super E> comparator)
    {
        this.elements = new ArrayList<>();
        this.comparator = comparator;
    }

    @Override
    public E lower(E e)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public E floor(E e)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public E ceiling(E e)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public E higher(E e)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public E pollFirst()
    {
        if (isEmpty())
            return null;
        
        return elements.remove(0);
    }

    @Override
    public E pollLast()
    {
        if (isEmpty())
            return null;

        return elements.remove(size() - 1);
    }

    @Override
    public int size()
    {
        return elements.size();
    }

    @Override
    public boolean isEmpty()
    {
        return elements.isEmpty();
    }

    @Override
    public boolean contains(Object o)
    {
        return elements.contains(o);
    }

    @Override
    public Iterator<E> iterator()
    {
        return elements.iterator();
    }

    @Override
    public Object[] toArray()
    {
        return elements.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a)
    {
        return elements.toArray(a);
    }

    @Override
    public boolean add(E e)
    {
        if (!isEmpty() && comparator.compare(e, last()) <= 0)
            throw new IllegalStateException("Cannot add element " + e + " as it is not greater than the current last element " + last());

        return elements.add(e);
    }

    @Override
    public boolean remove(Object o)
    {
        return elements.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c)
    {
        return elements.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c)
    {
        boolean modified = false;
        for (E element : c)
            if (add(element))
                modified = true;
        return modified;
    }

    @Override
    public boolean retainAll(Collection<?> c)
    {
        return elements.retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c)
    {
        return elements.removeAll(c);
    }

    @Override
    public void clear()
    {
        elements.clear();
    }

    @Override
    public NavigableSet<E> descendingSet()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<E> descendingIterator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<E> headSet(E toElement, boolean inclusive)
    {
        Preconditions.checkNotNull(toElement);

        if (isEmpty())
            return Collections.emptyNavigableSet();

        NavigableSet<E> head = new InsertionOrderedNavigableSet<>(comparator);

        for (E element : elements)
        {
            int comparison = comparator.compare(element, toElement);
            if (comparison > 0 || comparison == 0 && !inclusive)
                break;

            head.add(element);
        }

        return head;
    }

    @Override
    public NavigableSet<E> tailSet(E fromElement, boolean inclusive)
    {
        Preconditions.checkNotNull(fromElement);

        if (isEmpty())
            return Collections.emptyNavigableSet();

        NavigableSet<E> tail = new InsertionOrderedNavigableSet<>(comparator);

        for (E element : elements)
        {
            int comparison = comparator.compare(element, fromElement);
            if (comparison < 0 || comparison == 0 && !inclusive)
                continue;

            tail.add(element);
        }

        return tail;
    }

    @Override
    public Comparator<? super E> comparator()
    {
        return comparator;
    }

    @Override
    public SortedSet<E> subSet(E fromElement, E toElement)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedSet<E> headSet(E toElement)
    {
        return headSet(toElement, false);
    }

    @Override
    public SortedSet<E> tailSet(E fromElement)
    {
        return tailSet(fromElement, true);
    }

    @Override
    public E first()
    {
        if (isEmpty())
            throw new NoSuchElementException();

        return elements.get(0);
    }

    @Override
    public E last()
    {
        if (isEmpty())
            throw new NoSuchElementException();

        return elements.get(size() - 1);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InsertionOrderedNavigableSet<?> that = (InsertionOrderedNavigableSet<?>) o;
        return Objects.equals(elements, that.elements) && Objects.equals(comparator, that.comparator);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(elements, comparator);
    }
}
