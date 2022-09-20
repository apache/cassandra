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

package org.apache.cassandra.repair.asymmetric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class RangeMap<T> implements Map<Range<Token>, T>
{
    private static final Comparator<Range<Token>> comparator = Comparator.comparing((Range<Token> o) -> o.left);

    private final NavigableMap<Range<Token>, T> byStart;

    public RangeMap()
    {
        byStart = new TreeMap<>(comparator);
    }

    public int size()
    {
        return byStart.size();
    }

    public boolean isEmpty()
    {
        return byStart.isEmpty();
    }

    public boolean containsKey(Object key)
    {
        return byStart.containsKey(key);
    }

    public boolean containsValue(Object value)
    {
        return byStart.containsValue(value);
    }

    public T get(Object key)
    {
        return byStart.get(key);
    }

    public T put(Range<Token> key, T value)
    {
        assertNonIntersecting(key);
        return byStart.put(key, value);
    }

    private void assertNonIntersecting(Range<Token> range)
    {
        // todo: wraparound
        Range<Token> before = byStart.floorKey(range);
        Range<Token> after = byStart.ceilingKey(range);
        assert before == null || !before.intersects(range);
        assert after == null || !after.intersects(range);
    }

    public T remove(Object key)
    {
        return byStart.remove(key);
    }

    public void putAll(Map<? extends Range<Token>, ? extends T> m)
    {
        byStart.putAll(m);
    }

    public void clear()
    {
        byStart.clear();
    }

    public Set<Range<Token>> keySet()
    {
        return byStart.keySet();
    }

    public Collection<T> values()
    {
        return byStart.values();
    }

    public Set<Map.Entry<Range<Token>, T>> entrySet()
    {
        return byStart.entrySet();
    }

    /**
     * might return duplicate entries if range.isWrapAround()
     *
     * don't depend on the order of the entries returned
     */
    @VisibleForTesting
    Iterator<Map.Entry<Range<Token>, T>> intersectingEntryIterator(Range<Token> range)
    {
        return range.isWrapAround() ? new WrappingIntersectingIterator(range) : new IntersectingIterator(range);
    }

    public Set<Map.Entry<Range<Token>, T>> removeIntersecting(Range<Token> range)
    {
        Iterator<Map.Entry<Range<Token>, T>> iter = intersectingEntryIterator(range);
        Set<Map.Entry<Range<Token>, T>> intersecting = new HashSet<>();
        while (iter.hasNext())
        {
            Map.Entry<Range<Token>, T> entry = iter.next();
            intersecting.add(entry);
        }
        for (Map.Entry<Range<Token>, T> entry : intersecting)
            byStart.remove(entry.getKey());
        return intersecting;
    }

    private class WrappingIntersectingIterator extends AbstractIterator<Map.Entry<Range<Token>, T>>
    {
        private final Iterator<Iterator<Map.Entry<Range<Token>, T>>> iterators;
        private Iterator<Map.Entry<Range<Token>, T>> currentIter;

        public WrappingIntersectingIterator(Range<Token> range)
        {
            List<Iterator<Map.Entry<Range<Token>, T>>> iters = new ArrayList<>(2);
            for (Range<Token> unwrapped : range.unwrap())
                iters.add((new IntersectingIterator(unwrapped)));
            iterators = iters.iterator();
        }
        protected Map.Entry<Range<Token>, T> computeNext()
        {
            while (currentIter == null || !currentIter.hasNext())
            {
                if (!iterators.hasNext())
                    return endOfData();
                currentIter = iterators.next();
            }
            return currentIter.next();
        }
    }

    private class IntersectingIterator extends AbstractIterator<Map.Entry<Range<Token>, T>>
    {
        private final Iterator<Map.Entry<Range<Token>, T>> tailIterator;
        private final Range<Token> range;
        // since we guarantee no ranges overlap in byStart, we know the last entry is possibly the wrap around range
        private boolean shouldReturnLast = false;

        public IntersectingIterator(Range<Token> range)
        {
            Range<Token> startKey = byStart.floorKey(range);
            tailIterator = startKey == null ? byStart.entrySet().iterator() :
                                              byStart.tailMap(startKey, true).entrySet().iterator();
            Range<Token> last = byStart.isEmpty() ? null : byStart.lastKey();
            if (last != null && last.isWrapAround() && last.intersects(range))
                shouldReturnLast = true;
            this.range = range;
        }

        protected Map.Entry<Range<Token>, T> computeNext()
        {
            if (shouldReturnLast)
            {
                shouldReturnLast = false;
                return new Entry<>(byStart.lastEntry());
            }
            while (tailIterator.hasNext())
            {
                Entry<Range<Token>, T> candidateNext = new Entry<>(tailIterator.next());
                Range<Token> candidateRange = candidateNext.getKey();

                if (candidateRange.isWrapAround()) // we know we already returned any wrapping range
                    continue;

                if (candidateRange.left.compareTo(range.right) >= 0 && (!range.isWrapAround())) // range is unwrapped, but that means one range has right == min token and is still wrapping
                    return endOfData();

                if (range.left.compareTo(candidateRange.right) >= 0)
                    continue;

                return candidateNext;
            }
            return endOfData();
        }
    }

    public String toString()
    {
        return byStart.toString();
    }

    static class Entry<K, V> implements Map.Entry<K, V>
    {
        private final V v;
        private final K k;

        Entry(K key, V val)
        {
            this.k = key;
            this.v = val;
        }

        Entry(Map.Entry<K, V> toClone)
        {
            this(toClone.getKey(), toClone.getValue());
        }
        public K getKey()
        {
            return k;
        }

        public V getValue()
        {
            return v;
        }

        public V setValue(V value)
        {
            throw new UnsupportedOperationException();
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof Map.Entry)) return false;
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
            return v.equals(entry.getValue()) &&
                   k.equals(entry.getKey());
        }

        public int hashCode()
        {
            return Objects.hash(v, k);
        }

        public String toString()
        {
            return "Entry{" +
                   "v=" + v +
                   ", k=" + k +
                   '}';
        }
    }
}
