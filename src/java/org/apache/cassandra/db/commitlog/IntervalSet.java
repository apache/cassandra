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

package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.util.*;

import com.google.common.collect.ImmutableSortedMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * An immutable set of closed intervals, stored in normalized form (i.e. where overlapping intervals are converted
 * to a single interval covering both).
 *
 * The set is stored as a sorted map from interval starts to the corresponding end. The map satisfies
 *   {@code curr().getKey() <= curr().getValue() < next().getKey()}
 */
public class IntervalSet<T extends Comparable<T>>
{
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static final IntervalSet EMPTY = new IntervalSet(ImmutableSortedMap.of());

    final private NavigableMap<T, T> ranges;

    private IntervalSet(ImmutableSortedMap<T, T> ranges)
    {
        this.ranges = ranges;
    }

    /**
     * Construct new set containing the interval with the given start and end position.
     */
    public IntervalSet(T start, T end)
    {
        this(ImmutableSortedMap.of(start, end));
    }

    @SuppressWarnings("unchecked")
    public static <T extends Comparable<T>> IntervalSet<T> empty()
    {
        return EMPTY;
    }

    public boolean contains(T position)
    {
        // closed (i.e. inclusive) intervals
        Map.Entry<T, T> range = ranges.floorEntry(position);
        return range != null && position.compareTo(range.getValue()) <= 0;
    }

    public boolean isEmpty()
    {
        return ranges.isEmpty();
    }

    public Optional<T> lowerBound()
    {
        return isEmpty() ? Optional.empty() : Optional.of(ranges.firstKey());
    }

    public Optional<T> upperBound()
    {
        return isEmpty() ? Optional.empty() : Optional.of(ranges.lastEntry().getValue());
    }

    public Collection<T> starts()
    {
        return ranges.keySet();
    }

    public Collection<T> ends()
    {
        return ranges.values();
    }

    public String toString()
    {
        return ranges.toString();
    }

    @Override
    public int hashCode()
    {
        return ranges.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof IntervalSet && ranges.equals(((IntervalSet<?>) obj).ranges);
    }

    public static final <T extends Comparable<T>> ISerializer<IntervalSet<T>> serializer(ISerializer<T> pointSerializer)
    {
        return new ISerializer<IntervalSet<T>>()
        {
            public void serialize(IntervalSet<T> intervals, DataOutputPlus out) throws IOException
            {
                out.writeInt(intervals.ranges.size());
                for (Map.Entry<T, T> en : intervals.ranges.entrySet())
                {
                    pointSerializer.serialize(en.getKey(), out);
                    pointSerializer.serialize(en.getValue(), out);
                }
            }

            public IntervalSet<T> deserialize(DataInputPlus in) throws IOException
            {
                int count = in.readInt();
                NavigableMap<T, T> ranges = new TreeMap<>();
                for (int i = 0; i < count; ++i)
                    ranges.put(pointSerializer.deserialize(in), pointSerializer.deserialize(in));
                return new IntervalSet<T>(ImmutableSortedMap.copyOfSorted(ranges));
            }

            public long serializedSize(IntervalSet<T> intervals)
            {
                long size = TypeSizes.sizeof(intervals.ranges.size());
                for (Map.Entry<T, T> en : intervals.ranges.entrySet())
                {
                    size += pointSerializer.serializedSize(en.getKey());
                    size += pointSerializer.serializedSize(en.getValue());
                }
                return size;
            }
        };
    };

    /**
     * Builder of interval sets, applying the necessary normalization while adding ranges.
     *
     * Data is stored as above, as a sorted map from interval starts to the corresponding end, which satisfies
     *   {@code curr().getKey() <= curr().getValue() < next().getKey()}
     */
    static public class Builder<T extends Comparable<T>>
    {
        final NavigableMap<T, T> ranges;

        public Builder()
        {
            this.ranges = new TreeMap<>();
        }

        public Builder(T start, T end)
        {
            this();
            assert start.compareTo(end) <= 0;
            ranges.put(start, end);
        }

        /**
         * Add an interval to the set and perform normalization.
         */
        public void add(T start, T end)
        {
            assert start.compareTo(end) <= 0;
            // extend ourselves to cover any ranges we overlap
            // record directly preceding our end may extend past us, so take the max of our end and its
            Map.Entry<T, T> extend = ranges.floorEntry(end);
            if (extend != null && extend.getValue().compareTo(end) > 0)
                end = extend.getValue();

            // record directly preceding our start may extend into us; if it does, we take it as our start
            extend = ranges.lowerEntry(start);
            if (extend != null && extend.getValue().compareTo(start) >= 0)
                start = extend.getKey();

            // remove all covered intervals
            // since we have adjusted start and end to cover the ones that would be only partially covered, we
            // are certain that anything whose start falls within the span is completely covered
            ranges.subMap(start, end).clear();
            // add the new interval
            ranges.put(start, end);
        }

        public void addAll(IntervalSet<T> otherSet)
        {
            for (Map.Entry<T, T> en : otherSet.ranges.entrySet())
            {
                add(en.getKey(), en.getValue());
            }
        }

        public IntervalSet<T> build()
        {
            return new IntervalSet<T>(ImmutableSortedMap.copyOfSorted(ranges));
        }
    }

}
