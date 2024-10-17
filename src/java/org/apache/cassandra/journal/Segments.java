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
package org.apache.cassandra.journal;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.utils.concurrent.Refs;

/**
 * Consistent, immutable view of active + static segments
 * <p/>
 * TODO (performance, expected): an interval/range structure for StaticSegment lookup based on min/max key bounds
 */
class Segments<K, V>
{
    private final Long2ObjectHashMap<Segment<K, V>> segments;
    private SortedArrayList<Segment<K, V>> sorted;

    Segments(Long2ObjectHashMap<Segment<K, V>> segments)
    {
        this.segments = segments;
    }

    static <K, V> Segments<K, V> of(Collection<Segment<K, V>> segments)
    {
        Long2ObjectHashMap<Segment<K, V>> newSegments = newMap(segments.size());
        for (Segment<K, V> segment : segments)
            newSegments.put(segment.descriptor.timestamp, segment);
        return new Segments<>(newSegments);
    }

    static <K, V> Segments<K, V> none()
    {
        return new Segments<>(emptyMap());
    }

    Segments<K, V> withNewActiveSegment(ActiveSegment<K, V> activeSegment)
    {
        Long2ObjectHashMap<Segment<K, V>> newSegments = new Long2ObjectHashMap<>(segments);
        Segment<K, V> oldValue = newSegments.put(activeSegment.descriptor.timestamp, activeSegment);
        Invariants.checkState(oldValue == null);
        return new Segments<>(newSegments);
    }

    Segments<K, V> withoutEmptySegment(ActiveSegment<K, V> activeSegment)
    {
        Long2ObjectHashMap<Segment<K, V>> newSegments = new Long2ObjectHashMap<>(segments);
        Segment<K, V> oldValue = segments.remove(activeSegment.descriptor.timestamp);
        Invariants.checkState(oldValue.asActive().isEmpty());
        return new Segments<>(newSegments);
    }

    Segments<K, V> withCompletedSegment(ActiveSegment<K, V> activeSegment, StaticSegment<K, V> staticSegment)
    {
        Invariants.checkArgument(activeSegment.descriptor.equals(staticSegment.descriptor));
        Long2ObjectHashMap<Segment<K, V>> newSegments = new Long2ObjectHashMap<>(segments);
        Segment<K, V> oldValue = newSegments.put(staticSegment.descriptor.timestamp, staticSegment);
        Invariants.checkState(oldValue == activeSegment);
        return new Segments<>(newSegments);
    }

    Segments<K, V> withCompactedSegments(Collection<StaticSegment<K, V>> oldSegments, Collection<StaticSegment<K, V>> compactedSegments)
    {
        Long2ObjectHashMap<Segment<K, V>> newSegments = new Long2ObjectHashMap<>(segments);
        for (StaticSegment<K, V> oldSegment : oldSegments)
        {
            Segment<K, V> oldValue = newSegments.remove(oldSegment.descriptor.timestamp);
            Invariants.checkState(oldValue == oldSegment);
        }

        for (StaticSegment<K, V> compactedSegment : compactedSegments)
        {
            Segment<K, V> oldValue = newSegments.put(compactedSegment.descriptor.timestamp, compactedSegment);
            Invariants.checkState(oldValue == null);
        }

        return new Segments<>(newSegments);
    }

    Iterable<Segment<K, V>> all()
    {
        return this.segments.values();
    }

    /**
     * Returns segments in timestamp order. Will allocate and sort the segment collection.
     */
    List<Segment<K, V>> allSorted(boolean asc)
    {
        if (sorted == null)
            sorted = SortedArrayList.<Segment<K, V>>copyUnsorted(segments.values(), Segment[]::new);
        return asc ? sorted : sorted.reverse();
    }

    void selectActive(long maxTimestamp, Collection<ActiveSegment<K, V>> into)
    {
        for (Segment<K, V> segment : segments.values())
            if (segment.isActive() && segment.descriptor.timestamp <= maxTimestamp)
                into.add(segment.asActive());
    }

    boolean isSwitched(ActiveSegment<K, V> active)
    {
        for (Segment<K, V> segment : segments.values())
            if (!segment.isActive() && active.descriptor.equals(segment.descriptor))
                return true;

        return false;
    }

    ActiveSegment<K, V> oldestActive()
    {
        List<Segment<K, V>> sorted = allSorted(true);
        for (int i = 0 ; i < sorted.size() ; ++i)
        {
            Segment<K, V> segment = sorted.get(i);
            if (segment.isActive())
                return segment.asActive();
        }
        return null;
    }

    Segment<K, V> get(long timestamp)
    {
        return segments.get(timestamp);
    }

    void selectStatic(Collection<StaticSegment<K, V>> into)
    {
        for (Segment<K, V> segment : segments.values())
            if (segment.isStatic())
                into.add(segment.asStatic());
    }

    /**
     * Select segments that could potentially have an entry with the specified ids and
     * attempt to grab references to them all.
     *
     * @return a subset of segments with references to them, or {@code null} if failed to grab the refs
     */
    ReferencedSegments<K, V> selectAndReference(Predicate<Segment<K, V>> test)
    {
        Long2ObjectHashMap<Segment<K, V>> selectedSegments = select(test).segments;
        Refs<Segment<K, V>> refs = null;
        if (!selectedSegments.isEmpty())
        {
            refs = Refs.tryRef(selectedSegments.values());
            if (null == refs)
                return null;
        }
        return new ReferencedSegments<>(selectedSegments, refs);
    }

    /**
     * Select segments that could potentially have an entry with the specified ids and
     * attempt to grab references to them all.
     *
     * @return a subset of segments with references to them, or {@code null} if failed to grab the refs
     */
    Segments<K, V> select(Predicate<Segment<K, V>> test)
    {
        Long2ObjectHashMap<Segment<K, V>> selectedSegments = null;
        for (Segment<K, V> segment : segments.values())
        {
            if (test.test(segment))
            {
                if (null == selectedSegments)
                    selectedSegments = newMap(10);
                selectedSegments.put(segment.descriptor.timestamp, segment);
            }
        }

        if (null == selectedSegments)
            selectedSegments = emptyMap();

        return new Segments<>(selectedSegments);
    }

    static class ReferencedSegments<K, V> extends Segments<K, V> implements AutoCloseable
    {
        private final Refs<Segment<K, V>> refs;

        ReferencedSegments(Long2ObjectHashMap<Segment<K, V>> segments, Refs<Segment<K, V>> refs)
        {
            super(segments);
            this.refs = refs;
        }

        @Override
        public void close()
        {
            if (null != refs)
                refs.release();
        }
    }

    private static final Long2ObjectHashMap<?> EMPTY_MAP = new Long2ObjectHashMap<>();

    @SuppressWarnings("unchecked")
    private static <K> Long2ObjectHashMap<K> emptyMap()
    {
        return (Long2ObjectHashMap<K>) EMPTY_MAP;
    }

    private static <K> Long2ObjectHashMap<K> newMap(int expectedSize)
    {
        return new Long2ObjectHashMap<>(expectedSize, 0.65f, false);
    }
}
