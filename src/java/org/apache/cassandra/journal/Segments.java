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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import accord.utils.Invariants;
import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.Refs;

/**
 * Consistent, immutable view of active + static segments
 * <p/>
 * TODO (performance, expected): an interval/range structure for StaticSegment lookup based on min/max key bounds
 */
class Segments<K, V>
{
    private final Long2ObjectHashMap<Segment<K, V>> segments;

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
    List<Segment<K, V>> allSorted()
    {
        List<Segment<K, V>> segments = new ArrayList<>(this.segments.values());
        segments.sort(Comparator.comparing(s -> s.descriptor));
        return segments;
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
        Segment<K, V> oldest = null;
        for (Segment<K, V> segment : segments.values())
            if (segment.isActive() && (oldest == null || segment.descriptor.timestamp <= oldest.descriptor.timestamp))
                oldest = segment;

        return oldest == null ? null : oldest.asActive();
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

    void selectStatic(Consumer<StaticSegment<K, V>> into)
    {
        for (Segment<K, V> segment : segments.values())
            if (segment.isStatic())
                into.accept(segment.asStatic());
    }

    /**
     * Select segments that could potentially have an entry with the specified ids and
     * attempt to grab references to them all.
     *
     * @return a subset of segments with references to them, or {@code null} if failed to grab the refs
     */
    @SuppressWarnings("resource")
    ReferencedSegments<K, V> selectAndReference(Predicate<Segment<K, V>> test)
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

        Refs<Segment<K, V>> refs = null;
        if (!selectedSegments.isEmpty())
        {
            refs = Refs.tryRef(selectedSegments.values());
            if (null == refs)
                return null;
        }
        return new ReferencedSegments<>(selectedSegments, refs);
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

    boolean isFlushed(RecordPointer recordPointer)
    {
        Segment<K, V> segment = segments.get(recordPointer.segment);
        if (null == segment)
            throw new IllegalArgumentException("Can not reference segment " + recordPointer.segment);
        return segment.isFlushed(recordPointer.position);
    }

    ReferencedSegment<K, V> selectAndReference(long segmentTimestamp)
    {
        Segment<K, V> segment = segments.get(segmentTimestamp);
        if (null == segment)
            return new ReferencedSegment<>(null, null);
        Ref<Segment<K, V>> ref = segment.tryRef();
        if (null == ref)
            return null;
        return new ReferencedSegment<>(segment, ref);
    }

    static class ReferencedSegment<K, V> implements AutoCloseable
    {
        private final Segment<K, V> segment;
        private final Ref<Segment<K, V>> ref;

        ReferencedSegment(Segment<K, V> segment, Ref<Segment<K, V>> ref)
        {
            this.segment = segment;
            this.ref = ref;
        }

        Segment<K, V> segment()
        {
            return segment;
        }

        @Override
        public void close()
        {
            if (null != ref)
                ref.release();
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
        return new Long2ObjectHashMap<>(0, 0.65f, false);
    }
}
