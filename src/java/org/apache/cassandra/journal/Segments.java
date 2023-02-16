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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import accord.utils.Invariants;
import org.apache.cassandra.utils.concurrent.Refs;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * Consistent, immutable view of active + static segments
 * <p/>
 * TODO: an interval/range structure for StaticSegment lookup based on min/max key bounds
 */
class Segments<K>
{
    // active segments, containing unflushed data; the tail of this queue is the one we allocate writes from
    private final List<ActiveSegment<K>> activeSegments;

    // finalised segments, no longer written to
    private final Map<Descriptor, StaticSegment<K>> staticSegments;

    // cached Iterable of concatenated active and static segments
    private final Iterable<Segment<K>> allSegments;

    Segments(List<ActiveSegment<K>> activeSegments, Map<Descriptor, StaticSegment<K>> staticSegments)
    {
        this.activeSegments = activeSegments;
        this.staticSegments = staticSegments;
        this.allSegments = Iterables.concat(onlyActive(), onlyStatic());
    }

    static <K> Segments<K> ofStatic(Collection<StaticSegment<K>> segments)
    {
        HashMap<Descriptor, StaticSegment<K>> staticSegments =
            Maps.newHashMapWithExpectedSize(segments.size());
        for (StaticSegment<K> segment : segments)
            staticSegments.put(segment.descriptor, segment);
        return new Segments<>(new ArrayList<>(), staticSegments);
    }

    static <K> Segments<K> none()
    {
        return new Segments<>(Collections.emptyList(), Collections.emptyMap());
    }

    Segments<K> withNewActiveSegment(ActiveSegment<K> activeSegment)
    {
        ArrayList<ActiveSegment<K>> newActiveSegments =
            new ArrayList<>(activeSegments.size() + 1);
        newActiveSegments.addAll(activeSegments);
        newActiveSegments.add(activeSegment);
        return new Segments<>(newActiveSegments, staticSegments);
    }

    Segments<K> withCompletedSegment(ActiveSegment<K> activeSegment, StaticSegment<K> staticSegment)
    {
        Invariants.checkArgument(activeSegment.descriptor.equals(staticSegment.descriptor));

        ArrayList<ActiveSegment<K>> newActiveSegments =
            new ArrayList<>(activeSegments.size() - 1);
        for (ActiveSegment<K> segment : activeSegments)
            if (segment != activeSegment)
                newActiveSegments.add(segment);
        Invariants.checkState(newActiveSegments.size() == activeSegments.size() - 1);

        HashMap<Descriptor, StaticSegment<K>> newStaticSegments =
            Maps.newHashMapWithExpectedSize(staticSegments.size() + 1);
        newStaticSegments.putAll(staticSegments);
        if (newStaticSegments.put(staticSegment.descriptor, staticSegment) != null)
            throw new IllegalStateException();

        return new Segments<>(newActiveSegments, newStaticSegments);
    }

    Segments<K> withCompactedSegment(StaticSegment<K> oldSegment, StaticSegment<K> newSegment)
    {
        Invariants.checkArgument(oldSegment.descriptor.timestamp == newSegment.descriptor.timestamp);
        Invariants.checkArgument(oldSegment.descriptor.generation < newSegment.descriptor.generation);

        HashMap<Descriptor, StaticSegment<K>> newStaticSegments = new HashMap<>(staticSegments);
        if (!newStaticSegments.remove(oldSegment.descriptor, oldSegment))
            throw new IllegalStateException();
        if (null != newStaticSegments.put(newSegment.descriptor, newSegment))
            throw new IllegalStateException();

        return new Segments<>(activeSegments, newStaticSegments);
    }

    Segments<K> withoutInvalidatedSegment(StaticSegment<K> staticSegment)
    {
        HashMap<Descriptor, StaticSegment<K>> newStaticSegments = new HashMap<>(staticSegments);
        if (!newStaticSegments.remove(staticSegment.descriptor, staticSegment))
            throw new IllegalStateException();
        return new Segments<>(activeSegments, newStaticSegments);
    }

    Iterable<Segment<K>> all()
    {
        return allSegments;
    }

    Collection<ActiveSegment<K>> onlyActive()
    {
        return activeSegments;
    }

    Collection<StaticSegment<K>> onlyStatic()
    {
        return staticSegments.values();
    }

    /**
     * Select segments that could potentially have an entry with the specified id and
     * attempt to grab references to them all.
     *
     * @return a subset of segments with references to them, or {@code null} if failed to grab the refs
     */
    @SuppressWarnings("resource")
    ReferencedSegments<K> selectAndReference(K id)
    {
        List<ActiveSegment<K>> selectedActive = null;
        for (ActiveSegment<K> segment : onlyActive())
        {
            if (segment.index.mayContainId(id))
            {
                if (null == selectedActive)
                    selectedActive = new ArrayList<>();
                selectedActive.add(segment);
            }
        }
        if (null == selectedActive) selectedActive = emptyList();

        Map<Descriptor, StaticSegment<K>> selectedStatic = null;
        for (StaticSegment<K> segment : onlyStatic())
        {
            if (segment.index().mayContainId(id))
            {
                if (null == selectedStatic)
                    selectedStatic = new HashMap<>();
                selectedStatic.put(segment.descriptor, segment);
            }
        }
        if (null == selectedStatic) selectedStatic = emptyMap();

        Refs<Segment<K>> refs = null;
        if (!selectedActive.isEmpty() || !selectedStatic.isEmpty())
        {
            refs = Refs.tryRef(Iterables.concat(selectedActive, selectedStatic.values()));
            if (null == refs)
                return null;
        }
        return new ReferencedSegments<>(selectedActive, selectedStatic, refs);
    }

    static class ReferencedSegments<K> extends Segments<K> implements AutoCloseable
    {
        public final Refs<Segment<K>> refs;

        ReferencedSegments(
            List<ActiveSegment<K>> activeSegments, Map<Descriptor, StaticSegment<K>> staticSegments, Refs<Segment<K>> refs)
        {
            super(activeSegments, staticSegments);
            this.refs = refs;
        }

        @Override
        public void close()
        {
            if (null != refs)
                refs.release();
        }
    }
}
