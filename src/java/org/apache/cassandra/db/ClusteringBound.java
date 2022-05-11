/*
 *
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
 *
 */
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.utils.memory.ByteBufferCloner;

/**
 * The start or end of a range of clusterings, either inclusive or exclusive.
 */
public interface ClusteringBound<V> extends ClusteringBoundOrBoundary<V>
{
    /** The smallest start bound, i.e. the one that starts before any row. */
    public static final ClusteringBound<?> BOTTOM = new BufferClusteringBound(ClusteringPrefix.Kind.INCL_START_BOUND, BufferClusteringBound.EMPTY_VALUES_ARRAY);
    /** The biggest end bound, i.e. the one that ends after any row. */
    public static final ClusteringBound<?> TOP = new BufferClusteringBound(ClusteringPrefix.Kind.INCL_END_BOUND, BufferClusteringBound.EMPTY_VALUES_ARRAY);

    public static ClusteringPrefix.Kind boundKind(boolean isStart, boolean isInclusive)
    {
        return isStart
               ? (isInclusive ? ClusteringPrefix.Kind.INCL_START_BOUND : ClusteringPrefix.Kind.EXCL_START_BOUND)
               : (isInclusive ? ClusteringPrefix.Kind.INCL_END_BOUND : ClusteringPrefix.Kind.EXCL_END_BOUND);
    }

    @Override
    ClusteringBound<V> invert();

    @Override
    ClusteringBound<ByteBuffer> clone(ByteBufferCloner cloner);

    default boolean isStart()
    {
        return kind().isStart();
    }

    default boolean isEnd()
    {
        return !isStart();
    }

    default boolean isInclusive()
    {
        return kind() == Kind.INCL_START_BOUND || kind() == Kind.INCL_END_BOUND;
    }

    default boolean isExclusive()
    {
        return kind() == Kind.EXCL_START_BOUND || kind() == Kind.EXCL_END_BOUND;
    }

    // For use by intersects, it's called with the sstable bound opposite to the slice bound
    // (so if the slice bound is a start, it's call with the max sstable bound)
    default int compareTo(ClusteringComparator comparator, List<ByteBuffer> sstableBound)
    {
        for (int i = 0; i < sstableBound.size(); i++)
        {
            // Say the slice bound is a start. It means we're in the case where the max
            // sstable bound is say (1:5) while the slice start is (1). So the start
            // does start before the sstable end bound (and intersect it). It's the exact
            // inverse with a end slice bound.
            if (i >= size())
                return isStart() ? -1 : 1;

            int cmp = comparator.compareComponent(i, get(i), accessor(), sstableBound.get(i), ByteBufferAccessor.instance);
            if (cmp != 0)
                return cmp;
        }

        // Say the slice bound is a start. I means we're in the case where the max
        // sstable bound is say (1), while the slice start is (1:5). This again means
        // that the slice start before the end bound.
        if (size() > sstableBound.size())
            return isStart() ? -1 : 1;

        // The slice bound is equal to the sstable bound. Results depends on whether the slice is inclusive or not
        return isInclusive() ? 0 : (isStart() ? 1 : -1);
    }

    static <V> ClusteringBound<V> create(ClusteringPrefix.Kind kind, ClusteringPrefix<V> from)
    {
        return from.accessor().factory().bound(kind, from.getRawValues());
    }

    public static ClusteringBound<?> inclusiveStartOf(ClusteringPrefix<?> from)
    {
        return create(ClusteringPrefix.Kind.INCL_START_BOUND, from);
    }

    public static ClusteringBound<?> inclusiveEndOf(ClusteringPrefix<?> from)
    {
        return create(ClusteringPrefix.Kind.INCL_END_BOUND, from);
    }

    public static ClusteringBound<?> exclusiveStartOf(ClusteringPrefix<?> from)
    {
        return create(ClusteringPrefix.Kind.EXCL_START_BOUND, from);
    }

    public static ClusteringBound<?> exclusiveEndOf(ClusteringPrefix<?> from)
    {
        return create(ClusteringPrefix.Kind.EXCL_END_BOUND, from);
    }

    public static ClusteringBound<?> create(ClusteringComparator comparator, boolean isStart, boolean isInclusive, Object... values)
    {
        CBuilder builder = CBuilder.create(comparator);
        for (Object val : values)
        {
            if (val instanceof ByteBuffer)
                builder.add((ByteBuffer) val);
            else
                builder.add(val);
        }
        return builder.buildBound(isStart, isInclusive);
    }
}
