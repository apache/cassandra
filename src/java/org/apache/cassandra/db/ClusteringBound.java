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

import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * The start or end of a range of clusterings, either inclusive or exclusive.
 */
public class ClusteringBound extends ClusteringBoundOrBoundary
{
    /** The smallest start bound, i.e. the one that starts before any row. */
    public static final ClusteringBound BOTTOM = new ClusteringBound(Kind.INCL_START_BOUND, EMPTY_VALUES_ARRAY);
    /** The biggest end bound, i.e. the one that ends after any row. */
    public static final ClusteringBound TOP = new ClusteringBound(Kind.INCL_END_BOUND, EMPTY_VALUES_ARRAY);

    protected ClusteringBound(Kind kind, ByteBuffer[] values)
    {
        super(kind, values);
    }

    public static ClusteringBound create(Kind kind, ByteBuffer[] values)
    {
        assert !kind.isBoundary();
        return new ClusteringBound(kind, values);
    }

    public static Kind boundKind(boolean isStart, boolean isInclusive)
    {
        return isStart
             ? (isInclusive ? Kind.INCL_START_BOUND : Kind.EXCL_START_BOUND)
             : (isInclusive ? Kind.INCL_END_BOUND : Kind.EXCL_END_BOUND);
    }

    public static ClusteringBound inclusiveStartOf(ByteBuffer... values)
    {
        return create(Kind.INCL_START_BOUND, values);
    }

    public static ClusteringBound inclusiveEndOf(ByteBuffer... values)
    {
        return create(Kind.INCL_END_BOUND, values);
    }

    public static ClusteringBound exclusiveStartOf(ByteBuffer... values)
    {
        return create(Kind.EXCL_START_BOUND, values);
    }

    public static ClusteringBound exclusiveEndOf(ByteBuffer... values)
    {
        return create(Kind.EXCL_END_BOUND, values);
    }

    public static ClusteringBound inclusiveStartOf(ClusteringPrefix prefix)
    {
        ByteBuffer[] values = new ByteBuffer[prefix.size()];
        for (int i = 0; i < prefix.size(); i++)
            values[i] = prefix.get(i);
        return inclusiveStartOf(values);
    }

    public static ClusteringBound exclusiveStartOf(ClusteringPrefix prefix)
    {
        ByteBuffer[] values = new ByteBuffer[prefix.size()];
        for (int i = 0; i < prefix.size(); i++)
            values[i] = prefix.get(i);
        return exclusiveStartOf(values);
    }

    public static ClusteringBound inclusiveEndOf(ClusteringPrefix prefix)
    {
        ByteBuffer[] values = new ByteBuffer[prefix.size()];
        for (int i = 0; i < prefix.size(); i++)
            values[i] = prefix.get(i);
        return inclusiveEndOf(values);
    }

    public static ClusteringBound create(ClusteringComparator comparator, boolean isStart, boolean isInclusive, Object... values)
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

    @Override
    public ClusteringBound invert()
    {
        return create(kind().invert(), values);
    }

    public ClusteringBound copy(AbstractAllocator allocator)
    {
        return (ClusteringBound) super.copy(allocator);
    }

    public boolean isStart()
    {
        return kind().isStart();
    }

    public boolean isEnd()
    {
        return !isStart();
    }

    public boolean isInclusive()
    {
        return kind == Kind.INCL_START_BOUND || kind == Kind.INCL_END_BOUND;
    }

    public boolean isExclusive()
    {
        return kind == Kind.EXCL_START_BOUND || kind == Kind.EXCL_END_BOUND;
    }

    // For use by intersects, it's called with the sstable bound opposite to the slice bound
    // (so if the slice bound is a start, it's call with the max sstable bound)
    int compareTo(ClusteringComparator comparator, List<ByteBuffer> sstableBound)
    {
        for (int i = 0; i < sstableBound.size(); i++)
        {
            // Say the slice bound is a start. It means we're in the case where the max
            // sstable bound is say (1:5) while the slice start is (1). So the start
            // does start before the sstable end bound (and intersect it). It's the exact
            // inverse with a end slice bound.
            if (i >= size())
                return isStart() ? -1 : 1;

            int cmp = comparator.compareComponent(i, get(i), sstableBound.get(i));
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
}
