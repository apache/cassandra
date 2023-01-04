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

import org.apache.cassandra.utils.memory.ByteBufferCloner;

import static org.apache.cassandra.db.AbstractBufferClusteringPrefix.EMPTY_VALUES_ARRAY;

/**
 * The start or end of a range of clusterings, either inclusive or exclusive.
 */
public interface ClusteringBound<V> extends ClusteringBoundOrBoundary<V>
{
    /** The smallest start bound, i.e. the one that starts before any row. */
    ClusteringBound<?> BOTTOM = new BufferClusteringBound(ClusteringPrefix.Kind.INCL_START_BOUND, EMPTY_VALUES_ARRAY);
    /** The biggest end bound, i.e. the one that ends after any row. */
    ClusteringBound<?> TOP = new BufferClusteringBound(ClusteringPrefix.Kind.INCL_END_BOUND, EMPTY_VALUES_ARRAY);

    /** The biggest start bound, i.e. the one that starts after any row. */
    ClusteringBound<?> MAX_START = new BufferClusteringBound(Kind.EXCL_START_BOUND, EMPTY_VALUES_ARRAY);
    /** The smallest end bound, i.e. the one that end before any row. */
    ClusteringBound<?> MIN_END = new BufferClusteringBound(Kind.EXCL_END_BOUND, EMPTY_VALUES_ARRAY);

    static ClusteringPrefix.Kind boundKind(boolean isStart, boolean isInclusive)
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

    default boolean isArtificial()
    {
        return kind() == Kind.SSTABLE_LOWER_BOUND || kind() == Kind.SSTABLE_UPPER_BOUND;
    }

    default ClusteringBound<V> artificialLowerBound(boolean isReversed)
    {
        return create(!isReversed ? Kind.SSTABLE_LOWER_BOUND : Kind.SSTABLE_UPPER_BOUND, this);
    }

    static <V> ClusteringBound<V> create(ClusteringPrefix.Kind kind, ClusteringPrefix<V> from)
    {
        return from.accessor().factory().bound(kind, from.getRawValues());
    }

    static <V> ClusteringBound<V> inclusiveStartOf(ClusteringPrefix<V> from)
    {
        return create(ClusteringPrefix.Kind.INCL_START_BOUND, from);
    }

    static <V> ClusteringBound<V> inclusiveEndOf(ClusteringPrefix<V> from)
    {
        return create(ClusteringPrefix.Kind.INCL_END_BOUND, from);
    }

    static <V> ClusteringBound<V> exclusiveStartOf(ClusteringPrefix<V> from)
    {
        return create(ClusteringPrefix.Kind.EXCL_START_BOUND, from);
    }

    static <V> ClusteringBound<V> exclusiveEndOf(ClusteringPrefix<V> from)
    {
        return create(ClusteringPrefix.Kind.EXCL_END_BOUND, from);
    }

    static ClusteringBound<?> create(ClusteringComparator comparator, boolean isStart, boolean isInclusive, Object... values)
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
    default ClusteringBound<V> asStartBound()
    {
        assert isStart();
        return this;
    }

    @Override
    default ClusteringBound<V> asEndBound()
    {
        assert isEnd();
        return this;
    }
}