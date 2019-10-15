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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public class BufferClusteringBound extends BufferClusteringBoundOrBoundary implements ClusteringBound<ByteBuffer>
{
//    /** The smallest start bound, i.e. the one that starts before any row. */
//    public static final ClusteringBound BOTTOM = new BufferClusteringBound(ClusteringPrefix.Kind.INCL_START_BOUND, EMPTY_VALUES_ARRAY);
//    /** The biggest end bound, i.e. the one that ends after any row. */
//    public static final ClusteringBound TOP = new BufferClusteringBound(ClusteringPrefix.Kind.INCL_END_BOUND, EMPTY_VALUES_ARRAY);

    protected BufferClusteringBound(ClusteringPrefix.Kind kind, ByteBuffer[] values)
    {
        super(kind, values);
    }

    @Override
    public ClusteringBound<ByteBuffer> invert()
    {
        return create(kind().invert(), values);
    }

    public ClusteringBound<ByteBuffer> copy(AbstractAllocator allocator)
    {
        return (ClusteringBound<ByteBuffer>) super.copy(allocator);
    }

    public ClusteringPrefix<ByteBuffer> minimize()
    {
        if (!ByteBufferUtil.canMinimize(values))
            return this;
        return new BufferClusteringBound(kind, ByteBufferUtil.minimizeBuffers(values));
    }
    
    public static BufferClusteringBound create(ClusteringPrefix.Kind kind, ByteBuffer[] values)
    {
        assert !kind.isBoundary();
        return new BufferClusteringBound(kind, values);
    }

    public static BufferClusteringBound inclusiveStartOf(ByteBuffer... values)
    {
        return create(ClusteringPrefix.Kind.INCL_START_BOUND, values);
    }

    public static BufferClusteringBound inclusiveEndOf(ByteBuffer... values)
    {
        return create(ClusteringPrefix.Kind.INCL_END_BOUND, values);
    }

    public static BufferClusteringBound exclusiveStartOf(ByteBuffer... values)
    {
        return create(ClusteringPrefix.Kind.EXCL_START_BOUND, values);
    }

    public static BufferClusteringBound exclusiveEndOf(ByteBuffer... values)
    {
        return create(ClusteringPrefix.Kind.EXCL_END_BOUND, values);
    }

    public static ClusteringBound<ByteBuffer> create(ClusteringComparator comparator, boolean isStart, boolean isInclusive, Object... values)
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
