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

import com.google.common.base.Preconditions;

import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.ByteBufferCloner;

public class BufferClusteringBound extends BufferClusteringBoundOrBoundary implements ClusteringBound<ByteBuffer>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new BufferClusteringBound(Kind.INCL_START_BOUND, EMPTY_VALUES_ARRAY));

    public BufferClusteringBound(ClusteringPrefix.Kind kind, ByteBuffer[] values)
    {
        super(kind, values);
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(values);
    }

    @Override
    public ClusteringBound<ByteBuffer> invert()
    {
        return create(kind().invert(), values);
    }

    public ClusteringBound<ByteBuffer> clone(ByteBufferCloner cloner)
    {
        return (ClusteringBound<ByteBuffer>) super.clone(cloner);
    }

    public static BufferClusteringBound create(ClusteringPrefix.Kind kind, ByteBuffer[] values)
    {
        Preconditions.checkArgument(!kind.isBoundary(), "Expected bound clustering kind, got %s", kind);
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
}
