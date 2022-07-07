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

public class BufferClusteringBoundary extends BufferClusteringBoundOrBoundary implements ClusteringBoundary<ByteBuffer>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new BufferClusteringBoundary(Kind.INCL_START_BOUND, EMPTY_VALUES_ARRAY));

    public BufferClusteringBoundary(Kind kind, ByteBuffer[] values)
    {
        super(kind, values);
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(values);
    }

    public static ClusteringBoundary<ByteBuffer> create(Kind kind, ByteBuffer[] values)
    {
        Preconditions.checkArgument(kind.isBoundary(), "Expected boundary clustering kind, got %s", kind);
        return new BufferClusteringBoundary(kind, values);
    }

    @Override
    public ClusteringBoundary<ByteBuffer> invert()
    {
        return create(kind().invert(), values);
    }

    @Override
    public ClusteringBoundary<ByteBuffer> clone(ByteBufferCloner cloner)
    {
        return (ClusteringBoundary<ByteBuffer>) super.clone(cloner);
    }

    public ClusteringBound<ByteBuffer> openBound(boolean reversed)
    {
        return BufferClusteringBound.create(kind.openBoundOfBoundary(reversed), values);
    }

    public ClusteringBound<ByteBuffer> closeBound(boolean reversed)
    {
        return BufferClusteringBound.create(kind.closeBoundOfBoundary(reversed), values);
    }
}
