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

import org.apache.cassandra.utils.memory.AbstractAllocator;

public class ArrayClusteringBound extends ArrayClusteringBoundOrBoundary implements ClusteringBound<byte[]>
{
    public ArrayClusteringBound(Kind kind, byte[][] values)
    {
        super(kind, values);
    }

    @Override
    public ClusteringBound<byte[]> invert()
    {
        return create(kind().invert(), values);
    }

    public ClusteringBound<byte[]> copy(AbstractAllocator allocator)
    {
        return (ClusteringBound<byte[]>) super.copy(allocator);
    }

    public static ArrayClusteringBound create(ClusteringPrefix.Kind kind, byte[][] values)
    {
        assert !kind.isBoundary();
        return new ArrayClusteringBound(kind, values);
    }

    public static ArrayClusteringBound inclusiveStartOf(byte[]... values)
    {
        return create(ClusteringPrefix.Kind.INCL_START_BOUND, values);
    }

    public static ArrayClusteringBound inclusiveEndOf(byte[]... values)
    {
        return create(ClusteringPrefix.Kind.INCL_END_BOUND, values);
    }

    public static ArrayClusteringBound exclusiveStartOf(byte[]... values)
    {
        return create(ClusteringPrefix.Kind.EXCL_START_BOUND, values);
    }

    public static ArrayClusteringBound exclusiveEndOf(byte[]... values)
    {
        return create(ClusteringPrefix.Kind.EXCL_END_BOUND, values);
    }

    public static ClusteringBound create(ClusteringComparator comparator, boolean isStart, boolean isInclusive, Object... values)
    {
        CBuilder builder = CBuilder.create(comparator);
        for (Object val : values)
        {
            assert !(val instanceof byte[]) : "FIXME";
            if (val instanceof ByteBuffer)
                builder.add((ByteBuffer) val);
            else
                builder.add(val);
        }
        return builder.buildBound(isStart, isInclusive);
    }
}
