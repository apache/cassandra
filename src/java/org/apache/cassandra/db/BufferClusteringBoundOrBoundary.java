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

public abstract class BufferClusteringBoundOrBoundary extends AbstractBufferClusteringPrefix implements ClusteringBoundOrBoundary<ByteBuffer>
{
    protected BufferClusteringBoundOrBoundary(Kind kind, ByteBuffer[] values)
    {
        super(kind, values);
        assert values.length > 0 || !kind.isBoundary();
    }

    public static ClusteringBoundOrBoundary create(Kind kind, ByteBuffer[] values)
    {
        return kind.isBoundary()
               ? new BufferClusteringBoundary(kind, values)
               : new BufferClusteringBound(kind, values);
    }

    public static ClusteringBound inclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
    {
        return new BufferClusteringBound(reversed ? ClusteringPrefix.Kind.INCL_END_BOUND : ClusteringPrefix.Kind.INCL_START_BOUND, boundValues);
    }

    public static ClusteringBound exclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
    {
        return new BufferClusteringBound(reversed ? ClusteringPrefix.Kind.EXCL_END_BOUND : ClusteringPrefix.Kind.EXCL_START_BOUND, boundValues);
    }

    public static ClusteringBound inclusiveClose(boolean reversed, ByteBuffer[] boundValues)
    {
        return new BufferClusteringBound(reversed ? ClusteringPrefix.Kind.INCL_START_BOUND : ClusteringPrefix.Kind.INCL_END_BOUND, boundValues);
    }

    public static ClusteringBound exclusiveClose(boolean reversed, ByteBuffer[] boundValues)
    {
        return new BufferClusteringBound(reversed ? ClusteringPrefix.Kind.EXCL_START_BOUND : ClusteringPrefix.Kind.EXCL_END_BOUND, boundValues);
    }

    public static ClusteringBoundary inclusiveCloseExclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
    {
        return new BufferClusteringBoundary(reversed ? ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY : ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY, boundValues);
    }

    public static ClusteringBoundary exclusiveCloseInclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
    {
        return new BufferClusteringBoundary(reversed ? ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY : ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY, boundValues);
    }
}
