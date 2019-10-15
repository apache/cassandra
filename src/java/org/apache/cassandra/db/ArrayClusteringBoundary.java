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

import org.apache.cassandra.utils.memory.AbstractAllocator;

public class ArrayClusteringBoundary extends ArrayClusteringBoundOrBoundary implements ClusteringBoundary<byte[]>
{
    public ArrayClusteringBoundary(Kind kind, byte[][] values)
    {
        super(kind, values);
    }

    public static ClusteringBoundary<byte[]> create(Kind kind, byte[][] values)
    {
        assert kind.isBoundary();
        return new ArrayClusteringBoundary(kind, values);
    }

    @Override
    public ClusteringBoundary<byte[]> invert()
    {
        return create(kind().invert(), values);
    }

    @Override
    public ClusteringBoundary<byte[]> copy(AbstractAllocator allocator)
    {
        return (ClusteringBoundary<byte[]>) super.copy(allocator);
    }

    public ClusteringBound<byte[]> openBound(boolean reversed)
    {
        return ArrayClusteringBound.create(kind.openBoundOfBoundary(reversed), values);
    }

    public ClusteringBound<byte[]> closeBound(boolean reversed)
    {
        return ArrayClusteringBound.create(kind.closeBoundOfBoundary(reversed), values);
    }
}
