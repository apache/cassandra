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

import org.apache.cassandra.utils.ObjectSizes;

/**
 * The clustering column values for a row.
 * <p>
 * A {@code Clustering} is a {@code ClusteringPrefix} that must always be "complete", i.e. have
 * as many values as there is clustering columns in the table it is part of. It is the clustering
 * prefix used by rows.
 * <p>
 * Note however that while it's size must be equal to the table clustering size, a clustering can have
 * {@code null} values (this is currently only allowed in COMPACT table for historical reasons, but we
 * could imagine lifting that limitation if we decide it make sense from a CQL point of view).
 */
public class BufferClustering extends AbstractBufferClusteringPrefix implements Clustering<ByteBuffer>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new BufferClustering(EMPTY_VALUES_ARRAY));

    public BufferClustering(ByteBuffer... values)
    {
        super(Kind.CLUSTERING, values);
    }

    public long unsharedHeapSize()
    {
        if (this == Clustering.EMPTY || this == Clustering.STATIC_CLUSTERING)
            return 0;
        return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(values);
    }

    public long unsharedHeapSizeExcludingData()
    {
        if (this == Clustering.EMPTY || this == Clustering.STATIC_CLUSTERING)
            return 0;
        return EMPTY_SIZE + ObjectSizes.sizeOnHeapExcludingDataOf(values);
    }

    public static BufferClustering make(ByteBuffer... values)
    {
        return new BufferClustering(values);
    }
}
