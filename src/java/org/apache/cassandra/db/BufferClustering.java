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

/**
 * The clustering column values for a row.
 * <p>
 * A {@code Clustering} is a {@code ClusteringPrefix} that must always be "complete", i.e. have
 * as many values as there is clustering columns in the table it is part of. It is the clustering
 * prefix used by rows.
 * <p>
 * Note however that while it's size must be equal to the table clustering size, a clustering can have
 * {@code null} values, and this mostly for thrift backward compatibility (in practice, if a value is null,
 * all of the following ones will be too because that's what thrift allows, but it's never assumed by the
 * code so we could start generally allowing nulls for clustering columns if we wanted to).
 */
public class BufferClustering extends AbstractBufferClusteringPrefix implements Clustering
{
    BufferClustering(ByteBuffer... values)
    {
        super(Kind.CLUSTERING, values);
    }

    public ClusteringPrefix minimize()
    {
        if (!ByteBufferUtil.canMinimize(values))
            return this;
        return new BufferClustering(ByteBufferUtil.minimizeBuffers(values));    }
}
