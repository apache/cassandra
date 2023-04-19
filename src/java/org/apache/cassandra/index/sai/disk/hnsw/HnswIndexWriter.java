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

package org.apache.cassandra.index.sai.disk.hnsw;

import java.io.Closeable;
import java.io.IOException;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.OnHeapHnswGraph;

public class HnswIndexWriter implements Closeable
{
    private final IndexDescriptor indexDescriptor;
    private final IndexContext indexContext;
    private final boolean segmented;
    private OnHeapHnswGraph hnsw;

    public HnswIndexWriter(IndexDescriptor indexDescriptor, IndexContext indexContext, boolean segmented)
    {
        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;
        this.segmented = segmented;
    }

    public SegmentMetadata.ComponentMetadataMap writeAll(MemtableFloat32VectorValues vectors)
    {
        int seed = (int) (Math.random() * Integer.MAX_VALUE);
        // TODO expose M and beamWidth as configurable parameters
        var builder = HnswGraphBuilder.create(vectors, VectorEncoding.FLOAT32, VectorSimilarityFunction.COSINE, 16, 100, seed);
        hnsw = builder.build(vectors.copy());
    }

    @Override
    public void close() throws IOException
    {
        // no-op; all the world is done in writeAll
    }

    public long getNodeCount()
    {
        return hnsw.size();
    }
}
