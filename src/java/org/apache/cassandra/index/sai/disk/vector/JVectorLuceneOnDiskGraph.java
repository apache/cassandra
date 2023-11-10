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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;

import org.slf4j.Logger;

import io.github.jbellis.jvector.util.Bits;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.VectorPostingList;

/**
 * A common interface between Lucene and JVector graph indexes
 */
public abstract class JVectorLuceneOnDiskGraph implements AutoCloseable
{
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(JVectorLuceneOnDiskGraph.class);

    protected final PerIndexFiles indexFiles;
    protected final SegmentMetadata.ComponentMetadataMap componentMetadatas;

    protected JVectorLuceneOnDiskGraph(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerIndexFiles indexFiles)
    {
        this.componentMetadatas = componentMetadatas;
        this.indexFiles = indexFiles;
    }

    public abstract long ramBytesUsed();

    public abstract int size();

    public abstract OrdinalsView getOrdinalsView() throws IOException;

    /**
     * See CassandraDiskANN::search
     */
    public abstract VectorPostingList search(float[] queryVector, int topK, int limit, Bits bits, QueryContext context);

    /**
     * See CassandraDiskANN::search
     */
    public abstract VectorPostingList search(float[] queryVector, int topK, float threshold, int limit, Bits bits, QueryContext context);

    public abstract void close() throws IOException;

    protected SegmentMetadata.ComponentMetadata getComponentMetadata(IndexComponent component)
    {
        try
        {
            return componentMetadatas.get(component);
        }
        catch (IllegalArgumentException e)
        {
            logger.warn("Component metadata is missing " + component + ", assuming single segment");
            // take our best guess and assume there is a single segment
            // this will silently use the wrong data if it is actually a multi-segment file
            var file = indexFiles.getFile(component);
            // 7 is the length of the header written by SAICodecUtils
            return new SegmentMetadata.ComponentMetadata(-1, 7, file.onDiskLength - 7); // graph indexes ignore root
        }
    }
}
