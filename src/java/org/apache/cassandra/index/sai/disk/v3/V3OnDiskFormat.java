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

package org.apache.cassandra.index.sai.disk.v3;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v2.V2OnDiskFormat;

public class V3OnDiskFormat extends V2OnDiskFormat
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final V3OnDiskFormat instance = new V3OnDiskFormat();

    public static final Set<IndexComponent> VECTOR_COMPONENTS_V3 = EnumSet.of(IndexComponent.COLUMN_COMPLETION_MARKER,
                                                                              IndexComponent.META,
                                                                              IndexComponent.PQ,
                                                                              IndexComponent.TERMS_DATA,
                                                                              IndexComponent.POSTING_LISTS);

    private static final IndexFeatureSet v3IndexFeatureSet = new IndexFeatureSet()
    {
        @Override
        public boolean isRowAware()
        {
            return true;
        }

        @Override
        public boolean hasVectorIndexChecksum()
        {
            return false;
        }
    };

    @Override
    public IndexFeatureSet indexFeatureSet()
    {
        return v3IndexFeatureSet;
    }

    @Override
    public IndexSearcher newIndexSearcher(SSTableContext sstableContext,
                                          IndexContext indexContext,
                                          PerIndexFiles indexFiles,
                                          SegmentMetadata segmentMetadata) throws IOException
    {
        if (indexContext.isVector())
            return new V3VectorIndexSearcher(sstableContext.primaryKeyMapFactory(), indexFiles, segmentMetadata, sstableContext.indexDescriptor, indexContext);
        return super.newIndexSearcher(sstableContext, indexContext, indexFiles, segmentMetadata);
    }

    @Override
    public Set<IndexComponent> perIndexComponents(IndexContext indexContext)
    {
        // VSTODO add checksums and actual validation
        if (indexContext.isVector())
            return VECTOR_COMPONENTS_V3;
        return super.perIndexComponents(indexContext);
    }
}
