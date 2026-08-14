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

package org.apache.cassandra.index.sai.disk.v2;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.EnumSet;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Updates SAI OnDiskFormat to include full PK -> offset mapping, and adds vector components.
 */
public class V2OnDiskFormat extends V1OnDiskFormat
{
    private static final Set<IndexComponentType> PER_SSTABLE_COMPONENTS = EnumSet.of(IndexComponentType.GROUP_COMPLETION_MARKER,
                                                                                     IndexComponentType.GROUP_META,
                                                                                     IndexComponentType.TOKEN_VALUES,
                                                                                     IndexComponentType.PRIMARY_KEY_TRIE,
                                                                                     IndexComponentType.PRIMARY_KEY_BLOCKS,
                                                                                     IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS);

    public static final Set<IndexComponentType> VECTOR_COMPONENTS_V2 = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                                  IndexComponentType.META,
                                                                                  IndexComponentType.VECTOR,
                                                                                  IndexComponentType.TERMS_DATA,
                                                                                  IndexComponentType.POSTING_LISTS);

    public static final V2OnDiskFormat instance = new V2OnDiskFormat();

    private static final IndexFeatureSet v2IndexFeatureSet = new IndexFeatureSet()
    {
        @Override
        public boolean isRowAware()
        {
            return true;
        }

        @Override
        public boolean hasTermsHistogram()
        {
            return false;
        }
    };

    protected V2OnDiskFormat()
    {}

    @Override
    public IndexFeatureSet indexFeatureSet()
    {
        return v2IndexFeatureSet;
    }

    @Override
    public PrimaryKey.Factory newPrimaryKeyFactory(ClusteringComparator comparator)
    {
        return new V2RowAwarePrimaryKeyFactory(comparator);
    }

    @Override
    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(IndexComponents.ForRead perSSTableComponents, PrimaryKey.Factory primaryKeyFactory, SSTableReader sstable)
    {
        Preconditions.checkArgument(primaryKeyFactory instanceof V2RowAwarePrimaryKeyFactory);
        return new RowAwarePrimaryKeyMap.RowAwarePrimaryKeyMapFactory(perSSTableComponents, (V2RowAwarePrimaryKeyFactory) primaryKeyFactory, sstable);
    }

    @Override
    public IndexSearcher newIndexSearcher(SSTableContext sstableContext,
                                          IndexContext indexContext,
                                          PerIndexFiles indexFiles,
                                          SegmentMetadata segmentMetadata) throws IOException
    {
        if (indexContext.isVector())
            throw new IllegalStateException("V2 (HNSW) vector index support has been removed");
        if (indexContext.isLiteral())
            return new V2InvertedIndexSearcher(sstableContext, indexFiles, segmentMetadata, indexContext);
        return super.newIndexSearcher(sstableContext, indexContext, indexFiles, segmentMetadata);
    }

    @Override
    public PerSSTableWriter newPerSSTableWriter(IndexDescriptor indexDescriptor) throws IOException
    {
        return new V2SSTableComponentsWriter(indexDescriptor.newPerSSTableComponentsForWrite());
    }

    @Override
    public Set<IndexComponentType> perIndexComponentTypes(AbstractType<?> validator)
    {
        if (validator.isVector())
            return VECTOR_COMPONENTS_V2;
        return super.perIndexComponentTypes(validator);
    }

    @Override
    public Set<IndexComponentType> perSSTableComponentTypes(boolean hasClustering)
    {
        return PER_SSTABLE_COMPONENTS;
    }

    @Override
    public int openFilesPerSSTable(boolean hasClustering)
    {
        return 4;
    }

    @Override
    public ByteOrder byteOrderFor(IndexComponentType indexComponentType, IndexContext context)
    {
        // The little-endian files are written by Lucene, and the upgrade to Lucene 9 switched the byte order from big to little.
        switch (indexComponentType)
        {
            case META:
            case GROUP_META:
            case TOKEN_VALUES:
            case PRIMARY_KEY_BLOCK_OFFSETS:
            case KD_TREE:
            case KD_TREE_POSTING_LISTS:
                return ByteOrder.LITTLE_ENDIAN;
            case POSTING_LISTS:
                return (context != null && context.isVector()) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
            default:
                return ByteOrder.BIG_ENDIAN;
        }
    }
}
