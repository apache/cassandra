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

package org.apache.cassandra.index.sai.disk.v9;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.EnumSet;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v8.V8OnDiskFormat;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Updates SAI OnDiskFormat to separate partition key and clustering key components.
 */
public class V9OnDiskFormat extends V8OnDiskFormat
{
    @VisibleForTesting
    protected static final Set<IndexComponentType> SKINNY_PER_SSTABLE_COMPONENTS = EnumSet.of(IndexComponentType.GROUP_COMPLETION_MARKER,
                                                                                              IndexComponentType.GROUP_META,
                                                                                              IndexComponentType.ROW_TO_TOKEN,
                                                                                              IndexComponentType.ROW_TO_PARTITION,
                                                                                              IndexComponentType.PARTITION_TO_SIZE,
                                                                                              IndexComponentType.PARTITION_KEY_BLOCKS,
                                                                                              IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS);

    @VisibleForTesting
    protected static final Set<IndexComponentType> WIDE_PER_SSTABLE_COMPONENTS = EnumSet.of(IndexComponentType.GROUP_COMPLETION_MARKER,
                                                                                            IndexComponentType.GROUP_META,
                                                                                            IndexComponentType.ROW_TO_TOKEN,
                                                                                            IndexComponentType.ROW_TO_PARTITION,
                                                                                            IndexComponentType.PARTITION_TO_SIZE,
                                                                                            IndexComponentType.PARTITION_KEY_BLOCKS,
                                                                                            IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS,
                                                                                            IndexComponentType.CLUSTERING_KEY_BLOCKS,
                                                                                            IndexComponentType.CLUSTERING_KEY_BLOCK_OFFSETS);

    public static final V9OnDiskFormat instance = new V9OnDiskFormat();

    @Override
    public PrimaryKey.Factory newPrimaryKeyFactory(ClusteringComparator comparator)
    {
        return new V9RowAwarePrimaryKeyFactory(comparator);
    }

    @Override
    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(IndexComponents.ForRead perSSTableComponents, PrimaryKey.Factory primaryKeyFactory, SSTableReader sstable)
    {
        Preconditions.checkArgument(primaryKeyFactory instanceof V9RowAwarePrimaryKeyFactory);
        V9RowAwarePrimaryKeyFactory rowAwareFactory = (V9RowAwarePrimaryKeyFactory) primaryKeyFactory;
        return rowAwareFactory.hasClustering ? new WidePrimaryKeyMap.Factory(perSSTableComponents, rowAwareFactory, sstable)
                                             : new SkinnyPrimaryKeyMap.Factory(perSSTableComponents, rowAwareFactory, sstable);
    }

    @Override
    public PerSSTableWriter newPerSSTableWriter(IndexDescriptor indexDescriptor) throws IOException
    {
        return new V9SSTableComponentsWriter(indexDescriptor.newPerSSTableComponentsForWrite());
    }

    @Override
    public Set<IndexComponentType> perSSTableComponentTypes(boolean hasClustering)
    {
        return hasClustering ? WIDE_PER_SSTABLE_COMPONENTS : SKINNY_PER_SSTABLE_COMPONENTS;
    }

    @Override
    public int openFilesPerSSTable(boolean hasClustering)
    {
        // For the V9 format the number of open files depends on whether the table has clustering.
        // The number of open files correspond to the number of components except {@link IndexComponentType.GROUP_COMPLETION_MARKER}.
        return (hasClustering ? SKINNY_PER_SSTABLE_COMPONENTS.size() : WIDE_PER_SSTABLE_COMPONENTS.size()) - 1;
    }

    @Override
    public ByteOrder byteOrderFor(IndexComponentType indexComponentType, IndexContext context)
    {
        // The little-endian files are written by Lucene, and the upgrade to Lucene 9 switched the byte order from big to little.
        switch (indexComponentType)
        {
            case META:
            case GROUP_META:
            case ROW_TO_TOKEN:
            case ROW_TO_PARTITION:
            case PARTITION_TO_SIZE:
            case PARTITION_KEY_BLOCKS:
            case CLUSTERING_KEY_BLOCKS:
            case PARTITION_KEY_BLOCK_OFFSETS:
            case CLUSTERING_KEY_BLOCK_OFFSETS:
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
