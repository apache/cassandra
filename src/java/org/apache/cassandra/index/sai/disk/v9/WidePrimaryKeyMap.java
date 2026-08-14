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
import java.util.Arrays;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.bitpack.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v2.PrimaryKeyWithSource;
import org.apache.cassandra.index.sai.disk.v9.keystore.KeyLookup;
import org.apache.cassandra.index.sai.disk.v9.keystore.KeyLookupMeta;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * An extension of the {@link SkinnyPrimaryKeyMap} for wide tables (those with clustering columns).
 * <p>
 * This used the following additional on-disk structures to the {@link SkinnyPrimaryKeyMap}
 * <ul>
 *     <li>A block-packed structure for partitionId to partition size (number of rows in the partition) lookups using
 *     {@link BlockPackedReader}. Uses the {@link IndexComponentType#PARTITION_TO_SIZE} component</li>
 *     <li>A key store for rowId to {@link Clustering} and {@link Clustering} to rowId lookups using
 *     {@link KeyLookup}. Uses the {@link org.apache.cassandra.index.sai.disk.format.IndexComponentType#CLUSTERING_KEY_BLOCKS} and
 *     {@link org.apache.cassandra.index.sai.disk.format.IndexComponentType#CLUSTERING_KEY_BLOCK_OFFSETS} components</li>
 * </ul>
 * While the {@link Factory} is thread-safe, individual instances of the {@link WidePrimaryKeyMap}
 * are not.
 */
@NotThreadSafe
public class WidePrimaryKeyMap extends SkinnyPrimaryKeyMap
{
    @ThreadSafe
    public static class Factory extends SkinnyPrimaryKeyMap.Factory
    {
        // The class member is needed to avoid memory leaks and to be addressed by CNDB-17902
        private final ClusteringComparator clusteringComparator;
        private final KeyLookup clusteringKeyReader;
        private final LongArray.Factory partitionToSizeReaderFactory;
        private final FileHandle clusteringKeyBlockOffsetsFile;
        private final FileHandle clustingingKeyBlocksFile;
        private final FileHandle partitionToSizeFile;

        public Factory(IndexComponents.ForRead perSSTableComponents,
                       V9RowAwarePrimaryKeyFactory primaryKeyFactory,
                       SSTableReader sstable)
        {
            super(perSSTableComponents, primaryKeyFactory, sstable);

            FileHandle clusteringKeyBlockOffsetsFileLocal = null;
            FileHandle clustingingKeyBlocksFileLocal = null;
            FileHandle partitionToSizeFileLocal = null;

            try
            {
                clusteringKeyBlockOffsetsFileLocal = perSSTableComponents.get(IndexComponentType.CLUSTERING_KEY_BLOCK_OFFSETS).createFileHandle();
                clustingingKeyBlocksFileLocal = perSSTableComponents.get(IndexComponentType.CLUSTERING_KEY_BLOCKS).createFileHandle();

                NumericValuesMeta partitionSizeMeta = new NumericValuesMeta(metadataSource.get(perSSTableComponents.get(IndexComponentType.PARTITION_TO_SIZE)));
                partitionToSizeFileLocal = perSSTableComponents.get(IndexComponentType.PARTITION_TO_SIZE).createFileHandle();
                this.partitionToSizeReaderFactory = new BlockPackedReader(partitionToSizeFileLocal, partitionSizeMeta);

                NumericValuesMeta clusteringKeyBlockOffsetsMeta = new NumericValuesMeta(metadataSource.get(perSSTableComponents.get(IndexComponentType.CLUSTERING_KEY_BLOCK_OFFSETS)));
                KeyLookupMeta clusteringKeyMeta = new KeyLookupMeta(metadataSource.get(perSSTableComponents.get(IndexComponentType.CLUSTERING_KEY_BLOCKS)));
                this.clusteringKeyReader = new KeyLookup(clustingingKeyBlocksFileLocal, clusteringKeyBlockOffsetsFileLocal,
                                                         clusteringKeyMeta, clusteringKeyBlockOffsetsMeta);
            }
            catch (IOException e)
            {
                throw Throwables.unchecked(Throwables.close(e, clusteringKeyBlockOffsetsFileLocal, clustingingKeyBlocksFileLocal, partitionToSizeFileLocal));
            }
            this.clusteringComparator = sstable.metadata().comparator;

            this.clusteringKeyBlockOffsetsFile = clusteringKeyBlockOffsetsFileLocal;
            this.clustingingKeyBlocksFile = clustingingKeyBlocksFileLocal;
            this.partitionToSizeFile = partitionToSizeFileLocal;
        }

        @Override
        @SuppressWarnings({ "resource", "RedundantSuppression" })
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap()
        {
            LongArray rowIdToToken = new LongArray.DeferredLongArray(rowToTokenReaderFactory::open);
            LongArray partitionIdToToken = new LongArray.DeferredLongArray(rowToPartitionReaderFactory::open);
            LongArray partitionIdToSize = new LongArray.DeferredLongArray(partitionToSizeReaderFactory::open);

            return new WidePrimaryKeyMap(rowIdToToken,
                                         partitionIdToToken,
                                         partitionIdToSize,
                                         partitionKeyReader.openCursor(),
                                         clusteringKeyReader.openCursor(),
                                         partitioner,
                                         primaryKeyFactory,
                                         clusteringComparator,
                                         sstableId,
                                         hasStaticColumns);
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(Arrays.asList(clustingingKeyBlocksFile, clusteringKeyBlockOffsetsFile, partitionToSizeFile));
            super.close();
        }
    }

    private final LongArray partitionIdToSizeArray;
    private final ClusteringComparator clusteringComparator;
    private final KeyLookup.Cursor clusteringKeyCursor;

    private WidePrimaryKeyMap(LongArray rowIdToTokenArray,
                              LongArray rowIdToPartitionIdArray,
                              LongArray partitionIdToSizeArray,
                              KeyLookup.Cursor partitionKeyCursor,
                              KeyLookup.Cursor clusteringKeyCursor,
                              IPartitioner partitioner,
                              V9RowAwarePrimaryKeyFactory primaryKeyFactory,
                              ClusteringComparator clusteringComparator, 
                              SSTableId<?> sstableId,
                              boolean hasStaticColumns)
    {
        super(rowIdToTokenArray, rowIdToPartitionIdArray, partitionKeyCursor, partitioner, primaryKeyFactory,
              sstableId, hasStaticColumns);

        this.partitionIdToSizeArray = partitionIdToSizeArray;
        this.clusteringComparator = clusteringComparator;
        this.clusteringKeyCursor = clusteringKeyCursor;
    }

    /**
     * Returns a row Id for a {@link PrimaryKey}. If there is no such term,
     * returns the `-(next row id) - 1` where `next row id` is the row id
     * of the next greatest {@link PrimaryKey} in the map.
     *
     * @param key the {@link PrimaryKey} to lookup
     * @return a row id
     */
    @Override
    public long exactRowIdOrInvertedCeiling(PrimaryKey key)
    {
        if (key instanceof PrimaryKeyWithSource)
        {
            PrimaryKeyWithSource pkws = (PrimaryKeyWithSource) key;
            if (pkws.getSourceSstableId().equals(sstableId))
                return pkws.getSourceRowId();
        }

        // Find the partition using the token array for initial lookup
        long rowId = rowIdToTokenArray.indexOf(key.token().getLongValue());
        if (key.isTokenOnly() || rowId < 0)
            return rowId;
        // If we have skipped a token (shouldn't happen with indexOf, but check for safety)
        if (rowIdToTokenArray.get(rowId) != key.token().getLongValue())
            return rowId;

        // Handle token collisions by comparing partition keys using partitionKeyCursor
        rowId = tokenCeilingCollisionDetection(key, rowId);
        if (key.clustering().isEmpty())
            return rowId;

        // Now search within the partition for the clustering key
        long nextPartitionStart = jumpToNextPartitionStart(rowId);
        long clusteringRowId = clusteringKeyCursor.clusteredSeekToKey(
        clusteringComparator.asByteComparable(key.clustering()), rowId, nextPartitionStart);

        // clusteredSeekToKey returns the ceiling (next greater or equal key) or -1 if not found
        if (clusteringRowId < 0)
            return Long.MIN_VALUE;
        assert clusteringRowId < rowIdToTokenArray.length() : "Row ID should not be after the last row";

        // If clusteringRowId points to the next partition, it means the search key is greater
        // than all keys in the current partition. Return the inverted ceiling.
        if (clusteringRowId >= nextPartitionStart)
            return ~clusteringRowId;

        Clustering<?> foundClustering = readClusteringKey(clusteringRowId);
        // If STATIC CLUSTERING, then no clustering key is present.
        if (foundClustering.isEmpty())
            return ~clusteringRowId;

        // Check if this is an exact match by comparing the clustering key
        int cmp = clusteringComparator.compare(foundClustering, key.clustering());
        if (cmp == 0)
            return clusteringRowId;
        else
            return ~clusteringRowId;
    }

    /**
     * Returns the row ID of the smallest primary key greater than or equal to the given key.
     * Returns -1 if no such key exists (i.e., the given key is greater than all keys in the map).
     * <p>
     * For wide tables, this method leverages {@link #exactRowIdOrInvertedCeiling(PrimaryKey)}
     * and converts the inverted ceiling format to a regular ceiling.
     *
     * @param key the primary key to find the ceiling for
     * @return the row ID of the ceiling key, or -1 if no ceiling exists
     */
    @Override
    public long ceiling(PrimaryKey key)
    {
        long rowId = exactRowIdOrInvertedCeiling(key);
        if (rowId >= 0)
            return rowId;
        else if (rowId == Long.MIN_VALUE)
            return -1;
        else
            return ~rowId;
    }

    /**
     * Returns the row ID of the greatest primary key less than or equal to the given key.
     * Returns -1 if no such key exists (i.e., the given key is less than all keys in the map).
     * <p>
     * For wide tables, this method handles both token-only keys and full primary keys with clustering.
     * For token-only keys, it returns the last row of the matching partition if found.
     *
     * @param key the primary key to find the floor for
     * @return the row ID of the floor key, or -1 if no floor exists
     */
    @Override
    public long floor(PrimaryKey key)
    {
        if (key instanceof PrimaryKeyWithSource)
        {
            PrimaryKeyWithSource pkws = (PrimaryKeyWithSource) key;
            if (pkws.getSourceSstableId().equals(sstableId))
                return pkws.getSourceRowId();
        }

        long rowId = exactRowIdOrInvertedCeiling(key);

        // Exact match
        if (rowId >= 0)
        {
            // If the key is a prefix (token-only or partition-only), 
            // the floor is the *greatest* row ID associated with this prefix.
            if (key.isTokenOnly() || key.clustering().isEmpty())
                return startOfNextPartition(rowId) - 1;

            // If STATIC CLUSTERING, then rowID is the last row in the matching partition.
            // Floor reverts to return the first row in the partition instead.
            if (readClusteringKey(rowId).isEmpty())
            {
                long partitionId = rowIdToPartitionIdArray.get(rowId);
                return rowIdToPartitionIdArray.ceilingIndex(partitionId);
            }

            return rowId;
        }

        if (rowId == Long.MIN_VALUE)
            return rowIdToTokenArray.length() - 1;

        // rowId is -(ceiling) - 1. The floor is the row immediately before the ceiling.
        return -rowId - 2;
    }

    @Override
    public void close()
    {
        super.close();
        FileUtils.closeQuietly(clusteringKeyCursor, partitionIdToSizeArray);
    }

    @Override
    protected PrimaryKey supplier(long sstableRowId)
    {
        return primaryKeyFactory.create(readPartitionKey(sstableRowId), readClusteringKey(sstableRowId));
    }

    private Clustering<?> readClusteringKey(long sstableRowId)
    {
        ByteSource.Peekable peekable = ByteSource.peekable(clusteringKeyCursor.seekToPointId(sstableRowId)
                                                                              .asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));

        Clustering<?> clustering = clusteringComparator.clusteringFromByteComparable(ByteBufferAccessor.instance, v -> peekable, TypeUtil.BYTE_COMPARABLE_VERSION);

        if (clustering == null)
            clustering = Clustering.EMPTY;

        return clustering;
    }

    // Returns the rowId of the next partition or the number of rows if supplied rowId is in the last partition.
    // Requires that given row id is the first row in the current partition
    private long jumpToNextPartitionStart(long partitionStartRowId)
    {
        long partitionSize = partitionIdToSizeArray.get(rowIdToPartitionIdArray.get(partitionStartRowId));
        return partitionSize == -1 ? rowIdToPartitionIdArray.length() : partitionStartRowId + partitionSize;
    }

    // Returns the first rowId of the next partition or the number of rows if supplied rowId is in the last partition
    private long startOfNextPartition(long rowId)
    {
        long partitionId = rowIdToPartitionIdArray.get(rowId);
        long partitionSize = partitionIdToSizeArray.get(partitionId);
        if (partitionSize == -1)
            return rowIdToPartitionIdArray.length();

        // Find the first row of this partition, then add partition size
        long firstRowOfPartition = rowIdToPartitionIdArray.ceilingIndex(partitionId);
        return firstRowOfPartition + partitionSize;
    }
}
