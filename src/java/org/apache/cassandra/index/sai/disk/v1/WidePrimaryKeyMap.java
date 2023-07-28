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

package org.apache.cassandra.index.sai.disk.v1;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.keystore.KeyLookupMeta;
import org.apache.cassandra.index.sai.disk.v1.keystore.KeyLookup;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.Arrays;

/**
 * An extension of the {@link SkinnyPrimaryKeyMap} for wide tables (those with clustering columns).
 * <p>
 * This used the following additional on-disk structures to the {@link SkinnyPrimaryKeyMap}
 * <ul>
 *     <li>A key store for rowId to {@link Clustering} and {@link Clustering} to rowId lookups using
 *     {@link KeyLookup}. Uses the {@link IndexComponent#CLUSTERING_KEY_BLOCKS} and
 *     {@link IndexComponent#CLUSTERING_KEY_BLOCK_OFFSETS} components</li>
 * </ul>
 * While the {@link Factory} is threadsafe, individual instances of the {@link WidePrimaryKeyMap}
 * are not.
 */
@NotThreadSafe
public class WidePrimaryKeyMap extends SkinnyPrimaryKeyMap
{
    @ThreadSafe
    public static class Factory extends SkinnyPrimaryKeyMap.Factory
    {
        private final ClusteringComparator clusteringComparator;
        private final KeyLookup clusteringKeyReader;
        private final FileHandle clusteringKeyBlockOffsetsFile;
        private final FileHandle clustingingKeyBlocksFile;

        public Factory(IndexDescriptor indexDescriptor, SSTableReader sstable)
        {
            super(indexDescriptor, sstable);

            this.clusteringKeyBlockOffsetsFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.CLUSTERING_KEY_BLOCK_OFFSETS, this::close);
            this.clustingingKeyBlocksFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.CLUSTERING_KEY_BLOCKS, this::close);

            try
            {
                this.clusteringComparator = indexDescriptor.clusteringComparator;
                NumericValuesMeta clusteringKeyBlockOffsetsMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.CLUSTERING_KEY_BLOCK_OFFSETS)));
                KeyLookupMeta clusteringKeyMeta = new KeyLookupMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.CLUSTERING_KEY_BLOCKS)));
                this.clusteringKeyReader = new KeyLookup(clustingingKeyBlocksFile, clusteringKeyBlockOffsetsFile, clusteringKeyMeta, clusteringKeyBlockOffsetsMeta);
            }
            catch (Throwable t)
            {
                throw Throwables.unchecked(t);
            }
        }

        @Override
        @SuppressWarnings({ "resource", "RedundantSuppression" })
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap() throws IOException
        {
            LongArray rowIdToToken = new LongArray.DeferredLongArray(tokenReaderFactory::open);
            LongArray partitionIdToToken = new LongArray.DeferredLongArray(partitionReaderFactory::open);

            return new WidePrimaryKeyMap(rowIdToToken,
                                         partitionIdToToken,
                                         partitionKeyReader.openCursor(),
                                         clusteringKeyReader.openCursor(),
                                         partitioner,
                                         primaryKeyFactory,
                                         clusteringComparator);
        }

        @Override
        public void close()
        {
            super.close();
            FileUtils.closeQuietly(Arrays.asList(clustingingKeyBlocksFile, clusteringKeyBlockOffsetsFile));
        }
    }

    private final ClusteringComparator clusteringComparator;
    private final KeyLookup.Cursor clusteringKeyCursor;

    private WidePrimaryKeyMap(LongArray tokenArray,
                              LongArray partitionArray,
                              KeyLookup.Cursor partitionKeyCursor,
                              KeyLookup.Cursor clusteringKeyCursor,
                              IPartitioner partitioner,
                              PrimaryKey.Factory primaryKeyFactory,
                              ClusteringComparator clusteringComparator)
    {
        super(tokenArray, partitionArray, partitionKeyCursor, partitioner, primaryKeyFactory);

        this.clusteringComparator = clusteringComparator;
        this.clusteringKeyCursor = clusteringKeyCursor;
    }

    @Override
    public long rowIdFromPrimaryKey(PrimaryKey primaryKey)
    {
        long rowId = tokenArray.indexOf(primaryKey.token().getLongValue());

        // If the key only has a token (initial range skip in the query), the token is out of range,
        // or we have skipped a token, return the rowId from the token array.
        if (primaryKey.isTokenOnly() || rowId < 0 || tokenArray.get(rowId) != primaryKey.token().getLongValue())
            return rowId;

        rowId = tokenCollisionDetection(primaryKey, rowId);

        // Search the key store for the key in the same partition
        return clusteringKeyCursor.clusteredSeekToKey(clusteringComparator.asByteComparable(primaryKey.clustering()), rowId, startOfNextPartition(rowId));
    }

    @Override
    public void close()
    {
        super.close();
        FileUtils.closeQuietly(clusteringKeyCursor);
    }

    @Override
    protected PrimaryKey supplier(long sstableRowId)
    {
        return primaryKeyFactory.create(readPartitionKey(sstableRowId), readClusteringKey(sstableRowId));
    }

    private Clustering<?> readClusteringKey(long sstableRowId)
    {
        ByteSource.Peekable peekable = ByteSource.peekable(clusteringKeyCursor.seekToPointId(sstableRowId)
                                                                              .asComparableBytes(ByteComparable.Version.OSS50));

        Clustering<?> clustering = clusteringComparator.clusteringFromByteComparable(ByteBufferAccessor.instance, v -> peekable);

        if (clustering == null)
            clustering = Clustering.EMPTY;

        return clustering;
    }

    // Returns the rowId of the next partition or the number of rows if supplied rowId is in the last partition
    private long startOfNextPartition(long rowId)
    {
        long partitionId = partitionArray.get(rowId);
        long nextPartitionRowId = partitionArray.indexOf(++partitionId);
        if (nextPartitionRowId == -1)
            nextPartitionRowId = partitionArray.length();
        return nextPartitionRowId;
    }
}
