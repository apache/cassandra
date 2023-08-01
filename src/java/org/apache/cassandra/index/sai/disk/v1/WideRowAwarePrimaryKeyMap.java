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

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.bitpack.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.sortedterms.SortedTermsReader;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An extension of the {@link SkinnyRowAwarePrimaryKeyMap} for wide tables (those with clustering columns).
 * <p>
 * While the {@link WideRowAwarePrimaryKeyMapFactory} is threadsafe, individual instances of the {@link WideRowAwarePrimaryKeyMap}
 * are not.
 */
@NotThreadSafe
public class WideRowAwarePrimaryKeyMap extends SkinnyRowAwarePrimaryKeyMap
{
    @ThreadSafe
    public static class WideRowAwarePrimaryKeyMapFactory extends SkinnyRowAwarePrimaryKeyMapFactory
    {
        private final ClusteringComparator clusteringComparator;
        protected final LongArray.Factory partitionReaderFactory;
        protected FileHandle partitionsFile;

        public WideRowAwarePrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable)
        {
            super(indexDescriptor, sstable);

            try
            {
                this.clusteringComparator = indexDescriptor.clusteringComparator;
                NumericValuesMeta partitionsMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.PARTITION_SIZES)));
                this.partitionsFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PARTITION_SIZES);
                this.partitionReaderFactory = new MonotonicBlockPackedReader(partitionsFile, partitionsMeta);
            }
            catch (Throwable t)
            {
                throw Throwables.unchecked(Throwables.close(t, partitionsFile));
            }
        }

        @Override
        @SuppressWarnings({ "resource", "RedundantSuppression" })
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap() throws IOException
        {
            LongArray rowIdToToken = new LongArray.DeferredLongArray(tokenReaderFactory::open);
            LongArray partitionIdToToken = new LongArray.DeferredLongArray(partitionReaderFactory::open);

            return new WideRowAwarePrimaryKeyMap(rowIdToToken,
                                                 partitionIdToToken,
                                                 sortedTermsReader.openCursor(),
                                                 partitioner,
                                                 primaryKeyFactory,
                                                 clusteringComparator);
        }

        @Override
        public void close()
        {
            super.close();
            FileUtils.closeQuietly(partitionsFile);
        }
    }

    private final ClusteringComparator clusteringComparator;
    private final LongArray partitionArray;

    private WideRowAwarePrimaryKeyMap(LongArray tokenArray,
                                      LongArray partitionArray,
                                      SortedTermsReader.Cursor cursor,
                                      IPartitioner partitioner,
                                      PrimaryKey.Factory primaryKeyFactory,
                                      ClusteringComparator clusteringComparator)
    {
        super(tokenArray, cursor, partitioner, primaryKeyFactory);

        this.clusteringComparator = clusteringComparator;
        this.partitionArray = partitionArray;
    }

    @Override
    public long rowIdFromPrimaryKey(PrimaryKey key)
    {
        long rowId = tokenArray.indexOf(key.token().getLongValue());
        // If the key only has a token (initial range skip in the query), the token is out of range,
        // or we have skipped a token, return the rowId from the token array.
        if (key.isTokenOnly() || rowId < 0 || tokenArray.get(rowId) != key.token().getLongValue())
            return rowId;

        // Search the sorted terms for the key in the same partition
        return cursor.partitionedSeekToTerm(key, rowId, startOfNextPartition(rowId));
    }

    @Override
    public void close()
    {
        super.close();
        FileUtils.closeQuietly(partitionArray);
    }

    @Override
    protected PrimaryKey supplier(long sstableRowId)
    {
        ByteSource.Peekable peekable = ByteSource.peekable(cursor.seekForwardToPointId(sstableRowId).asComparableBytes(ByteComparable.Version.OSS50));

        byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable));

        assert keyBytes != null;

        ByteBuffer decoratedKey = ByteBuffer.wrap(keyBytes);
        DecoratedKey partitionKey = new BufferDecoratedKey(partitioner.getToken(decoratedKey), decoratedKey);

        Clustering<?> clustering = clusteringComparator.clusteringFromByteComparable(ByteBufferAccessor.instance,
                                                                                     v -> ByteSourceInverse.nextComponentSource(peekable));

        if (clustering == null)
            clustering = Clustering.EMPTY;

        return primaryKeyFactory.create(partitionKey, clustering);
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
