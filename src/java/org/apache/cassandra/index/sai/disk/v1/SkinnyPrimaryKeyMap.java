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
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.bitpack.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.sortedterms.SortedTermsMeta;
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
import java.util.Arrays;

/**
 * A row-aware {@link PrimaryKeyMap} for skinny tables (those with no clustering columns).
 * <p>
 * This uses the following on-disk structures:
 * <ul>
 *     <li>A block-packed structure for rowId to token lookups using {@link BlockPackedReader}.
 *     Uses the {@link IndexComponent#TOKEN_VALUES} component</li>
 *     <li>A monotonic block packed structure for rowId to partitionId lookups using {@link MonotonicBlockPackedReader}.
 *     Uses the {@link IndexComponent#PARTITION_SIZES} component</li>
 *     <li>A sorted terms structure for rowId to {@link PrimaryKey} and {@link PrimaryKey} to rowId lookups using
 *     {@link SortedTermsReader}. Uses the {@link IndexComponent#PARTITION_KEY_BLOCKS} and
 *     {@link IndexComponent#PARTITION_KEY_BLOCK_OFFSETS} components</li>
 * </ul>
 *
 * While the {@link Factory} is threadsafe, individual instances of the {@link SkinnyPrimaryKeyMap}
 * are not.
 */
@NotThreadSafe
public class SkinnyPrimaryKeyMap implements PrimaryKeyMap
{
    @ThreadSafe
    public static class Factory implements PrimaryKeyMap.Factory
    {
        protected final MetadataSource metadataSource;
        protected final LongArray.Factory tokenReaderFactory;
        protected final LongArray.Factory partitionReaderFactory;
        protected final SortedTermsReader partitionKeyReader;
        protected final IPartitioner partitioner;
        protected final PrimaryKey.Factory primaryKeyFactory;

        private final FileHandle tokensFile;
        private final FileHandle partitionsFile;
        private final FileHandle partitionKeyBlockOffsetsFile;
        private final FileHandle partitionKeyBlocksFile;

        public Factory(IndexDescriptor indexDescriptor, SSTableReader sstable)
        {
            this.tokensFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.TOKEN_VALUES, this::close);
            this.partitionsFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PARTITION_SIZES, this::close);
            this.partitionKeyBlockOffsetsFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PARTITION_KEY_BLOCK_OFFSETS, this::close);
            this.partitionKeyBlocksFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PARTITION_KEY_BLOCKS, this::close);
            try
            {
                this.metadataSource = MetadataSource.loadGroupMetadata(indexDescriptor);
                NumericValuesMeta tokensMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.TOKEN_VALUES)));
                this.tokenReaderFactory = new BlockPackedReader(tokensFile, tokensMeta);
                NumericValuesMeta partitionsMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.PARTITION_SIZES)));
                this.partitionReaderFactory = new MonotonicBlockPackedReader(partitionsFile, partitionsMeta);
                NumericValuesMeta partitionKeyBlockOffsetsMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.PARTITION_KEY_BLOCK_OFFSETS)));
                SortedTermsMeta partitionKeysMeta = new SortedTermsMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.PARTITION_KEY_BLOCKS)));
                this.partitionKeyReader = new SortedTermsReader(partitionKeyBlocksFile, partitionKeyBlockOffsetsFile, partitionKeysMeta, partitionKeyBlockOffsetsMeta);
                this.partitioner = sstable.metadata().partitioner;
                this.primaryKeyFactory = indexDescriptor.primaryKeyFactory;
            }
            catch (Throwable t)
            {
                throw Throwables.unchecked(t);
            }
        }

        @Override
        @SuppressWarnings({"resource", "RedundantSuppression"})
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap() throws IOException
        {
            LongArray rowIdToToken = new LongArray.DeferredLongArray(tokenReaderFactory::open);
            LongArray rowIdToPartitionId = new LongArray.DeferredLongArray(partitionReaderFactory::open);
            return new SkinnyPrimaryKeyMap(rowIdToToken,
                                           rowIdToPartitionId,
                                           partitionKeyReader.openCursor(),
                                           partitioner,
                                           primaryKeyFactory);
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(Arrays.asList(tokensFile, partitionsFile, partitionKeyBlocksFile, partitionKeyBlockOffsetsFile));
        }
    }

    protected final LongArray tokenArray;
    protected final LongArray partitionArray;
    protected final SortedTermsReader.Cursor partitionKeyCursor;
    protected final IPartitioner partitioner;
    protected final PrimaryKey.Factory primaryKeyFactory;
    protected final ByteBuffer tokenBuffer = ByteBuffer.allocate(Long.BYTES);

    protected SkinnyPrimaryKeyMap(LongArray tokenArray,
                                  LongArray partitionArray,
                                  SortedTermsReader.Cursor partitionKeyCursor,
                                  IPartitioner partitioner,
                                  PrimaryKey.Factory primaryKeyFactory)
    {
        this.tokenArray = tokenArray;
        this.partitionArray = partitionArray;
        this.partitionKeyCursor = partitionKeyCursor;
        this.partitioner = partitioner;
        this.primaryKeyFactory = primaryKeyFactory;
    }

    @Override
    public PrimaryKey primaryKeyFromRowId(long sstableRowId)
    {
        tokenBuffer.putLong(tokenArray.get(sstableRowId));
        tokenBuffer.rewind();
        return primaryKeyFactory.createDeferred(partitioner.getTokenFactory().fromByteArray(tokenBuffer), () -> supplier(sstableRowId));
    }

    @Override
    public long rowIdFromPrimaryKey(PrimaryKey key)
    {
        return tokenArray.indexOf(key.token().getLongValue());
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(Arrays.asList(partitionKeyCursor, tokenArray, partitionArray));
    }

    protected PrimaryKey supplier(long sstableRowId)
    {
        return primaryKeyFactory.create(readPartitionKey(sstableRowId), Clustering.EMPTY);
    }

    protected DecoratedKey readPartitionKey(long sstableRowId)
    {
        long partitionId = partitionArray.get(sstableRowId);
        ByteSource.Peekable peekable = ByteSource.peekable(partitionKeyCursor.seekForwardToPointId(partitionId).asComparableBytes(ByteComparable.Version.OSS50));

        byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(peekable);

        assert keyBytes != null : "Primary key from map did not contain a partition key";

        ByteBuffer decoratedKey = ByteBuffer.wrap(keyBytes);
        return new BufferDecoratedKey(partitioner.getToken(decoratedKey), decoratedKey);
    }
}
