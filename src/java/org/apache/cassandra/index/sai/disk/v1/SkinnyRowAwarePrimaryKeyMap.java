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
 *     <li>Block-packed structure for rowId to token lookups using {@link BlockPackedReader}.
 *     Uses component {@link IndexComponent#TOKEN_VALUES} </li>
 *     <li>A sorted-terms structure for rowId to {@link PrimaryKey} and {@link PrimaryKey} to rowId lookups using
 *     {@link SortedTermsReader}. Uses components {@link IndexComponent#PRIMARY_KEY_BLOCKS} and
 *     {@link IndexComponent#PRIMARY_KEY_BLOCK_OFFSETS}</li>
 * </ul>
 *
 * While the {@link SkinnyRowAwarePrimaryKeyMapFactory} is threadsafe, individual instances of the {@link SkinnyRowAwarePrimaryKeyMap}
 * are not.
 */
@NotThreadSafe
public class SkinnyRowAwarePrimaryKeyMap implements PrimaryKeyMap
{
    @ThreadSafe
    public static class SkinnyRowAwarePrimaryKeyMapFactory implements Factory
    {
        protected final MetadataSource metadataSource;
        protected final LongArray.Factory tokenReaderFactory;
        protected final SortedTermsReader sortedTermsReader;
        protected final SortedTermsMeta sortedTermsMeta;
        protected FileHandle tokensFile = null;
        protected FileHandle primaryKeyBlockOffsetsFile = null;
        protected FileHandle primaryKeyBlocksFile = null;
        protected final IPartitioner partitioner;
        protected final PrimaryKey.Factory primaryKeyFactory;

        public SkinnyRowAwarePrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable)
        {
            try
            {
                this.metadataSource = MetadataSource.loadGroupMetadata(indexDescriptor);
                NumericValuesMeta tokensMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.TOKEN_VALUES)));
                NumericValuesMeta blockOffsetsMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS)));
                this.sortedTermsMeta = new SortedTermsMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.PRIMARY_KEY_BLOCKS)));
                this.tokensFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.TOKEN_VALUES);
                this.tokenReaderFactory = new BlockPackedReader(tokensFile, tokensMeta);
                this.primaryKeyBlockOffsetsFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS);
                this.primaryKeyBlocksFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_BLOCKS);
                this.sortedTermsReader = new SortedTermsReader(primaryKeyBlocksFile, primaryKeyBlockOffsetsFile, sortedTermsMeta, blockOffsetsMeta);
                this.partitioner = sstable.metadata().partitioner;
                this.primaryKeyFactory = indexDescriptor.primaryKeyFactory;
            }
            catch (Throwable t)
            {
                throw Throwables.unchecked(Throwables.close(t, Arrays.asList(tokensFile, primaryKeyBlocksFile, primaryKeyBlockOffsetsFile)));
            }
        }

        @Override
        @SuppressWarnings({"resource", "RedundantSuppression"})
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap() throws IOException
        {
            LongArray rowIdToToken = new LongArray.DeferredLongArray(tokenReaderFactory::open);
            return new SkinnyRowAwarePrimaryKeyMap(rowIdToToken,
                                                   sortedTermsReader.openCursor(),
                                                   partitioner,
                                                   primaryKeyFactory);
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(Arrays.asList(tokensFile, primaryKeyBlocksFile, primaryKeyBlockOffsetsFile));
        }
    }

    protected final LongArray tokenArray;
    protected final SortedTermsReader.Cursor cursor;
    protected final IPartitioner partitioner;
    protected final PrimaryKey.Factory primaryKeyFactory;
    protected final ByteBuffer tokenBuffer = ByteBuffer.allocate(Long.BYTES);

    protected SkinnyRowAwarePrimaryKeyMap(LongArray tokenArray,
                                          SortedTermsReader.Cursor cursor,
                                          IPartitioner partitioner,
                                          PrimaryKey.Factory primaryKeyFactory)
    {
        this.tokenArray = tokenArray;
        this.cursor = cursor;
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
        FileUtils.closeQuietly(Arrays.asList(cursor, tokenArray));
    }

    protected PrimaryKey supplier(long sstableRowId)
    {
        ByteSource.Peekable peekable = ByteSource.peekable(cursor.seekForwardToPointId(sstableRowId).asComparableBytes(ByteComparable.Version.OSS50));

        byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(peekable);

        assert keyBytes != null : "Primary key from map did not contain a partition key";

        ByteBuffer decoratedKey = ByteBuffer.wrap(keyBytes);
        DecoratedKey partitionKey = new BufferDecoratedKey(partitioner.getToken(decoratedKey), decoratedKey);

        return primaryKeyFactory.create(partitionKey, Clustering.EMPTY);
    }
}
