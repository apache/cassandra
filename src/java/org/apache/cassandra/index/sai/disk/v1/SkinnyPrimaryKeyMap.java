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

import java.io.IOException;
import java.util.Arrays;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.bitpack.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.keystore.KeyLookupMeta;
import org.apache.cassandra.index.sai.disk.v1.keystore.KeyLookup;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

/**
 * A {@link PrimaryKeyMap} for skinny tables (those with no clustering columns).
 * <p>
 * This uses the following on-disk structures:
 * <ul>
 *     <li>A block-packed structure for rowId to token value lookups using {@link BlockPackedReader}.
 *     Uses the {@link IndexComponent#ROW_TO_TOKEN} component</li>
 *     <li>A monotonic block packed structure for rowId to partitionId lookups using {@link MonotonicBlockPackedReader}.
 *     Uses the {@link IndexComponent#ROW_TO_PARTITION} component</li>
 *     <li>A key store for rowId to {@link PrimaryKey} and {@link PrimaryKey} to rowId lookups using
 *     {@link KeyLookup}. Uses the {@link IndexComponent#PARTITION_KEY_BLOCKS} and
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
        protected final LongArray.Factory rowToTokenReaderFactory;
        protected final LongArray.Factory rowToPartitionReaderFactory;
        protected final KeyLookup partitionKeyReader;
        protected final PrimaryKey.Factory primaryKeyFactory;

        private final FileHandle rowToTokenFile;
        private final FileHandle rowToPartitionFile;
        private final FileHandle partitionKeyBlockOffsetsFile;
        private final FileHandle partitionKeyBlocksFile;

        public Factory(IndexDescriptor indexDescriptor)
        {
            this.rowToTokenFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.ROW_TO_TOKEN, this::close);
            this.rowToPartitionFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.ROW_TO_PARTITION, this::close);
            this.partitionKeyBlockOffsetsFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PARTITION_KEY_BLOCK_OFFSETS, this::close);
            this.partitionKeyBlocksFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PARTITION_KEY_BLOCKS, this::close);
            try
            {
                this.metadataSource = MetadataSource.loadGroupMetadata(indexDescriptor);
                NumericValuesMeta tokensMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.ROW_TO_TOKEN)));
                this.rowToTokenReaderFactory = new BlockPackedReader(rowToTokenFile, tokensMeta);
                NumericValuesMeta partitionsMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.ROW_TO_PARTITION)));
                this.rowToPartitionReaderFactory = new MonotonicBlockPackedReader(rowToPartitionFile, partitionsMeta);
                NumericValuesMeta partitionKeyBlockOffsetsMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.PARTITION_KEY_BLOCK_OFFSETS)));
                KeyLookupMeta partitionKeysMeta = new KeyLookupMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.PARTITION_KEY_BLOCKS)));
                this.partitionKeyReader = new KeyLookup(partitionKeyBlocksFile, partitionKeyBlockOffsetsFile, partitionKeysMeta, partitionKeyBlockOffsetsMeta);
                this.primaryKeyFactory = indexDescriptor.primaryKeyFactory;
            }
            catch (Throwable t)
            {
                throw Throwables.unchecked(t);
            }
        }

        @Override
        @SuppressWarnings({"resource", "RedundantSuppression"}) // rowIdToToken, rowIdToPartitionId and cursor are closed by the SkinnyPrimaryKeyMap#close method
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap() throws IOException
        {
            LongArray rowIdToToken = new LongArray.DeferredLongArray(rowToTokenReaderFactory::open);
            LongArray rowIdToPartitionId = new LongArray.DeferredLongArray(rowToPartitionReaderFactory::open);
            return new SkinnyPrimaryKeyMap(rowIdToToken,
                                           rowIdToPartitionId,
                                           partitionKeyReader.openCursor(),
                                           primaryKeyFactory);
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(Arrays.asList(rowToTokenFile, rowToPartitionFile, partitionKeyBlocksFile, partitionKeyBlockOffsetsFile));
        }
    }

    protected final LongArray rowIdToTokenArray;
    protected final LongArray rowIdToPartitionIdArray;
    protected final KeyLookup.Cursor partitionKeyCursor;
    protected final PrimaryKey.Factory primaryKeyFactory;

    protected SkinnyPrimaryKeyMap(LongArray rowIdToTokenArray,
                                  LongArray rowIdToPartitionIdArray,
                                  KeyLookup.Cursor partitionKeyCursor,
                                  PrimaryKey.Factory primaryKeyFactory)
    {
        this.rowIdToTokenArray = rowIdToTokenArray;
        this.rowIdToPartitionIdArray = rowIdToPartitionIdArray;
        this.partitionKeyCursor = partitionKeyCursor;
        this.primaryKeyFactory = primaryKeyFactory;
    }

    @Override
    public PrimaryKey primaryKeyFromRowId(long sstableRowId)
    {
        return primaryKeyFactory.create(readPartitionKey(sstableRowId));
    }

    @Override
    public long rowIdFromPrimaryKey(PrimaryKey primaryKey)
    {
        long rowId = rowIdToTokenArray.indexOf(primaryKey.token().getLongValue());
        // If the key is token only, the token is out of range, we are at the end of our keys, or we have skipped a token
        // we can return straight away.
        if (primaryKey.kind() == PrimaryKey.Kind.TOKEN ||
            rowId < 0 ||
            rowId + 1 == rowIdToTokenArray.length() || rowIdToTokenArray.get(rowId) != primaryKey.token().getLongValue())
            return rowId;
        // Otherwise we need to check for token collision.
        return tokenCollisionDetection(primaryKey, rowId);
    }

    @Override
    public long ceiling(Token token)
    {
        return rowIdToTokenArray.indexOf(token.getLongValue());
    }

    @Override
    public long floor(Token token)
    {
        if (token.isMinimum())
            return Long.MIN_VALUE;

        return rowIdToTokenArray.indexOf(token.getLongValue());
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(Arrays.asList(partitionKeyCursor, rowIdToTokenArray, rowIdToPartitionIdArray));
    }

    // Look for token collision by if the ajacent token in the token array matches the
    // current token. If we find a collision we need to compare the partition key instead.
    protected long tokenCollisionDetection(PrimaryKey primaryKey, long rowId)
    {
        // Look for collisions while we haven't reached the end of the tokens and the tokens don't collide
        while (rowId + 1 < rowIdToTokenArray.length() && primaryKey.token().getLongValue() == rowIdToTokenArray.get(rowId + 1))
        {
            // If we had a collision then see if the partition key for this row is >= to the lookup partition key
            if (readPartitionKey(rowId).compareTo(primaryKey.partitionKey()) >= 0)
                return rowId;

            rowId++;
        }
        // Note: We would normally expect to get here without going into the while loop
        return rowId;
    }

    protected DecoratedKey readPartitionKey(long sstableRowId)
    {
        return primaryKeyFactory.partitionKeyFromComparableBytes(partitionKeyCursor.seekToPointId(rowIdToPartitionIdArray.get(sstableRowId)));
    }
}
