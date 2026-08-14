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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.LongUnaryOperator;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.bitpack.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.MonotonicBlockPackedReader;
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
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * A {@link PrimaryKeyMap} for skinny tables (those with no clustering columns).
 * <p>
 * This uses the following on-disk structures:
 * <ul>
 *     <li>A block-packed structure for rowId to token lookups using {@link BlockPackedReader}.
 *     Uses the {@link IndexComponentType#ROW_TO_TOKEN} component.</li>
 *     <li>A monotonic block packed structure for rowId to partitionId lookups using {@link MonotonicBlockPackedReader}.
 *     Uses the {@link IndexComponentType#ROW_TO_PARTITION} component.</li>
 *     <li>A key store for rowId to {@link PrimaryKey} and {@link PrimaryKey} to rowId lookups using
 *     {@link KeyLookup}. Uses the {@link IndexComponentType#PARTITION_KEY_BLOCKS} and
 *     {@link IndexComponentType#PARTITION_KEY_BLOCK_OFFSETS} components.</li>
 * </ul>
 * <p>
 * While the {@link Factory} is thread-safe, individual instances of the {@link SkinnyPrimaryKeyMap}
 * are not.
 */
@NotThreadSafe
public class SkinnyPrimaryKeyMap implements PrimaryKeyMap
{
    @ThreadSafe
    public static class Factory implements PrimaryKeyMap.Factory
    {
        // The class member is needed to avoid memory leaks and to be addressed by CNDB-17902
        @SuppressWarnings("unused")
        private final IndexComponents.ForRead perSSTableComponents;
        protected final SSTableId<?> sstableId;
        protected final boolean hasStaticColumns;

        protected final MetadataSource metadataSource;
        protected final LongArray.Factory rowToTokenReaderFactory;
        protected final LongArray.Factory rowToPartitionReaderFactory;
        protected final KeyLookup partitionKeyReader;
        protected final IPartitioner partitioner;
        protected final V9RowAwarePrimaryKeyFactory primaryKeyFactory;

        private final FileHandle rowToTokenFile;
        private final FileHandle rowToPartitionFile;
        private final FileHandle partitionKeyBlockOffsetsFile;
        private final FileHandle partitionKeyBlocksFile;

        public Factory(IndexComponents.ForRead perSSTableComponents, V9RowAwarePrimaryKeyFactory primaryKeyFactory, SSTableReader sstable)
        {
            FileHandle rowToTokenFileLocal = null;
            FileHandle rowToPartitionFileLocal = null;
            FileHandle partitionKeyBlockOffsetsFileLocal = null;
            FileHandle partitionKeyBlocksFileLocal = null;
            try
            {
                this.metadataSource = MetadataSource.loadMetadata(perSSTableComponents);

                NumericValuesMeta tokensMeta = new NumericValuesMeta(metadataSource.get(perSSTableComponents.get(IndexComponentType.ROW_TO_TOKEN)));
                rowToTokenFileLocal = perSSTableComponents.get(IndexComponentType.ROW_TO_TOKEN).createFileHandle();
                this.rowToTokenReaderFactory = new BlockPackedReader(rowToTokenFileLocal, tokensMeta);

                NumericValuesMeta partitionsMeta = new NumericValuesMeta(metadataSource.get(perSSTableComponents.get(IndexComponentType.ROW_TO_PARTITION)));
                rowToPartitionFileLocal = perSSTableComponents.get(IndexComponentType.ROW_TO_PARTITION).createFileHandle();
                this.rowToPartitionReaderFactory = new MonotonicBlockPackedReader(rowToPartitionFileLocal, partitionsMeta);

                NumericValuesMeta partitionKeyBlockOffsetsMeta = new NumericValuesMeta(metadataSource.get(perSSTableComponents.get(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS)));
                KeyLookupMeta partitionKeysMeta = new KeyLookupMeta(metadataSource.get(perSSTableComponents.get(IndexComponentType.PARTITION_KEY_BLOCKS)));
                partitionKeyBlocksFileLocal = perSSTableComponents.get(IndexComponentType.PARTITION_KEY_BLOCKS).createFileHandle();
                partitionKeyBlockOffsetsFileLocal = perSSTableComponents.get(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS).createFileHandle();
                this.partitionKeyReader = new KeyLookup(partitionKeyBlocksFileLocal, partitionKeyBlockOffsetsFileLocal, partitionKeysMeta, partitionKeyBlockOffsetsMeta);
            }
            catch (IOException e)
            {
                throw Throwables.unchecked(Throwables.close(e, rowToTokenFileLocal, rowToPartitionFileLocal, partitionKeyBlocksFileLocal, partitionKeyBlockOffsetsFileLocal));
            }
            this.perSSTableComponents = perSSTableComponents;

            this.rowToTokenFile = rowToTokenFileLocal;
            this.rowToPartitionFile = rowToPartitionFileLocal;
            this.partitionKeyBlockOffsetsFile = partitionKeyBlockOffsetsFileLocal;
            this.partitionKeyBlocksFile = partitionKeyBlocksFileLocal;

            this.partitioner = sstable.metadata().partitioner;
            this.primaryKeyFactory = primaryKeyFactory;
            this.sstableId = sstable.getId();
            this.hasStaticColumns = sstable.metadata().hasStaticColumns();
        }

        @Override
        @SuppressWarnings({ "resource", "RedundantSuppression" })
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap()
        {
            LongArray rowIdToToken = new LongArray.DeferredLongArray(rowToTokenReaderFactory::open);
            LongArray rowIdToPartitionId = new LongArray.DeferredLongArray(rowToPartitionReaderFactory::open);
            return new SkinnyPrimaryKeyMap(rowIdToToken,
                                           rowIdToPartitionId,
                                           partitionKeyReader.openCursor(),
                                           partitioner,
                                           primaryKeyFactory,
                                           sstableId,
                                           hasStaticColumns);
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
    protected final IPartitioner partitioner;
    protected final V9RowAwarePrimaryKeyFactory primaryKeyFactory;
    protected final SSTableId<?> sstableId;
    private final boolean hasStaticColumns;

    protected SkinnyPrimaryKeyMap(LongArray rowIdToTokenArray,
                                  LongArray rowIdToPartitionIdArray,
                                  KeyLookup.Cursor partitionKeyCursor,
                                  IPartitioner partitioner,
                                  V9RowAwarePrimaryKeyFactory primaryKeyFactory,
                                  SSTableId<?> sstableId,
                                  boolean hasStaticColumns)
    {
        this.rowIdToTokenArray = rowIdToTokenArray;
        this.rowIdToPartitionIdArray = rowIdToPartitionIdArray;
        this.partitionKeyCursor = partitionKeyCursor;
        this.partitioner = partitioner;
        this.primaryKeyFactory = primaryKeyFactory;
        this.sstableId = sstableId;
        this.hasStaticColumns = hasStaticColumns;
    }

    @Override
    public SSTableId<?> getSSTableId()
    {
        return sstableId;
    }

    @Override
    public long count()
    {
        return rowIdToTokenArray.length();
    }

    @Override
    public PrimaryKey primaryKeyFromRowId(long sstableRowId)
    {
        long token = rowIdToTokenArray.get(sstableRowId);
        return primaryKeyFactory.createDeferred(partitioner.getTokenFactory().fromLongValue(token), () -> supplier(sstableRowId));
    }

    @Override
    public PrimaryKey primaryKeyFromRowId(long sstableRowId, PrimaryKey lowerBound, PrimaryKey upperBound)
    {
        return hasStaticColumns ? primaryKeyFromRowId(sstableRowId)
                                : primaryKeyFactory.createWithSource(this, sstableRowId, lowerBound, upperBound);
    }

    /**
     * Common implementation for row ID lookup operations that handles PrimaryKeyWithSource optimization
     * and token collision detection.
     *
     * @param key                the primary key to lookup
     * @param tokenLookup        function to perform the initial token-based lookup
     * @param collisionDetection function to handle token collisions
     * @return the row ID
     */
    protected long lookupRowId(PrimaryKey key,
                               LongUnaryOperator tokenLookup,
                               BiFunction<PrimaryKey, Long, Long> collisionDetection)
    {
        if (key instanceof PrimaryKeyWithSource)
        {
            PrimaryKeyWithSource pkws = (PrimaryKeyWithSource) key;
            if (pkws.getSourceSstableId().equals(sstableId))
                return pkws.getSourceRowId();
        }
        long rowId = tokenLookup.applyAsLong(key.token().getLongValue());
        if (key.isTokenOnly() || rowId < 0)
            return rowId;
        // The first index might not have been the correct match in the case of token collisions.
        return collisionDetection.apply(key, rowId);
    }

    @Override
    public long exactRowIdOrInvertedCeiling(PrimaryKey key)
    {
        return lookupRowId(key, rowIdToTokenArray::indexOf, this::tokenCeilingCollisionDetection);
    }

    @Override
    public long ceiling(PrimaryKey key)
    {
        return lookupRowId(key, rowIdToTokenArray::ceilingIndex, this::tokenCeilingCollisionDetection);
    }

    @Override
    public long floor(PrimaryKey key)
    {
        return lookupRowId(key, rowIdToTokenArray::floorIndex, this::tokenFloorCollisionDetection);
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(Arrays.asList(partitionKeyCursor, rowIdToTokenArray, rowIdToPartitionIdArray));
    }

    /**
     * Generic token collision detection that handles both ceiling and floor operations.
     * Look for token collision if the adjacent token in the token array matches the
     * current token. If we find a collision, we need to compare the partition key instead.
     *
     * @param key the key to search for
     * @param rowId      the initial row ID from token lookup
     * @param direction  1 for ceiling (forward search), -1 for floor (backward search)
     * @return the adjusted row ID after collision detection
     */
    private long tokenCollisionDetection(PrimaryKey key, long rowId, int direction)
    {
        assert direction == 1 || direction == -1 : "Direction must be 1 (ceiling) or -1 (floor)";

        long tokenValue = key.token().getLongValue();
        long nextRowId = rowId + direction;

        // Look for collisions while we haven't reached the boundaries and tokens match
        while (nextRowId >= 0 && nextRowId < rowIdToTokenArray.length() && tokenValue == rowIdToTokenArray.get(nextRowId))
        {
            // For ceiling: check if the partition key at current rowId is >= lookup key
            // For floor: check if the partition key at current rowId is <= lookup key
            int comparison = readPartitionKey(rowId).compareTo(key.partitionKey());
            if ((direction == 1 && comparison >= 0) || (direction == -1 && comparison <= 0))
                return rowId;

            rowId = nextRowId;
            nextRowId = rowId + direction;
        }
        // Note: We would normally expect to get here without going into the while loop
        return rowId;
    }

    protected long tokenCeilingCollisionDetection(PrimaryKey primaryKey, long rowId)
    {
        return tokenCollisionDetection(primaryKey, rowId, 1);
    }

    protected long tokenFloorCollisionDetection(PrimaryKey primaryKey, long rowId)
    {
        return tokenCollisionDetection(primaryKey, rowId, -1);
    }

    protected PrimaryKey supplier(long sstableRowId)
    {
        return primaryKeyFactory.create(readPartitionKey(sstableRowId), Clustering.EMPTY);
    }

    protected DecoratedKey readPartitionKey(long sstableRowId)
    {
        long partitionId = rowIdToPartitionIdArray.get(sstableRowId);
        ByteSource.Peekable peekable = ByteSource.peekable(partitionKeyCursor.seekToPointId(partitionId).asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));

        byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(peekable);

        assert keyBytes != null : "Primary key from map did not contain a partition key";

        ByteBuffer keyBuffer = ByteBuffer.wrap(keyBytes);
        return new BufferDecoratedKey(partitioner.getToken(keyBuffer), keyBuffer);
    }
}
