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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.bitpack.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsMeta;
import org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsReader;
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
 * A row-aware {@link PrimaryKeyMap}
 *
 * This uses the following on-disk structures:
 * <ul>
 *     <li>Block-packed structure for rowId to token lookups using {@link BlockPackedReader}.
 *     Uses component {@link IndexComponentType#TOKEN_VALUES} </li>
 *     <li>A sorted-terms structure for rowId to {@link PrimaryKey} and {@link PrimaryKey} to rowId lookups using
 *     {@link SortedTermsReader}. Uses components {@link IndexComponentType#PRIMARY_KEY_TRIE}, {@link IndexComponentType#PRIMARY_KEY_BLOCKS},
 *     {@link IndexComponentType#PRIMARY_KEY_BLOCK_OFFSETS}</li>
 * </ul>
 *
 * While the {@link RowAwarePrimaryKeyMapFactory} is threadsafe, individual instances of the {@link RowAwarePrimaryKeyMap}
 * are not.
 */
@NotThreadSafe
public class RowAwarePrimaryKeyMap implements PrimaryKeyMap
{
    @ThreadSafe
    public static class RowAwarePrimaryKeyMapFactory implements Factory
    {
        private final IndexComponents.ForRead perSSTableComponents;
        private final LongArray.Factory tokenReaderFactory;
        private final SortedTermsReader sortedTermsReader;
        private final long count;
        private final FileHandle token;
        private final FileHandle termsDataBlockOffsets;
        private final FileHandle termsData;
        private final FileHandle termsTrie;
        private final IPartitioner partitioner;
        private final ClusteringComparator clusteringComparator;
        private final V2RowAwarePrimaryKeyFactory primaryKeyFactory;
        private final SSTableId<?> sstableId;
        private final boolean hasStaticColumns;

        public RowAwarePrimaryKeyMapFactory(IndexComponents.ForRead perSSTableComponents, V2RowAwarePrimaryKeyFactory primaryKeyFactory, SSTableReader sstable)
        {
            FileHandle token = null;
            FileHandle termsDataBlockOffsets = null;
            FileHandle termsData = null;
            FileHandle termsTrie = null;

            try
            {
                MetadataSource metadataSource = MetadataSource.loadMetadata(perSSTableComponents);
                NumericValuesMeta tokensMeta = new NumericValuesMeta(metadataSource.get(perSSTableComponents.get(IndexComponentType.TOKEN_VALUES)));
                this.count = tokensMeta.valueCount;

                SortedTermsMeta sortedTermsMeta = new SortedTermsMeta(metadataSource.get(perSSTableComponents.get(IndexComponentType.PRIMARY_KEY_BLOCKS)));
                NumericValuesMeta blockOffsetsMeta = new NumericValuesMeta(metadataSource.get(perSSTableComponents.get(IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS)));

                token = perSSTableComponents.get(IndexComponentType.TOKEN_VALUES).createFileHandle();
                this.tokenReaderFactory = new BlockPackedReader(token, tokensMeta);
                termsDataBlockOffsets = perSSTableComponents.get(IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS).createFileHandle();
                termsData = perSSTableComponents.get(IndexComponentType.PRIMARY_KEY_BLOCKS).createFileHandle();
                termsTrie = perSSTableComponents.get(IndexComponentType.PRIMARY_KEY_TRIE).createFileHandle();
                this.sortedTermsReader = new SortedTermsReader(termsData, termsDataBlockOffsets, termsTrie, sortedTermsMeta, blockOffsetsMeta);
            }
            catch (Throwable t)
            {
                throw Throwables.unchecked(Throwables.close(t, token, termsData, termsDataBlockOffsets, termsTrie));
            }
            this.perSSTableComponents = perSSTableComponents;
            this.token = token;
            this.termsDataBlockOffsets = termsDataBlockOffsets;
            this.termsData = termsData;
            this.termsTrie = termsTrie;
            this.partitioner = sstable.metadata().partitioner;
            this.primaryKeyFactory = primaryKeyFactory;
            this.clusteringComparator = sstable.metadata().comparator;
            this.sstableId = sstable.getId();
            this.hasStaticColumns = sstable.metadata().hasStaticColumns();
        }

        @Override
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap()
        {
            final LongArray rowIdToToken = new LongArray.DeferredLongArray(tokenReaderFactory::open);
            try
            {
                return new RowAwarePrimaryKeyMap(rowIdToToken,
                                                 sortedTermsReader.openCursor(),
                                                 partitioner,
                                                 primaryKeyFactory,
                                                 clusteringComparator,
                                                 sstableId,
                                                 hasStaticColumns);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException("Failed to load PrimaryKeyMap for sstable: " + sstableId, e);
            }
        }

        @Override
        public long count()
        {
            return count;
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(token, termsData, termsDataBlockOffsets, termsTrie);
        }
    }

    private final LongArray rowIdToToken;
    private final SortedTermsReader.Cursor cursor;
    private final IPartitioner partitioner;
    private final V2RowAwarePrimaryKeyFactory primaryKeyFactory;
    private final ClusteringComparator clusteringComparator;
    private final SSTableId<?> sstableId;
    private final boolean hasStaticColumns;

    private RowAwarePrimaryKeyMap(LongArray rowIdToToken,
                                  SortedTermsReader.Cursor cursor,
                                  IPartitioner partitioner,
                                  V2RowAwarePrimaryKeyFactory primaryKeyFactory,
                                  ClusteringComparator clusteringComparator,
                                  SSTableId<?> sstableId,
                                  boolean hasStaticColumns)
    {
        this.rowIdToToken = rowIdToToken;
        this.cursor = cursor;
        this.partitioner = partitioner;
        this.primaryKeyFactory = primaryKeyFactory;
        this.clusteringComparator = clusteringComparator;
        this.sstableId = sstableId;
        this.hasStaticColumns = hasStaticColumns;
    }

    @Override
    public SSTableId<?> getSSTableId()
    {
        return sstableId;
    }

    public long count()
    {
        return rowIdToToken.length();
    }

    @Override
    public PrimaryKey primaryKeyFromRowId(long sstableRowId)
    {
        long token = rowIdToToken.get(sstableRowId);
        return primaryKeyFactory.createDeferred(partitioner.getTokenFactory().fromLongValue(token), () -> supplier(sstableRowId));
    }

    @Override
    public PrimaryKey primaryKeyFromRowId(long sstableRowId, PrimaryKey lowerBound, PrimaryKey upperBound)
    {
        return hasStaticColumns ? primaryKeyFromRowId(sstableRowId)
                                : primaryKeyFactory.createWithSource(this, sstableRowId, lowerBound, upperBound);
    }

    private long skinnyExactRowIdOrInvertedCeiling(PrimaryKey key)
    {
        // Fast path when there is no clustering, i.e., there is one row per partition.
        // (The reason we don't just make the Factory return a PartitionAware map for this case
        // is that it reads partition keys directly from the sstable using the offsets file.
        // While this worked in BDP, it was not efficient and caused problems because the
        // sstable reader was using 64k page sizes, and this caused page cache thrashing.
        long rowId = rowIdToToken.indexOf(key.token().getLongValue());
        if (rowId < 0)
            // No match found, return the inverted ceiling
            return rowId;
        // The first index might not have been the correct match in the case of token collisions.
        return tokenCollisionDetection(key, rowId);
    }

    /**
     * Returns a row Id for a {@link PrimaryKey}. If there is no such term, returns the `-(next row id) - 1` where
     * `next row id` is the row id of the next greatest {@link PrimaryKey} in the map.
     * @param key the {@link PrimaryKey} to lookup
     * @return a row id
     */
    @Override
    public long exactRowIdOrInvertedCeiling(PrimaryKey key)
    {
        if (key instanceof PrimaryKeyWithSource)
        {
            var pkws = (PrimaryKeyWithSource) key;
            if (pkws.getSourceSstableId().equals(sstableId))
                return pkws.getSourceRowId();
        }

        if (clusteringComparator.size() == 0)
            return skinnyExactRowIdOrInvertedCeiling(key);

        long pointId = cursor.getExactPointId(key::asComparableBytes);
        if (pointId >= 0)
            return pointId;
        long ceiling = cursor.ceiling(key::asComparableBytesMinPrefix);
        // Use min value since inverting Long.MIN_VALUE gives Long.MAX_VALUE.
        return ceiling < 0 ? Long.MIN_VALUE : ~ceiling;
    }

    @Override
    public long ceiling(PrimaryKey key)
    {
        if (key instanceof PrimaryKeyWithSource)
        {
            var pkws = (PrimaryKeyWithSource) key;
            if (pkws.getSourceSstableId().equals(sstableId))
                return pkws.getSourceRowId();
        }

        if (clusteringComparator.size() == 0)
        {
            long rowId = skinnyExactRowIdOrInvertedCeiling(key);
            if (rowId >= 0)
                return rowId;
            else
                if (rowId == Long.MIN_VALUE)
                    return -1;
                else
                    return ~rowId;
        }

        return cursor.ceiling(key::asComparableBytesMinPrefix);
    }

    @Override
    public long floor(PrimaryKey key)
    {
        return cursor.floor(key::asComparableBytesMaxPrefix);
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(cursor, rowIdToToken);
    }

    private PrimaryKey supplier(long sstableRowId)
    {
        try
        {
            cursor.seekToPointId(sstableRowId);
            ByteSource.Peekable peekable = cursor.term().asPeekableBytes(TypeUtil.BYTE_COMPARABLE_VERSION);

            Token token = partitioner.getTokenFactory().fromComparableBytes(ByteSourceInverse.nextComponentSource(peekable),
                                                                            TypeUtil.BYTE_COMPARABLE_VERSION);
            byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable));

            if (keyBytes == null)
                return primaryKeyFactory.createTokenOnly(token);

            DecoratedKey partitionKey = new BufferDecoratedKey(token, ByteBuffer.wrap(keyBytes));

            Clustering<?> clustering = clusteringComparator.size() == 0
                                    ? Clustering.EMPTY
                                    : clusteringComparator.clusteringFromByteComparable(ByteBufferAccessor.instance,
                                                                                        v -> ByteSourceInverse.nextComponentSource(peekable),
                                                                                        TypeUtil.BYTE_COMPARABLE_VERSION);

            return primaryKeyFactory.create(partitionKey, clustering);
        }
        catch (IOException e)
        {
            throw Throwables.cleaned(e);
        }
    }

    // Look for token collision by if the ajacent token in the token array matches the
    // current token. If we find a collision we need to compare the partition key instead.
    protected long tokenCollisionDetection(PrimaryKey primaryKey, long rowId)
    {
        // Look for collisions while we haven't reached the end of the tokens and the tokens don't collide
        while (rowId + 1 < rowIdToToken.length() && primaryKey.token().getLongValue() == rowIdToToken.get(rowId + 1))
        {
            // If we had a collision then see if the partition key for this row is >= to the lookup partition key
            if (primaryKeyFromRowId(rowId).compareTo(primaryKey) >= 0)
                return rowId;

            rowId++;
        }
        // Note: We would normally expect to get here without going into the while loop
        return rowId;
    }
}
