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
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.bitpack.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsMeta;
import org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsReader;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * A row-aware {@link PrimaryKeyMap}
 *
 * This uses the following on-disk structures:
 * <ul>
 *     <li>Block-packed structure for rowId to token lookups using {@link BlockPackedReader}.
 *     Uses component {@link IndexComponent#TOKEN_VALUES} </li>
 *     <li>A sorted-terms structure for rowId to {@link PrimaryKey} and {@link PrimaryKey} to rowId lookups using
 *     {@link SortedTermsReader}. Uses components {@link IndexComponent#PRIMARY_KEY_TRIE}, {@link IndexComponent#PRIMARY_KEY_BLOCKS},
 *     {@link IndexComponent#PRIMARY_KEY_BLOCK_OFFSETS}</li>
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
        private final LongArray.Factory tokenReaderFactory;
        private final SortedTermsReader sortedTermsReader;
        private FileHandle token = null;
        private FileHandle termsDataBlockOffsets = null;
        private FileHandle termsData = null;
        private FileHandle termsTrie = null;
        private final IPartitioner partitioner;
        private final ClusteringComparator clusteringComparator;
        private final PrimaryKey.Factory primaryKeyFactory;

        public RowAwarePrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable)
        {
            try
            {
                MetadataSource metadataSource = MetadataSource.loadGroupMetadata(indexDescriptor);
                NumericValuesMeta tokensMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.TOKEN_VALUES)));
                SortedTermsMeta sortedTermsMeta = new SortedTermsMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.PRIMARY_KEY_BLOCKS)));
                NumericValuesMeta blockOffsetsMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS)));

                token = indexDescriptor.createPerSSTableFileHandle(IndexComponent.TOKEN_VALUES);
                this.tokenReaderFactory = new BlockPackedReader(token, tokensMeta);
                this.termsDataBlockOffsets = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS);
                this.termsData = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_BLOCKS);
                this.termsTrie = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_TRIE);
                this.sortedTermsReader = new SortedTermsReader(termsData, termsDataBlockOffsets, termsTrie, sortedTermsMeta, blockOffsetsMeta);
                this.partitioner = sstable.metadata().partitioner;
                this.primaryKeyFactory = indexDescriptor.primaryKeyFactory;
                this.clusteringComparator = indexDescriptor.clusteringComparator;
            }
            catch (Throwable t)
            {
                throw Throwables.unchecked(Throwables.close(t, token, termsData, termsDataBlockOffsets, termsTrie));
            }
        }

        @Override
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap()
        {
            final LongArray rowIdToToken = new LongArray.DeferredLongArray(() -> tokenReaderFactory.open());
            try
            {
                return new RowAwarePrimaryKeyMap(rowIdToToken,
                                                 sortedTermsReader,
                                                 sortedTermsReader.openCursor(),
                                                 partitioner,
                                                 primaryKeyFactory,
                                                 clusteringComparator);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(token, termsData, termsDataBlockOffsets, termsTrie);
        }
    }

    private final LongArray rowIdToToken;
    private final SortedTermsReader sortedTermsReader;
    private final SortedTermsReader.Cursor cursor;
    private final IPartitioner partitioner;
    private final PrimaryKey.Factory primaryKeyFactory;
    private final ClusteringComparator clusteringComparator;
    private final ByteBuffer tokenBuffer = ByteBuffer.allocate(Long.BYTES);

    private RowAwarePrimaryKeyMap(LongArray rowIdToToken,
                                  SortedTermsReader sortedTermsReader,
                                  SortedTermsReader.Cursor cursor,
                                  IPartitioner partitioner,
                                  PrimaryKey.Factory primaryKeyFactory,
                                  ClusteringComparator clusteringComparator)
    {
        this.rowIdToToken = rowIdToToken;
        this.sortedTermsReader = sortedTermsReader;
        this.cursor = cursor;
        this.partitioner = partitioner;
        this.primaryKeyFactory = primaryKeyFactory;
        this.clusteringComparator = clusteringComparator;
    }

    @Override
    public PrimaryKey primaryKeyFromRowId(long sstableRowId)
    {
        tokenBuffer.putLong(rowIdToToken.get(sstableRowId));
        tokenBuffer.rewind();
        return primaryKeyFactory.createDeferred(partitioner.getTokenFactory().fromByteArray(tokenBuffer), () -> supplier(sstableRowId));
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

    @Override
    public long exactRowIdForPrimaryKey(PrimaryKey key)
    {
        if (clusteringComparator.size() == 0)
            return skinnyExactRowIdOrInvertedCeiling(key);

        return cursor.getExactPointId(v -> key.asComparableBytes(v));
    }

    /**
     * Returns a row Id for a {@link PrimaryKey}. If there is no such term, returns the `-(next row id) - 1` where
     * `next row id` is the row id of the next greatest {@link PrimaryKey} in the map. For {@link PrimaryKey} with
     * no clustering columns, this method is equivalent to {@link #exactRowIdForPrimaryKey(PrimaryKey)}.
     * @param key the {@link PrimaryKey} to lookup
     * @return a row id
     */
    @Override
    public long exactRowIdOrInvertedCeiling(PrimaryKey key)
    {
        if (clusteringComparator.size() == 0)
            return skinnyExactRowIdOrInvertedCeiling(key);

        long pointId = cursor.getExactPointId(v -> key.asComparableBytes(v));
        if (pointId >= 0)
            return pointId;
        long ceiling = cursor.ceiling(v -> key.asComparableBytesMinPrefix(v));
        // Use min value since -(Long.MIN_VALUE) - 1 == Long.MAX_VALUE.
        return ceiling < 0 ? Long.MIN_VALUE : -ceiling - 1;
    }

    @Override
    public long ceiling(PrimaryKey key)
    {
        if (clusteringComparator.size() == 0)
        {
            long rowId = skinnyExactRowIdOrInvertedCeiling(key);
            if (rowId >= 0)
                return rowId;
            else
                if (rowId == Long.MIN_VALUE)
                    return -1;
                else
                    return -rowId - 1;
        }

        return cursor.ceiling(v -> key.asComparableBytesMinPrefix(v));
    }

    @Override
    public long floor(PrimaryKey key)
    {
        return cursor.floor(v -> key.asComparableBytesMaxPrefix(v));
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
            ByteSource.Peekable peekable = cursor.term().asPeekableBytes(ByteComparable.Version.OSS41);

            Token token = partitioner.getTokenFactory().fromComparableBytes(ByteSourceInverse.nextComponentSource(peekable),
                                                                            ByteComparable.Version.OSS41);
            byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable));

            if (keyBytes == null)
                return primaryKeyFactory.createTokenOnly(token);

            DecoratedKey partitionKey = new BufferDecoratedKey(token, ByteBuffer.wrap(keyBytes));

            Clustering clustering = clusteringComparator.size() == 0
                                    ? Clustering.EMPTY
                                    : clusteringComparator.clusteringFromByteComparable(ByteBufferAccessor.instance,
                                                                                        v -> ByteSourceInverse.nextComponentSource(peekable));

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
