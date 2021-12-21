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
import org.apache.cassandra.index.sai.SSTableQueryContext;
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
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap(SSTableQueryContext context) throws IOException
        {
            final LongArray rowIdToToken = new LongArray.DeferredLongArray(() -> tokenReaderFactory.openTokenReader(0, context));
            return new RowAwarePrimaryKeyMap(rowIdToToken,
                                             sortedTermsReader,
                                             sortedTermsReader.openCursor(context),
                                             partitioner,
                                             primaryKeyFactory,
                                             clusteringComparator);
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

    @Override
    public long rowIdFromPrimaryKey(PrimaryKey key)
    {
        return sortedTermsReader.getPointId(v -> key.asComparableBytes(v));
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
}
