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
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.sortedterms.SortedTermsReader;
import org.apache.cassandra.index.sai.disk.v1.sortedterms.SortedTermsTrieSearcher;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * An extension of the {@link SkinnyRowAwarePrimaryKeyMap} for wide tables (those with clustering columns).
 * <p>
 * In addition to the on-disk structures used by the {@link SkinnyRowAwarePrimaryKeyMap} this uses:
 * <ul>
 *     <li>A primary key trie {@link SortedTermsTrieSearcher} for performing primary key -> rowId lookups. Uses the
 *     {@link IndexComponent#PRIMARY_KEY_TRIE} component.
 * </ul>
 *
 * While the {@link WideRowAwarePrimaryKeyMapFactory} is threadsafe, individual instances of the {@link WideRowAwarePrimaryKeyMap}
 * are not.
 */
@NotThreadSafe
public class WideRowAwarePrimaryKeyMap extends SkinnyRowAwarePrimaryKeyMap
{
    @ThreadSafe
    public static class WideRowAwarePrimaryKeyMapFactory extends SkinnyRowAwarePrimaryKeyMapFactory
    {
        private FileHandle primaryKeyTrieFile = null;
        private final ClusteringComparator clusteringComparator;

        public WideRowAwarePrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable)
        {
            super(indexDescriptor, sstable);

            try
            {
                this.primaryKeyTrieFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_TRIE);
                this.clusteringComparator = indexDescriptor.clusteringComparator;
            }
            catch (Throwable t)
            {
                throw Throwables.unchecked(Throwables.close(t, primaryKeyTrieFile));
            }
        }

        @Override
        @SuppressWarnings({ "resource", "RedundantSuppression" })
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap() throws IOException
        {
            LongArray rowIdToToken = new LongArray.DeferredLongArray(tokenReaderFactory::open);
            return new WideRowAwarePrimaryKeyMap(rowIdToToken,
                                                 new SortedTermsTrieSearcher(primaryKeyTrieFile.instantiateRebufferer(null), sortedTermsMeta),
                                                 sortedTermsReader.openCursor(),
                                                 partitioner,
                                                 primaryKeyFactory,
                                                 clusteringComparator);
        }
    }

    private final SortedTermsTrieSearcher trieSearcher;
    private final ClusteringComparator clusteringComparator;

    private WideRowAwarePrimaryKeyMap(LongArray tokenArray,
                                      SortedTermsTrieSearcher trieSearcher,
                                      SortedTermsReader.Cursor cursor,
                                      IPartitioner partitioner,
                                      PrimaryKey.Factory primaryKeyFactory,
                                      ClusteringComparator clusteringComparator)
    {
        super(tokenArray, cursor, partitioner, primaryKeyFactory);

        this.trieSearcher = trieSearcher;
        this.clusteringComparator = clusteringComparator;
    }

    @Override
    public long rowIdFromPrimaryKey(PrimaryKey key)
    {
        long rowId = tokenArray.findTokenRowID(key.token().getLongValue());
        // If the key only has a token (initial range skip in the query) or the token is out of range
        // the return the rowId from the token array.
        if (key.isTokenOnly() || rowId == -1)
            return rowId;
        // If the token for the rowId matches the key token then we need to search the keys in the trie
        // for a prefix match to get the correct rowId for the clustering key.
        if (tokenArray.get(rowId) == key.token().getLongValue())
            return trieSearcher.prefixSearch(key::asComparableBytes);
        return rowId;
    }

    @Override
    public void close()
    {
        super.close();
        FileUtils.closeQuietly(trieSearcher);
    }

    protected PrimaryKey supplier(long sstableRowId)
    {
        try
        {
            cursor.seekToPointId(sstableRowId);
            ByteSource.Peekable peekable = ByteSource.peekable(cursor.term().asComparableBytes(ByteComparable.Version.OSS50));

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
        catch (IOException e)
        {
            throw Throwables.cleaned(e);
        }
    }
}