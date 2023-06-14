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

package org.apache.cassandra.index.sai.disk;

import java.io.IOException;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * Iterates keys in the PrimaryKeyMap of an SSTable.
 * Iterating keys in the primary key map is faster than reading them from the SSTable data component
 * because we don't deserialize all the other columns except the primary key columns.
 * The primary key map components are also likely much smaller than the whole SSTable data component.
 * <p>
 * The keys are returned in token-clustering order.
 */
public final class PrimaryKeyMapIterator extends KeyRangeIterator
{
    private final PrimaryKeyMap keys;
    private long currentRowId;

    private PrimaryKeyMapIterator(PrimaryKeyMap keys, PrimaryKey min, PrimaryKey max)
    {
        super(min, max, keys.count());
        this.keys = keys;
        this.currentRowId = keys.rowIdFromPrimaryKey(min);
    }

    public static PrimaryKeyMapIterator create(SSTableContext ctx, AbstractBounds<PartitionPosition> keyRange) throws IOException
    {
        PrimaryKeyMap keys = ctx.primaryKeyMapFactory.newPerSSTablePrimaryKeyMap();
        long count = keys.count();

        PrimaryKey.Factory pkFactory = new PrimaryKey.Factory(ctx.sstable.metadata().comparator);
        Token minToken = keyRange.left.getToken();
        Token maxToken = keyRange.right.getToken();
        PrimaryKey minKeyBound = pkFactory.createTokenOnly(minToken);
        PrimaryKey maxKeyBound = pkFactory.createTokenOnly(maxToken);
        PrimaryKey sstableMinKey = count > 0 ? keys.primaryKeyFromRowId(0) : null;
        PrimaryKey sstableMaxKey = count > 0 ? keys.primaryKeyFromRowId(count - 1) : null;
        PrimaryKey minKey = (sstableMinKey == null || minKeyBound.compareTo(sstableMinKey) > 0)
                            ? minKeyBound
                            : sstableMinKey;
        PrimaryKey maxKey = (sstableMaxKey == null || !maxToken.isMinimum() && maxKeyBound.compareTo(sstableMaxKey) < 0)
                            ? maxKeyBound
                            : sstableMaxKey;
        return new PrimaryKeyMapIterator(keys, minKey, maxKey);
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        this.currentRowId = keys.rowIdFromPrimaryKey(nextKey);
    }

    @Override
    protected PrimaryKey computeNext()
    {
        return currentRowId < keys.count()
               ? keys.primaryKeyFromRowId(currentRowId++)
               : endOfData();
    }

    @Override
    public void close() throws IOException
    {
        keys.close();
    }

}
