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
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Iterates keys in the {@link PrimaryKeyMap} of a SSTable.
 * Iterating keys in the primary key map is faster than reading them from the SSTable data component
 * because we don't deserialize all the other columns except the primary key columns.
 * The primary key map components are also likely much smaller than the whole SSTable data component.
 * <p>
 * The keys are returned in token-clustering order.
 */
public final class PrimaryKeyMapIterator extends RangeIterator
{
    // KeyFilter controls which keys we want to return from the iterator.
    // This is a hack to make this iterator work correctly on schemas with static columns.
    // If the table has static columns, the primary key map component may contain both keys with clustering
    // and with no clustering. The keys of regular rows will likely have clustering and the keys associated with
    // updates of the static columns will have no clustering. Hence, depending on the type of the queried column,
    // we must return only all keys with clustering or only all keys with no clustering, but not mixed, or we may run
    // into duplicate row issues. We also shouldn't return keys without clustering for regular rows that expect
    // clustering information - as that would negate the row-awareness advantage.
    private enum KeyFilter
    {
        ALL,   // return all keys, fast, but safe only if we know there are no mixed keys with and without clustering
        KEYS_WITH_CLUSTERING     // return keys with clustering
    }

    private final PrimaryKeyMap keys;
    private final KeyFilter filter;
    private long currentRowId;


    private PrimaryKeyMapIterator(PrimaryKeyMap keys, PrimaryKey min, PrimaryKey max, long startRowId, KeyFilter filter)
    {
        super(min, max, keys.count());
        this.keys = keys;
        this.filter = filter;
        this.currentRowId = startRowId;
    }

    public static RangeIterator create(SSTableContext ctx, AbstractBounds<PartitionPosition> keyRange) throws IOException
    {
        KeyFilter filter;
        TableMetadata metadata = ctx.sstable().metadata();
        if (metadata.hasStaticColumns())
            filter = KeyFilter.KEYS_WITH_CLUSTERING;
        else // the table doesn't consist anything we want to filter out, so let's use the cheap option
            filter = KeyFilter.ALL;

        if (ctx.indexDescriptor().isSSTableEmpty())
            return RangeIterator.empty();

        PrimaryKeyMap keys = ctx.primaryKeyMapFactory.newPerSSTablePrimaryKeyMap();
        long count = keys.count();
        if (keys.count() == 0)
        {
            keys.close();
            return RangeIterator.empty();
        }

        PrimaryKey.Factory pkFactory = ctx.indexDescriptor.primaryKeyFactory;
        Token minToken = keyRange.left.getToken();
        PrimaryKey minKeyBound = pkFactory.createTokenOnly(minToken);
        PrimaryKey sstableMinKey = keys.primaryKeyFromRowId(0);
        PrimaryKey sstableMaxKey = keys.primaryKeyFromRowId(count - 1);
        PrimaryKey minKey = (minKeyBound.compareTo(sstableMinKey) > 0)
                            ? minKeyBound
                            : sstableMinKey;
        long startRowId = minToken.isMinimum() ? 0 : keys.ceiling(minKey);
        return new PrimaryKeyMapIterator(keys, sstableMinKey, sstableMaxKey, startRowId, filter);
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        this.currentRowId = keys.ceiling(nextKey);
    }

    @Override
    protected PrimaryKey computeNext()
    {
        while (currentRowId >= 0 && currentRowId < keys.count())
        {
            PrimaryKey key = keys.primaryKeyFromRowId(currentRowId++);
            if (filter == KeyFilter.KEYS_WITH_CLUSTERING && key.hasEmptyClustering())
                continue;
            return key;
        }
        return endOfData();
    }

    @Override
    public void close() throws IOException
    {
        keys.close();
    }

}
