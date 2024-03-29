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

package org.apache.cassandra.index.accord;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import accord.primitives.TxnId;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

public class RoutesSearcher
{
    private final ColumnFamilyStore cfs = Keyspace.open("system_accord").getColumnFamilyStore("commands");
    private final Index index = cfs.indexManager.getIndexByName("route");;
    private final ColumnMetadata route = AccordKeyspace.CommandsColumns.route;
    private final ColumnMetadata store_id = AccordKeyspace.CommandsColumns.store_id;
    private final ColumnMetadata txn_id = AccordKeyspace.CommandsColumns.txn_id;
    private final ColumnFilter columnFilter = ColumnFilter.selectionBuilder().add(store_id).add(txn_id).build();
    private final DataLimits limits = DataLimits.NONE;
    private final DataRange dataRange = DataRange.allData(cfs.getPartitioner());

    private CloseableIterator<Entry> searchKeysAccord(int store, AccordRoutingKey start, AccordRoutingKey end)
    {
        RowFilter rowFilter = RowFilter.create(false);
        rowFilter.add(route, Operator.GT, OrderedRouteSerializer.serializeRoutingKey(start));
        rowFilter.add(route, Operator.LTE, OrderedRouteSerializer.serializeRoutingKey(end));
        rowFilter.add(store_id, Operator.EQ, Int32Type.instance.decompose(store));

        var cmd = PartitionRangeReadCommand.create(cfs.metadata(),
                                                   FBUtilities.nowInSeconds(),
                                                   columnFilter,
                                                   rowFilter,
                                                   limits,
                                                   dataRange);
        Index.Searcher s = index.searcherFor(cmd);
        try (var controler = cmd.executionController())
        {
            UnfilteredPartitionIterator partitionIterator = s.search(controler);
            return new CloseableIterator<Entry>()
            {
                private final Entry entry = new Entry();
                @Override
                public void close()
                {
                    partitionIterator.close();
                }

                @Override
                public boolean hasNext()
                {
                    return partitionIterator.hasNext();
                }

                @Override
                public Entry next()
                {
                    UnfilteredRowIterator next = partitionIterator.next();
                    var partitionKeyComponents = AccordKeyspace.CommandRows.splitPartitionKey(next.partitionKey());
                    entry.store_id = AccordKeyspace.CommandRows.getStoreId(partitionKeyComponents);
                    entry.txnId = AccordKeyspace.CommandRows.getTxnId(partitionKeyComponents);
                    return entry;
                }
            };
        }
    }

    public Set<TxnId> intersects(int store, TokenRange range)
    {
        return intersects(store, (AccordRoutingKey) range.start(), (AccordRoutingKey) range.end());
    }

    public Set<TxnId> intersects(int store, AccordRoutingKey start, AccordRoutingKey end)
    {
        var set = new HashSet<TxnId>();
        try (var it = searchKeysAccord(store, start, end))
        {
            while (it.hasNext())
            {
                Entry next = it.next();
                if (next.store_id != store) continue; // the index should filter out, but just in case...
                set.add(next.txnId);
            }
        }
        return set.isEmpty() ? Collections.emptySet() : set;
    }

    private static final class Entry
    {
        public int store_id;
        public TxnId txnId;
    }
}
