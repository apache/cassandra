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

package org.apache.cassandra.index.sai.accord;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;

import accord.primitives.TxnId;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

public class RoutesSearcher
{
    private final ColumnFamilyStore cfs = Keyspace.open("system_accord").getColumnFamilyStore("commands");
    private final ColumnMetadata route = AccordKeyspace.CommandsColumns.route;
    private final ColumnMetadata store_id = AccordKeyspace.CommandsColumns.store_id;
    private final ColumnMetadata txn_id = AccordKeyspace.CommandsColumns.txn_id;
    private final ColumnFilter columnFilter = ColumnFilter.selectionBuilder().add(store_id).add(txn_id).build();
    private final DataLimits limits = DataLimits.NONE;
    private final DataRange dataRange = DataRange.allData(cfs.getPartitioner());

    private CloseableIterator<Entry> searchKeysInternal(int store, AccordRoutingKey start, AccordRoutingKey end)
    {
        RowFilter rowFilter = RowFilter.create(2);
        rowFilter.add(route, Operator.GT, SaiSerializer.serializeRoutingKey(start));
        rowFilter.add(route, Operator.LTE, SaiSerializer.serializeRoutingKey(end));
        rowFilter.add(store_id, Operator.EQ, Int32Type.instance.decompose(store));

        var cmd = PartitionRangeReadCommand.create(cfs.metadata(),
                                                   FBUtilities.nowInSeconds(),
                                                   columnFilter,
                                                   rowFilter,
                                                   limits,
                                                   dataRange);
        UnfilteredPartitionIterator iterator;
        try (var controller = cmd.executionController())
        {
            var queryContext = new QueryContext(cmd, 42);
            var queryController = new QueryController(cfs, cmd, rowFilter, queryContext, null);
            iterator = new SaiResultRetriever(queryController, controller, queryContext, false);
        }
        return new SearchResults(iterator).filter(e -> e.store_id == store);
    }

    public Set<TxnId> intersects(int store, TokenRange range)
    {
        return intersects(store, (AccordRoutingKey) range.start(), (AccordRoutingKey) range.end());
    }

    public Set<TxnId> intersects(int store, AccordRoutingKey start, AccordRoutingKey end)
    {
        var set = new HashSet<TxnId>();
        try (var it = searchKeysInternal(store, start, end))
        {
            while (it.hasNext())
                set.add(it.next().txnId);
        }
        return set.isEmpty() ? Collections.emptySet() : set;
    }

    private static class SearchResults implements CloseableIterator<Entry>
    {
        private final UnfilteredPartitionIterator it;
        private final Entry entry = new Entry();

        private SearchResults(UnfilteredPartitionIterator it)
        {
            this.it = it;
        }

        @Override
        public void close()
        {
            it.close();
        }

        @Override
        public boolean hasNext()
        {
            return it.hasNext();
        }

        @Override
        public Entry next()
        {
            var partitionKeyComponents = AccordKeyspace.CommandRows.splitPartitionKey(it.next().partitionKey());
            entry.store_id = AccordKeyspace.CommandRows.getStoreId(partitionKeyComponents);
            entry.txnId = AccordKeyspace.CommandRows.getTxnId(partitionKeyComponents);
            return entry;
        }
    }

    public static final class Entry
    {
        public int store_id;
        public TxnId txnId;
    }

    private static class SaiResultRetriever extends StorageAttachedIndexSearcher.ResultRetriever
    {
        public SaiResultRetriever(QueryController queryController, ReadExecutionController executionController, QueryContext queryContext, boolean topK)
        {
            super(queryController, executionController, queryContext, topK);
        }

        @Nonnull
        @Override
        protected UnfilteredRowIterator iteratePartition(@Nonnull UnfilteredRowIterator startIter)
        {
            return startIter;
        }

        @Override
        protected UnfilteredRowIterator queryStorageAndFilter(PrimaryKey key)
        {
            return toIterator(key);
        }
    }

    private static UnfilteredRowIterator toIterator(PrimaryKey key)
    {
        return new UnfilteredRowIterator()
        {
            @Override
            public boolean hasNext()
            {
                return false;
            }

            @Override
            public Unfiltered next()
            {
                throw new IllegalArgumentException("Should not have called yo!");
            }

            @Override
            public void close()
            {

            }

            @Override
            public DeletionTime partitionLevelDeletion()
            {
                return DeletionTime.LIVE; //TODO (now): LIES!
            }

            @Override
            public EncodingStats stats()
            {
                return EncodingStats.NO_STATS;
            }

            @Override
            public TableMetadata metadata()
            {
                return AccordKeyspace.Commands;
            }

            @Override
            public boolean isReverseOrder()
            {
                return false;
            }

            @Override
            public RegularAndStaticColumns columns()
            {
                return metadata().regularAndStaticColumns();
            }

            @Override
            public DecoratedKey partitionKey()
            {
                return key.partitionKey();
            }

            @Override
            public Row staticRow()
            {
                return Rows.EMPTY_STATIC_ROW;
            }
        };
    }
}
