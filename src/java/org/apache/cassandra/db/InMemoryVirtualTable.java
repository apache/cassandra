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
package org.apache.cassandra.db;

import static com.google.common.collect.Iterables.transform;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Row.SimpleBuilder;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.ColumnMetadata.Raw;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.PagingState.RowMark;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

/**
 * An implementation of VirtualTable that will handle paging, aggregations, filtering, selections and limits but
 * requires the entire result set to possibly be loaded into memory.
 *
 * Extending classes must handle the read method which will return up to the entire result but may optionally reduce the
 * returning rows itself based on the select statement.
 */
public abstract class InMemoryVirtualTable extends VirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(InMemoryVirtualTable.class);

    public InMemoryVirtualTable(TableMetadata metadata)
    {
        super(metadata);
    }

    public abstract void read(StatementRestrictions restrictions, QueryOptions options, ResultBuilder result);

    public ReadQuery getQuery(SelectStatement selectStatement, QueryOptions options, DataLimits limits, int nowInSec)
    {
        return new SimpleVirtualCommand(selectStatement, options, limits, options.getPagingState(), nowInSec);
    }

    protected final class SimpleVirtualCommand implements ReadQuery, QueryPager
    {
        private SelectStatement selectStatement;
        private StatementRestrictions restrictions;
        private QueryOptions options;
        private DataLimits limits;
        private int nowInSec;
        private ResultReadState readState;
        private RowFilter rowFilter;

        public SimpleVirtualCommand(SelectStatement selectStatement, QueryOptions options, DataLimits limits,
                PagingState state, int nowInSec)
        {
            this.selectStatement = selectStatement;
            this.options = options;
            this.limits = limits;
            this.nowInSec = nowInSec;
            this.restrictions = selectStatement.getRestrictions();
            this.rowFilter = RowFilter.create();
            for (ColumnMetadata cm : metadata.primaryKeyColumns())
            {
                for (Restriction r : restrictions.getPartitionKeyRestrictions().getRestrictions(cm))
                {
                    r.addRowFilterTo(rowFilter, null, options);
                }
                for (Restriction r : restrictions.getClusteringColumnsRestrictions().getRestrictions(cm))
                {
                    r.addRowFilterTo(rowFilter, null, options);
                }
            }

            readState = new ResultReadState(selectStatement, limits, options, state, fetch(options));
        }

        public ReadExecutionController executionController()
        {
            return ReadExecutionController.empty();
        }

        public ResultBuilder fetch(QueryOptions options)
                throws RequestExecutionException, RequestValidationException
        {
            ResultBuilder result = new ResultBuilder(rowFilter);
            Tracing.trace("Building virtual table results from {}", metadata.virtualClass().getName());
            read(restrictions, options, result);
            return result;
        }

        public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState,
                long queryStartNanoTime) throws RequestExecutionException
        {
            return UnfilteredPartitionIterators.filter(executeLocally(executionController()), nowInSec);
        }

        public PartitionIterator executeInternal(ReadExecutionController controller)
        {
            return UnfilteredPartitionIterators.filter(executeLocally(controller), nowInSec);
        }

        public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController)
        {
            return limits.filter(selectStatement
                    .getRestrictions()
                    .getRowFilter(null, options)
                    .filter(readState, nowInSec),
                    nowInSec, false);
        }

        public DataLimits limits()
        {
            return limits;
        }

        public QueryPager getPager(PagingState state, ProtocolVersion protocolVersion)
        {
            // clone of this to ensure mutable state doesnt conflict with other paged queries
            return new SimpleVirtualCommand(selectStatement, options, limits, state, nowInSec);
        }

        /* used for views */
        public boolean selectsKey(DecoratedKey key)
        {
            return false;
        }

        /* used for views */
        public boolean selectsClustering(DecoratedKey key, Clustering clustering)
        {
            return false;
        }

        /* used for views */
        public boolean selectsFullPartition()
        {
            return false;
        }

        public PartitionIterator fetchPage(int pageSize, ConsistencyLevel consistency, ClientState clientState,
                long queryStartNanoTime) throws RequestValidationException, RequestExecutionException
        {
            return fetchPageInternal(pageSize, null);
        }

        public PartitionIterator fetchPageInternal(int pageSize, ReadExecutionController executionController)
                throws RequestValidationException, RequestExecutionException
        {
            pageSize = Math.min(pageSize, limits.count());
            UnfilteredPartitionIterator i = limits.forPaging(pageSize)
                    .filter(executeLocally(executionController), nowInSec, false);
            return UnfilteredPartitionIterators.filter(i, nowInSec);
        }

        public boolean isExhausted()
        {
            return readState.isExhausted();
        }

        public int maxRemaining()
        {
            return readState.remaining();
        }

        public PagingState state()
        {
            return readState.state();
        }

        public QueryPager withUpdatedLimit(DataLimits newLimits)
        {
            limits = newLimits;
            return this;
        }
    }

    private class ResultReadState extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        private ByteBuffer lastPartition;
        private Row lastRow;
        private int seen;
        private int total = Integer.MAX_VALUE;
        private PagingState state;
        private Deque<Entry<DecoratedKey, List<Row>>> partitions;
        private ColumnFilter columnFilter;
        private ClusteringIndexFilter clusteringFilter;
        private DataLimits limits;

        public ResultReadState(SelectStatement selectStatement, DataLimits limits, QueryOptions options, PagingState state,
                ResultBuilder results)
        {
            this.state = state;
            this.limits = limits;
            columnFilter = selectStatement.getSelection().newSelectors(options).getColumnFilter();
            clusteringFilter = selectStatement.makeClusteringIndexFilter(options, columnFilter);
            results.add();
            total = (int) results.values.entrySet().stream().mapToLong(e -> e.getValue().size()).sum();
            for (Entry<DecoratedKey, List<Row>> e : results.values.entrySet())
            {
                List<Row> rows = e.getValue();
                if (selectStatement.parameters.orderings.isEmpty())
                    rows.sort(metadata.comparator);
                else
                {
                    ClusteringComparator clustering =
                            new ClusteringComparator(transform(metadata.clusteringColumns(),
                                    c -> {
                                        Boolean val = selectStatement.parameters.orderings.get(Raw.forUnquoted(c.name.toString()));
                                        if (val != null)
                                            return val? ReversedType.getInstance(c.type) : c.type;
                                        return c.type;
                                    }));
                    rows.sort(clustering);
                }
            }
            partitions = new LinkedList<>(results.values.entrySet());
            // skip ahead to where next partition was last one read in pagingstate
            Entry<DecoratedKey, List<Row>> rows = null;
            while (state != null
                    && metadata.partitionKeyType.compare(partitions.peek().getKey().getKey(), state.partitionKey) < 0)
            {
                rows = partitions.pop();
                seen += rows.getValue().size();
            }
        }

        public boolean isExhausted()
        {
            return total - seen <= 0;
        }

        public int remaining()
        {
            return total - seen;
        }

        public void close()
        {
        }

        public PagingState state()
        {
            if (lastPartition == null || lastRow == null || total <= seen)
                return null;
            return new PagingState(lastPartition, RowMark.create(metadata, lastRow, ProtocolVersion.CURRENT),
                    total - seen, 1);
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        private boolean shouldSkip(Row row)
        {
            if (state.rowMark == null)
                return false;
            if (metadata.comparator.compare(row.clustering(), state.rowMark.clustering(metadata)) < 0)
                return false;
            return true;
        }

        @Override
        protected synchronized UnfilteredRowIterator computeNext()
        {
            if (partitions.isEmpty())
                return endOfData();

            Entry<DecoratedKey, List<Row>> partition = partitions.peek();
            lastPartition = partition.getKey().getKey();
            List<Row> allRows = partition.getValue();
            Deque<Row> rows = new LinkedList<>(allRows.subList(0, Math.min(allRows.size(), limits.perPartitionCount())));
            // read through partition until get to last row read in page state
            while (state != null && !rows.isEmpty() && !shouldSkip(rows.pop()));

            if (!rows.isEmpty())
            {
                UnfilteredRowIterator r = new AbstractUnfilteredRowIterator(metadata,
                        partition.getKey(),
                        DeletionTime.LIVE,
                        metadata.regularAndStaticColumns(),
                        Rows.EMPTY_STATIC_ROW,
                        false,
                        EncodingStats.NO_STATS)
                {
                    protected Unfiltered computeNext()
                    {
                        if (rows.isEmpty())
                        {
                            state = null;
                            lastRow = null;
                            lastPartition = partitions.pop().getKey().getKey();
                            state = state();
                            return endOfData();
                        }
                        Row row = rows.pop();
                        seen++;
                        lastRow = row;
                        state = state();
                        return row;
                    }
                };
                return clusteringFilter.filterNotIndexed(columnFilter, r);
            }
            if (!partitions.isEmpty())
            {
                state = null;
                lastRow = null;
                partitions.pop();
                return computeNext();
            }
            return endOfData();
        }

    }

    public class PartitionBuilder
    {
        ResultBuilder parent;
        SimpleBuilder builder;
        DecoratedKey key;
        Row row;

        public PartitionBuilder(ResultBuilder parent, Object... keys)
        {
            this.parent = parent;
            int current = 0;
            int keyLength = metadata.partitionKeyColumns().size();
            key = SimpleBuilders.makePartitonKey(metadata, Arrays.copyOfRange(keys, 0, keyLength));
            current += keyLength;
            int clusteringLength = metadata.clusteringColumns().size();
            Object[] clustering = Arrays.copyOfRange(keys, current, current + clusteringLength);
            builder = new SimpleBuilders.RowBuilder(metadata, clustering);
        }

        public PartitionBuilder column(String key, Object value)
        {
            builder.add(key, value);
            return this;
        }

        public ResultBuilder endRow()
        {
            return parent;
        }
    }

    public Comparator<DecoratedKey> partitionComparator()
    {
        return (p1, p2) ->
        {
            return metadata.partitionKeyType.compare(p1.getKey(), p2.getKey());
        };
    }

    /**
     * Builds the Rows in the appropriate order, ignoring data that would be filtered.
     */
    public class ResultBuilder
    {
        TreeMap<DecoratedKey, List<Row>> values = new TreeMap<>(partitionComparator());
        RowFilter rowFilter;
        PartitionBuilder last;

        public ResultBuilder(RowFilter rowFilter)
        {
            this.rowFilter = rowFilter;
        }

        public PartitionBuilder row(Object... keys)
        {
            add();
            last = new PartitionBuilder(this, keys);
            return last;
        }

        void add()
        {
            if (last != null)
            {
                Row row = last.builder.build();
                if (rowFilter.isSatisfiedBy(metadata, last.key, row, FBUtilities.nowInSeconds()))
                    values.computeIfAbsent(last.key, k -> Lists.newArrayList()).add(row);
            }
            last = null;
        }

    }
}