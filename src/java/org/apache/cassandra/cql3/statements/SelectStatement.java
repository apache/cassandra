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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.Ordering;
import org.apache.cassandra.cql3.restrictions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.restrictions.ExternalRestriction;
import org.apache.cassandra.cql3.restrictions.Restrictions;
import org.apache.cassandra.cql3.restrictions.SingleRestriction;
import org.apache.cassandra.guardrails.Guardrails;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.ResultSetBuilder;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.selection.Selection.Selectors;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.AggregationQueryPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.db.filter.DataLimits.NO_LIMIT;
import static org.apache.cassandra.utils.ByteBufferUtil.UNSET_BYTE_BUFFER;

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 * A number of public methods here are only used internally. However,
 * many of these are made accessible for the benefit of custom
 * QueryHandler implementations, so before reducing their accessibility
 * due consideration should be given.
 */
public class SelectStatement implements CQLStatement.SingleKeyspaceCqlStatement
{
    private static final Logger logger = LoggerFactory.getLogger(SelectStatement.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(SelectStatement.logger, 1, TimeUnit.MINUTES);

    private final String rawCQLStatement;
    public final VariableSpecifications bindVariables;
    public final TableMetadata table;
    public final Parameters parameters;
    private final Selection selection;
    private final Term limit;
    private final Term perPartitionLimit;

    private final StatementRestrictions restrictions;

    private final boolean isReversed;

    /**
     * The <code>AggregationSpecification</code> used to make the aggregates.
     */
    private final AggregationSpecification aggregationSpec;

    /**
     * The comparator used to orders results when multiple keys are selected (using IN).
     */
    private final ColumnComparator<List<ByteBuffer>> orderingComparator;

    // Used by forSelection below
    private static final Parameters defaultParameters = new Parameters(Collections.emptyList(),
                                                                       Collections.emptyList(),
                                                                       false,
                                                                       false,
                                                                       false);

    public SelectStatement(String queryString,
                           TableMetadata table,
                           VariableSpecifications bindVariables,
                           Parameters parameters,
                           Selection selection,
                           StatementRestrictions restrictions,
                           boolean isReversed,
                           AggregationSpecification aggregationSpec,
                           ColumnComparator<List<ByteBuffer>> orderingComparator,
                           Term limit,
                           Term perPartitionLimit)
    {
        this.rawCQLStatement = queryString;
        this.table = table;
        this.bindVariables = bindVariables;
        this.selection = selection;
        this.restrictions = restrictions;
        this.isReversed = isReversed;
        this.aggregationSpec = aggregationSpec;
        this.orderingComparator = orderingComparator;
        this.parameters = parameters;
        this.limit = limit;
        this.perPartitionLimit = perPartitionLimit;
    }

    @Override
    public String getRawCQLStatement()
    {
        return rawCQLStatement;
    }

    @Override
    public List<ColumnSpecification> getBindVariables()
    {
        return bindVariables.getBindVariables();
    }

    @Override
    public short[] getPartitionKeyBindVariableIndexes()
    {
        return bindVariables.getPartitionKeyBindVariableIndexes(table);
    }

    @Override
    public Iterable<Function> getFunctions()
    {
        List<Function> functions = new ArrayList<>();
        addFunctionsTo(functions);
        return functions;
    }

    private void addFunctionsTo(List<Function> functions)
    {
        selection.addFunctionsTo(functions);
        restrictions.addFunctionsTo(functions);

        if (limit != null)
            limit.addFunctionsTo(functions);

        if (perPartitionLimit != null)
            perPartitionLimit.addFunctionsTo(functions);
    }

    /**
     * The columns to fetch internally for this SELECT statement (which can be more than the one selected by the
     * user as it also include any restricted column in particular).
     */
    public ColumnFilter queriedColumns()
    {
        return selection.newSelectors(QueryOptions.DEFAULT).getColumnFilter();
    }

    // Creates a simple select based on the given selection.
    // Note that the results select statement should not be used for actual queries, but only for processing already
    // queried data through processColumnFamily.
    static SelectStatement forSelection(TableMetadata table, Selection selection)
    {
        return new SelectStatement(null,
                                   table,
                                   VariableSpecifications.empty(),
                                   defaultParameters,
                                   selection,
                                   StatementRestrictions.empty(table),
                                   false,
                                   null,
                                   null,
                                   null,
                                   null);
    }

    public ResultSet.ResultMetadata getResultMetadata()
    {
        return selection.getResultMetadata();
    }

    public void authorize(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        if (table.isView())
        {
            TableMetadataRef baseTable = View.findBaseTable(keyspace(), columnFamily());
            if (baseTable != null)
                state.ensureTablePermission(baseTable, Permission.SELECT);
        }
        else
        {
            state.ensureTablePermission(table, Permission.SELECT);
        }

        for (Function function : getFunctions())
            state.ensurePermission(Permission.EXECUTE, function);
    }

    @Override
    public void validate(QueryState state) throws InvalidRequestException
    {
        // Nothing to do, all validation has been done by RawStatement.prepare()
    }

    /**
     * Adds the specified restrictions to the index restrictions.
     *
     * @param indexRestrictions the index restrictions to add
     * @return a new {@code SelectStatement} instance with the added index restrictions
     */
    public SelectStatement addIndexRestrictions(Restrictions indexRestrictions)
    {
        return new SelectStatement(rawCQLStatement,
                                   table,
                                   bindVariables,
                                   parameters,
                                   selection,
                                   restrictions.addIndexRestrictions(indexRestrictions),
                                   isReversed,
                                   aggregationSpec,
                                   orderingComparator,
                                   limit,
                                   perPartitionLimit);
    }

    /**
     * Adds the specified external restrictions to the index restrictions.
     *
     * @param indexRestrictions the index restrictions to add
     * @return a new {@code SelectStatement} instance with the added index restrictions
     */
    public SelectStatement addIndexRestrictions(Iterable<ExternalRestriction> indexRestrictions)
    {
        return new SelectStatement(rawCQLStatement,
                                   table,
                                   bindVariables,
                                   parameters,
                                   selection,
                                   restrictions.addExternalRestrictions(indexRestrictions),
                                   isReversed,
                                   aggregationSpec,
                                   orderingComparator,
                                   limit,
                                   perPartitionLimit);
    }

    private void validateQueryOptions(QueryState queryState, QueryOptions options)
    {
        PageSize pageSize = options.getPageSize();
        if (pageSize != null && options.getPageSize().isDefined() && pageSize.getUnit() == PageSize.PageUnit.BYTES)
        {
            Guardrails.pageSize.guard(pageSize.bytes(), "in bytes", false, queryState);
        }
    }

    /**
     * Returns whether the paging can be skipped based on the user limits and the page size - that is, if the user limit
     * is provided and is lower than the page size, it means that we will only return at most one page and thus paging
     * is unnecessary in this case. That applies to the page size defined in rows - if the page size is defined in bytes
     * we cannot say anything about the relation beteween the user rows limit and the page size.
     */
    private boolean canSkipPaging(DataLimits userLimits, PageSize pageSize, boolean topK)
    {
        return !pageSize.isDefined() ||
               pageSize.getUnit() == PageSize.PageUnit.ROWS && !pageSize.isCompleted(userLimits.count(), PageSize.PageUnit.ROWS) ||
               topK;
    }

    public ResultMessage.Rows execute(QueryState queryState, QueryOptions options, long queryStartNanoTime)
    {
        ConsistencyLevel cl = options.getConsistency();
        checkNotNull(cl, "Invalid empty consistency level");

        cl.validateForRead();
        validateQueryOptions(queryState, options);

        int nowInSec = options.getNowInSeconds(queryState);
        int userLimit = getLimit(options);
        int userPerPartitionLimit = getPerPartitionLimit(options);
        PageSize pageSize = options.getPageSize();

        Selectors selectors = selection.newSelectors(options);
        ReadQuery query = getQuery(queryState, options, selectors.getColumnFilter(), nowInSec, userLimit, userPerPartitionLimit, pageSize);

        if (query.limits().isGroupByLimit() && pageSize != null && pageSize.isDefined() && pageSize.getUnit() == PageSize.PageUnit.BYTES)
            throw new InvalidRequestException("Paging in bytes cannot be specified for aggregation queries");

        if (aggregationSpec == null && canSkipPaging(query.limits(), pageSize, query.isTopK()))
            return execute(query, options, queryState, selectors, nowInSec, userLimit, queryStartNanoTime);

        QueryPager pager = getPager(query, options);

        return execute(Pager.forDistributedQuery(pager, cl, queryState),
                       options,
                       selectors,
                       pageSize,
                       nowInSec,
                       userLimit,
                       queryStartNanoTime);
    }

    public ReadQuery getQuery(QueryState state, QueryOptions options, int nowInSec) throws RequestValidationException
    {
        Selectors selectors = selection.newSelectors(options);
        return getQuery(state,
                        options,
                        selectors.getColumnFilter(),
                        nowInSec,
                        getLimit(options),
                        getPerPartitionLimit(options),
                        options.getPageSize());
    }

    public ReadQuery getQuery(QueryState queryState,
                              QueryOptions options,
                              ColumnFilter columnFilter,
                              int nowInSec,
                              int userLimit,
                              int perPartitionLimit,
                              PageSize pageSize)
    {
        boolean isPartitionRangeQuery = restrictions.isKeyRange() || restrictions.usesSecondaryIndexing() || restrictions.isDisjunction();

        DataLimits limit = getDataLimits(queryState, userLimit, perPartitionLimit);

        if (isPartitionRangeQuery)
            return getRangeCommand(options, columnFilter, limit, nowInSec, queryState);

        return getSliceCommands(queryState, options, columnFilter, limit, nowInSec);
    }

    private ResultMessage.Rows execute(ReadQuery query,
                                       QueryOptions options,
                                       QueryState queryState,
                                       Selectors selectors,
                                       int nowInSec,
                                       int userLimit, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException
    {
        try (PartitionIterator data = query.execute(options.getConsistency(), queryState, queryStartNanoTime))
        {
            return processResults(data, options, selectors, nowInSec, userLimit);
        }
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.SELECT, keyspace(), table.name);
    }

    // Simple wrapper class to avoid some code duplication
    private static abstract class Pager
    {
        protected QueryPager pager;

        protected Pager(QueryPager pager)
        {
            this.pager = pager;
        }

        public static Pager forInternalQuery(QueryPager pager, ReadExecutionController executionController)
        {
            return new InternalPager(pager, executionController);
        }

        public static Pager forDistributedQuery(QueryPager pager, ConsistencyLevel consistency, QueryState queryState)
        {
            return new NormalPager(pager, consistency, queryState);
        }

        public boolean isExhausted()
        {
            return pager.isExhausted();
        }

        public PagingState state()
        {
            return pager.state();
        }

        public abstract PartitionIterator fetchPage(PageSize pageSize, long queryStartNanoTime);

        public static class NormalPager extends Pager
        {
            private final ConsistencyLevel consistency;
            private final QueryState queryState;

            private NormalPager(QueryPager pager, ConsistencyLevel consistency, QueryState queryState)
            {
                super(pager);
                this.consistency = consistency;
                this.queryState = queryState;
            }

            public PartitionIterator fetchPage(PageSize pageSize, long queryStartNanoTime)
            {
                return pager.fetchPage(pageSize, consistency, queryState, queryStartNanoTime);
            }
        }

        public static class InternalPager extends Pager
        {
            private final ReadExecutionController executionController;

            private InternalPager(QueryPager pager, ReadExecutionController executionController)
            {
                super(pager);
                this.executionController = executionController;
            }

            public PartitionIterator fetchPage(PageSize pageSize, long queryStartNanoTime)
            {
                return pager.fetchPageInternal(pageSize, executionController);
            }
        }
    }

    private ResultMessage.Rows execute(Pager pager,
                                       QueryOptions options,
                                       Selectors selectors,
                                       PageSize pageSize,
                                       int nowInSec,
                                       int userLimit,
                                       long queryStartNanoTime) throws RequestValidationException, RequestExecutionException
    {
        if (aggregationSpec != null)
        {
            if (!restrictions.hasPartitionKeyRestrictions())
            {
                warn("Aggregation query used without partition key");
                noSpamLogger.warn(String.format("Aggregation query used without partition key on table %s.%s, aggregation type: %s",
                                                 keyspace(), columnFamily(), aggregationSpec.kind()));
            }
            else if (restrictions.keyIsInRelation())
            {
                warn("Aggregation query used on multiple partition keys (IN restriction)");
                noSpamLogger.warn(String.format("Aggregation query used on multiple partition keys (IN restriction) on table %s.%s, aggregation type: %s",
                                                 keyspace(), columnFamily(), aggregationSpec.kind()));
            }
        }

        // We can't properly do post-query ordering if we page (see #6722)
        // For GROUP BY or aggregation queries we always page internally even if the user has turned paging off
        checkFalse(pageSize.isDefined() && needsPostQueryOrdering(),
                  "Cannot page queries with both ORDER BY and a IN restriction on the partition key;"
                  + " you must either remove the ORDER BY or the IN and sort client side, or disable paging for this query");

        ResultMessage.Rows msg;
        try (PartitionIterator page = pager.fetchPage(pageSize, queryStartNanoTime))
        {
            msg = processResults(page, options, selectors, nowInSec, userLimit);
        }

        // Please note that the isExhausted state of the pager only gets updated when we've closed the page, so this
        // shouldn't be moved inside the 'try' above.
        if (!pager.isExhausted() && !pager.pager.isTopK())
            msg.result.metadata.setHasMorePages(pager.state());

        return msg;
    }

    private void warn(String msg)
    {
        logger.warn(msg);
        ClientWarn.instance.warn(msg);
    }

    private ResultMessage.Rows processResults(PartitionIterator partitions,
                                              QueryOptions options,
                                              Selectors selectors,
                                              int nowInSec,
                                              int userLimit) throws RequestValidationException
    {
        ResultSet rset = process(partitions, options, selectors, nowInSec, userLimit);
        return new ResultMessage.Rows(rset);
    }

    public ResultMessage.Rows executeLocally(QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        return executeInternal(state, options, options.getNowInSeconds(state), System.nanoTime());
    }

    public ResultMessage.Rows executeInternal(QueryState state, QueryOptions options, int nowInSec, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
    {
        int userLimit = getLimit(options);
        int userPerPartitionLimit = getPerPartitionLimit(options);
        PageSize pageSize = options.getPageSize();

        Selectors selectors = selection.newSelectors(options);
        ReadQuery query = getQuery(state, options, selectors.getColumnFilter(), nowInSec, userLimit, userPerPartitionLimit, pageSize);

        try (ReadExecutionController executionController = query.executionController())
        {
            if (aggregationSpec == null && canSkipPaging(query.limits(), pageSize, query.isTopK()))
            {
                try (PartitionIterator data = query.executeInternal(executionController))
                {
                    return processResults(data, options, selectors, nowInSec, userLimit);
                }
            }

            QueryPager pager = getPager(query, options);

            return execute(Pager.forInternalQuery(pager, executionController),
                           options,
                           selectors,
                           pageSize,
                           nowInSec,
                           userLimit,
                           queryStartNanoTime);
        }
    }

    @VisibleForTesting
    public QueryPager getPager(ReadQuery query, QueryOptions options)
    {
        QueryPager pager = query.getPager(options.getPagingState(), options.getProtocolVersion());

        if (aggregationSpec == null || query.isEmpty())
            return pager;

        return new AggregationQueryPager(pager, DatabaseDescriptor.getAggregationSubPageSize(), query.limits());
    }

    public ResultSet process(PartitionIterator partitions, int nowInSec) throws InvalidRequestException
    {
        QueryOptions options = QueryOptions.DEFAULT;
        Selectors selectors = selection.newSelectors(options);
        return process(partitions, options, selectors, nowInSec, getLimit(options));
    }

    @Override
    public String keyspace()
    {
        return table.keyspace;
    }

    public String columnFamily()
    {
        return table.name;
    }

    /**
     * May be used by custom QueryHandler implementations
     */
    public Selection getSelection()
    {
        return selection;
    }

    /**
     * May be used by custom QueryHandler implementations
     */
    public StatementRestrictions getRestrictions()
    {
        return restrictions;
    }

    private ReadQuery getSliceCommands(QueryState queryState, QueryOptions options, ColumnFilter columnFilter, DataLimits limit, int nowInSec)
    {
        Collection<ByteBuffer> keys = restrictions.getPartitionKeys(options, queryState);
        if (keys.isEmpty())
            return ReadQuery.empty(table);

        Guardrails.partitionKeysInSelectQuery.guard(keys.size(), "Select query", false, queryState);

        ClusteringIndexFilter filter = makeClusteringIndexFilter(options, columnFilter, queryState);
        if (filter == null || filter.isEmpty(table.comparator))
            return ReadQuery.empty(table);

        RowFilter rowFilter = getRowFilter(options);

        List<DecoratedKey> decoratedKeys = new ArrayList<>(keys.size());
        for (ByteBuffer key : keys)
        {
            QueryProcessor.validateKey(key);
            decoratedKeys.add(table.partitioner.decorateKey(ByteBufferUtil.clone(key)));
        }

        return SinglePartitionReadQuery.createGroup(table, nowInSec, columnFilter, rowFilter, limit, decoratedKeys, filter);
    }

    /**
     * Returns the slices fetched by this SELECT, assuming an internal call (no bound values in particular).
     * <p>
     * Note that if the SELECT intrinsically selects rows by names, we convert them into equivalent slices for
     * the purpose of this method. This is used for MVs to restrict what needs to be read when we want to read
     * everything that could be affected by a given view (and so, if the view SELECT statement has restrictions
     * on the clustering columns, we can restrict what we read).
     */
    public Slices clusteringIndexFilterAsSlices()
    {
        QueryState state = QueryState.forInternalCalls();
        QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
        ColumnFilter columnFilter = selection.newSelectors(options).getColumnFilter();
        ClusteringIndexFilter filter = makeClusteringIndexFilter(options, columnFilter, state);
        if (filter instanceof ClusteringIndexSliceFilter)
            return ((ClusteringIndexSliceFilter)filter).requestedSlices();

        Slices.Builder builder = new Slices.Builder(table.comparator);
        for (Clustering<?> clustering: ((ClusteringIndexNamesFilter)filter).requestedRows())
            builder.add(Slice.make(clustering));
        return builder.build();
    }

    /**
     * Returns a read command that can be used internally to query all the rows queried by this SELECT for a
     * give key (used for materialized views).
     */
    public SinglePartitionReadCommand internalReadForView(DecoratedKey key, int nowInSec)
    {
        QueryState state = QueryState.forInternalCalls();
        QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
        ColumnFilter columnFilter = selection.newSelectors(options).getColumnFilter();
        ClusteringIndexFilter filter = makeClusteringIndexFilter(options, columnFilter, state);
        RowFilter rowFilter = getRowFilter(options);
        return SinglePartitionReadCommand.create(table, nowInSec, columnFilter, rowFilter, DataLimits.NONE, key, filter);
    }

    /**
     * The {@code RowFilter} for this SELECT, assuming an internal call (no bound values in particular).
     */
    public RowFilter rowFilterForInternalCalls()
    {
        return getRowFilter(QueryOptions.forInternalCalls(Collections.emptyList()));
    }

    private ReadQuery getRangeCommand(QueryOptions options, ColumnFilter columnFilter, DataLimits limit, int nowInSec, QueryState queryState)
    {
        ClusteringIndexFilter clusteringIndexFilter = makeClusteringIndexFilter(options, columnFilter, queryState);
        if (clusteringIndexFilter == null)
            return ReadQuery.empty(table);

        RowFilter rowFilter = getRowFilter(options);

        // The LIMIT provided by the user is the number of CQL row he wants returned.
        // We want to have getRangeSlice to count the number of columns, not the number of keys.
        AbstractBounds<PartitionPosition> keyBounds = restrictions.getPartitionKeyBounds(options);
        if (keyBounds == null)
            return ReadQuery.empty(table);

        ReadQuery command =
            PartitionRangeReadQuery.create(table, nowInSec, columnFilter, rowFilter, limit, new DataRange(keyBounds, clusteringIndexFilter));

        // If there's a secondary index that the command can use, have it validate the request parameters.
        command.maybeValidateIndex();

        return command;
    }

    private ClusteringIndexFilter makeClusteringIndexFilter(QueryOptions options, ColumnFilter columnFilter, QueryState queryState)
    {
        if (parameters.isDistinct)
        {
            // We need to be able to distinguish between partition having live rows and those that don't. But
            // doing so is not trivial since "having a live row" depends potentially on
            //   1) when the query is performed, due to TTLs
            //   2) how thing reconcile together between different nodes
            // so that it's hard to really optimize properly internally. So to keep it simple, we simply query
            // for the first row of the partition and hence uses Slices.ALL. We'll limit it to the first live
            // row however in getLimit().
            return new ClusteringIndexSliceFilter(Slices.ALL, false);
        }

        if (restrictions.isDisjunction())
        {
            return new ClusteringIndexSliceFilter(Slices.ALL, false);
        }

        if (restrictions.isColumnRange())
        {
            Slices slices = makeSlices(options);
            if (slices == Slices.NONE && !selection.containsStaticColumns())
                return null;

            return new ClusteringIndexSliceFilter(slices, isReversed);
        }

        NavigableSet<Clustering<?>> clusterings = getRequestedRows(options, queryState);
        // We can have no clusterings if either we're only selecting the static columns, or if we have
        // a 'IN ()' for clusterings. In that case, we still want to query if some static columns are
        // queried. But we're fine otherwise.
        if (clusterings.isEmpty() && columnFilter.fetchedColumns().statics.isEmpty())
            return null;

        return new ClusteringIndexNamesFilter(clusterings, isReversed);
    }

    @VisibleForTesting
    public Slices makeSlices(QueryOptions options)
    throws InvalidRequestException
    {
        SortedSet<ClusteringBound<?>> startBounds = restrictions.getClusteringColumnsBounds(Bound.START, options);
        SortedSet<ClusteringBound<?>> endBounds = restrictions.getClusteringColumnsBounds(Bound.END, options);
        assert startBounds.size() == endBounds.size();

        // The case where startBounds == 1 is common enough that it's worth optimizing
        if (startBounds.size() == 1)
        {
            ClusteringBound<?> start = startBounds.first();
            ClusteringBound<?> end = endBounds.first();
            return Slice.isEmpty(table.comparator, start, end)
                 ? Slices.NONE
                 : Slices.with(table.comparator, Slice.make(start, end));
        }

        Slices.Builder builder = new Slices.Builder(table.comparator, startBounds.size());
        Iterator<ClusteringBound<?>> startIter = startBounds.iterator();
        Iterator<ClusteringBound<?>> endIter = endBounds.iterator();
        while (startIter.hasNext() && endIter.hasNext())
        {
            ClusteringBound<?> start = startIter.next();
            ClusteringBound<?> end = endIter.next();

            // Ignore slices that are nonsensical
            if (Slice.isEmpty(table.comparator, start, end))
                continue;

            builder.add(start, end);
        }

        return builder.build();
    }

    private DataLimits getDataLimits(QueryState queryState, int userLimit, int perPartitionLimit)
    {
        int cqlRowLimit = NO_LIMIT;
        int cqlPerPartitionLimit = NO_LIMIT;

        // If we do post ordering we need to get all the results sorted before we can trim them.
        if (aggregationSpec != AggregationSpecification.AGGREGATE_EVERYTHING)
        {
            if (!needsToSkipUserLimit())
                cqlRowLimit = userLimit;
            cqlPerPartitionLimit = perPartitionLimit;
        }

        DataLimits limits = null;

        // Aggregation queries work fine on top of the group by paging but to maintain
        // backward compatibility we need to use the old way.
        if (aggregationSpec != null && aggregationSpec != AggregationSpecification.AGGREGATE_EVERYTHING)
        {
            if (parameters.isDistinct)
                limits = DataLimits.distinctLimits(cqlRowLimit);
            else
                limits = DataLimits.groupByLimits(cqlRowLimit,
                                                  cqlPerPartitionLimit,
                                                  NO_LIMIT,
                                                  NO_LIMIT,
                                                  aggregationSpec);
        }
        else
        {
            if (parameters.isDistinct)
                limits = cqlRowLimit == NO_LIMIT ? DataLimits.DISTINCT_NONE : DataLimits.distinctLimits(cqlRowLimit);
            else
                limits = DataLimits.cqlLimits(cqlRowLimit, cqlPerPartitionLimit);
        }

        if (!limits.isGroupByLimit() && Guardrails.pageSize.enabled(queryState))
        {
            int bytesLimit = DatabaseDescriptor.getGuardrailsConfig().page_size_failure_threshold_in_kb * 1024;
            String limitStr = "Applied page size limit of " + FBUtilities.prettyPrintMemory(bytesLimit);
            ClientWarn.instance.warn(limitStr);
            logger.trace(limitStr);
            limits = limits.forPaging(PageSize.inBytes(bytesLimit));
        }

        return limits;
    }

    /**
     * Returns the limit specified by the user.
     * May be used by custom QueryHandler implementations
     *
     * @return the limit specified by the user or <code>DataLimits.NO_LIMIT</code> if no value
     * as been specified.
     */
    public int getLimit(QueryOptions options)
    {
        return getLimit(limit, options);
    }

    /**
     * Returns the per partition limit specified by the user.
     * May be used by custom QueryHandler implementations
     *
     * @return the per partition limit specified by the user or <code>DataLimits.NO_LIMIT</code> if no value
     * as been specified.
     */
    public int getPerPartitionLimit(QueryOptions options)
    {
        return getLimit(perPartitionLimit, options);
    }

    private int getLimit(Term limit, QueryOptions options)
    {
        int userLimit = NO_LIMIT;

        if (limit != null)
        {
            ByteBuffer b = checkNotNull(limit.bindAndGet(options), "Invalid null value of limit");
            // treat UNSET limit value as 'unlimited'
            if (b != UNSET_BYTE_BUFFER)
            {
                try
                {
                    Int32Type.instance.validate(b);
                    userLimit = Int32Type.instance.compose(b);
                    checkTrue(userLimit > 0, "LIMIT must be strictly positive");
                }
                catch (MarshalException e)
                {
                    throw new InvalidRequestException("Invalid limit value");
                }
            }
        }
        return userLimit;
    }

    private NavigableSet<Clustering<?>> getRequestedRows(QueryOptions options, QueryState queryState) throws InvalidRequestException
    {
        // Note: getRequestedColumns don't handle static columns, but due to CASSANDRA-5762
        // we always do a slice for CQL3 tables, so it's ok to ignore them here
        assert !restrictions.isColumnRange();
        return restrictions.getClusteringColumns(options, queryState);
    }

    /**
     * May be used by custom QueryHandler implementations
     */
    public RowFilter getRowFilter(QueryOptions options) throws InvalidRequestException
    {
        IndexRegistry indexRegistry = IndexRegistry.obtain(table);
        return restrictions.getRowFilter(indexRegistry, options);
    }

    private ResultSet process(PartitionIterator partitions,
                              QueryOptions options,
                              Selectors selectors,
                              int nowInSec,
                              int userLimit) throws InvalidRequestException
    {
        GroupMaker groupMaker = aggregationSpec == null ? null : aggregationSpec.newGroupMaker();
        ResultSetBuilder result = new ResultSetBuilder(getResultMetadata(), selectors, groupMaker);

        while (partitions.hasNext())
        {
            try (RowIterator partition = partitions.next())
            {
                processPartition(partition, options, result, nowInSec);
            }
        }

        ResultSet cqlRows = result.build();

        ColumnFamilyStore store = cfs();
        if (store != null)
            store.metric.coordinatorReadSize.update(calculateSize(cqlRows.rows));

        orderResults(cqlRows, options);

        cqlRows.trim(userLimit);

        return cqlRows;
    }

    public ColumnFamilyStore cfs()
    {
        return Schema.instance.getColumnFamilyStoreInstance(table.id);
    }

    private int calculateSize(List<List<ByteBuffer>> rows)
    {
        int size = 0;
        for (List<ByteBuffer> row : rows)
        {
            for (int i = 0, isize = row.size(); i < isize; i++)
            {
                ByteBuffer value = row.get(i);
                size += value != null ? value.remaining() : 0;
            }
        }
        return size;
    }

    public static ByteBuffer[] getComponents(TableMetadata metadata, DecoratedKey dk)
    {
        ByteBuffer key = dk.getKey();
        if (metadata.partitionKeyType instanceof CompositeType)
        {
            return ((CompositeType)metadata.partitionKeyType).split(key);
        }
        else
        {
            return new ByteBuffer[]{ key };
        }
    }

    // Used by ModificationStatement for CAS operations
    void processPartition(RowIterator partition, QueryOptions options, ResultSetBuilder result, int nowInSec)
    throws InvalidRequestException
    {
        ProtocolVersion protocolVersion = options.getProtocolVersion();

        ByteBuffer[] keyComponents = getComponents(table, partition.partitionKey());

        Row staticRow = partition.staticRow();
        // If there is no rows, we include the static content if we should and we're done.
        if (!partition.hasNext())
        {
            if (!staticRow.isEmpty() && restrictions.returnStaticContentOnPartitionWithNoRows())
            {
                result.newRow(partition.partitionKey(), staticRow.clustering());
                for (ColumnMetadata def : selection.getColumns())
                {
                    switch (def.kind)
                    {
                        case PARTITION_KEY:
                            result.add(keyComponents[def.position()]);
                            break;
                        case STATIC:
                            addValue(result, def, staticRow, nowInSec, protocolVersion);
                            break;
                        default:
                            result.add((ByteBuffer)null);
                    }
                }
            }
            return;
        }

        while (partition.hasNext())
        {
            Row row = partition.next();
            result.newRow( partition.partitionKey(), row.clustering());
            // Respect selection order
            for (ColumnMetadata def : selection.getColumns())
            {
                switch (def.kind)
                {
                    case PARTITION_KEY:
                        result.add(keyComponents[def.position()]);
                        break;
                    case CLUSTERING:
                        result.add(row.clustering().bufferAt(def.position()));
                        break;
                    case REGULAR:
                        addValue(result, def, row, nowInSec, protocolVersion);
                        break;
                    case STATIC:
                        addValue(result, def, staticRow, nowInSec, protocolVersion);
                        break;
                }
            }
        }
    }

    private static void addValue(ResultSetBuilder result, ColumnMetadata def, Row row, int nowInSec, ProtocolVersion protocolVersion)
    {
        if (def.isComplex())
        {
            assert def.type.isMultiCell();
            ComplexColumnData complexData = row.getComplexColumnData(def);
            if (complexData == null)
                result.add(null);
            else if (def.type.isCollection())
                result.add(((CollectionType) def.type).serializeForNativeProtocol(complexData.iterator(), protocolVersion));
            else
                result.add(((UserType) def.type).serializeForNativeProtocol(complexData.iterator(), protocolVersion));
        }
        else
        {
            result.add(row.getCell(def), nowInSec);
        }
    }

    private boolean needsToSkipUserLimit()
    {
        // if post query ordering is required, and it's not ANN
        return needsPostQueryOrdering() && !needIndexOrdering();
    }

    private boolean needsPostQueryOrdering()
    {
        // We need post-query ordering only for queries with IN on the partition key and an ORDER BY or index restriction reordering
        return restrictions.keyIsInRelation() && !parameters.orderings.isEmpty() || needIndexOrdering();
    }

    private boolean needIndexOrdering()
    {
        return orderingComparator != null && orderingComparator.indexOrdering();
    }

    /**
     * Orders results when multiple keys are selected (using IN)
     */
    private void orderResults(ResultSet cqlRows, QueryOptions options)
    {
        if (cqlRows.size() == 0 || !needsPostQueryOrdering())
            return;

        Comparator<List<ByteBuffer>> comparator = orderingComparator.prepareFor(table, options);
        if (comparator != null)
            Collections.sort(cqlRows.rows, comparator);
    }

    public static class RawStatement extends QualifiedStatement<SelectStatement>
    {
        public final Parameters parameters;
        public final List<RawSelector> selectClause;
        public final WhereClause whereClause;
        public final Term.Raw limit;
        public final Term.Raw perPartitionLimit;

        public RawStatement(QualifiedName cfName,
                            Parameters parameters,
                            List<RawSelector> selectClause,
                            WhereClause whereClause,
                            Term.Raw limit,
                            Term.Raw perPartitionLimit)
        {
            super(cfName);
            this.parameters = parameters;
            this.selectClause = selectClause;
            this.whereClause = whereClause;
            this.limit = limit;
            this.perPartitionLimit = perPartitionLimit;
        }

        @Override
        public SelectStatement prepare(ClientState state, UnaryOperator<String> keyspaceMapper)
        {
            setKeyspace(state);
            return prepare(false, keyspaceMapper);
        }

        public SelectStatement prepare(boolean forView, UnaryOperator<String> keyspaceMapper) throws InvalidRequestException
        {
            String ks = keyspaceMapper.apply(keyspace());
            TableMetadata table = Schema.instance.validateTable(ks, name());

            List<Selectable> selectables = RawSelector.toSelectables(selectClause, table);
            boolean containsOnlyStaticColumns = selectOnlyStaticColumns(table, selectables);

            List<Ordering> orderings = getOrderings(table);
            StatementRestrictions restrictions = prepareRestrictions(
                    table, bindVariables, orderings, containsOnlyStaticColumns, forView);

            // If we order post-query, the sorted column needs to be in the ResultSet for sorting,
            // even if we don't ultimately ship them to the client (CASSANDRA-4911).
            Map<ColumnMetadata, Ordering> orderingColumns = getOrderingColumns(orderings);
            Set<ColumnMetadata> resultSetOrderingColumns = getResultSetOrdering(restrictions, orderingColumns);

            Selection selection = prepareSelection(table,
                                                   selectables,
                                                   bindVariables,
                                                   resultSetOrderingColumns,
                                                   restrictions);

            if (parameters.isDistinct)
            {
                checkNull(perPartitionLimit, "PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries");
                validateDistinctSelection(table, selection, restrictions);
            }

            AggregationSpecification aggregationSpec = getAggregationSpecification(table,
                                                                                   selection,
                                                                                   restrictions,
                                                                                   parameters.isDistinct);

            checkFalse(aggregationSpec == AggregationSpecification.AGGREGATE_EVERYTHING && perPartitionLimit != null,
                       "PER PARTITION LIMIT is not allowed with aggregate queries.");

            ColumnComparator<List<ByteBuffer>> orderingComparator = null;
            boolean isReversed = false;

            if (!orderingColumns.isEmpty())
            {
                assert !forView;
                verifyOrderingIsAllowed(restrictions, orderingColumns);
                orderingComparator = getOrderingComparator(selection, restrictions, orderingColumns);
                isReversed = isReversed(table, orderingColumns, restrictions);
                if (isReversed && orderingComparator != null)
                    orderingComparator = orderingComparator.reverse();
            }

            checkDisjunctionIsSupported(table, restrictions);

            checkNeedsFiltering(table, restrictions);

            return new SelectStatement(rawCQLStatement,
                                       table,
                                       bindVariables,
                                       parameters,
                                       selection,
                                       restrictions,
                                       isReversed,
                                       aggregationSpec,
                                       orderingComparator,
                                       prepareLimit(bindVariables, limit, ks, limitReceiver()),
                                       prepareLimit(bindVariables, perPartitionLimit, ks, perPartitionLimitReceiver()));
        }

        private Set<ColumnMetadata> getResultSetOrdering(StatementRestrictions restrictions, Map<ColumnMetadata, Ordering> orderingColumns)
        {
            if (restrictions.keyIsInRelation() || orderingColumns.values().stream().anyMatch(o -> o.expression.hasNonClusteredOrdering()))
                return orderingColumns.keySet();
            return Collections.emptySet();
        }

        private Selection prepareSelection(TableMetadata table,
                                           List<Selectable> selectables,
                                           VariableSpecifications boundNames,
                                           Set<ColumnMetadata> resultSetOrderingColumns,
                                           StatementRestrictions restrictions)
        {
            boolean hasGroupBy = !parameters.groups.isEmpty();

            if (selectables.isEmpty()) // wildcard query
            {
                return hasGroupBy ? Selection.wildcardWithGroupBy(table, boundNames, parameters.isJson, restrictions.returnStaticContentOnPartitionWithNoRows())
                                  : Selection.wildcard(table, parameters.isJson, restrictions.returnStaticContentOnPartitionWithNoRows());
            }

            return Selection.fromSelectors(table,
                                           selectables,
                                           boundNames,
                                           resultSetOrderingColumns,
                                           restrictions.nonPKRestrictedColumns(false),
                                           hasGroupBy,
                                           parameters.isJson,
                                           restrictions.returnStaticContentOnPartitionWithNoRows());
        }

        /**
         * Checks if the specified selectables select only partition key columns or static columns
         *
         * @param table the table metadata
         * @param selectables the selectables to check
         * @return {@code true} if the specified selectables select only partition key columns or static columns,
         * {@code false} otherwise.
         */
        private boolean selectOnlyStaticColumns(TableMetadata table, List<Selectable> selectables)
        {
            if (table.isStaticCompactTable())
                return false;

            if (!table.hasStaticColumns() || selectables.isEmpty())
                return false;

            return Selectable.selectColumns(selectables, (column) -> column.isStatic())
                    && !Selectable.selectColumns(selectables, (column) -> !column.isPartitionKey() && !column.isStatic());
        }

        /**
         * Returns the columns in the same order as given list used to order the data.
         * @return the columns in the same order as given list used to order the data.
         */
        private Map<ColumnMetadata, Ordering> getOrderingColumns(List<Ordering> orderings)
        {
            if (orderings.isEmpty())
                return Collections.emptyMap();

            Map<ColumnMetadata, Ordering> orderingColumns = new LinkedHashMap<>();
            for (Ordering ordering : orderings)
            {
                ColumnMetadata column = ordering.expression.getColumn();
                orderingColumns.put(column, ordering);
            }
            return orderingColumns;
        }

        private List<Ordering> getOrderings(TableMetadata table)
        {
            return parameters.orderings.stream()
                    .map(o -> o.bind(table, bindVariables))
                    .collect(Collectors.toList());
        }

        /**
         * Prepares the restrictions.
         *
         * @param metadata the column family meta data
         * @param boundNames the variable specifications
         * @param selectsOnlyStaticColumns {@code true} if the query select only static columns, {@code false} otherwise.
         * @return the restrictions
         * @throws InvalidRequestException if a problem occurs while building the restrictions
         */
        private StatementRestrictions prepareRestrictions(TableMetadata metadata,
                                                          VariableSpecifications boundNames,
                                                          List<Ordering> orderings,
                                                          boolean selectsOnlyStaticColumns,
                                                          boolean forView) throws InvalidRequestException
        {
            return StatementRestrictions.create(StatementType.SELECT,
                                                metadata,
                                                whereClause,
                                                boundNames,
                                                orderings,
                                                selectsOnlyStaticColumns,
                                                parameters.allowFiltering,
                                                forView);
        }

        /** Returns a Term for the limit or null if no limit is set */
        private Term prepareLimit(VariableSpecifications boundNames, Term.Raw limit,
                                  String keyspace, ColumnSpecification limitReceiver) throws InvalidRequestException
        {
            if (limit == null)
                return null;

            Term prepLimit = limit.prepare(keyspace, limitReceiver);
            prepLimit.collectMarkerSpecification(boundNames);
            return prepLimit;
        }

        private static void verifyOrderingIsAllowed(StatementRestrictions restrictions, Map<ColumnMetadata, Ordering> orderingColumns) throws InvalidRequestException
        {
            if (orderingColumns.values().stream().anyMatch(o -> o.expression.hasNonClusteredOrdering()))
                return;
            checkFalse(restrictions.usesSecondaryIndexing(), "ORDER BY with 2ndary indexes is not supported.");
            checkFalse(restrictions.isKeyRange(), "ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");
        }

        private static void validateDistinctSelection(TableMetadata metadata,
                                                      Selection selection,
                                                      StatementRestrictions restrictions)
                                                      throws InvalidRequestException
        {
            checkFalse(restrictions.hasClusteringColumnsRestrictions() ||
                       (restrictions.hasNonPrimaryKeyRestrictions() && !restrictions.nonPKRestrictedColumns(true).stream().allMatch(ColumnMetadata::isStatic)),
                       "SELECT DISTINCT with WHERE clause only supports restriction by partition key and/or static columns.");

            Collection<ColumnMetadata> requestedColumns = selection.getColumns();
            for (ColumnMetadata def : requestedColumns)
                checkFalse(!def.isPartitionKey() && !def.isStatic(),
                           "SELECT DISTINCT queries must only request partition key columns and/or static columns (not %s)",
                           def.name);

            // If it's a key range, we require that all partition key columns are selected so we don't have to bother
            // with post-query grouping.
            if (!restrictions.isKeyRange())
                return;

            for (ColumnMetadata def : metadata.partitionKeyColumns())
                checkTrue(requestedColumns.contains(def),
                          "SELECT DISTINCT queries must request all the partition key columns (missing %s)", def.name);
        }

        /**
         * Creates the <code>AggregationSpecification</code>s used to make the aggregates.
         *
         * @param metadata the table metadata
         * @param selection the selection
         * @param restrictions the restrictions
         * @param isDistinct <code>true</code> if the query is a DISTINCT one.
         * @return the <code>AggregationSpecification</code>s used to make the aggregates
         */
        private AggregationSpecification getAggregationSpecification(TableMetadata metadata,
                                                                     Selection selection,
                                                                     StatementRestrictions restrictions,
                                                                     boolean isDistinct)
        {
            if (parameters.groups.isEmpty())
                return selection.isAggregate() ? AggregationSpecification.AGGREGATE_EVERYTHING
                                               : null;

            int clusteringPrefixSize = 0;

            Iterator<ColumnMetadata> pkColumns = metadata.primaryKeyColumns().iterator();
            for (ColumnIdentifier id : parameters.groups)
            {
                ColumnMetadata def = metadata.getExistingColumn(id);

                checkTrue(def.isPartitionKey() || def.isClusteringColumn(),
                          "Group by is currently only supported on the columns of the PRIMARY KEY, got %s", def.name);

                while (true)
                {
                    checkTrue(pkColumns.hasNext(),
                              "Group by currently only support groups of columns following their declared order in the PRIMARY KEY");

                    ColumnMetadata pkColumn = pkColumns.next();

                    if (pkColumn.isClusteringColumn())
                        clusteringPrefixSize++;

                    // As we do not support grouping on only part of the partition key, we only need to know
                    // which clustering columns need to be used to build the groups
                    if (pkColumn.equals(def))
                        break;

                    checkTrue(restrictions.isColumnRestrictedByEq(pkColumn),
                              "Group by currently only support groups of columns following their declared order in the PRIMARY KEY");
                }
            }

            checkFalse(pkColumns.hasNext() && pkColumns.next().isPartitionKey(),
                       "Group by is not supported on only a part of the partition key");

            checkFalse(clusteringPrefixSize > 0 && isDistinct,
                       "Grouping on clustering columns is not allowed for SELECT DISTINCT queries");

            return AggregationSpecification.aggregatePkPrefix(metadata.comparator, clusteringPrefixSize);
        }

        private ColumnComparator<List<ByteBuffer>> getOrderingComparator(Selection selection,
                                                                         StatementRestrictions restrictions,
                                                                         Map<ColumnMetadata, Ordering> orderingColumns)
                                                                   throws InvalidRequestException
        {
            for (Map.Entry<ColumnMetadata, Ordering> e : orderingColumns.entrySet())
            {
                if (e.getValue().expression.hasNonClusteredOrdering())
                {
                    Preconditions.checkState(orderingColumns.size() == 1);
                    return new IndexColumnComparator<>(e.getValue().expression.toRestriction(), selection.getOrderingIndex(e.getKey()));
                }
            }

            if (!restrictions.keyIsInRelation())
                return null;

            List<Integer> idToSort = new ArrayList<Integer>(orderingColumns.size());
            List<Comparator<ByteBuffer>> sorters = new ArrayList<Comparator<ByteBuffer>>(orderingColumns.size());

            for (ColumnMetadata orderingColumn : orderingColumns.keySet())
            {
                idToSort.add(selection.getOrderingIndex(orderingColumn));
                sorters.add(orderingColumn.type);
            }

            return idToSort.size() == 1 ? new SingleColumnComparator(idToSort.get(0), sorters.get(0))
                    : new CompositeComparator(sorters, idToSort);
        }

        private boolean isReversed(TableMetadata table, Map<ColumnMetadata, Ordering> orderingColumns, StatementRestrictions restrictions) throws InvalidRequestException
        {
            // FIXME exception for ANN until we properly support general ORDER BY
            if (orderingColumns.values().stream().anyMatch(o -> o.expression.hasNonClusteredOrdering()))
                return false;

            Boolean[] reversedMap = new Boolean[table.clusteringColumns().size()];
            int i = 0;
            for (var entry : orderingColumns.entrySet())
            {
                ColumnMetadata def = entry.getKey();
                Ordering ordering = entry.getValue();
                boolean reversed = ordering.direction == Ordering.Direction.DESC;

                // VSTODO move this to verifyOrderingIsAllowed?
                checkTrue(def.isClusteringColumn(),
                          "Order by is currently only supported on the clustered columns of the PRIMARY KEY, got %s", def.name);
                while (i != def.position())
                {
                    checkTrue(restrictions.isColumnRestrictedByEq(table.clusteringColumns().get(i++)),
                              "Order by currently only supports the ordering of columns following their declared order in the PRIMARY KEY");
                }
                i++;
                reversedMap[def.position()] = (reversed != def.isReversedType());
            }

            // Check that all boolean in reversedMap, if set, agrees
            Boolean isReversed = null;
            for (Boolean b : reversedMap)
            {
                // Column on which order is specified can be in any order
                if (b == null)
                    continue;

                if (isReversed == null)
                {
                    isReversed = b;
                    continue;
                }
                checkTrue(isReversed.equals(b), "Unsupported order by relation");
            }
            assert isReversed != null;
            return isReversed;
        }

        /**
         * This verifies that if the expression contains a disjunction - "value = 1 or value = 2" or "value in (1, 2)"
         * the indexes involved in the query support disjunction.
         */
        private void checkDisjunctionIsSupported(TableMetadata table, StatementRestrictions restrictions) throws InvalidRequestException
        {
            if (restrictions.usesSecondaryIndexing())
                if (restrictions.needsDisjunctionSupport(table))
                    restrictions.throwsRequiresIndexSupportingDisjunctionError(table);
        }

        /** If ALLOW FILTERING was not specified, this verifies that it is not needed */
        private void checkNeedsFiltering(TableMetadata table, StatementRestrictions restrictions) throws InvalidRequestException
        {
            // non-key-range non-indexed queries cannot involve filtering underneath
            if (!parameters.allowFiltering && (restrictions.isKeyRange() || restrictions.usesSecondaryIndexing()))
            {
                // We will potentially filter data if the row filter is not the identity and there isn't any index group
                // supporting all the expressions in the filter.
                if (restrictions.needFiltering(table))
                {
                    restrictions.throwRequiresAllowFilteringError(table);
                }
            }
        }

        private ColumnSpecification limitReceiver()
        {
            return new ColumnSpecification(keyspace(), name(), new ColumnIdentifier("[limit]", true), Int32Type.instance);
        }

        private ColumnSpecification perPartitionLimitReceiver()
        {
            return new ColumnSpecification(keyspace(), name(), new ColumnIdentifier("[per_partition_limit]", true), Int32Type.instance);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                              .add("name", qualifiedName)
                              .add("selectClause", selectClause)
                              .add("whereClause", whereClause)
                              .add("isDistinct", parameters.isDistinct)
                              .toString();
        }
    }

    public static class Parameters
    {
        // Public because CASSANDRA-9858
        public final List<Ordering.Raw> orderings;
        public final List<ColumnIdentifier> groups;
        public final boolean isDistinct;
        public final boolean allowFiltering;
        public final boolean isJson;

        public Parameters(List<Ordering.Raw> orderings,
                          List<ColumnIdentifier> groups,
                          boolean isDistinct,
                          boolean allowFiltering,
                          boolean isJson)
        {
            this.orderings = orderings;
            this.groups = groups;
            this.isDistinct = isDistinct;
            this.allowFiltering = allowFiltering;
            this.isJson = isJson;
        }
    }

    private static abstract class ColumnComparator<T> implements Comparator<T>
    {
        protected final int compare(Comparator<ByteBuffer> comparator, ByteBuffer aValue, ByteBuffer bValue)
        {
            if (aValue == null)
                return bValue == null ? 0 : -1;

            return bValue == null ? 1 : comparator.compare(aValue, bValue);
        }

        public ColumnComparator<T> reverse()
        {
            return new ReversedColumnComparator<>(this);
        }

        /**
         * @return true if ordering is performed by index
         */
        public boolean indexOrdering()
        {
            return false;
        }

        /**
         * Produces a prepared {@link ColumnComparator} for current table and query-options
         */
        public Comparator<T> prepareFor(TableMetadata table, QueryOptions options)
        {
            return this;
        }
    }

    private static class ReversedColumnComparator<T> extends ColumnComparator<T>
    {
        private final ColumnComparator<T> wrapped;

        public ReversedColumnComparator(ColumnComparator<T> wrapped)
        {
            this.wrapped = wrapped;
        }

        @Override
        public int compare(T o1, T o2)
        {
            return wrapped.compare(o2, o1);
        }
    }
    /**
     * Used in orderResults(...) method when single 'ORDER BY' condition where given
     */
    private static class SingleColumnComparator extends ColumnComparator<List<ByteBuffer>>
    {
        private final int index;
        private final Comparator<ByteBuffer> comparator;

        public SingleColumnComparator(int columnIndex, Comparator<ByteBuffer> orderer)
        {
            index = columnIndex;
            comparator = orderer;
        }

        public int compare(List<ByteBuffer> a, List<ByteBuffer> b)
        {
            return compare(comparator, a.get(index), b.get(index));
        }
    }

    private static class IndexColumnComparator<T> extends ColumnComparator<List<ByteBuffer>>
    {
        private final SingleRestriction restriction;
        private final int columnIndex;

        // VSTODO maybe cache in prepared statement
        public IndexColumnComparator(SingleRestriction restriction, int columnIndex)
        {
            this.restriction = restriction;
            this.columnIndex = columnIndex;
        }

        @Override
        public boolean indexOrdering()
        {
            return true;
        }

        @Override
        public Comparator<List<ByteBuffer>> prepareFor(TableMetadata table, QueryOptions options)
        {
            Index index = restriction.findSupportingIndex(IndexRegistry.obtain(table));
            assert index != null;
            return index.getPostQueryOrdering(restriction, columnIndex, options);
        }

        @Override
        public int compare(List<ByteBuffer> o1, List<ByteBuffer> o2)
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Used in orderResults(...) method when multiple 'ORDER BY' conditions where given
     */
    private static class CompositeComparator extends ColumnComparator<List<ByteBuffer>>
    {
        private final List<Comparator<ByteBuffer>> orderTypes;
        private final List<Integer> positions;

        private CompositeComparator(List<Comparator<ByteBuffer>> orderTypes, List<Integer> positions)
        {
            this.orderTypes = orderTypes;
            this.positions = positions;
        }

        public int compare(List<ByteBuffer> a, List<ByteBuffer> b)
        {
            for (int i = 0; i < positions.size(); i++)
            {
                Comparator<ByteBuffer> type = orderTypes.get(i);
                int columnPos = positions.get(i);

                int comparison = compare(type, a.get(columnPos), b.get(columnPos));

                if (comparison != 0)
                    return comparison;
            }

            return 0;
        }
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
