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
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.restrictions.SingleRestriction;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.ResultSetBuilder;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.selection.Selectable.WithFunction;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.selection.Selection.Selectors;
import org.apache.cassandra.cql3.selection.Selector;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.metrics.ClientRequestSizeMetrics;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.AggregationQueryPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.utils.ByteBufferUtil.UNSET_BYTE_BUFFER;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 * A number of public methods here are only used internally. However,
 * many of these are made accessible for the benefit of custom
 * QueryHandler implementations, so before reducing their accessibility
 * due consideration should be given.
 *
 * Note that select statements can be accessed by multiple threads, so we cannot rely on mutable attributes.
 */
@ThreadSafe
public class SelectStatement implements CQLStatement.SingleKeyspaceCqlStatement
{
    private static final Logger logger = LoggerFactory.getLogger(SelectStatement.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(SelectStatement.logger, 1, TimeUnit.MINUTES);

    public static final int DEFAULT_PAGE_SIZE = 10000;
    public static final String TOPK_CONSISTENCY_LEVEL_ERROR = "Top-K queries can only be run with consistency level ONE/LOCAL_ONE. Consistency level %s was used.";
    public static final String TOPK_LIMIT_ERROR = "Top-K queries must have a limit specified and the limit must be less than the query page size";
    public static final String TOPK_PARTITION_LIMIT_ERROR = "Top-K queries do not support per-partition limits";
    public static final String TOPK_AGGREGATION_ERROR = "Top-K queries can not be run with aggregation";
    public static final String TOPK_CONSISTENCY_LEVEL_WARNING = "Top-K queries can only be run with consistency level ONE " +
                                                                "/ LOCAL_ONE / NODE_LOCAL. Consistency level %s was requested. " +
                                                                "Downgrading the consistency level to %s.";
    public static final String TOPK_PAGE_SIZE_WARNING = "Top-K queries do not support paging and the page size is set to %d, " +
                                                        "which is less than LIMIT %d. The page size has been set to %<d to match the LIMIT.";

    public final VariableSpecifications bindVariables;
    public final TableMetadata table;
    public final Parameters parameters;
    private final Selection selection;
    private final Term limit;
    private final Term perPartitionLimit;

    private final StatementRestrictions restrictions;

    private final boolean isReversed;

    /**
     * The {@code Factory} used to create the {@code AggregationSpecification}.
     */
    private final AggregationSpecification.Factory aggregationSpecFactory;

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

    public SelectStatement(TableMetadata table,
                           VariableSpecifications bindVariables,
                           Parameters parameters,
                           Selection selection,
                           StatementRestrictions restrictions,
                           boolean isReversed,
                           AggregationSpecification.Factory aggregationSpecFactory,
                           ColumnComparator<List<ByteBuffer>> orderingComparator,
                           Term limit,
                           Term perPartitionLimit)
    {
        this.table = table;
        this.bindVariables = bindVariables;
        this.selection = selection;
        this.restrictions = restrictions;
        this.isReversed = isReversed;
        this.aggregationSpecFactory = aggregationSpecFactory;
        this.orderingComparator = orderingComparator;
        this.parameters = parameters;
        this.limit = limit;
        this.perPartitionLimit = perPartitionLimit;
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

        if (aggregationSpecFactory != null)
            aggregationSpecFactory.addFunctionsTo(functions);

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
        return new SelectStatement(table,
                                   VariableSpecifications.empty(),
                                   defaultParameters,
                                   selection,
                                   StatementRestrictions.empty(StatementType.SELECT, table),
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
            TableMetadataRef baseTable = View.findBaseTable(keyspace(), table());
            if (baseTable != null)
                state.ensureTablePermission(baseTable, Permission.SELECT);
        }
        else
        {
            state.ensureTablePermission(table, Permission.SELECT);
        }

        for (Function function : getFunctions())
            state.ensurePermission(Permission.EXECUTE, function);

        if (!state.hasTablePermission(table, Permission.UNMASK) &&
            !state.hasTablePermission(table, Permission.SELECT_MASKED))
        {
            List<ColumnMetadata> queriedMaskedColumns = table.columns()
                                                             .stream()
                                                             .filter(ColumnMetadata::isMasked)
                                                             .filter(restrictions::isRestricted)
                                                             .collect(Collectors.toList());

            if (!queriedMaskedColumns.isEmpty())
                throw new UnauthorizedException(format("User %s has no UNMASK nor SELECT_MASKED permission on table %s.%s, " +
                                                       "cannot query masked columns %s",
                                                       state.getUser().getName(), keyspace(), table(), queriedMaskedColumns));
        }
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (parameters.allowFiltering && !SchemaConstants.isSystemKeyspace(table.keyspace))
            Guardrails.allowFilteringEnabled.ensureEnabled(state);
    }

    public ResultMessage.Rows execute(QueryState state, QueryOptions options, long queryStartNanoTime)
    {
        ConsistencyLevel cl = options.getConsistency();
        checkNotNull(cl, "Invalid empty consistency level");

        cl.validateForRead();
        Guardrails.readConsistencyLevels.guard(EnumSet.of(cl), state.getClientState());

        long nowInSec = options.getNowInSeconds(state);
        int userLimit = getLimit(options);
        int userPerPartitionLimit = getPerPartitionLimit(options);
        int pageSize = options.getPageSize();
        boolean unmask = !table.hasMaskedColumns() || state.getClientState().hasTablePermission(table, Permission.UNMASK);

        Selectors selectors = selection.newSelectors(options);
        AggregationSpecification aggregationSpec = getAggregationSpec(options);
        DataLimits limit = getDataLimits(userLimit, userPerPartitionLimit, pageSize, aggregationSpec);

        // Handle additional validation for topK queries
        if (restrictions.isTopK())
        {
            checkFalse(aggregationSpec != null, TOPK_AGGREGATION_ERROR);

            // We aren't going to allow SERIAL at all, so we can error out on those.
            checkFalse(options.getConsistency() == ConsistencyLevel.LOCAL_SERIAL ||
                       options.getConsistency() == ConsistencyLevel.SERIAL,
                       String.format(TOPK_CONSISTENCY_LEVEL_ERROR, options.getConsistency()));

            if (options.getConsistency() != ConsistencyLevel.ONE &&
                options.getConsistency() != ConsistencyLevel.LOCAL_ONE &&
                options.getConsistency() != ConsistencyLevel.NODE_LOCAL)
            {
                ConsistencyLevel supplied = options.getConsistency();
                ConsistencyLevel downgrade = supplied.isDatacenterLocal() ? ConsistencyLevel.LOCAL_ONE : ConsistencyLevel.ONE;

                options = QueryOptions.withConsistencyLevel(options, downgrade);

                ClientWarn.instance.warn(String.format(TOPK_CONSISTENCY_LEVEL_WARNING, supplied, downgrade));
            }

            checkFalse(limit.isUnlimited(), TOPK_LIMIT_ERROR);

            checkFalse(limit.perPartitionCount() != DataLimits.NO_LIMIT, TOPK_PARTITION_LIMIT_ERROR);

            if (pageSize > 0 && pageSize < limit.count())
            {
                int oldPageSize = pageSize;
                pageSize = limit.count();
                limit = getDataLimits(userLimit, userPerPartitionLimit, pageSize, aggregationSpec);
                options = QueryOptions.withPageSize(options, pageSize);
                ClientWarn.instance.warn(String.format(TOPK_PAGE_SIZE_WARNING, oldPageSize, limit.count()));
            }
        }

        ReadQuery query = getQuery(options, state.getClientState(), selectors.getColumnFilter(), nowInSec, limit);

        if (options.isReadThresholdsEnabled())
            query.trackWarnings();
        ResultMessage.Rows rows;

        if (aggregationSpec == null && (pageSize <= 0 || (query.limits().count() <= pageSize) || query.isTopK()))
        {
            rows = execute(query, options, state.getClientState(), selectors, nowInSec, userLimit, null, queryStartNanoTime, unmask);
        }
        else
        {
            QueryPager pager = getPager(query, options);

            rows = execute(state,
                           Pager.forDistributedQuery(pager, cl, state.getClientState()),
                           options,
                           selectors,
                           pageSize,
                           nowInSec,
                           userLimit,
                           aggregationSpec,
                           queryStartNanoTime,
                           unmask);
        }
        if (!SchemaConstants.isSystemKeyspace(table.keyspace))
            ClientRequestSizeMetrics.recordReadResponseMetrics(rows, restrictions, selection);

        return rows;
    }


    public AggregationSpecification getAggregationSpec(QueryOptions options)
    {
        return aggregationSpecFactory == null ? null : aggregationSpecFactory.newInstance(options);
    }

    public ReadQuery getQuery(QueryOptions options, long nowInSec) throws RequestValidationException
    {
        Selectors selectors = selection.newSelectors(options);
        return getQuery(options,
                        ClientState.forInternalCalls(),
                        selectors.getColumnFilter(),
                        nowInSec,
                        getLimit(options),
                        getPerPartitionLimit(options),
                        options.getPageSize(),
                        getAggregationSpec(options));
    }

    public ReadQuery getQuery(QueryOptions options,
                              ClientState state,
                              ColumnFilter columnFilter,
                              long nowInSec,
                              int userLimit,
                              int perPartitionLimit,
                              int pageSize,
                              AggregationSpecification aggregationSpec)
    {
        DataLimits limit = getDataLimits(userLimit, perPartitionLimit, pageSize, aggregationSpec);

        return getQuery(options, state, columnFilter, nowInSec, limit);
    }

    public ReadQuery getQuery(QueryOptions options,
                              ClientState state,
                              ColumnFilter columnFilter,
                              long nowInSec,
                              DataLimits limit)
    {
        boolean isPartitionRangeQuery = restrictions.isKeyRange() || restrictions.usesSecondaryIndexing();

        if (isPartitionRangeQuery)
            return getRangeCommand(options, state, columnFilter, limit, nowInSec);

        return getSliceCommands(options, state, columnFilter, limit, nowInSec);
    }

    private ResultMessage.Rows execute(ReadQuery query,
                                       QueryOptions options,
                                       ClientState state,
                                       Selectors selectors,
                                       long nowInSec,
                                       int userLimit,
                                       AggregationSpecification aggregationSpec,
                                       long queryStartNanoTime,
                                       boolean unmask)
    {
        try (PartitionIterator data = query.execute(options.getConsistency(), state, queryStartNanoTime))
        {
            return processResults(data, options, selectors, nowInSec, userLimit, aggregationSpec, unmask);
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

        public static Pager forDistributedQuery(QueryPager pager, ConsistencyLevel consistency, ClientState clientState)
        {
            return new NormalPager(pager, consistency, clientState);
        }

        public boolean isExhausted()
        {
            return pager.isExhausted();
        }

        public PagingState state()
        {
            return pager.state();
        }

        public abstract PartitionIterator fetchPage(int pageSize, long queryStartNanoTime);

        public static class NormalPager extends Pager
        {
            private final ConsistencyLevel consistency;
            private final ClientState clientState;

            private NormalPager(QueryPager pager, ConsistencyLevel consistency, ClientState clientState)
            {
                super(pager);
                this.consistency = consistency;
                this.clientState = clientState;
            }

            public PartitionIterator fetchPage(int pageSize, long queryStartNanoTime)
            {
                return pager.fetchPage(pageSize, consistency, clientState, queryStartNanoTime);
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

            public PartitionIterator fetchPage(int pageSize, long queryStartNanoTime)
            {
                return pager.fetchPageInternal(pageSize, executionController);
            }
        }
    }

    private ResultMessage.Rows execute(QueryState state,
                                       Pager pager,
                                       QueryOptions options,
                                       Selectors selectors,
                                       int pageSize,
                                       long nowInSec,
                                       int userLimit,
                                       AggregationSpecification aggregationSpec,
                                       long queryStartNanoTime,
                                       boolean unmask)
    {
        Guardrails.pageSize.guard(pageSize, table(), false, state.getClientState());

        if (aggregationSpecFactory != null)
        {
            if (!restrictions.hasPartitionKeyRestrictions())
            {
                warn("Aggregation query used without partition key");
                noSpamLogger.warn(String.format("Aggregation query used without partition key on table %s.%s, aggregation type: %s",
                                                 keyspace(), table(), aggregationSpec.kind()));
            }
            else if (restrictions.keyIsInRelation())
            {
                warn("Aggregation query used on multiple partition keys (IN restriction)");
                noSpamLogger.warn(String.format("Aggregation query used on multiple partition keys (IN restriction) on table %s.%s, aggregation type: %s",
                                                 keyspace(), table(), aggregationSpec.kind()));
            }
        }

        // We can't properly do post-query ordering if we page (see #6722)
        // For GROUP BY or aggregation queries we always page internally even if the user has turned paging off
        checkFalse(pageSize > 0 && needsPostQueryOrdering(),
                  "Cannot page queries with both ORDER BY and a IN restriction on the partition key;"
                  + " you must either remove the ORDER BY or the IN and sort client side, or disable paging for this query");

        ResultMessage.Rows msg;
        try (PartitionIterator page = pager.fetchPage(pageSize, queryStartNanoTime))
        {
            msg = processResults(page, options, selectors, nowInSec, userLimit, aggregationSpec, unmask);
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
                                              long nowInSec,
                                              int userLimit,
                                              AggregationSpecification aggregationSpec,
                                              boolean unmask) throws RequestValidationException
    {
        ResultSet rset = process(partitions, options, selectors, nowInSec, userLimit, aggregationSpec, unmask);
        return new ResultMessage.Rows(rset);
    }

    public ResultMessage.Rows executeLocally(QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        return executeInternal(state, options, options.getNowInSeconds(state), nanoTime());
    }

    public ResultMessage.Rows executeInternal(QueryState state,
                                              QueryOptions options,
                                              long nowInSec,
                                              long queryStartNanoTime)
    {
        int userLimit = getLimit(options);
        int userPerPartitionLimit = getPerPartitionLimit(options);
        int pageSize = options.getPageSize();
        boolean unmask = state.getClientState().hasTablePermission(table, Permission.UNMASK);

        Selectors selectors = selection.newSelectors(options);
        AggregationSpecification aggregationSpec = getAggregationSpec(options);
        ReadQuery query = getQuery(options,
                                   state.getClientState(),
                                   selectors.getColumnFilter(),
                                   nowInSec,
                                   userLimit,
                                   userPerPartitionLimit,
                                   pageSize,
                                   aggregationSpec);

        try (ReadExecutionController executionController = query.executionController())
        {
            if (aggregationSpec == null && (pageSize <= 0 || (query.limits().count() <= pageSize) || query.isTopK()))
            {
                try (PartitionIterator data = query.executeInternal(executionController))
                {
                    return processResults(data, options, selectors, nowInSec, userLimit, null, unmask);
                }
            }

            QueryPager pager = getPager(query, options);

            return execute(state,
                           Pager.forInternalQuery(pager, executionController),
                           options,
                           selectors,
                           pageSize,
                           nowInSec,
                           userLimit,
                           aggregationSpec,
                           queryStartNanoTime,
                           unmask);
        }
    }

    private QueryPager getPager(ReadQuery query, QueryOptions options)
    {
        QueryPager pager = query.getPager(options.getPagingState(), options.getProtocolVersion());

        if (aggregationSpecFactory == null || query.isEmpty())
            return pager;

        return new AggregationQueryPager(pager, query.limits());
    }

    public Map<DecoratedKey, List<Row>> executeRawInternal(QueryOptions options, ClientState state, long nowInSec) throws RequestExecutionException, RequestValidationException
    {
        int userLimit = getLimit(options);
        int userPerPartitionLimit = getPerPartitionLimit(options);
        if (options.getPageSize() > 0)
            throw new IllegalStateException();
        if (aggregationSpecFactory != null)
            throw new IllegalStateException();

        Selectors selectors = selection.newSelectors(options);
        ReadQuery query = getQuery(options, state, selectors.getColumnFilter(), nowInSec, userLimit, userPerPartitionLimit, Integer.MAX_VALUE, null);

        Map<DecoratedKey, List<Row>> result = Collections.emptyMap();
        try (ReadExecutionController executionController = query.executionController())
        {
            try (PartitionIterator data = query.executeInternal(executionController))
            {
                while (data.hasNext())
                {
                    try (RowIterator in = data.next())
                    {
                        List<Row> out = Collections.emptyList();
                        while (in.hasNext())
                        {
                            switch (out.size())
                            {
                                case 0:  out = Collections.singletonList(in.next()); break;
                                case 1:  out = new ArrayList<>(out);
                                default: out.add(in.next());
                            }
                        }
                        switch (result.size())
                        {
                            case 0:  result = Collections.singletonMap(in.partitionKey(), out); break;
                            case 1:  result = new TreeMap<>(result);
                            default: result.put(in.partitionKey(), out);
                        }
                    }
                }
                return result;
            }
        }
    }

    public ResultSet process(PartitionIterator partitions, long nowInSec, boolean unmask) throws InvalidRequestException
    {
        QueryOptions options = QueryOptions.DEFAULT;
        Selectors selectors = selection.newSelectors(options);
        return process(partitions, options, selectors, nowInSec, getLimit(options), getAggregationSpec(options), unmask);
    }

    @Override
    public String keyspace()
    {
        return table.keyspace;
    }

    public String table()
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

    private ReadQuery getSliceCommands(QueryOptions options, ClientState state, ColumnFilter columnFilter,
                                       DataLimits limit, long nowInSec)
    {
        Collection<ByteBuffer> keys = restrictions.getPartitionKeys(options, state);
        if (keys.isEmpty())
            return ReadQuery.empty(table);

        if (restrictions.keyIsInRelation())
        {
            Guardrails.partitionKeysInSelect.guard(keys.size(), table.name, false, state);
        }

        ClusteringIndexFilter filter = makeClusteringIndexFilter(options, state, columnFilter);
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
        QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
        ClientState state = ClientState.forInternalCalls();
        ColumnFilter columnFilter = selection.newSelectors(options).getColumnFilter();
        ClusteringIndexFilter filter = makeClusteringIndexFilter(options, state, columnFilter);
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
    public SinglePartitionReadCommand internalReadForView(DecoratedKey key, long nowInSec)
    {
        QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
        ClientState state = ClientState.forInternalCalls();
        ColumnFilter columnFilter = selection.newSelectors(options).getColumnFilter();
        ClusteringIndexFilter filter = makeClusteringIndexFilter(options, state, columnFilter);
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

    private ReadQuery getRangeCommand(QueryOptions options, ClientState state, ColumnFilter columnFilter, DataLimits limit, long nowInSec)
    {
        ClusteringIndexFilter clusteringIndexFilter = makeClusteringIndexFilter(options, state, columnFilter);
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

    private ClusteringIndexFilter makeClusteringIndexFilter(QueryOptions options, ClientState state, ColumnFilter columnFilter)
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

        if (restrictions.isColumnRange())
        {
            Slices slices = makeSlices(options);
            if (slices == Slices.NONE && !selection.containsStaticColumns())
                return null;

            return new ClusteringIndexSliceFilter(slices, isReversed);
        }

        NavigableSet<Clustering<?>> clusterings = getRequestedRows(options, state);
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

    private DataLimits getDataLimits(int userLimit,
                                     int perPartitionLimit,
                                     int pageSize,
                                     AggregationSpecification aggregationSpec)
    {
        int cqlRowLimit = DataLimits.NO_LIMIT;
        int cqlPerPartitionLimit = DataLimits.NO_LIMIT;

        // If we do post ordering we need to get all the results sorted before we can trim them.
        if (aggregationSpec != AggregationSpecification.AGGREGATE_EVERYTHING)
        {
            // If we aren't need post-query ordering but we are doing index ordering (currently ANN only) then
            // we do need to use the user limit.
            if (!needsPostQueryOrdering() || needIndexOrdering())
                cqlRowLimit = userLimit;
            cqlPerPartitionLimit = perPartitionLimit;
        }

        // Group by and aggregation queries will always be paged internally to avoid OOM.
        // If the user provided a pageSize we'll use that to page internally (because why not), otherwise we use our default
        if (pageSize <= 0)
            pageSize = DEFAULT_PAGE_SIZE;

        // Aggregation queries work fine on top of the group by paging but to maintain
        // backward compatibility we need to use the old way.
        if (aggregationSpec != null && aggregationSpec != AggregationSpecification.AGGREGATE_EVERYTHING)
        {
            if (parameters.isDistinct)
                return DataLimits.distinctLimits(cqlRowLimit);

            return DataLimits.groupByLimits(cqlRowLimit,
                                            cqlPerPartitionLimit,
                                            pageSize,
                                            aggregationSpec);
        }

        if (parameters.isDistinct)
            return cqlRowLimit == DataLimits.NO_LIMIT ? DataLimits.DISTINCT_NONE : DataLimits.distinctLimits(cqlRowLimit);

        return DataLimits.cqlLimits(cqlRowLimit, cqlPerPartitionLimit);
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
        int userLimit = DataLimits.NO_LIMIT;

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

    private NavigableSet<Clustering<?>> getRequestedRows(QueryOptions options, ClientState state) throws InvalidRequestException
    {
        // Note: getRequestedColumns don't handle static columns, but due to CASSANDRA-5762
        // we always do a slice for CQL3 tables, so it's ok to ignore them here
        assert !restrictions.isColumnRange();
        return restrictions.getClusteringColumns(options, state);
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
                              long nowInSec,
                              int userLimit,
                              AggregationSpecification aggregationSpec,
                              boolean unmask) throws InvalidRequestException
    {
        GroupMaker groupMaker = aggregationSpec == null ? null : aggregationSpec.newGroupMaker();
        ResultSetBuilder result = new ResultSetBuilder(getResultMetadata(), selectors, unmask, groupMaker);

        while (partitions.hasNext())
        {
            try (RowIterator partition = partitions.next())
            {
                processPartition(partition, options, result, nowInSec);
            }
        }

        ResultSet cqlRows = result.build();
        maybeWarn(result, options);

        orderResults(cqlRows, options);

        cqlRows.trim(userLimit);

        return cqlRows;
    }

    public static ByteBuffer[] getComponents(TableMetadata metadata, DecoratedKey dk)
    {
        ByteBuffer key = dk.getKey();
        if (metadata.partitionKeyColumns().size() == 1)
            return new ByteBuffer[]{ key };
        if (metadata.partitionKeyType instanceof CompositeType)
        {
            return ((CompositeType)metadata.partitionKeyType).split(key);
        }
        else
        {
            return new ByteBuffer[]{ key };
        }
    }

    private void maybeWarn(ResultSetBuilder result, QueryOptions options)
    {
        if (!options.isReadThresholdsEnabled())
            return;
        ColumnFamilyStore store = cfs();
        if (store != null)
            store.metric.coordinatorReadSize.update(result.getSize());
        if (result.shouldWarn(options.getCoordinatorReadSizeWarnThresholdBytes()))
        {
            String msg = String.format("Read on table %s has exceeded the size warning threshold of %,d bytes", table, options.getCoordinatorReadSizeWarnThresholdBytes());
            ClientState state = ClientState.forInternalCalls();
            ClientWarn.instance.warn(msg + " with " + loggableTokens(options, state));
            logger.warn("{} with query {}", msg, asCQL(options, state));
            if (store != null)
                store.metric.coordinatorReadSizeWarnings.mark();
        }
    }

    private void maybeFail(ResultSetBuilder result, QueryOptions options)
    {
        if (!options.isReadThresholdsEnabled())
            return;
        if (result.shouldReject(options.getCoordinatorReadSizeAbortThresholdBytes()))
        {
            String msg = String.format("Read on table %s has exceeded the size failure threshold of %,d bytes", table, options.getCoordinatorReadSizeAbortThresholdBytes());
            ClientState state = ClientState.forInternalCalls();
            String clientMsg = msg + " with " + loggableTokens(options, state);
            ClientWarn.instance.warn(clientMsg);
            logger.warn("{} with query {}", msg, asCQL(options, state));
            ColumnFamilyStore store = cfs();
            if (store != null)
            {
                store.metric.coordinatorReadSizeAborts.mark();
                store.metric.coordinatorReadSize.update(result.getSize());
            }
            // read errors require blockFor and recieved (its in the protocol message), but this isn't known;
            // to work around this, treat the coordinator as the only response we care about and mark it failed
            ReadSizeAbortException exception = new ReadSizeAbortException(clientMsg, options.getConsistency(), 0, 1, true,
                                                                          ImmutableMap.of(FBUtilities.getBroadcastAddressAndPort(), RequestFailureReason.READ_SIZE));
            StorageProxy.recordReadRegularAbort(options.getConsistency(), exception);
            throw exception;
        }
    }

    private ColumnFamilyStore cfs()
    {
        return Schema.instance.getColumnFamilyStoreInstance(table.id);
    }

    // Used by ModificationStatement for CAS operations
    public void processPartition(RowIterator partition, QueryOptions options, ResultSetBuilder result, long nowInSec)
    throws InvalidRequestException
    {
        maybeFail(result, options);
        ProtocolVersion protocolVersion = options.getProtocolVersion();

        ByteBuffer[] keyComponents = getComponents(table, partition.partitionKey());

        Row staticRow = partition.staticRow();
        // If there is no rows, we include the static content if we should and we're done.
        if (!partition.hasNext())
        {
            if (!staticRow.isEmpty() && restrictions.returnStaticContentOnPartitionWithNoRows())
            {
                result.newRow(protocolVersion, partition.partitionKey(), staticRow.clustering(), selection.getColumns());
                maybeFail(result, options);
                for (ColumnMetadata def : selection.getColumns())
                {
                    switch (def.kind)
                    {
                        case PARTITION_KEY:
                            result.add(keyComponents[def.position()]);
                            break;
                        case STATIC:
                            result.add(partition.staticRow().getColumnData(def), nowInSec);
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
            result.newRow(protocolVersion, partition.partitionKey(), row.clustering(), selection.getColumns());

            // reads aren't failed as soon the size exceeds the failure threshold, they're failed once the failure
            // threshold has been exceeded and we start adding more data. We're slightly more permissive to avoid
            // cases where a row can never be read. Since we only warn/fail after entire rows are read, this will
            // still allow the entire dataset to be read with LIMIT 1 queries, even if every row is oversized
            maybeFail(result, options);

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
                        result.add(row.getColumnData(def), nowInSec);
                        break;
                    case STATIC:
                        result.add(staticRow.getColumnData(def), nowInSec);
                        break;
                }
            }
        }
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
     * Orders results when multiple keys are selected (using IN).
     * <p>
     * In the case of ANN ordering the rows are first ordered in index column order and then by primary key.
     */
    private void orderResults(ResultSet cqlRows, QueryOptions options)
    {
        if (cqlRows.size() == 0 || !needsPostQueryOrdering())
            return;

        Comparator<List<ByteBuffer>> comparator = orderingComparator.prepareFor(table, getRowFilter(options), options);
        if (comparator != null)
            cqlRows.rows.sort(comparator);
    }

    public static class RawStatement extends QualifiedStatement
    {
        public final Parameters parameters;
        public final List<RawSelector> selectClause;
        public final WhereClause whereClause;
        public final Term.Raw limit;
        public final Term.Raw perPartitionLimit;
        private ClientState state;

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

        public SelectStatement prepare(ClientState state)
        {
            // Cache locally for use by Guardrails
            this.state = state;
            return prepare(state, false);
        }

        public SelectStatement prepare(ClientState state, boolean forView) throws InvalidRequestException
        {
            TableMetadata table = Schema.instance.validateTable(keyspace(), name());

            List<Selectable> selectables = RawSelector.toSelectables(selectClause, table);
            boolean containsOnlyStaticColumns = selectOnlyStaticColumns(table, selectables);

            List<Ordering> orderings = getOrderings(table);
            StatementRestrictions restrictions = prepareRestrictions(state, table, bindVariables, orderings, containsOnlyStaticColumns, forView);

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

            AggregationSpecification.Factory aggregationSpecFactory = getAggregationSpecFactory(table,
                                                                                                bindVariables,
                                                                                                selection,
                                                                                                restrictions,
                                                                                                parameters.isDistinct);

            checkFalse(aggregationSpecFactory == AggregationSpecification.AGGREGATE_EVERYTHING_FACTORY
                       && perPartitionLimit != null,
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

            checkNeedsFiltering(table, restrictions);

            return new SelectStatement(table,
                                       bindVariables,
                                       parameters,
                                       selection,
                                       restrictions,
                                       isReversed,
                                       aggregationSpecFactory,
                                       orderingComparator,
                                       prepareLimit(bindVariables, limit, keyspace(), limitReceiver()),
                                       prepareLimit(bindVariables, perPartitionLimit, keyspace(), perPartitionLimitReceiver()));
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

            if (hasGroupBy)
                Guardrails.groupByEnabled.ensureEnabled(state);

            boolean isJson = parameters.isJson;
            boolean returnStaticContentOnPartitionWithNoRows = restrictions.returnStaticContentOnPartitionWithNoRows();

            if (selectables.isEmpty()) // wildcard query
            {
                return hasGroupBy || table.hasMaskedColumns()
                       ? Selection.wildcardWithGroupByOrMaskedColumns(table, boundNames, resultSetOrderingColumns, isJson, returnStaticContentOnPartitionWithNoRows)
                       : Selection.wildcard(table, isJson, returnStaticContentOnPartitionWithNoRows);
            }

            return Selection.fromSelectors(table,
                                           selectables,
                                           boundNames,
                                           resultSetOrderingColumns,
                                           restrictions.nonPKRestrictedColumns(false),
                                           hasGroupBy,
                                           isJson,
                                           returnStaticContentOnPartitionWithNoRows);
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
        private StatementRestrictions prepareRestrictions(ClientState state,
                                                          TableMetadata metadata,
                                                          VariableSpecifications boundNames,
                                                          List<Ordering> orderings,
                                                          boolean selectsOnlyStaticColumns,
                                                          boolean forView) throws InvalidRequestException
        {
            return new StatementRestrictions(state,
                                             StatementType.SELECT,
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
            checkFalse(restrictions.usesSecondaryIndexing(), "ORDER BY with 2ndary indexes is not supported, except for ANN queries.");
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
         * Creates the {@code AggregationSpecification.Factory} used to make the aggregates.
         *
         * @param metadata the table metadata
         * @param selection the selection
         * @param restrictions the restrictions
         * @param isDistinct <code>true</code> if the query is a DISTINCT one. 
         * @return the {@code AggregationSpecification.Factory} used to make the aggregates
         */
        private AggregationSpecification.Factory getAggregationSpecFactory(TableMetadata metadata,
                                                                           VariableSpecifications boundNames,
                                                                           Selection selection,
                                                                           StatementRestrictions restrictions,
                                                                           boolean isDistinct)
        {
            if (parameters.groups.isEmpty())
                return selection.isAggregate() ? AggregationSpecification.AGGREGATE_EVERYTHING_FACTORY
                                               : null;

            int clusteringPrefixSize = 0;

            Iterator<ColumnMetadata> pkColumns = metadata.primaryKeyColumns().iterator();
            List<ColumnMetadata> columns = null;
            Selector.Factory selectorFactory = null;
            for (Selectable.Raw raw : parameters.groups)
            {
                Selectable selectable = raw.prepare(metadata);
                ColumnMetadata def = null;

                // For GROUP BY we only allow column names or functions at the higher level.
                if (selectable instanceof WithFunction)
                {
                    WithFunction withFunction = (WithFunction) selectable;
                    validateGroupByFunction(withFunction);
                    columns = new ArrayList<ColumnMetadata>();
                    selectorFactory = selectable.newSelectorFactory(metadata, null, columns, boundNames);
                    checkFalse(columns.isEmpty(), "GROUP BY functions must have one clustering column name as parameter");
                    if (columns.size() > 1)
                        throw invalidRequest("GROUP BY functions accept only one clustering column as parameter, got: %s",
                                             columns.stream().map(c -> c.name.toCQLString()).collect(Collectors.joining(",")));

                    def = columns.get(0);
                    checkTrue(def.isClusteringColumn(),
                              "Group by functions are only supported on clustering columns, got %s", def.name);
                }
                else
                {
                    def = (ColumnMetadata) selectable;
                    checkTrue(def.isPartitionKey() || def.isClusteringColumn(),
                              "Group by is currently only supported on the columns of the PRIMARY KEY, got %s", def.name);
                    checkNull(selectorFactory, "Functions are only supported on the last element of the GROUP BY clause");
                }

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

            return selectorFactory == null ? AggregationSpecification.aggregatePkPrefixFactory(metadata.comparator, clusteringPrefixSize)
                                           : AggregationSpecification.aggregatePkPrefixFactoryWithSelector(metadata.comparator,
                                                                                                           clusteringPrefixSize,
                                                                                                           selectorFactory,
                                                                                                           columns);
        }

        /**
         * Checks that the function used is a valid one for the GROUP BY clause.
         *
         * @param withFunction the {@code Selectable} from which the function must be retrieved.
         * @return the monotonic scalar function that must be used for determining the groups.
         */
        private void validateGroupByFunction(WithFunction withFunction)
        {
            Function f = withFunction.function;
            checkFalse(f.isAggregate(), "Aggregate functions are not supported within the GROUP BY clause, got: %s", f.name());
        }

        private ColumnComparator<List<ByteBuffer>> getOrderingComparator(Selection selection,
                                                                         StatementRestrictions restrictions,
                                                                         Map<ColumnMetadata, Ordering> orderingColumns) throws InvalidRequestException
        {
            for (Map.Entry<ColumnMetadata, Ordering> e : orderingColumns.entrySet())
            {
                if (e.getValue().expression.hasNonClusteredOrdering())
                {
                    Preconditions.checkState(orderingColumns.size() == 1);
                    return new IndexColumnComparator(e.getValue().expression.toRestriction(), selection.getOrderingIndex(e.getKey()));
                }
            }

            if (!restrictions.keyIsInRelation())
                return null;

            List<Integer> idToSort = new ArrayList<>(orderingColumns.size());
            List<Comparator<ByteBuffer>> sorters = new ArrayList<>(orderingColumns.size());

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
            if (orderingColumns.values().stream().anyMatch(o -> o.expression.hasNonClusteredOrdering()))
                return false;
            Boolean[] reversedMap = new Boolean[table.clusteringColumns().size()];
            int i = 0;
            for (var entry : orderingColumns.entrySet())
            {
                ColumnMetadata def = entry.getKey();
                Ordering ordering = entry.getValue();
                boolean reversed = ordering.direction == Ordering.Direction.DESC;

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

        /** If ALLOW FILTERING was not specified, this verifies that it is not needed */
        private void checkNeedsFiltering(TableMetadata table, StatementRestrictions restrictions) throws InvalidRequestException
        {
            // non-key-range non-indexed queries cannot involve filtering underneath
            if (!parameters.allowFiltering && (restrictions.isKeyRange() || restrictions.usesSecondaryIndexing()))
            {
                // We will potentially filter data if the row filter is not the identity and there isn't any index group
                // supporting all the expressions in the filter.
                if (restrictions.requiresAllowFilteringIfNotSpecified())
                    checkFalse(restrictions.needFiltering(table), StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
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
        public final List<Selectable.Raw> groups;
        public final boolean isDistinct;
        public final boolean allowFiltering;
        public final boolean isJson;

        public Parameters(List<Ordering.Raw> orderings,
                          List<Selectable.Raw> groups,
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
        public Comparator<T> prepareFor(TableMetadata table, RowFilter rowFilter, QueryOptions options)
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

    private static class IndexColumnComparator extends ColumnComparator<List<ByteBuffer>>
    {
        private final SingleRestriction restriction;
        private final int columnIndex;

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
        public Comparator<List<ByteBuffer>> prepareFor(TableMetadata table, RowFilter rowFilter, QueryOptions options)
        {
            if (table.indexes.isEmpty() || rowFilter.isEmpty())
                return this;

            Index.QueryPlan indexQueryPlan = Keyspace.openAndGetStore(table).indexManager.getBestIndexQueryPlanFor(rowFilter);

            Index index = restriction.findSupportingIndexFromQueryPlan(indexQueryPlan);
            assert index != null;
            Comparator<ByteBuffer> comparator = index.getPostQueryOrdering(restriction, options);
            return (a, b) -> compare(comparator, a.get(columnIndex), b.get(columnIndex));
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

    private String loggableTokens(QueryOptions options, ClientState state)
    {
        if (restrictions.isKeyRange() || restrictions.usesSecondaryIndexing())
        {
            AbstractBounds<PartitionPosition> bounds = restrictions.getPartitionKeyBounds(options);
            return "token range: " + (bounds.inclusiveLeft() ? '[' : '(') +
                   bounds.left.getToken().toString() + ", " +
                   bounds.right.getToken().toString() +
                   (bounds.inclusiveRight() ? ']' : ')');
        }
        else
        {
            Collection<ByteBuffer> keys = restrictions.getPartitionKeys(options, state);
            if (keys.size() == 1)
            {
                return "token: " + table.partitioner.getToken(Iterables.getOnlyElement(keys)).toString();
            }
            else
            {
                StringBuilder sb = new StringBuilder("tokens: [");
                boolean isFirst = true;
                for (ByteBuffer key : keys)
                {
                    if (!isFirst) sb.append(", ");
                    sb.append(table.partitioner.getToken(key).toString());
                    isFirst = false;
                }
                return sb.append(']').toString();
            }
        }
    }

    private String asCQL(QueryOptions options, ClientState state)
    {
        ColumnFilter columnFilter = selection.newSelectors(options).getColumnFilter();
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT ").append(queriedColumns().toCQLString());
        sb.append(" FROM ").append(table.keyspace).append('.').append(table.name);
        if (restrictions.isKeyRange() || restrictions.usesSecondaryIndexing())
        {
            // partition range
            ClusteringIndexFilter clusteringIndexFilter = makeClusteringIndexFilter(options, state, columnFilter);
            if (clusteringIndexFilter == null)
                return "EMPTY";

            RowFilter rowFilter = getRowFilter(options);

            // The LIMIT provided by the user is the number of CQL row he wants returned.
            // We want to have getRangeSlice to count the number of columns, not the number of keys.
            AbstractBounds<PartitionPosition> keyBounds = restrictions.getPartitionKeyBounds(options);
            if (keyBounds == null)
                return "EMPTY";

            DataRange dataRange = new DataRange(keyBounds, clusteringIndexFilter);

            if (!dataRange.isUnrestricted(table) || !rowFilter.isEmpty())
            {
                sb.append(" WHERE ");
                // We put the row filter first because the data range can end by "ORDER BY"
                if (!rowFilter.isEmpty())
                {
                    sb.append(rowFilter);
                    if (!dataRange.isUnrestricted(table))
                        sb.append(" AND ");
                }
                if (!dataRange.isUnrestricted(table))
                    sb.append(dataRange.toCQLString(table, rowFilter));
            }
        }
        else
        {
            // single partition
            Collection<ByteBuffer> keys = restrictions.getPartitionKeys(options, state);
            if (keys.isEmpty())
                return "EMPTY";
            ClusteringIndexFilter filter = makeClusteringIndexFilter(options, state, columnFilter);
            if (filter == null)
                return "EMPTY";

            sb.append(" WHERE ");


            boolean compoundPk = table.partitionKeyColumns().size() > 1;
            if (compoundPk) sb.append('(');
            sb.append(ColumnMetadata.toCQLString(table.partitionKeyColumns()));
            if (compoundPk) sb.append(')');
            if (keys.size() == 1)
            {
                sb.append(" = ");
                if (compoundPk) sb.append('(');
                DataRange.appendKeyString(sb, table.partitionKeyType, Iterables.getOnlyElement(keys));
                if (compoundPk) sb.append(')');
            }
            else
            {
                sb.append(" IN (");
                boolean first = true;
                for (ByteBuffer key : keys)
                {
                    if (!first)
                        sb.append(", ");

                    if (compoundPk) sb.append('(');
                    DataRange.appendKeyString(sb, table.partitionKeyType, key);
                    if (compoundPk) sb.append(')');
                    first = false;
                }

                sb.append(')');
            }

            RowFilter rowFilter = getRowFilter(options);
            if (!rowFilter.isEmpty())
                sb.append(" AND ").append(rowFilter);

            String filterString = filter.toCQLString(table, rowFilter);
            if (!filterString.isEmpty())
                sb.append(" AND ").append(filterString);
        }

        DataLimits limits = getDataLimits(getLimit(options), getPerPartitionLimit(options), options.getPageSize(), getAggregationSpec(options));
        if (limits != DataLimits.NONE)
            sb.append(' ').append(limits);
        return sb.toString();
    }
}
