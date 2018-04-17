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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;

import com.google.common.base.MoreObjects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.SuperColumnCompatibility;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
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
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.AggregationQueryPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
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
public class SelectStatement implements CQLStatement
{
    private static final Logger logger = LoggerFactory.getLogger(SelectStatement.class);

    public static final int DEFAULT_PAGE_SIZE = 10000;

    private final int boundTerms;
    public final CFMetaData cfm;
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
    private final Comparator<List<ByteBuffer>> orderingComparator;

    private final ColumnFilter queriedColumns;

    // Used by forSelection below
    private static final Parameters defaultParameters = new Parameters(Collections.emptyMap(),
                                                                       Collections.emptyList(),
                                                                       false,
                                                                       false,
                                                                       false);

    public SelectStatement(CFMetaData cfm,
                           int boundTerms,
                           Parameters parameters,
                           Selection selection,
                           StatementRestrictions restrictions,
                           boolean isReversed,
                           AggregationSpecification aggregationSpec,
                           Comparator<List<ByteBuffer>> orderingComparator,
                           Term limit,
                           Term perPartitionLimit)
    {
        this.cfm = cfm;
        this.boundTerms = boundTerms;
        this.selection = selection;
        this.restrictions = restrictions;
        this.isReversed = isReversed;
        this.aggregationSpec = aggregationSpec;
        this.orderingComparator = orderingComparator;
        this.parameters = parameters;
        this.limit = limit;
        this.perPartitionLimit = perPartitionLimit;
        this.queriedColumns = gatherQueriedColumns();
    }

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

    // Note that the queried columns internally is different from the one selected by the
    // user as it also include any column for which we have a restriction on.
    private ColumnFilter gatherQueriedColumns()
    {
        if (selection.isWildcard())
            return ColumnFilter.all(cfm);

        ColumnFilter.Builder builder = ColumnFilter.allColumnsBuilder(cfm);
        // Adds all selected columns
        for (ColumnDefinition def : selection.getColumns())
            if (!def.isPrimaryKeyColumn())
                builder.add(def);
        // as well as any restricted column (so we can actually apply the restriction)
        builder.addAll(restrictions.nonPKRestrictedColumns(true));
        return builder.build();
    }

    /**
     * The columns to fetch internally for this SELECT statement (which can be more than the one selected by the
     * user as it also include any restricted column in particular).
     */
    public ColumnFilter queriedColumns()
    {
        return queriedColumns;
    }

    // Creates a simple select based on the given selection.
    // Note that the results select statement should not be used for actual queries, but only for processing already
    // queried data through processColumnFamily.
    static SelectStatement forSelection(CFMetaData cfm, Selection selection)
    {
        return new SelectStatement(cfm,
                                   0,
                                   defaultParameters,
                                   selection,
                                   StatementRestrictions.empty(StatementType.SELECT, cfm),
                                   false,
                                   null,
                                   null,
                                   null,
                                   null);
    }

    public ResultSet.ResultMetadata getResultMetadata()
    {
        return selection.getResultMetadata(parameters.isJson);
    }

    public int getBoundTerms()
    {
        return boundTerms;
    }

    public void checkAccess(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        if (cfm.isView())
        {
            CFMetaData baseTable = View.findBaseTable(keyspace(), columnFamily());
            if (baseTable != null)
                state.hasColumnFamilyAccess(baseTable, Permission.SELECT);
        }
        else
        {
            state.hasColumnFamilyAccess(cfm, Permission.SELECT);
        }

        for (Function function : getFunctions())
            state.ensureHasPermission(Permission.EXECUTE, function);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        // Nothing to do, all validation has been done by RawStatement.prepare()
    }

    public ResultMessage.Rows execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
    {
        ConsistencyLevel cl = options.getConsistency();
        checkNotNull(cl, "Invalid empty consistency level");

        cl.validateForRead(keyspace());

        int nowInSec = FBUtilities.nowInSeconds();
        int userLimit = getLimit(options);
        int userPerPartitionLimit = getPerPartitionLimit(options);
        int pageSize = options.getPageSize();
        ReadQuery query = getQuery(options, nowInSec, userLimit, userPerPartitionLimit, pageSize);

        if (aggregationSpec == null && (pageSize <= 0 || (query.limits().count() <= pageSize)))
            return execute(query, options, state, nowInSec, userLimit, queryStartNanoTime);

        QueryPager pager = getPager(query, options);

        return execute(Pager.forDistributedQuery(pager, cl, state.getClientState()), options, pageSize, nowInSec, userLimit, queryStartNanoTime);
    }

    public ReadQuery getQuery(QueryOptions options, int nowInSec) throws RequestValidationException
    {
        return getQuery(options, nowInSec, getLimit(options), getPerPartitionLimit(options), options.getPageSize());
    }

    public ReadQuery getQuery(QueryOptions options, int nowInSec, int userLimit, int perPartitionLimit, int pageSize)
    {
        boolean isPartitionRangeQuery = restrictions.isKeyRange() || restrictions.usesSecondaryIndexing();

        DataLimits limit = getDataLimits(userLimit, perPartitionLimit, pageSize);

        if (isPartitionRangeQuery)
            return getRangeCommand(options, limit, nowInSec);

        return getSliceCommands(options, limit, nowInSec);
    }

    private ResultMessage.Rows execute(ReadQuery query,
                                       QueryOptions options,
                                       QueryState state,
                                       int nowInSec,
                                       int userLimit, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException
    {
        try (PartitionIterator data = query.execute(options.getConsistency(), state.getClientState(), queryStartNanoTime))
        {
            return processResults(data, options, nowInSec, userLimit);
        }
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

    private ResultMessage.Rows execute(Pager pager,
                                       QueryOptions options,
                                       int pageSize,
                                       int nowInSec,
                                       int userLimit,
                                       long queryStartNanoTime) throws RequestValidationException, RequestExecutionException
    {
        if (aggregationSpec != null)
        {
            if (!restrictions.hasPartitionKeyRestrictions())
            {
                warn("Aggregation query used without partition key");
            }
            else if (restrictions.keyIsInRelation())
            {
                warn("Aggregation query used on multiple partition keys (IN restriction)");
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
            msg = processResults(page, options, nowInSec, userLimit);
        }

        // Please note that the isExhausted state of the pager only gets updated when we've closed the page, so this
        // shouldn't be moved inside the 'try' above.
        if (!pager.isExhausted())
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
                                              int nowInSec,
                                              int userLimit) throws RequestValidationException
    {
        ResultSet rset = process(partitions, options, nowInSec, userLimit);
        return new ResultMessage.Rows(rset);
    }

    public ResultMessage.Rows executeInternal(QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        return executeInternal(state, options, FBUtilities.nowInSeconds(), System.nanoTime());
    }

    public ResultMessage.Rows executeInternal(QueryState state, QueryOptions options, int nowInSec, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
    {
        int userLimit = getLimit(options);
        int userPerPartitionLimit = getPerPartitionLimit(options);
        int pageSize = options.getPageSize();
        ReadQuery query = getQuery(options, nowInSec, userLimit, userPerPartitionLimit, pageSize);

        try (ReadExecutionController executionController = query.executionController())
        {
            if (aggregationSpec == null && (pageSize <= 0 || (query.limits().count() <= pageSize)))
            {
                try (PartitionIterator data = query.executeInternal(executionController))
                {
                    return processResults(data, options, nowInSec, userLimit);
                }
            }
            else
            {
                QueryPager pager = getPager(query, options);

                return execute(Pager.forInternalQuery(pager, executionController), options, pageSize, nowInSec, userLimit, queryStartNanoTime);
            }
        }
    }

    private QueryPager getPager(ReadQuery query, QueryOptions options)
    {
        QueryPager pager = query.getPager(options.getPagingState(), options.getProtocolVersion());

        if (aggregationSpec == null || query == ReadQuery.EMPTY)
            return pager;

        return new AggregationQueryPager(pager, query.limits());
    }

    public ResultSet process(PartitionIterator partitions, int nowInSec) throws InvalidRequestException
    {
        return process(partitions, QueryOptions.DEFAULT, nowInSec, getLimit(QueryOptions.DEFAULT));
    }

    public String keyspace()
    {
        return cfm.ksName;
    }

    public String columnFamily()
    {
        return cfm.cfName;
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

    private ReadQuery getSliceCommands(QueryOptions options, DataLimits limit, int nowInSec) throws RequestValidationException
    {
        Collection<ByteBuffer> keys = restrictions.getPartitionKeys(options);
        if (keys.isEmpty())
            return ReadQuery.EMPTY;

        ClusteringIndexFilter filter = makeClusteringIndexFilter(options);
        if (filter == null)
            return ReadQuery.EMPTY;

        RowFilter rowFilter = getRowFilter(options);

        // Note that we use the total limit for every key, which is potentially inefficient.
        // However, IN + LIMIT is not a very sensible choice.
        List<SinglePartitionReadCommand> commands = new ArrayList<>(keys.size());
        for (ByteBuffer key : keys)
        {
            QueryProcessor.validateKey(key);
            DecoratedKey dk = cfm.decorateKey(ByteBufferUtil.clone(key));
            ColumnFilter cf = (cfm.isSuper() && cfm.isDense()) ? SuperColumnCompatibility.getColumnFilter(cfm, options, restrictions.getSuperColumnRestrictions()) : queriedColumns;
            commands.add(SinglePartitionReadCommand.create(cfm, nowInSec, cf, rowFilter, limit, dk, filter));
        }

        return new SinglePartitionReadCommand.Group(commands, limit);
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
        ClusteringIndexFilter filter = makeClusteringIndexFilter(options);
        if (filter instanceof ClusteringIndexSliceFilter)
            return ((ClusteringIndexSliceFilter)filter).requestedSlices();

        Slices.Builder builder = new Slices.Builder(cfm.comparator);
        for (Clustering clustering: ((ClusteringIndexNamesFilter)filter).requestedRows())
            builder.add(Slice.make(clustering));
        return builder.build();
    }

    /**
     * Returns a read command that can be used internally to query all the rows queried by this SELECT for a
     * give key (used for materialized views).
     */
    public SinglePartitionReadCommand internalReadForView(DecoratedKey key, int nowInSec)
    {
        QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
        ClusteringIndexFilter filter = makeClusteringIndexFilter(options);
        RowFilter rowFilter = getRowFilter(options);
        return SinglePartitionReadCommand.create(cfm, nowInSec, queriedColumns, rowFilter, DataLimits.NONE, key, filter);
    }

    /**
     * The {@code RowFilter} for this SELECT, assuming an internal call (no bound values in particular).
     */
    public RowFilter rowFilterForInternalCalls()
    {
        return getRowFilter(QueryOptions.forInternalCalls(Collections.emptyList()));
    }

    private ReadQuery getRangeCommand(QueryOptions options, DataLimits limit, int nowInSec) throws RequestValidationException
    {
        ClusteringIndexFilter clusteringIndexFilter = makeClusteringIndexFilter(options);
        if (clusteringIndexFilter == null)
            return ReadQuery.EMPTY;

        RowFilter rowFilter = getRowFilter(options);

        // The LIMIT provided by the user is the number of CQL row he wants returned.
        // We want to have getRangeSlice to count the number of columns, not the number of keys.
        AbstractBounds<PartitionPosition> keyBounds = restrictions.getPartitionKeyBounds(options);
        if (keyBounds == null)
            return ReadQuery.EMPTY;

        PartitionRangeReadCommand command =
            PartitionRangeReadCommand.create(false, cfm, nowInSec, queriedColumns, rowFilter, limit, new DataRange(keyBounds, clusteringIndexFilter));

        // If there's a secondary index that the command can use, have it validate the request parameters.
        command.maybeValidateIndex();

        return command;
    }

    private ClusteringIndexFilter makeClusteringIndexFilter(QueryOptions options)
    throws InvalidRequestException
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
        else
        {
            NavigableSet<Clustering> clusterings = getRequestedRows(options);
            // We can have no clusterings if either we're only selecting the static columns, or if we have
            // a 'IN ()' for clusterings. In that case, we still want to query if some static columns are
            // queried. But we're fine otherwise.
            if (clusterings.isEmpty() && queriedColumns.fetchedColumns().statics.isEmpty())
                return null;

            return new ClusteringIndexNamesFilter(clusterings, isReversed);
        }
    }

    private Slices makeSlices(QueryOptions options)
    throws InvalidRequestException
    {
        SortedSet<ClusteringBound> startBounds = restrictions.getClusteringColumnsBounds(Bound.START, options);
        SortedSet<ClusteringBound> endBounds = restrictions.getClusteringColumnsBounds(Bound.END, options);
        assert startBounds.size() == endBounds.size();

        // The case where startBounds == 1 is common enough that it's worth optimizing
        if (startBounds.size() == 1)
        {
            ClusteringBound start = startBounds.first();
            ClusteringBound end = endBounds.first();
            return cfm.comparator.compare(start, end) > 0
                 ? Slices.NONE
                 : Slices.with(cfm.comparator, Slice.make(start, end));
        }

        Slices.Builder builder = new Slices.Builder(cfm.comparator, startBounds.size());
        Iterator<ClusteringBound> startIter = startBounds.iterator();
        Iterator<ClusteringBound> endIter = endBounds.iterator();
        while (startIter.hasNext() && endIter.hasNext())
        {
            ClusteringBound start = startIter.next();
            ClusteringBound end = endIter.next();

            // Ignore slices that are nonsensical
            if (cfm.comparator.compare(start, end) > 0)
                continue;

            builder.add(start, end);
        }

        return builder.build();
    }

    private DataLimits getDataLimits(int userLimit, int perPartitionLimit, int pageSize)
    {
        int cqlRowLimit = DataLimits.NO_LIMIT;
        int cqlPerPartitionLimit = DataLimits.NO_LIMIT;

        // If we do post ordering we need to get all the results sorted before we can trim them.
        if (aggregationSpec != AggregationSpecification.AGGREGATE_EVERYTHING)
        {
            if (!needsPostQueryOrdering())
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

    private NavigableSet<Clustering> getRequestedRows(QueryOptions options) throws InvalidRequestException
    {
        // Note: getRequestedColumns don't handle static columns, but due to CASSANDRA-5762
        // we always do a slice for CQL3 tables, so it's ok to ignore them here
        assert !restrictions.isColumnRange();
        return restrictions.getClusteringColumns(options);
    }

    /**
     * May be used by custom QueryHandler implementations
     */
    public RowFilter getRowFilter(QueryOptions options) throws InvalidRequestException
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(columnFamily());
        SecondaryIndexManager secondaryIndexManager = cfs.indexManager;
        RowFilter filter = restrictions.getRowFilter(secondaryIndexManager, options);
        return filter;
    }

    private ResultSet process(PartitionIterator partitions,
                              QueryOptions options,
                              int nowInSec,
                              int userLimit) throws InvalidRequestException
    {
        Selection.ResultSetBuilder result = selection.resultSetBuilder(options, parameters.isJson, aggregationSpec);

        while (partitions.hasNext())
        {
            try (RowIterator partition = partitions.next())
            {
                processPartition(partition, options, result, nowInSec);
            }
        }

        ResultSet cqlRows = result.build();

        orderResults(cqlRows);

        cqlRows.trim(userLimit);

        return cqlRows;
    }

    public static ByteBuffer[] getComponents(CFMetaData cfm, DecoratedKey dk)
    {
        ByteBuffer key = dk.getKey();
        if (cfm.getKeyValidator() instanceof CompositeType)
        {
            return ((CompositeType)cfm.getKeyValidator()).split(key);
        }
        else
        {
            return new ByteBuffer[]{ key };
        }
    }

    // Used by ModificationStatement for CAS operations
    void processPartition(RowIterator partition, QueryOptions options, Selection.ResultSetBuilder result, int nowInSec)
    throws InvalidRequestException
    {
        if (cfm.isSuper() && cfm.isDense())
        {
            SuperColumnCompatibility.processPartition(cfm, selection, partition, result, options.getProtocolVersion(), restrictions.getSuperColumnRestrictions(), options);
            return;
        }

        ProtocolVersion protocolVersion = options.getProtocolVersion();

        ByteBuffer[] keyComponents = getComponents(cfm, partition.partitionKey());

        Row staticRow = partition.staticRow();
        // If there is no rows, and there's no restriction on clustering/regular columns,
        // then provided the select was a full partition selection (either by partition key and/or by static column),
        // we want to include static columns and we're done.
        if (!partition.hasNext())
        {
            if (!staticRow.isEmpty() && (queriesFullPartitions() || cfm.isStaticCompactTable()))
            {
                result.newRow(partition.partitionKey(), staticRow.clustering());
                for (ColumnDefinition def : selection.getColumns())
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
            for (ColumnDefinition def : selection.getColumns())
            {
                switch (def.kind)
                {
                    case PARTITION_KEY:
                        result.add(keyComponents[def.position()]);
                        break;
                    case CLUSTERING:
                        result.add(row.clustering().get(def.position()));
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

    /**
     * Checks if the query is a full partitions selection.
     * @return {@code true} if the query is a full partitions selection, {@code false} otherwise.
     */
    private boolean queriesFullPartitions()
    {
        return !restrictions.hasClusteringColumnsRestrictions() && !restrictions.hasRegularColumnsRestrictions();
    }

    private static void addValue(Selection.ResultSetBuilder result, ColumnDefinition def, Row row, int nowInSec, ProtocolVersion protocolVersion)
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

    private boolean needsPostQueryOrdering()
    {
        // We need post-query ordering only for queries with IN on the partition key and an ORDER BY.
        return restrictions.keyIsInRelation() && !parameters.orderings.isEmpty();
    }

    /**
     * Orders results when multiple keys are selected (using IN)
     */
    private void orderResults(ResultSet cqlRows)
    {
        if (cqlRows.size() == 0 || !needsPostQueryOrdering())
            return;

        Collections.sort(cqlRows.rows, orderingComparator);
    }

    public static class RawStatement extends CFStatement
    {
        public final Parameters parameters;
        public final List<RawSelector> selectClause;
        public final WhereClause whereClause;
        public final Term.Raw limit;
        public final Term.Raw perPartitionLimit;

        public RawStatement(CFName cfName, Parameters parameters,
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

        public ParsedStatement.Prepared prepare(ClientState clientState) throws InvalidRequestException
        {
            return prepare(false, clientState);
        }

        public ParsedStatement.Prepared prepare(boolean forView, ClientState clientState) throws InvalidRequestException
        {
            CFMetaData cfm = ThriftValidation.validateColumnFamilyWithCompactMode(keyspace(), columnFamily(), clientState.isNoCompactMode());
            VariableSpecifications boundNames = getBoundVariables();

            Selection selection = prepareSelection(cfm, boundNames);

            StatementRestrictions restrictions = prepareRestrictions(cfm, boundNames, selection, forView);

            if (parameters.isDistinct)
            {
                checkNull(perPartitionLimit, "PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries");
                validateDistinctSelection(cfm, selection, restrictions);
            }

            AggregationSpecification aggregationSpec = getAggregationSpecification(cfm,
                                                                                   selection,
                                                                                   restrictions,
                                                                                   parameters.isDistinct);

            checkFalse(aggregationSpec == AggregationSpecification.AGGREGATE_EVERYTHING && perPartitionLimit != null,
                       "PER PARTITION LIMIT is not allowed with aggregate queries.");

            Comparator<List<ByteBuffer>> orderingComparator = null;
            boolean isReversed = false;

            if (!parameters.orderings.isEmpty())
            {
                assert !forView;
                verifyOrderingIsAllowed(restrictions);
                orderingComparator = getOrderingComparator(cfm, selection, restrictions, parameters.isJson);
                isReversed = isReversed(cfm);
                if (isReversed)
                    orderingComparator = Collections.reverseOrder(orderingComparator);
            }

            checkNeedsFiltering(restrictions);

            SelectStatement stmt = new SelectStatement(cfm,
                                                       boundNames.size(),
                                                       parameters,
                                                       selection,
                                                       restrictions,
                                                       isReversed,
                                                       aggregationSpec,
                                                       orderingComparator,
                                                       prepareLimit(boundNames, limit, keyspace(), limitReceiver()),
                                                       prepareLimit(boundNames, perPartitionLimit, keyspace(), perPartitionLimitReceiver()));

            return new ParsedStatement.Prepared(stmt, boundNames, boundNames.getPartitionKeyBindIndexes(cfm));
        }

        /**
         * Prepares the selection to use for the statement.
         *
         * @param cfm the table metadata
         * @param boundNames the bound names
         * @return the selection to use for the statement
         */
        private Selection prepareSelection(CFMetaData cfm, VariableSpecifications boundNames)
        {
            boolean hasGroupBy = !parameters.groups.isEmpty();

            if (selectClause.isEmpty())
                return hasGroupBy ? Selection.wildcardWithGroupBy(cfm, boundNames) : Selection.wildcard(cfm);

            return Selection.fromSelectors(cfm, selectClause, boundNames, hasGroupBy);
        }

        /**
         * Prepares the restrictions.
         *
         * @param cfm the column family meta data
         * @param boundNames the variable specifications
         * @param selection the selection
         * @return the restrictions
         * @throws InvalidRequestException if a problem occurs while building the restrictions
         */
        private StatementRestrictions prepareRestrictions(CFMetaData cfm,
                                                          VariableSpecifications boundNames,
                                                          Selection selection,
                                                          boolean forView) throws InvalidRequestException
        {
            return new StatementRestrictions(StatementType.SELECT,
                                             cfm,
                                             whereClause,
                                             boundNames,
                                             selection.containsOnlyStaticColumns(),
                                             selection.containsAComplexColumn(),
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

        private static void verifyOrderingIsAllowed(StatementRestrictions restrictions) throws InvalidRequestException
        {
            checkFalse(restrictions.usesSecondaryIndexing(), "ORDER BY with 2ndary indexes is not supported.");
            checkFalse(restrictions.isKeyRange(), "ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");
        }

        private static void validateDistinctSelection(CFMetaData cfm,
                                                      Selection selection,
                                                      StatementRestrictions restrictions)
                                                      throws InvalidRequestException
        {
            checkFalse(restrictions.hasClusteringColumnsRestrictions() ||
                       (restrictions.hasNonPrimaryKeyRestrictions() && !restrictions.nonPKRestrictedColumns(true).stream().allMatch(ColumnDefinition::isStatic)),
                       "SELECT DISTINCT with WHERE clause only supports restriction by partition key and/or static columns.");

            Collection<ColumnDefinition> requestedColumns = selection.getColumns();
            for (ColumnDefinition def : requestedColumns)
                checkFalse(!def.isPartitionKey() && !def.isStatic(),
                           "SELECT DISTINCT queries must only request partition key columns and/or static columns (not %s)",
                           def.name);

            // If it's a key range, we require that all partition key columns are selected so we don't have to bother
            // with post-query grouping.
            if (!restrictions.isKeyRange())
                return;

            for (ColumnDefinition def : cfm.partitionKeyColumns())
                checkTrue(requestedColumns.contains(def),
                          "SELECT DISTINCT queries must request all the partition key columns (missing %s)", def.name);
        }

        /**
         * Creates the <code>AggregationSpecification</code>s used to make the aggregates.
         *
         * @param cfm the column family metadata
         * @param selection the selection
         * @param restrictions the restrictions
         * @param isDistinct <code>true</code> if the query is a DISTINCT one. 
         * @return the <code>AggregationSpecification</code>s used to make the aggregates
         */
        private AggregationSpecification getAggregationSpecification(CFMetaData cfm,
                                                                     Selection selection,
                                                                     StatementRestrictions restrictions,
                                                                     boolean isDistinct)
        {
            if (parameters.groups.isEmpty())
                return selection.isAggregate() ? AggregationSpecification.AGGREGATE_EVERYTHING
                                               : null;

            int clusteringPrefixSize = 0;

            Iterator<ColumnDefinition> pkColumns = cfm.primaryKeyColumns().iterator();
            for (ColumnDefinition.Raw raw : parameters.groups)
            {
                ColumnDefinition def = raw.prepare(cfm);

                checkTrue(def.isPartitionKey() || def.isClusteringColumn(),
                          "Group by is currently only supported on the columns of the PRIMARY KEY, got %s", def.name);

                while (true)
                {
                    checkTrue(pkColumns.hasNext(),
                              "Group by currently only support groups of columns following their declared order in the PRIMARY KEY");

                    ColumnDefinition pkColumn = pkColumns.next();

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

            return AggregationSpecification.aggregatePkPrefix(cfm.comparator, clusteringPrefixSize);
        }

        private Comparator<List<ByteBuffer>> getOrderingComparator(CFMetaData cfm,
                                                                   Selection selection,
                                                                   StatementRestrictions restrictions,
                                                                   boolean isJson)
                                                                   throws InvalidRequestException
        {
            if (!restrictions.keyIsInRelation())
                return null;

            Map<ColumnDefinition, Integer> orderingIndexes = getOrderingIndex(cfm, selection, isJson);

            List<Integer> idToSort = new ArrayList<Integer>();
            List<Comparator<ByteBuffer>> sorters = new ArrayList<Comparator<ByteBuffer>>();

            for (ColumnDefinition.Raw raw : parameters.orderings.keySet())
            {
                ColumnDefinition orderingColumn = raw.prepare(cfm);
                idToSort.add(orderingIndexes.get(orderingColumn));
                sorters.add(orderingColumn.type);
            }
            return idToSort.size() == 1 ? new SingleColumnComparator(idToSort.get(0), sorters.get(0))
                    : new CompositeComparator(sorters, idToSort);
        }

        private Map<ColumnDefinition, Integer> getOrderingIndex(CFMetaData cfm, Selection selection, boolean isJson)
                throws InvalidRequestException
        {
            // If we order post-query (see orderResults), the sorted column needs to be in the ResultSet for sorting,
            // even if we don't
            // ultimately ship them to the client (CASSANDRA-4911).
            for (ColumnDefinition.Raw raw : parameters.orderings.keySet())
            {
                final ColumnDefinition def = raw.prepare(cfm);
                selection.addColumnForOrdering(def);
            }
            return selection.getOrderingIndex(isJson);
        }

        private boolean isReversed(CFMetaData cfm) throws InvalidRequestException
        {
            Boolean[] reversedMap = new Boolean[cfm.clusteringColumns().size()];
            int i = 0;
            for (Map.Entry<ColumnDefinition.Raw, Boolean> entry : parameters.orderings.entrySet())
            {
                ColumnDefinition def = entry.getKey().prepare(cfm);
                boolean reversed = entry.getValue();

                checkTrue(def.isClusteringColumn(),
                          "Order by is currently only supported on the clustered columns of the PRIMARY KEY, got %s", def.name);

                checkTrue(i++ == def.position(),
                          "Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY");

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
        private void checkNeedsFiltering(StatementRestrictions restrictions) throws InvalidRequestException
        {
            // non-key-range non-indexed queries cannot involve filtering underneath
            if (!parameters.allowFiltering && (restrictions.isKeyRange() || restrictions.usesSecondaryIndexing()))
            {
                // We will potentially filter data if either:
                //  - Have more than one IndexExpression
                //  - Have no index expression and the row filter is not the identity
                checkFalse(restrictions.needFiltering(), StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
            }
        }

        private ColumnSpecification limitReceiver()
        {
            return new ColumnSpecification(keyspace(), columnFamily(), new ColumnIdentifier("[limit]", true), Int32Type.instance);
        }

        private ColumnSpecification perPartitionLimitReceiver()
        {
            return new ColumnSpecification(keyspace(), columnFamily(), new ColumnIdentifier("[per_partition_limit]", true), Int32Type.instance);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                              .add("name", cfName)
                              .add("selectClause", selectClause)
                              .add("whereClause", whereClause)
                              .add("isDistinct", parameters.isDistinct)
                              .toString();
        }
    }

    public static class Parameters
    {
        // Public because CASSANDRA-9858
        public final Map<ColumnDefinition.Raw, Boolean> orderings;
        public final List<ColumnDefinition.Raw> groups;
        public final boolean isDistinct;
        public final boolean allowFiltering;
        public final boolean isJson;

        public Parameters(Map<ColumnDefinition.Raw, Boolean> orderings,
                          List<ColumnDefinition.Raw> groups,
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
}
