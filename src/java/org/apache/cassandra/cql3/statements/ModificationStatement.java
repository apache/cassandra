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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.monitoring.BadQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.view.BaseReadCommandBuilder;
import org.apache.cassandra.db.view.ViewUpdateGenerator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.conditions.ColumnConditions;
import org.apache.cassandra.cql3.conditions.Conditions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.ResultSetBuilder;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.selection.Selection.Selectors;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.db.view.ViewUtils.ViewRowComparison;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryAnalyticsService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.disk.usage.DiskUsageBroadcaster;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.BallotGenerator;
import org.apache.cassandra.service.paxos.Commit.Proposal;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.service.paxos.Ballot.Flag.NONE;

/*
 * Abstract parent class of individual modifications, i.e. INSERT, UPDATE and DELETE.
 */
public abstract class ModificationStatement implements CQLStatement.SingleKeyspaceCqlStatement
{
    protected static final Logger logger = LoggerFactory.getLogger(ModificationStatement.class);

    private final static MD5Digest EMPTY_HASH = MD5Digest.wrap(new byte[] {});

    public static final String CUSTOM_EXPRESSIONS_NOT_ALLOWED =
        "Custom index expressions cannot be used in WHERE clauses for UPDATE or DELETE statements";

    public static final ColumnIdentifier CAS_RESULT_COLUMN = new ColumnIdentifier("[applied]", false);

    protected final StatementType type;

    protected final VariableSpecifications bindVariables;

    public final TableMetadata metadata;
    private final Attributes attrs;

    private final StatementRestrictions restrictions;

    private final Operations operations;

    private final RegularAndStaticColumns updatedColumns;

    private final Conditions conditions;

    private final RegularAndStaticColumns conditionColumns;

    private final RegularAndStaticColumns requiresRead;

    public ModificationStatement(StatementType type,
                                 VariableSpecifications bindVariables,
                                 TableMetadata metadata,
                                 Operations operations,
                                 StatementRestrictions restrictions,
                                 Conditions conditions,
                                 Attributes attrs)
    {
        this.type = type;
        this.bindVariables = bindVariables;
        this.metadata = metadata;
        this.restrictions = restrictions;
        this.operations = operations;
        this.conditions = conditions;
        this.attrs = attrs;

        if (!conditions.isEmpty())
        {
            checkFalse(metadata.isCounter(), "Conditional updates are not supported on counter tables");
            checkFalse(attrs.isTimestampSet(), "Cannot provide custom timestamp for conditional updates");
        }

        RegularAndStaticColumns.Builder conditionColumnsBuilder = RegularAndStaticColumns.builder();
        Iterable<ColumnMetadata> columns = conditions.getColumns();
        if (columns != null)
            conditionColumnsBuilder.addAll(columns);

        RegularAndStaticColumns.Builder updatedColumnsBuilder = RegularAndStaticColumns.builder();
        RegularAndStaticColumns.Builder requiresReadBuilder = RegularAndStaticColumns.builder();
        for (Operation operation : operations)
        {
            updatedColumnsBuilder.add(operation.column);
            // If the operation requires a read-before-write and we're doing a conditional read, we want to read
            // the affected column as part of the read-for-conditions paxos phase (see #7499).
            if (operation.requiresRead())
            {
                conditionColumnsBuilder.add(operation.column);
                requiresReadBuilder.add(operation.column);
            }
        }

        RegularAndStaticColumns modifiedColumns = updatedColumnsBuilder.build();

        // Compact tables have not row marker. So if we don't actually update any particular column,
        // this means that we're only updating the PK, which we allow if only those were declared in
        // the definition. In that case however, we do went to write the compactValueColumn (since again
        // we can't use a "row marker") so add it automatically.
        if (metadata.isCompactTable() && modifiedColumns.isEmpty() && updatesRegularRows())
            modifiedColumns = metadata.regularAndStaticColumns();

        this.updatedColumns = modifiedColumns;
        this.conditionColumns = conditionColumnsBuilder.build();
        this.requiresRead = requiresReadBuilder.build();
    }

    @Override
    public List<ColumnSpecification> getBindVariables()
    {
        return bindVariables.getBindVariables();
    }

    @Override
    public short[] getPartitionKeyBindVariableIndexes()
    {
        return bindVariables.getPartitionKeyBindVariableIndexes(metadata);
    }

    @Override
    public Iterable<Function> getFunctions()
    {
        List<Function> functions = new ArrayList<>();
        addFunctionsTo(functions);
        return functions;
    }

    public void addFunctionsTo(List<Function> functions)
    {
        attrs.addFunctionsTo(functions);
        restrictions.addFunctionsTo(functions);
        operations.addFunctionsTo(functions);
        conditions.addFunctionsTo(functions);
    }

    public TableMetadata metadata()
    {
        return metadata;
    }

    /*
     * May be used by QueryHandler implementations
     */
    public StatementRestrictions getRestrictions()
    {
        return restrictions;
    }

    public abstract void addUpdateForKey(PartitionUpdate.Builder updateBuilder, Clustering<?> clustering, UpdateParameters params);

    public abstract void addUpdateForKey(PartitionUpdate.Builder updateBuilder, Slice slice, UpdateParameters params);

    @Override
    public String keyspace()
    {
        return metadata.keyspace;
    }

    public String table()
    {
        return metadata.name;
    }

    public boolean isCounter()
    {
        return metadata().isCounter();
    }

    public boolean isView()
    {
        return metadata().isView();
    }

    public boolean isVirtual()
    {
        return metadata().isVirtual();
    }

    public long getTimestamp(long now, QueryOptions options) throws InvalidRequestException
    {
        return attrs.getTimestamp(now, options);
    }

    public boolean isTimestampSet()
    {
        return attrs.isTimestampSet();
    }

    public int getTimeToLive(QueryOptions options) throws InvalidRequestException
    {
        return attrs.getTimeToLive(options, metadata);
    }

    public void authorize(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        state.ensureTablePermission(metadata, Permission.MODIFY);

        // CAS updates can be used to simulate a SELECT query, so should require Permission.SELECT as well.
        if (hasConditions())
            state.ensureTablePermission(metadata, Permission.SELECT);

        // MV updates need to get the current state from the table, and might update the views
        // Require Permission.SELECT on the base table, and Permission.MODIFY on the views
        Iterator<ViewMetadata> views = View.findAll(keyspace(), table()).iterator();
        if (views.hasNext())
        {
            state.ensureTablePermission(metadata, Permission.SELECT);
            do
            {
                state.ensureTablePermission(views.next().metadata, Permission.MODIFY);
            } while (views.hasNext());
        }

        for (Function function : getFunctions())
            state.ensurePermission(Permission.EXECUTE, function);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (DatabaseDescriptor.getMaterializedViewsBasetableMetricCollectionEnabled())
        {
            ColumnFamilyStore cfs = Keyspace.openAndGetStoreIfExists(metadata);
            if (cfs != null && cfs.viewManager.hasViews())
            {
                if (attrs.isTimestampSet())
                    cfs.metric.viewBaseTableModificationWithTimestamp.inc();

                if (restrictions.clusteringKeyRestrictionsHasIN() || restrictions.keyIsInRelation())
                    cfs.metric.viewBaseTableInRestirctionsUsed.inc();
            }
        }

        if (metadata().strictMVEnabled())
        {
            checkFalse(attrs.isTimestampSet(), "Cannot provide custom timestamp for strict MV consistency enabled table");
            checkFalse(restrictions.clusteringKeyRestrictionsHasIN() || restrictions.keyIsInRelation(),
                       "Cannot use IN restritions in statement for strict MV consistency enabled table");
        }

        checkFalse(hasConditions() && attrs.isTimestampSet(), "Cannot provide custom timestamp for conditional updates");
        checkFalse(isCounter() && attrs.isTimestampSet(), "Cannot provide custom timestamp for counter updates");
        checkFalse(isCounter() && attrs.isTimeToLiveSet(), "Cannot provide custom TTL for counter updates");
        if (StorageService.instance.getDirectMaterializedViewModificationEnabled() && isView())
        {
            ClientRequestsMetricsHolder.viewWriteMetrics.directMVModification.inc();
        }
        else
        {
            checkFalse(isView(), "Cannot directly modify a materialized view");
        }

        checkFalse(isVirtual() && attrs.isTimestampSet(), "Custom timestamp is not supported by virtual tables");
        checkFalse(isVirtual() && attrs.isTimeToLiveSet(), "Expiring columns are not supported by virtual tables");
        checkFalse(isVirtual() && hasConditions(), "Conditional updates are not supported by virtual tables");

        if (attrs.isTimestampSet())
            Guardrails.userTimestampsEnabled.ensureEnabled(state);
    }

    public void validateDiskUsage(QueryOptions options, ClientState state)
    {
        // reject writes if any replica exceeds disk usage failure limit or warn if it exceeds warn limit
        if (Guardrails.replicaDiskUsage.enabled(state) && DiskUsageBroadcaster.instance.hasStuffedOrFullNode())
        {
            Keyspace keyspace = Keyspace.open(keyspace());

            for (ByteBuffer key : buildPartitionKeyNames(options, state))
            {
                Token token = metadata().partitioner.getToken(key);

                for (Replica replica : ReplicaLayout.forTokenWriteLiveAndDown(keyspace, token).all())
                {
                    Guardrails.replicaDiskUsage.guard(replica.endpoint(), state);
                }
            }
        }
    }

    public RegularAndStaticColumns updatedColumns()
    {
        return updatedColumns;
    }

    public RegularAndStaticColumns conditionColumns()
    {
        return conditionColumns;
    }

    public boolean updatesRegularRows()
    {
        // We're updating regular rows if all the clustering columns are provided.
        // Note that the only case where we're allowed not to provide clustering
        // columns is if we set some static columns, and in that case no clustering
        // columns should be given. So in practice, it's enough to check if we have
        // either the table has no clustering or if it has at least one of them set.
        return metadata().clusteringColumns().isEmpty() || restrictions.hasClusteringColumnsRestrictions();
    }

    public boolean updatesStaticRow()
    {
        return operations.appliesToStaticColumns();
    }

    public List<Operation> getRegularOperations()
    {
        return operations.regularOperations();
    }

    public List<Operation> getStaticOperations()
    {
        return operations.staticOperations();
    }

    public Iterable<Operation> allOperations()
    {
        return operations;
    }

    public Iterable<ColumnMetadata> getColumnsWithConditions()
    {
         return conditions.getColumns();
    }

    public boolean hasIfNotExistCondition()
    {
        return conditions.isIfNotExists();
    }

    public boolean hasIfExistCondition()
    {
        return conditions.isIfExists();
    }

    public List<ByteBuffer> buildPartitionKeyNames(QueryOptions options, ClientState state)
    throws InvalidRequestException
    {
        List<ByteBuffer> partitionKeys = restrictions.getPartitionKeys(options, state);
        for (ByteBuffer key : partitionKeys)
            QueryProcessor.validateKey(key);

        return partitionKeys;
    }

    public NavigableSet<Clustering<?>> createClustering(QueryOptions options, ClientState state)
    throws InvalidRequestException
    {
        if (appliesOnlyToStaticColumns() && !restrictions.hasClusteringColumnsRestrictions())
            return FBUtilities.singleton(CBuilder.STATIC_BUILDER.build(), metadata().comparator);

        return restrictions.getClusteringColumns(options, state);
    }

    /**
     * Checks that the modification only apply to static columns.
     * @return <code>true</code> if the modification only apply to static columns, <code>false</code> otherwise.
     */
    private boolean appliesOnlyToStaticColumns()
    {
        return appliesOnlyToStaticColumns(operations, conditions);
    }

    /**
     * Checks that the specified operations and conditions only apply to static columns.
     * @return <code>true</code> if the specified operations and conditions only apply to static columns,
     * <code>false</code> otherwise.
     */
    public static boolean appliesOnlyToStaticColumns(Operations operation, Conditions conditions)
    {
        return !operation.appliesToRegularColumns() && !conditions.appliesToRegularColumns()
                && (operation.appliesToStaticColumns() || conditions.appliesToStaticColumns());
    }

    public boolean requiresRead()
    {
        // A subset of operations require a read before write:
        // * Setting list element by index
        // * Deleting list element by index
        // * Deleting list element by value
        // * Performing addition on a StringType (i.e. concatenation, only supported for CAS operations)
        // * Performing addition on a NumberType, again only supported for CAS operations.
        return !requiresRead.isEmpty();
    }

    private Map<DecoratedKey, Partition> readRequiredLists(Collection<ByteBuffer> partitionKeys,
                                                           ClusteringIndexFilter filter,
                                                           DataLimits limits,
                                                           boolean local,
                                                           ConsistencyLevel cl,
                                                           int nowInSeconds,
                                                           Dispatcher.RequestTime requestTime)
    {
        if (!requiresRead())
            return null;

        try
        {
            cl.validateForRead();
        }
        catch (InvalidRequestException e)
        {
            throw new InvalidRequestException(String.format("Write operation require a read but consistency %s is not supported on reads", cl));
        }

        List<SinglePartitionReadCommand> commands = new ArrayList<>(partitionKeys.size());
        for (ByteBuffer key : partitionKeys)
            commands.add(SinglePartitionReadCommand.create(metadata(),
                                                           nowInSeconds,
                                                           ColumnFilter.selection(this.requiresRead),
                                                           RowFilter.NONE,
                                                           limits,
                                                           metadata().partitioner.decorateKey(key),
                                                           filter));

        SinglePartitionReadCommand.Group group = SinglePartitionReadCommand.Group.create(commands, DataLimits.NONE);

        if (local)
        {
            try (ReadExecutionController executionController = group.executionController();
                 PartitionIterator iter = group.executeInternal(executionController))
            {
                return asMaterializedMap(iter);
            }
        }

        try (PartitionIterator iter = group.execute(cl, null, requestTime))
        {
            return asMaterializedMap(iter);
        }
    }

    private Map<DecoratedKey, Partition> asMaterializedMap(PartitionIterator iterator)
    {
        Map<DecoratedKey, Partition> map = new HashMap<>();
        while (iterator.hasNext())
        {
            try (RowIterator partition = iterator.next())
            {
                map.put(partition.partitionKey(), FilteredPartition.create(partition));
            }
        }
        return map;
    }

    public boolean hasConditions()
    {
        return !conditions.isEmpty();
    }

    public boolean hasSlices()
    {
        return type.allowClusteringColumnSlices()
               && getRestrictions().hasClusteringColumnsRestrictions()
               && getRestrictions().isColumnRange();
    }

    public ResultMessage execute(QueryState queryState, QueryOptions options, Dispatcher.RequestTime requestTime)
    throws RequestExecutionException, RequestValidationException
    {
        BadQuery.checkForCLSettings(this.metadata(), options.getConsistency(), true);

        if (options.getConsistency() == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        Guardrails.writeConsistencyLevels.guard(EnumSet.of(options.getConsistency(), options.getSerialConsistency()),
                                                queryState.getClientState());
        Guardrails.enforceWriteCL(queryState.getClientState(), keyspace(), options.getConsistency(),
                                  options.getSerialConsistency(), options::setConsistency);


        // Only when we allow direct modification of views on MV repair, isView can be true
        if (DatabaseDescriptor.getViewKeyRebuildOnDeletionEnabled() && isView())
            return rebuildMVKey(queryState, options, requestTime);

        if (metadata.strictMVEnabled())
        {
            return executeWithStrictMVConsistency(queryState, options, requestTime);
        }

        return hasConditions()
             ? executeWithCondition(queryState, options, requestTime)
             : executeWithoutCondition(queryState, options, requestTime);
    }

    private ResultMessage rebuildMVKey(QueryState queryState, QueryOptions options, Dispatcher.RequestTime requestTime)
    throws InvalidRequestException
    {
        checkTrue(type.isDelete(), "Can only use DELETE statements to rebuildMVKey");
        checkFalse(restrictions.clusteringKeyRestrictionsHasIN() || restrictions.keyIsInRelation(),
                   "Cannot use IN restrictions in rebuildMVKey");
        checkTrue(restrictions.hasAllPKColumnsRestrictedByEqualities(), "rebuildMVKey requires all primary key restricted by equalities");
        ColumnFamilyStore viewCfs = Schema.instance.getColumnFamilyStoreInstance(metadata.id);
        Keyspace keyspace = Schema.instance.getKeyspaceInstance(keyspace());
        if (keyspace == null)
            throw new InvalidRequestException("Cannot find the keyspace " + keyspace() + " for " + metadata.name);
        View view = keyspace.viewManager.getByName(viewCfs.name);
        // enforce serial read
        ConsistencyLevel readCL = options.getConsistency() == ConsistencyLevel.LOCAL_QUORUM
                ? ConsistencyLevel.LOCAL_SERIAL
                : ConsistencyLevel.SERIAL;
        List<ByteBuffer> partitionKeys = buildPartitionKeyNames(options, queryState.getClientState());
        if (partitionKeys.size() != 1)
            throw new InvalidRequestException("rebuildMVKey can only be called with a single partition key");
        Clustering<?> clusterings = Iterables.getOnlyElement(createClustering(options, queryState.getClientState()));

        // record time spent
        try (TableMetrics.TableTimer.Context c = viewCfs.metric.viewRebuildKeyTime.time())
        {
            int nowInSeconds = FBUtilities.nowInSeconds();
            BaseReadCommandBuilder builder = new BaseReadCommandBuilder(view, partitionKeys.get(0), clusterings);
            SinglePartitionReadCommand readCommand = builder.buildBaseTableReadCommand(nowInSeconds);
            Row baseRow;
            long readTimestamp = queryState.getTimestamp();
            try (RowIterator iter = StorageProxy.readOne(readCommand, readCL, requestTime))
            {
                baseRow = iter.hasNext() ? iter.next() : null;
            }
            ViewUpdateGenerator generator = new ViewUpdateGenerator(view, builder.buildBasePartitionKey(), nowInSeconds);
            if (DatabaseDescriptor.getViewKeyRebuildViewReadEnabled())
            {
                // Info only: shouldn't fail the flow
                try
                {
                    // read from view table as well for logging purposes
                    SinglePartitionReadCommand viewReadCommand = builder.buildViewTableReadCommand(partitionKeys.get(0), clusterings, nowInSeconds);
                    Row viewRow;
                    try (RowIterator iter = StorageProxy.readOne(viewReadCommand, readCL, requestTime))
                    {
                        viewRow = iter.hasNext() ? iter.next() : null;
                    }

                    // Compare base row and view row
                    ViewRowComparison.Result res = ViewRowComparison.compare(view, baseRow, viewRow,
                                                                             builder.buildBasePartitionKey(),
                                                                             builder.buildNonPrimaryKeyValue(),
                                                                             nowInSeconds);
                    // TODO: metrics on comparison result
                    if (DatabaseDescriptor.getViewKeyRebuildVerboseLoggingEnabled())
                    {
                        logger.info("rebuildMVKey view={} pk={} status={} {}",
                                    view.name, builder.formatViewPrimaryKey(), res.status, res.summary);
                        if (res.viewAhead)
                            logger.warn("rebuildMVKey VIEW_AHEAD view={} pk={} - view has newer timestamp than base. {}",
                                        view.name, builder.formatViewPrimaryKey(), res.summary);
                    }
                }
                catch (Exception e)
                {
                    logger.error("failed to read view row during rebuildMVKey view={} pk={}",
                                 view.name, builder.formatViewPrimaryKey(), e);
                }
            }

            // for now, we rebuild the view row from base table row
            generator.addBaseTableRowForReadRebuild(baseRow, builder.buildBaseClusteringKey(), readTimestamp, builder.buildNonPrimaryKeyValue());
            Collection<PartitionUpdate> updates = generator.generateViewUpdates();
            List<Mutation> mutations = new ArrayList<>(updates.size());
            for (PartitionUpdate update : updates)
                mutations.add(new Mutation(update));
            if (mutations.isEmpty())
                throw new InvalidRequestException("No mutations were generated for rebuildMVKey");
            if (DatabaseDescriptor.getViewKeyRebuildApplyMutationsEnabled())
            {
                StorageProxy.mutateWithTriggers(mutations, options.getConsistency(), false, requestTime);
                // TODO: metrics on successful mutations
                if (DatabaseDescriptor.getViewKeyRebuildVerboseLoggingEnabled())
                {
                    for (PartitionUpdate update : updates)
                    {
                        logger.info("rebuildMVKey applied to view {}, mutation={}", viewCfs.name, update.toString());
                    }
                }
            }
        }
        catch (Exception e)
        {
            logger.error("rebuildMVKey failed: ", e);
            throw e;
        }
        return null;
    }

    private ResultMessage executeWithoutCondition(QueryState queryState, QueryOptions options, Dispatcher.RequestTime requestTime)
    throws RequestExecutionException, RequestValidationException
    {
        if (isVirtual())
            return executeInternalWithoutCondition(queryState, options, requestTime);

        ConsistencyLevel cl = options.getConsistency();
        if (isCounter())
            cl.validateCounterForWrite(metadata());
        else
            cl.validateForWrite();

        validateDiskUsage(options, queryState.getClientState());

        List<? extends IMutation> mutations =
            getMutations(queryState.getClientState(),
                         options,
                         false,
                         options.getTimestamp(queryState),
                         options.getNowInSeconds(queryState),
                         requestTime);
        
        if (!mutations.isEmpty())
        {
            long startTime = MonotonicClock.Global.preciseTime.now();
            StorageProxy.mutateWithTriggers(mutations, cl, false, requestTime);
            
            // Emit Query Analytics metrics for write operations
            try
            {
                if (DatabaseDescriptor.getQueryAnalyticsConfig().isQueryAnalyticsEnabled())
                {
                    long latency = MonotonicClock.Global.preciseTime.now() - startTime;
                    QueryAnalyticsService.instance.processWriteMetric(latency, type, metadata, mutations);
                }
            }
            catch (Exception e)
            {
                logger.warn("Error processing write metric: ", e);
            }
        }

        return null;
    }

    private ResultMessage executeWithCondition(QueryState queryState, QueryOptions options, Dispatcher.RequestTime requestTime)
    {
        CQL3CasRequest request = makeCasRequest(queryState, options);

        try (RowIterator result = StorageProxy.cas(keyspace(),
                                                   table(),
                                                   request.key,
                                                   request,
                                                   options.getSerialConsistency(),
                                                   options.getConsistency(),
                                                   queryState.getClientState(),
                                                   options.getNowInSeconds(queryState),
                                                   requestTime))
        {
            return new ResultMessage.Rows(buildCasResultSet(result, queryState, options));
        }
    }

    private ResultMessage executeWithStrictMVConsistency(QueryState queryState, QueryOptions options, Dispatcher.RequestTime requestTime)
    {
        assert Paxos.useV2(); // strict MV consistency requires Paxos V2
        options.overwriteSerialConsistencyLevel();
        ResultMessage result = executeWithCondition(queryState, options, requestTime);
        if (hasConditions())
        {
            return result;
        }
        return null;
    }

    private CQL3CasRequest makeCasRequest(QueryState queryState, QueryOptions options)
    {
        ClientState clientState = queryState.getClientState();
        List<ByteBuffer> keys = buildPartitionKeyNames(options, clientState);
        // We don't support IN for CAS operation so far
        checkFalse(restrictions.keyIsInRelation(),
                   "IN on the partition key is not supported with conditional %s",
                   type.isUpdate()? "updates" : "deletions");

        DecoratedKey key = metadata().partitioner.decorateKey(keys.get(0));
        long timestamp = options.getTimestamp(queryState);
        int nowInSeconds = options.getNowInSeconds(queryState);

        checkFalse(restrictions.clusteringKeyRestrictionsHasIN(),
                   "IN on the clustering key columns is not supported with conditional %s",
                    type.isUpdate()? "updates" : "deletions");

        Clustering<?> clustering = Iterables.getOnlyElement(createClustering(options, clientState));
        CQL3CasRequest request = new CQL3CasRequest(metadata(), key, conditionColumns(), updatesRegularRows(), updatesStaticRow());

        addConditions(clustering, request, options);
        request.addRowUpdate(clustering, this, options, timestamp, nowInSeconds);

        return request;
    }

    public void addConditions(Clustering<?> clustering, CQL3CasRequest request, QueryOptions options) throws InvalidRequestException
    {
        conditions.addConditionsTo(request, clustering, options);
    }

    private static ResultSet.ResultMetadata buildCASSuccessMetadata(String ksName, String cfName)
    {
        List<ColumnSpecification> specs = new ArrayList<>();
        specs.add(casResultColumnSpecification(ksName, cfName));

        return new ResultSet.ResultMetadata(EMPTY_HASH, specs);
    }

    private static ColumnSpecification casResultColumnSpecification(String ksName, String cfName)
    {
        return new ColumnSpecification(ksName, cfName, CAS_RESULT_COLUMN, BooleanType.instance);
    }

    private ResultSet buildCasResultSet(RowIterator partition, QueryState state, QueryOptions options)
    {
        return buildCasResultSet(keyspace(), table(), partition, getColumnsWithConditions(), false, state, options);
    }

    static ResultSet buildCasResultSet(String ksName,
                                       String tableName,
                                       RowIterator partition,
                                       Iterable<ColumnMetadata> columnsWithConditions,
                                       boolean isBatch,
                                       QueryState state,
                                       QueryOptions options)
    {
        boolean success = partition == null;

        ResultSet.ResultMetadata metadata = buildCASSuccessMetadata(ksName, tableName);
        List<List<ByteBuffer>> rows = Collections.singletonList(Collections.singletonList(BooleanType.instance.decompose(success)));

        ResultSet rs = new ResultSet(metadata, rows);
        return success ? rs : merge(rs, buildCasFailureResultSet(partition, columnsWithConditions, isBatch, options, options.getNowInSeconds(state)));
    }

    private static ResultSet merge(ResultSet left, ResultSet right)
    {
        if (left.size() == 0)
            return right;
        else if (right.size() == 0)
            return left;

        assert left.size() == 1;
        int size = left.metadata.names.size() + right.metadata.names.size();
        List<ColumnSpecification> specs = new ArrayList<ColumnSpecification>(size);
        specs.addAll(left.metadata.names);
        specs.addAll(right.metadata.names);
        List<List<ByteBuffer>> rows = new ArrayList<>(right.size());
        for (int i = 0; i < right.size(); i++)
        {
            List<ByteBuffer> row = new ArrayList<ByteBuffer>(size);
            row.addAll(left.rows.get(0));
            row.addAll(right.rows.get(i));
            rows.add(row);
        }
        return new ResultSet(new ResultSet.ResultMetadata(EMPTY_HASH, specs), rows);
    }

    private static ResultSet buildCasFailureResultSet(RowIterator partition,
                                                      Iterable<ColumnMetadata> columnsWithConditions,
                                                      boolean isBatch,
                                                      QueryOptions options,
                                                      int nowInSeconds)
    {
        TableMetadata metadata = partition.metadata();
        Selection selection;
        if (columnsWithConditions == null)
        {
            selection = Selection.wildcard(metadata, false, false);
        }
        else
        {
            // We can have multiple conditions on the same columns (for collections) so use a set
            // to avoid duplicate, but preserve the order just to it follows the order of IF in the query in general
            Set<ColumnMetadata> defs = new LinkedHashSet<>();
            // Adding the partition key for batches to disambiguate if the conditions span multipe rows (we don't add them outside
            // of batches for compatibility sakes).
            if (isBatch)
                Iterables.addAll(defs, metadata.primaryKeyColumns());
            Iterables.addAll(defs, columnsWithConditions);
            selection = Selection.forColumns(metadata, new ArrayList<>(defs), false);

        }

        Selectors selectors = selection.newSelectors(options);
        ResultSetBuilder builder = new ResultSetBuilder(selection.getResultMetadata(), selectors);
        SelectStatement.forSelection(metadata, selection)
                       .processPartition(partition, options, builder, nowInSeconds);

        return builder.build();
    }

    public ResultMessage executeLocally(QueryState queryState, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        return hasConditions()
               ? executeInternalWithCondition(queryState, options)
               : executeInternalWithoutCondition(queryState, options, Dispatcher.RequestTime.forImmediateExecution());
    }

    public ResultMessage executeInternalWithoutCondition(QueryState queryState, QueryOptions options, Dispatcher.RequestTime requestTime)
    throws RequestValidationException, RequestExecutionException
    {
        long timestamp = options.getTimestamp(queryState);
        int nowInSeconds = options.getNowInSeconds(queryState);
        for (IMutation mutation : getMutations(queryState.getClientState(), options, true, timestamp, nowInSeconds, requestTime))
            mutation.apply();
        return null;
    }

    public ResultMessage executeInternalWithCondition(QueryState state, QueryOptions options)
    {
        CQL3CasRequest request = makeCasRequest(state, options);

        try (RowIterator result = casInternal(state.getClientState(), request, options.getTimestamp(state), options.getNowInSeconds(state)))
        {
            return new ResultMessage.Rows(buildCasResultSet(result, state, options));
        }
    }

    static RowIterator casInternal(ClientState state, CQL3CasRequest request, long timestamp, int nowInSeconds)
    {
        Ballot ballot = BallotGenerator.Global.atUnixMicros(timestamp, NONE);

        SinglePartitionReadQuery readCommand = request.readCommand(nowInSeconds);
        FilteredPartition current;
        try (ReadExecutionController executionController = readCommand.executionController();
             PartitionIterator iter = readCommand.executeInternal(executionController))
        {
            current = FilteredPartition.create(PartitionIterators.getOnlyElement(iter, readCommand));
        }

        if (!request.appliesTo(current))
            return current.rowIterator();

        PartitionUpdate updates = request.makeUpdates(current, state, ballot);
        updates = TriggerExecutor.instance.execute(updates);

        Proposal proposal = Proposal.of(ballot, updates);
        proposal.makeMutation().apply();
        return null;
    }

    /**
     * Convert statement into a list of mutations to apply on the server
     *
     * @param state the client state
     * @param options value for prepared statement markers
     * @param local if true, any requests (for collections) performed by getMutation should be done locally only.
     * @param timestamp the current timestamp in microseconds to use if no timestamp is user provided.
     *
     * @return list of the mutations
     */
    private List<? extends IMutation> getMutations(ClientState state,
                                                   QueryOptions options,
                                                   boolean local,
                                                   long timestamp,
                                                   int nowInSeconds,
                                                   Dispatcher.RequestTime requestTime)
    {
        List<ByteBuffer> keys = buildPartitionKeyNames(options, state);
        HashMultiset<ByteBuffer> perPartitionKeyCounts = HashMultiset.create(keys);
        SingleTableUpdatesCollector collector = new SingleTableUpdatesCollector(metadata, updatedColumns, perPartitionKeyCounts);
        addUpdates(collector, keys, state, options, local, timestamp, nowInSeconds, requestTime);
        return collector.toMutations();
    }

    final void addUpdates(UpdatesCollector collector,
                          List<ByteBuffer> keys,
                          ClientState state,
                          QueryOptions options,
                          boolean local,
                          long timestamp,
                          int nowInSeconds,
                          Dispatcher.RequestTime requestTime)
    {
        if (hasSlices())
        {
            Slices slices = createSlices(options);

            // If all the ranges were invalid we do not need to do anything.
            if (slices.isEmpty())
                return;

            UpdateParameters params = makeUpdateParameters(keys,
                                                           new ClusteringIndexSliceFilter(slices, false),
                                                           state,
                                                           options,
                                                           DataLimits.NONE,
                                                           local,
                                                           timestamp,
                                                           nowInSeconds,
                                                           requestTime);
            for (ByteBuffer key : keys)
            {
                Validation.validateKey(metadata(), key);
                DecoratedKey dk = metadata().partitioner.decorateKey(key);

                PartitionUpdate.Builder updateBuilder = collector.getPartitionUpdateBuilder(metadata(), dk, options.getConsistency());

                for (Slice slice : slices)
                    addUpdateForKey(updateBuilder, slice, params);
            }
        }
        else
        {
            NavigableSet<Clustering<?>> clusterings = createClustering(options, state);

            // If some of the restrictions were unspecified (e.g. empty IN restrictions) we do not need to do anything.
            if (restrictions.hasClusteringColumnsRestrictions() && clusterings.isEmpty())
                return;

            UpdateParameters params = makeUpdateParameters(keys, clusterings, state, options, local, timestamp, nowInSeconds, requestTime);

            for (ByteBuffer key : keys)
            {
                Validation.validateKey(metadata(), key);
                DecoratedKey dk = metadata().partitioner.decorateKey(key);

                PartitionUpdate.Builder updateBuilder = collector.getPartitionUpdateBuilder(metadata(), dk, options.getConsistency());

                if (!restrictions.hasClusteringColumnsRestrictions())
                {
                    addUpdateForKey(updateBuilder, Clustering.EMPTY, params);
                }
                else
                {
                    for (Clustering<?> clustering : clusterings)
                    {
                        validateClustering(clustering);
                        addUpdateForKey(updateBuilder, clustering, params);
                    }
                }
            }
        }
    }

    private <V> void validateClustering(Clustering<V> clustering)
    {
        ValueAccessor<V> accessor = clustering.accessor();
        for (V v : clustering.getRawValues())
        {
            if (v != null && accessor.size(v) > FBUtilities.MAX_UNSIGNED_SHORT)
                throw new InvalidRequestException(String.format("Key length of %d is longer than maximum of %d",
                                                                clustering.dataSize(),
                                                                FBUtilities.MAX_UNSIGNED_SHORT));
        }
    }

    public Slices createSlices(QueryOptions options)
    {
        SortedSet<ClusteringBound<?>> startBounds = restrictions.getClusteringColumnsBounds(Bound.START, options);
        SortedSet<ClusteringBound<?>> endBounds = restrictions.getClusteringColumnsBounds(Bound.END, options);

        return toSlices(startBounds, endBounds);
    }

    private UpdateParameters makeUpdateParameters(Collection<ByteBuffer> keys,
                                                  NavigableSet<Clustering<?>> clusterings,
                                                  ClientState state,
                                                  QueryOptions options,
                                                  boolean local,
                                                  long timestamp,
                                                  int nowInSeconds,
                                                  Dispatcher.RequestTime requestTime)
    {
        if (clusterings.contains(Clustering.STATIC_CLUSTERING))
            return makeUpdateParameters(keys,
                                        new ClusteringIndexSliceFilter(Slices.ALL, false),
                                        state,
                                        options,
                                        DataLimits.cqlLimits(1),
                                        local,
                                        timestamp,
                                        nowInSeconds,
                                        requestTime);

        return makeUpdateParameters(keys,
                                    new ClusteringIndexNamesFilter(clusterings, false),
                                    state,
                                    options,
                                    DataLimits.NONE,
                                    local,
                                    timestamp,
                                    nowInSeconds,
                                    requestTime);
    }

    private UpdateParameters makeUpdateParameters(Collection<ByteBuffer> keys,
                                                  ClusteringIndexFilter filter,
                                                  ClientState state,
                                                  QueryOptions options,
                                                  DataLimits limits,
                                                  boolean local,
                                                  long timestamp,
                                                  int nowInSeconds,
                                                  Dispatcher.RequestTime requestTime)
    {
        // Some lists operation requires reading
        Map<DecoratedKey, Partition> lists =
            readRequiredLists(keys,
                              filter,
                              limits,
                              local,
                              options.getConsistency(),
                              nowInSeconds,
                              requestTime);

        return new UpdateParameters(metadata(),
                                    updatedColumns(),
                                    state,
                                    options,
                                    getTimestamp(timestamp, options),
                                    nowInSeconds,
                                    getTimeToLive(options),
                                    lists);
    }

    private Slices toSlices(SortedSet<ClusteringBound<?>> startBounds, SortedSet<ClusteringBound<?>> endBounds)
    {
        return toSlices(metadata, startBounds, endBounds);
    }

    public static Slices toSlices(TableMetadata metadata, SortedSet<ClusteringBound<?>> startBounds, SortedSet<ClusteringBound<?>> endBounds)
    {
        return toSlices(metadata.comparator, startBounds, endBounds);
    }

    public static Slices toSlices(ClusteringComparator comparator, SortedSet<ClusteringBound<?>> startBounds, SortedSet<ClusteringBound<?>> endBounds)
    {
        assert startBounds.size() == endBounds.size();

        Slices.Builder builder = new Slices.Builder(comparator);

        Iterator<ClusteringBound<?>> starts = startBounds.iterator();
        Iterator<ClusteringBound<?>> ends = endBounds.iterator();

        while (starts.hasNext())
        {
            Slice slice = Slice.make(starts.next(), ends.next());
            if (!slice.isEmpty(comparator))
            {
                builder.add(slice);
            }
        }

        return builder.build();
    }

    public static abstract class Parsed extends QualifiedStatement
    {
        protected final StatementType type;
        private final Attributes.Raw attrs;
        private final List<Pair<ColumnIdentifier, ColumnCondition.Raw>> conditions;
        private final boolean ifNotExists;
        private final boolean ifExists;

        protected Parsed(QualifiedName name,
                         StatementType type,
                         Attributes.Raw attrs,
                         List<Pair<ColumnIdentifier, ColumnCondition.Raw>> conditions,
                         boolean ifNotExists,
                         boolean ifExists)
        {
            super(name);
            this.type = type;
            this.attrs = attrs;
            this.conditions = conditions == null ? Collections.emptyList() : conditions;
            this.ifNotExists = ifNotExists;
            this.ifExists = ifExists;
        }

        public ModificationStatement prepare(ClientState state)
        {
            return prepare(bindVariables);
        }

        public ModificationStatement prepare(VariableSpecifications bindVariables)
        {
            TableMetadata metadata = Schema.instance.validateTable(keyspace(), name());

            Attributes preparedAttributes = attrs.prepare(keyspace(), name());
            preparedAttributes.collectMarkerSpecification(bindVariables);

            Conditions preparedConditions = prepareConditions(metadata, bindVariables);

            return prepareInternal(metadata, bindVariables, preparedConditions, preparedAttributes);
        }

        /**
         * Returns the column conditions.
         *
         * @param metadata the column family meta data
         * @param bindVariables the bound names
         * @return the column conditions.
         */
        private Conditions prepareConditions(TableMetadata metadata, VariableSpecifications bindVariables)
        {
            // To have both 'IF EXISTS'/'IF NOT EXISTS' and some other conditions doesn't make sense.
            // So far this is enforced by the parser, but let's assert it for sanity if ever the parse changes.
            if (ifExists)
            {
                assert conditions.isEmpty();
                assert !ifNotExists;
                return Conditions.IF_EXISTS_CONDITION;
            }

            if (ifNotExists)
            {
                assert conditions.isEmpty();
                assert !ifExists;
                return Conditions.IF_NOT_EXISTS_CONDITION;
            }

            if (conditions.isEmpty())
                return Conditions.EMPTY_CONDITION;

            return prepareColumnConditions(metadata, bindVariables);
        }

        /**
         * Returns the column conditions.
         *
         * @param metadata the column family meta data
         * @param bindVariables the bound names
         * @return the column conditions.
         */
        private ColumnConditions prepareColumnConditions(TableMetadata metadata, VariableSpecifications bindVariables)
        {
            checkNull(attrs.timestamp, "Cannot provide custom timestamp for conditional updates");

            ColumnConditions.Builder builder = ColumnConditions.newBuilder();

            for (Pair<ColumnIdentifier, ColumnCondition.Raw> entry : conditions)
            {
                ColumnMetadata def = metadata.getExistingColumn(entry.left);
                ColumnCondition condition = entry.right.prepare(keyspace(), def, metadata);
                condition.collectMarkerSpecification(bindVariables);

                checkFalse(def.isPrimaryKeyColumn(), "PRIMARY KEY column '%s' cannot have IF conditions", def.name);
                builder.add(condition);
            }
            return builder.build();
        }

        protected abstract ModificationStatement prepareInternal(TableMetadata metadata,
                                                                 VariableSpecifications bindVariables,
                                                                 Conditions conditions,
                                                                 Attributes attrs);

        /**
         * Creates the restrictions.
         *
         * @param metadata the column family meta data
         * @param boundNames the bound names
         * @param operations the column operations
         * @param where the where clause
         * @param conditions the conditions
         * @return the restrictions
         */
        protected StatementRestrictions newRestrictions(TableMetadata metadata,
                                                        VariableSpecifications boundNames,
                                                        Operations operations,
                                                        WhereClause where,
                                                        Conditions conditions)
        {
            if (where.containsCustomExpressions())
                throw new InvalidRequestException(CUSTOM_EXPRESSIONS_NOT_ALLOWED);

            boolean applyOnlyToStaticColumns = appliesOnlyToStaticColumns(operations, conditions);
            return new StatementRestrictions(type, metadata, where, boundNames, applyOnlyToStaticColumns, false, false);
        }

        public List<Pair<ColumnIdentifier, ColumnCondition.Raw>> getConditions()
        {
            return conditions;
        }
    }
}
