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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.db.marshal.ValueAccessor;
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
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.guardrails.Guardrails;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.disk.usage.DiskUsageBroadcaster;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNull;

/*
 * Abstract parent class of individual modifications, i.e. INSERT, UPDATE and DELETE.
 */
public abstract class ModificationStatement implements CQLStatement.SingleKeyspaceCqlStatement
{
    protected static final Logger logger = LoggerFactory.getLogger(ModificationStatement.class);

    private final static MD5Digest EMPTY_HASH = MD5Digest.wrap(new byte[] {});

    public static final String CUSTOM_EXPRESSIONS_NOT_ALLOWED =
        "Custom index expressions cannot be used in WHERE clauses for UPDATE or DELETE statements";

    private static final ColumnIdentifier CAS_RESULT_COLUMN = new ColumnIdentifier("[applied]", false);

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

    public String columnFamily()
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

        // Modification on base table with MV should skip SELECT access control to base table and WRITE access control to view table.

        for (Function function : getFunctions())
            state.ensurePermission(Permission.EXECUTE, function);
    }

    @Override
    public void validate(QueryState state) throws InvalidRequestException
    {
        checkFalse(hasConditions() && attrs.isTimestampSet(), "Cannot provide custom timestamp for conditional updates");
        checkFalse(isCounter() && attrs.isTimestampSet(), "Cannot provide custom timestamp for counter updates");
        checkFalse(isCounter() && attrs.isTimeToLiveSet(), "Cannot provide custom TTL for counter updates");
        checkFalse(isView(), "Cannot directly modify a materialized view");
        checkFalse(isVirtual() && attrs.isTimeToLiveSet(), "Expiring columns are not supported by virtual tables");
        checkFalse(isVirtual() && hasConditions(), "Conditional updates are not supported by virtual tables");

        // there are system queries with USING TIMESTAMP, e.g. SchemaKeyspace#saveSystemKeyspacesSchema
        if (SchemaConstants.isUserKeyspace(metadata.keyspace) && attrs.isTimestampSet())
            Guardrails.userTimestampsEnabled.ensureEnabled(state);
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

    public List<ByteBuffer> buildPartitionKeyNames(QueryOptions options, QueryState queryState)
    throws InvalidRequestException
    {
        List<ByteBuffer> partitionKeys = restrictions.getPartitionKeys(options, queryState);
        for (ByteBuffer key : partitionKeys)
            QueryProcessor.validateKey(key);

        return partitionKeys;
    }

    public NavigableSet<Clustering<?>> createClustering(QueryOptions options, QueryState queryState)
    throws InvalidRequestException
    {
        if (appliesOnlyToStaticColumns() && !restrictions.hasClusteringColumnsRestrictions())
            return FBUtilities.singleton(CBuilder.STATIC_BUILDER.build(), metadata().comparator);

        return restrictions.getClusteringColumns(options, queryState);
    }

    /**
     * Checks that the modification only apply to static columns.
     * @return <code>true</code> if the modification only apply to static columns, <code>false</code> otherwise.
     */
    private boolean appliesOnlyToStaticColumns()
    {
        return appliesOnlyToStaticColumns(operations, conditions);
    }

    public void validateDiskUsage(QueryState state, QueryOptions options)
    {
        // reject writes if any replica exceeds disk usage failure limit or warn if exceeds warn limit
        if (Guardrails.replicaDiskUsage.enabled(state) && DiskUsageBroadcaster.instance.hasStuffedOrFullNode())
        {
            for (ByteBuffer keyValue : buildPartitionKeyNames(options, state))
            {
                for (InetAddressAndPort replica : StorageService.instance.getNaturalReplicasForToken(keyspace(), keyValue).endpointList())
                    Guardrails.replicaDiskUsage.guard(replica, state);
            }
        }
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
        // Lists SET operation incurs a read.
        return !requiresRead.isEmpty();
    }

    private Map<DecoratedKey, Partition> readRequiredLists(Collection<ByteBuffer> partitionKeys,
                                                           ClusteringIndexFilter filter,
                                                           DataLimits limits,
                                                           boolean local,
                                                           ConsistencyLevel cl,
                                                           int nowInSeconds,
                                                           long queryStartNanoTime)
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

        SinglePartitionReadCommand.Group group = new SinglePartitionReadCommand.Group(commands, DataLimits.NONE);

        if (local)
        {
            try (ReadExecutionController executionController = group.executionController();
                 PartitionIterator iter = group.executeInternal(executionController))
            {
                return asMaterializedMap(iter);
            }
        }

        try (PartitionIterator iter = group.execute(cl, null, queryStartNanoTime))
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

    public ResultMessage execute(QueryState queryState, QueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        if (options.getConsistency() == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        return hasConditions()
             ? executeWithCondition(queryState, options, queryStartNanoTime)
             : executeWithoutCondition(queryState, options, queryStartNanoTime);
    }

    private ResultMessage executeWithoutCondition(QueryState queryState, QueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        if (isVirtual())
            return executeInternalWithoutCondition(queryState, options, queryStartNanoTime);

        ConsistencyLevel cl = options.getConsistency();
        validateConsistency(cl, queryState);
        validateDiskUsage(queryState, options);

        List<? extends IMutation> mutations =
            getMutations(queryState,
            options,
            false,
            options.getTimestamp(queryState),
            options.getNowInSeconds(queryState), queryStartNanoTime);
        if (!mutations.isEmpty())
            StorageProxy.mutateWithTriggers(mutations, cl, false, queryStartNanoTime);

        return null;
    }

    public void validateConsistency(ConsistencyLevel cl, QueryState queryState)
    {
        if (isCounter())
            cl.validateCounterForWrite(metadata(), queryState);
        else
            cl.validateForWrite(metadata.keyspace, queryState);
    }

    private ResultMessage executeWithCondition(QueryState queryState, QueryOptions options, long queryStartNanoTime)
    {
        CQL3CasRequest request = makeCasRequest(queryState, options);

        try (RowIterator result = StorageProxy.cas(keyspace(),
                                                   columnFamily(),
                                                   request.key,
                                                   request,
                                                   options.getSerialConsistency(),
                                                   options.getConsistency(),
                                                   queryState,
                                                   options.getNowInSeconds(queryState),
                                                   queryStartNanoTime))
        {
            return new ResultMessage.Rows(buildCasResultSet(result, queryState, options));
        }
    }

    private CQL3CasRequest makeCasRequest(QueryState queryState, QueryOptions options)
    {
        List<ByteBuffer> keys = buildPartitionKeyNames(options, queryState);
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

        Clustering<?> clustering = Iterables.getOnlyElement(createClustering(options, queryState));
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
        return buildCasResultSet(keyspace(), columnFamily(), partition, getColumnsWithConditions(), false, state, options);
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
               : executeInternalWithoutCondition(queryState, options, System.nanoTime());
    }

    public ResultMessage executeInternalWithoutCondition(QueryState queryState, QueryOptions options, long queryStartNanoTime)
    throws RequestValidationException, RequestExecutionException
    {
        long timestamp = options.getTimestamp(queryState);
        int nowInSeconds = options.getNowInSeconds(queryState);
        for (IMutation mutation : getMutations(queryState, options, true, timestamp, nowInSeconds, queryStartNanoTime))
            mutation.apply();
        return null;
    }

    public ResultMessage executeInternalWithCondition(QueryState state, QueryOptions options)
    {
        CQL3CasRequest request = makeCasRequest(state, options);

        try (RowIterator result = casInternal(request, options.getTimestamp(state), options.getNowInSeconds(state), state))
        {
            return new ResultMessage.Rows(buildCasResultSet(result, state, options));
        }
    }

    static RowIterator casInternal(CQL3CasRequest request, long timestamp, int nowInSeconds, QueryState state)
    {
        UUID ballot = UUIDGen.getTimeUUIDFromMicros(timestamp);

        SinglePartitionReadQuery readCommand = request.readCommand(nowInSeconds);
        FilteredPartition current;
        try (ReadExecutionController executionController = readCommand.executionController();
             PartitionIterator iter = readCommand.executeInternal(executionController))
        {
            current = FilteredPartition.create(PartitionIterators.getOnlyElement(iter, readCommand));
        }

        if (!request.appliesTo(current))
            return current.rowIterator();

        PartitionUpdate updates = request.makeUpdates(current, state);
        updates = TriggerExecutor.instance.execute(updates);

        Commit proposal = Commit.newProposal(ballot, updates);
        proposal.makeMutation().apply();
        return null;
    }

    /**
     * Convert statement into a list of mutations to apply on the server
     *
     *
     * @param state The query state
     * @param options value for prepared statement markers
     * @param local if true, any requests (for collections) performed by getMutation should be done locally only.
     * @param timestamp the current timestamp in microseconds to use if no timestamp is user provided.
     *
     * @return list of the mutations
     */
    private List<? extends IMutation> getMutations(QueryState state,
                                                   QueryOptions options,
                                                   boolean local,
                                                   long timestamp,
                                                   int nowInSeconds,
                                                   long queryStartNanoTime)
    {
        List<ByteBuffer> keys = buildPartitionKeyNames(options, state);
        HashMultiset<ByteBuffer> perPartitionKeyCounts = HashMultiset.create(keys);
        SingleTableUpdatesCollector collector = new SingleTableUpdatesCollector(metadata, updatedColumns, perPartitionKeyCounts);
        addUpdates(collector, keys, state, options, local, timestamp, nowInSeconds, queryStartNanoTime);
        return collector.toMutations();
    }

    final void addUpdates(UpdatesCollector collector,
                          List<ByteBuffer> keys,
                          QueryState state,
                          QueryOptions options,
                          boolean local,
                          long timestamp,
                          int nowInSeconds,
                          long queryStartNanoTime)
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
                                                           queryStartNanoTime);
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

            UpdateParameters params = makeUpdateParameters(keys, clusterings, state, options, local, timestamp, nowInSeconds, queryStartNanoTime);

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

    Slices createSlices(QueryOptions options)
    {
        SortedSet<ClusteringBound<?>> startBounds = restrictions.getClusteringColumnsBounds(Bound.START, options);
        SortedSet<ClusteringBound<?>> endBounds = restrictions.getClusteringColumnsBounds(Bound.END, options);

        return toSlices(startBounds, endBounds);
    }

    private UpdateParameters makeUpdateParameters(Collection<ByteBuffer> keys,
                                                  NavigableSet<Clustering<?>> clusterings,
                                                  QueryState state,
                                                  QueryOptions options,
                                                  boolean local,
                                                  long timestamp,
                                                  int nowInSeconds,
                                                  long queryStartNanoTime)
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
                                        queryStartNanoTime);

        return makeUpdateParameters(keys,
                                    new ClusteringIndexNamesFilter(clusterings, false),
                                    state,
                                    options,
                                    DataLimits.NONE,
                                    local,
                                    timestamp,
                                    nowInSeconds,
                                    queryStartNanoTime);
    }

    private UpdateParameters makeUpdateParameters(Collection<ByteBuffer> keys,
                                                  ClusteringIndexFilter filter,
                                                  QueryState state,
                                                  QueryOptions options,
                                                  DataLimits limits,
                                                  boolean local,
                                                  long timestamp,
                                                  int nowInSeconds,
                                                  long queryStartNanoTime)
    {
        // Some lists operation requires reading
        Map<DecoratedKey, Partition> lists =
            readRequiredLists(keys,
                              filter,
                              limits,
                              local,
                              options.getConsistency(),
                              nowInSeconds,
                              queryStartNanoTime);

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
        assert startBounds.size() == endBounds.size();

        Slices.Builder builder = new Slices.Builder(metadata().comparator);

        Iterator<ClusteringBound<?>> starts = startBounds.iterator();
        Iterator<ClusteringBound<?>> ends = endBounds.iterator();

        while (starts.hasNext())
        {
            Slice slice = Slice.make(starts.next(), ends.next());
            if (!slice.isEmpty(metadata().comparator))
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
