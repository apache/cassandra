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

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ViewDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.ColumnIdentifier.Raw;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNull;

/*
 * Abstract parent class of individual modifications, i.e. INSERT, UPDATE and DELETE.
 */
public abstract class ModificationStatement implements CQLStatement
{
    protected static final Logger logger = LoggerFactory.getLogger(ModificationStatement.class);

    public static final String CUSTOM_EXPRESSIONS_NOT_ALLOWED =
        "Custom index expressions cannot be used in WHERE clauses for UPDATE or DELETE statements";

    private static final ColumnIdentifier CAS_RESULT_COLUMN = new ColumnIdentifier("[applied]", false);

    protected final StatementType type;

    private final int boundTerms;
    public final CFMetaData cfm;
    private final Attributes attrs;

    private final StatementRestrictions restrictions;

    private final Operations operations;

    private final PartitionColumns updatedColumns;

    private final Conditions conditions;

    private final PartitionColumns conditionColumns;

    private final PartitionColumns requiresRead;

    public ModificationStatement(StatementType type,
                                 int boundTerms,
                                 CFMetaData cfm,
                                 Operations operations,
                                 StatementRestrictions restrictions,
                                 Conditions conditions,
                                 Attributes attrs)
    {
        this.type = type;
        this.boundTerms = boundTerms;
        this.cfm = cfm;
        this.restrictions = restrictions;
        this.operations = operations;
        this.conditions = conditions;
        this.attrs = attrs;

        if (!conditions.isEmpty())
        {
            checkFalse(cfm.isCounter(), "Conditional updates are not supported on counter tables");
            checkFalse(attrs.isTimestampSet(), "Cannot provide custom timestamp for conditional updates");
        }

        PartitionColumns.Builder conditionColumnsBuilder = PartitionColumns.builder();
        Iterable<ColumnDefinition> columns = conditions.getColumns();
        if (columns != null)
            conditionColumnsBuilder.addAll(columns);

        PartitionColumns.Builder updatedColumnsBuilder = PartitionColumns.builder();
        PartitionColumns.Builder requiresReadBuilder = PartitionColumns.builder();
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

        PartitionColumns modifiedColumns = updatedColumnsBuilder.build();
        // Compact tables have not row marker. So if we don't actually update any particular column,
        // this means that we're only updating the PK, which we allow if only those were declared in
        // the definition. In that case however, we do went to write the compactValueColumn (since again
        // we can't use a "row marker") so add it automatically.
        if (cfm.isCompactTable() && modifiedColumns.isEmpty() && updatesRegularRows())
            modifiedColumns = cfm.partitionColumns();

        this.updatedColumns = modifiedColumns;
        this.conditionColumns = conditionColumnsBuilder.build();
        this.requiresRead = requiresReadBuilder.build();
    }

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

    public abstract void addUpdateForKey(PartitionUpdate update, Clustering clustering, UpdateParameters params);

    public abstract void addUpdateForKey(PartitionUpdate update, Slice slice, UpdateParameters params);

    public int getBoundTerms()
    {
        return boundTerms;
    }

    public String keyspace()
    {
        return cfm.ksName;
    }

    public String columnFamily()
    {
        return cfm.cfName;
    }

    public boolean isCounter()
    {
        return cfm.isCounter();
    }

    public boolean isView()
    {
        return cfm.isView();
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
        return attrs.getTimeToLive(options);
    }

    public void checkAccess(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.MODIFY);

        // CAS updates can be used to simulate a SELECT query, so should require Permission.SELECT as well.
        if (hasConditions())
            state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.SELECT);

        // MV updates need to get the current state from the table, and might update the views
        // Require Permission.SELECT on the base table, and Permission.MODIFY on the views
        Iterator<ViewDefinition> views = View.findAll(keyspace(), columnFamily()).iterator();
        if (views.hasNext())
        {
            state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.SELECT);
            do
            {
                state.hasColumnFamilyAccess(keyspace(), views.next().viewName, Permission.MODIFY);
            } while (views.hasNext());
        }

        for (Function function : getFunctions())
            state.ensureHasPermission(Permission.EXECUTE, function);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        checkFalse(hasConditions() && attrs.isTimestampSet(), "Cannot provide custom timestamp for conditional updates");
        checkFalse(isCounter() && attrs.isTimestampSet(), "Cannot provide custom timestamp for counter updates");
        checkFalse(isCounter() && attrs.isTimeToLiveSet(), "Cannot provide custom TTL for counter updates");
        checkFalse(isView(), "Cannot directly modify a materialized view");
    }

    public PartitionColumns updatedColumns()
    {
        return updatedColumns;
    }

    public PartitionColumns conditionColumns()
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
        return cfm.clusteringColumns().isEmpty() || restrictions.hasClusteringColumnsRestriction();
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

    public Iterable<ColumnDefinition> getColumnsWithConditions()
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

    public List<ByteBuffer> buildPartitionKeyNames(QueryOptions options)
    throws InvalidRequestException
    {
        List<ByteBuffer> partitionKeys = restrictions.getPartitionKeys(options);
        for (ByteBuffer key : partitionKeys)
            QueryProcessor.validateKey(key);

        return partitionKeys;
    }

    public NavigableSet<Clustering> createClustering(QueryOptions options)
    throws InvalidRequestException
    {
        if (appliesOnlyToStaticColumns() && !restrictions.hasClusteringColumnsRestriction())
            return FBUtilities.singleton(CBuilder.STATIC_BUILDER.build(), cfm.comparator);

        return restrictions.getClusteringColumns(options);
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
        // Lists SET operation incurs a read.
        for (Operation op : allOperations())
            if (op.requiresRead())
                return true;

        return false;
    }

    private Map<DecoratedKey, Partition> readRequiredLists(Collection<ByteBuffer> partitionKeys,
                                                           ClusteringIndexFilter filter,
                                                           DataLimits limits,
                                                           boolean local,
                                                           ConsistencyLevel cl)
    {
        if (!requiresRead())
            return null;

        try
        {
            cl.validateForRead(keyspace());
        }
        catch (InvalidRequestException e)
        {
            throw new InvalidRequestException(String.format("Write operation require a read but consistency %s is not supported on reads", cl));
        }

        List<SinglePartitionReadCommand> commands = new ArrayList<>(partitionKeys.size());
        int nowInSec = FBUtilities.nowInSeconds();
        for (ByteBuffer key : partitionKeys)
            commands.add(SinglePartitionReadCommand.create(cfm,
                                                           nowInSec,
                                                           ColumnFilter.selection(this.requiresRead),
                                                           RowFilter.NONE,
                                                           limits,
                                                           cfm.decorateKey(key),
                                                           filter));

        SinglePartitionReadCommand.Group group = new SinglePartitionReadCommand.Group(commands, DataLimits.NONE);

        if (local)
        {
            try (ReadOrderGroup orderGroup = group.startOrderGroup(); PartitionIterator iter = group.executeInternal(orderGroup))
            {
                return asMaterializedMap(iter);
            }
        }

        try (PartitionIterator iter = group.execute(cl, null))
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

    public ResultMessage execute(QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        if (options.getConsistency() == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        return hasConditions()
             ? executeWithCondition(queryState, options)
             : executeWithoutCondition(queryState, options);
    }

    private ResultMessage executeWithoutCondition(QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        ConsistencyLevel cl = options.getConsistency();
        if (isCounter())
            cl.validateCounterForWrite(cfm);
        else
            cl.validateForWrite(cfm.ksName);

        Collection<? extends IMutation> mutations = getMutations(options, false, options.getTimestamp(queryState));
        if (!mutations.isEmpty())
            StorageProxy.mutateWithTriggers(mutations, cl, false);

        return null;
    }

    public ResultMessage executeWithCondition(QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        CQL3CasRequest request = makeCasRequest(queryState, options);

        try (RowIterator result = StorageProxy.cas(keyspace(),
                                                   columnFamily(),
                                                   request.key,
                                                   request,
                                                   options.getSerialConsistency(),
                                                   options.getConsistency(),
                                                   queryState.getClientState()))
        {
            return new ResultMessage.Rows(buildCasResultSet(result, options));
        }
    }

    private CQL3CasRequest makeCasRequest(QueryState queryState, QueryOptions options)
    {
        List<ByteBuffer> keys = buildPartitionKeyNames(options);
        // We don't support IN for CAS operation so far
        checkFalse(keys.size() > 1,
                   "IN on the partition key is not supported with conditional %s",
                   type.isUpdate()? "updates" : "deletions");

        DecoratedKey key = cfm.decorateKey(keys.get(0));
        long now = options.getTimestamp(queryState);
        SortedSet<Clustering> clusterings = createClustering(options);

        checkFalse(clusterings.size() > 1,
                   "IN on the clustering key columns is not supported with conditional %s",
                    type.isUpdate()? "updates" : "deletions");

        Clustering clustering = Iterables.getOnlyElement(clusterings);

        CQL3CasRequest request = new CQL3CasRequest(cfm, key, false, conditionColumns(), updatesRegularRows(), updatesStaticRow());

        addConditions(clustering, request, options);
        request.addRowUpdate(clustering, this, options, now);

        return request;
    }

    public void addConditions(Clustering clustering, CQL3CasRequest request, QueryOptions options) throws InvalidRequestException
    {
        conditions.addConditionsTo(request, clustering, options);
    }

    private ResultSet buildCasResultSet(RowIterator partition, QueryOptions options) throws InvalidRequestException
    {
        return buildCasResultSet(keyspace(), columnFamily(), partition, getColumnsWithConditions(), false, options);
    }

    public static ResultSet buildCasResultSet(String ksName, String tableName, RowIterator partition, Iterable<ColumnDefinition> columnsWithConditions, boolean isBatch, QueryOptions options)
    throws InvalidRequestException
    {
        boolean success = partition == null;

        ColumnSpecification spec = new ColumnSpecification(ksName, tableName, CAS_RESULT_COLUMN, BooleanType.instance);
        ResultSet.ResultMetadata metadata = new ResultSet.ResultMetadata(Collections.singletonList(spec));
        List<List<ByteBuffer>> rows = Collections.singletonList(Collections.singletonList(BooleanType.instance.decompose(success)));

        ResultSet rs = new ResultSet(metadata, rows);
        return success ? rs : merge(rs, buildCasFailureResultSet(partition, columnsWithConditions, isBatch, options));
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
        return new ResultSet(new ResultSet.ResultMetadata(specs), rows);
    }

    private static ResultSet buildCasFailureResultSet(RowIterator partition, Iterable<ColumnDefinition> columnsWithConditions, boolean isBatch, QueryOptions options)
    throws InvalidRequestException
    {
        CFMetaData cfm = partition.metadata();
        Selection selection;
        if (columnsWithConditions == null)
        {
            selection = Selection.wildcard(cfm);
        }
        else
        {
            // We can have multiple conditions on the same columns (for collections) so use a set
            // to avoid duplicate, but preserve the order just to it follows the order of IF in the query in general
            Set<ColumnDefinition> defs = new LinkedHashSet<>();
            // Adding the partition key for batches to disambiguate if the conditions span multipe rows (we don't add them outside
            // of batches for compatibility sakes).
            if (isBatch)
            {
                defs.addAll(cfm.partitionKeyColumns());
                defs.addAll(cfm.clusteringColumns());
            }
            for (ColumnDefinition def : columnsWithConditions)
                defs.add(def);
            selection = Selection.forColumns(cfm, new ArrayList<>(defs));

        }

        Selection.ResultSetBuilder builder = selection.resultSetBuilder(false);
        SelectStatement.forSelection(cfm, selection).processPartition(partition, options, builder, FBUtilities.nowInSeconds());

        return builder.build(options.getProtocolVersion());
    }

    public ResultMessage executeInternal(QueryState queryState, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        return hasConditions()
               ? executeInternalWithCondition(queryState, options)
               : executeInternalWithoutCondition(queryState, options);
    }

    public ResultMessage executeInternalWithoutCondition(QueryState queryState, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        for (IMutation mutation : getMutations(options, true, queryState.getTimestamp()))
        {
            assert mutation instanceof Mutation || mutation instanceof CounterMutation;

            if (mutation instanceof Mutation)
                ((Mutation) mutation).apply();
            else if (mutation instanceof CounterMutation)
                ((CounterMutation) mutation).apply();
        }
        return null;
    }

    public ResultMessage executeInternalWithCondition(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        CQL3CasRequest request = makeCasRequest(state, options);
        try (RowIterator result = casInternal(request, state))
        {
            return new ResultMessage.Rows(buildCasResultSet(result, options));
        }
    }

    static RowIterator casInternal(CQL3CasRequest request, QueryState state)
    {
        UUID ballot = UUIDGen.getTimeUUIDFromMicros(state.getTimestamp());

        SinglePartitionReadCommand readCommand = request.readCommand(FBUtilities.nowInSeconds());
        FilteredPartition current;
        try (ReadOrderGroup orderGroup = readCommand.startOrderGroup(); PartitionIterator iter = readCommand.executeInternal(orderGroup))
        {
            current = FilteredPartition.create(PartitionIterators.getOnlyElement(iter, readCommand));
        }

        if (!request.appliesTo(current))
            return current.rowIterator();

        PartitionUpdate updates = request.makeUpdates(current);
        updates = TriggerExecutor.instance.execute(updates);

        Commit proposal = Commit.newProposal(ballot, updates);
        proposal.makeMutation().apply();
        return null;
    }

    /**
     * Convert statement into a list of mutations to apply on the server
     *
     * @param options value for prepared statement markers
     * @param local if true, any requests (for collections) performed by getMutation should be done locally only.
     * @param now the current timestamp in microseconds to use if no timestamp is user provided.
     *
     * @return list of the mutations
     */
    private Collection<? extends IMutation> getMutations(QueryOptions options, boolean local, long now)
    {
        UpdatesCollector collector = new UpdatesCollector(Collections.singletonMap(cfm.cfId, updatedColumns), 1);
        addUpdates(collector, options, local, now);
        collector.validateIndexedColumns();

        return collector.toMutations();
    }

    final void addUpdates(UpdatesCollector collector,
                          QueryOptions options,
                          boolean local,
                          long now)
    {
        List<ByteBuffer> keys = buildPartitionKeyNames(options);

        if (type.allowClusteringColumnSlices()
                && restrictions.hasClusteringColumnsRestriction()
                && restrictions.isColumnRange())
        {
            Slices slices = createSlice(options);

            // If all the ranges were invalid we do not need to do anything.
            if (slices.isEmpty())
                return;

            UpdateParameters params = makeUpdateParameters(keys,
                                                           new ClusteringIndexSliceFilter(slices, false),
                                                           options,
                                                           DataLimits.NONE,
                                                           local,
                                                           now);
            for (ByteBuffer key : keys)
            {
                ThriftValidation.validateKey(cfm, key);
                DecoratedKey dk = cfm.decorateKey(key);

                PartitionUpdate upd = collector.getPartitionUpdate(cfm, dk, options.getConsistency());

                for (Slice slice : slices)
                    addUpdateForKey(upd, slice, params);
            }
        }
        else
        {
            NavigableSet<Clustering> clusterings = createClustering(options);

            UpdateParameters params = makeUpdateParameters(keys, clusterings, options, local, now);

            for (ByteBuffer key : keys)
            {
                ThriftValidation.validateKey(cfm, key);
                DecoratedKey dk = cfm.decorateKey(key);

                PartitionUpdate upd = collector.getPartitionUpdate(cfm, dk, options.getConsistency());

                if (clusterings.isEmpty())
                {
                    addUpdateForKey(upd, Clustering.EMPTY, params);
                }
                else
                {
                    for (Clustering clustering : clusterings)
                    {
                       for (ByteBuffer c : clustering.getRawValues())
                       {
                           if (c != null && c.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
                               throw new InvalidRequestException(String.format("Key length of %d is longer than maximum of %d",
                                                                               clustering.dataSize(),
                                                                               FBUtilities.MAX_UNSIGNED_SHORT));
                       }

                        addUpdateForKey(upd, clustering, params);
                    }
                }
            }
        }
    }

    private Slices createSlice(QueryOptions options)
    {
        SortedSet<Slice.Bound> startBounds = restrictions.getClusteringColumnsBounds(Bound.START, options);
        SortedSet<Slice.Bound> endBounds = restrictions.getClusteringColumnsBounds(Bound.END, options);

        return toSlices(startBounds, endBounds);
    }

    private UpdateParameters makeUpdateParameters(Collection<ByteBuffer> keys,
                                                  NavigableSet<Clustering> clusterings,
                                                  QueryOptions options,
                                                  boolean local,
                                                  long now)
    {
        if (clusterings.contains(Clustering.STATIC_CLUSTERING))
            return makeUpdateParameters(keys,
                                        new ClusteringIndexSliceFilter(Slices.ALL, false),
                                        options,
                                        DataLimits.cqlLimits(1),
                                        local,
                                        now);

        return makeUpdateParameters(keys,
                                    new ClusteringIndexNamesFilter(clusterings, false),
                                    options,
                                    DataLimits.NONE,
                                    local,
                                    now);
    }

    private UpdateParameters makeUpdateParameters(Collection<ByteBuffer> keys,
                                                  ClusteringIndexFilter filter,
                                                  QueryOptions options,
                                                  DataLimits limits,
                                                  boolean local,
                                                  long now)
    {
        // Some lists operation requires reading
        Map<DecoratedKey, Partition> lists = readRequiredLists(keys, filter, limits, local, options.getConsistency());
        return new UpdateParameters(cfm, updatedColumns(), options, getTimestamp(now, options), getTimeToLive(options), lists);
    }

    private Slices toSlices(SortedSet<Slice.Bound> startBounds, SortedSet<Slice.Bound> endBounds)
    {
        assert startBounds.size() == endBounds.size();

        Slices.Builder builder = new Slices.Builder(cfm.comparator);

        Iterator<Slice.Bound> starts = startBounds.iterator();
        Iterator<Slice.Bound> ends = endBounds.iterator();

        while (starts.hasNext())
        {
            Slice slice = Slice.make(starts.next(), ends.next());
            if (!slice.isEmpty(cfm.comparator))
            {
                builder.add(slice);
            }
        }

        return builder.build();
    }

    public static abstract class Parsed extends CFStatement
    {
        protected final StatementType type;
        private final Attributes.Raw attrs;
        private final List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions;
        private final boolean ifNotExists;
        private final boolean ifExists;

        protected Parsed(CFName name,
                         StatementType type,
                         Attributes.Raw attrs,
                         List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions,
                         boolean ifNotExists,
                         boolean ifExists)
        {
            super(name);
            this.type = type;
            this.attrs = attrs;
            this.conditions = conditions == null ? Collections.<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>>emptyList() : conditions;
            this.ifNotExists = ifNotExists;
            this.ifExists = ifExists;
        }

        public ParsedStatement.Prepared prepare()
        {
            VariableSpecifications boundNames = getBoundVariables();
            ModificationStatement statement = prepare(boundNames);
            CFMetaData cfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
            return new ParsedStatement.Prepared(statement, boundNames, boundNames.getPartitionKeyBindIndexes(cfm));
        }

        public ModificationStatement prepare(VariableSpecifications boundNames)
        {
            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());

            Attributes preparedAttributes = attrs.prepare(keyspace(), columnFamily());
            preparedAttributes.collectMarkerSpecification(boundNames);

            Conditions preparedConditions = prepareConditions(metadata, boundNames);

            return prepareInternal(metadata,
                                   boundNames,
                                   preparedConditions,
                                   preparedAttributes);
        }

        /**
         * Returns the column conditions.
         *
         * @param metadata the column family meta data
         * @param boundNames the bound names
         * @return the column conditions.
         */
        private Conditions prepareConditions(CFMetaData metadata, VariableSpecifications boundNames)
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

            return prepareColumnConditions(metadata, boundNames);
        }

        /**
         * Returns the column conditions.
         *
         * @param metadata the column family meta data
         * @param boundNames the bound names
         * @return the column conditions.
         */
        private ColumnConditions prepareColumnConditions(CFMetaData metadata, VariableSpecifications boundNames)
        {
            checkNull(attrs.timestamp, "Cannot provide custom timestamp for conditional updates");

            ColumnConditions.Builder builder = ColumnConditions.newBuilder();

            for (Pair<ColumnIdentifier.Raw, ColumnCondition.Raw> entry : conditions)
            {
                ColumnIdentifier id = entry.left.prepare(metadata);
                ColumnDefinition def = metadata.getColumnDefinition(id);
                checkNotNull(metadata.getColumnDefinition(id), "Unknown identifier %s in IF conditions", id);

                ColumnCondition condition = entry.right.prepare(keyspace(), def);
                condition.collectMarkerSpecification(boundNames);

                checkFalse(def.isPrimaryKeyColumn(), "PRIMARY KEY column '%s' cannot have IF conditions", id);
                builder.add(condition);
            }
            return builder.build();
        }

        protected abstract ModificationStatement prepareInternal(CFMetaData cfm,
                                                                 VariableSpecifications boundNames,
                                                                 Conditions conditions,
                                                                 Attributes attrs);

        /**
         * Creates the restrictions.
         *
         * @param cfm the column family meta data
         * @param boundNames the bound names
         * @param operations the column operations
         * @param where the where clause
         * @param conditions the conditions
         * @return the restrictions
         */
        protected StatementRestrictions newRestrictions(CFMetaData cfm,
                                                        VariableSpecifications boundNames,
                                                        Operations operations,
                                                        WhereClause where,
                                                        Conditions conditions)
        {
            if (where.containsCustomExpressions())
                throw new InvalidRequestException(CUSTOM_EXPRESSIONS_NOT_ALLOWED);

            boolean applyOnlyToStaticColumns = appliesOnlyToStaticColumns(operations, conditions);
            return new StatementRestrictions(type, cfm, where, boundNames, applyOnlyToStaticColumns, false, false, false);
        }

        /**
         * Retrieves the <code>ColumnDefinition</code> corresponding to the specified raw <code>ColumnIdentifier</code>.
         *
         * @param cfm the column family meta data
         * @param rawId the raw <code>ColumnIdentifier</code>
         * @return the <code>ColumnDefinition</code> corresponding to the specified raw <code>ColumnIdentifier</code>
         */
        protected static ColumnDefinition getColumnDefinition(CFMetaData cfm, Raw rawId)
        {
            ColumnIdentifier id = rawId.prepare(cfm);
            return checkNotNull(cfm.getColumnDefinition(id), "Unknown identifier %s", id);
        }
    }
}
