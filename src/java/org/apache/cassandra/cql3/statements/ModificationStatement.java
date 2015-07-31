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
import org.apache.cassandra.config.MaterializedViewDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/*
 * Abstract parent class of individual modifications, i.e. INSERT, UPDATE and DELETE.
 */
public abstract class ModificationStatement implements CQLStatement
{
    protected static final Logger logger = LoggerFactory.getLogger(ModificationStatement.class);

    private static final ColumnIdentifier CAS_RESULT_COLUMN = new ColumnIdentifier("[applied]", false);

    public static enum StatementType { INSERT, UPDATE, DELETE }
    public final StatementType type;

    private final int boundTerms;
    public final CFMetaData cfm;
    public final Attributes attrs;

    protected final Map<ColumnIdentifier, Restriction> processedKeys = new HashMap<>();
    private final List<Operation> regularOperations = new ArrayList<>();
    private final List<Operation> staticOperations = new ArrayList<>();

    // TODO: If we had a builder for this statement, we could have updatedColumns/conditionColumns final and only have
    // updatedColumnsBuilder/conditionColumnsBuilder in the builder ...
    private PartitionColumns updatedColumns;
    private PartitionColumns.Builder updatedColumnsBuilder = PartitionColumns.builder();
    private PartitionColumns conditionColumns;
    private PartitionColumns.Builder conditionColumnsBuilder = PartitionColumns.builder();

    // Separating normal and static conditions makes things somewhat easier
    private List<ColumnCondition> columnConditions;
    private List<ColumnCondition> staticConditions;
    private boolean ifNotExists;
    private boolean ifExists;

    private boolean hasNoClusteringColumns = true;

    private boolean setsStaticColumns;
    private boolean setsRegularColumns;

    private final com.google.common.base.Function<ColumnCondition, ColumnDefinition> getColumnForCondition =
      new com.google.common.base.Function<ColumnCondition, ColumnDefinition>()
    {
        public ColumnDefinition apply(ColumnCondition cond)
        {
            return cond.column;
        }
    };

    public ModificationStatement(StatementType type, int boundTerms, CFMetaData cfm, Attributes attrs)
    {
        this.type = type;
        this.boundTerms = boundTerms;
        this.cfm = cfm;
        this.attrs = attrs;
    }

    public Iterable<Function> getFunctions()
    {
        Iterable<Function> functions = attrs.getFunctions();

        for (Restriction restriction : processedKeys.values())
            functions = Iterables.concat(functions, restriction.getFunctions());

        for (Operation operation : allOperations())
            functions = Iterables.concat(functions, operation.getFunctions());

        for (ColumnCondition condition : allConditions())
            functions = Iterables.concat(functions, condition.getFunctions());

        return functions;
    }

    public boolean hasNoClusteringColumns()
    {
        return hasNoClusteringColumns;
    }

    public abstract boolean requireFullClusteringKey();
    public abstract void addUpdateForKey(PartitionUpdate update, CBuilder clusteringBuilder, UpdateParameters params) throws InvalidRequestException;

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

    public boolean isMaterializedView()
    {
        return cfm.isMaterializedView();
    }

    public boolean hasMaterializedViews()
    {
        return !cfm.getMaterializedViews().isEmpty();
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

        // MV updates need to get the current state from the table, and might update the materialized views
        // Require Permission.SELECT on the base table, and Permission.MODIFY on the views
        if (hasMaterializedViews())
        {
            state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.SELECT);
            for (MaterializedViewDefinition view : cfm.getMaterializedViews())
                state.hasColumnFamilyAccess(keyspace(), view.viewName, Permission.MODIFY);
        }

        for (Function function : getFunctions())
            state.ensureHasPermission(Permission.EXECUTE, function);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (hasConditions() && attrs.isTimestampSet())
            throw new InvalidRequestException("Cannot provide custom timestamp for conditional updates");

        if (isCounter() && attrs.isTimestampSet())
            throw new InvalidRequestException("Cannot provide custom timestamp for counter updates");

        if (isCounter() && attrs.isTimeToLiveSet())
            throw new InvalidRequestException("Cannot provide custom TTL for counter updates");

        if (isMaterializedView())
            throw new InvalidRequestException("Cannot directly modify a materialized view");
    }

    public void addOperation(Operation op)
    {
        updatedColumnsBuilder.add(op.column);
        // If the operation requires a read-before-write and we're doing a conditional read, we want to read
        // the affected column as part of the read-for-conditions paxos phase (see #7499).
        if (op.requiresRead())
            conditionColumnsBuilder.add(op.column);

        if (op.column.isStatic())
        {
            setsStaticColumns = true;
            staticOperations.add(op);
        }
        else
        {
            setsRegularColumns = true;
            regularOperations.add(op);
        }
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
        return cfm.clusteringColumns().isEmpty() || !hasNoClusteringColumns;
    }

    public boolean updatesStaticRow()
    {
        return !staticOperations.isEmpty();
    }

    private void finishPreparation()
    {
        updatedColumns = updatedColumnsBuilder.build();
        // Compact tables have not row marker. So if we don't actually update any particular column,
        // this means that we're only updating the PK, which we allow if only those were declared in
        // the definition. In that case however, we do went to write the compactValueColumn (since again
        // we can't use a "row marker") so add it automatically.
        if (cfm.isCompactTable() && updatedColumns.isEmpty() && updatesRegularRows())
            updatedColumns = cfm.partitionColumns();

        conditionColumns = conditionColumnsBuilder.build();
    }

    public List<Operation> getRegularOperations()
    {
        return regularOperations;
    }

    public List<Operation> getStaticOperations()
    {
        return staticOperations;
    }

    public Iterable<Operation> allOperations()
    {
        return Iterables.concat(staticOperations, regularOperations);
    }

    public Iterable<ColumnDefinition> getColumnsWithConditions()
    {
        if (ifNotExists || ifExists)
            return null;

        return Iterables.concat(columnConditions == null ? Collections.<ColumnDefinition>emptyList() : Iterables.transform(columnConditions, getColumnForCondition),
                                staticConditions == null ? Collections.<ColumnDefinition>emptyList() : Iterables.transform(staticConditions, getColumnForCondition));
    }

    public Iterable<ColumnCondition> allConditions()
    {
        if (staticConditions == null)
            return columnConditions == null ? Collections.<ColumnCondition>emptySet(): columnConditions;
        if (columnConditions == null)
            return staticConditions;
        return Iterables.concat(staticConditions, columnConditions);
    }

    public void addCondition(ColumnCondition cond)
    {
        conditionColumnsBuilder.add(cond.column);

        List<ColumnCondition> conds = null;
        if (cond.column.isStatic())
        {
            setsStaticColumns = true;
            if (staticConditions == null)
                staticConditions = new ArrayList<ColumnCondition>();
            conds = staticConditions;
        }
        else
        {
            setsRegularColumns = true;
            if (columnConditions == null)
                columnConditions = new ArrayList<ColumnCondition>();
            conds = columnConditions;
        }
        conds.add(cond);
    }

    public void setIfNotExistCondition()
    {
        ifNotExists = true;
    }

    public boolean hasIfNotExistCondition()
    {
        return ifNotExists;
    }

    public void setIfExistCondition()
    {
        ifExists = true;
    }

    public boolean hasIfExistCondition()
    {
        return ifExists;
    }

    private void addKeyValues(ColumnDefinition def, Restriction values) throws InvalidRequestException
    {
        if (def.kind == ColumnDefinition.Kind.CLUSTERING)
            hasNoClusteringColumns = false;
        if (processedKeys.put(def.name, values) != null)
            throw new InvalidRequestException(String.format("Multiple definitions found for PRIMARY KEY part %s", def.name));
    }

    public void addKeyValue(ColumnDefinition def, Term value) throws InvalidRequestException
    {
        addKeyValues(def, new SingleColumnRestriction.EQRestriction(def, value));
    }

    public void processWhereClause(List<Relation> whereClause, VariableSpecifications names) throws InvalidRequestException
    {
        for (Relation relation : whereClause)
        {
            if (relation.isMultiColumn())
            {
                throw new InvalidRequestException(
                        String.format("Multi-column relations cannot be used in WHERE clauses for UPDATE and DELETE statements: %s", relation));
            }
            SingleColumnRelation rel = (SingleColumnRelation) relation;

            if (rel.onToken())
                throw new InvalidRequestException(String.format("The token function cannot be used in WHERE clauses for UPDATE and DELETE statements: %s", relation));

            ColumnIdentifier id = rel.getEntity().prepare(cfm);
            ColumnDefinition def = cfm.getColumnDefinition(id);
            if (def == null)
                throw new InvalidRequestException(String.format("Unknown key identifier %s", id));

            switch (def.kind)
            {
                case PARTITION_KEY:
                case CLUSTERING:
                    Restriction restriction;

                    if (rel.isEQ() || (def.isPartitionKey() && rel.isIN()))
                    {
                        restriction = rel.toRestriction(cfm, names);
                    }
                    else
                    {
                        throw new InvalidRequestException(String.format("Invalid operator %s for PRIMARY KEY part %s", rel.operator(), def.name));
                    }

                    addKeyValues(def, restriction);
                    break;
                default:
                    throw new InvalidRequestException(String.format("Non PRIMARY KEY %s found in where clause", def.name));
            }
        }
    }

    public List<ByteBuffer> buildPartitionKeyNames(QueryOptions options)
    throws InvalidRequestException
    {
        MultiCBuilder keyBuilder = MultiCBuilder.create(cfm.getKeyValidatorAsClusteringComparator());
        for (ColumnDefinition def : cfm.partitionKeyColumns())
        {
            Restriction r = checkNotNull(processedKeys.get(def.name), "Missing mandatory PRIMARY KEY part %s", def.name);
            r.appendTo(keyBuilder, options);
        }

        NavigableSet<Clustering> clusterings = keyBuilder.build();
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>(clusterings.size());
        for (Clustering clustering : clusterings)
        {
            ByteBuffer key = CFMetaData.serializePartitionKey(clustering);
            ThriftValidation.validateKey(cfm, key);
            keys.add(key);
        }
        return keys;
    }

    public CBuilder createClustering(QueryOptions options)
    throws InvalidRequestException
    {
        // If the only updated/deleted columns are static, then we don't need clustering columns.
        // And in fact, unless it is an INSERT, we reject if clustering colums are provided as that
        // suggest something unintended. For instance, given:
        //   CREATE TABLE t (k int, v int, s int static, PRIMARY KEY (k, v))
        // it can make sense to do:
        //   INSERT INTO t(k, v, s) VALUES (0, 1, 2)
        // but both
        //   UPDATE t SET s = 3 WHERE k = 0 AND v = 1
        //   DELETE v FROM t WHERE k = 0 AND v = 1
        // sounds like you don't really understand what your are doing.
        if (setsStaticColumns && !setsRegularColumns)
        {
            // If we set no non-static columns, then it's fine not to have clustering columns
            if (hasNoClusteringColumns)
                return CBuilder.STATIC_BUILDER;

            // If we do have clustering columns however, then either it's an INSERT and the query is valid
            // but we still need to build a proper prefix, or it's not an INSERT, and then we want to reject
            // (see above)
            if (type != StatementType.INSERT)
            {
                for (ColumnDefinition def : cfm.clusteringColumns())
                    if (processedKeys.get(def.name) != null)
                        throw new InvalidRequestException(String.format("Invalid restriction on clustering column %s since the %s statement modifies only static columns", def.name, type));
                // we should get there as it contradicts hasNoClusteringColumns == false
                throw new AssertionError();
            }
        }

        return createClusteringInternal(options);
    }

    private CBuilder createClusteringInternal(QueryOptions options)
    throws InvalidRequestException
    {
        CBuilder builder = CBuilder.create(cfm.comparator);
        MultiCBuilder multiBuilder = MultiCBuilder.wrap(builder);

        ColumnDefinition firstEmptyKey = null;
        for (ColumnDefinition def : cfm.clusteringColumns())
        {
            Restriction r = processedKeys.get(def.name);
            if (r == null)
            {
                firstEmptyKey = def;
                checkFalse(requireFullClusteringKey() && !cfm.isDense() && cfm.isCompound(),
                           "Missing mandatory PRIMARY KEY part %s", def.name);
            }
            else if (firstEmptyKey != null)
            {
                throw invalidRequest("Missing PRIMARY KEY part %s since %s is set", firstEmptyKey.name, def.name);
            }
            else
            {
                r.appendTo(multiBuilder, options);
            }
        }
        return builder;
    }

    protected ColumnDefinition getFirstEmptyKey()
    {
        for (ColumnDefinition def : cfm.clusteringColumns())
        {
            if (processedKeys.get(def.name) == null)
                return def;
        }
        return null;
    }

    public boolean requiresRead()
    {
        // Lists SET operation incurs a read.
        for (Operation op : allOperations())
            if (op.requiresRead())
                return true;

        return false;
    }

    protected Map<DecoratedKey, Partition> readRequiredLists(Collection<ByteBuffer> partitionKeys, CBuilder cbuilder, boolean local, ConsistencyLevel cl)
    throws RequestExecutionException, RequestValidationException
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

        // TODO: no point in recomputing that every time. Should move to preparation phase.
        PartitionColumns.Builder builder = PartitionColumns.builder();
        for (Operation op : allOperations())
            if (op.requiresRead())
                builder.add(op.column);

        PartitionColumns toRead = builder.build();

        NavigableSet<Clustering> clusterings = BTreeSet.of(cfm.comparator, cbuilder.build());
        List<SinglePartitionReadCommand<?>> commands = new ArrayList<>(partitionKeys.size());
        int nowInSec = FBUtilities.nowInSeconds();
        for (ByteBuffer key : partitionKeys)
            commands.add(new SinglePartitionNamesCommand(cfm,
                                                         nowInSec,
                                                         ColumnFilter.selection(toRead),
                                                         RowFilter.NONE,
                                                         DataLimits.NONE,
                                                         StorageService.getPartitioner().decorateKey(key),
                                                         new ClusteringIndexNamesFilter(clusterings, false)));

        Map<DecoratedKey, Partition> map = new HashMap();

        SinglePartitionReadCommand.Group group = new SinglePartitionReadCommand.Group(commands, DataLimits.NONE);

        if (local)
        {
            try (ReadOrderGroup orderGroup = group.startOrderGroup(); PartitionIterator iter = group.executeInternal(orderGroup))
            {
                return asMaterializedMap(iter);
            }
        }
        else
        {
            try (PartitionIterator iter = group.execute(cl, null))
            {
                return asMaterializedMap(iter);
            }
        }
    }

    private Map<DecoratedKey, Partition> asMaterializedMap(PartitionIterator iterator)
    {
        Map<DecoratedKey, Partition> map = new HashMap();
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
        return ifNotExists
            || ifExists
            || (columnConditions != null && !columnConditions.isEmpty())
            || (staticConditions != null && !staticConditions.isEmpty());
    }

    public ResultMessage execute(QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        if (options.getConsistency() == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        if (hasConditions() && options.getProtocolVersion() == 1)
            throw new InvalidRequestException("Conditional updates are not supported by the protocol version in use. You need to upgrade to a driver using the native protocol v2.");

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
        if (keys.size() > 1)
            throw new InvalidRequestException("IN on the partition key is not supported with conditional updates");

        DecoratedKey key = StorageService.getPartitioner().decorateKey(keys.get(0));
        long now = options.getTimestamp(queryState);
        CBuilder cbuilder = createClustering(options);

        CQL3CasRequest request = new CQL3CasRequest(cfm, key, false, conditionColumns(), updatesRegularRows(), updatesStaticRow());
        addConditions(cbuilder.build(), request, options);
        request.addRowUpdate(cbuilder, this, options, now);
        return request;
    }

    public void addConditions(Clustering clustering, CQL3CasRequest request, QueryOptions options) throws InvalidRequestException
    {
        if (ifNotExists)
        {
            // If we use ifNotExists, if the statement applies to any non static columns, then the condition is on the row of the non-static
            // columns and the prefix should be the clustering. But if only static columns are set, then the ifNotExists apply to the existence
            // of any static columns and we should use the prefix for the "static part" of the partition.
            request.addNotExist(clustering);
        }
        else if (ifExists)
        {
            request.addExist(clustering);
        }
        else
        {
            if (columnConditions != null)
                request.addConditions(clustering, columnConditions, options);
            if (staticConditions != null)
                request.addConditions(Clustering.STATIC_CLUSTERING, staticConditions, options);
        }
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
        CFMetaData metadata = Schema.instance.getCFMetaData(request.cfm.ksName, request.cfm.cfName);

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
     * @throws InvalidRequestException on invalid requests
     */
    private Collection<? extends IMutation> getMutations(QueryOptions options, boolean local, long now)
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> keys = buildPartitionKeyNames(options);
        CBuilder clustering = createClustering(options);

        UpdateParameters params = makeUpdateParameters(keys, clustering, options, local, now);

        Collection<IMutation> mutations = new ArrayList<IMutation>(keys.size());
        for (ByteBuffer key: keys)
        {
            ThriftValidation.validateKey(cfm, key);
            DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
            PartitionUpdate upd = new PartitionUpdate(cfm, dk, updatedColumns(), 1);
            addUpdateForKey(upd, clustering, params);
            Mutation mut = new Mutation(upd);

            mutations.add(isCounter() ? new CounterMutation(mut, options.getConsistency()) : mut);
        }
        return mutations;
    }

    public UpdateParameters makeUpdateParameters(Collection<ByteBuffer> keys,
                                                 CBuilder clustering,
                                                 QueryOptions options,
                                                 boolean local,
                                                 long now)
    throws RequestExecutionException, RequestValidationException
    {
        // Some lists operation requires reading
        Map<DecoratedKey, Partition> lists = readRequiredLists(keys, clustering, local, options.getConsistency());
        return new UpdateParameters(cfm, updatedColumns(), options, getTimestamp(now, options), getTimeToLive(options), lists, true);
    }

    /**
     * If there are conditions on the statement, this is called after the where clause and conditions have been
     * processed to check that they are compatible.
     * @throws InvalidRequestException
     */
    protected void validateWhereClauseForConditions() throws InvalidRequestException
    {
        //  no-op by default
    }

    public static abstract class Parsed extends CFStatement
    {
        protected final Attributes.Raw attrs;
        protected final List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions;
        private final boolean ifNotExists;
        private final boolean ifExists;

        protected Parsed(CFName name, Attributes.Raw attrs, List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions, boolean ifNotExists, boolean ifExists)
        {
            super(name);
            this.attrs = attrs;
            this.conditions = conditions == null ? Collections.<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>>emptyList() : conditions;
            this.ifNotExists = ifNotExists;
            this.ifExists = ifExists;
        }

        public ParsedStatement.Prepared prepare() throws InvalidRequestException
        {
            VariableSpecifications boundNames = getBoundVariables();
            ModificationStatement statement = prepare(boundNames);
            CFMetaData cfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
            return new ParsedStatement.Prepared(statement, boundNames, boundNames.getPartitionKeyBindIndexes(cfm));
        }

        public ModificationStatement prepare(VariableSpecifications boundNames) throws InvalidRequestException
        {
            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());

            Attributes preparedAttributes = attrs.prepare(keyspace(), columnFamily());
            preparedAttributes.collectMarkerSpecification(boundNames);

            ModificationStatement stmt = prepareInternal(metadata, boundNames, preparedAttributes);

            if (ifNotExists || ifExists || !conditions.isEmpty())
            {
                if (stmt.isCounter())
                    throw new InvalidRequestException("Conditional updates are not supported on counter tables");

                if (attrs.timestamp != null)
                    throw new InvalidRequestException("Cannot provide custom timestamp for conditional updates");

                if (ifNotExists)
                {
                    // To have both 'IF NOT EXISTS' and some other conditions doesn't make sense.
                    // So far this is enforced by the parser, but let's assert it for sanity if ever the parse changes.
                    assert conditions.isEmpty();
                    assert !ifExists;
                    stmt.setIfNotExistCondition();
                }
                else if (ifExists)
                {
                    assert conditions.isEmpty();
                    assert !ifNotExists;
                    stmt.setIfExistCondition();
                }
                else
                {
                    for (Pair<ColumnIdentifier.Raw, ColumnCondition.Raw> entry : conditions)
                    {
                        ColumnIdentifier id = entry.left.prepare(metadata);
                        ColumnDefinition def = metadata.getColumnDefinition(id);
                        if (def == null)
                            throw new InvalidRequestException(String.format("Unknown identifier %s", id));

                        ColumnCondition condition = entry.right.prepare(keyspace(), def);
                        condition.collectMarkerSpecification(boundNames);

                        switch (def.kind)
                        {
                            case PARTITION_KEY:
                            case CLUSTERING:
                                throw new InvalidRequestException(String.format("PRIMARY KEY column '%s' cannot have IF conditions", id));
                            default:
                                stmt.addCondition(condition);
                                break;
                        }
                    }
                }

                stmt.validateWhereClauseForConditions();
            }

            stmt.finishPreparation();
            return stmt;
        }

        protected abstract ModificationStatement prepareInternal(CFMetaData cfm, VariableSpecifications boundNames, Attributes attrs) throws InvalidRequestException;
    }
}
