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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;

/**
 * A <code>BATCH</code> statement parsed from a CQL query.
 */
public class BatchStatement implements CQLStatement
{
    public static enum Type
    {
        LOGGED, UNLOGGED, COUNTER
    }

    private final int boundTerms;
    public final Type type;
    private final List<ModificationStatement> statements;
    private final PartitionColumns updatedColumns;
    private final PartitionColumns conditionColumns;
    private final boolean updatesRegularRows;
    private final boolean updatesStaticRow;
    private final Attributes attrs;
    private final boolean hasConditions;
    private static final Logger logger = LoggerFactory.getLogger(BatchStatement.class);

    private static final String UNLOGGED_BATCH_WARNING = "Unlogged batch covering {} partition{} detected " +
                                                         "against table{} {}. You should use a logged batch for " +
                                                         "atomicity, or asynchronous writes for performance.";

    private static final String LOGGED_BATCH_LOW_GCGS_WARNING = "Executing a LOGGED BATCH on table{} {}, configured with a " +
                                                                "gc_grace_seconds of 0. The gc_grace_seconds is used to TTL " +
                                                                "batchlog entries, so setting gc_grace_seconds too low on " +
                                                                "tables involved in an atomic batch might cause batchlog " +
                                                                "entries to expire before being replayed.";

    /**
     * Creates a new BatchStatement from a list of statements and a
     * Thrift consistency level.
     *
     * @param type       type of the batch
     * @param statements a list of UpdateStatements
     * @param attrs      additional attributes for statement (CL, timestamp, timeToLive)
     */
    public BatchStatement(int boundTerms, Type type, List<ModificationStatement> statements, Attributes attrs)
    {
        this.boundTerms = boundTerms;
        this.type = type;
        this.statements = statements;
        this.attrs = attrs;

        boolean hasConditions = false;
        PartitionColumns.Builder regularBuilder = PartitionColumns.builder();
        PartitionColumns.Builder conditionBuilder = PartitionColumns.builder();
        boolean updateRegular = false;
        boolean updateStatic = false;

        for (ModificationStatement stmt : statements)
        {
            regularBuilder.addAll(stmt.updatedColumns());
            updateRegular |= stmt.updatesRegularRows();
            if (stmt.hasConditions())
            {
                hasConditions = true;
                conditionBuilder.addAll(stmt.conditionColumns());
                updateStatic |= stmt.updatesStaticRow();
            }
        }

        this.updatedColumns = regularBuilder.build();
        this.conditionColumns = conditionBuilder.build();
        this.updatesRegularRows = updateRegular;
        this.updatesStaticRow = updateStatic;
        this.hasConditions = hasConditions;
    }

    public Iterable<org.apache.cassandra.cql3.functions.Function> getFunctions()
    {
        Iterable<org.apache.cassandra.cql3.functions.Function> functions = attrs.getFunctions();
        for (ModificationStatement statement : statements)
            functions = Iterables.concat(functions, statement.getFunctions());
        return functions;
    }

    public int getBoundTerms()
    {
        return boundTerms;
    }

    public void checkAccess(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        for (ModificationStatement statement : statements)
            statement.checkAccess(state);
    }

    // Validates a prepared batch statement without validating its nested statements.
    public void validate() throws InvalidRequestException
    {
        if (attrs.isTimeToLiveSet())
            throw new InvalidRequestException("Global TTL on the BATCH statement is not supported.");

        boolean timestampSet = attrs.isTimestampSet();
        if (timestampSet)
        {
            if (hasConditions)
                throw new InvalidRequestException("Cannot provide custom timestamp for conditional BATCH");

            if (isCounter())
                throw new InvalidRequestException("Cannot provide custom timestamp for counter BATCH");
        }

        boolean hasCounters = false;
        boolean hasNonCounters = false;

        for (ModificationStatement statement : statements)
        {
            if (timestampSet && statement.isCounter())
                throw new InvalidRequestException("Cannot provide custom timestamp for a BATCH containing counters");

            if (timestampSet && statement.isTimestampSet())
                throw new InvalidRequestException("Timestamp must be set either on BATCH or individual statements");

            if (isCounter() && !statement.isCounter())
                throw new InvalidRequestException("Cannot include non-counter statement in a counter batch");

            if (isLogged() && statement.isCounter())
                throw new InvalidRequestException("Cannot include a counter statement in a logged batch");

            if (statement.isCounter())
                hasCounters = true;
            else
                hasNonCounters = true;
        }

        if (hasCounters && hasNonCounters)
            throw new InvalidRequestException("Counter and non-counter mutations cannot exist in the same batch");

        if (hasConditions)
        {
            String ksName = null;
            String cfName = null;
            for (ModificationStatement stmt : statements)
            {
                if (ksName != null && (!stmt.keyspace().equals(ksName) || !stmt.columnFamily().equals(cfName)))
                    throw new InvalidRequestException("Batch with conditions cannot span multiple tables");
                ksName = stmt.keyspace();
                cfName = stmt.columnFamily();
            }
        }
    }

    private boolean isCounter()
    {
        return type == Type.COUNTER;
    }

    private boolean isLogged()
    {
        return type == Type.LOGGED;
    }

    // The batch itself will be validated in either Parsed#prepare() - for regular CQL3 batches,
    //   or in QueryProcessor.processBatch() - for native protocol batches.
    public void validate(ClientState state) throws InvalidRequestException
    {
        for (ModificationStatement statement : statements)
            statement.validate(state);
    }

    public List<ModificationStatement> getStatements()
    {
        return statements;
    }

    private Collection<? extends IMutation> getMutations(BatchQueryOptions options, boolean local, long now)
    throws RequestExecutionException, RequestValidationException
    {
        Set<String> tablesWithZeroGcGs = null;

        Map<String, Map<ByteBuffer, IMutation>> mutations = new HashMap<>();
        for (int i = 0; i < statements.size(); i++)
        {
            ModificationStatement statement = statements.get(i);
            if (isLogged() && statement.cfm.params.gcGraceSeconds == 0)
            {
                if (tablesWithZeroGcGs == null)
                    tablesWithZeroGcGs = new HashSet<>();
                tablesWithZeroGcGs.add(String.format("%s.%s", statement.cfm.ksName, statement.cfm.cfName));
            }
            QueryOptions statementOptions = options.forStatement(i);
            long timestamp = attrs.getTimestamp(now, statementOptions);
            addStatementMutations(statement, statementOptions, local, timestamp, mutations);
        }

        if (tablesWithZeroGcGs != null)
        {
            String suffix = tablesWithZeroGcGs.size() == 1 ? "" : "s";
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES, LOGGED_BATCH_LOW_GCGS_WARNING,
                             suffix, tablesWithZeroGcGs);
            ClientWarn.warn(MessageFormatter.arrayFormat(LOGGED_BATCH_LOW_GCGS_WARNING, new Object[] { suffix, tablesWithZeroGcGs })
                                            .getMessage());
        }

        return unzipMutations(mutations);
    }

    private Collection<? extends IMutation> unzipMutations(Map<String, Map<ByteBuffer, IMutation>> mutations)
    {

        // The case where all statement where on the same keyspace is pretty common
        if (mutations.size() == 1)
            return mutations.values().iterator().next().values();


        List<IMutation> ms = new ArrayList<>();
        for (Map<ByteBuffer, IMutation> ksMap : mutations.values())
            ms.addAll(ksMap.values());

        return ms;
    }

    private PartitionColumns updatedColumns()
    {
        return updatedColumns;
    }

    private int updatedRows()
    {
        // Note: it's possible for 2 statements to actually apply to the same row, but that's just an estimation
        // for sizing our PartitionUpdate backing array, so it's good enough.
        return statements.size();
    }

    private void addStatementMutations(ModificationStatement statement,
                                       QueryOptions options,
                                       boolean local,
                                       long now,
                                       Map<String, Map<ByteBuffer, IMutation>> mutations)
    throws RequestExecutionException, RequestValidationException
    {
        String ksName = statement.keyspace();
        Map<ByteBuffer, IMutation> ksMap = mutations.get(ksName);
        if (ksMap == null)
        {
            ksMap = new HashMap<>();
            mutations.put(ksName, ksMap);
        }

        // The following does the same than statement.getMutations(), but we inline it here because
        // we don't want to recreate mutations every time as this is particularly inefficient when applying
        // multiple batch to the same partition (see #6737).
        List<ByteBuffer> keys = statement.buildPartitionKeyNames(options);
        CBuilder clustering = statement.createClustering(options);
        UpdateParameters params = statement.makeUpdateParameters(keys, clustering, options, local, now);

        for (ByteBuffer key : keys)
        {
            DecoratedKey dk = statement.cfm.decorateKey(key);
            IMutation mutation = ksMap.get(dk.getKey());
            Mutation mut;
            if (mutation == null)
            {
                mut = new Mutation(ksName, dk);
                mutation = statement.cfm.isCounter() ? new CounterMutation(mut, options.getConsistency()) : mut;
                ksMap.put(dk.getKey(), mutation);
            }
            else
            {
                mut = statement.cfm.isCounter() ? ((CounterMutation) mutation).getMutation() : (Mutation) mutation;
            }

            PartitionUpdate upd = mut.get(statement.cfm);
            if (upd == null)
            {
                upd = new PartitionUpdate(statement.cfm, dk, updatedColumns(), updatedRows());
                mut.add(upd);
            }

            statement.addUpdateForKey(upd, clustering, params);
        }
    }

    /**
     * Checks batch size to ensure threshold is met. If not, a warning is logged.
     *
     * @param cfs ColumnFamilies that will store the batch's mutations.
     */
    public static void verifyBatchSize(Iterable<PartitionUpdate> updates) throws InvalidRequestException
    {
        long size = 0;
        long warnThreshold = DatabaseDescriptor.getBatchSizeWarnThreshold();
        long failThreshold = DatabaseDescriptor.getBatchSizeFailThreshold();

        for (PartitionUpdate update : updates)
            size += update.dataSize();

        if (size > warnThreshold)
        {
            Set<String> tableNames = new HashSet<>();
            for (PartitionUpdate update : updates)
                tableNames.add(String.format("%s.%s", update.metadata().ksName, update.metadata().cfName));

            String format = "Batch of prepared statements for {} is of size {}, exceeding specified threshold of {} by {}.{}";
            if (size > failThreshold)
            {
                Tracing.trace(format, tableNames, size, failThreshold, size - failThreshold, " (see batch_size_fail_threshold_in_kb)");
                logger.error(format, tableNames, size, failThreshold, size - failThreshold, " (see batch_size_fail_threshold_in_kb)");
                throw new InvalidRequestException("Batch too large");
            }
            else if (logger.isWarnEnabled())
            {
                logger.warn(format, tableNames, size, warnThreshold, size - warnThreshold, "");
            }
            ClientWarn.warn(MessageFormatter.arrayFormat(format, new Object[] {tableNames, size, warnThreshold, size - warnThreshold, ""}).getMessage());
        }
    }

    private void verifyBatchType(Iterable<PartitionUpdate> updates)
    {
        if (!isLogged() && Iterables.size(updates) > 1)
        {
            Set<DecoratedKey> keySet = new HashSet<>();
            Set<String> tableNames = new HashSet<>();

            for (PartitionUpdate update : updates)
            {
                keySet.add(update.partitionKey());
                tableNames.add(String.format("%s.%s", update.metadata().ksName, update.metadata().cfName));
            }

            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES, UNLOGGED_BATCH_WARNING,
                             keySet.size(), keySet.size() == 1 ? "" : "s",
                             tableNames.size() == 1 ? "" : "s", tableNames);

            ClientWarn.warn(MessageFormatter.arrayFormat(UNLOGGED_BATCH_WARNING, new Object[]{keySet.size(), keySet.size() == 1 ? "" : "s",
                                                    tableNames.size() == 1 ? "" : "s", tableNames}).getMessage());

        }
    }

    public ResultMessage execute(QueryState queryState, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        return execute(queryState, BatchQueryOptions.withoutPerStatementVariables(options));
    }

    public ResultMessage execute(QueryState queryState, BatchQueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        return execute(queryState, options, false, options.getTimestamp(queryState));
    }

    private ResultMessage execute(QueryState queryState, BatchQueryOptions options, boolean local, long now)
    throws RequestExecutionException, RequestValidationException
    {
        if (options.getConsistency() == null)
            throw new InvalidRequestException("Invalid empty consistency level");
        if (options.getSerialConsistency() == null)
            throw new InvalidRequestException("Invalid empty serial consistency level");

        if (hasConditions)
            return executeWithConditions(options, queryState);

        executeWithoutConditions(getMutations(options, local, now), options.getConsistency());
        return new ResultMessage.Void();
    }

    private void executeWithoutConditions(Collection<? extends IMutation> mutations, ConsistencyLevel cl) throws RequestExecutionException, RequestValidationException
    {
        // Extract each collection of updates from it's IMutation and then lazily concatenate all of them into a single Iterable.
        Iterable<PartitionUpdate> updates = Iterables.concat(Iterables.transform(mutations, new Function<IMutation, Collection<PartitionUpdate>>()
        {
            public Collection<PartitionUpdate> apply(IMutation im)
            {
                return im.getPartitionUpdates();
            }
        }));

        verifyBatchSize(updates);
        verifyBatchType(updates);

        boolean mutateAtomic = (isLogged() && mutations.size() > 1);
        StorageProxy.mutateWithTriggers(mutations, cl, mutateAtomic);
    }

    private ResultMessage executeWithConditions(BatchQueryOptions options, QueryState state)
    throws RequestExecutionException, RequestValidationException
    {
        Pair<CQL3CasRequest, Set<ColumnDefinition>> p = makeCasRequest(options, state);
        CQL3CasRequest casRequest = p.left;
        Set<ColumnDefinition> columnsWithConditions = p.right;

        String ksName = casRequest.cfm.ksName;
        String tableName = casRequest.cfm.cfName;

        try (RowIterator result = StorageProxy.cas(ksName,
                                                   tableName,
                                                   casRequest.key,
                                                   casRequest,
                                                   options.getSerialConsistency(),
                                                   options.getConsistency(),
                                                   state.getClientState()))
        {
            return new ResultMessage.Rows(ModificationStatement.buildCasResultSet(ksName, tableName, result, columnsWithConditions, true, options.forStatement(0)));
        }
    }


    private Pair<CQL3CasRequest,Set<ColumnDefinition>> makeCasRequest(BatchQueryOptions options, QueryState state)
    {
        long now = state.getTimestamp();
        DecoratedKey key = null;
        CQL3CasRequest casRequest = null;
        Set<ColumnDefinition> columnsWithConditions = new LinkedHashSet<>();

        for (int i = 0; i < statements.size(); i++)
        {
            ModificationStatement statement = statements.get(i);
            QueryOptions statementOptions = options.forStatement(i);
            long timestamp = attrs.getTimestamp(now, statementOptions);
            List<ByteBuffer> pks = statement.buildPartitionKeyNames(statementOptions);
            if (pks.size() > 1)
                throw new IllegalArgumentException("Batch with conditions cannot span multiple partitions (you cannot use IN on the partition key)");
            if (key == null)
            {
                key = statement.cfm.decorateKey(pks.get(0));
                casRequest = new CQL3CasRequest(statement.cfm, key, true, conditionColumns, updatesRegularRows, updatesStaticRow);
            }
            else if (!key.getKey().equals(pks.get(0)))
            {
                throw new InvalidRequestException("Batch with conditions cannot span multiple partitions");
            }

            CBuilder cbuilder = statement.createClustering(statementOptions);
            if (statement.hasConditions())
            {
                statement.addConditions(cbuilder.build(), casRequest, statementOptions);
                // As soon as we have a ifNotExists, we set columnsWithConditions to null so that everything is in the resultSet
                if (statement.hasIfNotExistCondition() || statement.hasIfExistCondition())
                    columnsWithConditions = null;
                else if (columnsWithConditions != null)
                    Iterables.addAll(columnsWithConditions, statement.getColumnsWithConditions());
            }
            casRequest.addRowUpdate(cbuilder, statement, statementOptions, timestamp);
        }

        return Pair.create(casRequest, columnsWithConditions);
    }

    public ResultMessage executeInternal(QueryState queryState, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        if (hasConditions)
            return executeInternalWithConditions(BatchQueryOptions.withoutPerStatementVariables(options), queryState);

        executeInternalWithoutCondition(queryState, options);
        return new ResultMessage.Void();
    }

    private ResultMessage executeInternalWithoutCondition(QueryState queryState, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        for (IMutation mutation : getMutations(BatchQueryOptions.withoutPerStatementVariables(options), true, queryState.getTimestamp()))
        {
            assert mutation instanceof Mutation || mutation instanceof CounterMutation;

            if (mutation instanceof Mutation)
                ((Mutation) mutation).apply();
            else if (mutation instanceof CounterMutation)
                ((CounterMutation) mutation).apply();
        }
        return null;
    }

    private ResultMessage executeInternalWithConditions(BatchQueryOptions options, QueryState state) throws RequestExecutionException, RequestValidationException
    {
        Pair<CQL3CasRequest, Set<ColumnDefinition>> p = makeCasRequest(options, state);
        CQL3CasRequest request = p.left;
        Set<ColumnDefinition> columnsWithConditions = p.right;

        String ksName = request.cfm.ksName;
        String tableName = request.cfm.cfName;

        try (RowIterator result = ModificationStatement.casInternal(request, state))
        {
            return new ResultMessage.Rows(ModificationStatement.buildCasResultSet(ksName, tableName, result, columnsWithConditions, true, options.forStatement(0)));
        }
    }

    public String toString()
    {
        return String.format("BatchStatement(type=%s, statements=%s)", type, statements);
    }

    public static class Parsed extends CFStatement
    {
        private final Type type;
        private final Attributes.Raw attrs;
        private final List<ModificationStatement.Parsed> parsedStatements;

        public Parsed(Type type, Attributes.Raw attrs, List<ModificationStatement.Parsed> parsedStatements)
        {
            super(null);
            this.type = type;
            this.attrs = attrs;
            this.parsedStatements = parsedStatements;
        }

        @Override
        public void prepareKeyspace(ClientState state) throws InvalidRequestException
        {
            for (ModificationStatement.Parsed statement : parsedStatements)
                statement.prepareKeyspace(state);
        }

        public ParsedStatement.Prepared prepare() throws InvalidRequestException
        {
            VariableSpecifications boundNames = getBoundVariables();

            String firstKS = null;
            String firstCF = null;
            boolean haveMultipleCFs = false;

            List<ModificationStatement> statements = new ArrayList<>(parsedStatements.size());
            for (ModificationStatement.Parsed parsed : parsedStatements)
            {
                if (firstKS == null)
                {
                    firstKS = parsed.keyspace();
                    firstCF = parsed.columnFamily();
                }
                else if (!haveMultipleCFs)
                {
                    haveMultipleCFs = !firstKS.equals(parsed.keyspace()) || !firstCF.equals(parsed.columnFamily());
                }

                statements.add(parsed.prepare(boundNames));
            }

            Attributes prepAttrs = attrs.prepare("[batch]", "[batch]");
            prepAttrs.collectMarkerSpecification(boundNames);

            BatchStatement batchStatement = new BatchStatement(boundNames.size(), type, statements, prepAttrs);
            batchStatement.validate();

            // Use the CFMetadata of the first statement for partition key bind indexes.  If the statements affect
            // multiple tables, we won't send partition key bind indexes.
            Short[] partitionKeyBindIndexes = haveMultipleCFs ? null
                                                              : boundNames.getPartitionKeyBindIndexes(batchStatement.statements.get(0).cfm);

            return new ParsedStatement.Prepared(batchStatement, boundNames, partitionKeyBindIndexes);
        }
    }
}
