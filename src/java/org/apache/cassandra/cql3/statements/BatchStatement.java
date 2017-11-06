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

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;

/**
 * A <code>BATCH</code> statement parsed from a CQL query.
 */
public class BatchStatement implements CQLStatement
{
    public enum Type
    {
        LOGGED, UNLOGGED, COUNTER
    }

    private final int boundTerms;
    public final Type type;
    private final List<ModificationStatement> statements;

    // Columns modified for each table (keyed by the table ID)
    private final Map<UUID, PartitionColumns> updatedColumns;
    // Columns on which there is conditions. Note that if there is any, then the batch can only be on a single partition (and thus table).
    private final PartitionColumns conditionColumns;

    private final boolean updatesRegularRows;
    private final boolean updatesStaticRow;
    private final Attributes attrs;
    private final boolean hasConditions;
    private static final Logger logger = LoggerFactory.getLogger(BatchStatement.class);

    private static final String UNLOGGED_BATCH_WARNING = "Unlogged batch covering {} partitions detected " +
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
        MultiTableColumnsBuilder regularBuilder = new MultiTableColumnsBuilder();
        PartitionColumns.Builder conditionBuilder = PartitionColumns.builder();
        boolean updateRegular = false;
        boolean updateStatic = false;

        for (ModificationStatement stmt : statements)
        {
            regularBuilder.addAll(stmt.cfm, stmt.updatedColumns());
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
        List<org.apache.cassandra.cql3.functions.Function> functions = new ArrayList<>();
        for (ModificationStatement statement : statements)
            statement.addFunctionsTo(functions);
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

    private Collection<? extends IMutation> getMutations(BatchQueryOptions options, boolean local, long now, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        Set<String> tablesWithZeroGcGs = null;
        UpdatesCollector collector = new UpdatesCollector(updatedColumns, updatedRows());
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
            statement.addUpdates(collector, statementOptions, local, timestamp, queryStartNanoTime);
        }

        if (tablesWithZeroGcGs != null)
        {
            String suffix = tablesWithZeroGcGs.size() == 1 ? "" : "s";
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES, LOGGED_BATCH_LOW_GCGS_WARNING,
                             suffix, tablesWithZeroGcGs);
            ClientWarn.instance.warn(MessageFormatter.arrayFormat(LOGGED_BATCH_LOW_GCGS_WARNING, new Object[] { suffix, tablesWithZeroGcGs })
                                                     .getMessage());
        }

        collector.validateIndexedColumns();
        return collector.toMutations();
    }

    private int updatedRows()
    {
        // Note: it's possible for 2 statements to actually apply to the same row, but that's just an estimation
        // for sizing our PartitionUpdate backing array, so it's good enough.
        return statements.size();
    }

    /**
     * Checks batch size to ensure threshold is met. If not, a warning is logged.
     *
     * @param updates - the batch mutations.
     */
    private static void verifyBatchSize(Collection<? extends IMutation> mutations) throws InvalidRequestException
    {
        // We only warn for batch spanning multiple mutations (#10876)
        if (mutations.size() <= 1)
            return;

        long size = 0;
        long warnThreshold = DatabaseDescriptor.getBatchSizeWarnThreshold();

        for (IMutation mutation : mutations)
        {
            for (PartitionUpdate update : mutation.getPartitionUpdates())
                size += update.dataSize();
        }

        if (size > warnThreshold)
        {
            Set<String> tableNames = new HashSet<>();
            for (IMutation mutation : mutations)
            {
                for (PartitionUpdate update : mutation.getPartitionUpdates())
                    tableNames.add(String.format("%s.%s", update.metadata().ksName, update.metadata().cfName));
            }

            long failThreshold = DatabaseDescriptor.getBatchSizeFailThreshold();

            String format = "Batch for {} is of size {}, exceeding specified threshold of {} by {}.{}";
            if (size > failThreshold)
            {
                Tracing.trace(format, tableNames, FBUtilities.prettyPrintMemory(size), FBUtilities.prettyPrintMemory(failThreshold),
                              FBUtilities.prettyPrintMemory(size - failThreshold), " (see batch_size_fail_threshold_in_kb)");
                logger.error(format, tableNames, FBUtilities.prettyPrintMemory(size), FBUtilities.prettyPrintMemory(failThreshold),
                             FBUtilities.prettyPrintMemory(size - failThreshold), " (see batch_size_fail_threshold_in_kb)");
                throw new InvalidRequestException("Batch too large");
            }
            else if (logger.isWarnEnabled())
            {
                logger.warn(format, tableNames, FBUtilities.prettyPrintMemory(size), FBUtilities.prettyPrintMemory(warnThreshold),
                            FBUtilities.prettyPrintMemory(size - warnThreshold), "");
            }
            ClientWarn.instance.warn(MessageFormatter.arrayFormat(format, new Object[] {tableNames, size, warnThreshold, size - warnThreshold, ""}).getMessage());
        }
    }

    private void verifyBatchType(Collection<? extends IMutation> mutations)
    {
        if (!isLogged() && mutations.size() > 1)
        {
            Set<DecoratedKey> keySet = new HashSet<>();
            Set<String> tableNames = new HashSet<>();

            for (IMutation mutation : mutations)
            {
                for (PartitionUpdate update : mutation.getPartitionUpdates())
                {
                    keySet.add(update.partitionKey());

                    tableNames.add(String.format("%s.%s", update.metadata().ksName, update.metadata().cfName));
                }
            }

            // CASSANDRA-11529: log only if we have more than a threshold of keys, this was also suggested in the
            // original ticket that introduced this warning, CASSANDRA-9282
            if (keySet.size() > DatabaseDescriptor.getUnloggedBatchAcrossPartitionsWarnThreshold())
            {
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES, UNLOGGED_BATCH_WARNING,
                                 keySet.size(), tableNames.size() == 1 ? "" : "s", tableNames);

                ClientWarn.instance.warn(MessageFormatter.arrayFormat(UNLOGGED_BATCH_WARNING, new Object[]{keySet.size(),
                                                    tableNames.size() == 1 ? "" : "s", tableNames}).getMessage());
            }
        }
    }


    public ResultMessage execute(QueryState queryState, QueryOptions options, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
    {
        return execute(queryState, BatchQueryOptions.withoutPerStatementVariables(options), queryStartNanoTime);
    }

    public ResultMessage execute(QueryState queryState, BatchQueryOptions options, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
    {
        return execute(queryState, options, false, options.getTimestamp(queryState), queryStartNanoTime);
    }

    private ResultMessage execute(QueryState queryState, BatchQueryOptions options, boolean local, long now, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        if (options.getConsistency() == null)
            throw new InvalidRequestException("Invalid empty consistency level");
        if (options.getSerialConsistency() == null)
            throw new InvalidRequestException("Invalid empty serial consistency level");

        if (hasConditions)
            return executeWithConditions(options, queryState, queryStartNanoTime);

        executeWithoutConditions(getMutations(options, local, now, queryStartNanoTime), options.getConsistency(), queryStartNanoTime);
        return new ResultMessage.Void();
    }

    private void executeWithoutConditions(Collection<? extends IMutation> mutations, ConsistencyLevel cl, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
    {
        if (mutations.isEmpty())
            return;

        verifyBatchSize(mutations);
        verifyBatchType(mutations);

        boolean mutateAtomic = (isLogged() && mutations.size() > 1);
        StorageProxy.mutateWithTriggers(mutations, cl, mutateAtomic, queryStartNanoTime);
    }

    private ResultMessage executeWithConditions(BatchQueryOptions options, QueryState state, long queryStartNanoTime)
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
                                                   state.getClientState(),
                                                   queryStartNanoTime))
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
            if (statement.getRestrictions().keyIsInRelation())
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

            checkFalse(statement.getRestrictions().clusteringKeyRestrictionsHasIN(),
                       "IN on the clustering key columns is not supported with conditional %s",
                       statement.type.isUpdate()? "updates" : "deletions");

            if (statement.hasSlices())
            {
                // All of the conditions require meaningful Clustering, not Slices
                assert !statement.hasConditions();

                Slices slices = statement.createSlices(statementOptions);
                // If all the ranges were invalid we do not need to do anything.
                if (slices.isEmpty())
                    continue;

                for (Slice slice : slices)
                {
                    casRequest.addRangeDeletion(slice, statement, statementOptions, timestamp);
                }

            }
            else
            {
                Clustering clustering = Iterables.getOnlyElement(statement.createClustering(statementOptions));
                if (statement.hasConditions())
                {
                    statement.addConditions(clustering, casRequest, statementOptions);
                    // As soon as we have a ifNotExists, we set columnsWithConditions to null so that everything is in the resultSet
                    if (statement.hasIfNotExistCondition() || statement.hasIfExistCondition())
                        columnsWithConditions = null;
                    else if (columnsWithConditions != null)
                        Iterables.addAll(columnsWithConditions, statement.getColumnsWithConditions());
                }
                casRequest.addRowUpdate(clustering, statement, statementOptions, timestamp);
            }
        }

        return Pair.create(casRequest, columnsWithConditions);
    }

    public ResultMessage executeInternal(QueryState queryState, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        if (hasConditions)
            return executeInternalWithConditions(BatchQueryOptions.withoutPerStatementVariables(options), queryState);

        executeInternalWithoutCondition(queryState, options, System.nanoTime());
        return new ResultMessage.Void();
    }

    private ResultMessage executeInternalWithoutCondition(QueryState queryState, QueryOptions options, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException
    {
        for (IMutation mutation : getMutations(BatchQueryOptions.withoutPerStatementVariables(options), true, queryState.getTimestamp(), queryStartNanoTime))
            mutation.apply();
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

        public ParsedStatement.Prepared prepare(ClientState clientState) throws InvalidRequestException
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

                statements.add(parsed.prepare(boundNames, clientState));
            }

            Attributes prepAttrs = attrs.prepare("[batch]", "[batch]");
            prepAttrs.collectMarkerSpecification(boundNames);

            BatchStatement batchStatement = new BatchStatement(boundNames.size(), type, statements, prepAttrs);
            batchStatement.validate();

            // Use the CFMetadata of the first statement for partition key bind indexes.  If the statements affect
            // multiple tables, we won't send partition key bind indexes.
            short[] partitionKeyBindIndexes = (haveMultipleCFs || batchStatement.statements.isEmpty())? null
                                                              : boundNames.getPartitionKeyBindIndexes(batchStatement.statements.get(0).cfm);

            return new ParsedStatement.Prepared(batchStatement, boundNames, partitionKeyBindIndexes);
        }
    }

    private static class MultiTableColumnsBuilder
    {
        private final Map<UUID, PartitionColumns.Builder> perTableBuilders = new HashMap<>();

        public void addAll(CFMetaData table, PartitionColumns columns)
        {
            PartitionColumns.Builder builder = perTableBuilders.get(table.cfId);
            if (builder == null)
            {
                builder = PartitionColumns.builder();
                perTableBuilders.put(table.cfId, builder);
            }
            builder.addAll(columns);
        }

        public Map<UUID, PartitionColumns> build()
        {
            Map<UUID, PartitionColumns> m = Maps.newHashMapWithExpectedSize(perTableBuilders.size());
            for (Map.Entry<UUID, PartitionColumns.Builder> p : perTableBuilders.entrySet())
                m.put(p.getKey(), p.getValue().build());
            return m;
        }
    }
}
