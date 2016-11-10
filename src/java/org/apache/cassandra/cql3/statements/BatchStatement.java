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
import org.apache.cassandra.db.composites.Composite;
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
    public enum Type
    {
        LOGGED, UNLOGGED, COUNTER
    }

    private final int boundTerms;
    public final Type type;
    private final List<ModificationStatement> statements;
    private final Attributes attrs;
    private final boolean hasConditions;
    private static final Logger logger = LoggerFactory.getLogger(BatchStatement.class);
    private static final String unloggedBatchWarning = "Unlogged batch covering {} partitions detected against table{} {}. " +
                                                       "You should use a logged batch for atomicity, or asynchronous writes for performance.";

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
        boolean hasConditions = false;
        for (ModificationStatement statement : statements)
            hasConditions |= statement.hasConditions();

        this.boundTerms = boundTerms;
        this.type = type;
        this.statements = statements;
        this.attrs = attrs;
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
            if (type == Type.COUNTER)
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

            if (type == Type.COUNTER && !statement.isCounter())
                throw new InvalidRequestException("Cannot include non-counter statement in a counter batch");

            if (type == Type.LOGGED && statement.isCounter())
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
        Map<String, Map<ByteBuffer, IMutation>> mutations = new HashMap<>();
        for (int i = 0; i < statements.size(); i++)
        {
            ModificationStatement statement = statements.get(i);
            QueryOptions statementOptions = options.forStatement(i);
            long timestamp = attrs.getTimestamp(now, statementOptions);
            addStatementMutations(statement, statementOptions, local, timestamp, mutations);
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
        Composite clusteringPrefix = statement.createClusteringPrefix(options);
        UpdateParameters params = statement.makeUpdateParameters(keys, clusteringPrefix, options, local, now);

        for (ByteBuffer key : keys)
        {
            IMutation mutation = ksMap.get(key);
            Mutation mut;
            if (mutation == null)
            {
                mut = new Mutation(ksName, key);
                mutation = statement.cfm.isCounter() ? new CounterMutation(mut, options.getConsistency()) : mut;
                ksMap.put(key, mutation);
            }
            else
            {
                mut = statement.cfm.isCounter() ? ((CounterMutation) mutation).getMutation() : (Mutation) mutation;
            }

            statement.addUpdateForKey(mut.addOrGet(statement.cfm), key, clusteringPrefix, params);
        }
    }

    /**
     * Checks batch size to ensure threshold is met. If not, a warning is logged.
     *
     * @param cfs ColumnFamilies that will store the batch's mutations.
     */
    public static void verifyBatchSize(Iterable<ColumnFamily> cfs) throws InvalidRequestException
    {
        long size = 0;
        long warnThreshold = DatabaseDescriptor.getBatchSizeWarnThreshold();
        long failThreshold = DatabaseDescriptor.getBatchSizeFailThreshold();

        for (ColumnFamily cf : cfs)
            size += cf.dataSize();

        if (size > warnThreshold)
        {
            Set<String> ksCfPairs = new HashSet<>();
            for (ColumnFamily cf : cfs)
                ksCfPairs.add(String.format("%s.%s", cf.metadata().ksName, cf.metadata().cfName));

            String format = "Batch of prepared statements for {} is of size {}, exceeding specified threshold of {} by {}.{}";
            if (size > failThreshold)
            {
                Tracing.trace(format, ksCfPairs, size, failThreshold, size - failThreshold, " (see batch_size_fail_threshold_in_kb)");
                logger.error(format, ksCfPairs, size, failThreshold, size - failThreshold, " (see batch_size_fail_threshold_in_kb)");
                throw new InvalidRequestException("Batch too large");
            }
            else if (logger.isWarnEnabled())
            {
                logger.warn(format, ksCfPairs, size, warnThreshold, size - warnThreshold, "");
            }
            ClientWarn.instance.warn(MessageFormatter.arrayFormat(format, new Object[]{ ksCfPairs, size, warnThreshold, size - warnThreshold, "" }).getMessage());
        }
    }

    private void verifyBatchType(Collection<? extends IMutation> mutations)
    {
        if (type != Type.LOGGED && mutations.size() > 1)
        {
            Set<String> ksCfPairs = new HashSet<>();
            Set<ByteBuffer> keySet = new HashSet<>();

            for (IMutation im : mutations)
            {
                keySet.add(im.key());

                for (ColumnFamily cf : im.getColumnFamilies())
                    ksCfPairs.add(String.format("%s.%s", cf.metadata().ksName, cf.metadata().cfName));
            }

            // CASSANDRA-11529: log only if we have more than a threshold of keys, this was also suggested in the
            // original ticket that introduced this warning, CASSANDRA-9282
            if (keySet.size() > DatabaseDescriptor.getUnloggedBatchAcrossPartitionsWarnThreshold())
            {
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES, unloggedBatchWarning,
                                 keySet.size(), ksCfPairs.size() == 1 ? "" : "s", ksCfPairs);

                ClientWarn.instance.warn(MessageFormatter.arrayFormat(unloggedBatchWarning,
                                                                  new Object[]{
                                                                              keySet.size(),
                                                                              ksCfPairs.size() == 1 ? "" : "s",
                                                                              ksCfPairs
                                                                  }).getMessage());
            }
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
        // Extract each collection of cfs from it's IMutation and then lazily concatenate all of them into a single Iterable.
        Iterable<ColumnFamily> cfs = Iterables.concat(Iterables.transform(mutations, new Function<IMutation, Collection<ColumnFamily>>()
        {
            public Collection<ColumnFamily> apply(IMutation im)
            {
                return im.getColumnFamilies();
            }
        }));

        verifyBatchSize(cfs);
        verifyBatchType(mutations);

        boolean mutateAtomic = (type == Type.LOGGED && mutations.size() > 1);
        StorageProxy.mutateWithTriggers(mutations, cl, mutateAtomic);
    }

    private ResultMessage executeWithConditions(BatchQueryOptions options, QueryState state)
    throws RequestExecutionException, RequestValidationException
    {
        Pair<CQL3CasRequest, Set<ColumnDefinition>> p = makeCasRequest(options, state);
        CQL3CasRequest casRequest = p.left;
        Set<ColumnDefinition> columnsWithConditions = p.right;

        ColumnFamily result = StorageProxy.cas(casRequest.cfm.ksName,
                                               casRequest.cfm.cfName,
                                               casRequest.key,
                                               casRequest,
                                               options.getSerialConsistency(),
                                               options.getConsistency(),
                                               state.getClientState());

        return new ResultMessage.Rows(ModificationStatement.buildCasResultSet(casRequest.cfm.ksName,
                                                                              casRequest.key,
                                                                              casRequest.cfm.cfName,
                                                                              result,
                                                                              columnsWithConditions,
                                                                              true,
                                                                              options.forStatement(0)));
    }

    private Pair<CQL3CasRequest,Set<ColumnDefinition>> makeCasRequest(BatchQueryOptions options, QueryState state)
    {
        long now = state.getTimestamp();
        ByteBuffer key = null;
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
                key = pks.get(0);
                casRequest = new CQL3CasRequest(statement.cfm, key, true);
            }
            else if (!key.equals(pks.get(0)))
            {
                throw new InvalidRequestException("Batch with conditions cannot span multiple partitions");
            }

            Composite clusteringPrefix = statement.createClusteringPrefix(statementOptions);
            if (statement.hasConditions())
            {
                statement.addConditions(clusteringPrefix, casRequest, statementOptions);
                // As soon as we have a ifNotExists, we set columnsWithConditions to null so that everything is in the resultSet
                if (statement.hasIfNotExistCondition() || statement.hasIfExistCondition())
                    columnsWithConditions = null;
                else if (columnsWithConditions != null)
                    Iterables.addAll(columnsWithConditions, statement.getColumnsWithConditions());
            }
            casRequest.addRowUpdate(clusteringPrefix, statement, statementOptions, timestamp);
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

        ColumnFamily result = ModificationStatement.casInternal(request, state);

        return new ResultMessage.Rows(ModificationStatement.buildCasResultSet(request.cfm.ksName,
                                                                              request.key,
                                                                              request.cfm.cfName,
                                                                              result,
                                                                              columnsWithConditions,
                                                                              true,
                                                                              options.forStatement(0)));
    }

    public interface BatchVariables
    {
        public List<ByteBuffer> getVariablesForStatement(int statementInBatch);
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
            Short[] partitionKeyBindIndexes = (haveMultipleCFs || batchStatement.statements.isEmpty())? null
                                                              : boundNames.getPartitionKeyBindIndexes(batchStatement.statements.get(0).cfm);

            return new ParsedStatement.Prepared(batchStatement, boundNames, partitionKeyBindIndexes);
        }
    }
}
