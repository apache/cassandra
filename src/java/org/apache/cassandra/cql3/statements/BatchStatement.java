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
import org.github.jamm.MemoryMeter;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * A <code>BATCH</code> statement parsed from a CQL query.
 *
 */
public class BatchStatement implements CQLStatement, MeasurableForPreparedCache
{
    public static enum Type
    {
        LOGGED, UNLOGGED, COUNTER
    }

    private final int boundTerms;
    public final Type type;
    private final List<ModificationStatement> statements;
    private final Attributes attrs;
    private final boolean hasConditions;

    /**
     * Creates a new BatchStatement from a list of statements and a
     * Thrift consistency level.
     *
     * @param type type of the batch
     * @param statements a list of UpdateStatements
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    public BatchStatement(int boundTerms, Type type, List<ModificationStatement> statements, Attributes attrs)
    {
        this(boundTerms, type, statements, attrs, false);
    }

    public BatchStatement(int boundTerms, Type type, List<ModificationStatement> statements, Attributes attrs, boolean hasConditions)
    {
        this.boundTerms = boundTerms;
        this.type = type;
        this.statements = statements;
        this.attrs = attrs;
        this.hasConditions = hasConditions;
    }

    public long measureForPreparedCache(MemoryMeter meter)
    {
        long size = meter.measure(this)
                  + meter.measureDeep(type)
                  + meter.measure(statements)
                  + meter.measureDeep(attrs);
        for (ModificationStatement stmt : statements)
            size += stmt.measureForPreparedCache(meter);
        return size;
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

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (attrs.isTimeToLiveSet())
            throw new InvalidRequestException("Global TTL on the BATCH statement is not supported.");

        for (ModificationStatement statement : statements)
        {
            if (attrs.isTimestampSet() && statement.isTimestampSet())
                throw new InvalidRequestException("Timestamp must be set either on BATCH or individual statements");
        }
    }

    public List<ModificationStatement> getStatements()
    {
        return statements;
    }

    private Collection<? extends IMutation> getMutations(BatchVariables variables, boolean local, ConsistencyLevel cl, long now)
    throws RequestExecutionException, RequestValidationException
    {
        Map<String, Map<ByteBuffer, IMutation>> mutations = new HashMap<>();
        for (int i = 0; i < statements.size(); i++)
        {
            ModificationStatement statement = statements.get(i);
            List<ByteBuffer> statementVariables = variables.getVariablesForStatement(i);
            addStatementMutations(statement, statementVariables, local, cl, now, mutations);
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
                                       List<ByteBuffer> variables,
                                       boolean local,
                                       ConsistencyLevel cl,
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
        List<ByteBuffer> keys = statement.buildPartitionKeyNames(variables);
        ColumnNameBuilder clusteringPrefix = statement.createClusteringPrefixBuilder(variables);
        UpdateParameters params = statement.makeUpdateParameters(keys, clusteringPrefix, variables, local, cl, now);

        for (ByteBuffer key : keys)
        {
            IMutation mutation = ksMap.get(key);
            RowMutation rm;
            if (mutation == null)
            {
                rm = new RowMutation(ksName, key);
                mutation = type == Type.COUNTER ? new CounterMutation(rm, cl) : rm;
                ksMap.put(key, mutation);
            }
            else
            {
                rm = type == Type.COUNTER ? ((CounterMutation)mutation).rowMutation() : (RowMutation)mutation;
            }

            statement.addUpdateForKey(rm.addOrGet(statement.cfm, UnsortedColumns.factory), key, clusteringPrefix, params);
        }
    }

    public ResultMessage execute(QueryState queryState, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        if (options.getConsistency() == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        return execute(new PreparedBatchVariables(options.getValues()), false, options.getConsistency(), options.getSerialConsistency(), queryState.getTimestamp());
    }

    public ResultMessage executeWithPerStatementVariables(ConsistencyLevel cl, QueryState queryState, List<List<ByteBuffer>> variables) throws RequestExecutionException, RequestValidationException
    {
        if (cl == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        return execute(new BatchOfPreparedVariables(variables), false, cl, ConsistencyLevel.SERIAL, queryState.getTimestamp());
    }

    public ResultMessage execute(BatchVariables variables, boolean local, ConsistencyLevel cl, ConsistencyLevel serialCl, long now)
    throws RequestExecutionException, RequestValidationException
    {
        // TODO: we don't support a serial consistency for batches in the protocol so defaulting to SERIAL for now.
        // We'll need to fix that.
        if (hasConditions)
            return executeWithConditions(variables, cl, serialCl, now);

        executeWithoutConditions(getMutations(variables, local, cl, now), cl);
        return null;
    }

    private void executeWithoutConditions(Collection<? extends IMutation> mutations, ConsistencyLevel cl) throws RequestExecutionException, RequestValidationException
    {
        boolean mutateAtomic = (type == Type.LOGGED && mutations.size() > 1);
        StorageProxy.mutateWithTriggers(mutations, cl, mutateAtomic);
    }

    private ResultMessage executeWithConditions(BatchVariables variables, ConsistencyLevel cl, ConsistencyLevel serialCf, long now)
    throws RequestExecutionException, RequestValidationException
    {
        ByteBuffer key = null;
        String ksName = null;
        String cfName = null;
        ColumnFamily updates = null;
        CQL3CasConditions conditions = null;
        Set<ColumnIdentifier> columnsWithConditions = new LinkedHashSet<ColumnIdentifier>();

        for (int i = 0; i < statements.size(); i++)
        {
            ModificationStatement statement = statements.get(i);
            List<ByteBuffer> statementVariables = variables.getVariablesForStatement(i);
            long timestamp = attrs.getTimestamp(now, statementVariables);
            List<ByteBuffer> pks = statement.buildPartitionKeyNames(statementVariables);
            if (pks.size() > 1)
                throw new IllegalArgumentException("Batch with conditions cannot span multiple partitions (you cannot use IN on the partition key)");
            if (key == null)
            {
                key = pks.get(0);
                ksName = statement.cfm.ksName;
                cfName = statement.cfm.cfName;
                conditions = new CQL3CasConditions(statement.cfm, now);
                updates = UnsortedColumns.factory.create(statement.cfm);
            }
            else if (!key.equals(pks.get(0)))
            {
                throw new InvalidRequestException("Batch with conditions cannot span multiple partitions");
            }

            ColumnNameBuilder clusteringPrefix = statement.createClusteringPrefixBuilder(statementVariables);
            if (statement.hasConditions())
            {
                statement.addUpdatesAndConditions(key, clusteringPrefix, updates, conditions, statementVariables, timestamp);
                // As soon as we have a ifNotExists, we set columnsWithConditions to null so that everything is in the resultSet
                if (statement.hasIfNotExistCondition() || statement.hasIfExistCondition())
                    columnsWithConditions = null;
                else if (columnsWithConditions != null)
                    Iterables.addAll(columnsWithConditions, statement.getColumnsWithConditions());
            }
            else
            {
                UpdateParameters params = statement.makeUpdateParameters(Collections.singleton(key), clusteringPrefix, statementVariables, false, cl, now);
                statement.addUpdateForKey(updates, key, clusteringPrefix, params);
            }
        }

        ColumnFamily result = StorageProxy.cas(ksName, cfName, key, conditions, updates, serialCf, cl);
        return new ResultMessage.Rows(ModificationStatement.buildCasResultSet(ksName, key, cfName, result, columnsWithConditions, true));
    }

    public ResultMessage executeInternal(QueryState queryState) throws RequestValidationException, RequestExecutionException
    {
        assert !hasConditions;

        for (IMutation mutation : getMutations(new PreparedBatchVariables(Collections.<ByteBuffer>emptyList()), true, null, queryState.getTimestamp()))
            mutation.apply();
        return null;
    }

    public interface BatchVariables
    {
        public List<ByteBuffer> getVariablesForStatement(int statementInBatch);
    }

    public static class PreparedBatchVariables implements BatchVariables
    {
        private final List<ByteBuffer> variables;

        public PreparedBatchVariables(List<ByteBuffer> variables)
        {
            this.variables = variables;
        }

        public List<ByteBuffer> getVariablesForStatement(int statementInBatch)
        {
            return variables;
        }
    }

    public static class BatchOfPreparedVariables implements BatchVariables
    {
        private final List<List<ByteBuffer>> variables;

        public BatchOfPreparedVariables(List<List<ByteBuffer>> variables)
        {
            this.variables = variables;
        }

        public List<ByteBuffer> getVariablesForStatement(int statementInBatch)
        {
            return variables.get(statementInBatch);
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

            List<ModificationStatement> statements = new ArrayList<ModificationStatement>(parsedStatements.size());
            boolean hasConditions = false;
            for (ModificationStatement.Parsed parsed : parsedStatements)
            {
                ModificationStatement stmt = parsed.prepare(boundNames);
                if (stmt.hasConditions())
                    hasConditions = true;

                if (stmt.isCounter() && type != Type.COUNTER)
                    throw new InvalidRequestException("Counter mutations are only allowed in COUNTER batches");

                if (!stmt.isCounter() && type == Type.COUNTER)
                    throw new InvalidRequestException("Only counter mutations are allowed in COUNTER batches");

                statements.add(stmt);
            }

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

            Attributes prepAttrs = attrs.prepare("[batch]", "[batch]");
            prepAttrs.collectMarkerSpecification(boundNames);

            return new ParsedStatement.Prepared(new BatchStatement(boundNames.size(), type, statements, prepAttrs, hasConditions), boundNames);
        }
    }
}
