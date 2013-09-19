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

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Pair;

/**
 * A <code>BATCH</code> statement parsed from a CQL query.
 *
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
    private final Attributes attrs;

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
        this.boundTerms = boundTerms;
        this.type = type;
        this.statements = statements;
        this.attrs = attrs;
    }

    public int getBoundsTerms()
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

    private Collection<? extends IMutation> getMutations(List<ByteBuffer> variables, boolean local, ConsistencyLevel cl, long now)
    throws RequestExecutionException, RequestValidationException
    {
        Map<Pair<String, ByteBuffer>, IMutation> mutations = new HashMap<Pair<String, ByteBuffer>, IMutation>();
        for (ModificationStatement statement : statements)
            addStatementMutations(statement, variables, local, cl, now, mutations);

        return mutations.values();
    }

    private Collection<? extends IMutation> getMutations(List<List<ByteBuffer>> variables, ConsistencyLevel cl, long now)
    throws RequestExecutionException, RequestValidationException
    {
        Map<Pair<String, ByteBuffer>, IMutation> mutations = new HashMap<Pair<String, ByteBuffer>, IMutation>();
        for (int i = 0; i < statements.size(); i++)
        {
            ModificationStatement statement = statements.get(i);
            List<ByteBuffer> statementVariables = variables.get(i);
            addStatementMutations(statement, statementVariables, false, cl, now, mutations);
        }
        return mutations.values();
    }

    private void addStatementMutations(ModificationStatement statement,
                                       List<ByteBuffer> variables,
                                       boolean local,
                                       ConsistencyLevel cl,
                                       long now,
                                       Map<Pair<String, ByteBuffer>, IMutation> mutations)
    throws RequestExecutionException, RequestValidationException
    {
        // Group mutation together, otherwise they won't get applied atomically
        for (IMutation m : statement.getMutations(variables, local, cl, attrs.getTimestamp(now, variables), true))
        {
            Pair<String, ByteBuffer> key = Pair.create(m.getKeyspaceName(), m.key());
            IMutation existing = mutations.get(key);

            if (existing == null)
            {
                mutations.put(key, m);
            }
            else
            {
                existing.addAll(m);
            }
        }
    }

    public ResultMessage execute(QueryState queryState, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        if (options.getConsistency() == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        execute(getMutations(options.getValues(), false, options.getConsistency(), queryState.getTimestamp()), options.getConsistency());
        return null;
    }

    public void executeWithPerStatementVariables(ConsistencyLevel cl, QueryState queryState, List<List<ByteBuffer>> variables) throws RequestExecutionException, RequestValidationException
    {
        if (cl == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        execute(getMutations(variables, cl, queryState.getTimestamp()), cl);
    }

    private void execute(Collection<? extends IMutation> mutations, ConsistencyLevel cl) throws RequestExecutionException, RequestValidationException
    {
        boolean mutateAtomic = (type == Type.LOGGED && mutations.size() > 1);
        StorageProxy.mutateWithTriggers(mutations, cl, mutateAtomic);
    }

    public ResultMessage executeInternal(QueryState queryState) throws RequestValidationException, RequestExecutionException
    {
        for (IMutation mutation : getMutations(Collections.<ByteBuffer>emptyList(), true, null, queryState.getTimestamp()))
            mutation.apply();
        return null;
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
            VariableSpecifications boundNames = getBoundsVariables();

            List<ModificationStatement> statements = new ArrayList<ModificationStatement>(parsedStatements.size());
            for (ModificationStatement.Parsed parsed : parsedStatements)
            {
                ModificationStatement stmt = parsed.prepare(boundNames);
                if (stmt.hasConditions())
                    throw new InvalidRequestException("Conditional updates are not allowed in batches");

                if (stmt.isCounter() && type != Type.COUNTER)
                    throw new InvalidRequestException("Counter mutations are only allowed in COUNTER batches");

                if (!stmt.isCounter() && type == Type.COUNTER)
                    throw new InvalidRequestException("Only counter mutations are allowed in COUNTER batches");

                statements.add(stmt);
            }

            Attributes prepAttrs = attrs.prepare("[batch]", "[batch]");
            prepAttrs.collectMarkerSpecification(boundNames);

            return new ParsedStatement.Prepared(new BatchStatement(boundNames.size(), type, statements, prepAttrs), boundNames);
        }
    }
}
