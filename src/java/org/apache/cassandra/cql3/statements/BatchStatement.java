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

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.RequestType;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.utils.Pair;

/**
 * A <code>BATCH</code> statement parsed from a CQL query.
 *
 */
public class BatchStatement extends ModificationStatement
{
    // statements to execute
    protected final List<ModificationStatement> statements;

    /**
     * Creates a new BatchStatement from a list of statements and a
     * Thrift consistency level.
     *
     * @param statements a list of UpdateStatements
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    public BatchStatement(List<ModificationStatement> statements, Attributes attrs)
    {
        super(null, attrs);
        this.statements = statements;
    }

    @Override
    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        for (ModificationStatement statement : statements)
            statement.prepareKeyspace(state);
    }

    @Override
    public void checkAccess(ClientState state) throws InvalidRequestException
    {
        Set<String> cfamsSeen = new HashSet<String>();
        for (ModificationStatement statement : statements)
        {
            // Avoid unnecessary authorizations.
            if (!(cfamsSeen.contains(statement.columnFamily())))
            {
                state.hasColumnFamilyAccess(statement.keyspace(), statement.columnFamily(), Permission.WRITE);
                cfamsSeen.add(statement.columnFamily());
            }
        }
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (getTimeToLive() != 0)
            throw new InvalidRequestException("Global TTL on the BATCH statement is not supported.");

        for (ModificationStatement statement : statements)
        {
            if (statement.isSetConsistencyLevel())
                throw new InvalidRequestException("Consistency level must be set on the BATCH, not individual statements");

            if (isSetTimestamp() && statement.isSetTimestamp())
                throw new InvalidRequestException("Timestamp must be set either on BATCH or individual statements");

            if (statement.getTimeToLive() < 0)
                throw new InvalidRequestException("A TTL must be greater or equal to 0");

            ThriftValidation.validateConsistencyLevel(statement.keyspace(), getConsistencyLevel(), RequestType.WRITE);
        }
    }

    public List<IMutation> getMutations(ClientState clientState, List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        Map<Pair<String, ByteBuffer>, RowAndCounterMutation> mutations = new HashMap<Pair<String, ByteBuffer>, RowAndCounterMutation>();
        for (ModificationStatement statement : statements)
        {
            if (isSetTimestamp())
                statement.timestamp = timestamp;

            List<IMutation> lm = statement.getMutations(clientState, variables);
            // Group mutation together, otherwise they won't get applied atomically
            for (IMutation m : lm)
            {
                Pair<String, ByteBuffer> key = Pair.create(m.getTable(), m.key());
                RowAndCounterMutation racm = mutations.get(key);
                if (racm == null)
                {
                    racm = new RowAndCounterMutation();
                    mutations.put(key, racm);
                }

                if (m instanceof CounterMutation)
                {
                    if (racm.cm == null)
                        racm.cm = (CounterMutation)m;
                    else
                        racm.cm.addAll(m);
                }
                else
                {
                    assert m instanceof RowMutation;
                    if (racm.rm == null)
                        racm.rm = (RowMutation)m;
                    else
                        racm.rm.addAll(m);
                }
            }
        }

        List<IMutation> batch = new LinkedList<IMutation>();
        for (RowAndCounterMutation racm : mutations.values())
        {
            if (racm.rm != null)
                batch.add(racm.rm);
            if (racm.cm != null)
                batch.add(racm.cm);
        }
        return batch;
    }

    public ParsedStatement.Prepared prepare(CFDefinition.Name[] boundNames) throws InvalidRequestException
    {
        // XXX: we use our knowledge that Modification don't create new statement upon call to prepare()
        for (ModificationStatement statement : statements)
        {
            statement.prepare(boundNames);
        }
        return new ParsedStatement.Prepared(this, Arrays.<CFDefinition.Name>asList(boundNames));
    }

    public ParsedStatement.Prepared prepare() throws InvalidRequestException
    {
        CFDefinition.Name[] boundNames = new CFDefinition.Name[getBoundsTerms()];
        return prepare(boundNames);
    }

    public String toString()
    {
        return String.format("BatchStatement(statements=%s, consistency=%s)", statements, cLevel);
    }

    private static class RowAndCounterMutation
    {
        public RowMutation rm;
        public CounterMutation cm;
    }
}
