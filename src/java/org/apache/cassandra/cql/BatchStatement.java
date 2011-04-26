/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cql;

import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;

/**
 * A <code>BATCH</code> statement parsed from a CQL query.
 *
 */
public class BatchStatement
{
    // statements to execute
    protected final List<AbstractModification> statements;

    // global consistency level
    protected final ConsistencyLevel consistency;


    /**
     * Creates a new BatchStatement from a list of statements and a
     * Thrift consistency level.
     *
     * @param statements a list of UpdateStatements
     * @param level Thrift consistency level enum
     */
    public BatchStatement(List<AbstractModification> statements, ConsistencyLevel level)
    {
        this.statements = statements;
        consistency = level;
    }

    public List<AbstractModification> getStatements()
    {
        return statements;
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return consistency;
    }

    public List<RowMutation> getMutations(String keyspace, ClientState clientState) throws InvalidRequestException
    {
        List<RowMutation> batch = new LinkedList<RowMutation>();

        for (AbstractModification statement : statements)
        {
            batch.addAll(statement.prepareRowMutations(keyspace, clientState));
        }

        return batch;
    }


    public String toString()
    {
        return String.format("BatchStatement(statements=%s, consistency=%s)", statements, consistency);
    }
}
