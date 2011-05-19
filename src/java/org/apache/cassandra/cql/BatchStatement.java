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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;

import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

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

    // global timestamp to apply for each mutation
    protected final Long timestamp;

    // global time to live
    protected final int timeToLive;

    /**
     * Creates a new BatchStatement from a list of statements and a
     * Thrift consistency level.
     *
     * @param statements a list of UpdateStatements
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    public BatchStatement(List<AbstractModification> statements, Attributes attrs)
    {
        this.statements = statements;
        this.consistency = attrs.getConsistencyLevel();
        this.timestamp = attrs.getTimestamp();
        this.timeToLive = attrs.getTimeToLive();
    }

    public List<AbstractModification> getStatements()
    {
        return statements;
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return consistency;
    }

    public int getTimeToLive()
    {
        return timeToLive;
    }

    public List<RowMutation> getMutations(String keyspace, ClientState clientState) throws InvalidRequestException
    {
        // To avoid unnecessary authorizations.
        List<String> seenColumnFamilies = new ArrayList<String>();

        List<RowMutation> batch = new LinkedList<RowMutation>();

        for (AbstractModification statement : statements)
        {
            final String columnFamily = statement.getColumnFamily();

            authorizeColumnFamily(keyspace, columnFamily, clientState, seenColumnFamilies);

            AbstractType<?> keyValidator = getKeyType(keyspace, columnFamily);

            for (Term rawKey : statement.getKeys()) // for each key of the statement
            {
                ByteBuffer key = rawKey.getByteBuffer(keyValidator);

                boolean found = false;

                for (RowMutation mutation : batch)
                {
                    if (mutation.key().equals(key) && hasColumnFamily(mutation.getColumnFamilies(), columnFamily))
                    {
                        statement.mutationForKey(mutation, keyspace, timestamp);

                        found = true;
                        break;
                    }
                }

                if (!found) // if mutation was not found we should add a new one
                    batch.add(statement.mutationForKey(key, keyspace, timestamp));
            }
        }

        return batch;
    }

    public boolean isSetTimestamp()
    {
        return timestamp != null;
    }

    private boolean hasColumnFamily(Collection<ColumnFamily> columnFamilies, String columnFamily)
    {
        for (ColumnFamily cf : columnFamilies)
        {
            if (cf.metadata().cfName.equals(columnFamily))
                return true;
        }

        return false;
    }

    private void authorizeColumnFamily(String keyspace, String columnFamily, ClientState state, List<String> seenCFs)
    throws InvalidRequestException
    {
        validateColumnFamily(keyspace, columnFamily, false);

        if (!seenCFs.contains(columnFamily))
        {
            state.hasColumnFamilyAccess(columnFamily, Permission.WRITE);
            seenCFs.add(columnFamily);
        }
    }

    public AbstractType<?> getKeyType(String keyspace, String columnFamily)
    {
        return DatabaseDescriptor.getCFMetaData(keyspace, columnFamily).getKeyValidator();
    }

    public String toString()
    {
        return String.format("BatchStatement(statements=%s, consistency=%s)", statements, consistency);
    }
}
