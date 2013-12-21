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
package org.apache.cassandra.cql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.thrift.ThriftClientState;

import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;
import static org.apache.cassandra.cql.QueryProcessor.validateColumnName;

/**
 * A <code>DELETE</code> parsed from a CQL query statement.
 *
 */
public class DeleteStatement extends AbstractModification
{
    private List<Term> columns;
    private List<Term> keys;

    public DeleteStatement(List<Term> columns, String keyspace, String columnFamily, String keyName, List<Term> keys, Attributes attrs)
    {
        super(keyspace, columnFamily, keyName, attrs);

        this.columns = columns;
        this.keys = keys;
    }

    public List<Term> getColumns()
    {
        return columns;
    }

    public List<Term> getKeys()
    {
        return keys;
    }

    public List<IMutation> prepareRowMutations(String keyspace, ThriftClientState clientState, List<ByteBuffer> variables)
    throws InvalidRequestException, UnauthorizedException
    {
        return prepareRowMutations(keyspace, clientState, null, variables);
    }

    public List<IMutation> prepareRowMutations(String keyspace, ThriftClientState clientState, Long timestamp, List<ByteBuffer> variables)
    throws InvalidRequestException, UnauthorizedException
    {
        CFMetaData metadata = validateColumnFamily(keyspace, columnFamily);

        clientState.hasColumnFamilyAccess(keyspace, columnFamily, Permission.MODIFY);
        AbstractType<?> keyType = Schema.instance.getCFMetaData(keyspace, columnFamily).getKeyValidator();

        List<IMutation> mutations = new ArrayList<IMutation>(keys.size());

        for (Term key : keys)
            mutations.add(mutationForKey(key.getByteBuffer(keyType, variables), keyspace, timestamp, clientState, variables, metadata));

        return mutations;
    }

    public Mutation mutationForKey(ByteBuffer key, String keyspace, Long timestamp, ThriftClientState clientState, List<ByteBuffer> variables, CFMetaData metadata)
    throws InvalidRequestException
    {
        Mutation mutation = new Mutation(keyspace, key);

        QueryProcessor.validateKeyAlias(metadata, keyName);

        if (columns.size() < 1)
        {
            // No columns, delete the partition
            mutation.delete(columnFamily, (timestamp == null) ? getTimestamp(clientState) : timestamp);
        }
        else
        {
            // Delete specific columns
            AbstractType<?> at = metadata.comparator.asAbstractType();
            for (Term column : columns)
            {
                CellName columnName = metadata.comparator.cellFromByteBuffer(column.getByteBuffer(at, variables));
                validateColumnName(columnName);
                mutation.delete(columnFamily, columnName, (timestamp == null) ? getTimestamp(clientState) : timestamp);
            }
        }

        return mutation;
    }

    public String toString()
    {
        return String.format("DeleteStatement(columns=%s, keyspace=%s, columnFamily=%s, consistency=%s keys=%s)",
                             columns,
                             keyspace,
                             columnFamily,
                             cLevel,
                             keys);
    }
}
