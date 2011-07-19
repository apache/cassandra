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
import java.util.List;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;

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
    
    public DeleteStatement(List<Term> columns, String columnFamily, String keyName, List<Term> keys, Attributes attrs)
    {
        super(columnFamily, keyName, attrs);

        this.columns = columns;
        this.keys = keys;
    }

    public List<Term> getColumns()
    {
        return columns;
    }

    /** {@inheritDoc} */
    public List<Term> getKeys()
    {
        return keys;
    }

    /** {@inheritDoc} */
    public List<IMutation> prepareRowMutations(String keyspace, ClientState clientState) throws InvalidRequestException
    {
        return prepareRowMutations(keyspace, clientState, null);
    }

    /** {@inheritDoc} */
    public List<IMutation> prepareRowMutations(String keyspace, ClientState clientState, Long timestamp) throws InvalidRequestException
    {
        clientState.hasColumnFamilyAccess(columnFamily, Permission.WRITE);
        AbstractType<?> keyType = DatabaseDescriptor.getCFMetaData(keyspace, columnFamily).getKeyValidator();

        List<IMutation> rowMutations = new ArrayList<IMutation>();

        for (Term key : keys)
        {
            rowMutations.add(mutationForKey(key.getByteBuffer(keyType), keyspace, timestamp));
        }

        return rowMutations;
    }

    /** {@inheritDoc} */
    public RowMutation mutationForKey(ByteBuffer key, String keyspace, Long timestamp) throws InvalidRequestException
    {
        RowMutation rm = new RowMutation(keyspace, key);

        mutationForKey(rm, keyspace, timestamp);

        return rm;
    }

    /** {@inheritDoc} */
    public void mutationForKey(RowMutation mutation, String keyspace, Long timestamp) throws InvalidRequestException
    {
        CFMetaData metadata = validateColumnFamily(keyspace, columnFamily);
        QueryProcessor.validateKeyAlias(metadata, keyName);

        AbstractType comparator = metadata.getComparatorFor(null);

        if (columns.size() < 1) // No columns, delete the row
            mutation.delete(new QueryPath(columnFamily), (timestamp == null) ? getTimestamp() : timestamp);
        else    // Delete specific columns
        {
            for (Term column : columns)
            {
                ByteBuffer columnName = column.getByteBuffer(comparator);
                validateColumnName(columnName);
                mutation.delete(new QueryPath(columnFamily, null, columnName), (timestamp == null) ? getTimestamp() : timestamp);
            }
        }
    }

    public String toString()
    {
        return String.format("DeleteStatement(columns=%s, columnFamily=%s, consistency=%s keys=%s)",
                             columns,
                             columnFamily,
                             cLevel,
                             keys);
    }
}
