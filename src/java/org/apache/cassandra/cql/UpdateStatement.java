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
import java.util.*;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;

import static org.apache.cassandra.cql.QueryProcessor.validateColumn;

import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;
import static org.apache.cassandra.thrift.ThriftValidation.validateCommutativeForWrite;

/**
 * An <code>UPDATE</code> statement parsed from a CQL query statement.
 *
 */
public class UpdateStatement extends AbstractModification
{
    private Map<Term, Operation> columns;
    private List<Term> columnNames, columnValues;
    private List<Term> keys;

    /**
     * Creates a new UpdateStatement from a column family name, columns map, consistency
     * level, and key term.
     *
     * @param keyspace Keyspace (optional)
     * @param columnFamily column family name
     * @param keyName alias key name
     * @param columns a map of column name/values pairs
     * @param keys the keys to update
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    public UpdateStatement(String keyspace,
                           String columnFamily,
                           String keyName,
                           Map<Term, Operation> columns,
                           List<Term> keys,
                           Attributes attrs)
    {
        super(keyspace, columnFamily, keyName, attrs);

        this.columns = columns;
        this.keys = keys;
    }
    
    /**
     * Creates a new UpdateStatement from a column family name, a consistency level,
     * key, and lists of column names and values.  It is intended for use with the
     * alternate update format, <code>INSERT</code>.
     *
     * @param keyspace Keyspace (optional)
     * @param columnFamily column family name
     * @param keyName alias key name
     * @param columnNames list of column names
     * @param columnValues list of column values (corresponds to names)
     * @param keys the keys to update
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    public UpdateStatement(String keyspace,
                           String columnFamily,
                           String keyName,
                           List<Term> columnNames,
                           List<Term> columnValues,
                           List<Term> keys,
                           Attributes attrs)
    {
        super(keyspace, columnFamily, keyName, attrs);

        this.columnNames = columnNames;
        this.columnValues = columnValues;
        this.keys = keys;
    }

    /**
     * Returns the consistency level of this <code>UPDATE</code> statement, either
     * one parsed from the CQL statement, or the default level otherwise.
     * 
     * @return the consistency level as a Thrift enum.
     */
    public ConsistencyLevel getConsistencyLevel()
    {
        return (cLevel != null) ? cLevel : defaultConsistency;
    }
    
    /**
     * True if an explicit consistency level was parsed from the statement.
     * 
     * @return true if a consistency was parsed, false otherwise.
     */
    public boolean isSetConsistencyLevel()
    {
        return (cLevel != null);
    }

    /** {@inheritDoc} */
    public List<IMutation> prepareRowMutations(String keyspace, ClientState clientState) throws InvalidRequestException
    {
        return prepareRowMutations(keyspace, clientState, null);
    }

    /** {@inheritDoc} */
    public List<IMutation> prepareRowMutations(String keyspace, ClientState clientState, Long timestamp) throws InvalidRequestException
    {
        List<String> cfamsSeen = new ArrayList<String>();

        boolean hasCommutativeOperation = false;

        for (Map.Entry<Term, Operation> column : getColumns().entrySet())
        {
            if (!column.getValue().isUnary())
                hasCommutativeOperation = true;

            if (hasCommutativeOperation && column.getValue().isUnary())
                throw new InvalidRequestException("Mix of commutative and non-commutative operations is not allowed.");
        }

        CFMetaData metadata = validateColumnFamily(keyspace, columnFamily, hasCommutativeOperation);
        if (hasCommutativeOperation)
            validateCommutativeForWrite(metadata, cLevel);

        QueryProcessor.validateKeyAlias(metadata, keyName);

        // Avoid unnecessary authorizations.
        if (!(cfamsSeen.contains(columnFamily)))
        {
            clientState.hasColumnFamilyAccess(columnFamily, Permission.WRITE);
            cfamsSeen.add(columnFamily);
        }

        List<IMutation> rowMutations = new LinkedList<IMutation>();

        for (Term key: keys)
        {
            rowMutations.add(mutationForKey(keyspace, key.getByteBuffer(getKeyType(keyspace)), metadata, timestamp, clientState));
        }

        return rowMutations;
    }

    /**
     * Compute a row mutation for a single key
     *
     *
     * @param keyspace working keyspace
     * @param key key to change
     * @param metadata information about CF
     * @param timestamp global timestamp to use for every key mutation
     *
     * @param clientState
     * @return row mutation
     *
     * @throws InvalidRequestException on the wrong request
     */
    private IMutation mutationForKey(String keyspace, ByteBuffer key, CFMetaData metadata, Long timestamp, ClientState clientState) throws InvalidRequestException
    {
        AbstractType<?> comparator = getComparator(keyspace);

        // if true we need to wrap RowMutation into CounterMutation
        boolean hasCounterColumn = false;
        RowMutation rm = new RowMutation(keyspace, key);

        for (Map.Entry<Term, Operation> column : getColumns().entrySet())
        {
            ByteBuffer colName = column.getKey().getByteBuffer(comparator);
            Operation op = column.getValue();

            if (op.isUnary())
            {
                if (hasCounterColumn)
                    throw new InvalidRequestException("Mix of commutative and non-commutative operations is not allowed.");

                ByteBuffer colValue = op.a.getByteBuffer(getValueValidator(keyspace, colName));

                validateColumn(metadata, colName, colValue);
                rm.add(new QueryPath(columnFamily, null, colName),
                       colValue,
                       (timestamp == null) ? getTimestamp(clientState) : timestamp,
                       getTimeToLive());
            }
            else
            {
                hasCounterColumn = true;

                if (!column.getKey().getText().equals(op.a.getText()))
                    throw new InvalidRequestException("Only expressions like X = X + <long> are supported.");

                long value;

                try
                {
                    value = Long.parseLong(op.b.getText());
                }
                catch (NumberFormatException e)
                {
                    throw new InvalidRequestException(String.format("'%s' is an invalid value, should be a long.",
                                                      op.b.getText()));
                }

                rm.addCounter(new QueryPath(columnFamily, null, colName), value);
            }
        }

        return (hasCounterColumn) ? new CounterMutation(rm, getConsistencyLevel()) : rm;
    }

    public String getColumnFamily()
    {
        return columnFamily;
    }

    /** {@inheritDoc} */
    public List<Term> getKeys()
    {
        return keys;
    }
    
    public Map<Term, Operation> getColumns() throws InvalidRequestException
    {
        // Created from an UPDATE
        if (columns != null)
            return columns;
        
        // Created from an INSERT
        
        // Don't hate, validate.
        if (columnNames.size() != columnValues.size())
            throw new InvalidRequestException("unmatched column names/values");
        if (columnNames.size() < 1)
            throw new InvalidRequestException("no columns specified for INSERT");
        
        columns = new HashMap<Term, Operation>();
        
        for (int i = 0; i < columnNames.size(); i++)
            columns.put(columnNames.get(i), new Operation(columnValues.get(i)));

        return columns;
    }
    
    public String toString()
    {
        return String.format("UpdateStatement(keyspace=%s, columnFamily=%s, keys=%s, columns=%s, consistency=%s, timestamp=%s, timeToLive=%s)",
                             keyspace,
                             columnFamily,
                             keys,
                             columns,
                             getConsistencyLevel(),
                             timestamp,
                             timeToLive);
    }
    
    public AbstractType<?> getKeyType(String keyspace)
    {
        return Schema.instance.getCFMetaData(keyspace, columnFamily).getKeyValidator();
    }
    
    public AbstractType<?> getComparator(String keyspace)
    {
        return Schema.instance.getComparator(keyspace, columnFamily);
    }
    
    public AbstractType<?> getValueValidator(String keyspace, ByteBuffer column)
    {
        return Schema.instance.getValueValidator(keyspace, columnFamily, column);
    }
}
