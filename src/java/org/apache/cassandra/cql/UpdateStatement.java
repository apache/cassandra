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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;

import static org.apache.cassandra.cql.QueryProcessor.validateColumn;

import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

/**
 * An <code>UPDATE</code> statement parsed from a CQL query statement.
 *
 */
public class UpdateStatement extends AbstractModification
{
    private Map<Term, Term> columns;
    private List<Term> columnNames, columnValues;
    private List<Term> keys;
    
    /**
     * Creates a new UpdateStatement from a column family name, columns map, consistency
     * level, and key term.
     * 
     * @param columnFamily column family name
     * @param columns a map of column name/values pairs
     * @param keys the keys to update
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    public UpdateStatement(String columnFamily,
                           Map<Term, Term> columns,
                           List<Term> keys,
                           Attributes attrs)
    {
        super(columnFamily, attrs);

        this.columns = columns;
        this.keys = keys;
    }
    
    /**
     * Creates a new UpdateStatement from a column family name, a consistency level,
     * key, and lists of column names and values.  It is intended for use with the
     * alternate update format, <code>INSERT</code>.
     * 
     * @param columnFamily column family name
     * @param columnNames list of column names
     * @param columnValues list of column values (corresponds to names)
     * @param keys the keys to update
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    public UpdateStatement(String columnFamily,
                           List<Term> columnNames,
                           List<Term> columnValues,
                           List<Term> keys,
                           Attributes attrs)
    {
        super(columnFamily, attrs);

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
    public List<RowMutation> prepareRowMutations(String keyspace, ClientState clientState) throws InvalidRequestException
    {
        return prepareRowMutations(keyspace, clientState, null);
    }

    /** {@inheritDoc} */
    public List<RowMutation> prepareRowMutations(String keyspace, ClientState clientState, Long timestamp) throws InvalidRequestException
    {
        List<String> cfamsSeen = new ArrayList<String>();

        CFMetaData metadata = validateColumnFamily(keyspace, columnFamily, false);

        // Avoid unnecessary authorizations.
        if (!(cfamsSeen.contains(columnFamily)))
        {
            clientState.hasColumnFamilyAccess(columnFamily, Permission.WRITE);
            cfamsSeen.add(columnFamily);
        }

        List<RowMutation> rowMutations = new LinkedList<RowMutation>();

        for (Term key: keys)
        {
            rowMutations.add(mutationForKey(keyspace, key.getByteBuffer(getKeyType(keyspace)), metadata, timestamp));
        }

        return rowMutations;
    }

    /**
     * Compute a row mutation for a single key
     *
     * @param keyspace working keyspace
     * @param key key to change
     * @param metadata information about CF
     * @param timestamp global timestamp to use for every key mutation
     *
     * @return row mutation
     *
     * @throws InvalidRequestException on the wrong request
     */
    private RowMutation mutationForKey(String keyspace, ByteBuffer key, CFMetaData metadata, Long timestamp) throws InvalidRequestException
    {
        RowMutation rm = new RowMutation(keyspace, key);

        mutationForKey(rm, keyspace, metadata, timestamp);

        return rm;
    }

    /** {@inheritDoc} */
    public RowMutation mutationForKey(ByteBuffer key, String keyspace, Long timestamp) throws InvalidRequestException
    {
        return mutationForKey(keyspace, key, validateColumnFamily(keyspace, columnFamily, false), timestamp);
    }

    /** {@inheritDoc} */
    public void mutationForKey(RowMutation mutation, String keyspace, Long timestamp) throws InvalidRequestException
    {
        mutationForKey(mutation, keyspace, validateColumnFamily(keyspace, columnFamily, false), timestamp);
    }

    private void mutationForKey(RowMutation mutation, String keyspace, CFMetaData metadata, Long timestamp) throws InvalidRequestException
    {
        AbstractType<?> comparator = getComparator(keyspace);

        for (Map.Entry<Term, Term> column : getColumns().entrySet())
        {
            ByteBuffer colName = column.getKey().getByteBuffer(comparator);
            ByteBuffer colValue = column.getValue().getByteBuffer(getValueValidator(keyspace, colName));

            validateColumn(metadata, colName, colValue);

            mutation.add(new QueryPath(columnFamily, null, colName),
                         colValue,
                         (timestamp == null) ? getTimestamp() : timestamp,
                         getTimeToLive());
        }
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

    public Map<Term, Term> getColumns() throws InvalidRequestException
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
        
        columns = new HashMap<Term, Term>();
        
        for (int i = 0; i < columnNames.size(); i++)
            columns.put(columnNames.get(i), columnValues.get(i));
        
        return columns;
    }
    
    public String toString()
    {
        return String.format("UpdateStatement(columnFamily=%s, keys=%s, columns=%s, consistency=%s, timestamp=%s, timeToLive=%s)",
                             columnFamily,
                             keys,
                             columns,
                             getConsistencyLevel(),
                             timestamp,
                             timeToLive);
    }
    
    public AbstractType<?> getKeyType(String keyspace)
    {
        return DatabaseDescriptor.getCFMetaData(keyspace, columnFamily).getKeyValidator();
    }
    
    public AbstractType<?> getComparator(String keyspace)
    {
        return DatabaseDescriptor.getComparator(keyspace, columnFamily);
    }
    
    public AbstractType<?> getValueValidator(String keyspace, ByteBuffer column)
    {
        return DatabaseDescriptor.getValueValidator(keyspace, columnFamily, column);
    }
}
