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
import java.util.*;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.thrift.ThriftClientState;

import static org.apache.cassandra.cql.QueryProcessor.validateColumn;
import static org.apache.cassandra.cql.QueryProcessor.validateKey;
import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

/**
 * An <code>UPDATE</code> statement parsed from a CQL query statement.
 *
 */
public class UpdateStatement extends AbstractModification
{
    private Map<Term, Operation> columns;
    private List<Term> columnNames, columnValues;
    private final List<Term> keys;

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
    public List<IMutation> prepareRowMutations(String keyspace, ThriftClientState clientState, List<ByteBuffer> variables)
    throws InvalidRequestException, UnauthorizedException
    {
        return prepareRowMutations(keyspace, clientState, null, variables);
    }

    /** {@inheritDoc} */
    public List<IMutation> prepareRowMutations(String keyspace, ThriftClientState clientState, Long timestamp, List<ByteBuffer> variables)
    throws InvalidRequestException, UnauthorizedException
    {
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
            getConsistencyLevel().validateCounterForWrite(metadata);

        QueryProcessor.validateKeyAlias(metadata, keyName);

        clientState.hasColumnFamilyAccess(keyspace, columnFamily, Permission.MODIFY);

        List<IMutation> mutations = new LinkedList<>();

        for (Term key: keys)
            mutations.add(mutationForKey(keyspace, key.getByteBuffer(getKeyType(keyspace),variables), metadata, timestamp, clientState, variables));

        return mutations;
    }

    /**
     * Compute a mutation for a single key
     *
     *
     * @param keyspace working keyspace
     * @param key key to change
     * @param metadata information about CF
     * @param timestamp global timestamp to use for every key mutation
     *
     * @param clientState
     * @return mutation
     *
     * @throws InvalidRequestException on the wrong request
     */
    private IMutation mutationForKey(String keyspace, ByteBuffer key, CFMetaData metadata, Long timestamp, ThriftClientState clientState, List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        validateKey(key);
        CellNameType comparator = metadata.comparator;
        AbstractType<?> at = comparator.asAbstractType();

        // if true we need to wrap Mutation into CounterMutation
        boolean hasCounterColumn = false;
        Mutation mutation = new Mutation(keyspace, key);

        for (Map.Entry<Term, Operation> column : getColumns().entrySet())
        {
            CellName colName = comparator.cellFromByteBuffer(column.getKey().getByteBuffer(at, variables));
            Operation op = column.getValue();

            if (op.isUnary())
            {
                if (hasCounterColumn)
                    throw new InvalidRequestException("Mix of commutative and non-commutative operations is not allowed.");

                ByteBuffer colValue = op.a.getByteBuffer(metadata.getValueValidator(colName),variables);

                validateColumn(metadata, colName, colValue);
                mutation.add(columnFamily,
                             colName,
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

                mutation.addCounter(columnFamily, colName, value);
            }
        }

        return (hasCounterColumn) ? new CounterMutation(mutation, getConsistencyLevel()) : mutation;
    }

    public String getColumnFamily()
    {
        return columnFamily;
    }

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

    public List<Term> getColumnNames()
    {
        return columnNames;
    }

    public List<Term> getColumnValues()
    {
        return columnValues;
    }

}
