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
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql.QueryProcessor.validateColumnName;
import static org.apache.cassandra.cql.QueryProcessor.validateKey;

import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;
import static org.apache.cassandra.thrift.ThriftValidation.validateCommutativeForWrite;

/**
 * An <code>UPDATE</code> statement parsed from a CQL query statement.
 *
 */
public class UpdateStatement extends ModificationStatement
{
    private CFDefinition cfDef;
    private final Map<ColumnIdentifier, Operation> columns;
    private final List<ColumnIdentifier> columnNames;
    private final List<Term> columnValues;
    private final List<Relation> whereClause;

    private final Map<ColumnIdentifier, Operation> processedColumns = new HashMap<ColumnIdentifier, Operation>();
    private final Map<ColumnIdentifier, List<Term>> processedKeys = new HashMap<ColumnIdentifier, List<Term>>();

    /**
     * Creates a new UpdateStatement from a column family name, columns map, consistency
     * level, and key term.
     *
     * @param name column family being operated on
     * @param columns a map of column name/values pairs
     * @param whereClause the where clause
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    public UpdateStatement(CFName name,
                           Map<ColumnIdentifier, Operation> columns,
                           List<Relation> whereClause,
                           Attributes attrs)
    {
        super(name, attrs);

        this.columns = columns;
        this.whereClause = whereClause;
        this.columnNames = null;
        this.columnValues = null;
    }

    /**
     * Creates a new UpdateStatement from a column family name, a consistency level,
     * key, and lists of column names and values.  It is intended for use with the
     * alternate update format, <code>INSERT</code>.
     *
     * @param name column family being operated on
     * @param columnNames list of column names
     * @param columnValues list of column values (corresponds to names)
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    public UpdateStatement(CFName name,
                           List<ColumnIdentifier> columnNames,
                           List<Term> columnValues,
                           Attributes attrs)
    {
        super(name, attrs);

        this.columnNames = columnNames;
        this.columnValues = columnValues;
        this.whereClause = null;
        this.columns = null;
    }

    /** {@inheritDoc} */
    public List<IMutation> getMutations(ClientState clientState, List<ByteBuffer> variables) throws InvalidRequestException
    {
        // Check key
        List<Term> keys = processedKeys.get(cfDef.key.name);
        if (keys == null || keys.isEmpty())
            throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", cfDef.key));

        ColumnNameBuilder builder = cfDef.getColumnNameBuilder();
        CFDefinition.Name firstEmpty = null;
        for (CFDefinition.Name name : cfDef.columns.values())
        {
            List<Term> values = processedKeys.get(name.name);
            if (values == null || values.isEmpty())
            {
                firstEmpty = name;
                // For sparse, we must have all components
                if (cfDef.isComposite && !cfDef.isCompact)
                    throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", name));
            }
            else if (firstEmpty != null)
            {
                throw new InvalidRequestException(String.format("Missing PRIMARY KEY part %s since %s is set", firstEmpty.name, name.name));
            }
            else
            {
                assert values.size() == 1; // We only allow IN for row keys so far
                builder.add(values.get(0), Relation.Type.EQ, variables);
            }
        }

        List<IMutation> rowMutations = new LinkedList<IMutation>();

        for (Term key: keys)
        {
            ByteBuffer rawKey = key.getByteBuffer(cfDef.key.type, variables);
            rowMutations.add(mutationForKey(cfDef, clientState, rawKey, builder, variables));
        }

        return rowMutations;
    }

    /**
     * Compute a row mutation for a single key
     *
     * @param cfDef column family being operated on
     * @param clientState user/session state
     * @param key key to change
     * @param builder ongoing column name accumulator for the current statement
     * @param variables positional values
     *
     * @return row mutation
     *
     * @throws InvalidRequestException on the wrong request
     */
    private IMutation mutationForKey(CFDefinition cfDef, ClientState clientState, ByteBuffer key, ColumnNameBuilder builder, List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        validateKey(key);
        // if true we need to wrap RowMutation into CounterMutation
        boolean hasCounterColumn = false;

        QueryProcessor.validateKey(key);
        RowMutation rm = new RowMutation(cfDef.cfm.ksName, key);
        ColumnFamily cf = rm.addOrGet(cfDef.cfm.cfName);

        if (cfDef.isCompact)
        {
            if (builder.componentCount() == 0)
                throw new InvalidRequestException(String.format("Missing PRIMARY KEY part %s", cfDef.columns.values().iterator().next()));

            Operation value = processedColumns.get(cfDef.value.name);
            if (value == null)
                throw new InvalidRequestException(String.format("Missing mandatory column %s", cfDef.value));
            hasCounterColumn = addToMutation(clientState, cf, builder.build(), cfDef.value, value, variables);
        }
        else
        {
            for (CFDefinition.Name name : cfDef.metadata.values())
            {
                Operation value = processedColumns.get(name.name);
                if (value == null)
                    continue;

                ByteBuffer colName = builder.copy().add(name.name.key).build();
                hasCounterColumn |= addToMutation(clientState, cf, colName, name, value, variables);
            }
        }

        return (hasCounterColumn) ? new CounterMutation(rm, getConsistencyLevel()) : rm;
    }

    private boolean addToMutation(ClientState clientState,
                                  ColumnFamily cf,
                                  ByteBuffer colName,
                                  CFDefinition.Name valueDef,
                                  Operation value,
                                  List<ByteBuffer> variables) throws InvalidRequestException
    {
        if (value.isUnary())
        {
            validateColumnName(colName);
            ByteBuffer valueBytes = value.value.getByteBuffer(valueDef.type, variables);
            Column c = timeToLive > 0
                       ? new ExpiringColumn(colName, valueBytes, getTimestamp(clientState), timeToLive)
                       : new Column(colName, valueBytes, getTimestamp(clientState));
            cf.addColumn(c);
            return false;
        }
        else
        {
            if (!valueDef.name.equals(value.ident))
                throw new InvalidRequestException("Only expressions like X = X + <long> are supported.");

            long val;
            try
            {
                val = ByteBufferUtil.toLong(value.value.getByteBuffer(LongType.instance, variables));
            }
            catch (NumberFormatException e)
            {
                throw new InvalidRequestException(String.format("'%s' is an invalid value, should be a long.",
                            value.value.getText()));
            }

            if (value.type == Operation.Type.MINUS)
            {
                if (val == Long.MIN_VALUE)
                    throw new InvalidRequestException("The negation of " + val + " overflows supported integer precision (signed 8 bytes integer)");
                else
                    val = -val;
            }
            cf.addCounter(new QueryPath(columnFamily(), null, colName), val);
            return true;
        }
    }

    public ParsedStatement.Prepared prepare(CFDefinition.Name[] boundNames) throws InvalidRequestException
    {
        boolean hasCommutativeOperation = false;

        if (columns != null)
        {
            for (Map.Entry<ColumnIdentifier, Operation> column : columns.entrySet())
            {
                if (!column.getValue().isUnary())
                    hasCommutativeOperation = true;

                if (hasCommutativeOperation && column.getValue().isUnary())
                    throw new InvalidRequestException("Mix of commutative and non-commutative operations is not allowed.");
            }
        }

        // Deal here with the keyspace overwrite thingy to avoid mistake
        CFMetaData metadata = validateColumnFamily(keyspace(), columnFamily(), hasCommutativeOperation);
        if (hasCommutativeOperation)
            validateCommutativeForWrite(metadata, cLevel);

        cfDef = metadata.getCfDef();

        if (columns == null)
        {
            // Created from an INSERT
            // Don't hate, validate.
            if (columnNames.size() != columnValues.size())
                throw new InvalidRequestException("unmatched column names/values");
            if (columnNames.size() < 1)
                throw new InvalidRequestException("no columns specified for INSERT");

            for (int i = 0; i < columnNames.size(); i++)
            {
                CFDefinition.Name name = cfDef.get(columnNames.get(i));
                if (name == null)
                    throw new InvalidRequestException(String.format("Unknown identifier %s", columnNames.get(i)));

                Term value = columnValues.get(i);
                if (value.isBindMarker())
                    boundNames[value.bindIndex] = name;

                switch (name.kind)
                {
                    case KEY_ALIAS:
                    case COLUMN_ALIAS:
                        if (processedKeys.containsKey(name.name))
                            throw new InvalidRequestException(String.format("Multiple definition found for PRIMARY KEY part %s", name));
                        processedKeys.put(name.name, Collections.singletonList(value));
                        break;
                    case VALUE_ALIAS:
                    case COLUMN_METADATA:
                        if (processedColumns.containsKey(name.name))
                            throw new InvalidRequestException(String.format("Multiple definition found for column %s", name));
                        processedColumns.put(name.name, new Operation(value));
                        break;
                }
            }
        }
        else
        {
            // Created from an UPDATE
            for (Map.Entry<ColumnIdentifier, Operation> entry : columns.entrySet())
            {
                CFDefinition.Name name = cfDef.get(entry.getKey());
                if (name == null)
                    throw new InvalidRequestException(String.format("Unknown identifier %s", entry.getKey()));

                switch (name.kind)
                {
                    case KEY_ALIAS:
                    case COLUMN_ALIAS:
                        throw new InvalidRequestException(String.format("PRIMARY KEY part %s found in SET part", entry.getKey()));
                    case VALUE_ALIAS:
                    case COLUMN_METADATA:
                        if (processedColumns.containsKey(name.name))
                            throw new InvalidRequestException(String.format("Multiple definition found for column %s", name));
                        Operation op = entry.getValue();
                        if (op.value.isBindMarker())
                            boundNames[op.value.bindIndex] = name;
                        processedColumns.put(name.name, op);
                        break;
                }
            }
            processKeys(cfDef, whereClause, processedKeys, boundNames);
        }

        return new ParsedStatement.Prepared(this, Arrays.<ColumnSpecification>asList(boundNames));
    }

    public ParsedStatement.Prepared prepare() throws InvalidRequestException
    {
        CFDefinition.Name[] names = new CFDefinition.Name[getBoundsTerms()];
        return prepare(names);
    }

    // Reused by DeleteStatement
    static void processKeys(CFDefinition cfDef, List<Relation> keys, Map<ColumnIdentifier, List<Term>> processed, CFDefinition.Name[] names) throws InvalidRequestException
    {
        for (Relation rel : keys)
        {
            CFDefinition.Name name = cfDef.get(rel.getEntity());
            if (name == null)
                throw new InvalidRequestException(String.format("Unknown key identifier %s", rel.getEntity()));

            switch (name.kind)
            {
                case KEY_ALIAS:
                case COLUMN_ALIAS:
                    List<Term> values;
                    if (rel.operator() == Relation.Type.EQ)
                        values = Collections.singletonList(rel.getValue());
                    else if (name.kind == CFDefinition.Name.Kind.KEY_ALIAS && rel.operator() == Relation.Type.IN)
                        values = rel.getInValues();
                    else
                        throw new InvalidRequestException(String.format("Invalid operator %s for key %s", rel.operator(), rel.getEntity()));

                    if (processed.containsKey(name.name))
                        throw new InvalidRequestException(String.format("Multiple definition found for PRIMARY KEY part %s", name));
                    for (Term value : values)
                        if (value.isBindMarker())
                            names[value.bindIndex] = name;
                    processed.put(name.name, values);
                    break;
                case VALUE_ALIAS:
                case COLUMN_METADATA:
                    throw new InvalidRequestException(String.format("PRIMARY KEY part %s found in SET part", rel.getEntity()));
            }
        }
    }

    public String toString()
    {
        return String.format("UpdateStatement(name=%s, keys=%s, columns=%s, consistency=%s, timestamp=%s, timeToLive=%s)",
                             cfName,
                             whereClause,
                             columns,
                             getConsistencyLevel(),
                             timestamp,
                             timeToLive);
    }
}
