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

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ArrayListMultimap;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

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
    private final List<Pair<ColumnIdentifier, Operation>> columns;
    private final List<ColumnIdentifier> columnNames;
    private final List<Value> columnValues;
    private final List<Relation> whereClause;

    private final ArrayListMultimap<CFDefinition.Name, Operation> processedColumns = ArrayListMultimap.create();
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
                           List<Pair<ColumnIdentifier, Operation>> columns,
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
                           Attributes attrs,
                           List<ColumnIdentifier> columnNames,
                           List<Value> columnValues)
    {
        super(name, attrs);

        this.columnNames = columnNames;
        this.columnValues = columnValues;
        this.whereClause = null;
        this.columns = null;
    }


    /** {@inheritDoc} */
    public List<IMutation> getMutations(ClientState clientState, List<ByteBuffer> variables) throws UnavailableException, TimeoutException, InvalidRequestException
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

        List<ByteBuffer> rawKeys = new ArrayList<ByteBuffer>(keys.size());
        for (Term key: keys)
            rawKeys.add(key.getByteBuffer(cfDef.key.type, variables));

        // Lists SET operation incurs a read. Do that now. Note that currently,
        // if there is at least one list, we just read the whole "row" (in the CQL sense of
        // row) to simplify. Once #3885 is in, we can improve.
        boolean needsReading = false;
        for (Map.Entry<CFDefinition.Name, Operation> entry : processedColumns.entries())
        {
            CFDefinition.Name name = entry.getKey();
            Operation value = entry.getValue();

            if (!(name.type instanceof ListType))
                continue;

            if (value == null || value.type != Operation.Type.FUNCTION)
                continue;

            Operation.Function fOp = (Operation.Function)value;
            if (fOp.fct.needsReading)
            {
                needsReading = true;
                break;
            }
        }

        Map<ByteBuffer, ColumnGroupMap> rows = needsReading ? readRows(rawKeys, builder, (CompositeType)cfDef.cfm.comparator) : null;

        List<IMutation> rowMutations = new LinkedList<IMutation>();
        UpdateParameters params = new UpdateParameters(variables, getTimestamp(clientState), timeToLive);

        for (ByteBuffer key: rawKeys)
            rowMutations.add(mutationForKey(cfDef, key, builder, params, rows == null ? null : rows.get(key)));

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
    private IMutation mutationForKey(CFDefinition cfDef, ByteBuffer key, ColumnNameBuilder builder, UpdateParameters params, ColumnGroupMap group)
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

            List<Operation> value = processedColumns.get(cfDef.value);
            if (value.isEmpty())
                throw new InvalidRequestException(String.format("Missing mandatory column %s", cfDef.value));
            assert value.size() == 1;
            hasCounterColumn = addToMutation(cf, builder, cfDef.value, value.get(0), params, null);
        }
        else
        {
            for (Map.Entry<CFDefinition.Name, Operation> entry : processedColumns.entries())
            {
                CFDefinition.Name name = entry.getKey();
                Operation value = entry.getValue();

                hasCounterColumn |= addToMutation(cf, builder.copy().add(name.name.key), name, value, params, group == null ? null : group.getCollection(name.name.key));
            }
        }

        return (hasCounterColumn) ? new CounterMutation(rm, getConsistencyLevel()) : rm;
    }

    private boolean addToMutation(ColumnFamily cf,
                                  ColumnNameBuilder builder,
                                  CFDefinition.Name valueDef,
                                  Operation value,
                                  UpdateParameters params,
                                  List<Pair<ByteBuffer, IColumn>> list) throws InvalidRequestException
    {
        switch (value.type)
        {
            case SET:
                Value v = ((Operation.Set)value).value;
                if (v instanceof Term)
                {
                    ByteBuffer colName = builder.build();
                    validateColumnName(colName);
                    ByteBuffer valueBytes = ((Term)v).getByteBuffer(valueDef.type, params.variables);
                    cf.addColumn(params.makeColumn(colName, valueBytes));
                }
                else
                {
                    assert v instanceof Value.CollectionLiteral;
                    Value.CollectionLiteral l = (Value.CollectionLiteral)v;
                    l.validateType(valueDef);

                    // Remove previous
                    cf.addAtom(params.makeTombstoneForOverwrite(builder.copy().build(), builder.copy().buildAsEndOfRange()));

                    if (!l.isEmpty())
                        addToMutation(cf, builder, valueDef, new Operation.Function(l.constructionFunction(), l.asList()), params, null);
                }
                return false;
            case COUNTER:
                Operation.Counter cOp = (Operation.Counter)value;
                long val;
                try
                {
                    val = ByteBufferUtil.toLong(cOp.value.getByteBuffer(LongType.instance, params.variables));
                }
                catch (NumberFormatException e)
                {
                    throw new InvalidRequestException(String.format("'%s' is an invalid value, should be a long.",
                                cOp.value.getText()));
                }

                if (cOp.isSubstraction)
                {
                    if (val == Long.MIN_VALUE)
                        throw new InvalidRequestException("The negation of " + val + " overflows supported integer precision (signed 8 bytes integer)");
                    else
                        val = -val;
                }
                cf.addCounter(new QueryPath(columnFamily(), null, builder.build()), val);
                return true;
            case FUNCTION:
                Operation.Function fOp = (Operation.Function)value;
                if (!(valueDef.type instanceof CollectionType))
                    throw new InvalidRequestException(String.format("Invalid operation %s, %s is not a collection", fOp.fct, valueDef.name));

                if ((valueDef.type instanceof ListType) && fOp.fct.needsReading)
                    ((ListType)valueDef.type).execute(cf, builder, fOp.fct, fOp.arguments, params, list);
                else
                    ((CollectionType)valueDef.type).execute(cf, builder, fOp.fct, fOp.arguments, params);

                return false;
        }
        throw new AssertionError();
    }

    public ParsedStatement.Prepared prepare(CFDefinition.Name[] boundNames) throws InvalidRequestException
    {
        boolean hasCommutativeOperation = false;

        if (columns != null)
        {
            for (Pair<ColumnIdentifier, Operation> column : columns)
            {
                if (column.right.type == Operation.Type.COUNTER)
                    hasCommutativeOperation = true;

                if (hasCommutativeOperation && column.right.type != Operation.Type.COUNTER)
                    throw new InvalidRequestException("Mix of counter and non-counter operations is not allowed.");
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

                Value value = columnValues.get(i);
                for (Term t : value.asList())
                    if (t.isBindMarker())
                        boundNames[t.bindIndex] = name;

                switch (name.kind)
                {
                    case KEY_ALIAS:
                    case COLUMN_ALIAS:
                        if (processedKeys.containsKey(name.name))
                            throw new InvalidRequestException(String.format("Multiple definitions found for PRIMARY KEY part %s", name));
                        if (!(value instanceof Term))
                            throw new InvalidRequestException(String.format("Invalid definition for %s, not a collection type", name));
                        processedKeys.put(name.name, Collections.singletonList((Term)value));
                        break;
                    case VALUE_ALIAS:
                    case COLUMN_METADATA:
                        if (processedColumns.containsKey(name))
                            throw new InvalidRequestException(String.format("Multiple definitions found for column %s", name));
                        processedColumns.put(name, new Operation.Set(value));
                        break;
                }
            }
        }
        else
        {
            // Created from an UPDATE
            for (Pair<ColumnIdentifier, Operation> entry : columns)
            {
                CFDefinition.Name name = cfDef.get(entry.left);
                if (name == null)
                    throw new InvalidRequestException(String.format("Unknown identifier %s", entry.left));

                switch (name.kind)
                {
                    case KEY_ALIAS:
                    case COLUMN_ALIAS:
                        throw new InvalidRequestException(String.format("PRIMARY KEY part %s found in SET part", entry.left));
                    case VALUE_ALIAS:
                    case COLUMN_METADATA:
                        for (Operation op : processedColumns.get(name))
                            if (op.type != Operation.Type.FUNCTION)
                                throw new InvalidRequestException(String.format("Multiple definitions found for column %s", name));

                        Operation op = entry.right;
                        for (Term t : op.allTerms())
                            if (t.isBindMarker())
                                boundNames[t.bindIndex] = name;
                        processedColumns.put(name, op);
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
                        throw new InvalidRequestException(String.format("Multiple definitions found for PRIMARY KEY part %s", name));
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
