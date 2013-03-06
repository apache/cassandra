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
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql.QueryProcessor.validateKey;
import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

/**
 * An <code>UPDATE</code> statement parsed from a CQL query statement.
 *
 */
public class UpdateStatement extends ModificationStatement
{
    private CFDefinition cfDef;

    // Provided for an UPDATE
    private final List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations;
    private final List<Relation> whereClause;

    // Provided for an INSERT
    private final List<ColumnIdentifier> columnNames;
    private final List<Term.Raw> columnValues;

    private final List<Operation> processedColumns = new ArrayList<Operation>();
    private final Map<ColumnIdentifier, List<Term>> processedKeys = new HashMap<ColumnIdentifier, List<Term>>();

    private static final Operation setToEmptyOperation = new Constants.Setter(null, new Constants.Value(ByteBufferUtil.EMPTY_BYTE_BUFFER));

    /**
     * Creates a new UpdateStatement from a column family name, columns map, consistency
     * level, and key term.
     *
     * @param name column family being operated on
     * @param operations a map of column operations to perform
     * @param whereClause the where clause
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    public UpdateStatement(CFName name,
                           List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations,
                           List<Relation> whereClause,
                           Attributes attrs)
    {
        super(name, attrs);
        this.operations = operations;
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
                           List<Term.Raw> columnValues)
    {
        super(name, attrs);
        this.columnNames = columnNames;
        this.columnValues = columnValues;
        this.operations = null;
        this.whereClause = null;
    }

    protected void validateConsistency(ConsistencyLevel cl) throws InvalidRequestException
    {
        if (type == Type.COUNTER)
            cl.validateCounterForWrite(cfDef.cfm);
        else
            cl.validateForWrite(cfDef.cfm.ksName);
    }

    /** {@inheritDoc} */
    public Collection<IMutation> getMutations(List<ByteBuffer> variables, boolean local, ConsistencyLevel cl, long now)
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> keys = buildKeyNames(cfDef, processedKeys, variables);

        ColumnNameBuilder builder = cfDef.getColumnNameBuilder();
        buildColumnNames(cfDef, processedKeys, builder, variables, true);

        // Lists SET operation incurs a read.
        Set<ByteBuffer> toRead = null;
        for (Operation op : processedColumns)
        {
            if (op.requiresRead())
            {
                if (toRead == null)
                    toRead = new TreeSet<ByteBuffer>(UTF8Type.instance);
                toRead.add(op.columnName.key);
            }
        }

        Map<ByteBuffer, ColumnGroupMap> rows = toRead != null ? readRows(keys, builder, toRead, (CompositeType)cfDef.cfm.comparator, local, cl) : null;

        Collection<IMutation> mutations = new LinkedList<IMutation>();
        UpdateParameters params = new UpdateParameters(variables, getTimestamp(now), getTimeToLive(), rows);

        for (ByteBuffer key: keys)
            mutations.add(mutationForKey(cfDef, key, builder, params, cl));

        return mutations;
    }

    // Returns the first empty component or null if none are
    static CFDefinition.Name buildColumnNames(CFDefinition cfDef, Map<ColumnIdentifier, List<Term>> processed, ColumnNameBuilder builder, List<ByteBuffer> variables, boolean requireAllComponent)
    throws InvalidRequestException
    {
        CFDefinition.Name firstEmpty = null;
        for (CFDefinition.Name name : cfDef.columns.values())
        {
            List<Term> values = processed.get(name.name);
            if (values == null || values.isEmpty())
            {
                firstEmpty = name;
                if (requireAllComponent && cfDef.isComposite && !cfDef.isCompact)
                    throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", name));
            }
            else if (firstEmpty != null)
            {
                throw new InvalidRequestException(String.format("Missing PRIMARY KEY part %s since %s is set", firstEmpty.name, name.name));
            }
            else
            {
                assert values.size() == 1; // We only allow IN for row keys so far
                ByteBuffer val = values.get(0).bindAndGet(variables);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null value for clustering key part %s", name));
                builder.add(val);
            }
        }
        return firstEmpty;
    }

    static List<ByteBuffer> buildKeyNames(CFDefinition cfDef, Map<ColumnIdentifier, List<Term>> processed, List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        ColumnNameBuilder keyBuilder = cfDef.getKeyNameBuilder();
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        for (CFDefinition.Name name : cfDef.keys.values())
        {
            List<Term> values = processed.get(name.name);
            if (values == null || values.isEmpty())
                throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", name));

            if (keyBuilder.remainingCount() == 1)
            {
                for (Term t : values)
                {
                    ByteBuffer val = t.bindAndGet(variables);
                    if (val == null)
                        throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", name));
                    keys.add(keyBuilder.copy().add(val).build());
                }
            }
            else
            {
                if (values.size() > 1)
                    throw new InvalidRequestException("IN is only supported on the last column of the partition key");
                ByteBuffer val = values.get(0).bindAndGet(variables);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", name));
                keyBuilder.add(val);
            }
        }
        return keys;
    }

    /**
     * Compute a row mutation for a single key
     *
     * @return row mutation
     *
     * @throws InvalidRequestException on the wrong request
     */
    private IMutation mutationForKey(CFDefinition cfDef, ByteBuffer key, ColumnNameBuilder builder, UpdateParameters params, ConsistencyLevel cl)
    throws InvalidRequestException
    {
        validateKey(key);

        QueryProcessor.validateKey(key);
        RowMutation rm = new RowMutation(cfDef.cfm.ksName, key);
        ColumnFamily cf = rm.addOrGet(cfDef.cfm.cfName);

        // Inserting the CQL row marker (see #4361)
        // We always need to insert a marker, because of the following situation:
        //   CREATE TABLE t ( k int PRIMARY KEY, c text );
        //   INSERT INTO t(k, c) VALUES (1, 1)
        //   DELETE c FROM t WHERE k = 1;
        //   SELECT * FROM t;
        // The last query should return one row (but with c == null). Adding
        // the marker with the insert make sure the semantic is correct (while making sure a
        // 'DELETE FROM t WHERE k = 1' does remove the row entirely)
        if (cfDef.isComposite && !cfDef.isCompact)
        {
            ByteBuffer name = builder.copy().add(ByteBufferUtil.EMPTY_BYTE_BUFFER).build();
            cf.addColumn(params.makeColumn(name, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        }

        if (cfDef.isCompact)
        {
            if (builder.componentCount() == 0)
                throw new InvalidRequestException(String.format("Missing PRIMARY KEY part %s", cfDef.columns.values().iterator().next()));

            if (cfDef.value == null)
            {
                // compact + no compact value implies there is no column outside the PK. So no operation could
                // have passed through validation
                assert processedColumns.isEmpty();
                setToEmptyOperation.execute(key, cf, builder.copy(), params);
            }
            else
            {
                // compact means we don't have a row marker, so don't accept to set only the PK (Note: we
                // could accept it and use an empty value!?)
                if (processedColumns.isEmpty())
                    throw new InvalidRequestException(String.format("Missing mandatory column %s", cfDef.value));

                for (Operation op : processedColumns)
                    op.execute(key, cf, builder.copy(), params);
            }
        }
        else
        {
            for (Operation op : processedColumns)
                op.execute(key, cf, builder.copy(), params);
        }

        return type == Type.COUNTER ? new CounterMutation(rm, cl) : rm;
    }

    public ParsedStatement.Prepared prepare(ColumnSpecification[] boundNames) throws InvalidRequestException
    {
        // Deal here with the keyspace overwrite thingy to avoid mistake
        CFMetaData metadata = validateColumnFamily(keyspace(), columnFamily());
        cfDef = metadata.getCfDef();

        type = metadata.getDefaultValidator().isCommutative() ? Type.COUNTER : Type.LOGGED;

        if (operations == null)
        {
            // Created from an INSERT
            if (type == Type.COUNTER)
                throw new InvalidRequestException("INSERT statement are not allowed on counter tables, use UPDATE instead");
            if (columnNames.size() != columnValues.size())
                throw new InvalidRequestException("Unmatched column names/values");
            if (columnNames.isEmpty())
                throw new InvalidRequestException("No columns provided to INSERT");

            for (int i = 0; i < columnNames.size(); i++)
            {
                CFDefinition.Name name = cfDef.get(columnNames.get(i));
                if (name == null)
                    throw new InvalidRequestException(String.format("Unknown identifier %s", columnNames.get(i)));

                // For UPDATES, the parser validates we don't set the same value twice but we must check it here for INSERT
                for (int j = 0; j < i; j++)
                    if (name.name.equals(columnNames.get(j)))
                        throw new InvalidRequestException(String.format("Multiple definitions found for column %s", name));

                Term.Raw value = columnValues.get(i);

                switch (name.kind)
                {
                    case KEY_ALIAS:
                    case COLUMN_ALIAS:
                        Term t = value.prepare(name);
                        t.collectMarkerSpecification(boundNames);
                        if (processedKeys.put(name.name, Collections.singletonList(t)) != null)
                            throw new InvalidRequestException(String.format("Multiple definitions found for PRIMARY KEY part %s", name));
                        break;
                    case VALUE_ALIAS:
                    case COLUMN_METADATA:
                        Operation operation = new Operation.SetValue(value).prepare(name);
                        operation.collectMarkerSpecification(boundNames);
                        processedColumns.add(operation);
                        break;
                }
            }
        }
        else
        {
            // Created from an UPDATE
            for (Pair<ColumnIdentifier, Operation.RawUpdate> entry : operations)
            {
                CFDefinition.Name name = cfDef.get(entry.left);
                if (name == null)
                    throw new InvalidRequestException(String.format("Unknown identifier %s", entry.left));

                Operation operation = entry.right.prepare(name);
                operation.collectMarkerSpecification(boundNames);

                switch (name.kind)
                {
                    case KEY_ALIAS:
                    case COLUMN_ALIAS:
                        throw new InvalidRequestException(String.format("PRIMARY KEY part %s found in SET part", entry.left));
                    case VALUE_ALIAS:
                    case COLUMN_METADATA:
                        processedColumns.add(operation);
                        break;
                }
            }
            processKeys(cfDef, whereClause, processedKeys, boundNames);
        }

        return new ParsedStatement.Prepared(this, Arrays.<ColumnSpecification>asList(boundNames));
    }

    public ParsedStatement.Prepared prepare() throws InvalidRequestException
    {
        ColumnSpecification[] names = new ColumnSpecification[getBoundsTerms()];
        return prepare(names);
    }

    // Reused by DeleteStatement
    static void processKeys(CFDefinition cfDef, List<Relation> keys, Map<ColumnIdentifier, List<Term>> processed, ColumnSpecification[] names) throws InvalidRequestException
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
                    List<Term.Raw> rawValues;
                    if (rel.operator() == Relation.Type.EQ)
                        rawValues = Collections.singletonList(rel.getValue());
                    else if (name.kind == CFDefinition.Name.Kind.KEY_ALIAS && rel.operator() == Relation.Type.IN)
                        rawValues = rel.getInValues();
                    else
                        throw new InvalidRequestException(String.format("Invalid operator %s for PRIMARY KEY part %s", rel.operator(), name));

                    List<Term> values = new ArrayList<Term>(rawValues.size());
                    for (Term.Raw raw : rawValues)
                    {
                        Term t = raw.prepare(name);
                        t.collectMarkerSpecification(names);
                        values.add(t);
                    }

                    if (processed.put(name.name, values) != null)
                        throw new InvalidRequestException(String.format("Multiple definitions found for PRIMARY KEY part %s", name));
                    break;
                case VALUE_ALIAS:
                case COLUMN_METADATA:
                    throw new InvalidRequestException(String.format("Non PRIMARY KEY %s found in where clause", name));
            }
        }
    }

    public String toString()
    {
        return String.format("UpdateStatement(name=%s, keys=%s, columns=%s, timestamp=%s, timeToLive=%s)",
                             cfName,
                             whereClause,
                             operations,
                             isSetTimestamp() ? getTimestamp(-1) : "<now>",
                             getTimeToLive());
    }
}
