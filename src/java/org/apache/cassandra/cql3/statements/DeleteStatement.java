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
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.operations.ListOperation;
import org.apache.cassandra.cql3.operations.MapOperation;
import org.apache.cassandra.cql3.operations.Operation;
import org.apache.cassandra.cql3.operations.SetOperation;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.utils.Pair;

/**
 * A <code>DELETE</code> parsed from a CQL query statement.
 */
public class DeleteStatement extends ModificationStatement
{
    private CFDefinition cfDef;
    private final List<Selector> columns;
    private final List<Relation> whereClause;

    private final List<Pair<CFDefinition.Name, Term>> toRemove;

    private final Map<ColumnIdentifier, List<Term>> processedKeys = new HashMap<ColumnIdentifier, List<Term>>();

    public DeleteStatement(CFName name, List<Selector> columns, List<Relation> whereClause, Attributes attrs)
    {
        super(name, attrs);

        this.columns = columns;
        this.whereClause = whereClause;
        this.toRemove = new ArrayList<Pair<CFDefinition.Name, Term>>(columns.size());
    }

    public List<IMutation> getMutations(ClientState clientState, List<ByteBuffer> variables) throws UnavailableException, TimeoutException, InvalidRequestException
    {
        // Check key
        List<Term> keys = processedKeys.get(cfDef.key.name);
        if (keys == null || keys.isEmpty())
            throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", cfDef.key.name));

        ColumnNameBuilder builder = cfDef.getColumnNameBuilder();
        CFDefinition.Name firstEmpty = null;
        for (CFDefinition.Name name : cfDef.columns.values())
        {
            List<Term> values = processedKeys.get(name.name);
            if (values == null || values.isEmpty())
            {
                firstEmpty = name;
            }
            else if (firstEmpty != null)
            {
                throw new InvalidRequestException(String.format("Missing PRIMARY KEY part %s since %s is set", firstEmpty, name));
            }
            else
            {
                assert values.size() == 1; // We only allow IN for keys so far
                builder.add(values.get(0), Relation.Type.EQ, variables);
            }
        }

        boolean fullKey = builder.componentCount() == cfDef.columns.size();
        boolean isRange = cfDef.isCompact ? !fullKey : (!fullKey || toRemove.isEmpty());

        if (!toRemove.isEmpty() && isRange)
            throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s since %s specified", firstEmpty, toRemove.iterator().next().left));

        // Lists DISCARD operation incurs a read. Do that now.
        boolean needsReading = false;
        for (Pair<CFDefinition.Name, Term> p : toRemove)
        {
            CFDefinition.Name name = p.left;
            Term value = p.right;

            if ((name.type instanceof ListType) && value != null)
            {
                needsReading = true;
                break;
            }
        }

        List<ByteBuffer> rawKeys = new ArrayList<ByteBuffer>(keys.size());
        for (Term key: keys)
            rawKeys.add(key.getByteBuffer(cfDef.key.type, variables));

        Map<ByteBuffer, ColumnGroupMap> rows = needsReading ? readRows(rawKeys, builder, (CompositeType)cfDef.cfm.comparator) : null;

        List<IMutation> rowMutations = new ArrayList<IMutation>(keys.size());
        UpdateParameters params = new UpdateParameters(variables, getTimestamp(clientState), -1);

        for (ByteBuffer key : rawKeys)
            rowMutations.add(mutationForKey(cfDef, key, builder, isRange, params, rows == null ? null : rows.get(key)));

        return rowMutations;
    }

    public RowMutation mutationForKey(CFDefinition cfDef, ByteBuffer key, ColumnNameBuilder builder, boolean isRange, UpdateParameters params, ColumnGroupMap group)
    throws InvalidRequestException
    {
        QueryProcessor.validateKey(key);
        RowMutation rm = new RowMutation(cfDef.cfm.ksName, key);
        ColumnFamily cf = rm.addOrGet(columnFamily());

        if (columns.isEmpty() && builder.componentCount() == 0)
        {
            // No columns, delete the row
            cf.delete(new DeletionInfo(params.timestamp, params.localDeletionTime));
        }
        else
        {
            if (isRange)
            {
                ByteBuffer start = builder.copy().build();
                ByteBuffer end = builder.buildAsEndOfRange();
                QueryProcessor.validateColumnName(start); // If start is good, end is too
                cf.addAtom(params.makeRangeTombstone(start, end));
            }
            else
            {
                // Delete specific columns
                if (cfDef.isCompact)
                {
                    ByteBuffer columnName = builder.build();
                    QueryProcessor.validateColumnName(columnName);
                    cf.addColumn(params.makeTombstone(columnName));
                }
                else
                {
                    Iterator<Pair<CFDefinition.Name, Term>> iter = toRemove.iterator();
                    while (iter.hasNext())
                    {
                        Pair<CFDefinition.Name, Term> p = iter.next();
                        CFDefinition.Name column = p.left;

                        if (column.type.isCollection())
                        {
                            CollectionType validator = (CollectionType) column.type;
                            Term keySelected = p.right;

                            if (keySelected == null)
                            {
                                // Delete the whole collection
                                ByteBuffer start = builder.copy().add(column.name.key).build();
                                QueryProcessor.validateColumnName(start);
                                ColumnNameBuilder b = iter.hasNext() ? builder.copy() : builder;
                                ByteBuffer end = b.add(column.name.key).buildAsEndOfRange();
                                cf.addAtom(params.makeRangeTombstone(start, end));
                            }
                            else
                            {
                                builder.add(column.name.key);
                                List<Term> args = Collections.singletonList(keySelected);

                                Operation op;
                                switch (validator.kind)
                                {
                                    case LIST:
                                        op = ListOperation.DiscardKey(args);
                                        break;
                                    case SET:
                                        op = SetOperation.Discard(args);
                                        break;
                                    case MAP:
                                        op = MapOperation.DiscardKey(keySelected);
                                        break;
                                    default:
                                        throw new InvalidRequestException("Unknown collection type: " + validator.kind);
                                }

                                op.execute(cf, builder, validator, params, group == null ? null : group.getCollection(column.name.key));
                            }
                        }
                        else
                        {
                            ColumnNameBuilder b = iter.hasNext() ? builder.copy() : builder;
                            ByteBuffer columnName = b.add(column.name.key).build();
                            QueryProcessor.validateColumnName(columnName);
                            cf.addColumn(params.makeTombstone(columnName));
                        }
                    }
                }
            }
        }

        return rm;
    }

    public ParsedStatement.Prepared prepare(CFDefinition.Name[] boundNames) throws InvalidRequestException
    {
        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
        cfDef = metadata.getCfDef();
        UpdateStatement.processKeys(cfDef, whereClause, processedKeys, boundNames);

        for (Selector column : columns)
        {
            CFDefinition.Name name = cfDef.get(column.id());
            if (name == null)
                throw new InvalidRequestException(String.format("Unknown identifier %s", column));

            // For compact, we only have one value except the key, so the only form of DELETE that make sense is without a column
            // list. However, we support having the value name for coherence with the static/sparse case
            if (name.kind != CFDefinition.Name.Kind.COLUMN_METADATA && name.kind != CFDefinition.Name.Kind.VALUE_ALIAS)
                throw new InvalidRequestException(String.format("Invalid identifier %s for deletion (should not be a PRIMARY KEY part)", column));

            if (column.key() != null && !(name.type instanceof ListType || name.type instanceof MapType))
                throw new InvalidRequestException(String.format("Invalid selection %s since %s is neither a list or a map", column, column.id()));

            toRemove.add(Pair.create(name, column.key()));
        }

        return new ParsedStatement.Prepared(this, Arrays.<ColumnSpecification>asList(boundNames));
    }

    public ParsedStatement.Prepared prepare() throws InvalidRequestException
    {
        CFDefinition.Name[] boundNames = new CFDefinition.Name[getBoundsTerms()];
        return prepare(boundNames);
    }

    public String toString()
    {
        return String.format("DeleteStatement(name=%s, columns=%s, consistency=%s keys=%s)",
                             cfName,
                             columns,
                             cLevel,
                             whereClause);
    }
}
