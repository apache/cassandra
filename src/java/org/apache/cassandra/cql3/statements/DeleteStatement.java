/*
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
 */
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.ThriftValidation;

/**
 * A <code>DELETE</code> parsed from a CQL query statement.
 */
public class DeleteStatement extends ModificationStatement
{
    private CFDefinition cfDef;
    private final List<ColumnIdentifier> columns;
    private final List<Relation> whereClause;

    private final Map<ColumnIdentifier, List<Term>> processedKeys = new HashMap<ColumnIdentifier, List<Term>>();

    public DeleteStatement(CFName name, List<ColumnIdentifier> columns, List<Relation> whereClause, Attributes attrs)
    {
        super(name, attrs);

        this.columns = columns;
        this.whereClause = whereClause;
    }

    public List<IMutation> getMutations(ClientState clientState, List<ByteBuffer> variables) throws InvalidRequestException
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
                // For composites, we must either have all component or none
                if (cfDef.isComposite && builder.componentCount() != 0)
                    throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", name));
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

        List<IMutation> rowMutations = new ArrayList<IMutation>(keys.size());

        for (Term key : keys)
        {
            ByteBuffer rawKey = key.getByteBuffer(cfDef.key.type, variables);
            rowMutations.add(mutationForKey(cfDef, clientState, rawKey, builder, variables));
        }

        return rowMutations;
    }

    public RowMutation mutationForKey(CFDefinition cfDef, ClientState clientState, ByteBuffer key, ColumnNameBuilder builder, List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        QueryProcessor.validateKey(key);
        RowMutation rm = new RowMutation(cfDef.cfm.ksName, key);

        if (columns.isEmpty() && builder.componentCount() == 0)
        {
            // No columns, delete the row
            rm.delete(new QueryPath(columnFamily()), getTimestamp(clientState));
        }
        else
        {
            for (ColumnIdentifier column : columns)
            {
                CFDefinition.Name name = cfDef.get(column);
                if (name == null)
                    throw new InvalidRequestException(String.format("Unknown identifier %s", column));

                // For compact, we only have one value except the key, so the only form of DELETE that make sense is without a column
                // list. However, we support having the value name for coherence with the static/sparse case
                if (name.kind != CFDefinition.Name.Kind.COLUMN_METADATA && name.kind != CFDefinition.Name.Kind.VALUE_ALIAS)
                    throw new InvalidRequestException(String.format("Invalid identifier %s for deletion (should not be a PRIMARY KEY part)", column));
            }

            if (cfDef.isCompact)
            {
                    ByteBuffer columnName = builder.build();
                    QueryProcessor.validateColumnName(columnName);
                    rm.delete(new QueryPath(columnFamily(), null, columnName), getTimestamp(clientState));
            }
            else
            {
                Iterator<ColumnIdentifier> iter;
                if (columns.isEmpty())
                    // It's a DELETE *, remove all columns individually (#3708 will replace that by a single range tombstone)
                    iter = cfDef.metadata.keySet().iterator();
                else
                    // Delete specific columns
                    iter = columns.iterator();

                while (iter.hasNext())
                {
                    ColumnIdentifier column = iter.next();
                    ColumnNameBuilder b = iter.hasNext() ? builder.copy() : builder;
                    ByteBuffer columnName = b.add(column.key).build();
                    QueryProcessor.validateColumnName(columnName);
                    rm.delete(new QueryPath(columnFamily(), null, columnName), getTimestamp(clientState));
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
        return new ParsedStatement.Prepared(this, Arrays.<CFDefinition.Name>asList(boundNames));
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
