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
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

/**
 * An <code>UPDATE</code> statement parsed from a CQL query statement.
 *
 */
public class UpdateStatement extends ModificationStatement
{
    private static final Operation setToEmptyOperation = new Constants.Setter(null, new Constants.Value(ByteBufferUtil.EMPTY_BYTE_BUFFER));

    private UpdateStatement(StatementType type, CFMetaData cfm, Attributes attrs)
    {
        super(type, cfm, attrs);
    }

    public boolean requireFullClusteringKey()
    {
        return true;
    }

    public void addUpdateForKey(ColumnFamily cf, ByteBuffer key, ColumnNameBuilder builder, UpdateParameters params)
    throws InvalidRequestException
    {
        CFDefinition cfDef = cfm.getCfDef();

        // Inserting the CQL row marker (see #4361)
        // We always need to insert a marker for INSERT, because of the following situation:
        //   CREATE TABLE t ( k int PRIMARY KEY, c text );
        //   INSERT INTO t(k, c) VALUES (1, 1)
        //   DELETE c FROM t WHERE k = 1;
        //   SELECT * FROM t;
        // The last query should return one row (but with c == null). Adding the marker with the insert make sure
        // the semantic is correct (while making sure a 'DELETE FROM t WHERE k = 1' does remove the row entirely)
        //
        // We do not insert the marker for UPDATE however, as this amount to updating the columns in the WHERE
        // clause which is inintuitive (#6782)
        //
        // We never insert markers for Super CF as this would confuse the thrift side.
        if (type == StatementType.INSERT && cfDef.isComposite && !cfDef.isCompact && !cfm.isSuper())
        {
            ByteBuffer name = builder.copy().add(ByteBufferUtil.EMPTY_BYTE_BUFFER).build();
            cf.addColumn(params.makeColumn(name, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        }

        List<Operation> updates = getOperations();

        if (cfDef.isCompact)
        {
            if (builder.componentCount() == 0)
                throw new InvalidRequestException(String.format("Missing PRIMARY KEY part %s", cfDef.clusteringColumns().iterator().next()));

            if (cfDef.compactValue() == null)
            {
                // compact + no compact value implies there is no column outside the PK. So no operation could
                // have passed through validation
                assert updates.isEmpty();
                setToEmptyOperation.execute(key, cf, builder.copy(), params);
            }
            else
            {
                // compact means we don't have a row marker, so don't accept to set only the PK. See CASSANDRA-5648.
                if (updates.isEmpty())
                    throw new InvalidRequestException(String.format("Column %s is mandatory for this COMPACT STORAGE table", cfDef.compactValue()));

                for (Operation update : updates)
                    update.execute(key, cf, builder.copy(), params);
            }
        }
        else
        {
            for (Operation update : updates)
                update.execute(key, cf, builder.copy(), params);
        }
    }

    public static class ParsedInsert extends ModificationStatement.Parsed
    {
        private final List<ColumnIdentifier> columnNames;
        private final List<Term.Raw> columnValues;

        /**
         * A parsed <code>INSERT</code> statement.
         *
         * @param name column family being operated on
         * @param columnNames list of column names
         * @param columnValues list of column values (corresponds to names)
         * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
         */
        public ParsedInsert(CFName name,
                            Attributes.Raw attrs,
                            List<ColumnIdentifier> columnNames, List<Term.Raw> columnValues,
                            boolean ifNotExists)
        {
            super(name, attrs, null, ifNotExists, false);
            this.columnNames = columnNames;
            this.columnValues = columnValues;
        }

        protected ModificationStatement prepareInternal(CFDefinition cfDef, VariableSpecifications boundNames, Attributes attrs) throws InvalidRequestException
        {
            UpdateStatement stmt = new UpdateStatement(ModificationStatement.StatementType.INSERT, cfDef.cfm, attrs);

            // Created from an INSERT
            if (stmt.isCounter())
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
                        stmt.addKeyValue(name, t);
                        break;
                    case VALUE_ALIAS:
                    case COLUMN_METADATA:
                    case STATIC:
                        Operation operation = new Operation.SetValue(value).prepare(name);
                        operation.collectMarkerSpecification(boundNames);
                        stmt.addOperation(operation);
                        break;
                }
            }
            return stmt;
        }
    }

    public static class ParsedUpdate extends ModificationStatement.Parsed
    {
        // Provided for an UPDATE
        private final List<Pair<ColumnIdentifier, Operation.RawUpdate>> updates;
        private final List<Relation> whereClause;

        /**
         * Creates a new UpdateStatement from a column family name, columns map, consistency
         * level, and key term.
         *
         * @param name column family being operated on
         * @param attrs additional attributes for statement (timestamp, timeToLive)
         * @param updates a map of column operations to perform
         * @param whereClause the where clause
         */
        public ParsedUpdate(CFName name,
                            Attributes.Raw attrs,
                            List<Pair<ColumnIdentifier, Operation.RawUpdate>> updates,
                            List<Relation> whereClause,
                            List<Pair<ColumnIdentifier, ColumnCondition.Raw>> conditions)
        {
            super(name, attrs, conditions, false, false);
            this.updates = updates;
            this.whereClause = whereClause;
        }

        protected ModificationStatement prepareInternal(CFDefinition cfDef, VariableSpecifications boundNames, Attributes attrs) throws InvalidRequestException
        {
            UpdateStatement stmt = new UpdateStatement(ModificationStatement.StatementType.UPDATE, cfDef.cfm, attrs);

            for (Pair<ColumnIdentifier, Operation.RawUpdate> entry : updates)
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
                    case STATIC:
                        stmt.addOperation(operation);
                        break;
                }
            }

            stmt.processWhereClause(whereClause, boundNames);
            return stmt;
        }
    }
}
