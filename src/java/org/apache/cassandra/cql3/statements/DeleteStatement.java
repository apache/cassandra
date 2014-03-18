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
import org.apache.cassandra.utils.Pair;

/**
 * A <code>DELETE</code> parsed from a CQL query statement.
 */
public class DeleteStatement extends ModificationStatement
{
    private DeleteStatement(StatementType type, CFMetaData cfm, Attributes attrs)
    {
        super(type, cfm, attrs);
    }

    public boolean requireFullClusteringKey()
    {
        return false;
    }

    public void addUpdateForKey(ColumnFamily cf, ByteBuffer key, ColumnNameBuilder builder, UpdateParameters params)
    throws InvalidRequestException
    {
        CFDefinition cfDef = cfm.getCfDef();
        List<Operation> deletions = getOperations();

        boolean fullKey = builder.componentCount() == cfDef.clusteringColumnsCount();
        boolean isRange = cfDef.isCompact ? !fullKey : (!fullKey || deletions.isEmpty());

        if (!deletions.isEmpty() && isRange)
        {
            // We only get there if we have at least one non-static columns selected, as otherwise the builder will be
            // the "static" builder and isRange will be false. But we may still have static columns, so pick the first
            // non static one for the error message so it's not confusing
            for (Operation deletion : deletions)
                if (cfm.getCfDef().get(deletion.columnName).kind != CFDefinition.Name.Kind.STATIC)
                    throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s since %s specified", getFirstEmptyKey(), deletion.columnName));
            throw new AssertionError();
        }

        if (deletions.isEmpty() && builder.componentCount() == 0)
        {
            // No columns specified, delete the row
            cf.delete(new DeletionInfo(params.timestamp, params.localDeletionTime));
        }
        else
        {
            if (isRange)
            {
                assert deletions.isEmpty();
                ByteBuffer start = builder.build();
                ByteBuffer end = builder.buildAsEndOfRange();
                cf.addAtom(params.makeRangeTombstone(start, end));
            }
            else
            {
                // Delete specific columns
                if (cfDef.isCompact)
                {
                    ByteBuffer columnName = builder.build();
                    cf.addColumn(params.makeTombstone(columnName));
                }
                else
                {
                    for (Operation deletion : deletions)
                        deletion.execute(key, cf, builder.copy(), params);
                }
            }
        }
    }

    public static class Parsed extends ModificationStatement.Parsed
    {
        private final List<Operation.RawDeletion> deletions;
        private final List<Relation> whereClause;

        public Parsed(CFName name,
                      Attributes.Raw attrs,
                      List<Operation.RawDeletion> deletions,
                      List<Relation> whereClause,
                      List<Pair<ColumnIdentifier, ColumnCondition.Raw>> conditions,
                      boolean ifExists)
        {
            super(name, attrs, conditions, false, ifExists);
            this.deletions = deletions;
            this.whereClause = whereClause;
        }

        protected ModificationStatement prepareInternal(CFDefinition cfDef, VariableSpecifications boundNames, Attributes attrs) throws InvalidRequestException
        {
            DeleteStatement stmt = new DeleteStatement(ModificationStatement.StatementType.DELETE, cfDef.cfm, attrs);

            for (Operation.RawDeletion deletion : deletions)
            {
                CFDefinition.Name name = cfDef.get(deletion.affectedColumn());
                if (name == null)
                    throw new InvalidRequestException(String.format("Unknown identifier %s", deletion.affectedColumn()));

                // For compact, we only have one value except the key, so the only form of DELETE that make sense is without a column
                // list. However, we support having the value name for coherence with the static/sparse case
                if (name.isPrimaryKeyColumn())
                    throw new InvalidRequestException(String.format("Invalid identifier %s for deletion (should not be a PRIMARY KEY part)", name));

                Operation op = deletion.prepare(name);
                op.collectMarkerSpecification(boundNames);
                stmt.addOperation(op);
            }

            stmt.processWhereClause(whereClause, boundNames);
            return stmt;
        }
    }
}
