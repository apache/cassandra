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
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.utils.Pair;

/**
 * A <code>DELETE</code> parsed from a CQL query statement.
 */
public class DeleteStatement extends ModificationStatement
{
    private DeleteStatement(int boundTerms, CFMetaData cfm, Attributes attrs)
    {
        super(boundTerms, cfm, attrs);
    }

    public boolean requireFullClusteringKey()
    {
        return false;
    }

    public ColumnFamily updateForKey(ByteBuffer key, Composite prefix, UpdateParameters params)
    throws InvalidRequestException
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfm);
        List<Operation> deletions = getOperations();

        if (prefix.size() < cfm.clusteringColumns().size() && !deletions.isEmpty())
            throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s since %s specified", getFirstEmptyKey(), deletions.iterator().next().column.name));

        if (deletions.isEmpty())
        {
            // We delete the slice selected by the prefix.
            // However, for performance reasons, we distinguish 2 cases:
            //   - It's a full internal row delete
            //   - It's a full cell name (i.e it's a dense layout and the prefix is full)
            if (prefix.isEmpty())
            {
                // No columns specified, delete the row
                cf.delete(new DeletionInfo(params.timestamp, params.localDeletionTime));
            }
            else if (cfm.comparator.isDense() && prefix.size() == cfm.clusteringColumns().size())
            {
                cf.addAtom(params.makeTombstone(cfm.comparator.create(prefix, null)));
            }
            else
            {
                cf.addAtom(params.makeRangeTombstone(prefix.slice()));
            }
        }
        else
        {
            for (Operation op : deletions)
                op.execute(key, cf, prefix, params);
        }

        return cf;
    }

    public static class Parsed extends ModificationStatement.Parsed
    {
        private final List<Operation.RawDeletion> deletions;
        private final List<Relation> whereClause;

        public Parsed(CFName name,
                      Attributes.Raw attrs,
                      List<Operation.RawDeletion> deletions,
                      List<Relation> whereClause,
                      List<Pair<ColumnIdentifier, Operation.RawUpdate>> conditions)
        {
            super(name, attrs, conditions, false);
            this.deletions = deletions;
            this.whereClause = whereClause;
        }

        protected ModificationStatement prepareInternal(CFMetaData cfm, VariableSpecifications boundNames, Attributes attrs) throws InvalidRequestException
        {
            DeleteStatement stmt = new DeleteStatement(boundNames.size(), cfm, attrs);

            for (Operation.RawDeletion deletion : deletions)
            {
                ColumnDefinition def = cfm.getColumnDefinition(deletion.affectedColumn());
                if (def == null)
                    throw new InvalidRequestException(String.format("Unknown identifier %s", deletion.affectedColumn()));

                // For compact, we only have one value except the key, so the only form of DELETE that make sense is without a column
                // list. However, we support having the value name for coherence with the static/sparse case
                if (def.kind != ColumnDefinition.Kind.REGULAR && def.kind != ColumnDefinition.Kind.COMPACT_VALUE)
                    throw new InvalidRequestException(String.format("Invalid identifier %s for deletion (should not be a PRIMARY KEY part)", def.name));

                Operation op = deletion.prepare(cfm.ksName, def);
                op.collectMarkerSpecification(boundNames);
                stmt.addOperation(op);
            }

            stmt.processWhereClause(whereClause, boundNames);
            return stmt;
        }
    }
}
