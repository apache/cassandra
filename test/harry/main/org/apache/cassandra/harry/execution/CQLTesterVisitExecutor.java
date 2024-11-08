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

package org.apache.cassandra.harry.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.model.Model;

import static org.apache.cassandra.harry.MagicConstants.*;

public class CQLTesterVisitExecutor extends CQLVisitExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(CQLTesterVisitExecutor.class);
    private final Function<CompiledStatement, UntypedResultSet> execute;

    public CQLTesterVisitExecutor(SchemaSpec schema,
                                  DataTracker dataTracker,
                                  Model model,
                                  Function<CompiledStatement, UntypedResultSet> execute)
    {
        super(schema, dataTracker, model, new QueryBuildingVisitExecutor(schema, QueryBuildingVisitExecutor.WrapQueries.UNLOGGED_BATCH));
        this.execute = execute;
    }

    @Override
    public List<ResultSetRow> executeWithResult(Visit visit, CompiledStatement statement)
    {
        List<ResultSetRow> actual = new ArrayList<>();
        // TODO: Have never tested with multiple
        Invariants.checkState(visit.operations.length == 1);
        for (UntypedResultSet.Row row : execute.apply(statement))
            actual.add(resultSetToRow(schema, (Operations.SelectStatement) visit.operations[0], row));
        return actual;
    }

    protected void executeWithoutResult(Visit visit, CompiledStatement statement)
    {
        execute.apply(statement);
    }

    public static ResultSetRow resultSetToRow(SchemaSpec schema, Operations.SelectStatement select, UntypedResultSet.Row row)
    {
        // TODO: do we want to use selection?
        Operations.Selection selection = Operations.Selection.fromBitSet(select.selection(), schema);

        long pd = UNKNOWN_DESCR;
        if (selection.selectsAllOf(schema.partitionKeys))
        {
            Object[] partitionKey = new Object[schema.partitionKeys.size()];
            for (int i = 0; i < schema.partitionKeys.size(); i++)
            {
                ColumnSpec<?> column = schema.partitionKeys.get(i);
                partitionKey[i] = column.type.asServerType().compose(row.getBytes(column.name));
            }

            pd = schema.valueGenerators.pkGen.deflate(partitionKey);
        }


        long cd = UNKNOWN_DESCR;
        if (selection.selectsAllOf(schema.clusteringKeys))
        {
            Object[] clusteringKey = new Object[schema.clusteringKeys.size()];
            for (int i = 0; i < schema.clusteringKeys.size(); i++)
            {
                ColumnSpec<?> column = schema.clusteringKeys.get(i);
                if (row.has(column.name))
                {
                    clusteringKey[i] = column.type.asServerType().compose(row.getBytes(column.name));
                }
                else
                {
                    for (int j = 0; j < schema.clusteringKeys.size(); j++)
                    {
                        Invariants.checkState(!row.has(schema.clusteringKeys.get(j).name),
                                              "All elements of clustering key should have been null");
                    }
                    clusteringKey = NIL_KEY;
                    break;
                }
            }

            // Clusterings can not be set to nil, so if we do not see, we assume it is unset
            if (clusteringKey == NIL_KEY)
                cd = UNSET_DESCR;
            else
                cd = schema.valueGenerators.ckGen.deflate(clusteringKey);
        }

        long[] regularColumns = new long[schema.regularColumns.size()];
        for (int i = 0; i < schema.regularColumns.size(); i++)
        {
            ColumnSpec<?> column = schema.regularColumns.get(i);
            if (selection.selects(column))
            {
                if (row.has(column.name))
                {
                    Object value = column.type.asServerType().compose(row.getBytes(column.name));
                    regularColumns[i] = schema.valueGenerators.regularColumnGens.get(i).deflate(value);
                }
                else
                {
                    regularColumns[i] = NIL_DESCR;
                }
            }
            else
            {
                regularColumns[i] = UNKNOWN_DESCR;
            }
        }

        long[] staticColumns = new long[schema.staticColumns.size()];
        for (int i = 0; i < schema.staticColumns.size(); i++)
        {
            ColumnSpec<?> column = schema.staticColumns.get(i);
            if (selection.selects(column))
            {
                if (row.has(column.name))
                {
                    Object value = column.type.asServerType().compose(row.getBytes(column.name));
                    staticColumns[i] = schema.valueGenerators.staticColumnGens.get(i).deflate(value);
                }
                else
                {
                    staticColumns[i] = NIL_DESCR;
                }
            }
            else
            {
                staticColumns[i] = UNKNOWN_DESCR;
            }
        }

        long[] regularLts = LTS_UNKNOWN;
        if (selection.includeTimestamps())
            throw new IllegalStateException("not implemented for CQL Tester");
        long[] staticLts = LTS_UNKNOWN;
        if (selection.includeTimestamps())
            throw new IllegalStateException("not implemented for CQL Tester");

        return new ResultSetRow(pd, cd, staticColumns, staticLts, regularColumns, regularLts);
    }
}
