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

package org.apache.cassandra.harry.cql;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.cql3.ast.Bind;
import org.apache.cassandra.cql3.ast.FunctionCall;
import org.apache.cassandra.cql3.ast.Reference;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.cql3.ast.Where;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.Relations;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.execution.CompiledStatement;

public class SelectHelper
{
    public static CompiledStatement select(Operations.SelectPartition select, SchemaSpec schema)
    {
        Select.Builder builder = commmonPart(select, schema);

        if (select.orderBy() == Operations.ClusteringOrderBy.DESC)
        {
            for (int i = 0; i < schema.clusteringKeys.size(); i++)
            {
                ColumnSpec<?> c = schema.clusteringKeys.get(i);
                builder.withOrderByColumn(c.name, c.type.asServerType(), c.isReversed() ? Select.OrderBy.Ordering.ASC : Select.OrderBy.Ordering.DESC);
            }
        }

        return toCompiled(builder.build());
    }

    public static CompiledStatement select(Operations.SelectRow select, SchemaSpec schema)
    {
        Select.Builder builder = commmonPart(select, schema);

        Object[] ck = schema.valueGenerators.ckGen.inflate(select.cd());

        for (int i = 0; i < schema.clusteringKeys.size(); i++)
        {
            ColumnSpec<?> column = schema.clusteringKeys.get(i);
            builder.withWhere(Reference.of(new Symbol(column.name, column.type.asServerType())),
                              toInequalities(Relations.RelationKind.EQ),
                              new Bind(ck[i], column.type.asServerType()));
        }

        return toCompiled(builder.build());
    }

    public static CompiledStatement select(Operations.SelectRange select, SchemaSpec schema)
    {
        Select.Builder builder = commmonPart(select, schema);

        Object[] lowBound = schema.valueGenerators.ckGen.inflate(select.lowerBound());
        Object[] highBound = schema.valueGenerators.ckGen.inflate(select.upperBound());

        for (int i = 0; i < schema.clusteringKeys.size(); i++)
        {
            ColumnSpec<?> column = schema.clusteringKeys.get(i);
            if (select.lowerBoundRelation()[i] != null)
            {
                builder.withWhere(Reference.of(new Symbol(column.name, column.type.asServerType())),
                                  toInequalities(select.lowerBoundRelation()[i]),
                                  new Bind(lowBound[i], column.type.asServerType()));
            }

            if (select.upperBoundRelation()[i] != null)
            {
                builder.withWhere(Reference.of(new Symbol(column.name, column.type.asServerType())),
                                  toInequalities(select.upperBoundRelation()[i]),
                                  new Bind(highBound[i], column.type.asServerType()));
            }
        }

        if (select.orderBy() == Operations.ClusteringOrderBy.DESC)
        {
            for (int i = 0; i < schema.clusteringKeys.size(); i++)
            {
                ColumnSpec<?> c = schema.clusteringKeys.get(i);
                builder.withOrderByColumn(c.name, c.type.asServerType(), c.isReversed() ? Select.OrderBy.Ordering.ASC : Select.OrderBy.Ordering.DESC);
            }
        }

        return toCompiled(builder.build());
    }

    public static CompiledStatement select(Operations.SelectCustom select, SchemaSpec schema)
    {
        Select.Builder builder = commmonPart(select, schema);

        Map<Long, Object[]> cache = new HashMap<>();
        for (Relations.Relation relation : select.ckRelations())
        {
            Object[] query = cache.computeIfAbsent(relation.descriptor, schema.valueGenerators.ckGen::inflate);
            ColumnSpec<?> column = schema.clusteringKeys.get(relation.column);
            builder.withWhere(Reference.of(new Symbol(column.name, column.type.asServerType())),
                              toInequalities(relation.kind),
                              new Bind(query[relation.column], column.type.asServerType()));
        }

        for (Relations.Relation relation : select.regularRelations())
        {
            ColumnSpec<?> column = schema.regularColumns.get(relation.column);
            Object query = schema.valueGenerators.regularColumnGens.get(relation.column).inflate(relation.descriptor);
            builder.withWhere(Reference.of(new Symbol(column.name, column.type.asServerType())),
                              toInequalities(relation.kind),
                              new Bind(query, column.type.asServerType()));
        }

        for (Relations.Relation relation : select.staticRelations())
        {
            Object query = schema.valueGenerators.staticColumnGens.get(relation.column).inflate(relation.descriptor);
            ColumnSpec<?> column = schema.staticColumns.get(relation.column);
            builder.withWhere(Reference.of(new Symbol(column.name, column.type.asServerType())),
                              toInequalities(relation.kind),
                              new Bind(query, column.type.asServerType()));
        }

        if (select.orderBy() == Operations.ClusteringOrderBy.DESC)
        {
            for (int i = 0; i < schema.clusteringKeys.size(); i++)
            {
                ColumnSpec<?> c = schema.clusteringKeys.get(i);
                builder.withOrderByColumn(c.name, c.type.asServerType(), c.isReversed() ? Select.OrderBy.Ordering.ASC : Select.OrderBy.Ordering.DESC);
            }
        }

        builder.allowFiltering();

        return toCompiled(builder.build());
    }

    public static Select.Builder commmonPart(Operations.SelectStatement select, SchemaSpec schema)
    {
        Select.Builder builder = new Select.Builder();

        Operations.Selection selection = Operations.Selection.fromBitSet(select.selection(), schema);
        if (selection.isWildcard())
        {
            builder.withWildcard();
        }
        else
        {
            for (int i = 0; i < schema.allColumnInSelectOrder.size(); i++)
            {
                ColumnSpec<?> spec = schema.allColumnInSelectOrder.get(i);
                if (!selection.columns().contains(spec))
                    continue;

                builder.withColumnSelection(spec.name, spec.type.asServerType());
            }

            if (selection.includeTimestamps())
            {
                for (ColumnSpec<?> spec : schema.staticColumns)
                {
                    if (!selection.columns().contains(spec))
                        continue;
                    builder.withSelection(FunctionCall.writetime(spec.name, spec.type.asServerType()));
                }

                for (ColumnSpec<?> spec : schema.regularColumns)
                {
                    if (!selection.columns().contains(spec))
                        continue;
                    builder.withSelection(FunctionCall.writetime(spec.name, spec.type.asServerType()));
                }
            }
        }

        builder.withTable(schema.keyspace, schema.table);

        Object[] pk = schema.valueGenerators.pkGen.inflate(select.pd());
        for (int i = 0; i < schema.partitionKeys.size(); i++)
        {
            ColumnSpec<?> column = schema.partitionKeys.get(i);
            Object value = pk[i];
            builder.withWhere(Reference.of(new Symbol(column.name, column.type.asServerType())),
                              Where.Inequalities.EQUAL,
                              new Bind(value, column.type.asServerType()));
        }

        return builder;
    }

    private static Where.Inequalities toInequalities(Relations.RelationKind kind)
    {
        Where.Inequalities inequalities;
        switch (kind)
        {
            case LT:
                inequalities = Where.Inequalities.LESS_THAN;
                break;
            case LTE:
                inequalities = Where.Inequalities.LESS_THAN_EQ;
                break;
            case GT:
                inequalities = Where.Inequalities.GREATER_THAN;
                break;
            case GTE:
                inequalities = Where.Inequalities.GREATER_THAN_EQ;
                break;
            case EQ:
                inequalities = Where.Inequalities.EQUAL;
                break;
            default:
                throw new UnsupportedOperationException("Unknown kind: " + kind);
        }
        return inequalities;
    }

    private static CompiledStatement toCompiled(Select select)
    {
        // Select does not add ';' by default, but CompiledStatement expects this
        String cql = select.toCQL().replace("\n", " ") + ";";
        Object[] bindingsArr = select.binds();
        return new CompiledStatement(cql, bindingsArr);
    }

}
