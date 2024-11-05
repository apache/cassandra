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

package org.apache.cassandra.harry.model;

import java.util.*;

import org.apache.cassandra.cql3.ast.Bind;
import org.apache.cassandra.cql3.ast.FunctionCall;
import org.apache.cassandra.cql3.ast.Reference;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.cql3.ast.Where;
import org.apache.cassandra.harry.data.ResultSetRow;
import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.DataGenerators;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.operations.Relation;
import org.apache.cassandra.harry.operations.Query;

import static org.apache.cassandra.harry.gen.DataGenerators.UNSET_DESCR;

public class SelectHelper
{
    private static final long[] EMPTY_ARR = new long[]{};
    public static CompiledStatement selectWildcard(SchemaSpec schema, long pd)
    {
        return select(schema, pd, null, Collections.emptyList(), false, true);
    }

    public static CompiledStatement select(SchemaSpec schema, long pd)
    {
        return select(schema, pd, schema.allColumnsSet, Collections.emptyList(), false, true);
    }

    /**
     * Here, {@code reverse} should be understood not in ASC/DESC sense, but rather in terms
     * of how we're going to iterate through this partition (in other words, if first clustering component order
     * is DESC, we'll iterate in ASC order)
     */
    public static CompiledStatement select(SchemaSpec schema, long pd, List<Relation> relations, boolean reverse, boolean includeWriteTime)
    {
        return select(schema, pd, schema.allColumnsSet, relations, reverse, includeWriteTime);
    }

    public static CompiledStatement selectWildcard(SchemaSpec schema, long pd, List<Relation> relations, boolean reverse, boolean includeWriteTime)
    {
        return select(schema, pd, null, relations, reverse, includeWriteTime);
    }

    public static CompiledStatement select(SchemaSpec schema, Long pd, Set<ColumnSpec<?>> columns, List<Relation> relations, boolean reverse, boolean includeWriteTime)
    {
        Select.Builder builder = new Select.Builder();
        if (columns == null)
        {
            // these are not touched in this case, but still better to be safe than sorry
            columns = schema.allColumnsSet;
            includeWriteTime = false;
            builder.withWildcard();
        }
        else
        {
            for (int i = 0; i < schema.allColumns.size(); i++)
            {
                ColumnSpec<?> spec = schema.allColumns.get(i);
                if (columns != null && !columns.contains(spec))
                    continue;

                builder.withColumnSelection(spec.name, spec.type.asServerType());
            }

            if (includeWriteTime)
            {
                for (ColumnSpec<?> spec : schema.staticColumns)
                {
                    if (columns != null && !columns.contains(spec))
                        continue;
                    builder.withSelection(FunctionCall.writetime(spec.name, spec.type.asServerType()));
                }

                for (ColumnSpec<?> spec : schema.regularColumns)
                {
                    if (columns != null && !columns.contains(spec))
                        continue;
                    builder.withSelection(FunctionCall.writetime(spec.name, spec.type.asServerType()));
                }
            }

            if (schema.trackLts)
            {
                ColumnSpec<?> spec = schema.ltsColumn();
                builder.withColumnSelection(spec.name, spec.type.asServerType());
            }
        }

        builder.withTable(schema.keyspace, schema.table);

        SchemaSpec.AddRelationCallback consumer = appendToBuilderCallback(builder);
        if (pd != null)
        {
            Object[] pk = schema.inflatePartitionKey(pd);
            for (int i = 0; i < pk.length; i++)
                consumer.accept(schema.partitionKeys.get(i), Relation.RelationKind.EQ, pk[i]);

        }
        schema.inflateRelations(relations, consumer);

        addOrderBy(schema, builder, reverse);

        return toCompiled(builder.build());
    }

    public static CompiledStatement count(SchemaSpec schema, long pd)
    {
        Select.Builder builder = new Select.Builder();
        builder.withSelection(FunctionCall.countStar());
        builder.withTable(schema.keyspace, schema.table);
        schema.inflateRelations(pd, Collections.emptyList(), appendToBuilderCallback(builder));

        return toCompiled(builder.build());
    }

    private static SchemaSpec.AddRelationCallback appendToBuilderCallback(Select.Builder builder)
    {
        return (spec, kind, value) ->
               builder.withWhere(Reference.of(new Symbol(spec.name, spec.type.asServerType())), toInequalities(kind), new Bind(value, spec.type.asServerType()));
    }

    private static Where.Inequalities toInequalities(Relation.RelationKind kind)
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
        String cql = select.toCQL() + ";";
        Object[] bindingsArr = select.binds();
        return new CompiledStatement(cql, bindingsArr);
    }

    private static void addOrderBy(SchemaSpec schema, Select.Builder builder, boolean reverse)
    {
        if (reverse && schema.clusteringKeys.size() > 0)
        {
            for (int i = 0; i < schema.clusteringKeys.size(); i++)
            {
                ColumnSpec<?> c = schema.clusteringKeys.get(i);
                builder.withOrderByColumn(c.name, c.type.asServerType(), c.isReversed() ? Select.OrderBy.Ordering.ASC : Select.OrderBy.Ordering.DESC);
            }
        }
    }


    public static Object[] broadenResult(SchemaSpec schemaSpec, Set<ColumnSpec<?>> columns, Object[] result)
    {
        boolean isWildcardQuery = columns == null;

        if (isWildcardQuery)
            columns = schemaSpec.allColumnsSet;
        else if (schemaSpec.allColumns.size() == columns.size())
            return result;

        Object[] newRes = new Object[schemaSpec.allColumns.size() + schemaSpec.staticColumns.size() + schemaSpec.regularColumns.size()];

        int origPointer = 0;
        int newPointer = 0;
        for (int i = 0; i < schemaSpec.allColumns.size(); i++)
        {
            ColumnSpec<?> column = schemaSpec.allColumns.get(i);
            if (columns.contains(column))
                newRes[newPointer] = result[origPointer++];
            else
                newRes[newPointer] = DataGenerators.UNSET_VALUE;
            newPointer++;
        }

        // Make sure to include writetime, but only in case query actually includes writetime (for example, it's not a wildcard query)
        for (int i = 0; i < schemaSpec.staticColumns.size() && origPointer < result.length; i++)
        {
            ColumnSpec<?> column = schemaSpec.staticColumns.get(i);
            if (columns.contains(column))
                newRes[newPointer] = result[origPointer++];
            else
                newRes[newPointer] = null;
            newPointer++;
        }

        for (int i = 0; i < schemaSpec.regularColumns.size() && origPointer < result.length; i++)
        {
            ColumnSpec<?> column = schemaSpec.regularColumns.get(i);
            if (columns.contains(column))
                newRes[newPointer] = result[origPointer++];
            else
                newRes[newPointer] = null;
            newPointer++;
        }

        return newRes;
    }

    static boolean isDeflatable(Object[] columns)
    {
        for (Object column : columns)
        {
            if (column == DataGenerators.UNSET_VALUE)
                return false;
        }
        return true;
    }

    public static ResultSetRow resultSetToRow(SchemaSpec schema, OpSelectors.Clock clock, Object[] result)
    {
        Object[] partitionKey = new Object[schema.partitionKeys.size()];
        Object[] clusteringKey = new Object[schema.clusteringKeys.size()];
        Object[] staticColumns = new Object[schema.staticColumns.size()];
        Object[] regularColumns = new Object[schema.regularColumns.size()];

        System.arraycopy(result, 0, partitionKey, 0, partitionKey.length);
        System.arraycopy(result, partitionKey.length, clusteringKey, 0, clusteringKey.length);
        System.arraycopy(result, partitionKey.length + clusteringKey.length, staticColumns, 0, staticColumns.length);
        System.arraycopy(result, partitionKey.length + clusteringKey.length + staticColumns.length, regularColumns, 0, regularColumns.length);


        List<Long> visited_lts_list;
        if (schema.trackLts)
        {
            visited_lts_list = (List<Long>) result[result.length - 1];
            visited_lts_list.sort(Long::compare);
        }
        else
        {
            visited_lts_list = Collections.emptyList();
        }

        long[] slts = new long[schema.staticColumns.size()];
        Arrays.fill(slts, Model.NO_TIMESTAMP);
        for (int i = 0, sltsBase = schema.allColumns.size(); i < slts.length && sltsBase + i < result.length; i++)
        {
            Object v = result[schema.allColumns.size() + i];
            if (v != null)
                slts[i] = clock.lts((long) v);
        }

        long[] lts = new long[schema.regularColumns.size()];
        Arrays.fill(lts, Model.NO_TIMESTAMP);
        for (int i = 0, ltsBase = schema.allColumns.size() + slts.length; i < lts.length && ltsBase + i < result.length; i++)
        {
            Object v = result[ltsBase + i];
            if (v != null)
                lts[i] = clock.lts((long) v);
        }

        return new ResultSetRow(isDeflatable(partitionKey) ? schema.deflatePartitionKey(partitionKey) : UNSET_DESCR,
                                isDeflatable(clusteringKey) ? schema.deflateClusteringKey(clusteringKey) : UNSET_DESCR,
                                schema.staticColumns.isEmpty() ? EMPTY_ARR : schema.deflateStaticColumns(staticColumns),
                                schema.staticColumns.isEmpty() ? EMPTY_ARR : slts,
                                schema.deflateRegularColumns(regularColumns),
                                lts,
                                visited_lts_list);
    }

    public static List<ResultSetRow> execute(SystemUnderTest sut, OpSelectors.Clock clock, Query query)
    {
        return execute(sut, clock, query, query.schemaSpec.allColumnsSet);
    }

    public static List<ResultSetRow> execute(SystemUnderTest sut, OpSelectors.Clock clock, Query query, Set<ColumnSpec<?>> columns)
    {
        CompiledStatement compiled = query.toSelectStatement(columns, !query.schemaSpec.isWriteTimeFromAccord());
        Object[][] objects = sut.executeIdempotent(compiled.cql(), SystemUnderTest.ConsistencyLevel.QUORUM, compiled.bindings());
        List<ResultSetRow> result = new ArrayList<>();
        for (Object[] obj : objects)
            result.add(resultSetToRow(query.schemaSpec, clock, broadenResult(query.schemaSpec, columns, obj)));
        return result;
    }

    public static List<ResultSetRow> execute(SystemUnderTest sut, OpSelectors.Clock clock, CompiledStatement compiled, SchemaSpec schemaSpec)
    {
        Set<ColumnSpec<?>> columns = schemaSpec.allColumnsSet;
        Object[][] objects = sut.executeIdempotent(compiled.cql(), SystemUnderTest.ConsistencyLevel.QUORUM, compiled.bindings());
        List<ResultSetRow> result = new ArrayList<>();
        for (Object[] obj : objects)
            result.add(resultSetToRow(schemaSpec, clock, broadenResult(schemaSpec, columns, obj)));
        return result;
    }
}
