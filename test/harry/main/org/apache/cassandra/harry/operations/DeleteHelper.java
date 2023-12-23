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

package org.apache.cassandra.harry.operations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.IntConsumer;

import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.util.BitSet;

public class DeleteHelper
{
    public static CompiledStatement deleteColumn(SchemaSpec schema,
                                                 long pd,
                                                 long cd,
                                                 BitSet columns,
                                                 BitSet mask,
                                                 long rts)
    {
        if (columns == null || columns.allUnset(mask))
            throw new IllegalArgumentException("Can't have a delete column query with no columns set. Column mask: " + columns);

        return delete(schema, pd, cd, columns, mask, rts);
    }

    public static CompiledStatement deleteColumn(SchemaSpec schema,
                                                 long pd,
                                                 BitSet columns,
                                                 BitSet mask,
                                                 long rts)
    {
        if (columns == null || columns.allUnset(mask))
            throw new IllegalArgumentException("Can't have a delete column query with no columns set. Column mask: " + columns);

        return delete(schema, pd, columns, mask, rts);
    }

    public static CompiledStatement deleteRow(SchemaSpec schema,
                                              long pd,
                                              long cd,
                                              long rts)
    {
        return delete(schema, pd, cd, null, null, rts);
    }

    public static CompiledStatement delete(SchemaSpec schema,
                                           long pd,
                                           List<Relation> relations,
                                           BitSet columnsToDelete,
                                           BitSet mask,
                                           long rts)
    {
        assert (columnsToDelete == null && mask == null) || (columnsToDelete != null && mask != null);
        return compile(schema,
                       pd,
                       relations,
                       columnsToDelete,
                       mask,
                       rts);
    }

    private static CompiledStatement delete(SchemaSpec schema,
                                            long pd,
                                            long cd,
                                            BitSet columnsToDelete,
                                            BitSet mask,
                                            long rts)
    {
        return compile(schema,
                       pd,
                       Relation.eqRelations(schema.ckGenerator.slice(cd),
                                            schema.clusteringKeys),
                       columnsToDelete,
                       mask,
                       rts);
    }

    private static CompiledStatement delete(SchemaSpec schema,
                                            long pd,
                                            BitSet columnsToDelete,
                                            BitSet mask,
                                            long rts)
    {
        return compile(schema,
                       pd,
                       new ArrayList<>(),
                       columnsToDelete,
                       mask,
                       rts);
    }

    public static CompiledStatement delete(SchemaSpec schema,
                                           long pd,
                                           long rts)
    {
        return compile(schema,
                       pd,
                       Collections.emptyList(),
                       null,
                       null,
                       rts);
    }

    private static CompiledStatement compile(SchemaSpec schema,
                                             long pd,
                                             List<Relation> relations,
                                             BitSet columnsToDelete,
                                             BitSet mask,
                                             long ts)
    {
        StringBuilder b = new StringBuilder();
        b.append("DELETE ");
        if (columnsToDelete != null)
        {
            assert mask != null;
            assert relations == null || relations.stream().allMatch((r) -> r.kind == Relation.RelationKind.EQ);
            String[] names = columnNames(schema.allColumns, columnsToDelete, mask);
            for (int i = 0; i < names.length; i++)
            {
                if (i > 0)
                    b.append(", ");
                b.append(names[i]);
            }
            b.append(" ");
        }
        b.append("FROM ")
         .append(schema.keyspace).append(".").append(schema.table)
         .append(" USING TIMESTAMP ")
         .append(ts)
         .append(" WHERE ");

        List<Object> bindings = new ArrayList<>();

        schema.inflateRelations(pd,
                                relations,
                                new SchemaSpec.AddRelationCallback()
                                {
                                    boolean isFirst = true;
                                    public void accept(ColumnSpec<?> spec, Relation.RelationKind kind, Object value)
                                    {
                                        if (isFirst)
                                            isFirst = false;
                                        else
                                            b.append(" AND ");
                                        b.append(kind.getClause(spec));
                                        bindings.add(value);
                                    }
                                });

        b.append(";");

        Object[] bindingsArr = bindings.toArray(new Object[bindings.size()]);

        return new CompiledStatement(b.toString(), bindingsArr);
    }

    private static String[] columnNames(List<ColumnSpec<?>> columns, BitSet selectedColumns, BitSet mask)
    {
        String[] columnNames = new String[selectedColumns.setCount(mask)];
        selectedColumns.eachSetBit(new IntConsumer()
        {
            int i = 0;

            public void accept(int idx)
            {
                columnNames[i++] = columns.get(idx).name;
            }
        }, mask);
        return columnNames;
    }
}
