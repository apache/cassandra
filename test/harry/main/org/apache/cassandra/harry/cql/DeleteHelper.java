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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.Relations;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.execution.CompiledStatement;
import org.apache.cassandra.harry.util.BitSet;

public class DeleteHelper
{
    public static CompiledStatement inflateDelete(Operations.DeletePartition delete,
                                                  SchemaSpec schema,
                                                  long timestamp)
    {
        StringBuilder b = new StringBuilder();
        b.append("DELETE FROM ")
         .append(Symbol.maybeQuote(schema.keyspace))
         .append(".")
         .append(Symbol.maybeQuote(schema.table));

        if (timestamp != -1 && schema.options.addWriteTimestamps())
            b.append(" USING TIMESTAMP ").append(timestamp);

        b.append(" WHERE ");

        List<Object> bindings = new ArrayList<>();

        Object[] pk = schema.valueGenerators.pkGen.inflate(delete.pd());

        RelationWriter writer = new RelationWriter(b, bindings::add) ;

        for (int i = 0; i < pk.length; i++)
            writer.write(schema.partitionKeys.get(i), Relations.RelationKind.EQ, pk[i]);

        b.append(";");

        Object[] bindingsArr = bindings.toArray(new Object[bindings.size()]);

        return new CompiledStatement(b.toString(), bindingsArr);
    }

    public static CompiledStatement inflateDelete(Operations.DeleteRow delete,
                                                  SchemaSpec schema,
                                                  long timestamp)
    {
        StringBuilder b = new StringBuilder();
        b.append("DELETE FROM ")
         .append(Symbol.maybeQuote(schema.keyspace))
         .append(".")
         .append(Symbol.maybeQuote(schema.table));

        if (timestamp != -1 && schema.options.addWriteTimestamps())
            b.append(" USING TIMESTAMP ").append(timestamp);

        b.append(" WHERE ");

        List<Object> bindings = new ArrayList<>();

        Object[] pk = schema.valueGenerators.pkGen.inflate(delete.pd());
        Object[] ck = schema.valueGenerators.ckGen.inflate(delete.cd());

        RelationWriter writer = new RelationWriter(b, bindings::add);

        for (int i = 0; i < pk.length; i++)
            writer.write(schema.partitionKeys.get(i), Relations.RelationKind.EQ, pk[i]);
        for (int i = 0; i < ck.length; i++)
            writer.write(schema.clusteringKeys.get(i), Relations.RelationKind.EQ, ck[i]);

        b.append(";");

        Object[] bindingsArr = bindings.toArray(new Object[bindings.size()]);

        return new CompiledStatement(b.toString(), bindingsArr);
    }

    public static CompiledStatement inflateDelete(Operations.DeleteColumns delete,
                                                  SchemaSpec schema,
                                                  long timestamp)
    {
        StringBuilder b = new StringBuilder();
        b.append("DELETE ");

        {
            String[] names = columnNames(schema.regularColumns, delete.regularColumns());
            for (int i = 0; i < names.length; i++)
            {
                if (i > 0)
                    b.append(", ");
                b.append(Symbol.maybeQuote(names[i]));
            }
            b.append(" ");
        }

        {
            String[] names = columnNames(schema.staticColumns, delete.staticColumns());
            for (int i = 0; i < names.length; i++)
            {
                if (i > 0)
                    b.append(", ");
                b.append(Symbol.maybeQuote(names[i]));
            }
            b.append(" ");
        }

        b.append("FROM ")
         .append(Symbol.maybeQuote(schema.keyspace))
         .append(".")
         .append(Symbol.maybeQuote(schema.table));

        if (timestamp != -1 && schema.options.addWriteTimestamps())
            b.append(" USING TIMESTAMP ").append(timestamp);

        b.append(" WHERE ");

        List<Object> bindings = new ArrayList<>();

        Object[] pk = schema.valueGenerators.pkGen.inflate(delete.pd());
        Object[] ck = schema.valueGenerators.ckGen.inflate(delete.cd());

        RelationWriter writer = new RelationWriter(b, bindings::add);

        for (int i = 0; i < pk.length; i++)
            writer.write(schema.partitionKeys.get(i), Relations.RelationKind.EQ, pk[i]);
        for (int i = 0; i < ck.length; i++)
            writer.write(schema.clusteringKeys.get(i), Relations.RelationKind.EQ, ck[i]);

        b.append(";");

        Object[] bindingsArr = bindings.toArray(new Object[bindings.size()]);

        return new CompiledStatement(b.toString(), bindingsArr);
    }

    public static CompiledStatement inflateDelete(Operations.DeleteRange delete,
                                                  SchemaSpec schema,
                                                  long timestamp)
    {
        StringBuilder b = new StringBuilder();
        b.append("DELETE FROM ")
         .append(Symbol.maybeQuote(schema.keyspace))
         .append(".")
         .append(Symbol.maybeQuote(schema.table));

        if (timestamp != -1 && schema.options.addWriteTimestamps())
            b.append(" USING TIMESTAMP ").append(timestamp);

        b.append(" WHERE ");

        List<Object> bindings = new ArrayList<>();

        Object[] pk = schema.valueGenerators.pkGen.inflate(delete.pd());
        Object[] lowBound = schema.valueGenerators.ckGen.inflate(delete.lowerBound());
        Object[] highBound = schema.valueGenerators.ckGen.inflate(delete.upperBound());

        RelationWriter writer = new RelationWriter(b, bindings::add);

        for (int i = 0; i < pk.length; i++)
            writer.write(schema.partitionKeys.get(i), Relations.RelationKind.EQ, pk[i]);
        for (int i = 0; i < delete.lowerBoundRelation().length; i++)
        {
            Relations.RelationKind kind;
            kind = delete.lowerBoundRelation()[i];
            if (kind != null)
                writer.write(schema.clusteringKeys.get(i), kind, lowBound[i]);
            kind = delete.upperBoundRelation()[i];
            if (kind != null)
                writer.write(schema.clusteringKeys.get(i), kind, highBound[i]);
        }

        b.append(";");

        Object[] bindingsArr = bindings.toArray(new Object[bindings.size()]);

        return new CompiledStatement(b.toString(), bindingsArr);
    }

    private static final class RelationWriter
    {
        boolean isFirst = true;
        final StringBuilder builder;
        final Consumer<Object> collectBindings;

        private RelationWriter(StringBuilder builder, Consumer<Object> bindings)
        {
            this.builder = builder;
            this.collectBindings = bindings;
        }

        public void write(ColumnSpec<?> column, Relations.RelationKind kind, Object value)
        {
            if (isFirst)
                isFirst = false;
            else
                builder.append(" AND ");

            builder.append(Symbol.maybeQuote(column.name))
             .append(" ")
             .append(kind.symbol())
             .append(" ")
             .append("?");
            collectBindings.accept(value);
        }
    }


    private static String[] columnNames(List<ColumnSpec<?>> columns, BitSet selectedColumns)
    {
        String[] columnNames = new String[selectedColumns.size()];
        selectedColumns.eachSetBit(new IntConsumer()
        {
            int i = 0;

            public void accept(int idx)
            {
                columnNames[i++] = columns.get(idx).name;
            }
        });
        return columnNames;
    }
}
