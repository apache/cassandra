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

import java.util.List;

import accord.utils.Invariants;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.execution.CompiledStatement;
import org.apache.cassandra.harry.op.Operations;

public class WriteHelper
{
    public static CompiledStatement inflateInsert(Operations.WriteOp op,
                                                  SchemaSpec schema,
                                                  long timestamp)
    {
        assert op.vds().length == schema.regularColumns.size();
        assert op.sds().length == schema.staticColumns.size();
        assert op.vds().length == schema.valueGenerators.regularColumnCount();
        assert op.sds().length == schema.valueGenerators.staticColumnCount();

        Object[] partitionKey = schema.valueGenerators.pkGen().inflate(op.pd());
        assert partitionKey.length == schema.partitionKeys.size();
        Object[] clusteringKey = schema.valueGenerators.ckGen().inflate(op.cd());
        assert clusteringKey.length == schema.clusteringKeys.size();
        Object[] regularColumns = new Object[op.vds().length];
        Object[] staticColumns = new Object[op.sds().length];

        for (int i = 0; i < op.vds().length; i++)
        {
            long descriptor = op.vds()[i];
            if (descriptor == MagicConstants.UNSET_DESCR)
                regularColumns[i] = MagicConstants.UNSET_VALUE;
            else
                regularColumns[i] = schema.valueGenerators.regularColumnGen(i).inflate(descriptor);
        }

        for (int i = 0; i < op.sds().length; i++)
        {
            long descriptor = op.sds()[i];
            if (descriptor == MagicConstants.UNSET_DESCR)
                staticColumns[i] = MagicConstants.UNSET_VALUE;
            else
                staticColumns[i] = schema.valueGenerators.staticColumnGen(i).inflate(descriptor);
        }

        Object[] bindings = new Object[schema.allColumnInSelectOrder.size()];

        StringBuilder b = new StringBuilder();
        b.append("INSERT INTO ")
         .append(schema.keyspace)
         .append('.')
         .append(schema.table)
         .append(" (");

        int bindingsCount = 0;
        bindingsCount += appendStatements(b, bindings, schema.partitionKeys, partitionKey, bindingsCount, true, ",", "%s");
        bindingsCount += appendStatements(b, bindings, schema.clusteringKeys, clusteringKey, bindingsCount, false, ",", "%s");
        bindingsCount += appendStatements(b, bindings, schema.regularColumns, regularColumns, bindingsCount, false, ",", "%s");
        bindingsCount += appendStatements(b, bindings, schema.staticColumns, staticColumns, bindingsCount, false, ",", "%s");

        b.append(") VALUES (");

        for (int i = 0; i < bindingsCount; i++)
        {
            if (i > 0)
                b.append(", ");
            b.append("?");
        }

        b.append(")");
        if (timestamp != -1 && schema.options.addWriteTimestamps())
        {
            b.append(" USING TIMESTAMP ")
             .append(timestamp);
        }

        b.append(";");
        return new CompiledStatement(b.toString(), adjustArraySize(bindings, bindingsCount));
    }

    public static Object[] adjustArraySize(Object[] bindings, int bindingsCount)
    {
        if (bindingsCount != bindings.length)
        {
            Object[] tmp = new Object[bindingsCount];
            System.arraycopy(bindings, 0, tmp, 0, bindingsCount);
            bindings = tmp;
        }
        return bindings;
    }

    public static CompiledStatement inflateUpdate(Operations.WriteOp op,
                                                  SchemaSpec schema,
                                                  long timestamp)
    {
        assert op.vds().length == schema.regularColumns.size();
        assert op.sds().length == schema.staticColumns.size();
        assert op.vds().length == schema.valueGenerators.regularColumnCount();
        assert op.sds().length == schema.valueGenerators.staticColumnCount();

        Object[] partitionKey = schema.valueGenerators.pkGen().inflate(op.pd);
        assert partitionKey.length == schema.partitionKeys.size();
        Object[] clusteringKey = schema.valueGenerators.ckGen().inflate(op.cd());
        assert clusteringKey.length == schema.clusteringKeys.size();
        Object[] regularColumns = new Object[op.vds().length];
        Object[] staticColumns = new Object[op.sds().length];

        for (int i = 0; i < op.vds().length; i++)
            regularColumns[i] = schema.valueGenerators.regularColumnGen(i).inflate(op.vds()[i]);

        for (int i = 0; i < op.sds().length; i++)
            staticColumns[i] = schema.valueGenerators.staticColumnGen(i).inflate(op.sds()[i]);

        Object[] bindings = new Object[schema.allColumnInSelectOrder.size()];

        StringBuilder b = new StringBuilder();
        b.append("UPDATE ")
         .append(schema.keyspace)
         .append('.')
         .append(schema.table);

        if (timestamp != -1 && schema.options.addWriteTimestamps())
        {
            b.append(" USING TIMESTAMP ")
             .append(timestamp)
             .append(" SET ");
        }

        int bindingsCount = 0;
        bindingsCount += addSetStatements(b, bindings, schema.regularColumns, regularColumns, bindingsCount);
        if (staticColumns.length != 0)
            bindingsCount += addSetStatements(b, bindings, schema.staticColumns, staticColumns, bindingsCount);

        assert bindingsCount > 0 : "Can not have an UPDATE statement without any updates";
        b.append(" WHERE ");

        bindingsCount += addWhereStatements(b, bindings, schema.partitionKeys, partitionKey, bindingsCount, true);
        bindingsCount += addWhereStatements(b, bindings, schema.clusteringKeys, clusteringKey, bindingsCount, false);
        b.append(";");
        return new CompiledStatement(b.toString(), adjustArraySize(bindings, bindingsCount));
    }

    private static int addSetStatements(StringBuilder b,
                                        Object[] bindings,
                                        List<ColumnSpec<?>> columns,
                                        Object[] values,
                                        int bound)
    {
        return appendStatements(b, bindings, columns, values, bound, bound == 0, ", ", "%s = ?");
    }

    private static int addWhereStatements(StringBuilder b,
                                          Object[] bindings,
                                          List<ColumnSpec<?>> columns,
                                          Object[] values,
                                          int bound,
                                          boolean firstStatement)
    {
        return appendStatements(b, bindings, columns, values, bound, firstStatement, " AND ", "%s = ?");
    }

    private static int appendStatements(StringBuilder b,
                                        Object[] allBindings,
                                        List<ColumnSpec<?>> columns,
                                        Object[] values,
                                        int bound,
                                        boolean firstStatement,
                                        String separator,
                                        String nameFormatter)
    {
        int bindingsCount = 0;
        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            if (value == MagicConstants.UNSET_VALUE)
                continue;
            Invariants.nonNull(value);
            ColumnSpec<?> column = columns.get(i);
            if (bindingsCount > 0 || !firstStatement)
                b.append(separator);

            b.append(String.format(nameFormatter, column.name));
            allBindings[bound + bindingsCount] = value;
            bindingsCount++;
        }
        return bindingsCount;
    }
}