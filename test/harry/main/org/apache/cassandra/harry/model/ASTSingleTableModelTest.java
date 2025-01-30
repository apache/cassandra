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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.cql3.ast.Bind;
import org.apache.cassandra.cql3.ast.Conditional;
import org.apache.cassandra.cql3.ast.FunctionCall;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ASTSingleTableModelTest
{
    public static final ByteBuffer ZERO = ByteBufferUtil.bytes(0);
    public static final ByteBuffer ONE = ByteBufferUtil.bytes(1);
    public static final ByteBuffer TWO = ByteBufferUtil.bytes(2);
    public static final ByteBuffer THREE = ByteBufferUtil.bytes(3);
    public static final ByteBuffer[][] EMPTY = new ByteBuffer[0][];

    @Test
    public void singlePartition()
    {
        for (TableMetadata metadata : defaultTables())
        {
            ASTSingleTableModel model = new ASTSingleTableModel(metadata);
            ByteBuffer[][] expected = new ByteBuffer[][]{
            insert(model, ZERO),
            insert(model, (kind, offset) -> kind == ColumnMetadata.Kind.CLUSTERING ? ONE : ZERO)
            };
            // insert partition that shouldn't be returned
            insert(model, ONE);

            Select.Builder builder = Select.builder().table(metadata);
            for (var pk : metadata.partitionKeyColumns())
                builder.value(new Symbol(pk), ZERO);
            Select select = builder.build();
            model.validate(expected, select);
        }
    }

    @Test
    public void singleToken()
    {
        for (TableMetadata metadata : defaultTables())
        {
            ASTSingleTableModel model = new ASTSingleTableModel(metadata);
            ByteBuffer[][] expected = new ByteBuffer[][]{
            insert(model, ZERO),
            insert(model, (kind, offset) -> kind == ColumnMetadata.Kind.CLUSTERING ? ONE : ZERO)
            };
            // insert partition that shouldn't be returned
            insert(model, ONE);

            Select.Builder builder = Select.builder().table(metadata);
            builder.where(FunctionCall.tokenByColumns(metadata.partitionKeyColumns().stream().map(Symbol::new).collect(Collectors.toList())),
                          Conditional.Where.Inequality.EQUAL,
                          FunctionCall.tokenByValue(metadata.partitionKeyColumns().stream().map(i -> new Bind(ZERO, Int32Type.instance)).collect(Collectors.toList())));

            Select select = builder.build();
            model.validate(expected, select);
        }
    }

    @Test
    public void singleRow()
    {
        for (TableMetadata metadata : defaultTables())
        {
            ASTSingleTableModel model = new ASTSingleTableModel(metadata);
            ByteBuffer[] expectedRow = insert(model, ZERO);
            // insert row that shouldn't be returned
            insert(model, (kind, offset) -> kind == ColumnMetadata.Kind.CLUSTERING ? ONE : ZERO);
            // insert partition that shouldn't be returned
            insert(model, ONE);

            Select.Builder builder = Select.builder().table(metadata);
            for (var col : metadata.primaryKeyColumns())
                builder.value(new Symbol(col), ZERO);
            Select select = builder.build();
            model.validate(new ByteBuffer[][] {expectedRow}, select);
        }
    }

    @Test
    public void eqNoMatches()
    {
        for (TableMetadata metadata : defaultTables())
        {
            // this test only works when there are regular/static columns
            if (metadata.regularAndStaticColumns().isEmpty()) continue;
            ASTSingleTableModel model = new ASTSingleTableModel(metadata);
            insert(model, ZERO);
            // insert row that shouldn't be returned
            insert(model, (kind, offset) -> kind == ColumnMetadata.Kind.CLUSTERING ? ONE : ZERO);
            // insert partition that shouldn't be returned
            insert(model, ONE);

            for (boolean includeClustering : Arrays.asList(true, false))
            {
                Select.Builder builder = Select.builder().table(metadata);
                for (var col : metadata.partitionKeyColumns())
                    builder.value(new Symbol(col), ZERO);
                if (includeClustering)
                {
                    for (var col : metadata.clusteringColumns())
                        builder.value(new Symbol(col), ONE);
                }
                for (var col : metadata.staticColumns())
                    builder.value(new Symbol(col), TWO);
                for (var col : metadata.regularColumns())
                    builder.value(new Symbol(col), THREE);
                Select select = builder.build();
                model.validate(EMPTY, select);
            }
        }
    }

    @Test
    public void selectWhereIn()
    {
        TableMetadata metadata = new Builder().pk(1).build();
        ASTSingleTableModel model = new ASTSingleTableModel(metadata);
        // insert in token order to keep the logic simple
        ByteBuffer[][] expected = { insert(model, ONE),
                                    insert(model, TWO),
                                    insert(model, ZERO) };

        model.validate(expected, Select.builder()
                                       .table(metadata)
                                       .in("pk", 0, 1, 2)
                                       .build());
    }

    @Test
    public void nullColumnSelect()
    {
        TableMetadata metadata = TableMetadata.builder("ks", "tbl")
                                              .partitioner(Murmur3Partitioner.instance)
                                              .addPartitionKeyColumn("pk0", InetAddressType.instance)
                                              .addClusteringColumn("ck0", ReversedType.getInstance(ShortType.instance))
                                              .addRegularColumn("v0", TimestampType.instance)
                                              .addRegularColumn("v1", LexicalUUIDType.instance)
                                              .build();
        ASTSingleTableModel model = new ASTSingleTableModel(metadata);

        String pk0 = "'e44b:bdaf:aeb:f68b:1cff:ecbd:8b54:2295'";
        ByteBuffer pk0BB = InetAddressType.instance.asCQL3Type().fromCQLLiteral(pk0);

        Short row1 = Short.valueOf((short) -14407);
        ByteBuffer row1BB = ShortType.instance.decompose(row1);
        String row1V1 = "0x00000000000049008a00000000000000";
        ByteBuffer row1V1BB = LexicalUUIDType.instance.asCQL3Type().fromCQLLiteral(row1V1);

        Short row2 = Short.valueOf((short) ((short) 18175 - (short) 23847));
        ByteBuffer row2BB = ShortType.instance.decompose(row2);
        String row2V0 = "'1989-01-11T15:00:30.950Z'";
        ByteBuffer row2V0BB = TimestampType.instance.asCQL3Type().fromCQLLiteral(row2V0);
        String row2V1 = "0x0000000000001f00a700000000000000";
        ByteBuffer row2V1BB = LexicalUUIDType.instance.asCQL3Type().fromCQLLiteral(row2V1);

        Select selectPk = Select.builder(metadata)
                                .value("pk0", pk0)
                                .build();

        Select selectColumn = Select.builder(metadata)
                                    .value("pk0", pk0)
                                    .where("v0", Conditional.Where.Inequality.GREATER_THAN, row2V0)
                                    .build();

        model.update(Mutation.update(metadata)
                             .set("v1", row1V1)
                             .value("pk0", pk0)
                             .value("ck0", row1)
                             .build());

        model.validate(new ByteBuffer[][]{ new ByteBuffer[]{ pk0BB, row1BB, null, row1V1BB } }, selectPk);
        model.validate(new ByteBuffer[0][], selectColumn);


        model.update(Mutation.insert(metadata)
                             .value("pk0", pk0)
                             .value("ck0", row2)
                             .value("v0", row2V0)
                             .value("v1", row2V1)
                             .build());

        model.validate(new ByteBuffer[][]{
        new ByteBuffer[]{ pk0BB, row2BB, row2V0BB, row2V1BB },
        new ByteBuffer[]{ pk0BB, row1BB, null, row1V1BB },
        }, selectPk);

        model.validate(new ByteBuffer[0][], selectColumn);
    }

    private interface ColumnValue
    {
        ByteBuffer accept(ColumnMetadata.Kind kind, int offset);
    }

    private static ByteBuffer[] insert(ASTSingleTableModel model, ByteBuffer value)
    {
        return insert(model, (i1, i2) -> value);
    }

    private static ByteBuffer[] insert(ASTSingleTableModel model, ColumnValue fn)
    {
        TableMetadata metadata = model.factory.metadata;
        ByteBuffer[] expectedRow = new ByteBuffer[metadata.columns().size()];
        var builder = Mutation.insert(metadata);
        int offset = 0;
        int idx = 0;
        for (var col : metadata.partitionKeyColumns())
        {
            ByteBuffer value = fn.accept(ColumnMetadata.Kind.PARTITION_KEY, idx++);
            builder.value(new Symbol(col), value);
            expectedRow[offset++] = value;
        }
        idx = 0;
        for (var col : metadata.clusteringColumns())
        {
            ByteBuffer value = fn.accept(ColumnMetadata.Kind.CLUSTERING, idx++);
            builder.value(new Symbol(col), value);
            expectedRow[offset++] = value;
        }
        idx = 0;
        for (var col : metadata.staticColumns())
        {
            ByteBuffer value = fn.accept(ColumnMetadata.Kind.STATIC, idx++);
            builder.value(new Symbol(col), value);
            expectedRow[offset++] = value;
        }
        idx = 0;
        for (var col : metadata.regularColumns())
        {
            ByteBuffer value = fn.accept(ColumnMetadata.Kind.REGULAR, idx++);
            builder.value(new Symbol(col), value);
            expectedRow[offset++] = value;
        }
        model.update(builder.build());
        return expectedRow;
    }

    private static List<TableMetadata> defaultTables()
    {
        List<TableMetadata> tables = new ArrayList<>();
        for (int pk : Arrays.asList(1, 2))
        {
            for (int ck : Arrays.asList(0, 1, 2))
            {
                for (int statics : Arrays.asList(0, 1, 2))
                {
                    for (int regular : Arrays.asList(0, 1, 2))
                    {
                        tables.add(new Builder()
                                   .pk(pk)
                                   .ck(ck)
                                   .statics(statics)
                                   .regular(regular)
                                   .build());
                    }
                }
            }
        }
        return tables;
    }

    private static class Builder
    {
        private int numPk = 1;
        private int numCk = 0;
        private int numStatic = 0;
        private int numRegular = 0;

        private Builder pk(int count)
        {
            numPk = count;
            return this;
        }

        private Builder ck(int count)
        {
            numCk = count;
            return this;
        }

        private Builder statics(int count)
        {
            numStatic = count;
            return this;
        }

        private Builder regular(int count)
        {
            numRegular = count;
            return this;
        }

        private TableMetadata build()
        {
            TableMetadata.Builder builder = TableMetadata.builder("ks", "tbl")
                                                         .kind(TableMetadata.Kind.REGULAR)
                                                         .partitioner(Murmur3Partitioner.instance);
            addColumn("pk", numPk, n -> builder.addPartitionKeyColumn(n, Int32Type.instance));
            addColumn("ck", numCk, n -> builder.addClusteringColumn(n, Int32Type.instance));
            addColumn("s", numStatic, n -> builder.addStaticColumn(n, Int32Type.instance));
            addColumn("v", numRegular, n -> builder.addRegularColumn(n, Int32Type.instance));

            return builder.build();
        }

        private static void addColumn(String prefix, int count, Consumer<String> addColumn)
        {
            if (count == 0)
                return;
            if (count == 1)
            {
                addColumn.accept(prefix);
            }
            else
            {
                for (int i = 0; i < count; i++)
                    addColumn.accept(prefix + i);
            }
        }
    }
}