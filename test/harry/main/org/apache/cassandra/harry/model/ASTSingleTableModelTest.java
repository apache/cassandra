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
import java.util.EnumSet;
import java.util.List;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.cql3.ast.Bind;
import org.apache.cassandra.cql3.ast.Conditional.Where.Inequality;
import org.apache.cassandra.cql3.ast.FunctionCall;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
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

    private static final EnumSet<Inequality> RANGE_INEQUALITY = EnumSet.of(Inequality.LESS_THAN, Inequality.LESS_THAN_EQ,
                                                                           Inequality.GREATER_THAN, Inequality.GREATER_THAN_EQ);

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
            builder.where(FunctionCall.tokenByColumns(model.factory.partitionColumns),
                          Inequality.EQUAL,
                          FunctionCall.tokenByValue(model.factory.partitionColumns.stream().map(i -> new Bind(ZERO, Int32Type.instance)).collect(Collectors.toList())));

            Select select = builder.build();
            model.validate(expected, select);
        }
    }

    @Test
    public void tokenSearch()
    {
        for (TableMetadata metadata : defaultTables())
        {
            ASTSingleTableModel model = new ASTSingleTableModel(metadata);
            ModelModel modelModel = new ModelModel(model);
            boolean hasClustering = !model.factory.clusteringColumns.isEmpty();
            List<ByteBuffer> partitionValues = Arrays.asList(ONE, TWO, THREE);

            for (ByteBuffer value : partitionValues)
            {
                if (hasClustering)
                    modelModel.add(insert(model, (kind, offset) -> kind == ColumnMetadata.Kind.CLUSTERING ? ZERO : value));
                modelModel.add(insert(model, value));
            }

            FunctionCall tokenByColumns = FunctionCall.tokenByColumns(model.factory.partitionColumns);

            // unbound range: < / >
            for (BytesPartitionState.Ref ref : modelModel.refs())
            {
                FunctionCall tokenByValue = tokenFunction(ref);
                for (Inequality inequality : RANGE_INEQUALITY)
                {
                    model.validate(modelModel.allWhere(inequality, ref.token),
                                   Select.builder(metadata)
                                         .where(tokenByColumns, inequality, tokenByValue)
                                         .build());
                }
            }
            // bound range: < and >, > and <
            for (BytesPartitionState.Ref leftValue : modelModel.refs())
            {
                FunctionCall leftTokenFunction = tokenFunction(leftValue);
                for (Inequality left : RANGE_INEQUALITY)
                {
                    for (BytesPartitionState.Ref rightValue : modelModel.refs())
                    {
                        FunctionCall rightTokenFunction = tokenFunction(rightValue);
                        for (Inequality right : RANGE_INEQUALITY)
                        {
                            model.validate(modelModel.allWhere(left, leftValue.token,
                                                               right, rightValue.token),
                                           Select.builder(metadata)
                                                 .where(tokenByColumns, left, leftTokenFunction)
                                                 .where(tokenByColumns, right, rightTokenFunction)
                                                 .build());
                        }
                    }
                }
            }
            // between (same as bound range, but different syntax)
            for (BytesPartitionState.Ref left : modelModel.refs())
            {
                FunctionCall leftTokenFunction = tokenFunction(left);
                for (BytesPartitionState.Ref right : modelModel.refs())
                {
                    FunctionCall rightTokenFunction = tokenFunction(right);
                    model.validate(modelModel.allWhere(Inequality.GREATER_THAN_EQ, left.token,
                                                       Inequality.LESS_THAN_EQ, right.token),
                                   Select.builder(metadata)
                                         .between(tokenByColumns, leftTokenFunction, rightTokenFunction)
                                         .build());
                }
            }
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
        // This example was found from a test, hence why more complex types are used.
        // This test didn't end up depending on these complexities as the issue was null (delete or undefined column)
        // handle, which is type agnostic.
        TableMetadata metadata = defaultTable()
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
                                    .where("v0", Inequality.GREATER_THAN, row2V0)
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

    @Test
    public void selectStar()
    {
        for (TableMetadata metadata : defaultTables())
        {
            ASTSingleTableModel model = new ASTSingleTableModel(metadata);
            ModelModel modelModel = new ModelModel(model);
            boolean hasClustering = !model.factory.clusteringColumns.isEmpty();
            for (ByteBuffer value : Arrays.asList(ONE, TWO, THREE))
            {
                if (hasClustering)
                    modelModel.add(insert(model, (kind, offset) -> kind == ColumnMetadata.Kind.CLUSTERING ? ZERO : value));
                modelModel.add(insert(model, value));
            }

            model.validate(modelModel.all(), Select.builder(metadata).build());
        }
    }

    @Test
    public void simpleSearch()
    {
        for (TableMetadata metadata : defaultTables())
        {
            ASTSingleTableModel model = new ASTSingleTableModel(metadata);
            ModelModel modelModel = new ModelModel(model);
            boolean hasClustering = !model.factory.clusteringColumns.isEmpty();
            List<ByteBuffer> partitionValues = Arrays.asList(ONE, TWO, THREE);
            List<ByteBuffer> allValues = ImmutableList.<ByteBuffer>builder()
                                                      .add(ZERO)
                                                      .addAll(partitionValues)
                                                      .build();

            for (ByteBuffer value : partitionValues)
            {
                if (hasClustering)
                    modelModel.add(insert(model, (kind, offset) -> kind == ColumnMetadata.Kind.CLUSTERING ? ZERO : value));
                modelModel.add(insert(model, value));
            }

            for (Symbol column : model.factory.selectionOrder)
            {
                // test eq
                for (ByteBuffer value : allValues)
                {
                    model.validate(modelModel.allEq(column, value),
                                   Select.builder(metadata).value(column, value).build());
                }
                // unbound range: < / >
                for (ByteBuffer value : allValues)
                {
                    for (Inequality inequality : RANGE_INEQUALITY)
                    {
                        model.validate(modelModel.allWhere(column, inequality, value),
                                       Select.builder(metadata).where(column, inequality, value).build());
                    }
                }
                // bound range: < and >, > and <
                for (ByteBuffer leftValue : allValues)
                {
                    for (Inequality left : RANGE_INEQUALITY)
                    {
                        for (ByteBuffer rightValue : allValues)
                        {
                            for (Inequality right : RANGE_INEQUALITY)
                            {
                                model.validate(modelModel.allWhere(column,
                                                                   left, leftValue,
                                                                   right, rightValue),
                                               Select.builder(metadata)
                                                     .where(column, left, leftValue)
                                                     .where(column, right, rightValue)
                                                     .build());
                            }
                        }
                    }
                }
                // between (same as bound range, but different syntax)
                for (ByteBuffer left : allValues)
                {
                    for (ByteBuffer right : allValues)
                    {
                        model.validate(modelModel.allWhere(column,
                                                           Inequality.GREATER_THAN_EQ, left,
                                                           Inequality.LESS_THAN_EQ, right),
                                       Select.builder(metadata)
                                             .between(column, new Bind(left, column.type()), new Bind(right, column.type()))
                                             .build());
                    }
                }
            }
        }
    }

    @Test
    public void staticOnlyWrite()
    {
        TableMetadata metadata = new Builder().pk(1).ck(1).statics(1).regular(1).build();
        ASTSingleTableModel model = new ASTSingleTableModel(metadata);
        model.update(Mutation.insert(metadata)
                             .value("pk", 0)
                             .value("s", 0)
                             .build());
        model.update(Mutation.update(metadata)
                             .set("s", 1)
                             .value("pk", 1)
                             .build());
        ByteBuffer[] rowZero = { ZERO, null, ZERO, null };
        ByteBuffer[] rowOne = { ONE, null, ONE, null };
        ByteBuffer[][] allExpected = { rowOne, rowZero, };
        model.validate(allExpected, Select.builder(metadata).build());
        model.validate(new ByteBuffer[][] {rowZero}, Select.builder(metadata).value("pk", 0).build());
        model.validate(new ByteBuffer[][] {rowZero}, Select.builder(metadata).value("s", 0).build());
        model.validate(new ByteBuffer[][] {rowOne}, Select.builder(metadata).value("pk", 1).build());
        model.validate(new ByteBuffer[][] {rowOne}, Select.builder(metadata).value("s", 1).build());
    }

    @Test
    public void deleteRowImpactsSearch()
    {
        TableMetadata metadata = new Builder().pk(1).ck(1).statics(1).regular(1).build();
        ASTSingleTableModel model = new ASTSingleTableModel(metadata);
        model.update(Mutation.insert(metadata)
                             .value("pk", 0)
                             .value("ck", 0)
                             .value("s", 0)
                             .value("v", 0)
                             .build());
        model.update(Mutation.delete(metadata)
                             .value("pk", 0)
                             .value("ck", 0)
                             .build());

        model.validate(EMPTY, Select.builder(metadata)
                                    .value("v", 0)
                                    .build());

        model.validate(EMPTY, Select.builder(metadata)
                                    .value("pk", 0)
                                    .value("v", 0)
                                    .build());
    }

    @Test
    public void tokenEqIncludesEmptyPartition()
    {
        // regression test; history
        /*
        History:
		1: INSERT INTO ks1.tbl (pk0, ck0, s0, v0, v1, v2, v3) VALUES (false, false, 'S' + '#', 0x7b, '21:54:38.042512095', -1220695853 + 487670685, 00000000-0000-1a00-b300-000000000000) -- on node1
		10: UPDATE ks1.tbl SET s0='\u001C{c|\u001Dz' + '\u0006rO\u0007``', v0=0xfffa8e324eb60d5510, v1='05:09:16.823129832', v2=519617565, v3=00000000-0000-1e00-b100-000000000000 WHERE  pk0 = true AND  ck0 = true -- on node1
		27: DELETE FROM ks1.tbl WHERE  pk0 = false AND  ck0 = false -- on node1
		69: DELETE s0 FROM ks1.tbl WHERE  pk0 = false -- on node1
		72: SELECT * FROM ks1.tbl WHERE token(pk0) = token(false) -- by token, on node1, fetch size 1
         */
        TableMetadata metadata = defaultTable()
                                 .addPartitionKeyColumn("pk", BooleanType.instance)
                                 .addClusteringColumn("ck", BooleanType.instance)
                                 .addStaticColumn("s", AsciiType.instance)
                                 .addRegularColumn("v0", BytesType.instance)
                                 .build();
        ASTSingleTableModel model = new ASTSingleTableModel(metadata);
        model.update(Mutation.insert(metadata)
                             .value("pk", false)
                             .value("ck", false)
                             .value("s", "'first'")
                             .value("v0", "0x7b")
                             .build());
        model.update(Mutation.update(metadata)
                             .set("s", "'second'")
                             .set("v0", "0xfffa8e324eb60d5510")
                             .value("pk", true)
                             .value("ck", true)
                             .build());
        model.update(Mutation.delete(metadata)
                             .value("pk", false)
                             .value("ck", false)
                             .build());
        // when deleting static columns the check if the partition should be deleted didn't happen, and the filtering
        // logic never excluded shouldDelete partitions
        model.update(Mutation.delete(metadata)
                             .column("s")
                             .value("pk", false)
                             .build());

        model.validate(EMPTY, Select.builder(metadata)
                                    .where(FunctionCall.tokenByColumns(new Symbol("pk", BooleanType.instance)),
                                           Inequality.EQUAL,
                                           FunctionCall.tokenByValue(new Bind(false, BooleanType.instance)))
                                    .build());
    }

    private static TableMetadata.Builder defaultTable()
    {
        return TableMetadata.builder("ks", "tbl")
                            .kind(TableMetadata.Kind.REGULAR)
                            .partitioner(Murmur3Partitioner.instance);
    }

    private static FunctionCall tokenFunction(BytesPartitionState.Ref ref)
    {
        return FunctionCall.tokenByValue(Stream.of(ref.key.getBufferArray()).map(bb -> new Bind(bb, BytesType.instance)).collect(Collectors.toList()));
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

    private static class ModelModel
    {
        private final ASTSingleTableModel model;
        private final TreeMap<BytesPartitionState.Ref, List<ByteBuffer[]>> partitions = new TreeMap<>();

        private ModelModel(ASTSingleTableModel model)
        {
            this.model = model;
        }

        Iterable<BytesPartitionState.Ref> refs()
        {
            return partitions.keySet();
        }

        ByteBuffer[] add(ByteBuffer[] row)
        {
            BytesPartitionState.Ref ref = createRef(row);
            partitions.computeIfAbsent(ref, i -> new ArrayList<>()).add(row);
            return row;
        }

        private BytesPartitionState.Ref createRef(ByteBuffer[] row)
        {
            ByteBuffer[] pks = Arrays.copyOf(row, model.factory.partitionColumns.size());
            return model.factory.createRef(new BufferClustering(pks));
        }

        public ByteBuffer[][] all()
        {
            return allWhere(i -> true);
        }

        public ByteBuffer[][] allEq(Symbol column, ByteBuffer value)
        {
            return allWhere(column, Inequality.EQUAL, value);
        }

        public ByteBuffer[][] allWhere(Symbol column, Inequality inequality, ByteBuffer value)
        {
            int idx = model.factory.selectionOrder.indexOf(column);
            return allWhere(row -> {
                ByteBuffer actual = row[idx];
                if (actual == null) return false;
                return include(column.type(), actual, inequality, value);
            });
        }

        public ByteBuffer[][] allWhere(Symbol column,
                                       Inequality left, ByteBuffer leftValue,
                                       Inequality right, ByteBuffer rightValue)
        {
            int idx = model.factory.selectionOrder.indexOf(column);
            return allWhere(row -> {
                ByteBuffer actual = row[idx];
                if (actual == null) return false;
                return include(column.type(), actual, left, leftValue) &&
                       include(column.type(), actual, right, rightValue);
            });
        }

        private ByteBuffer[][] allWhere(Inequality inequality, Token token)
        {
            return allWhere(ref -> include(ref, inequality, token), i -> true);
        }

        private ByteBuffer[][] allWhere(Inequality left, Token leftToken,
                                        Inequality right, Token rightToken)
        {
            return allWhere(ref -> include(ref, left, leftToken) && include(ref, right, rightToken), i -> true);
        }

        private ByteBuffer[][] allWhere(Predicate<ByteBuffer[]> predicate)
        {
            return allWhere(i -> true, predicate);
        }

        private ByteBuffer[][] allWhere(Predicate<BytesPartitionState.Ref> partitionPredicate,
                                        Predicate<ByteBuffer[]> rowPredicate)
        {
            List<ByteBuffer[]> rows = new ArrayList<>();
            for (var e : partitions.entrySet())
            {
                BytesPartitionState.Ref ref = e.getKey();
                if (!partitionPredicate.test(ref))
                    continue;
                List<ByteBuffer[]> partition = e.getValue();
                for (ByteBuffer[] row : partition)
                {
                    if (rowPredicate.test(row))
                        rows.add(row);
                }
            }
            return rows.toArray(ByteBuffer[][]::new);
        }

        private static boolean include(AbstractType<?> type, ByteBuffer actual, Inequality inequality, ByteBuffer value)
        {
            return include(inequality, type.compare(actual, value), () -> actual.equals(value));
        }

        private static boolean include(BytesPartitionState.Ref ref, Inequality inequality, Token token)
        {
            return include(inequality, ref.token.compareTo(token), () -> ref.token.equals(token));
        }

        private static boolean include(Inequality inequality, int rc, BooleanSupplier eq)
        {
            switch (inequality)
            {
                case EQUAL:
                    if (eq.getAsBoolean())
                        return true;
                    break;
                case NOT_EQUAL:
                    if (!eq.getAsBoolean())
                        return true;
                    break;
                case LESS_THAN_EQ:
                    if (rc == 0)
                        return true;
                case LESS_THAN:
                    if (rc < 0)
                        return true;
                    break;
                case GREATER_THAN_EQ:
                    if (rc == 0)
                        return true;
                case GREATER_THAN:
                    if (rc > 0)
                        return true;
                    break;
                default:
                    throw new UnsupportedOperationException(inequality.name());
            }
            return false;
        }
    }

    private interface ColumnValue
    {
        ByteBuffer accept(ColumnMetadata.Kind kind, int offset);
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
            TableMetadata.Builder builder = defaultTable();
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
