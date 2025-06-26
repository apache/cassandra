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

package org.apache.cassandra.service.accord.txn;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

import accord.utils.DefaultRandom;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.SimplePartition;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.conditions.ColumnConditionTest;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.Generators;

import static accord.utils.Property.qt;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.Generators.toGen;

//TOOD (maintaince): rather than copy the condition supported kinds, maybe references directly from the type?
public class TxnConditionTest
{
    private static final SchemaProvider SCHEMA = new SchemaProvider();

    static
    {
        // ColumnMetadata serializer only stores the ks/table/name and uses Schema to load it
        Schema.instance = SCHEMA;
    }

    private static final FieldIdentifier UDT_F1 = FieldIdentifier.forInternalString("f1");
    private static final UserType UDT = new UserType("ks", ByteBufferUtil.bytes("udt"), Collections.singletonList(UDT_F1), Collections.singletonList(Int32Type.instance), true);
    private static final UserType FROZEN_UDT = UDT.freeze();

    private static final SetType<Integer> SET = SetType.getInstance(Int32Type.instance, true);
    private static final SetType<Integer> FROZEN_SET = SET.freeze();

    private static ColumnIdentifier name(ColumnMetadata.Kind kind, int offset)
    {
        StringBuilder sb = new StringBuilder(3);
        switch (kind)
        {
            case PARTITION_KEY:
                sb.append("pk");
                break;
            case CLUSTERING:
                sb.append("ck");
                break;
            case STATIC:
                sb.append('s');
                break;
            case REGULAR:
                sb.append('v');
                break;
            default:
                throw new UnsupportedOperationException(kind.name());
        }
        sb.append(offset);
        return ColumnIdentifier.getInterned(sb.toString(), false);
    }
    private static final ColumnIdentifier COL_PK1 = name(ColumnMetadata.Kind.PARTITION_KEY, 1);
    private static final ColumnIdentifier COL_S1 = name(ColumnMetadata.Kind.STATIC, 1);
    private static final ColumnIdentifier COL_S2 = name(ColumnMetadata.Kind.STATIC, 2);
    private static final ColumnIdentifier COL_S3 = name(ColumnMetadata.Kind.STATIC, 3);
    private static final ColumnIdentifier COL_S4 = name(ColumnMetadata.Kind.STATIC, 4);
    private static final ColumnIdentifier COL_S5 = name(ColumnMetadata.Kind.STATIC, 5);
    private static final ColumnIdentifier COL_CK1 = name(ColumnMetadata.Kind.CLUSTERING, 1);
    private static final ColumnIdentifier COL_R1 = name(ColumnMetadata.Kind.REGULAR, 1);
    private static final ColumnIdentifier COL_R2 = name(ColumnMetadata.Kind.REGULAR, 2);
    private static final ColumnIdentifier COL_R3 = name(ColumnMetadata.Kind.REGULAR, 3);
    private static final ColumnIdentifier COL_R4 = name(ColumnMetadata.Kind.REGULAR, 4);
    private static final ColumnIdentifier COL_R5 = name(ColumnMetadata.Kind.REGULAR, 5);
    private static final TableMetadata tb1 = TableMetadata.builder("ks", "tbl1")
                                                          .addPartitionKeyColumn(COL_PK1, Int32Type.instance)
                                                          .addStaticColumn(COL_S1, Int32Type.instance)
                                                          .addStaticColumn(COL_S2, FROZEN_UDT)
                                                          .addStaticColumn(COL_S3, FROZEN_SET)
                                                          .addStaticColumn(COL_S4, UDT)
                                                          .addStaticColumn(COL_S5, SET)
                                                          .addClusteringColumn(COL_CK1, Int32Type.instance)
                                                          .addRegularColumn(COL_R1, Int32Type.instance)
                                                          .addRegularColumn(COL_R2, FROZEN_UDT)
                                                          .addRegularColumn(COL_R3, FROZEN_SET)
                                                          .addRegularColumn(COL_R4, UDT)
                                                          .addRegularColumn(COL_R5, SET)
                                                          .partitioner(Murmur3Partitioner.instance)
                                                          .build();

    private static final ByteBuffer INT_1 = ByteBufferUtil.bytes(1);

    private static Gen<TxnCondition.Kind> BOOLEAN_KIND_GEN = Gens.pick(TxnCondition.Kind.AND, TxnCondition.Kind.OR);
    private static Gen<TxnCondition.Kind> EXISTS_KIND_GEN = Gens.pick(TxnCondition.Kind.IS_NOT_NULL, TxnCondition.Kind.IS_NULL);
    private static Gen<TxnCondition.Kind> VALUE_KIND_GEN = Gens.pick(TxnCondition.Kind.EQUAL, TxnCondition.Kind.NOT_EQUAL,
                                                                     TxnCondition.Kind.GREATER_THAN, TxnCondition.Kind.GREATER_THAN_OR_EQUAL,
                                                                     TxnCondition.Kind.LESS_THAN, TxnCondition.Kind.LESS_THAN_OR_EQUAL);
    private static Gen<ProtocolVersion> PROTOCOL_VERSION_GEN = Gens.enums().all(ProtocolVersion.class);
    private static Gen<ColumnMetadata> COLUM_METADATA_GEN = toGen(CassandraGenerators.columnMetadataGen()).map(cm -> {
        SCHEMA.add(cm);
        return cm;
    });
    private static Gen<ByteBuffer> BYTES_GEN = toGen(Generators.directAndHeapBytes(0, 10));
    private static Gen<TxnReference> TXN_REF_GEN = rs -> {
        {
            ColumnMetadata cm = COLUM_METADATA_GEN.next(rs);
            TableMetadata.Builder builder = TableMetadata.builder("", "", TableId.generate())
                                                        .addColumn(cm);
            if (!cm.isPartitionKey())
                builder.addPartitionKeyColumn(cm.name.toString().equals("_") ? "__" : "_", Int32Type.instance);
            TableMetadata tm = builder.build();
            cm = tm.getExistingColumn(cm.name);
            return rs.nextBoolean() ? TxnReference.column(rs.nextInt(0, Integer.MAX_VALUE), tm, cm)
                                    : TxnReference.column(rs.nextInt(0, Integer.MAX_VALUE), tm, cm, CellPath.create(BYTES_GEN.next(rs)));
        }
    };
    private static Gen<Clustering<?>> CLUSTERING_GEN = toGen(CassandraGenerators.CLUSTERING_GEN);
    private static Gen<ColumnCondition.Bound> BOUND_GEN = ColumnConditionTest.boundGen().map(b -> {
        SCHEMA.add(b.column);
        return b;
    });

    @Test
    public void serde()
    {
        DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(txnConditionGen()).check(condition -> {
            TableMetadatas.Collector collector = new TableMetadatas.Collector();
            condition.collect(collector);
            TableMetadatas tables = collector.build();
            Serializers.testSerde(output, TxnCondition.serializer, condition, tables);
            SCHEMA.clear();
        });
    }

    @Test
    public void isNullWithEmptyRows()
    {
        DecoratedKey key = tb1.partitioner.decorateKey(EMPTY_BYTE_BUFFER);
        TxnData data = TxnData.of(0, new TxnDataKeyValue(EmptyIterators.row(tb1, key, false)));
        TxnReference row = TxnReference.row(0);
        TxnReference pk = TxnReference.column(0, tb1, tb1.getExistingColumn(COL_PK1));
        TxnReference s = TxnReference.column(0, tb1, tb1.getExistingColumn(COL_CK1));
        TxnReference ck = TxnReference.column(0, tb1, tb1.getExistingColumn(COL_CK1));
        TxnReference r = TxnReference.column(0, tb1, tb1.getExistingColumn(COL_R1));

        for (TxnReference ref : Arrays.asList(row, pk, s, ck, r))
            assertExists(data, ref, false);
    }

    @Test
    public void isNullWithNullColumn()
    {
        IsNullTest simpleTest = (partition, clustering, column, nonNullValue) -> {
            // now include empty row (acts as null for most types)
            partition.clear().add(clustering)
                     .add(column, EMPTY_BYTE_BUFFER)
                     .build();
            TxnData data = TxnData.of(0, new TxnDataKeyValue(partition.filtered()));
            assertExists(data, TxnReference.column(0, tb1, column), !column.type.isNull(EMPTY_BYTE_BUFFER));

            // now include row
            partition.clear().add(clustering)
                     .add(column, nonNullValue)
                     .build();
            data = TxnData.of(0, new TxnDataKeyValue(partition.filtered()));
            assertExists(data, TxnReference.column(0, tb1, column), true);
        };
        IsNullFrozenFieldOrElementTest frozenFieldOrElementTest = (partition, clustering, column, path, nonNullValue, expectedValue) -> {
            var columnType = column.type.unwrap();
            var fieldOrElementType = columnType.isUDT() ? ((UserType) columnType).fieldType(path) : ((CollectionType<?>) columnType).nameComparator();
            // now include empty row (acts as null for most types)
            partition.clear().add(clustering)
                     .add(column, EMPTY_BYTE_BUFFER)
                     .build();
            TxnData data = TxnData.of(0, new TxnDataKeyValue(partition.filtered()));
            assertExists(data, TxnReference.column(0, tb1, column, path), false);

            // now include row
            partition.clear().add(clustering)
                     .add(column, nonNullValue)
                     .build();
            data = TxnData.of(0, new TxnDataKeyValue(partition.filtered()));
            assertExists(data, TxnReference.column(0, tb1, column, path), !fieldOrElementType.isNull(expectedValue));
        };
        IsNullComplexTest complexTest = (partition, clustering, column, values) -> {
            // now include column without value (tombstone)
            partition.clear().add(clustering)
                     .addComplex(column, Collections.emptyList())
                     .build();
            TxnData data = TxnData.of(0, new TxnDataKeyValue(partition.filtered()));
            assertExists(data, TxnReference.column(0, tb1, column), false);

            if (values.isEmpty()) return; // already tested
            // now include row
            partition.clear().add(clustering)
                     .addComplex(column, values)
                     .build();
            data = TxnData.of(0, new TxnDataKeyValue(partition.filtered()));
            assertExists(data, TxnReference.column(0, tb1, column), true);
        };
        IsNullFieldOrElementTest fieldOrElementTest = (partition, clustering, column, path, values, expectedValue) -> {
            var columnType = column.type.unwrap();
            var fieldOrElementType = columnType.isUDT() ? ((UserType) columnType).fieldType(path) : ((CollectionType<?>) columnType).nameComparator();
            // now include empty row (acts as null for most types)
            partition.clear().add(clustering)
                     .addComplex(column, Collections.emptyList())
                     .build();
            TxnData data = TxnData.of(0, new TxnDataKeyValue(partition.filtered()));
            assertExists(data, TxnReference.column(0, tb1, column, path), false);

            // now include row
            partition.clear().add(clustering)
                     .addComplex(column, values)
                     .build();
            data = TxnData.of(0, new TxnDataKeyValue(partition.filtered()));
            assertExists(data, TxnReference.column(0, tb1, column, path), !fieldOrElementType.isNull(expectedValue));
        };

        DecoratedKey key = tb1.partitioner.decorateKey(INT_1);
        BufferClustering ck = BufferClustering.make(INT_1);
        SimplePartition partition = new SimplePartition(tb1, key);


        partition.clear().addEmpty(ck);
        TxnData data = TxnData.of(0, new TxnDataKeyValue(partition.filtered()));
        assertExists(data, TxnReference.column(0, tb1, tb1.getExistingColumn(COL_PK1)), true);
        assertExists(data, TxnReference.column(0, tb1, tb1.getExistingColumn(COL_CK1)), false);
        assertExists(data, TxnReference.column(0, tb1, tb1.getExistingColumn(COL_S1)), false);
        assertExists(data, TxnReference.column(0, tb1, tb1.getExistingColumn(COL_R1)), false);

        // now run with liveness set
        partition.clear().addEmptyAndLive(ck);
        data = TxnData.of(0, new TxnDataKeyValue(partition.filtered()));
        assertExists(data, TxnReference.column(0, tb1, tb1.getExistingColumn(COL_CK1)), true);

        for (Clustering<?> clustering : Arrays.asList(Clustering.STATIC_CLUSTERING, ck))
        {
            ColumnMetadata.Kind kind = clustering == Clustering.STATIC_CLUSTERING ? ColumnMetadata.Kind.STATIC : ColumnMetadata.Kind.REGULAR;

            simpleTest.test(partition, clustering, tb1.getExistingColumn(name(kind, 1)), INT_1);

            simpleTest.test(partition, clustering, tb1.getExistingColumn(name(kind, 2)), FROZEN_UDT.pack(INT_1));
            frozenFieldOrElementTest.test(partition, clustering, tb1.getExistingColumn(name(kind, 2)), FROZEN_UDT.cellPathForField(UDT_F1), FROZEN_UDT.pack(INT_1), INT_1);
            frozenFieldOrElementTest.test(partition, clustering, tb1.getExistingColumn(name(kind, 2)), FROZEN_UDT.cellPathForField(UDT_F1), FROZEN_UDT.pack(EMPTY_BYTE_BUFFER), EMPTY_BYTE_BUFFER);

            //TODO (coverage): test list type, which supports by-offset and by-value
            //TODO (coverage): test map type, which has key/value which allows empty
            simpleTest.test(partition, clustering, tb1.getExistingColumn(name(kind, 3)), FROZEN_SET.pack(Collections.singletonList(INT_1)));
            frozenFieldOrElementTest.test(partition, clustering, tb1.getExistingColumn(name(kind, 3)), CellPath.create(INT_1), FROZEN_SET.pack(Collections.singletonList(INT_1)), INT_1);
            frozenFieldOrElementTest.test(partition, clustering, tb1.getExistingColumn(name(kind, 3)), CellPath.create(EMPTY_BYTE_BUFFER), FROZEN_SET.pack(Collections.singletonList(EMPTY_BYTE_BUFFER)), EMPTY_BYTE_BUFFER);

            complexTest.test(partition, clustering, tb1.getExistingColumn(name(kind, 4)), Collections.singletonList(INT_1));
            fieldOrElementTest.test(partition, clustering, tb1.getExistingColumn(name(kind, 4)), UDT.cellPathForField(UDT_F1), Collections.singletonList(INT_1), INT_1);
            fieldOrElementTest.test(partition, clustering, tb1.getExistingColumn(name(kind, 4)), UDT.cellPathForField(UDT_F1), Collections.singletonList(EMPTY_BYTE_BUFFER), EMPTY_BYTE_BUFFER);

            complexTest.test(partition, clustering, tb1.getExistingColumn(name(kind, 5)), Collections.singletonList(INT_1));
            fieldOrElementTest.test(partition, clustering, tb1.getExistingColumn(name(kind, 5)), CellPath.create(INT_1), Collections.singletonList(INT_1), INT_1);
            fieldOrElementTest.test(partition, clustering, tb1.getExistingColumn(name(kind, 5)), CellPath.create(EMPTY_BYTE_BUFFER), Collections.singletonList(EMPTY_BYTE_BUFFER), EMPTY_BYTE_BUFFER);
        }
    }

    @Test
    public void harryPotterAndTheMeaninglessEmptyness()
    {
        for (var type : AbstractTypeGenerators.meaninglessEmptyness())
        {
            if (type == CounterColumnType.instance) continue;
            TableMetadata metadata = TableMetadata.builder("ks", "tbl")
                                                  .addPartitionKeyColumn("pk", type)
                                                  .addClusteringColumn("ck", type)
                                                  .addRegularColumn("r", type)
                                                  .addStaticColumn("s", type)
                                                  .partitioner(Murmur3Partitioner.instance)
                                                  .build();
            ByteBuffer nonEmpty = toGen(AbstractTypeGenerators.getTypeSupport(type).bytesGen()).next(new DefaultRandom(42));
            Assertions.assertThat(nonEmpty).isNotEqualTo(EMPTY_BYTE_BUFFER); // double check...

            SimplePartition partition = new SimplePartition(metadata, metadata.partitioner.decorateKey(nonEmpty));
            Clustering<?> clustering = BufferClustering.make(nonEmpty);
            for (var c : metadata.regularAndStaticColumns())
                partition.add(c.isStatic() ? Clustering.STATIC_CLUSTERING : clustering)
                         .add(c, nonEmpty)
                         .build();
            TxnData data = TxnData.of(0, new TxnDataKeyValue(partition.filtered()));
            // the test is against empty, so can not ever apply
            for (var column : metadata.columns())
            {
                for (TxnCondition.Kind kind : TxnCondition.Value.supported())
                {
                    for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
                    {
                        TxnReference ref = TxnReference.column(0, metadata, column);
                        // empty logic reuses this which doesn't make the most sense... flesh it out
                        TxnCondition.Value condition = new TxnCondition.Value(ref.asColumn(), kind, EMPTY_BYTE_BUFFER, version);
                        Assertions.assertThat(condition.applies(data))
                                  .describedAs("column=%s, type=%s, kind=%s", column.name, type.asCQL3Type(), kind.name())
                                  .isFalse();
                    }
                }
            }

            // partition values have empty, so can not ever apply
            partition = new SimplePartition(metadata, metadata.partitioner.decorateKey(EMPTY_BYTE_BUFFER));
            clustering = BufferClustering.make(EMPTY_BYTE_BUFFER);
            for (var c : metadata.regularAndStaticColumns())
                partition.add(c.isStatic() ? Clustering.STATIC_CLUSTERING : clustering)
                         .add(c, EMPTY_BYTE_BUFFER)
                         .build();
            data = TxnData.of(0, new TxnDataKeyValue(partition.filtered()));
            for (var column : metadata.columns())
            {
                for (TxnCondition.Kind kind : TxnCondition.Value.supported())
                {
                    for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
                    {
                        TxnReference ref = TxnReference.column(0, metadata, column);
                        // empty logic reuses this which doesn't make the most sense... flesh it out
                        TxnCondition.Value condition = new TxnCondition.Value(ref.asColumn(), kind, nonEmpty, version);
                        Assertions.assertThat(condition.applies(data))
                                  .describedAs("column=%s, type=%s, kind=%s", column.name, type.asCQL3Type(), kind.name())
                                  .isFalse();
                    }
                }
            }
        }
    }

    @Test
    public void value()
    {
        Gen<AbstractType<?>> typeGen = toGen(new AbstractTypeGenerators.TypeGenBuilder()
                                             .withoutUnsafeEquality()
                                             .build());
        qt().check(rs -> {
            AbstractType<?> type = typeGen.next(rs);
            TableMetadata metadata = TableMetadata.builder("ks", "tbl")
                                                  .addPartitionKeyColumn("pk", type.freeze())
                                                  .addClusteringColumn("ck", type.freeze())
                                                  .addRegularColumn("r", type)
                                                  .addStaticColumn("s", type)
                                                  .partitioner(Murmur3Partitioner.instance)
                                                  .build();
            ByteBuffer value = toGen(AbstractTypeGenerators.getTypeSupport(type).bytesGen()).next(rs);
            List<ByteBuffer> complexValue = type.isMultiCell() ? split(type, value) : null;
            Clustering<?> clustering = BufferClustering.make(value);
            SimplePartition partition = new SimplePartition(metadata, metadata.partitioner.decorateKey(value));
            for (TxnCondition.Kind kind : TxnCondition.Value.supported())
            {
                for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
                {
                    for (ColumnMetadata column : metadata.columns())
                    {
                        TxnReference ref = TxnReference.column(0, metadata, column);
                        // empty logic reuses this which doesn't make the most sense... flesh it out
                        TxnCondition.Value condition = new TxnCondition.Value(ref.asColumn(), kind, value, version);

                        partition.clear().addEmptyAndLive(clustering);
                        // empty partition
                        boolean expected;
                        switch (kind)
                        {
                            case EQUAL:
                            case LESS_THAN_OR_EQUAL:
                            case GREATER_THAN_OR_EQUAL:
                                expected = column.isPrimaryKeyColumn();
                                break;
                            case NOT_EQUAL:
                            case LESS_THAN:
                            case GREATER_THAN:
                                expected = false;
                                break;
                            default:
                                throw new UnsupportedOperationException(kind.name());
                        }
                        Assertions.assertThat(condition.applies(TxnData.of(0, new TxnDataKeyValue(partition.filtered()))))
                                  .describedAs("column=%s, type=%s, kind=%s", column.name, type.asCQL3Type(), kind.name())
                                  .isEqualTo(expected);


                        if (column.isPrimaryKeyColumn()) continue;

                        // with value
                        if (type.isMultiCell())
                        {
                            partition.clear()
                                     .add(column.isStatic() ? Clustering.STATIC_CLUSTERING : clustering)
                                     .addComplex(column, complexValue)
                                     .build();
                        }
                        else
                        {
                            partition.clear()
                                     .add(column.isStatic() ? Clustering.STATIC_CLUSTERING : clustering)
                                     .add(column, value)
                                     .build();
                        }
                        switch (kind)
                        {
                            case EQUAL:
                            case LESS_THAN_OR_EQUAL:
                            case GREATER_THAN_OR_EQUAL:
                                expected = true;
                                break;
                            case NOT_EQUAL:
                            case LESS_THAN:
                            case GREATER_THAN:
                                expected = false;
                                break;
                            default:
                                throw new UnsupportedOperationException(kind.name());
                        }
                        Assertions.assertThat(condition.applies(TxnData.of(0, new TxnDataKeyValue(partition.filtered()))))
                                  .describedAs("column=%s, type=%s, kind=%s", column.name, type.asCQL3Type(), kind.name())
                                  .isEqualTo(expected);
                    }
                }
            }
        });
    }

    private static List<ByteBuffer> split(AbstractType<?> type, ByteBuffer value)
    {
        type = type.unwrap();
        if (type.isUDT())
        {
            return ((UserType) type).unpack(value);
        }
        else if (type.isCollection())
        {
            return ((CollectionType<?>) type).unpack(value);
        }
        throw new UnsupportedOperationException(type.asCQL3Type().toString());
    }

    private static void assertExists(TxnData data, TxnReference ref, boolean exists)
    {
        Assertions.assertThat(new TxnCondition.Exists(ref, TxnCondition.Kind.IS_NULL).applies(data)).describedAs("ref=%s %s", ref, exists ? "exists but shouldn't have applied" : "does not exist but should have applied").isEqualTo(!exists);
        Assertions.assertThat(new TxnCondition.Exists(ref, TxnCondition.Kind.IS_NOT_NULL).applies(data)).describedAs("ref=%s %s", ref, exists ? "exists but should have applied" : "does not exist but shouldn't have applied").isEqualTo(exists);
    }

    private Gen<TxnCondition> txnConditionGen()
    {
        return rs -> {
            switch (rs.nextInt(1, 5))
            {
                case 0: return TxnCondition.none();
                case 1: return new TxnCondition.Exists(TXN_REF_GEN.next(rs), EXISTS_KIND_GEN.next(rs));
                case 2: return new TxnCondition.Value(TXN_REF_GEN.next(rs).asColumn(), VALUE_KIND_GEN.next(rs), BYTES_GEN.next(rs), PROTOCOL_VERSION_GEN.next(rs));
                case 3: return new TxnCondition.ColumnConditionsAdapter(CLUSTERING_GEN.next(rs), Gens.lists(BOUND_GEN).ofSizeBetween(0, 3).next(rs));
                case 4: return new TxnCondition.BooleanGroup(BOOLEAN_KIND_GEN.next(rs), Gens.lists(txnConditionGen()).ofSizeBetween(0, 3).next(rs));
                default: throw new AssertionError();
            }
        };
    }

    private interface IsNullTest // jdk16+ lets this be in-lined with the test method rather than be here
    {
        void test(SimplePartition partition, Clustering<?> clustering, ColumnMetadata column, ByteBuffer nonNullValue);
    }

    private interface IsNullFrozenFieldOrElementTest // jdk16+ lets this be in-lined with the test method rather than be here
    {
        void test(SimplePartition partition, Clustering<?> clustering, ColumnMetadata column, CellPath path, ByteBuffer nonNullValue, ByteBuffer expectedValue);
    }

    private interface IsNullFieldOrElementTest // jdk16+ lets this be in-lined with the test method rather than be here
    {
        void test(SimplePartition partition, Clustering<?> clustering, ColumnMetadata column, CellPath path, List<ByteBuffer> values, ByteBuffer expectedValue);
    }

    private interface IsNullComplexTest // jdk16+ lets this be in-lined with the test method rather than be here
    {
        void test(SimplePartition partition, Clustering<?> clustering, ColumnMetadata column, List<ByteBuffer> values);
    }

    private static class SchemaProvider extends MockSchema.MockSchemaProvider
    {
        private final class Key
        {
            private final String keyspace, table;
            private final ByteBuffer name;

            private Key(String keyspace, String table, ByteBuffer name)
            {
                this.keyspace = keyspace;
                this.table = table;
                this.name = name;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Key key = (Key) o;
                return keyspace.equals(key.keyspace) && table.equals(key.table) && name.equals(key.name);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(keyspace, table, name);
            }

            @Override
            public String toString()
            {
                try
                {
                    return keyspace + "." + table + "/" + ByteBufferUtil.string(name);
                }
                catch (CharacterCodingException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
        private final Map<Key, ColumnMetadata> columns = new HashMap<>();

        public void add(ColumnMetadata cm)
        {
            columns.put(new Key(cm.ksName, cm.cfName, cm.name.bytes), cm);
        }

        public void clear()
        {
            columns.clear();
        }

        @Nullable
        @Override
        public ColumnMetadata getColumnMetadata(String keyspace, String table, ByteBuffer name)
        {
            Key key = new Key(keyspace, table, name);
            ColumnMetadata match = columns.get(key);
            if (match == null)
            {
                throw new AssertionError("Unable to find ColumnMetadata for " + key + "; known columns are " + columns.keySet());
            }
            return match;
        }
    }
}