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
import java.util.List;

import javax.annotation.Nullable;

import org.junit.Test;

import accord.utils.Gen;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Generators;

import static accord.utils.Property.qt;

public class TxnReferenceOperationTest
{

    private static final String KS = "ks";
    private static final TxnData EMPTY = new TxnData();

    @Test
    public void serde()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().withExamples(10_000).forAll(gen()).check(txnOp -> {
            Serializers.testSerde(output, TxnReferenceOperation.serializer, txnOp, TableMetadatas.of(txnOp.table));

            txnOp.toOperation(EMPTY);
        });
    }

    private enum Group
    {
        Setter, SetterByIndex, SetterByKey, SetterByField,
        Adder, Subtracter,
        Appender, Putter, Discarder,// Prepender,
    }

    private static Gen<TxnReferenceOperation> gen()
    {
        /*
            ListPrepender       - x = ? + x
         */
        return rs -> {
            TxnReferenceOperation.Kind kind;
            ColumnMetadata receiver;
            TableMetadata table;
            @Nullable ByteBuffer key = null;
            @Nullable ByteBuffer field = null;
            TxnReferenceValue value;
            Group group = rs.pick(Group.values());
            switch (group)
            {
                case Discarder:
                {
                    CollectionType<?> type = (CollectionType<?>) Generators.toGen(AbstractTypeGenerators.builder()
                                                                                                        .withMaxDepth(1)
                                                                                                        .withTypeKinds(AbstractTypeGenerators.TypeKind.LIST,
                                                                                                                       AbstractTypeGenerators.TypeKind.SET,
                                                                                                                       AbstractTypeGenerators.TypeKind.MAP)
                                                                                                        .build())
                                                                           .next(rs);
                    kind = type instanceof ListType
                           ? TxnReferenceOperation.Kind.ListDiscarder
                           : TxnReferenceOperation.Kind.SetDiscarder;
                    table = table(type);
                    receiver = table.getColumn(ColumnIdentifier.getInterned("col", true));
                    if (kind == TxnReferenceOperation.Kind.ListDiscarder && rs.nextBoolean())
                    {
                        // DiscarderByIndex
                        kind = TxnReferenceOperation.Kind.ListDiscarderByIndex;
                        value = new TxnReferenceValue.Constant(Int32Type.instance.decompose(42));
                    }
                    else
                    {
                        CollectionType<?> discardType = type instanceof MapType
                                                        ? SetType.getInstance(((MapType<?, ?>) type).getKeysType(), true)
                                                        : type;
                        value = new TxnReferenceValue.Constant(Generators.toGen(AbstractTypeGenerators.getTypeSupport(discardType).bytesGen()).next(rs));
                    }
                }
                break;
                case Adder:
                case Subtracter:
                {
                    if (group == Group.Adder && rs.nextBoolean())
                    {
                        var type = SetType.getInstance(Int32Type.instance, true);
                        table = table(type);
                        receiver = table.getColumn(ColumnIdentifier.getInterned("col", true));
                        value = new TxnReferenceValue.Constant(Generators.toGen(AbstractTypeGenerators.getTypeSupport(Int32Type.instance).bytesGen()).next(rs));
                        kind = TxnReferenceOperation.Kind.SetAdder;
                    }
                    else
                    {
                        var type = Int32Type.instance;
                        table = table(type);
                        receiver = table.getColumn(ColumnIdentifier.getInterned("col", true));
                        value = new TxnReferenceValue.Constant(Generators.toGen(AbstractTypeGenerators.getTypeSupport(type).bytesGen()).next(rs));
                        kind = group == Group.Adder ? TxnReferenceOperation.Kind.ConstantAdder : TxnReferenceOperation.Kind.ConstantSubtracter;
                    }
                }
                break;
                case Setter:
                {
                    var type = Generators.toGen(AbstractTypeGenerators.builder()
                                                                      .withoutUnsafeEquality()
                                                                      .withMaxDepth(1)
                                                                      .build())
                               .next(rs);
                    table = table(type);
                    receiver = table.getColumn(ColumnIdentifier.getInterned("col", true));
                    value = new TxnReferenceValue.Constant(Generators.toGen(AbstractTypeGenerators.getTypeSupport(type).bytesGen()).next(rs));
                    if (type instanceof ListType)
                        kind = TxnReferenceOperation.Kind.ListSetter;
                    else if (type instanceof SetType)
                        kind = TxnReferenceOperation.Kind.SetSetter;
                    else if (type instanceof MapType)
                        kind = TxnReferenceOperation.Kind.MapSetter;
                    else if (type instanceof UserType)
                        kind = TxnReferenceOperation.Kind.UserTypeSetter;
                    else
                        kind = TxnReferenceOperation.Kind.ConstantSetter;
                }
                break;
                case SetterByIndex:
                {
                    ListType<String> type = ListType.getInstance(UTF8Type.instance, true);
                    table = table(type);
                    receiver = table.getColumn(ColumnIdentifier.getInterned("col", true));
                    value = new TxnReferenceValue.Constant(Generators.toGen(AbstractTypeGenerators.getTypeSupport(type.getElementsType()).bytesGen()).next(rs));
                    kind = TxnReferenceOperation.Kind.ListSetterByIndex;// x[?] = ?
                    key = Int32Type.instance.decompose(42);
                }
                break;
                case Appender:
                {
                    ListType<String> type = ListType.getInstance(UTF8Type.instance, true);
                    table = table(type);
                    receiver = table.getColumn(ColumnIdentifier.getInterned("col", true));
                    value = new TxnReferenceValue.Constant(Generators.toGen(AbstractTypeGenerators.getTypeSupport(type).bytesGen()).next(rs));
                    kind = TxnReferenceOperation.Kind.ListAppender;
                }
                break;
                case SetterByKey:
                {
                    MapType<Integer, String> type = MapType.getInstance(Int32Type.instance, UTF8Type.instance, true);
                    table = table(type);
                    receiver = table.getColumn(ColumnIdentifier.getInterned("col", true));
                    value = new TxnReferenceValue.Constant(Generators.toGen(AbstractTypeGenerators.getTypeSupport(type.getValuesType()).bytesGen()).next(rs));
                    kind = TxnReferenceOperation.Kind.MapSetterByKey;
                    key = Int32Type.instance.decompose(42);
                }
                break;
                case Putter: // x += {foo: bar, baz: biz} -- basically Appender but for Map!
                {
                    MapType<Integer, String> type = MapType.getInstance(Int32Type.instance, UTF8Type.instance, true);
                    table = table(type);
                    receiver = table.getColumn(ColumnIdentifier.getInterned("col", true));
                    value = new TxnReferenceValue.Constant(Generators.toGen(AbstractTypeGenerators.getTypeSupport(type).bytesGen()).next(rs));
                    kind = TxnReferenceOperation.Kind.MapPutter;
                }
                break;
                case SetterByField:
                {
                    UserType type = new UserType(KS, ByteBufferUtil.bytes("udt"),
                                                 List.of(FieldIdentifier.forUnquoted("f1")),
                                                 List.of(UTF8Type.instance),
                                                 true);
                    kind = TxnReferenceOperation.Kind.UserTypeSetterByField;
                    table = table(type);
                    receiver = table.getColumn(ColumnIdentifier.getInterned("col", true));
                    value = new TxnReferenceValue.Constant(Generators.toGen(AbstractTypeGenerators.getTypeSupport(UTF8Type.instance).bytesGen()).next(rs));
                    field = FieldIdentifier.forUnquoted("f1").bytes;
                }
                break;

                default:
                    throw new UnsupportedOperationException();
            }
            return new TxnReferenceOperation(kind, receiver, table, key, field, value);
        };
    }

    private static TableMetadata table(AbstractType<?> type)
    {
        return TableMetadata.builder(KS, "tbl")
               .partitioner(Murmur3Partitioner.instance)
               .addPartitionKeyColumn("pk", Int32Type.instance)
               .addRegularColumn("col", type)
               .build();
    }
}