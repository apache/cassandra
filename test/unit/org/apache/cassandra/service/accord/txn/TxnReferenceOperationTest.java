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

import javax.annotation.Nullable;

import org.junit.Test;

import accord.utils.Gen;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.Generators;

import static accord.utils.Property.qt;

public class TxnReferenceOperationTest
{
    @Test
    public void serde()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().withOnlySeed(3448382568143800303L).forAll(gen()).check(txnOp -> {
            Serializers.testSerde(output, TxnReferenceOperation.serializer, txnOp, TableMetadatas.of(txnOp.table));
        });
    }

    private enum Group
    {
        Setter,
//        SetterByIndex, SetterByKey, SetterByField,
//        Adder, Subtracter,
//        Appender, Discarder, Prepender,
    }

    private static Gen<TxnReferenceOperation> gen()
    {
        return rs -> {
            TxnReferenceOperation.Kind kind;
            ColumnMetadata receiver;
            TableMetadata table;
            @Nullable ByteBuffer key = null;
            @Nullable ByteBuffer field = null;
            TxnReferenceValue value;
            switch (rs.pick(Group.values()))
            {
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
//                case Constant:
//                {
//                    /*
//                    ConstantAdder
//                    ConstantSetter
//                    ConstantSubtracter
//                     */
//                }
//                break;
//                case List:
//                {
//                    /*
//                    ListAppender        - x += [...]
//                    ListDiscarder       - DELETE x[?]
//                    ListPrepender       - x = ? + x
//                    ListSetter          - x = [...]
//                    ListSetterByIndex   - x[?] = ?
//                     */
//                }
//                break;
//                case Set:
//                {
//                    /*
//                    SetAdder
//                    SetDiscarder
//                    SetSetter
//                     */
//                }
//                break;
//                case Map:
//                {
//                    /*
//                    MapPutter
//                    MapSetter
//                    MapSetterByKey
//                     */
//                }
//                break;
//                case UserType:
//                {
//                    /*
//                    UserTypeSetter
//                    UserTypeSetterByField
//                     */
//                }
//                break;
                default:
                    throw new UnsupportedOperationException();
            }
            return new TxnReferenceOperation(kind, receiver, table, key, field, value);
        };
    }

    private static TableMetadata table(AbstractType<?> type)
    {
        return TableMetadata.builder("ks", "tbl")
               .partitioner(Murmur3Partitioner.instance)
               .addPartitionKeyColumn("pk", Int32Type.instance)
               .addRegularColumn("col", type)
               .build();
    }
}