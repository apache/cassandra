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

package org.apache.cassandra.fuzz.harry.integration.model;

import java.util.Arrays;

import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.Surjections;
import org.apache.cassandra.harry.util.BitSet;

public class MockSchema
{
    public static final String KEYSPACE = "harry";
    public static final Surjections.Surjection<BitSet> columnMaskSelector1;
    public static final Surjections.Surjection<BitSet> columnMaskSelector2;
    public static final Surjections.Surjection<BitSet> compactColumnMaskSelector;

    public static final SchemaSpec tbl1;
    public static final SchemaSpec tbl2;
    public static final SchemaSpec compact_schema;

    static
    {
        columnMaskSelector1 = Surjections.pick(BitSet.create(0b0000111, 7),
                                               BitSet.create(0b1110000, 7),
                                               BitSet.create(0b1111111, 7));

        columnMaskSelector2 = Surjections.pick(BitSet.create(0b11, 2),
                                               BitSet.create(0b01, 2),
                                               BitSet.create(0b10, 2));

        compactColumnMaskSelector = Surjections.pick(BitSet.create(1, 1));

        tbl1 = new SchemaSpec(KEYSPACE, "tbl1",
                              Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType),
                                            ColumnSpec.pk("pk2", ColumnSpec.int64Type)),
                              Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType, false),
                                            ColumnSpec.ck("ck2", ColumnSpec.int64Type, false)),
                              Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.int32Type),
                                            ColumnSpec.regularColumn("v2", ColumnSpec.int64Type),
                                            // TODO: test boolean values; before that - submit a PR to Cassandra where we add boolean to ByteBufferUtil#objectToBytes
                                            ColumnSpec.regularColumn("v3", ColumnSpec.int32Type),
                                            ColumnSpec.regularColumn("v4", ColumnSpec.asciiType),
                                            ColumnSpec.regularColumn("v5", ColumnSpec.int64Type),
                                            ColumnSpec.regularColumn("v6", ColumnSpec.floatType),
                                            ColumnSpec.regularColumn("v7", ColumnSpec.doubleType)),
                              Arrays.asList(ColumnSpec.staticColumn("static1", ColumnSpec.asciiType),
                                            ColumnSpec.staticColumn("static2", ColumnSpec.int64Type)));

        tbl2 = new SchemaSpec(KEYSPACE, "tbl2",
                              Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType),
                                            ColumnSpec.pk("pk2", ColumnSpec.int64Type)),
                              Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType, true)),
                              Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.int32Type),
                                            ColumnSpec.regularColumn("v2", ColumnSpec.asciiType)),
                              Arrays.asList(ColumnSpec.regularColumn("static1", ColumnSpec.int32Type),
                                            ColumnSpec.regularColumn("static2", ColumnSpec.asciiType)));

        compact_schema = new SchemaSpec(KEYSPACE, "tbl3",
                                        Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType),
                                                      ColumnSpec.pk("pk2", ColumnSpec.int64Type)),
                                        Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType, true),
                                                      ColumnSpec.ck("ck2", ColumnSpec.int64Type, false)),
                                        Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.asciiType)),
                                        Arrays.asList(ColumnSpec.staticColumn("static1", ColumnSpec.asciiType)))
                         .withCompactStorage();
    }

    public static SchemaSpec randomSchema(String keyspace, String table, long seed)
    {
        return new SchemaGenerators.Builder(keyspace, () -> table)
               .partitionKeySpec(1, 4,
//                                 ColumnSpec.int8Type,
//                                 ColumnSpec.int16Type,
//                                 ColumnSpec.int32Type,
                                 ColumnSpec.int64Type,
                                 ColumnSpec.floatType,
                                 ColumnSpec.doubleType,
                                 ColumnSpec.asciiType(4, 10))
               .clusteringKeySpec(1, 4,
//                                  ColumnSpec.int8Type,
//                                  ColumnSpec.int16Type,
//                                  ColumnSpec.int32Type,
                                  ColumnSpec.int64Type,
                                  ColumnSpec.floatType,
                                  ColumnSpec.doubleType,
                                  ColumnSpec.asciiType(4, 10))
               .regularColumnSpec(2, 10,
                                  ColumnSpec.int8Type,
                                  ColumnSpec.int16Type,
                                  ColumnSpec.int32Type,
                                  ColumnSpec.int64Type,
                                  ColumnSpec.floatType,
                                  ColumnSpec.doubleType,
                                  ColumnSpec.asciiType(5, 10))
               .surjection()
               .inflate(seed);
    }
}
