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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

public class ComplexQueryTest extends SAITester
{
    @Test
    public void partialUpdateTest()
    {
        createTable("CREATE TABLE %s (pk int, c1 text, c2 text, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(c1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c2) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, c1, c2) VALUES (?, ?, ?)", 1, "a", "a");
        flush();
        execute("UPDATE %s SET c1 = ? WHERE pk = ?", "b", 1);
        flush();
        execute("UPDATE %s SET c2 = ? WHERE pk = ?", "c", 1);
        flush();

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE c1 = 'b' AND c2='c'");
        assertRows(resultSet, row(1));
    }

    @Test
    public void splitRowsWithBooleanLogic()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, str_val text, val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        disableCompaction(KEYSPACE);

        // flush a sstable with 2 partial rows
        execute("INSERT INTO %s (pk, str_val) VALUES (3, 'A')");
        execute("INSERT INTO %s (pk, val) VALUES (1, 'A')");
        flush();

        // flush another sstable with 2 more partial rows, where PK 3 is now a complete row
        execute("INSERT INTO %s (pk, val) VALUES (3, 'A')");
        execute("INSERT INTO %s (pk, str_val) VALUES (2, 'A')");
        flush();

        // pk 3 should match
        var result = execute("SELECT pk FROM %s WHERE str_val = 'A' AND val = 'A'");
        assertRows(result, row(3));
    }

    @Test
    public void compositeTypeWithMapInsideQuery()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (" +
                              "pk1 frozen<map<'CompositeType(IntegerType,SimpleDateType)', 'DynamicCompositeType(Q=>LongType,I=>ByteType,6=>LexicalUUIDType)'>>," +
                              "pk2 frozen<tuple<frozen<tuple<float>>>>," +
                              "ck1 frozen<list<frozen<map<'LexicalUUIDType', ascii>>>>," +
                              "ck2 tinyint," +
                              "r1 frozen<list<'DynamicCompositeType(X=>DecimalType,y=>TimestampType,f=>BooleanType)'>> static," +
                              "r2 'DynamicCompositeType(P=>ShortType)'," +
                              "r3 'CompositeType(FrozenType(ListType(DoubleType)),FrozenType(MapType(LongType,DoubleType)),DoubleType)'," +
                              "r4 frozen<list<frozen<list<time>>>>," +
                              "r5 'CompositeType(CompositeType(ShortType,SimpleDateType,BooleanType),CompositeType(FloatType),MapType(ByteType,TimeType))'," +
                              "r6 set<smallint>," +
                              "PRIMARY KEY ((pk1, pk2), ck1, ck2))");



        createIndex("CREATE INDEX ON %s (FULL(ck1)) USING 'SAI'");
        createIndex("CREATE INDEX ON %s (FULL(pk1)) USING 'SAI'");
        createIndex("CREATE INDEX ON %s (FULL(r4)) USING 'SAI'");
        createIndex("CREATE INDEX ON %s (r2) USING 'SAI'");
        createIndex("CREATE INDEX ON %s (r3) USING 'SAI'");


        UntypedResultSet withMultipleColumns = execute("SELECT pk1 FROM " +
                                          "%s " +
                                          "WHERE r5 = 0x0010000230bd00000457f0bd31000001000000000700049f647252000000260000000200000001f300000008000001c4e14bba4b00000001260000000800003f2b300d385d00" +
                                          " AND r3 = 0x001c00000002000000083380d171eace676900000008e153bb97fdd5c22e00006d000000030000000897c5493857999fc000000013f08cc4fad0f04d0de51cff28d4ae743d2da1c40000000857108e8c372c868400000013f0cc6bca55f0ee240b27ff12c77a7b7dc3c665000000086c07d25fcdd3403500000013f0745922bdf0ac44c9b5ffd80f025ded9a211d000008200547f5da7a43aa00" +
                                          " AND  r2 = 0x8050000255e200 " +
                                          " AND pk2 = ((-1.2651989E-23))" +
                                          " ALLOW FILTERING;");

        assertRowCount(withMultipleColumns, 0);

        UntypedResultSet withoutSAI = execute("SELECT pk1 FROM " +
                                          "%s " +
                                          " WHERE r5 = 0x001c00000002000000083380d171eace676900000008e153bb97fdd5c22e00006d000000030000000897c5493857999fc000000013f08cc4fad0f04d0de51cff28d4ae743d2da1c40000000857108e8c372c868400000013f0cc6bca55f0ee240b27ff12c77a7b7dc3c665000000086c07d25fcdd3403500000013f0745922bdf0ac44c9b5ffd80f025ded9a211d000008200547f5da7a43aa00" +
                                          " ALLOW FILTERING;");


        assertRowCount(withoutSAI, 0);
    }
}
