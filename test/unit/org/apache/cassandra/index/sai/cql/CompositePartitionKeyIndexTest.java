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

import java.math.BigInteger;

import org.junit.Test;

import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.index.sai.SAITester;

public class CompositePartitionKeyIndexTest extends SAITester
{
    @Test
    public void testIntersectionOnMixedPostingsOnDelete() throws Throwable
    {
        createTable("CREATE TABLE %s (pk0 boolean, pk1 uuid, ck0 date, ck1 smallint, s0 timeuuid static, v0 bigint, v1 float, PRIMARY KEY ((pk0, pk1), ck0, ck1)) WITH CLUSTERING ORDER BY (ck0 DESC, ck1 ASC)");

        createIndex("CREATE INDEX tbl_pk0 ON %s(pk0) USING 'sai'");
        createIndex("CREATE INDEX tbl_ck0 ON %s(ck0) USING 'sai'");

        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, s0) VALUES (true, 00000000-0000-4700-8d00-000000000000, '-3038243-10-30', -12906, 00000000-0000-1900-aa00-000000000000)");        
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, v0, v1) VALUES (false, 00000000-0000-4f00-a200-000000000000, '-1225324-10-07', -3223, -7318794006633168842, 8.0350916E-32 + 6.127658E28)");
        execute("DELETE FROM %s WHERE  pk0 = false AND  pk1 = 00000000-0000-4f00-a200-000000000000 AND  ck0 = '-1111567-10-09' AND  ck1 = 25967");
        execute("DELETE s0 FROM %s WHERE  pk0 = false AND  pk1 = 00000000-0000-4500-9200-000000000000");

        beforeAndAfterFlush(() ->
                            assertRows(execute("SELECT * FROM %s WHERE pk0 = false AND ck0 = '-1225324-10-07'"),
                                       row(false, UUIDType.instance.fromString("00000000-0000-4f00-a200-000000000000"), 
                                           SimpleDateType.instance.fromString("-1225324-10-07"), (short) -3223, null,
                                           -7318794006633168842L, FloatType.instance.fromString("6.127658E28"))));
    }

    @Test
    public void testIntersectionOnMixedPostingsOnUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (pk0 boolean, pk1 uuid, ck0 date, ck1 smallint, s0 timeuuid static, v0 bigint, v1 float, PRIMARY KEY ((pk0, pk1), ck0, ck1)) WITH CLUSTERING ORDER BY (ck0 DESC, ck1 ASC)");

        createIndex("CREATE INDEX tbl_pk0 ON %s(pk0) USING 'sai'");
        createIndex("CREATE INDEX tbl_ck0 ON %s(ck0) USING 'sai'");

        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, s0) VALUES (true, 00000000-0000-4700-8d00-000000000000, '-3038243-10-30', -12906, 00000000-0000-1900-aa00-000000000000)");
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, v0, v1) VALUES (false, 00000000-0000-4f00-a200-000000000000, '-1225324-10-07', -3223, -7318794006633168842, 8.0350916E-32 + 6.127658E28)");
        execute("UPDATE %s SET v1 = 2.1 WHERE pk0 = false AND  pk1 = 00000000-0000-4f00-a200-000000000000 AND  ck0 = '-1111567-10-09' AND  ck1 = 25967");
        execute("UPDATE %s SET s0 = 00000000-0000-1900-aa00-000000000000 WHERE pk0 = false AND  pk1 = 00000000-0000-4500-9200-000000000000");

        beforeAndAfterFlush(() ->
                            assertRows(execute("SELECT * FROM %s WHERE pk0 = false AND ck0 = '-1225324-10-07'"),
                                       row(false, UUIDType.instance.fromString("00000000-0000-4f00-a200-000000000000"),
                                           SimpleDateType.instance.fromString("-1225324-10-07"), (short) -3223, null,
                                           -7318794006633168842L, FloatType.instance.fromString("6.127658E28"))));
    }

    @Test
    public void testIntersectionWithStaticOverlap() throws Throwable
    {
        createTable("CREATE TABLE %s (pk0 int, pk1 int, ck0 int, s1 int static, v0 int, PRIMARY KEY((pk0, pk1), ck0))");
        createIndex("CREATE INDEX ON %s(pk0) USING 'sai'");

        execute("UPDATE %s USING TIMESTAMP 1 SET s1 = 0, v0 = 0 WHERE pk0 = 0 AND pk1 = 1 AND ck0 = 0");
        execute("DELETE FROM %s USING TIMESTAMP 2 WHERE pk0 = 0 AND pk1 = 1");

        // If the STATIC and WIDE PrimaryKey objects in this partition are not compared strictly, the new WIDE key
        // will be interpreted as a duplicate and not added to the Memtable-adjacent index. Then, on flush, the row
        // corresponding to that WIDE key will be missing from the index.
        execute("UPDATE %s USING TIMESTAMP 3 SET v0 = 1 WHERE pk0 = 0 AND pk1 = 1 AND ck0 = 1");

        beforeAndAfterFlush(() -> assertRows(execute("SELECT * FROM %s WHERE v0 = 1 AND pk0 = 0 ALLOW FILTERING"), row(0, 1, 1, null, 1)));
    }

    @Test
    public void testIntersectionWithStaticUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (pk0 time, pk1 varint, ck0 date, s0 boolean static, s1 text static, v0 boolean, PRIMARY KEY ((pk0, pk1), ck0))");
        createIndex("CREATE INDEX tbl_pk0 ON %s(pk0) USING 'sai'");
        createIndex("CREATE INDEX tbl_s0 ON %s(s0) USING 'sai'");

        // pk0: 23:15:13.897962392 -> (static clustering, -1296648-01-08)
        // s0: false -> (static clustering, -1296648-01-08)
        execute("INSERT INTO %s (pk0, pk1, ck0, s0, s1, v0) VALUES ('23:15:13.897962392', -2272, '-1296648-01-08', false, 'ᕊଖꥬ㨢걲映㚃', false)");

        // pk0: 23:15:13.897962392 -> (static clustering (existing), -1296648-01-08, -1306427-11-21)
        // s0: true -> (static clustering, -1306427-11-21)
        execute("UPDATE %s SET s0=true, s1='뾕⌒籖' + '鋿紞', v0=true WHERE  pk0 = '23:15:13.897962392' AND  pk1 = -2272 AND  ck0 = '-1306427-11-21'");

        // Since the value of "true" is never mapped to the clustering -1296648-01-08, the intersection must begin
        // at the STATIC key. Otherwise, we will miss the WIDE key for clustering -1296648-01-08.
        beforeAndAfterFlush(() -> 
                            assertRows(execute("SELECT * FROM %s WHERE s0 = true AND pk0 = '23:15:13.897962392'"),
                                       row(TimeType.instance.fromString("23:15:13.897962392"), new BigInteger("-2272"),
                                           SimpleDateType.instance.fromString("-1306427-11-21"), true, "뾕⌒籖鋿紞", true),
                                       row(TimeType.instance.fromString("23:15:13.897962392"), new BigInteger("-2272"),
                                           SimpleDateType.instance.fromString("-1296648-01-08"), true, "뾕⌒籖鋿紞", false)));
    }

    @Test
    public void testCompositePartitionIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 text, val int, PRIMARY KEY((pk1, pk2)))");
        createIndex("CREATE INDEX ON %s(pk1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(pk2) USING 'sai'");

        execute("INSERT INTO %s (pk1, pk2, val) VALUES (1, '1', 1)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (2, '2', 2)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (3, '3', 3)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (4, '4', 4)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (5, '5', 5)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (6, '6', 6)");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 = 2"),
                                    expectedRow(2));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 > 1"),
                                    expectedRow(2),
                                    expectedRow(3),
                                    expectedRow(4),
                                    expectedRow(5),
                                    expectedRow(6));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 >= 3"),
                                    expectedRow(3),
                                    expectedRow(4),
                                    expectedRow(5),
                                    expectedRow(6));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 < 3"),
                                    expectedRow(1),
                                    expectedRow(2));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 <= 3"),
                                    expectedRow(1),
                                    expectedRow(2),
                                    expectedRow(3));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk2 = '2'"),
                                    expectedRow(2));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 > 1 AND pk2 = '2'"),
                                    expectedRow(2));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 = -1 AND pk2 = '2'"));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT * FROM %s WHERE pk1 = -1 AND val = 2");
        });
    }

    @Test
    public void testFilterWithIndexForContains() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v set<int>, PRIMARY KEY ((k1, k2)))");
        createIndex("CREATE INDEX ON %s(k2) USING 'sai'");

        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 0, set(1, 2, 3));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 1, set(2, 3, 4));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 0, set(3, 4, 5));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 1, set(4, 5, 6));

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE k2 = ?", 1),
                       row(0, 1, set(2, 3, 4)),
                       row(1, 1, set(4, 5, 6))
            );

            assertRows(execute("SELECT * FROM %s WHERE k2 = ? AND v CONTAINS ? ALLOW FILTERING", 1, 6),
                       row(1, 1, set(4, 5, 6))
            );

            assertEmpty(execute("SELECT * FROM %s WHERE k2 = ? AND v CONTAINS ? ALLOW FILTERING", 1, 7));
        });
    }

    private Object[] expectedRow(int index)
    {
        return row(index, Integer.toString(index), index);
    }
}
