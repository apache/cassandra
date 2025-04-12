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
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.distributed.test.cql3.SingleNodeTableWalkTest;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CompositePartitionKeyIndexTest extends SAITester
{
    /**
     * Originally discovered by {@link SingleNodeTableWalkTest} with the following seeds:
     * 8837255108450816265
     * 1164443107607596330
     * -6614981692374717168
     * 1746205502103206170
     */
    @Test
    public void testStaticAndNonStaticKeysOnFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (pk0 tinyint, pk1 bigint, ck0 blob, s1 text static, s0 set<uuid> static, v0 smallint, PRIMARY KEY ((pk0, pk1), ck0)) WITH CLUSTERING ORDER BY (ck0 DESC)");
        disableCompaction(KEYSPACE);
        createIndex("CREATE INDEX tbl_pk0 ON %s(pk0) USING 'sai'");
        createIndex("CREATE INDEX tbl_pk1 ON %s(pk1) USING 'sai'");
        createIndex("CREATE INDEX tbl_s1 ON %s(s1) USING 'sai'");
        createIndex("CREATE INDEX tbl_v0 ON %s(v0) USING  'sai'");

        execute("INSERT INTO %s (pk0, pk1, ck0, s1, s0, v0) VALUES (-62, -5815950741950477880, 0x326f, '켅\uF6EB憓ᤃ\uEF32ꝃ窰ŷ', {00000000-0000-4700-aa00-000000000000}, 19310) USING TIMESTAMP 1");
        execute("DELETE FROM %s USING TIMESTAMP 2 WHERE  pk0 = 45 AND  pk1 = 6014418364385708772 AND  ck0 = 0x7c10");
        execute("DELETE FROM %s USING TIMESTAMP 3 WHERE  pk0 = -41 AND  pk1 = -3934225888295599640");
        execute("INSERT INTO %s (pk0, pk1, ck0, s1, s0, v0) " +
                "VALUES (-64, 7973592261481566341, 0x0d, '\uE11B摻', {00000000-0000-4800-8900-000000000000, 00000000-0000-4900-8600-000000000000}, -23873) USING TIMESTAMP 4");
        flush(KEYSPACE);

        execute("UPDATE %s USING TIMESTAMP 5 SET v0=-359, s1='ل≻Ⱆ喡䮠?' WHERE  pk0 = -64 AND  pk1 = 7973592261481566341 AND  ck0 = 0x99d570024de738f37877");
        execute("INSERT INTO %s (pk0, pk1, ck0, v0, s1, s0) " +
                "VALUES (-104, -4990846884898776392, 0xf7ac771298eaf1d4, -6977, '凘纖볭菮⏏↶?蜑', null) USING TIMESTAMP 6");
        execute("INSERT INTO %s (pk0, pk1, ck0, s1, s0, v0) " +
                "VALUES (-62, -5815950741950477880, 0x9277e744212e1c4b50, '\uF6AD瀛⛕徳倬糽ᢷ' + '雴', {00000000-0000-4700-b100-000000000000, 00000000-0000-4800-9300-000000000000}, 5423) USING TIMESTAMP 7");
        execute("DELETE FROM %s USING TIMESTAMP 8 WHERE  pk0 = -64 AND  pk1 = 7973592261481566341");
        flush(KEYSPACE);

        execute("DELETE s0, s1, s0 FROM %s USING TIMESTAMP 9 WHERE  pk0 = -62 AND  pk1 = -5815950741950477880");
        execute("DELETE FROM %s USING TIMESTAMP 10 WHERE  pk0 = -41 AND  pk1 = -3934225888295599640 AND  ck0 = 0xd753dc3a473acaf665");
        execute("INSERT INTO %s (pk0, pk1, ck0, s1, s0, v0) " +
                "VALUES (-62, -5815950741950477880, 0xd1e07b568a7188, 'ᑿ鼾戆' + '篐뵡?䰫', {00000000-0000-4500-b000-000000000000}, 17933) USING TIMESTAMP 11");
        execute("UPDATE %s USING TIMESTAMP 12 SET v0=null, s0={00000000-0000-4600-a000-000000000000, 00000000-0000-4d00-8200-000000000000, 00000000-0000-4f00-9200-000000000000} " +
                "WHERE  pk0 = -41 AND  pk1 = -3934225888295599640 AND  ck0 = 0x0dab3b038131efa2");

        assertRowCount(execute("SELECT * FROM %s WHERE pk0 >= ? LIMIT 81", (byte) -104), 5);
        execute("DELETE FROM %s USING TIMESTAMP 13 WHERE  pk0 = -64 AND  pk1 = 7973592261481566341");
        flush(KEYSPACE);

        beforeAndAfterFlush(() ->
                            assertRows(execute("SELECT pk0, pk1, ck0 FROM %s WHERE pk0 >= ?", (byte) -104),
                                       row((byte) -62, -5815950741950477880L, ByteBufferUtil.hexToBytes("d1e07b568a7188")),
                                       row((byte) -62, -5815950741950477880L, ByteBufferUtil.hexToBytes("9277e744212e1c4b50")),
                                       row((byte) -62, -5815950741950477880L, ByteBufferUtil.hexToBytes("326f")),
                                       row((byte) -104, -4990846884898776392L, ByteBufferUtil.hexToBytes("f7ac771298eaf1d4")),
                                       row((byte) -41, -3934225888295599640L, null)));
    }

    /**
     * Originally discovered by {@link SingleNodeTableWalkTest} with seed -5732060315438955166
     */
    @Test
    public void testIgnoreCellDeletions() throws Throwable
    {
        createTable("CREATE TABLE %s (pk0 boolean, pk1 varint, ck0 tinyint, ck1 varint, s0 list<frozen<map<double, smallint>>> static, " +
                    "                 s1 map<frozen<set<uuid>>, frozen<map<inet, date>>> static, v0 frozen<map<frozen<set<text>>, uuid>>, " +
                    "                 PRIMARY KEY ((pk0, pk1), ck0, ck1)) WITH CLUSTERING ORDER BY (ck0 DESC, ck1 DESC)");
        disableCompaction(KEYSPACE);
        createIndex("CREATE INDEX tbl_pk0 ON %s(pk0) USING 'sai'");

        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, s0, s1, v0) " +
                "VALUES (true, 0, 109, 0, [{2.2352903520430565E260: -29214, 2.605618737869944E274: -13041}], " +
                "        {{00000000-0000-4400-9f00-000000000000, 00000000-0000-4500-9b00-000000000000, 00000000-0000-4b00-bf00-000000000000}: {'18.112.79.221': '-2306623-03-19', '227.58.183.116': '-3929454-04-25'}}, " +
                "        {{'⭎憢?', '黣偛紑'}: 00000000-0000-4900-8600-000000000000, {'㛽ꓗ', '剢ꮱ死䰀륬ਐ喑ퟚ', '竖䝏爐뷤曀'}: 00000000-0000-4900-bc00-000000000000}) USING TIMESTAMP 1");
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, s1, v0) " +
                "VALUES (true, 0, 114, 742, {{00000000-0000-4000-9a00-000000000000, 00000000-0000-4700-ba00-000000000000}: {'96.31.70.25': '-912836-06-15', '185.90.18.173': '-5257542-01-31', '223.18.191.245': '-4633145-10-30'}}, " +
                "                           {{'뫥㩎뎠ྭẒ'}: 00000000-0000-4800-8600-000000000000}) USING TIMESTAMP 2");

        // This will result in the creation of erroneous postings if cell deletions are not accounted for:
        execute("DELETE v0, s1, s0 FROM %s USING TIMESTAMP 6 WHERE  pk0 = true AND  pk1 = 0 AND  ck0 = 121 AND  ck1 = 1");

        execute("UPDATE %s USING TIMESTAMP 8 SET s0 += [{4.3056056376102396E-169: 22551, 1.439623561042819E208: 20450}, {-2.7900719406964408E-242: 30147, 8.586565205109037E-211: 28721, 4.603864140847754E20: -12814}], " +
                "                                s1 += {{00000000-0000-4200-b900-000000000000, 00000000-0000-4500-ab00-000000000000}: {'2.67.240.121': '-471656-04-17', '134.186.187.51': '-2056459-04-13'}}, " +
                "                                v0={{'?', '蠥╩徰昰弳펠재', '됢簔Ὕ텇⢌យ稭澣'}: 00000000-0000-4d00-8d00-000000000000} " +
                "WHERE  pk0 = true AND  pk1 = 0 AND  ck0 = 37 AND  ck1 = 0");

        beforeAndAfterFlush(() ->
                assertRows(execute("SELECT pk0, pk1, ck0, ck1 FROM %s WHERE pk0 = ? LIMIT 4", true),
                           row(true, IntegerType.instance.fromString("0"), ByteType.instance.fromString("114"), IntegerType.instance.fromString("742")),
                           row(true, IntegerType.instance.fromString("0"), ByteType.instance.fromString("109"), IntegerType.instance.fromString("0")),
                           row(true, IntegerType.instance.fromString("0"), ByteType.instance.fromString("37"), IntegerType.instance.fromString("0"))));
    }

    @Test
    public void testIntersectionOnMixedPostingsOnDelete() throws Throwable
    {
        createTable("CREATE TABLE %s (pk0 boolean, pk1 uuid, ck0 date, ck1 smallint, s0 timeuuid static, v0 bigint, v1 float, PRIMARY KEY ((pk0, pk1), ck0, ck1)) WITH CLUSTERING ORDER BY (ck0 DESC, ck1 ASC)");
        disableCompaction(KEYSPACE);
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
        disableCompaction(KEYSPACE);
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
        disableCompaction(KEYSPACE);
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
        disableCompaction(KEYSPACE);
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
        disableCompaction(KEYSPACE);
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
        disableCompaction(KEYSPACE);
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
