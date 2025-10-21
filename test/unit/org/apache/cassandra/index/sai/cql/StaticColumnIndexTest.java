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

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.distributed.test.cql3.SingleNodeTableWalkTest;
import org.apache.cassandra.index.sai.SAITester;

public class StaticColumnIndexTest extends SAITester
{
    @Test
    public void staticIndexReturnsAllRowsInPartition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, val1 int static, val2 int, PRIMARY KEY(pk, ck))");
        disableCompaction(KEYSPACE);
        createIndex("CREATE INDEX ON %s(val1) USING 'sai'");

        execute("INSERT INTO %s(pk, ck, val1, val2) VALUES(?, ?, ?, ?)", 1, 1, 2, 1);
        execute("INSERT INTO %s(pk, ck,       val2) VALUES(?, ?,    ?)", 1, 2,    2);
        execute("INSERT INTO %s(pk, ck,       val2) VALUES(?, ?,    ?)", 1, 3,    3);

        beforeAndAfterFlush(() -> assertRows(execute("SELECT pk, ck, val1, val2 FROM %s WHERE val1 = 2"),
                                             row(1, 1, 2, 1), row(1, 2, 2, 2), row(1, 3, 2, 3)));
    }

    @Test
    public void staticIndexAndNonStaticIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, val1 int static, val2 int, PRIMARY KEY(pk, ck))");
        disableCompaction(KEYSPACE);
        createIndex("CREATE INDEX ON %s(val1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(val2) USING 'sai'");

        execute("INSERT INTO %s(pk, ck, val1, val2) VALUES(?, ?, ?, ?)", 1, 1, 20, 1000);
        execute("INSERT INTO %s(pk, ck,       val2) VALUES(?, ?,    ?)", 1, 2,     2000);
        execute("INSERT INTO %s(pk, ck, val1, val2) VALUES(?, ?, ?, ?)", 2, 1, 40, 2000);

        beforeAndAfterFlush(() -> assertRows(execute("SELECT pk, ck, val1, val2 FROM %s WHERE val1 = 20 AND val2 = 2000"),
                                             row(1, 2, 20, 2000)));
    }

    @Test
    public void staticAndNonStaticRangeIntersection() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v1 int, s1 int static, PRIMARY KEY(pk, ck))");
        disableCompaction(KEYSPACE);
        createIndex("CREATE INDEX ON %s(v1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(s1) USING 'sai'");

        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 1, 0);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 2, 1);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 3, 2);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 4, 3);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 5, 4);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 6, 5);

        execute("INSERT INTO %s (pk, s1) VALUES (?, ?)", 0, 100);

        beforeAndAfterFlush(() -> assertRowCount(execute("SELECT * FROM %s WHERE pk = ? AND v1 > ? AND s1 = ?", 0, 2, 100), 3));
    }

    /**
     * Originally discovered by {@link SingleNodeTableWalkTest} with seed -464866883761188308
     */
    @Test
    public void testTupleAndBlobFiltering() throws Throwable
    {
        String blobTupleType = createType("CREATE TYPE IF NOT EXISTS %s (f0 blob)");
        String boolTinyTextType = createType("CREATE TYPE IF NOT EXISTS %s (f0 boolean, f1 tinyint, f2 text)");
        createTable("CREATE TABLE %s (pk0 time, pk1 uuid, ck0 uuid, ck1 blob, s0 frozen<tuple<smallint, frozen<set<float>>>> static, " +
                    "                 v0 vector<vector<int, 2>, 3>, v1 frozen<map<frozen<" + blobTupleType + ">, vector<bigint, 2>>>, " +
                    "                 v2 vector<frozen<" + boolTinyTextType + ">, 2>, v3 bigint, PRIMARY KEY ((pk0, pk1), ck0, ck1)) WITH CLUSTERING ORDER BY (ck0 DESC, ck1 DESC)");
        disableCompaction(KEYSPACE);
        createIndex("CREATE INDEX tbl_pk1 ON %s(pk1) USING 'sai'");
        createIndex("CREATE INDEX tbl_s0 ON %s(s0) USING 'sai'");

        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, s0, v0, v1, v2, v3) " +
                "VALUES ('02:43:47.716011275', 00000000-0000-4200-b200-000000000000, 00000000-0000-4e00-8400-000000000000, 0xf2791941aea8e469, " +
                "        (12129, {-2.58545975E14}), [[-1781797567, 330686172], [103364202, 2031130152], [-550709009, 492544493]], " +
                "        {{f0: 0x34839b8bae653b2bdee8}: [-8431172225521461427, 8894719445990427242]}, [{f0: false, f1: 53, f2: '嵆왛孷쏆䊖恣'}, {f0: true, f1: 21, f2: 'ᨚ?榥쯢?ɚ챛ퟡ'}], 9167463065336786821) USING TIMESTAMP 3");
        execute("UPDATE %s USING TIMESTAMP 4 " +
                "SET s0=(23307, {-8.214548E-18}), v0=[[672139924, -1253475201], [353181149, -1829076723], [179355765, 379303855]], " +
                "    v1={{f0: 0x64850696464d}: [-7485547085069825418, 7795885370802556756], {f0: 0x67633db6f091}: [-8484578637223040646, 8216210044102487771]}, " +
                "    v2=[{f0: true, f1: 68, f2: '䝿ᝧ䶨푥펟겭매郂쀌'}, {f0: true, f1: 98, f2: '髃爫삿챥卛☓읂ີ?'}], v3=-4626482462417652499 * -7377486305688263453 " +
                "WHERE  pk0 = '03:36:30.876439626' AND  pk1 = 00000000-0000-4000-ad00-000000000000 AND  ck0 = 00000000-0000-4000-9f00-000000000000 AND  ck1 = 0xa06bb301");
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, s0, v0, v1, v2, v3) " +
                "VALUES ('07:08:47.775161332', 00000000-0000-4800-ad00-000000000000, 00000000-0000-4a00-a500-000000000000, 0xfef0d63ff7, (-15283, {-1.132058E24, 2.9319742E-31}), " +
                "        [[-335960956, 678086816], [-2139882146, 1011627708], [-55338955, -2094185756]], {{f0: 0xd9c3ab}: [-9002034104664383537, -8074261670215737032]}, " +
                "        [{f0: true, f1: -79, f2: '霠♘칳⦵ঋ幗䶐'}, {f0: true, f1: 7, f2: '䉻ݹ鞞텔㙠'}], 1885613374025825905) USING TIMESTAMP 5");
        execute("DELETE FROM %s USING TIMESTAMP 6 WHERE  pk0 = '14:02:14.975449434' AND  pk1 = 00000000-0000-4900-9900-000000000000");
        execute("DELETE FROM %s USING TIMESTAMP 7 WHERE  pk0 = '12:15:35.151327231' AND  pk1 = 00000000-0000-4500-ac00-000000000000");
        execute("DELETE FROM %s USING TIMESTAMP 8 WHERE  pk0 = '07:08:47.775161332' AND  pk1 = 00000000-0000-4800-ad00-000000000000 AND  ck0 = 00000000-0000-4b00-b000-000000000000 AND  ck1 = 0xa4121adb08");
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, s0, v0, v1, v2, v3) " +
                "VALUES ('03:36:30.876439626', 00000000-0000-4000-ad00-000000000000, 00000000-0000-4600-b400-000000000000, 0x63f5, (28387, {-1.18764904E-20}), " +
                "        [[-441895935, 313114446], [-740629531, -678512740], [1429899934, -1259907921]], {{f0: 0x5df1}: [414225888834712632, -5730196176171247108], " +
                "        {f0: 0x92c1497d7072b81c91}: [-7587541014989351350, -2813091340484612608]}, [{f0: true, f1: 41, f2: '쎺╇⒀왶'}, {f0: true, f1: -84, f2: '턺䋏篷'}], -1473884563651667176 + 128345915915881356) USING TIMESTAMP 9");

        beforeAndAfterFlush(() -> assertRows(execute("SELECT pk0, pk1, ck0, ck1 FROM %s WHERE s0 = (28387, {-1.18764904E-20}) AND pk1 = 00000000-0000-4000-ad00-000000000000 AND ck1 = 0xa06bb301 LIMIT 307 ALLOW FILTERING"),
                                             row(TimeType.instance.fromString("03:36:30.876439626"), UUIDType.instance.fromString("00000000-0000-4000-ad00-000000000000"), 
                                                 UUIDType.instance.fromString("00000000-0000-4000-9f00-000000000000"), BytesType.instance.fromString("a06bb301"))));
    }
}
