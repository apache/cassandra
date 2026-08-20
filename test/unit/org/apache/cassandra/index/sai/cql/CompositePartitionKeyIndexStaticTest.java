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

import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.index.sai.SAITester;

public class CompositePartitionKeyIndexStaticTest extends SAITester
{
    @Test
    public void testIntersectionWithStaticOverlap() throws Throwable
    {
        createTable("CREATE TABLE %s (pk0 int, pk1 int, ck0 int, s1 int static, v0 int, PRIMARY KEY((pk0, pk1), ck0))");
        createIndex("CREATE CUSTOM INDEX ON %s(pk0) USING 'StorageAttachedIndex'");

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
        createIndex("CREATE CUSTOM INDEX tbl_pk0 ON %s(pk0) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX tbl_s0 ON %s(s0) USING 'StorageAttachedIndex'");

        // pk0: 23:15:13.897962392 -> (static clustering, -1296648-01-08)
        // s0: false -> (static clustering, -1296648-01-08)
        execute("INSERT INTO %s (pk0, pk1, ck0, s0, s1, v0) VALUES ('23:15:13.897962392', -2272, '-1296648-01-08', false, 'ᕊଖꥬ㨢걲映㚃', false)");

        // pk0: 23:15:13.897962392 -> (static clustering (existing), -1296648-01-08, -1306427-11-21)
        // s0: true -> (static clustering, -1306427-11-21)
        // TODO: C* uses s1='뾕⌒籖' + '鋿紞', which is not yet supported by CC, but will be supported in CC 5.0
        execute("UPDATE %s SET s0=true, s1='뾕⌒籖鋿紞', v0=true WHERE  pk0 = '23:15:13.897962392' AND  pk1 = -2272 AND  ck0 = '-1306427-11-21'");

        // Since the value of "true" is never mapped to the clustering -1296648-01-08, the intersection must begin
        // at the STATIC key. Otherwise, we will miss the WIDE key for clustering -1296648-01-08.
        beforeAndAfterFlush(() ->
                            assertRows(execute("SELECT * FROM %s WHERE s0 = true AND pk0 = '23:15:13.897962392'"),
                                       row(TimeType.instance.fromString("23:15:13.897962392"), new BigInteger("-2272"),
                                           SimpleDateType.instance.fromString("-1306427-11-21"), true, "뾕⌒籖鋿紞", true),
                                       row(TimeType.instance.fromString("23:15:13.897962392"), new BigInteger("-2272"),
                                           SimpleDateType.instance.fromString("-1296648-01-08"), true, "뾕⌒籖鋿紞", false)));
    }
}
