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

package org.apache.cassandra.distributed.test.tracking;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.test.sai.SAIUtil;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;

public class MutationTrackingPartitionReadTest extends TestBaseImpl
{
    private static final int REPLICAS = 3;

    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = Cluster.build()
                         .withNodes(REPLICAS)
                         .withConfig(cfg -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                                             .set("hinted_handoff_enabled", false))
                         .start();
    }

    @AfterClass
    public static void teardown()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testEqQueryOnStaticColumn()
    {
        String keyspace = "test_eq_query_on_static_column";
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'", keyspace));

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk0 varint, pk1 uuid, ck0 time, s0 ascii static, v1 double, PRIMARY KEY ((pk0, pk1), ck0)) " +
                                          "WITH CLUSTERING ORDER BY (ck0 DESC) AND read_repair = 'NONE'", keyspace));
        cluster.schemaChange(withKeyspace("CREATE INDEX tbl_s0 ON %s.tbl(s0) USING 'sai'", keyspace));
        SAIUtil.waitForIndexQueryable(cluster, keyspace);
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, s0) VALUES (-58, 00000000-0000-4d00-8600-000000000000, 'foo') USING TIMESTAMP 5", keyspace));
        cluster.get(1).executeInternal(withKeyspace("DELETE s0, s0 FROM %s.tbl USING TIMESTAMP 13 WHERE  pk0 = 7 AND  pk1 = 00000000-0000-4e00-9600-000000000000", keyspace));
        cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, v1) VALUES (-58, 00000000-0000-4d00-8600-000000000000, '16:40:27.677919817', 1.6896613611522374E184) USING TIMESTAMP 14", keyspace));
        cluster.get(1).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 15 SET v1=8.05223257349057E-164 WHERE  pk0 = -58 AND  pk1 = 00000000-0000-4d00-8600-000000000000 AND  ck0 = '20:02:33.822429155'", keyspace));
        cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, s0) VALUES (-58, 00000000-0000-4d00-8600-000000000000, 'bar') USING TIMESTAMP 18", keyspace));

        String select = withKeyspace("SELECT pk0, pk1, ck0 FROM %s.tbl WHERE pk0 = -58 AND pk1 = 00000000-0000-4d00-8600-000000000000 AND s0 = 'bar'", keyspace);
        Object[][] result = cluster.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(result, row(BigInteger.valueOf(-58), UUID.fromString("00000000-0000-4d00-8600-000000000000"), 72153822429155L),
                           row(BigInteger.valueOf(-58), UUID.fromString("00000000-0000-4d00-8600-000000000000"), 60027677919817L));
    }

    @Test
    public void testMissingPartitionDelete()
    {
        String keyspace = "test_missing_partition_delete";
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'", keyspace));

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk0 text, pk1 bigint, ck0 smallint, v0 timestamp, v1 int, PRIMARY KEY ((pk0, pk1), ck0)) " +
                                          "WITH CLUSTERING ORDER BY (ck0 ASC) AND read_repair = 'NONE'", keyspace));
        cluster.schemaChange(withKeyspace("CREATE INDEX tbl_v1_idx ON %s.tbl(v1) USING 'sai'", keyspace));
        SAIUtil.waitForIndexQueryable(cluster, keyspace);
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        cluster.get(2).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 8 SET v1=1778069545 WHERE pk0 = 'ad1b:bbdc:e712:574:e7ca:104e:5abb:d9e1' AND pk1 = -5572993830691022649 AND ck0 = 32379", keyspace));
        cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 12 WHERE pk0 = 'ad1b:bbdc:e712:574:e7ca:104e:5abb:d9e1' AND pk1 = -5572993830691022649", keyspace));
        cluster.get(2).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 14 SET v0=null, v1=1353378764 WHERE pk0 = 'ad1b:bbdc:e712:574:e7ca:104e:5abb:d9e1' AND pk1 = -5572993830691022649 AND ck0 = 29521", keyspace));

        String select = withKeyspace("SELECT pk0, pk1, ck0, v1 FROM %s.tbl WHERE pk0 = 'ad1b:bbdc:e712:574:e7ca:104e:5abb:d9e1' AND pk1 = -5572993830691022649 AND v1 <= 1353378764 LIMIT 136", keyspace);
        cluster.coordinator(1).execute(select, ConsistencyLevel.ALL);

        select = withKeyspace("SELECT pk0, pk1, ck0, v1 FROM %s.tbl WHERE pk0 = 'ad1b:bbdc:e712:574:e7ca:104e:5abb:d9e1' AND pk1 = -5572993830691022649 AND v1 >= 1353378764 LIMIT 116", keyspace);
        Object[][] result = cluster.coordinator(1).execute(select, ConsistencyLevel.ALL);

        assertRows(result, row("ad1b:bbdc:e712:574:e7ca:104e:5abb:d9e1", -5572993830691022649L, (short) 29521, 1353378764));
    }

    @Test
    public void testMultiColumnPartitionRestrictedQuery()
    {
        String keyspace = "test_multi_column_partition_restricted";
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND replication_type='tracked'", keyspace));

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk0 varint, ck0 timeuuid,ck1 uuid, s0 int static, s1 vector<timeuuid, 2> static, v0 frozen<set<vector<time, 1>>>, v2 date, v3 int, v1 set<frozen<set<boolean>>>, PRIMARY KEY (pk0, ck0, ck1)) " +
                                          "WITH CLUSTERING ORDER BY (ck0 ASC, ck1 DESC) AND read_repair = 'NONE'", keyspace));

        cluster.schemaChange(withKeyspace("CREATE INDEX tbl_ck0 ON %s.tbl(ck0) USING 'sai'", keyspace));
        cluster.schemaChange(withKeyspace("CREATE INDEX tbl_ck1 ON %s.tbl(ck1) USING 'sai'", keyspace));
        cluster.schemaChange(withKeyspace("CREATE INDEX tbl_v3 ON %s.tbl(v3) USING 'sai'", keyspace));
        SAIUtil.waitForIndexQueryable(cluster, keyspace);
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 160 WHERE pk0 = -320778557", keyspace));
        cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, s0, s1) VALUES (-320778557, 1363549784, [00000000-0000-1d00-8300-000000000000, 00000000-0000-1a00-8900-000000000000]) USING TIMESTAMP 166", keyspace));
        cluster.get(1).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 168 SET v1 = {{false}, {false, true}}, v3 = -1669443995, s0 = 1234171012 " +
                                                    "WHERE pk0 = -320778557 AND ck0 = 00000000-0000-1400-9600-000000000000 " +
                                                    "AND ck1 IN (00000000-0000-4e00-bf00-000000000000, 00000000-0000-4500-a100-000000000000, 00000000-0000-4000-9100-000000000000)", keyspace));

        cluster.forEach(i -> i.nodetoolResult("flush", keyspace, "tbl").asserts().success());

        String select = withKeyspace("SELECT * FROM %s.tbl WHERE pk0 = -320778557 AND ck1 = 00000000-0000-4c00-a700-000000000000 AND v3 = -1669443995 AND ck0 = 00000000-0000-1300-b800-000000000000 ALLOW FILTERING", keyspace);
        Object[][] result = cluster.coordinator(2).execute(select, ConsistencyLevel.ALL);
        assertRows(result);
    }

    @Test
    public void testMissingRowWithMultipleIndexedColumns()
    {
        String keyspace = "test_missing_row";
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND replication_type='tracked'", keyspace));

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk0 timeuuid, pk1 timestamp, ck0 boolean, ck1 varint, v0 frozen<set<int>>, v1 text, v2 frozen<tuple<frozen<set<tinyint>>, frozen<list<boolean>>>>, v3 ascii, PRIMARY KEY ((pk0, pk1), ck0, ck1)) " +
                                          "WITH CLUSTERING ORDER BY (ck0 DESC, ck1 ASC) AND read_repair = 'NONE'", keyspace));

        cluster.schemaChange(withKeyspace("CREATE INDEX tbl_v2 ON %s.tbl(v2) USING 'sai'", keyspace));
        cluster.schemaChange(withKeyspace("CREATE INDEX tbl_v3 ON %s.tbl(v3) USING 'sai'", keyspace));
        SAIUtil.waitForIndexQueryable(cluster, keyspace);

        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        // Insert row on node3 at ts=11 with v2 and v3
        cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, ck1, v0, v1, v2, v3) " +
                                                    "VALUES (00000000-0000-1200-8700-000000000000, '2028-05-17T03:51:34.765Z', true, 0, {1, 2, 3}, 'test', ({13, 55}, [true]), 'original_value') " +
                                                    "USING TIMESTAMP 11", keyspace));

        cluster.get(3).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 12 " +
                                                    "WHERE pk0 = 00000000-0000-1200-8700-000000000000 AND pk1 = '2028-05-17T03:51:34.765Z' AND ck0 = false AND ck1 = 0", keyspace));

        cluster.forEach(i -> i.nodetoolResult("flush", keyspace, "tbl").asserts().success());

        cluster.get(3).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 15 SET v0 = {10, 20}, v2 = ({-13, 44}, [false]), v1 = 'updated' " +
                                                    "WHERE pk0 = 00000000-0000-1200-8700-000000000000 AND pk1 = '2028-05-17T03:51:34.765Z' AND ck0 = true AND ck1 = 0", keyspace));

        cluster.forEach(i -> i.nodetoolResult("flush", keyspace, "tbl").asserts().success());

        String select = withKeyspace("SELECT pk0, pk1, ck0, ck1, v3 FROM %s.tbl " +
                                     "WHERE pk0 = 00000000-0000-1200-8700-000000000000 AND pk1 = '2028-05-17T03:51:34.765Z' AND v2 = ({-13, 44}, [false]) AND v3 = 'original_value' " +
                                     "LIMIT 47 ALLOW FILTERING", keyspace);

        Object[][] result = cluster.coordinator(1).execute(select, ConsistencyLevel.ALL);

        assertEquals("Query should return exactly 1 row", 1, result.length);
        assertEquals("ck0 should be true", true, result[0][2]);
        assertEquals("ck1 should be 0", BigInteger.ZERO, result[0][3]);
        assertEquals("v3 should be 'original_value'", "original_value", result[0][4]);
    }

    @Test
    public void testStaticColumnUpdateWithRowQuery()
    {
        String keyspace = "test_static_column_update";
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'", keyspace));

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk0 int, pk1 text, ck0 int, s0 frozen<map<text, int>> static, v0 int, v1 text, v3 int, PRIMARY KEY ((pk0, pk1), ck0)) " +
                                          "WITH CLUSTERING ORDER BY (ck0 ASC) AND read_repair = 'NONE'", keyspace));

        cluster.schemaChange(withKeyspace("CREATE INDEX tbl_ck0 ON %s.tbl(ck0) USING 'sai'", keyspace));
        cluster.schemaChange(withKeyspace("CREATE INDEX tbl_s0 ON %s.tbl(FULL(s0)) USING 'sai'", keyspace));
        SAIUtil.waitForIndexQueryable(cluster, keyspace);
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        // Step 1: UPDATE on node2 @ ts=6 - sets s0=null and regular columns for ck0=100
        cluster.get(2).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 6 SET s0=null, v0=42, v1='value_from_ts6', v3=999 WHERE pk0 = 1 AND pk1 = 'partition1' AND ck0 = 100", keyspace));

        // Flush to ensure data is in SSTables
        cluster.forEach(i -> i.nodetoolResult("flush", keyspace, "tbl").asserts().success());

        // Step 2: INSERT on node1 @ ts=23 - updates static column s0 only
        cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, s0) VALUES (1, 'partition1', {'key1': 10, 'key2': 20, 'key3': 30}) USING TIMESTAMP 23", keyspace));
        cluster.forEach(i -> i.nodetoolResult("flush", keyspace, "tbl").asserts().success());

        String select = withKeyspace("SELECT pk0, pk1, ck0, s0, v0, v1, v3 FROM %s.tbl " +
                                     "WHERE pk0 = 1 AND pk1 = 'partition1' AND ck0 = 100 AND v0 = 42 AND v3 = 999 AND s0 = {'key1': 10, 'key2': 20, 'key3': 30} ALLOW FILTERING", keyspace);

        Object[][] result = cluster.coordinator(2).execute(select, ConsistencyLevel.ALL);
        assertRows(result, row(1, "partition1", 100, Map.of("key1", 10, "key2", 20, "key3", 30), 42, "value_from_ts6", 999));
    }

    public static String withKeyspace(String replaceIn, String keyspace)
    {
        return String.format(replaceIn, keyspace);
    }
}
