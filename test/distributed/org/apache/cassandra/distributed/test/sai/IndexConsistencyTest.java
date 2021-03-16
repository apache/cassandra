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

package org.apache.cassandra.distributed.test.sai;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.shared.Byteman;
import org.apache.cassandra.distributed.shared.Shared;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.StorageAttachedIndex;

import static org.apache.cassandra.distributed.shared.AssertUtils.fail;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;

/**
 * Tests scenarios where two replicas have different versions of the same rows.
 *
 * If the coordinator detects rows that are present in only some of the replica responses, it should ask for them by
 * primary key to those replicas where they were omitted to check if they have a more recent version that wasn't sent
 * because of not satisfying the row filter.
 *
 * See CASSANDRA-8272, CASSANDRA-8273.
 */
public class IndexConsistencyTest extends TestBaseImpl
{
    private static final int NUM_REPLICAS = 2;

    private static final String INJECTION_SCRIPT = "RULE fail indexer\n" +
                                                   "CLASS org.apache.cassandra.index.sai.StorageAttachedIndexGroup\n" +
                                                   "METHOD indexerFor\n" +
                                                   "AT ENTRY\n" +
                                                   "IF org.apache.cassandra.distributed.test.sai.IndexConsistencyTest$FailureEnabled.isEnabled(%d)\n" +
                                                   "DO\n" +
                                                   "   throw new java.lang.RuntimeException(\"Injected index failure\")\n" +
                                                   "ENDRULE\n" +
                                                   "RULE count indexer\n" +
                                                   "CLASS org.apache.cassandra.index.sai.StorageAttachedIndexGroup\n" +
                                                   "METHOD indexerFor\n" +
                                                   "AT ENTRY\n" +
                                                   "IF TRUE\n" +
                                                   "DO\n" +
                                                   "   org.apache.cassandra.distributed.test.sai.IndexConsistencyTest$Counter.increment(%d)\n" +
                                                   "ENDRULE\n";




    private static AtomicInteger seq = new AtomicInteger();
    private static String table;

    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws Exception
    {
        cluster = Cluster.build(NUM_REPLICAS)
                         .withConfig(config -> config.set("hinted_handoff_enabled", false))
                         .withInstanceInitializer((cl, nodeNumber) -> {
                             Byteman.createFromText(String.format(INJECTION_SCRIPT, nodeNumber, nodeNumber)).install(cl);
                         })
                         .start();

        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                          "{'class': 'SimpleStrategy', 'replication_factor': " + NUM_REPLICAS + "};"));
    }

    @AfterClass
    public static void closeCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Before
    public void before()
    {
        table = "t_" + seq.getAndIncrement();
    }

    @After
    public void after()
    {
        cluster.schemaChange(formatQuery("DROP TABLE IF EXISTS %s"));
        FailureEnabled.clear();
        Counter.clear();
    }

    @Test
    public void testUpdateOnSkinnyTable() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int PRIMARY KEY, v text)"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s(k, v) VALUES (0, 'old')");

        executeIsolated(1, "UPDATE %s SET v = 'new' WHERE k = 0");

        assertEmpty("SELECT * FROM %s WHERE v = 'old'");
        assertRows("SELECT * FROM %s WHERE v = 'new'", row(0, "new"));
    }

    @Test
    public void testUpdateOnWideTable() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, v text, s int STATIC, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s(k, s) VALUES (0, 9)",
                "INSERT INTO %s(k, c, v) VALUES (0, -1, 'old')",
                "INSERT INTO %s(k, c, v) VALUES (0, 0, 'old')",
                "INSERT INTO %s(k, c, v) VALUES (0, 1, 'old')");

        executeIsolated(1, "UPDATE %s SET v = 'new' WHERE k = 0 AND c = 0");

        assertRows("SELECT * FROM %s WHERE v = 'old'", row(0, -1, 9, "old"), row(0, 1, 9, "old"));
        assertRows("SELECT * FROM %s WHERE v = 'new'", row(0, 0, 9, "new"));
    }

    @Test
    public void testUpdateOnWideTableCaseInsensitive() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k1 int, k2 int, v text, v1 text, primary key(k1, k2)) with read_repair='NONE'"));
        cluster.schemaChange(formatQuery(createIndexQuery("v", false, false)));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s(k1, k2, v, v1) VALUES (0, 0, 'Old', '0')");

        executeIsolated(1, "UPDATE %s SET v = 'New' WHERE k1=0 and k2=0");

        assertEmpty("SELECT * FROM %s WHERE v = 'Old'");
        assertEmpty("SELECT * FROM %s WHERE v = 'old'");

        assertRows("SELECT * FROM %s WHERE v = 'NEW'", row(0, 0, "New", "0"));
        assertRows("SELECT * FROM %s WHERE v = 'NEW' and v1 ='0' ALLOW FILTERING", row(0, 0, "New", "0"));
        assertEmpty("SELECT * FROM %s WHERE v = 'NEW' and v1 ='1' ALLOW FILTERING");
    }

    @Test
    public void testUpdateOnWideTableNormalized() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k1 int, k2 int, v text, v1 text, primary key(k1, k2)) with read_repair='NONE'"));
        cluster.schemaChange(formatQuery(createIndexQuery("v", true, true)));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s(k1, k2, v, v1) VALUES (0, 0, '\u00E1bc', '0')"); //

        executeIsolated(1, "UPDATE %s SET v = '\u0061\u0301bc' WHERE k1=0 and k2=0");

        assertEmpty("SELECT * FROM %s WHERE v = '\u0061\u0301bc' and v1 ='1' ALLOW FILTERING");
        assertRows("SELECT * FROM %s WHERE v = '\u0061\u0301bc' and v1 ='0' ALLOW FILTERING",
                   row(0, 0, "\u0061\u0301bc", "0"));
        assertRows("SELECT * FROM %s WHERE v = '\u00E1bc'", row(0, 0, "\u0061\u0301bc", "0"));
        assertRows("SELECT * FROM %s WHERE v = '\u0061\u0301bc'", row(0, 0, "\u0061\u0301bc", "0"));
    }

    @Test
    public void testUpdateOnWideTableCaseInsensitiveNormalized() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k1 int, k2 int, v text, v1 text, primary key(k1, k2)) with read_repair='NONE'"));
        cluster.schemaChange(formatQuery(createIndexQuery("v", false, true)));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s(k1, k2, v, v1) VALUES (0, 0, '\u00E1Bc', '0')");

        executeIsolated(1, "UPDATE %s SET v = '\u0061\u0301bC' WHERE k1=0 and k2=0");

        assertEmpty("SELECT * FROM %s WHERE v = '\u0061\u0301bc' and v1 ='1' ALLOW FILTERING");
        assertRows("SELECT * FROM %s WHERE v = '\u0061\u0301bc' and v1 ='0' ALLOW FILTERING",
                   row(0, 0, "\u0061\u0301bC", "0"));
        assertRows("SELECT * FROM %s WHERE v = '\u00E1Bc'", row(0, 0, "\u0061\u0301bC", "0"));
        assertRows("SELECT * FROM %s WHERE v = '\u0061\u0301Bc'", row(0, 0, "\u0061\u0301bC", "0"));
    }

    @Test
    public void testUpdateOnStaticColumnWithEmptyPartition() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, v int, s text STATIC, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("s")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s(k, s) VALUES (0, 'old')",
                "INSERT INTO %s(k, s) VALUES (1, 'old')");

        executeIsolated(1, "UPDATE %s SET s = 'new' WHERE k = 0");

        assertRows("SELECT * FROM %s WHERE s = 'old'", row(1, null, "old", null));
        assertRows("SELECT * FROM %s WHERE s = 'new'", row(0, null, "new", null));
    }

    @Test
    public void testUpdateOnStaticColumnWithNotEmptyPartition() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, v int, s text STATIC, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("s")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s(k, s) VALUES (0, 'old')",
                "INSERT INTO %s(k, s) VALUES (1, 'old')",
                "INSERT INTO %s(k, c, v) VALUES (0, 10, 100)",
                "INSERT INTO %s(k, c, v) VALUES (0, 20, 200)",
                "INSERT INTO %s(k, c, v) VALUES (1, 30, 300)",
                "INSERT INTO %s(k, c, v) VALUES (1, 40, 400)");

        executeIsolated(1, "UPDATE %s SET s = 'new' WHERE k = 0");

        assertRows("SELECT * FROM %s WHERE s = 'old'", row(1, 30, "old", 300), row(1, 40, "old", 400));
        assertRows("SELECT * FROM %s WHERE s = 'new'", row(0, 10, "new", 100), row(0, 20, "new", 200));
    }

    @Test
    public void testComplementaryDeletionWithLimitOnPartitionKeyColumnWithEmptyPartitions() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k1 int, k2 int, c int, s int STATIC, PRIMARY KEY((k1, k2), c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("k1")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s (k1, k2, s) VALUES (0, 1, 10)",
                "INSERT INTO %s (k1, k2, s) VALUES (0, 2, 20)");

        executeIsolated(1, "DELETE FROM %s WHERE k1 = 0 AND k2 = 1");
        executeIsolated(2, "DELETE FROM %s WHERE k1 = 0 AND k2 = 2");

        assertEmpty("SELECT * FROM %s WHERE k1 = 0 LIMIT 1");
    }

    @Test
    public void testComplementaryDeletionWithLimitOnPartitionKeyColumnWithNotEmptyPartitions() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k1 int, k2 int, c int, s int STATIC, PRIMARY KEY((k1, k2), c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("k1")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s (k1, k2, c, s) VALUES (0, 1, 10, 100)",
                "INSERT INTO %s (k1, k2, c, s) VALUES (0, 2, 20, 200)");

        executeIsolated(1, "DELETE FROM %s WHERE k1 = 0 AND k2 = 1");
        executeIsolated(2, "DELETE FROM %s WHERE k1 = 0 AND k2 = 2");

        assertEmpty("SELECT * FROM %s WHERE k1 = 0 LIMIT 1");
    }

    @Test
    public void testComplementaryDeletionWithLimitOnClusteringKeyColumn() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("c")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s (k, c) VALUES (1, 0)",
                "INSERT INTO %s (k, c) VALUES (2, 0)");

        executeIsolated(1, "DELETE FROM %s WHERE k = 1");
        executeIsolated(2, "DELETE FROM %s WHERE k = 2");

        assertEmpty("SELECT * FROM %s WHERE c = 0 LIMIT 1");
    }

    @Test
    public void testComplementaryDeletionWithLimitOnStaticColumnWithEmptyPartitions() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, s int STATIC, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("s")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s (k, s) VALUES (1, 0)",
                "INSERT INTO %s (k, s) VALUES (2, 0)");

        executeIsolated(1, "DELETE FROM %s WHERE k = 1");
        executeIsolated(2, "DELETE FROM %s WHERE k = 2");

        assertEmpty("SELECT * FROM %s WHERE s = 0 LIMIT 1");
    }

    @Test
    public void testComplementaryDeletionWithLimitOnStaticColumnWithEmptyPartitionsAndRowsAfter() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, s int STATIC, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("s")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s (k, s) VALUES (1, 0)",
                "INSERT INTO %s (k, s) VALUES (2, 0)",
                "INSERT INTO %s (k, s) VALUES (3, 0)",
                "INSERT INTO %s (k, c) VALUES (3, 1)",
                "INSERT INTO %s (k, c) VALUES (3, 2)");

        executeIsolated(1, "DELETE FROM %s WHERE k = 1");
        executeIsolated(2, "DELETE FROM %s WHERE k = 2");

        assertRows("SELECT * FROM %s WHERE s = 0 LIMIT 1", row(3, 1, 0));
        assertRows("SELECT * FROM %s WHERE s = 0 LIMIT 10", row(3, 1, 0), row(3, 2, 0));
        assertRows("SELECT * FROM %s WHERE s = 0", row(3, 1, 0), row(3, 2, 0));
    }

    @Test
    public void testComplementaryDeletionWithLimitOnStaticColumnWithNotEmptyPartitions() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, s int STATIC, v int, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("s")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s (k, c, v, s) VALUES (1, 10, 100, 0)",
                "INSERT INTO %s (k, c, v, s) VALUES (2, 20, 200, 0)");

        executeIsolated(1, "DELETE FROM %s WHERE k = 1");
        executeIsolated(2, "DELETE FROM %s WHERE k = 2");

        assertEmpty("SELECT * FROM %s WHERE s = 0 LIMIT 1");
    }

    @Test
    public void testComplementaryDeletionWithLimitOnStaticColumnWithNotEmptyPartitionsAndRowsAfter() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, s int STATIC, v int, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("s")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s (k, c, v, s) VALUES (1, 10, 100, 0)",
                "INSERT INTO %s (k, c, v, s) VALUES (2, 20, 200, 0)",
                "INSERT INTO %s (k, s) VALUES (3, 0)",
                "INSERT INTO %s (k, c) VALUES (3, 1)",
                "INSERT INTO %s (k, c) VALUES (3, 2)");

        executeIsolated(1, "DELETE FROM %s WHERE k = 1");
        executeIsolated(2, "DELETE FROM %s WHERE k = 2");

        assertRows("SELECT * FROM %s WHERE s = 0 LIMIT 1", row(3, 1, 0, null));
        assertRows("SELECT * FROM %s WHERE s = 0 LIMIT 10", row(3, 1, 0, null), row(3, 2, 0, null));
        assertRows("SELECT * FROM %s WHERE s = 0", row(3, 1, 0, null), row(3, 2, 0, null));
    }

    @Test
    public void testComplementaryDeletionWithLimitOnRegularColumn() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY(k, c)) WITH speculative_retry = 'NONE'"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 0)",
                "INSERT INTO %s (k, c, v) VALUES (0, 2, 0)");

        executeIsolated(1, "DELETE FROM %s WHERE k = 0 AND c = 1");
        executeIsolated(2, "DELETE FROM %s WHERE k = 0 AND c = 2");

        assertEmpty("SELECT * FROM %s WHERE v = 0 LIMIT 1");
    }

    @Test
    public void testComplementaryDeletionWithLimitAndRowsAfter() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 0)",
                "INSERT INTO %s (k, c, v) VALUES (0, 2, 0)",
                "INSERT INTO %s (k, c, v) VALUES (0, 3, 0)");

        executeIsolated(1,
                        "DELETE FROM %s WHERE k = 0 AND c = 1",
                        "INSERT INTO %s (k, c, v) VALUES (0, 4, 0)");
        executeIsolated(2,
                        "INSERT INTO %s (k, c, v) VALUES (0, 5, 0)",
                        "DELETE FROM %s WHERE k = 0 AND c = 2");

        assertRows("SELECT * FROM %s WHERE v = 0 LIMIT 1", row(0, 3, 0));
        assertRows("SELECT * FROM %s WHERE v = 0 LIMIT 2", row(0, 3, 0), row(0, 4, 0));
        assertRows("SELECT * FROM %s WHERE v = 0 LIMIT 3", row(0, 3, 0), row(0, 4, 0), row(0, 5, 0));
        assertRows("SELECT * FROM %s WHERE v = 0 LIMIT 4", row(0, 3, 0), row(0, 4, 0), row(0, 5, 0));
    }

    @Test
    public void testComplementaryDeletionWithLimitAndRowsBetween() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 0)",
                "INSERT INTO %s (k, c, v) VALUES (0, 4, 0)");

        executeIsolated(1,
                        "DELETE FROM %s WHERE k = 0 AND c = 1");
        executeIsolated(2,
                        "INSERT INTO %s (k, c, v) VALUES (0, 2, 0)",
                        "INSERT INTO %s (k, c, v) VALUES (0, 3, 0)",
                        "DELETE FROM %s WHERE k = 0 AND c = 4");

        assertRows("SELECT * FROM %s WHERE v = 0 LIMIT 1", row(0, 2, 0));
        assertRows("SELECT * FROM %s WHERE v = 0 LIMIT 2", row(0, 2, 0), row(0, 3, 0));
        assertRows("SELECT * FROM %s WHERE v = 0 LIMIT 3", row(0, 2, 0), row(0, 3, 0));
    }

    @Test
    public void testComplementaryUpdateWithLimitOnStaticColumnWithEmptyPartitions() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, s text STATIC, v int, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("s")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s (k, s) VALUES (1, 'old')",
                "INSERT INTO %s (k, s) VALUES (2, 'old')");

        executeIsolated(1, "UPDATE %s SET s = 'new' WHERE k = 1");
        executeIsolated(2, "UPDATE %s SET s = 'new' WHERE k = 2");

        assertEmpty("SELECT * FROM %s WHERE s = 'old' LIMIT 1");
        assertRows("SELECT k, s FROM %s WHERE s = 'new' LIMIT 1", row(1, "new"));
        assertRows("SELECT k, s FROM %s WHERE s = 'new'", row(1, "new"), row(2, "new"));
    }

    @Test
    public void testComplementaryUpdateWithLimitOnStaticColumnWithNotEmptyPartitions() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, s text STATIC, v int, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("s")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s (k, c, v, s) VALUES (1, 10, 100, 'old')",
                "INSERT INTO %s (k, c, v, s) VALUES (2, 20, 200, 'old')");

        executeIsolated(1, "UPDATE %s SET s = 'new' WHERE k = 1");
        executeIsolated(2, "UPDATE %s SET s = 'new' WHERE k = 2");

        assertEmpty("SELECT * FROM %s WHERE s = 'old' LIMIT 1");
        assertRows("SELECT k, c, v, s FROM %s WHERE s = 'new' LIMIT 1", row(1, 10, 100, "new"));
        assertRows("SELECT k, c, v, s FROM %s WHERE s = 'new'",
                   row(1, 10, 100, "new"), row(2, 20, 200, "new"));
    }

    @Test
    public void testComplementaryUpdateWithLimitOnRegularColumn() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 'old')",
                "INSERT INTO %s (k, c, v) VALUES (0, 2, 'old')");

        executeIsolated(1, "UPDATE %s SET v = 'new' WHERE k = 0 AND c = 1");
        executeIsolated(2, "UPDATE %s SET v = 'new' WHERE k = 0 AND c = 2");

        assertEmpty("SELECT * FROM %s WHERE v = 'old' LIMIT 1");
        assertRows("SELECT * FROM %s WHERE v = 'new' LIMIT 1", row(0, 1, "new"));
        assertRows("SELECT * FROM %s WHERE v = 'new'", row(0, 1, "new"), row(0, 2, "new"));
    }

    @Test
    public void testComplementaryUpdateWithLimitAndRowsBetween() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 'old')",
                "INSERT INTO %s (k, c, v) VALUES (0, 4, 'old')");

        executeIsolated(1,
                        "UPDATE %s SET v = 'new' WHERE k = 0 AND c = 1");
        executeIsolated(2,
                        "INSERT INTO %s (k, c, v) VALUES (0, 2, 'old')",
                        "INSERT INTO %s (k, c, v) VALUES (0, 3, 'old')",
                        "UPDATE %s SET v = 'new' WHERE k = 0 AND c = 4");

        assertRows("SELECT * FROM %s WHERE v = 'old' LIMIT 1", row(0, 2, "old"));
        assertRows("SELECT * FROM %s WHERE v = 'old' LIMIT 2", row(0, 2, "old"), row(0, 3, "old"));
        assertRows("SELECT * FROM %s WHERE v = 'old' LIMIT 3", row(0, 2, "old"), row(0, 3, "old"));
        assertRows("SELECT * FROM %s WHERE v = 'new' LIMIT 1", row(0, 1, "new"));
        assertRows("SELECT * FROM %s WHERE v = 'new' ", row(0, 1, "new"), row(0, 4, "new"));
    }

    @Test
    public void testPartitionDeletionOnSkinnyTable() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int PRIMARY KEY, v text)"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        executeIsolated(1, "INSERT INTO %s (k, v) VALUES (0, 'old') USING TIMESTAMP 1");
        executeIsolated(2, "DELETE FROM %s WHERE k = 0");

        assertEmpty("SELECT * FROM %s WHERE v = 'old' LIMIT 1");
        assertEmpty("SELECT * FROM %s WHERE v = 'old'");
    }

    @Test
    public void testPartitionDeletionOnWideTable() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        executeIsolated(1, "INSERT INTO %s (k, c, v) VALUES (0, 1, 'old') USING TIMESTAMP 1");
        executeIsolated(2, "DELETE FROM %s WHERE k = 0");

        assertEmpty("SELECT * FROM %s WHERE v = 'old' LIMIT 1");
        assertEmpty("SELECT * FROM %s WHERE v = 'old'");
    }

    @Test
    public void testRowDeletionOnWideTable() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        executeIsolated(1, "INSERT INTO %s (k, c, v) VALUES (0, 1, 'old') USING TIMESTAMP 1");
        executeIsolated(2, "DELETE FROM %s WHERE k = 0 AND c = 1");

        assertEmpty("SELECT * FROM %s WHERE v = 'old' LIMIT 1");
        assertEmpty("SELECT * FROM %s WHERE v = 'old'");
    }

    @Test
    public void testRangeDeletionOnWideTable() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        executeIsolated(1,
                        "INSERT INTO %s (k, c, v) VALUES (0, 1, 'old') USING TIMESTAMP 1",
                        "INSERT INTO %s (k, c, v) VALUES (0, 2, 'old') USING TIMESTAMP 1",
                        "INSERT INTO %s (k, c, v) VALUES (0, 3, 'old') USING TIMESTAMP 1",
                        "INSERT INTO %s (k, c, v) VALUES (0, 4, 'old') USING TIMESTAMP 1");
        executeIsolated(2, "DELETE FROM %s WHERE k = 0 AND c > 1 AND c < 4");

        assertRows("SELECT * FROM %s WHERE v = 'old' LIMIT 1", row(0, 1, "old"));
        assertRows("SELECT * FROM %s WHERE v = 'old'", row(0, 1, "old"), row(0, 4, "old"));
    }

    @Test
    public void testMismatchingInsertionsOnSkinnyTable() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int PRIMARY KEY, v text)"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        executeIsolated(1, "INSERT INTO %s (k, v) VALUES (0, 'old') USING TIMESTAMP 1");
        executeIsolated(2, "INSERT INTO %s (k, v) VALUES (0, 'new') USING TIMESTAMP 2");

        assertEmpty("SELECT * FROM %s WHERE v = 'old' LIMIT 1");
        assertEmpty("SELECT * FROM %s WHERE v = 'old'");
        assertRows("SELECT * FROM %s WHERE v = 'new' ", row(0, "new"));
    }

    @Test
    public void testMismatchingInsertionsOnWideTable() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY(k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        executeIsolated(1, "INSERT INTO %s (k, c, v) VALUES (0, 1, 'old') USING TIMESTAMP 1");
        executeIsolated(2, "INSERT INTO %s (k, c, v) VALUES (0, 1, 'new') USING TIMESTAMP 2");

        assertEmpty("SELECT * FROM %s WHERE v = 'old' LIMIT 1");
        assertEmpty("SELECT * FROM %s WHERE v = 'old'");
        assertRows("SELECT * FROM %s WHERE v = 'new' ", row(0, 1, "new"));
    }

    @Test
    public void testConsistentSkinnyTable()
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int PRIMARY KEY, v text)"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s(k, v) VALUES (1, 'old')", // updated to 'new'
                "INSERT INTO %s(k, v) VALUES (2, 'old')",
                "INSERT INTO %s(k, v) VALUES (3, 'old')", // updated to 'new'
                "INSERT INTO %s(k, v) VALUES (4, 'old')",
                "INSERT INTO %s(k, v) VALUES (5, 'old')", // deleted partition
                "UPDATE %s SET v = 'new' WHERE k = 1",
                "UPDATE %s SET v = 'new' WHERE k = 3",
                "DELETE FROM %s WHERE k = 5");

        assertRows("SELECT * FROM %s WHERE v = 'old' LIMIT 1", row(2, "old"));
        assertRows("SELECT * FROM %s WHERE v = 'new' LIMIT 1", row(1, "new"));
        assertRows("SELECT * FROM %s WHERE v = 'old'", row(2, "old"), row(4, "old"));
        assertRows("SELECT * FROM %s WHERE v = 'new'", row(1, "new"), row(3, "new"));
    }

    @Test
    public void testConsistentWideTable()
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s(k, c, v) VALUES (0, 1, 'old')", // updated to 'new'
                "INSERT INTO %s(k, c, v) VALUES (0, 2, 'old')",
                "INSERT INTO %s(k, c, v) VALUES (0, 3, 'old')", // updated to 'new'
                "INSERT INTO %s(k, c, v) VALUES (0, 4, 'old')",
                "INSERT INTO %s(k, c, v) VALUES (0, 5, 'old')", // deleted row
                "INSERT INTO %s(k, c, v) VALUES (1, 1, 'old')", // deleted partition
                "INSERT INTO %s(k, c, v) VALUES (1, 2, 'old')", // deleted partition
                "UPDATE %s SET v = 'new' WHERE k = 0 AND c = 1",
                "UPDATE %s SET v = 'new' WHERE k = 0 AND c = 3",
                "DELETE FROM %s WHERE k = 0 AND c = 5",
                "DELETE FROM %s WHERE k = 1");

        assertRows("SELECT * FROM %s WHERE v = 'old' LIMIT 1", row(0, 2, "old"));
        assertRows("SELECT * FROM %s WHERE v = 'new' LIMIT 1", row(0, 1, "new"));
        assertRows("SELECT * FROM %s WHERE v = 'old'", row(0, 2, "old"), row(0, 4, "old"));
        assertRows("SELECT * FROM %s WHERE v = 'new'", row(0, 1, "new"), row(0, 3, "new"));
    }

    @Test
    public void testCount() throws Exception
    {
        cluster.schemaChange(formatQuery("CREATE TABLE %s (k int PRIMARY KEY, v text)"));
        cluster.schemaChange(formatQuery(createIndexQuery("v")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        execute("INSERT INTO %s(k, v) VALUES (1, 'old')",
                "INSERT INTO %s(k, v) VALUES (2, 'old')",
                "INSERT INTO %s(k, v) VALUES (3, 'old')",
                "INSERT INTO %s(k, v) VALUES (4, 'old')",
                "INSERT INTO %s(k, v) VALUES (5, 'old')");

        executeIsolated(1,
                        "UPDATE %s SET v = 'new' WHERE k = 2",
                        "UPDATE %s SET v = 'new' WHERE k = 4");

        assertRows("SELECT COUNT(*) FROM %s WHERE v = 'old' LIMIT 1", row(3L));
        assertRows("SELECT COUNT(*) FROM %s WHERE v = 'old'", row(3L));
        assertRows("SELECT COUNT(*) FROM %s WHERE v = 'new'", row(2L));
    }

    /**
     * Executes the specified CQL query with CL=ALL, so all replicas get it.
     *
     * @param query the CQL queries to be executed in all replicas
     *
     * @return the query result
     */
    private static Object[][] execute(String query)
    {
        return cluster.coordinator(1).execute(formatQuery(query), ConsistencyLevel.ALL);
    }

    /**
     * Executes the specified CQL queries with CL=ALL, so all replicas get them.
     *
     * @param queries the CQL queries to be executed in all replicas
     */
    private static void execute(String... queries)
    {
        for (String query : queries)
        {
            execute(query);
        }
    }

    /**
     * Executes the specified CQL queries only in the specified replica, with CL=ONE and the other replicas temporally
     * rejecting mutations.
     *
     * @param targetNode the index of the replica that is going to receive the queries in isolation
     * @param queries the CQL queries to be executed in a single replica
     */
    private static void executeIsolated(int targetNode, String... queries) throws Exception
    {
        try
        {
            // enable mutation failure and reset its verification counter in all the replicas of the target node
            for (int node = 1; node <= NUM_REPLICAS; node++)
            {
                if (node != targetNode)
                {
                    FailureEnabled.enable(node);
                    Counter.reset(node);
                }
            }

            // execute queries in the target node with CL=ONE
            for (String query : queries)
            {
                cluster.coordinator(targetNode).execute(formatQuery(query), ConsistencyLevel.ONE);
            }

            // verify that no mutation has been run in all the replicas of the target node
            for (int node = 1; node <= NUM_REPLICAS; node++)
            {
                if (node != targetNode)
                {
                    assertEquals(0, Counter.get(node));
                }
            }
        }
        finally
        {
            // disable mutation failure in all the replicas of the target node
            for (int node = 1; node <= NUM_REPLICAS; node++)
            {
                if (node != targetNode)
                {
                    FailureEnabled.disable(node);
                }
            }
        }
    }

    private static void assertEmpty(String query)
    {
        Object[][] result = execute(query);
        if (result != null && result.length > 0)
            fail(String.format("Expected empty result but got %d rows", result.length));
    }

    private static void assertRows(String query, Object[]... expected)
    {
        AssertUtils.assertRows(execute(query), expected);
    }

    private static String formatQuery(String query)
    {
        return String.format(query, KEYSPACE + "." + table);
    }

    private static String createIndexQuery(String column, boolean caseSensitive, boolean normalize)
    {
        String options = String.format("WITH OPTIONS = { 'case_sensitive' : %s, 'normalize' : %s };", caseSensitive, normalize);
        return String.format("CREATE CUSTOM INDEX ON %%s(%s) USING '%s' %s", column, StorageAttachedIndex.class.getName(), options);
    }

    private static String createIndexQuery(String column)
    {
        return String.format("CREATE CUSTOM INDEX ON %%s(%s) USING '%s'", column, StorageAttachedIndex.class.getName());
    }

    @Shared
    private static final class FailureEnabled
    {
        private static volatile Map<Integer, Boolean> enabled = new HashMap<>();

        public static boolean isEnabled(int node)
        {
            return enabled.containsKey(node) && enabled.get(node);
        }

        public static void enable(int node)
        {
            enabled.put(node, true);
        }

        public static void disable(int node)
        {
            enabled.put(node, false);
        }

        public static void clear()
        {
            enabled.clear();
        }
    }

    @Shared
    private static final class Counter
    {
        private static volatile ConcurrentMap<Integer, Integer> counters = new ConcurrentHashMap<>();

        public static void increment(int node)
        {
            counters.put(node, counters.getOrDefault(node, 0) + 1);
        }

        public static int get(int node)
        {
            return counters.getOrDefault(node, 0);
        }

        public static void reset(int node)
        {
            counters.put(node, 0);
        }

        public static void clear()
        {
            counters.clear();
        }
    }
}
