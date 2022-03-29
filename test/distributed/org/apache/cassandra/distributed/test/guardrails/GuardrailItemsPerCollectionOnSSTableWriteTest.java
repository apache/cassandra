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

package org.apache.cassandra.distributed.test.guardrails;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;

/**
 * Tests the guardrail for the number of items on a collection, {@link Guardrails#itemsPerCollection}.
 * <p>
 * This test only includes the activation of the guardrail during sstable writes, all other cases are covered by
 * {@link org.apache.cassandra.db.guardrails.GuardrailItemsPerCollectionTest}.
 */
public class GuardrailItemsPerCollectionOnSSTableWriteTest extends GuardrailTester
{
    private static final int NUM_NODES = 2;

    private static final int WARN_THRESHOLD = 2;
    private static final int FAIL_THRESHOLD = 4;

    private static Cluster cluster;
    private static ICoordinator coordinator;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(NUM_NODES)
                              .withConfig(c -> c.set("items_per_collection_warn_threshold", WARN_THRESHOLD)
                                                .set("items_per_collection_fail_threshold", FAIL_THRESHOLD))
                              .start());
        cluster.disableAutoCompaction(KEYSPACE);
        coordinator = cluster.coordinator(1);
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Override
    protected Cluster getCluster()
    {
        return cluster;
    }

    @Test
    public void testSetSize() throws Throwable
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v set<int>)");

        execute("INSERT INTO %s (k, v) VALUES (0, null)");
        execute("INSERT INTO %s (k, v) VALUES (1, {1})");
        execute("INSERT INTO %s (k, v) VALUES (2, {1, 2})");
        assertNotWarnedOnFlush();

        execute("INSERT INTO %s (k, v) VALUES (3, {1, 2, 3})");
        execute("INSERT INTO %s (k, v) VALUES (4, {1, 2, 3, 4})");
        assertWarnedOnFlush(warnMessage("3", 3), warnMessage("4", 4));

        execute("INSERT INTO %s (k, v) VALUES (5, {1, 2, 3, 4, 5})");
        execute("INSERT INTO %s (k, v) VALUES (6, {1, 2, 3, 4, 5, 6})");
        assertFailedOnFlush(failMessage("5", 5), failMessage("6", 6));
    }

    @Test
    public void testSetSizeFrozen()
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v frozen<set<int>>)");

        execute("INSERT INTO %s (k, v) VALUES (3, {1, 2, 3})");
        execute("INSERT INTO %s (k, v) VALUES (5, {1, 2, 3, 4, 5})");

        // the size of frozen collections is not checked during sstable write
        assertNotWarnedOnFlush();
    }

    @Test
    public void testSetSizeWithUpdates()
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v set<int>)");

        execute("UPDATE %s SET v = v + {1, 2} WHERE k = 1");
        execute("UPDATE %s SET v = v - {1, 2} WHERE k = 2");
        assertNotWarnedOnFlush();

        execute("UPDATE %s SET v = v + {1, 2, 3} WHERE k = 3");
        execute("UPDATE %s SET v = v - {1, 2, 3} WHERE k = 4");
        assertWarnedOnFlush(warnMessage("3", 3));

        execute("UPDATE %s SET v = v + {1, 2, 3, 4, 5} WHERE k = 5");
        execute("UPDATE %s SET v = v - {1, 2, 3, 4, 5} WHERE k = 6");
        assertFailedOnFlush(failMessage("5", 5));
    }

    @Test
    public void testSetSizeAfterCompaction() throws Throwable
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v set<int>)");

        execute("INSERT INTO %s (k, v) VALUES (0, {1})");
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + {2} WHERE k = 0");
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();

        execute("INSERT INTO %s (k, v) VALUES (1, {1})");
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + {2, 3} WHERE k = 1");
        assertNotWarnedOnFlush();
        assertWarnedOnCompact(warnMessage("1", 3));

        execute("INSERT INTO %s (k, v) VALUES (2, {1, 2})");
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + {3, 4, 5} WHERE k = 2");
        assertWarnedOnFlush(warnMessage("2", 3));
        assertFailedOnCompact(failMessage("2", 5));
    }

    @Test
    public void testListSize() throws Throwable
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v list<int>)");

        execute("INSERT INTO %s (k, v) VALUES (0, null)");
        execute("INSERT INTO %s (k, v) VALUES (1, [1])");
        execute("INSERT INTO %s (k, v) VALUES (2, [1, 2])");
        assertNotWarnedOnFlush();

        execute("INSERT INTO %s (k, v) VALUES (3, [1, 2, 3])");
        execute("INSERT INTO %s (k, v) VALUES (4, [1, 2, 3, 4])");
        assertWarnedOnFlush(warnMessage("3", 3), warnMessage("4", 4));

        execute("INSERT INTO %s (k, v) VALUES (5, [1, 2, 3, 4, 5])");
        execute("INSERT INTO %s (k, v) VALUES (6, [1, 2, 3, 4, 5, 6])");
        assertFailedOnFlush(failMessage("5", 5), failMessage("6", 6));
    }

    @Test
    public void testListSizeFrozen() throws Throwable
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v frozen<list<int>>)");

        execute("INSERT INTO %s (k, v) VALUES (3, [1, 2, 3])");
        execute("INSERT INTO %s (k, v) VALUES (5, [1, 2, 3, 4, 5])");

        // the size of frozen collections is not checked during sstable write
        assertNotWarnedOnFlush();
    }

    @Test
    public void testListSizeWithUpdates()
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v list<int>)");

        execute("UPDATE %s SET v = v + [1, 2] WHERE k = 1");
        execute("UPDATE %s SET v = v - [1, 2] WHERE k = 2");
        assertNotWarnedOnFlush();

        execute("UPDATE %s SET v = v + [1, 2, 3] WHERE k = 3");
        execute("UPDATE %s SET v = v - [1, 2, 3] WHERE k = 4");
        assertWarnedOnFlush(warnMessage("3", 3));

        execute("UPDATE %s SET v = v + [1, 2, 3, 4, 5] WHERE k = 5");
        execute("UPDATE %s SET v = v - [1, 2, 3, 4, 5] WHERE k = 6");
        assertFailedOnFlush(failMessage("5", 5));
    }

    @Test
    public void testListSizeAfterCompaction() throws Throwable
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v list<int>)");

        execute("INSERT INTO %s (k, v) VALUES (0, [1])");
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + [2] WHERE k = 0");
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();

        execute("INSERT INTO %s (k, v) VALUES (1, [1])");
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + [2, 3] WHERE k = 1");
        assertNotWarnedOnFlush();
        assertWarnedOnCompact(warnMessage("1", 3));

        execute("INSERT INTO %s (k, v) VALUES (2, [1, 2])");
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + [3, 4, 5] WHERE k = 2");
        assertWarnedOnFlush(warnMessage("2", 3));
        assertFailedOnCompact(failMessage("2", 5));

        execute("INSERT INTO %s (k, v) VALUES (3, [1, 2, 3])");
        assertWarnedOnFlush(warnMessage("3", 3));
        execute("UPDATE %s SET v[1] = null WHERE k = 3");
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();
    }

    @Test
    public void testMapSize() throws Throwable
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v map<int, int>)");

        execute("INSERT INTO %s (k, v) VALUES (0, null)");
        execute("INSERT INTO %s (k, v) VALUES (1, {1:10})");
        execute("INSERT INTO %s (k, v) VALUES (2, {1:10, 2:20})");
        assertNotWarnedOnFlush();

        execute("INSERT INTO %s (k, v) VALUES (3, {1:10, 2:20, 3:30})");
        execute("INSERT INTO %s (k, v) VALUES (4, {1:10, 2:20, 3:30, 4:40})");
        assertWarnedOnFlush(warnMessage("3", 3), warnMessage("4", 4));

        execute("INSERT INTO %s (k, v) VALUES (5, {1:10, 2:20, 3:30, 4:40, 5:50})");
        execute("INSERT INTO %s (k, v) VALUES (6, {1:10, 2:20, 3:30, 4:40, 5:50, 6:60})");
        assertFailedOnFlush(failMessage("5", 5), failMessage("6", 6));
    }

    @Test
    public void testMapSizeFrozen()
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v frozen<map<int, int>>)");

        execute("INSERT INTO %s (k, v) VALUES (3, {1:10, 2:20, 3:30})");
        execute("INSERT INTO %s (k, v) VALUES (4, {1:10, 2:20, 3:30, 4:40})");

        // the size of frozen collections is not checked during sstable write
        assertNotWarnedOnFlush();
    }

    @Test
    public void testMapSizeWithUpdates()
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v map<int, int>)");

        execute("UPDATE %s SET v = v + {1:10, 2:20} WHERE k = 1");
        execute("UPDATE %s SET v = v - {1, 2} WHERE k = 2");
        assertNotWarnedOnFlush();

        execute("UPDATE %s SET v = v + {1:10, 2:20, 3:30} WHERE k = 3");
        execute("UPDATE %s SET v = v - {1, 2, 3} WHERE k = 4");
        assertWarnedOnFlush(warnMessage("3", 3));

        execute("UPDATE %s SET v = v + {1:10, 2:20, 3:30, 4:40, 5:50} WHERE k = 5");
        execute("UPDATE %s SET v = v - {1, 2, 3, 4, 5} WHERE k = 6");
        assertFailedOnFlush(failMessage("5", 5));
    }

    @Test
    public void testMapSizeAfterCompaction()
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v map<int, int>)");

        execute("INSERT INTO %s (k, v) VALUES (0, {1:10})");
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + {2:20} WHERE k = 0");
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();

        execute("INSERT INTO %s (k, v) VALUES (1, {1:10})");
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + {2:20, 3:30} WHERE k = 1");
        assertNotWarnedOnFlush();
        assertWarnedOnCompact(warnMessage("1", 3));

        execute("INSERT INTO %s (k, v) VALUES (2, {1:10, 2:20})");
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + {3:30, 4:40, 5:50} WHERE k = 2");
        assertWarnedOnFlush(warnMessage("2", 3));
        assertFailedOnCompact(failMessage("2", 5));
    }

    @Test
    public void testCompositePartitionKey()
    {
        schemaChange("CREATE TABLE %s (k1 int, k2 text, v set<int>, PRIMARY KEY((k1, k2)))");

        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 'a', {1, 2, 3})");
        assertWarnedOnFlush(warnMessage("(0, 'a')", 3));

        execute("INSERT INTO %s (k1, k2, v) VALUES (1, 'b', {1, 2, 3, 4, 5})");
        assertFailedOnFlush(failMessage("(1, 'b')", 5));
    }

    @Test
    public void testCompositeClusteringKey()
    {
        schemaChange("CREATE TABLE %s (k int, c1 int, c2 text, v set<int>, PRIMARY KEY(k, c1, c2))");

        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 10, 'a', {1, 2, 3})");
        assertWarnedOnFlush(warnMessage("(1, 10, 'a')", 3));

        execute("INSERT INTO %s (k, c1, c2, v) VALUES (2, 20, 'b', {1, 2, 3, 4, 5})");
        assertFailedOnFlush(failMessage("(2, 20, 'b')", 5));
    }

    private void execute(String query)
    {
        coordinator.execute(format(query), ConsistencyLevel.ALL);
    }

    private String warnMessage(String key, int numItems)
    {
        return String.format("Detected collection v in row %s in table %s with %d items, " +
                             "this exceeds the warning threshold of %d.",
                             key, qualifiedTableName, numItems, WARN_THRESHOLD);
    }

    private String failMessage(String key, int numItems)
    {
        return String.format("Detected collection v in row %s in table %s with %d items, " +
                             "this exceeds the failure threshold of %d.",
                             key, qualifiedTableName, numItems, FAIL_THRESHOLD);
    }
}
