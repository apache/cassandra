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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;

import static java.nio.ByteBuffer.allocate;

/**
 * Tests the guardrail for the size of collections, {@link Guardrails#collectionSize}.
 * <p>
 * This test only includes the activation of the guardrail during sstable writes, all other cases are covered by
 * {@link org.apache.cassandra.db.guardrails.GuardrailCollectionSizeTest}.
 */
public class GuardrailCollectionSizeOnSSTableWriteTest extends GuardrailTester
{
    private static final int NUM_NODES = 2;

    private static final int WARN_THRESHOLD = 1024;
    private static final int FAIL_THRESHOLD = WARN_THRESHOLD * 4;

    private static Cluster cluster;
    private static com.datastax.driver.core.Cluster driverCluster;
    private static Session driverSession;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(NUM_NODES)
                              .withConfig(c -> c.with(Feature.GOSSIP, Feature.NATIVE_PROTOCOL)
                                                .set("collection_size_warn_threshold", WARN_THRESHOLD + "B")
                                                .set("collection_size_fail_threshold", FAIL_THRESHOLD + "B"))
                              .start());
        cluster.disableAutoCompaction(KEYSPACE);
        driverCluster = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
        driverSession = driverCluster.connect();
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (driverSession != null)
            driverSession.close();

        if (driverCluster != null)
            driverCluster.close();

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
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v set<blob>)");

        execute("INSERT INTO %s (k, v) VALUES (0, null)");
        execute("INSERT INTO %s (k, v) VALUES (1, ?)", set());
        execute("INSERT INTO %s (k, v) VALUES (2, ?)", set(allocate(1)));
        execute("INSERT INTO %s (k, v) VALUES (3, ?)", set(allocate(WARN_THRESHOLD / 2)));
        assertNotWarnedOnFlush();

        execute("INSERT INTO %s (k, v) VALUES (4, ?)", set(allocate(WARN_THRESHOLD)));
        assertWarnedOnFlush(warnMessage("4"));

        execute("INSERT INTO %s (k, v) VALUES (5, ?)", set(allocate(WARN_THRESHOLD / 4), allocate(WARN_THRESHOLD * 3 / 4)));
        assertWarnedOnFlush(warnMessage("5"));

        execute("INSERT INTO %s (k, v) VALUES (6, ?)", set(allocate(FAIL_THRESHOLD)));
        assertFailedOnFlush(failMessage("6"));

        execute("INSERT INTO %s (k, v) VALUES (7, ?)", set(allocate(FAIL_THRESHOLD / 4), allocate(FAIL_THRESHOLD * 3 / 4)));
        assertFailedOnFlush(failMessage("7"));
    }

    @Test
    public void testSetSizeFrozen()
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v frozen<set<blob>>)");

        execute("INSERT INTO %s (k, v) VALUES (0, null)");
        execute("INSERT INTO %s (k, v) VALUES (1, ?)", set());
        execute("INSERT INTO %s (k, v) VALUES (2, ?)", set(allocate(1)));
        execute("INSERT INTO %s (k, v) VALUES (4, ?)", set(allocate(WARN_THRESHOLD)));
        execute("INSERT INTO %s (k, v) VALUES (5, ?)", set(allocate(FAIL_THRESHOLD)));

        // frozen collections size is not checked during sstable write
        assertNotWarnedOnFlush();
    }

    @Test
    public void testSetSizeWithUpdates()
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v set<blob>)");

        execute("INSERT INTO %s (k, v) VALUES (0, ?)", set(allocate(1)));
        execute("UPDATE %s SET v = v + ? WHERE k = 0", set(allocate(1)));
        assertNotWarnedOnFlush();

        execute("INSERT INTO %s (k, v) VALUES (1, ?)", set(allocate(WARN_THRESHOLD / 4)));
        execute("UPDATE %s SET v = v + ? WHERE k = 1", set(allocate(WARN_THRESHOLD * 3 / 4)));
        assertWarnedOnFlush(warnMessage("1"));

        execute("INSERT INTO %s (k, v) VALUES (2, ?)", set(allocate(FAIL_THRESHOLD / 4)));
        execute("UPDATE %s SET v = v + ? WHERE k = 2", set(allocate(FAIL_THRESHOLD * 3 / 4)));
        assertFailedOnFlush(failMessage("2"));

        execute("INSERT INTO %s (k, v) VALUES (4, ?)", set(allocate(FAIL_THRESHOLD)));
        execute("UPDATE %s SET v = v - ? WHERE k = 4", set(allocate(FAIL_THRESHOLD)));
        assertNotWarnedOnFlush();
    }

    @Test
    public void testSetSizeAfterCompaction() throws Throwable
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v set<blob>)");

        execute("INSERT INTO %s (k, v) VALUES (0, ?)", set(allocate(1)));
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + ? WHERE k = 0", set(allocate(1)));
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();

        execute("INSERT INTO %s (k, v) VALUES (1, ?)", set(allocate(WARN_THRESHOLD * 3 / 4)));
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + ? WHERE k = 1", set(allocate(WARN_THRESHOLD / 4)));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact(warnMessage("1"));

        execute("INSERT INTO %s (k, v) VALUES (2, ?)", set(allocate(FAIL_THRESHOLD * 3 / 4)));
        assertWarnedOnFlush(warnMessage("2"));
        execute("UPDATE %s SET v = v + ? WHERE k = 2", set(allocate(FAIL_THRESHOLD / 4)));
        assertWarnedOnFlush(warnMessage("2"));
        assertFailedOnCompact(failMessage("2"));

        execute("INSERT INTO %s (k, v) VALUES (3, ?)", set(allocate(FAIL_THRESHOLD)));
        assertFailedOnFlush(failMessage("3"));
        execute("UPDATE %s SET v = v - ? WHERE k = 3", set(allocate(FAIL_THRESHOLD)));
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();
    }

    @Test
    public void testListSize() throws Throwable
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v list<blob>)");

        execute("INSERT INTO %s (k, v) VALUES (0, null)");
        execute("INSERT INTO %s (k, v) VALUES (1, ?)", list());
        execute("INSERT INTO %s (k, v) VALUES (2, ?)", list(allocate(1)));
        execute("INSERT INTO %s (k, v) VALUES (3, ?)", list(allocate(WARN_THRESHOLD / 2)));
        assertNotWarnedOnFlush();

        execute("INSERT INTO %s (k, v) VALUES (4, ?)", list(allocate(WARN_THRESHOLD)));
        assertWarnedOnFlush(warnMessage("4"));

        execute("INSERT INTO %s (k, v) VALUES (5, ?)", list(allocate(WARN_THRESHOLD / 2), allocate(WARN_THRESHOLD / 2)));
        assertWarnedOnFlush(warnMessage("5"));

        execute("INSERT INTO %s (k, v) VALUES (6, ?)", list(allocate(FAIL_THRESHOLD)));
        assertFailedOnFlush(failMessage("6"));

        execute("INSERT INTO %s (k, v) VALUES (7, ?)", list(allocate(FAIL_THRESHOLD / 2), allocate(FAIL_THRESHOLD / 2)));
        assertFailedOnFlush(failMessage("7"));
    }

    @Test
    public void testListSizeFrozen()
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v frozen<list<blob>>)");

        execute("INSERT INTO %s (k, v) VALUES (0, null)");
        execute("INSERT INTO %s (k, v) VALUES (1, ?)", list());
        execute("INSERT INTO %s (k, v) VALUES (2, ?)", list(allocate(1)));
        execute("INSERT INTO %s (k, v) VALUES (4, ?)", list(allocate(WARN_THRESHOLD)));
        execute("INSERT INTO %s (k, v) VALUES (5, ?)", list(allocate(FAIL_THRESHOLD)));

        // frozen collections size is not checked during sstable write
        assertNotWarnedOnFlush();
    }

    @Test
    public void testListSizeWithUpdates()
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v list<blob>)");

        execute("INSERT INTO %s (k, v) VALUES (0, ?)", list(allocate(1)));
        execute("UPDATE %s SET v = v + ? WHERE k = 0", list(allocate(1)));
        assertNotWarnedOnFlush();

        execute("INSERT INTO %s (k, v) VALUES (1, ?)", list(allocate(WARN_THRESHOLD / 2)));
        execute("UPDATE %s SET v = v + ? WHERE k = 1", list(allocate(WARN_THRESHOLD / 2)));
        assertWarnedOnFlush(warnMessage("1"));

        execute("INSERT INTO %s (k, v) VALUES (2, ?)", list(allocate(FAIL_THRESHOLD / 2)));
        execute("UPDATE %s SET v = v + ? WHERE k = 2", list(allocate(FAIL_THRESHOLD / 2)));
        assertFailedOnFlush(failMessage("2"));

        execute("INSERT INTO %s (k, v) VALUES (4, ?)", list(allocate(FAIL_THRESHOLD)));
        execute("UPDATE %s SET v = v - ? WHERE k = 4", list(allocate(FAIL_THRESHOLD)));
        assertNotWarnedOnFlush();
    }

    @Test
    public void testListSizeAfterCompaction() throws Throwable
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v list<blob>)");

        execute("INSERT INTO %s (k, v) VALUES (0, ?)", list(allocate(1)));
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + ? WHERE k = 0", list(allocate(1)));
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();

        execute("INSERT INTO %s (k, v) VALUES (1, ?)", list(allocate(WARN_THRESHOLD / 2)));
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + ? WHERE k = 1", list(allocate(WARN_THRESHOLD / 2)));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact(warnMessage("1"));

        execute("INSERT INTO %s (k, v) VALUES (2, ?)", list(allocate(FAIL_THRESHOLD / 2)));
        assertWarnedOnFlush(warnMessage("2"));
        execute("UPDATE %s SET v = v + ? WHERE k = 2", list(allocate(FAIL_THRESHOLD / 2)));
        assertNotWarnedOnFlush();
        assertFailedOnCompact(failMessage("2"));

        execute("INSERT INTO %s (k, v) VALUES (3, ?)", list(allocate(FAIL_THRESHOLD)));
        assertFailedOnFlush(failMessage("3"));
        execute("UPDATE %s SET v[0] = ? WHERE k = 3", allocate(1));
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();
    }

    @Test
    public void testMapSize() throws Throwable
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v map<blob, blob>)");

        execute("INSERT INTO %s (k, v) VALUES (0, null)");
        execute("INSERT INTO %s (k, v) VALUES (1, ?)", map());
        execute("INSERT INTO %s (k, v) VALUES (2, ?)", map(allocate(1), allocate(1)));
        execute("INSERT INTO %s (k, v) VALUES (3, ?)", map(allocate(1), allocate(WARN_THRESHOLD / 2)));
        execute("INSERT INTO %s (k, v) VALUES (4, ?)", map(allocate(WARN_THRESHOLD / 2), allocate(1)));
        assertNotWarnedOnFlush();

        execute("INSERT INTO %s (k, v) VALUES (5, ?)", map(allocate(WARN_THRESHOLD), allocate(1)));
        assertWarnedOnFlush(warnMessage("5"));

        execute("INSERT INTO %s (k, v) VALUES (6, ?)", map(allocate(1), allocate(WARN_THRESHOLD)));
        assertWarnedOnFlush(warnMessage("6"));

        execute("INSERT INTO %s (k, v) VALUES (7, ?)", map(allocate(WARN_THRESHOLD), allocate(WARN_THRESHOLD)));
        assertWarnedOnFlush(warnMessage("7"));

        execute("INSERT INTO %s (k, v) VALUES (8, ?)", map(allocate(FAIL_THRESHOLD), allocate(1)));
        assertFailedOnFlush(failMessage("8"));

        execute("INSERT INTO %s (k, v) VALUES (9, ?)", map(allocate(1), allocate(FAIL_THRESHOLD)));
        assertFailedOnFlush(failMessage("9"));

        execute("INSERT INTO %s (k, v) VALUES (10, ?)", map(allocate(FAIL_THRESHOLD), allocate(FAIL_THRESHOLD)));
        assertFailedOnFlush(failMessage("10"));
    }

    @Test
    public void testMapSizeFrozen()
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v frozen<map<blob, blob>>)");

        execute("INSERT INTO %s (k, v) VALUES (0, null)");
        execute("INSERT INTO %s (k, v) VALUES (1, ?)", map());
        execute("INSERT INTO %s (k, v) VALUES (2, ?)", map(allocate(1), allocate(1)));
        execute("INSERT INTO %s (k, v) VALUES (4, ?)", map(allocate(1), allocate(WARN_THRESHOLD)));
        execute("INSERT INTO %s (k, v) VALUES (5, ?)", map(allocate(WARN_THRESHOLD), allocate(1)));
        execute("INSERT INTO %s (k, v) VALUES (6, ?)", map(allocate(WARN_THRESHOLD), allocate(WARN_THRESHOLD)));
        execute("INSERT INTO %s (k, v) VALUES (7, ?)", map(allocate(1), allocate(FAIL_THRESHOLD)));
        execute("INSERT INTO %s (k, v) VALUES (8, ?)", map(allocate(FAIL_THRESHOLD), allocate(1)));
        execute("INSERT INTO %s (k, v) VALUES (9, ?)", map(allocate(FAIL_THRESHOLD), allocate(FAIL_THRESHOLD)));

        // frozen collections size is not checked during sstable write
        assertNotWarnedOnFlush();
    }

    @Test
    public void testMapSizeWithUpdates()
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v map<blob, blob>)");

        execute("INSERT INTO %s (k, v) VALUES (0, ?)", map(allocate(1), allocate(1)));
        execute("UPDATE %s SET v = v + ? WHERE k = 0", map(allocate(1), allocate(1)));
        assertNotWarnedOnFlush();

        execute("INSERT INTO %s (k, v) VALUES (1, ?)", map(allocate(1), allocate(WARN_THRESHOLD / 2)));
        execute("UPDATE %s SET v = v + ? WHERE k = 1", map(allocate(2), allocate(WARN_THRESHOLD / 2)));
        assertWarnedOnFlush(warnMessage("1"));

        execute("INSERT INTO %s (k, v) VALUES (2, ?)", map(allocate(WARN_THRESHOLD / 4), allocate(1)));
        execute("UPDATE %s SET v = v + ? WHERE k = 2", map(allocate(WARN_THRESHOLD * 3 / 4), allocate(1)));
        assertWarnedOnFlush(warnMessage("2"));

        execute("INSERT INTO %s (k, v) VALUES (3, ?)", map(allocate(WARN_THRESHOLD / 4), allocate(WARN_THRESHOLD / 4)));
        execute("UPDATE %s SET v = v + ? WHERE k = 3", map(allocate(WARN_THRESHOLD / 4 + 1), allocate(WARN_THRESHOLD / 4)));
        assertWarnedOnFlush(warnMessage("3"));

        execute("INSERT INTO %s (k, v) VALUES (4, ?)", map(allocate(1), allocate(FAIL_THRESHOLD / 2)));
        execute("UPDATE %s SET v = v + ? WHERE k = 4", map(allocate(2), allocate(FAIL_THRESHOLD / 2)));
        assertFailedOnFlush(failMessage("4"));

        execute("INSERT INTO %s (k, v) VALUES (5, ?)", map(allocate(FAIL_THRESHOLD / 4), allocate(1)));
        execute("UPDATE %s SET v = v + ? WHERE k = 5", map(allocate(FAIL_THRESHOLD * 3 / 4), allocate(1)));
        assertFailedOnFlush(failMessage("5"));

        execute("INSERT INTO %s (k, v) VALUES (6, ?)", map(allocate(FAIL_THRESHOLD / 4), allocate(FAIL_THRESHOLD / 4)));
        execute("UPDATE %s SET v = v + ? WHERE k = 6", map(allocate(FAIL_THRESHOLD / 4 + 1), allocate(FAIL_THRESHOLD / 4)));
        assertFailedOnFlush(failMessage("6"));
    }

    @Test
    public void testMapSizeAfterCompaction()
    {
        schemaChange("CREATE TABLE %s (k int PRIMARY KEY, v map<blob, blob>)");

        execute("INSERT INTO %s (k, v) VALUES (0, ?)", map(allocate(1), allocate(1)));
        execute("UPDATE %s SET v = v + ? WHERE k = 0", map(allocate(1), allocate(1)));
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();

        execute("INSERT INTO %s (k, v) VALUES (1, ?)", map(allocate(1), allocate(WARN_THRESHOLD / 2)));
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + ? WHERE k = 1", map(allocate(2), allocate(WARN_THRESHOLD / 2)));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact(warnMessage("1"));

        execute("INSERT INTO %s (k, v) VALUES (2, ?)", map(allocate(WARN_THRESHOLD / 4), allocate(1)));
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + ? WHERE k = 2", map(allocate(WARN_THRESHOLD * 3 / 4), allocate(1)));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact(warnMessage("2"));

        execute("INSERT INTO %s (k, v) VALUES (3, ?)", map(allocate(WARN_THRESHOLD / 4), allocate(WARN_THRESHOLD / 4)));
        assertNotWarnedOnFlush();
        execute("UPDATE %s SET v = v + ? WHERE k = 3", map(allocate(WARN_THRESHOLD / 4 + 1), allocate(WARN_THRESHOLD / 4)));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact(warnMessage("3"));

        execute("INSERT INTO %s (k, v) VALUES (4, ?)", map(allocate(1), allocate(FAIL_THRESHOLD / 2)));
        assertWarnedOnFlush(failMessage("4"));
        execute("UPDATE %s SET v = v + ? WHERE k = 4", map(allocate(2), allocate(FAIL_THRESHOLD / 2)));
        assertWarnedOnFlush(warnMessage("4"));
        assertFailedOnCompact(failMessage("4"));

        execute("INSERT INTO %s (k, v) VALUES (5, ?)", map(allocate(FAIL_THRESHOLD / 4), allocate(1)));
        assertWarnedOnFlush(failMessage("5"));
        execute("UPDATE %s SET v = v + ? WHERE k = 5", map(allocate(FAIL_THRESHOLD * 3 / 4), allocate(1)));
        assertWarnedOnFlush(warnMessage("5"));
        assertFailedOnCompact(failMessage("5"));

        execute("INSERT INTO %s (k, v) VALUES (6, ?)", map(allocate(FAIL_THRESHOLD / 4), allocate(FAIL_THRESHOLD / 4)));
        assertWarnedOnFlush(failMessage("6"));
        execute("UPDATE %s SET v = v + ? WHERE k = 6", map(allocate(FAIL_THRESHOLD / 4 + 1), allocate(FAIL_THRESHOLD / 4)));
        assertWarnedOnFlush(warnMessage("6"));
        assertFailedOnCompact(failMessage("6"));
    }

    @Test
    public void testCompositePartitionKey()
    {
        schemaChange("CREATE TABLE %s (k1 int, k2 text, v set<blob>, PRIMARY KEY((k1, k2)))");

        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 'a', ?)", set(allocate(WARN_THRESHOLD)));
        assertWarnedOnFlush(warnMessage("(0, 'a')"));

        execute("INSERT INTO %s (k1, k2, v) VALUES (1, 'b', ?)", set(allocate(FAIL_THRESHOLD)));
        assertFailedOnFlush(failMessage("(1, 'b')"));
    }

    @Test
    public void testCompositeClusteringKey()
    {
        schemaChange("CREATE TABLE %s (k int, c1 int, c2 text, v set<blob>, PRIMARY KEY(k, c1, c2))");

        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 10, 'a', ?)", set(allocate(WARN_THRESHOLD)));
        assertWarnedOnFlush(warnMessage("(1, 10, 'a')"));

        execute("INSERT INTO %s (k, c1, c2, v) VALUES (2, 20, 'b', ?)", set(allocate(FAIL_THRESHOLD)));
        assertFailedOnFlush(failMessage("(2, 20, 'b')"));
    }

    private void execute(String query, Object... args)
    {
        SimpleStatement stmt = new SimpleStatement(format(query), args);
        stmt.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL);
        driverSession.execute(stmt);
    }

    private String warnMessage(String key)
    {
        return String.format("Detected collection v in row %s in table %s of size", key, qualifiedTableName);
    }

    private String failMessage(String key)
    {
        return String.format("Detected collection v in row %s in table %s of size", key, qualifiedTableName);
    }
}
