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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.collect.Iterators;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * CASSANDRA-11745: byte-limited short-read retries can return fewer rows than requested without exhausting a replica.
 * Previously, treating those responses as exhaustion truncated the result. Retries must check the byte limit too.
 */
public class ShortReadProtectionByteLimitTest extends TestBaseImpl
{
    private static final int NODES = 3;
    private static final int ROWS = 300;
    // Large enough that only one (or two) rows fit in a single byte-limited page/retry.
    private static final String VALUE = repeat('x', 200);

    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(NODES)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false))
                              .start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void byteLimitedShortReadProtectionSilentlyTruncatesResults()
    {
        String table = KEYSPACE + ".srp_byte_limit";
        cluster.schemaChange(format("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))", table));

        // Only nodes 2 and 3 receive the rows.
        for (int node = 2; node <= NODES; node++)
            for (int ck = 0; ck < ROWS; ck++)
                cluster.get(node).executeInternal(format("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 0", table),
                                                  ck, VALUE);

        // Only node 1 receives the deletion. Reconciliation shadows ck=0..5 on nodes 2 and 3,
        // forcing several byte-limited retries before a live row appears.
        cluster.get(1).executeInternal(format("DELETE FROM %s USING TIMESTAMP 1 WHERE pk = 0 AND ck IN (0,1,2,3,4,5)", table));

        String query = format("SELECT ck FROM %s WHERE pk = 0", table);

        // Read with byte paging first: an earlier coordinated read could repair the divergence and mask the regression.
        Iterator<Object[]> pagedIterator = cluster.coordinator(1).executeWithPagingInBytes(query, ALL, 256);
        Object[][] paged = Iterators.toArray(pagedIterator, Object[].class);

        // Read the repaired data locally on node 2 without further coordination or repair.
        Object[][] expected = cluster.get(2).executeInternal(query);
        assertEquals("sanity check: only ck=0..5 should be missing", ROWS - 6, expected.length);

        assertEquals("byte-paged read silently dropped rows that short read protection gave up on too early",
                     expected.length, paged.length);
    }

    @Test
    public void staticPayloadDoesNotExhaustShortReadRetries()
    {
        // HARNESS: reuse the replicas with a static value larger than each byte page.
        String table = KEYSPACE + ".srp_static_byte_limit";
        cluster.schemaChange(format("CREATE TABLE %s (pk int, ck int, s text static, v int, PRIMARY KEY (pk, ck))", table));
        for (int node = 1; node <= NODES; node++)
            cluster.get(node).executeInternal(format("INSERT INTO %s (pk, s) VALUES (0, ?) USING TIMESTAMP 0", table), VALUE);
        for (int node = 2; node <= NODES; node++)
            for (int ck = 0; ck < 12; ck++)
                cluster.get(node).executeInternal(format("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 0", table), ck, ck);

        // TRIGGER: the first six rows are shadowed, so several byte-limited retries are needed before a live row appears.
        cluster.get(1).executeInternal(format("DELETE FROM %s USING TIMESTAMP 1 WHERE pk = 0 AND ck IN (0,1,2,3,4,5)", table));
        Iterator<Object[]> results = cluster.coordinator(1).executeWithPagingInBytes(format("SELECT ck, s, v FROM %s WHERE pk = 0", table), ALL, 128);
        Object[][] actual = Iterators.toArray(results, Object[].class);

        // ORACLE: static bytes fill each retry; a short regular-row payload must not hide the six surviving rows.
        assertEquals(6, actual.length);
        for (int i = 0; i < actual.length; i++)
            assertArrayEquals(new Object[]{ i + 6, VALUE, i + 6 }, actual[i]);
    }

    private static String repeat(char c, int count)
    {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, c);
        return new String(chars);
    }
}
