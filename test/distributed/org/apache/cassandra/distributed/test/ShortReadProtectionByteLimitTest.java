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
 * Demonstrates that short read protection (SRP) can silently truncate a query's results when the page is bounded
 * by bytes instead of rows.
 * <p>
 * {@link org.apache.cassandra.service.reads.ShortReadRowsProtection} decides whether a replica's partition is
 * genuinely exhausted by comparing how many rows it asked for ({@code lastQueried}) against how many rows came
 * back ({@code lastFetched}): fewer rows back than asked for is taken to mean "the replica has nothing left".
 * That is only true when the request was bounded by row count. {@code lastQueried} is computed purely from the
 * row limits ({@code DataLimits.count()} / {@code perPartitionCount()}, which default to {@code Integer.MAX_VALUE}
 * when the query has no {@code LIMIT}), while the SRP retry command
 * ({@code DataLimits.forShortReadRetry(int)}) still carries the page's byte limit. So a byte-paged retry can stop
 * on bytes long before {@code lastQueried} rows are reached, and SRP wrongly concludes the partition is fully
 * consumed - abandoning it after a single retry even though the replica has many more live rows to give.
 * <p>
 * The pager then treats that under-filled page as the last page ({@code AbstractQueryPager} sees the merged page
 * counter come up short of the page's own limits and sets {@code exhausted = true}), so no further paging state
 * is returned to the client. The query completes "successfully" with a fraction of the true result set and no
 * error, warning, or paging state hinting that anything was cut short.
 * <p>
 * See CASSANDRA-11745.
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

    /**
     * Reproduces the bug: a run of rows deleted on a minority of replicas forces short read protection to retry
     * that partition, but a small byte-sized page makes every such retry come back byte-bound rather than
     * row-bound, so SRP gives up after a single retry - even though it only recovered a couple of rows and the
     * shadowed run is longer than that. The page ends up believing the partition is exhausted when hundreds of
     * live rows are still sitting right behind it.
     * <p>
     * Node 1 is given nothing but tombstones for ck=0..5, so it runs out of real data almost immediately and
     * never needs (or gets) a retry itself. Node 2 and node 3 hold the entire, genuinely live partition and never
     * saw the delete, so it is entirely up to their SRP retries to discover that ck=0..5 were shadowed and find
     * the rest of the partition beyond that run - which is exactly the retry path a small byte budget cripples.
     */
    @Test
    public void byteLimitedShortReadProtectionSilentlyTruncatesResults()
    {
        String table = KEYSPACE + ".srp_byte_limit";
        cluster.schemaChange(format("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))", table));

        // Node 2 and node 3 hold the full, genuinely live partition. Node 1 never receives these writes.
        for (int node = 2; node <= NODES; node++)
            for (int ck = 0; ck < ROWS; ck++)
                cluster.get(node).executeInternal(format("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 0", table),
                                                  ck, VALUE);

        // Node 1 only knows about a delete of ck=0..5, and has no other data for this partition at all. At CL=ALL
        // these tombstones shadow ck=0..5 during reconciliation - node 2 and node 3 each believed the rows they
        // returned were live and counted, so the coordinator must retry them to find out otherwise and to keep
        // reading past the shadowed run. The run is deliberately longer than what a single byte-limited retry can
        // recover, so that even a genuine (but too-short) retry still leaves the page believing there is nothing
        // left, when hundreds of live rows remain right behind it.
        cluster.get(1).executeInternal(format("DELETE FROM %s USING TIMESTAMP 1 WHERE pk = 0 AND ck IN (0,1,2,3,4,5)", table));

        String query = format("SELECT ck FROM %s WHERE pk = 0", table);

        // Run the byte-paged CL=ALL query FIRST. If a CL=ALL read touched this partition beforehand, blocking
        // read repair would push the missing rows to node 1 and mask the bug we're after here.
        Iterator<Object[]> pagedIterator = cluster.coordinator(1).executeWithPagingInBytes(query, ALL, 256);
        Object[][] paged = Iterators.toArray(pagedIterator, Object[].class);

        // Reference answer: read directly from node 2's local, complete, live data - no coordination or repair.
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
