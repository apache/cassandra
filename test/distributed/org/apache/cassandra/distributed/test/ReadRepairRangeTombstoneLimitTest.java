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

import com.google.common.collect.Iterators;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.Verb.READ_REQ;

/**
 * Blocking read repair must propagate a range tombstone that shadowed rows read by the query, even when the
 * coordinator stops consuming the merged partition (because of the query limit or the page size) before reaching
 * the close bound of that tombstone.
 */
public class ReadRepairRangeTombstoneLimitTest extends TestBaseImpl
{
    private static Cluster cluster;
    private static int tableNum = 0;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(3)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                          .with(Feature.GOSSIP, Feature.NETWORK))
                              .start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testLimit()
    {
        testLimit(false);
    }

    @Test
    public void testLimitReversed()
    {
        testLimit(true);
    }

    @Test
    public void testPaging()
    {
        testPaging(false);
    }

    @Test
    public void testPagingReversed()
    {
        testPaging(true);
    }

    @Test
    public void testPartitionRangeLimit()
    {
        testPartitionRangeLimit(false);
    }

    @Test
    public void testPartitionRangeLimitReversed()
    {
        testPartitionRangeLimit(true);
    }

    @Test
    public void testPerPartitionLimit()
    {
        testPerPartitionLimit(false);
    }

    @Test
    public void testPerPartitionLimitReversed()
    {
        testPerPartitionLimit(true);
    }

    @Test
    public void testQuorumLimit()
    {
        testQuorumLimit(false);
    }

    @Test
    public void testQuorumLimitReversed()
    {
        testQuorumLimit(true);
    }

    private void testPerPartitionLimit(boolean reverse)
    {
        String table = createTable(reverse);
        assertRows(cluster.coordinator(1).execute("SELECT c1, c2, v FROM " + table + " WHERE pk = 1 PER PARTITION LIMIT 1", ALL),
                   row(0, 2, 2));
        assertRepaired(table, reverse);
    }

    private void testQuorumLimit(boolean reverse)
    {
        String table = createTable(reverse);
        // a QUORUM read served by nodes 1 and 3 must repair node 1
        cluster.filters().verbs(READ_REQ.id).to(2).drop();
        try
        {
            assertRows(cluster.coordinator(1).execute("SELECT c1, c2, v FROM " + table + " WHERE pk = 1 LIMIT 1", QUORUM),
                       row(0, 2, 2));
        }
        finally
        {
            cluster.filters().reset();
        }
        assertRows(cluster.get(1).executeInternal("SELECT c1, c2, v FROM " + table + " WHERE pk = 1"),
                   row(0, 2, 2), sentinel(reverse));
    }

    private void testLimit(boolean reverse)
    {
        String table = createTable(reverse);
        assertRows(cluster.coordinator(1).execute("SELECT c1, c2, v FROM " + table + " WHERE pk = 1 LIMIT 1", ALL),
                   row(0, 2, 2));
        assertRepaired(table, reverse);
    }

    private void testPaging(boolean reverse)
    {
        String table = createTable(reverse);
        Object[][] rows = Iterators.toArray(cluster.coordinator(1).executeWithPaging("SELECT c1, c2, v FROM " + table + " WHERE pk = 1", ALL, 1),
                                            Object[].class);
        assertRows(rows, row(0, 2, 2), sentinel(reverse));
        assertRepaired(table, reverse);
    }

    private void testPartitionRangeLimit(boolean reverse)
    {
        String table = createTable(reverse);
        assertRows(cluster.coordinator(1).execute("SELECT c1, c2, v FROM " + table + " LIMIT 1", ALL),
                   row(0, 2, 2));
        assertRepaired(table, reverse);
    }

    /**
     * All nodes have rows (0, 1) and (0, 2), and a sentinel row outside the c1 = 0 prefix, after it in query order.
     * Only node 3 has the deletion of the c1 = 0 prefix, which shadows (0, 1) but neither (0, 2) nor the sentinel.
     */
    private static String createTable(boolean reverse)
    {
        String table = withKeyspace("%s.t" + tableNum++);
        cluster.schemaChange("CREATE TABLE " + table + " (pk int, c1 int, c2 int, v int, PRIMARY KEY (pk, c1, c2)) " +
                             "WITH CLUSTERING ORDER BY (c1 " + (reverse ? "DESC" : "ASC") + ", c2 ASC) " +
                             "AND read_repair = 'BLOCKING' AND speculative_retry = 'ALWAYS'");

        Object[] sentinel = sentinel(reverse);
        for (int node = 1; node <= 3; node++)
        {
            cluster.get(node).executeInternal("INSERT INTO " + table + " (pk, c1, c2, v) VALUES (1, 0, 1, 1) USING TIMESTAMP 10");
            cluster.get(node).executeInternal("INSERT INTO " + table + " (pk, c1, c2, v) VALUES (1, 0, 2, 2) USING TIMESTAMP 30");
            cluster.get(node).executeInternal("INSERT INTO " + table + " (pk, c1, c2, v) VALUES (1, ?, ?, ?) USING TIMESTAMP 10", sentinel);
        }
        cluster.get(3).executeInternal("DELETE FROM " + table + " USING TIMESTAMP 20 WHERE pk = 1 AND c1 = 0");
        return table;
    }

    private static Object[] sentinel(boolean reverse)
    {
        return reverse ? row(-1, 0, 3) : row(1, 0, 3);
    }

    private static void assertRepaired(String table, boolean reverse)
    {
        // nodes 1 and 2 must have received the deletion of (0, 1), and nothing beyond the rows read
        for (int node = 1; node <= 2; node++)
            assertRows(cluster.get(node).executeInternal("SELECT c1, c2, v FROM " + table + " WHERE pk = 1"),
                       row(0, 2, 2), sentinel(reverse));

        // so a QUORUM read that doesn't reach node 3 must not bring back (0, 1)
        cluster.filters().verbs(READ_REQ.id).to(3).drop();
        try
        {
            assertRows(cluster.coordinator(1).execute("SELECT c1, c2, v FROM " + table + " WHERE pk = 1", QUORUM),
                       row(0, 2, 2), sentinel(reverse));
        }
        finally
        {
            cluster.filters().reset();
        }
    }
}
