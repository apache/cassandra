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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertEquals;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.service.reads.repair.ReadRepairStrategy.NONE;

/**
 * Tests basic read repair functionality with different types of query types and schemas.
 * <p>
 * Each test verifies that its tested query triggers read repair propagating mismatched rows/columns and row/column
 * deletions. They also verify that the selected rows and columns are propagated through read repair on missmatch,
 * and that unselected rows/columns are not repaired.
 * <p>
 * The tests are parameterized for:
 * <ul>
 *     <li><Both {@code NONE} and {@code BLOCKING} read repair stratregies/li>
 *     <li>Data to be repaired residing on the query coordinator or a replica</li>
 *     <li>Data to be repaired residing on memtables or flushed to sstables</li>
 * </ul>
 * <p>
 * All the included tests have a similar behaviour:
 * <ul>
 *     <li>Create a keyspace with RF=2 and a table</li>
 *     <li>Insert some data in only one of the nodes</li>
 *     <li>Run the tested read query selecting a subset of the inserted columns with CL=ALL</li>
 *     <li>Verify that the previous read has triggered read repair propagating only the queried columns</li>
 *     <li>Run the tested read query again but this time selecting all the columns</li>
 *     <li>Verify that the previous read has triggered read repair propagating the rest of the queried rows</li>
 *     <li>Delete one of the involved columns in just one node</li>
 *     <li>Run the tested read query again but this time selecting a column different to the deleted one</li>
 *     <li>Verify that the previous read hasn't propagated the column deletion</li>
 *     <li>Run the tested read query again selecting all the columns</li>
 *     <li>Verify that the previous read has triggered read repair propagating the column deletion</li>
 *     <li>Delete one of the involved rows in just one node</li>
 *     <li>Run the tested read query again selecting all the columns</li>
 *     <li>Verify that the previous read has triggered read repair propagating the row deletions</li>
 *     <li>Verify the final status of each node and drop the table</li>
 * </ul>
 */
@RunWith(Parameterized.class)
public class ReadRepairQueryTypesTest extends TestBaseImpl
{
    private static final int NUM_NODES = 2;

    /**
     * The read repair strategy to be used
     */
    @Parameterized.Parameter
    public ReadRepairStrategy strategy;

    /**
     * The node to be used as coordinator
     */
    @Parameterized.Parameter(1)
    public int coordinator;

    /**
     * Whether to flush data after mutations
     */
    @Parameterized.Parameter(2)
    public boolean flush;

    /**
     * Whether paging is used for the distributed queries
     */
    @Parameterized.Parameter(3)
    public boolean paging;

    @Parameterized.Parameters(name = "{index}: strategy={0} coordinator={1} flush={2} paging={3}")
    public static Collection<Object[]> data()
    {
        List<Object[]> result = new ArrayList<>();
        for (int coordinator = 1; coordinator <= NUM_NODES; coordinator++)
            for (boolean flush : BOOLEANS)
                for (boolean paging : BOOLEANS)
                    result.add(new Object[]{ ReadRepairStrategy.BLOCKING, coordinator, flush, paging });
        result.add(new Object[]{ ReadRepairStrategy.NONE, 1, false, false });
        return result;
    }

    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(NUM_NODES)
                              .withConfig(config -> config.set("read_request_timeout_in_ms", MINUTES.toMillis(1))
                                                          .set("write_request_timeout_in_ms", MINUTES.toMillis(1)))
                              .start());
        cluster.schemaChange(withKeyspace("CREATE TYPE %s.udt (x int, y int)"));
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    /**
     * Test queries without restrictions on a table without clustering columns.
     */
    @Test
    public void testUnrestrictedQueryOnSkinnyTable()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (1, 10, 100)",
                "INSERT INTO %s (k, a, b) VALUES (2, 20, 200)")
        .queryColumns("a", 2, 2,
                      rows(row(10), row(20)),
                      rows(row(1, 10, 100), row(2, 20, 200)),
                      rows(row(1, 10, null), row(2, 20, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=1", "b", 0, 1,
                      rows(row(100), row(200)),
                      rows(row(1, null, 100), row(2, 20, 200)),
                      rows(row(1, 10, 100), row(2, 20, 200)))
        .deleteRows("DELETE FROM %s WHERE k=1", 1,
                    rows(row(2, 20, 200)),
                    rows(row(1, null, 100), row(2, 20, 200)))
        .tearDown(0,
                  rows(rows(row(2, 20, 200))),
                  rows(rows(row(2, 20, 200))));
    }

    /**
     * Test queries without restrictions on a table with clustering columns.
     */
    @Test
    public void testUnrestrictedQueryOnWideTable()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (1, 10, 100, 1000)",
                "INSERT INTO %s (k, c, a, b) VALUES (1, 20, 200, 2000)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 30, 300, 3000)")
        .queryColumns("a", paging ? 3 : 2, paging ? 3 : 2,
                      rows(row(100), row(200), row(300)),
                      rows(row(1, 10, 100, 1000), row(1, 20, 200, 2000), row(2, 30, 300, 3000)),
                      rows(row(1, 10, 100, null), row(1, 20, 200, null), row(2, 30, 300, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=1 AND c=20", "b", 0, 1,
                      rows(row(1000), row(2000), row(3000)),
                      rows(row(1, 10, 100, 1000), row(1, 20, null, 2000), row(2, 30, 300, 3000)),
                      rows(row(1, 10, 100, 1000), row(1, 20, 200, 2000), row(2, 30, 300, 3000)))
        .deleteRows("DELETE FROM %s WHERE k=1 AND c=20", 1,
                    rows(row(1, 10, 100, 1000), row(2, 30, 300, 3000)),
                    rows(row(1, 10, 100, 1000), row(1, 20, null, 2000), row(2, 30, 300, 3000)))
        .deleteRows("DELETE FROM %s WHERE k=1", 1,
                    rows(row(2, 30, 300, 3000)),
                    rows(row(1, 10, 100, 1000), row(2, 30, 300, 3000)))
        .tearDown(0,
                  rows(row(2, 30, 300, 3000)),
                  rows(row(2, 30, 300, 3000)));
    }

    /**
     * Test range queries on a table with static columns.
     */
    @Test
    public void testUnrestrictedQueryWithStaticColumns()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int, s1 int static, s2 int static, c int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, s1, s2) VALUES (1, 10, 100)",
                "INSERT INTO %s (k, c) VALUES (1, 1)",
                "INSERT INTO %s (k, c) VALUES (1, 2)",
                "INSERT INTO %s (k, s1, s2) VALUES (2, 10, 100)")
        .queryColumns("s1", paging ? 3 : 2, 2,
                      rows(row(10), row(10), row(10)),
                      rows(row(1, 1, 10, 100), row(1, 2, 10, 100), row(2, null, 10, 100)),
                      rows(row(1, 1, 10, null), row(1, 2, 10, null), row(2, null, 10, null)))
        .deleteColumn("DELETE s1 FROM %s WHERE k=1", "s2", 0, 1,
                      rows(row(100), row(100), row(100)),
                      rows(row(1, 1, null, 100), row(1, 2, null, 100), row(2, null, 10, 100)),
                      rows(row(1, 1, 10, 100), row(1, 2, 10, 100), row(2, null, 10, 100)))
        .deleteRows("DELETE FROM %s WHERE k=1 AND c=1", 1,
                    rows(row(1, 2, null, 100), row(2, null, 10, 100)),
                    rows(row(1, 1, null, 100), row(1, 2, null, 100), row(2, null, 10, 100)))
        .deleteRows("DELETE FROM %s WHERE k=1", 1,
                    rows(row(2, null, 10, 100)),
                    rows(row(1, 2, null, 100), row(2, null, 10, 100)))
        .tearDown(0,
                  rows(row(2, null, 10, 100)),
                  rows(row(2, null, 10, 100)));
    }

    /**
     * Test queries selecting a specific row from a table without clustering columns.
     */
    @Test
    public void testPointQueryOnSkinnyTable()
    {
        tester("WHERE k=1")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (1, 2, 3)")
        .queryColumns("a", 1, 1,
                      rows(row(2)),
                      rows(row(1, 2, 3)),
                      rows(row(1, 2, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=1", "b", 0, 1,
                      rows(row(3)),
                      rows(row(1, null, 3)),
                      rows(row(1, 2, 3)))
        .deleteRows("DELETE FROM %s WHERE k=1", 1,
                    rows(),
                    rows(row(1, null, 3)))
        .tearDown();
    }

    /**
     * Test queries selecting a specific row from a table with clustering columns.
     */
    @Test
    public void testPointQueryOnWideTable()
    {
        tester("WHERE k=0 AND c=2")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (0, 1, 10, 100)",
                "INSERT INTO %s (k, c, a, b) VALUES (0, 2, 20, 200)",
                "INSERT INTO %s (k, c, a, b) VALUES (0, 3, 30, 300)")
        .queryColumns("a", 1, 1,
                      rows(row(20)),
                      rows(row(0, 2, 20, 200)),
                      rows(row(0, 2, 20, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=0 AND c=2", "b", 0, 1,
                      rows(row(200)),
                      rows(row(0, 2, null, 200)),
                      rows(row(0, 2, 20, 200)))
        .deleteRows("DELETE FROM %s WHERE k=0 AND c=2", 1,
                    rows(),
                    rows(row(0, 2, null, 200)))
        .tearDown(paging ? 2 : 1,
                  rows(row(0, 1, 10, 100), row(0, 3, 30, 300)),
                  rows());
    }

    /**
     * Test range queries on a table without clustering columns.
     */
    @Test
    public void testRangeQueryOnSkinnyTable()
    {
        tester("WHERE token(k) >= token(2)")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (1, 10, 100)",
                "INSERT INTO %s (k, a, b) VALUES (2, 20, 200)",
                "INSERT INTO %s (k, a, b) VALUES (3, 30, 300)")
        .queryColumns("a", 2, 2,
                      rows(row(20), row(30)),
                      rows(row(2, 20, 200), row(3, 30, 300)),
                      rows(row(2, 20, null), row(3, 30, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=2", "b", 0, 1,
                      rows(row(200), row(300)),
                      rows(row(2, null, 200), row(3, 30, 300)),
                      rows(row(2, 20, 200), row(3, 30, 300)))
        .deleteRows("DELETE FROM %s WHERE k=3", 1,
                    rows(row(2, null, 200)),
                    rows(row(2, null, 200), row(3, 30, 300)))
        .tearDown(1,
                  rows(row(1, 10, 100), row(2, null, 200)),
                  rows(row(2, null, 200)));
    }

    /**
     * Test range queries on a table with clustering columns.
     */
    @Test
    public void testRangeQueryOnWideTable()
    {
        tester("WHERE token(k) >= token(2)")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (1, 10, 100, 1000)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 20, 200, 2000)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 21, 201, 2001)",
                "INSERT INTO %s (k, c, a, b) VALUES (3, 30, 300, 3000)")
        .queryColumns("a",
                      paging ? 3 : 2,
                      paging ? 3 : 2,
                      rows(row(200), row(201), row(300)),
                      rows(row(2, 20, 200, 2000), row(2, 21, 201, 2001), row(3, 30, 300, 3000)),
                      rows(row(2, 20, 200, null), row(2, 21, 201, null), row(3, 30, 300, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=2 AND c=21", "b", 0, 1,
                      rows(row(2000), row(2001), row(3000)),
                      rows(row(2, 20, 200, 2000), row(2, 21, null, 2001), row(3, 30, 300, 3000)),
                      rows(row(2, 20, 200, 2000), row(2, 21, 201, 2001), row(3, 30, 300, 3000)))
        .deleteRows("DELETE FROM %s WHERE k=2 AND c=21", 1,
                    rows(row(2, 20, 200, 2000), row(3, 30, 300, 3000)),
                    rows(row(2, 20, 200, 2000), row(2, 21, null, 2001), row(3, 30, 300, 3000)))
        .deleteRows("DELETE FROM %s WHERE k=2", 1,
                    rows(row(3, 30, 300, 3000)),
                    rows(row(2, 20, 200, 2000), row(3, 30, 300, 3000)))
        .tearDown(1,
                  rows(row(1, 10, 100, 1000), row(3, 30, 300, 3000)),
                  rows(row(3, 30, 300, 3000)));
    }

    /**
     * Test range queries without restrictions but with a limit on a table without clustering columns.
     */
    @Test
    public void testRangeQueryWithLimitOnSkinnyTable()
    {
        tester("WHERE token(k) >= token(1) LIMIT 2")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (1, 10, 100)",
                "INSERT INTO %s (k, a, b) VALUES (2, 20, 200)",
                "INSERT INTO %s (k, a, b) VALUES (3, 30, 300)")
        .queryColumns("a", 2, 2,
                      rows(row(10), row(20)),
                      rows(row(1, 10, 100), row(2, 20, 200)),
                      rows(row(1, 10, null), row(2, 20, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=2", "b", 0, 1,
                      rows(row(100), row(200)),
                      rows(row(1, 10, 100), row(2, null, 200)),
                      rows(row(1, 10, 100), row(2, 20, 200)))
        .deleteRows("DELETE FROM %s WHERE k=1", 2,
                    rows(row(2, null, 200), row(3, 30, 300)),
                    rows(row(1, 10, 100), row(2, null, 200)))
        .tearDown(0,
                  rows(row(2, null, 200), row(3, 30, 300)),
                  rows(row(2, null, 200), row(3, 30, 300)));
    }

    /**
     * Test range queries without restrictions but with a limit on a table with clustering columns.
     */
    @Test
    public void testRangeQueryWithLimitOnWideTable()
    {
        tester("WHERE token(k) >= token(1) LIMIT 2")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (1, 10, 100, 1000)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 20, 200, 2000)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 21, 201, 2001)",
                "INSERT INTO %s (k, c, a, b) VALUES (3, 30, 300, 3000)")
        .queryColumns("a", 2, 2,
                      rows(row(100), row(200)),
                      rows(row(1, 10, 100, 1000), row(2, 20, 200, 2000)),
                      rows(row(1, 10, 100, null), row(2, 20, 200, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=2 AND c=20", "b", 0, 1,
                      rows(row(1000), row(2000)),
                      rows(row(1, 10, 100, 1000), row(2, 20, null, 2000)),
                      rows(row(1, 10, 100, 1000), row(2, 20, 200, 2000)))
        .deleteRows("DELETE FROM %s WHERE k=2 AND c=20", 1,
                    rows(row(1, 10, 100, 1000), row(2, 21, 201, 2001)),
                    rows(row(1, 10, 100, 1000), row(2, 20, null, 2000)))
        .deleteRows("DELETE FROM %s WHERE k=2", 2,
                    rows(row(1, 10, 100, 1000), row(3, 30, 300, 3000)),
                    rows(row(1, 10, 100, 1000), row(2, 21, 201, 2001)))
        .tearDown(0,
                  rows(row(1, 10, 100, 1000), row(3, 30, 300, 3000)),
                  rows(row(1, 10, 100, 1000), row(3, 30, 300, 3000)));
    }

    /**
     * Test range queries using filtering on a selected column on a table without clustering columns.
     */
    @Test
    public void testRangeQueryWithFilterOnSelectedColumnOnSkinnyTable()
    {
        tester("WHERE a=2 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (1, 2, 3)",
                "INSERT INTO %s (k, a, b) VALUES (10, 20, 30)")
        .queryColumns("a", 1, 1,
                      rows(row(2)),
                      rows(row(1, 2, 3)),
                      rows(row(1, 2, null)))
        .deleteColumn("DELETE b FROM %s WHERE k=1", "a", 0, 1,
                      rows(row(2)),
                      rows(row(1, 2, null)),
                      rows(row(1, 2, 3)))
        .deleteRows("DELETE FROM %s WHERE k=1", 1,
                    rows(),
                    rows(row(1, 2, null)))
        .tearDown(1,
                  rows(row(10, 20, 30)),
                  rows());
    }

    /**
     * Test range queries using filtering on an selected column on a table with clustering columns.
     */
    @Test
    public void testRangeQueryWithFilterOnSelectedColumnOnWideTable()
    {
        tester("WHERE a=1 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (1, 1, 1, 1)",
                "INSERT INTO %s (k, c, a, b) VALUES (1, 2, 2, 2)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 1, 1, 1)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 2, 2, 2)")
        .queryColumns("a", 2, 2,
                      rows(row(1), row(1)),
                      rows(row(1, 1, 1, 1), row(2, 1, 1, 1)),
                      rows(row(1, 1, 1, null), row(2, 1, 1, null)))
        .deleteColumn("DELETE b FROM %s WHERE k=1 AND c=1", "a", 0, 1,
                      rows(row(1), row(1)),
                      rows(row(1, 1, 1, null), row(2, 1, 1, 1)),
                      rows(row(1, 1, 1, 1), row(2, 1, 1, 1)))
        .deleteRows("DELETE FROM %s WHERE k=1 AND c=1", 1,
                    rows(row(2, 1, 1, 1)),
                    rows(row(1, 1, 1, null), row(2, 1, 1, 1)))
        .deleteRows("DELETE FROM %s WHERE k=2", 1,
                    rows(),
                    rows(row(2, 1, 1, 1)))
        .tearDown(1,
                  rows(row(1, 2, 2, 2)),
                  rows());
    }

    /**
     * Test range queries using filtering on an unselected column on a table without clustering columns.
     */
    @Test
    public void testRangeQueryWithFilterOnUnselectedColumnOnSkinnyTable()
    {
        tester("WHERE b=3 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (1, 2, 3)",
                "INSERT INTO %s (k, a, b) VALUES (10, 20, 30)")
        .queryColumns("a", 1, 0,
                      rows(row(2)),
                      rows(row(1, 2, 3)),
                      rows(row(1, 2, 3))) // the filtered column is repaired even if it isn't selected
        .deleteColumn("DELETE a FROM %s WHERE k=1", "b", 0, 1,
                      rows(row(3)),
                      rows(row(1, null, 3)),
                      rows(row(1, 2, 3)))
        .deleteRows("DELETE FROM %s WHERE k=1", 1,
                    rows(),
                    rows(row(1, null, 3)))
        .tearDown(1,
                  rows(row(10, 20, 30)),
                  rows());
    }

    /**
     * Test range queries using filtering on an unselected column on a table with clustering columns.
     */
    @Test
    public void testRangeQueryWithFilterOnUnselectedColumnOnWideTable()
    {
        tester("WHERE b=2 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (1, 1, 1, 1)",
                "INSERT INTO %s (k, c, a, b) VALUES (1, 2, 2, 2)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 1, 1, 1)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 2, 2, 2)")
        .queryColumns("a", 2, 0,
                      rows(row(2), row(2)),
                      rows(row(1, 2, 2, 2), row(2, 2, 2, 2)),
                      rows(row(1, 2, 2, 2), row(2, 2, 2, 2))) // the filtered column is repaired even if it isn't selected
        .deleteColumn("DELETE a FROM %s WHERE k=1 AND c=2", "b", 0, 1,
                      rows(row(2), row(2)),
                      rows(row(1, 2, null, 2), row(2, 2, 2, 2)),
                      rows(row(1, 2, 2, 2), row(2, 2, 2, 2)))
        .deleteRows("DELETE FROM %s WHERE k=2 AND c=2", 1,
                    rows(row(1, 2, null, 2)),
                    rows(row(1, 2, null, 2), row(2, 2, 2, 2)))
        .deleteRows("DELETE FROM %s WHERE k=1", 1,
                    rows(),
                    rows(row(1, 2, null, 2)))
        .tearDown(1,
                  rows(row(2, 1, 1, 1)),
                  rows());
    }

    /**
     * Test slice queries without additional restrictions.
     */
    @Test
    public void testSliceQuery()
    {
        tester("WHERE k=0 AND c>1 AND c<4")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (0, 1, 10, 100)",
                "INSERT INTO %s (k, c, a, b) VALUES (0, 2, 20, 200)",
                "INSERT INTO %s (k, c, a, b) VALUES (0, 3, 30, 300)",
                "INSERT INTO %s (k, c, a, b) VALUES (0, 4, 40, 400)")
        .queryColumns("a", paging ? 2 : 1, paging ? 2 : 1,
                      rows(row(20), row(30)),
                      rows(row(0, 2, 20, 200), row(0, 3, 30, 300)),
                      rows(row(0, 2, 20, null), row(0, 3, 30, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=0 AND c=2", "b", 0, 1,
                      rows(row(200), row(300)),
                      rows(row(0, 2, null, 200), row(0, 3, 30, 300)),
                      rows(row(0, 2, 20, 200), row(0, 3, 30, 300)))
        .deleteRows("DELETE FROM %s WHERE k=0 AND c=3", 1,
                    rows(row(0, 2, null, 200)),
                    rows(row(0, 2, null, 200), row(0, 3, 30, 300)))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, 2, null, 200)))
        .tearDown();
    }

    /**
     * Test slice queries using filtering.
     */
    @Test
    public void testSliceQueryWithFilter()
    {
        tester("WHERE k=0 AND a>10 AND a<40 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (0, 1, 10, 100)",
                "INSERT INTO %s (k, c, a, b) VALUES (0, 2, 20, 200)",
                "INSERT INTO %s (k, c, a, b) VALUES (0, 3, 30, 300)",
                "INSERT INTO %s (k, c, a, b) VALUES (0, 4, 40, 400)")
        .queryColumns("a", paging ? 2 : 1, paging ? 2 : 1,
                      rows(row(20), row(30)),
                      rows(row(0, 2, 20, 200), row(0, 3, 30, 300)),
                      rows(row(0, 2, 20, null), row(0, 3, 30, null)))
        .deleteColumn("DELETE b FROM %s WHERE k=0 AND c=2", "a", 0, 1,
                      rows(row(20), row(30)),
                      rows(row(0, 2, 20, null), row(0, 3, 30, 300)),
                      rows(row(0, 2, 20, 200), row(0, 3, 30, 300)))
        .deleteRows("DELETE FROM %s WHERE k=0 AND c=3", 1,
                    rows(row(0, 2, 20, null)),
                    rows(row(0, 2, 20, null), row(0, 3, 30, 300)))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, 2, 20, null)))
        .tearDown();
    }

    /**
     * Test slice queries without restrictions but with a limit.
     */
    @Test
    public void testSliceQueryWithLimit()
    {
        tester("WHERE k=0 AND c>1 LIMIT 2")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (0, 1, 10, 100)",
                "INSERT INTO %s (k, c, a, b) VALUES (0, 2, 20, 200)",
                "INSERT INTO %s (k, c, a, b) VALUES (0, 3, 30, 300)",
                "INSERT INTO %s (k, c, a, b) VALUES (0, 4, 40, 400)")
        .queryColumns("a", paging ? 2 : 1, paging ? 2 : 1,
                      rows(row(20), row(30)),
                      rows(row(0, 2, 20, 200), row(0, 3, 30, 300)),
                      rows(row(0, 2, 20, null), row(0, 3, 30, null)))
        .deleteColumn("DELETE b FROM %s WHERE k=0 AND c=2", "a", 0, 1,
                      rows(row(20), row(30)),
                      rows(row(0, 2, 20, null), row(0, 3, 30, 300)),
                      rows(row(0, 2, 20, 200), row(0, 3, 30, 300)))
        .deleteRows("DELETE FROM %s WHERE k=0 AND c=3", 1,
                    rows(row(0, 2, 20, null), row(0, 4, 40, 400)),
                    rows(row(0, 2, 20, null), row(0, 3, 30, 300)))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, 2, 20, null), row(0, 4, 40, 400)))
        .tearDown();
    }

    /**
     * Test slice queries on a table with static columns.
     */
    @Test
    public void testSliceQueryWithStaticColumns()
    {
        tester("WHERE k=0 AND c>1")
        .createTable("CREATE TABLE %s (k int, s1 int static, s2 int static, c int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, s1, s2) VALUES (0, 10, 100)",
                "INSERT INTO %s (k, c) VALUES (0, 1)",
                "INSERT INTO %s (k, c) VALUES (0, 2)",
                "INSERT INTO %s (k, c) VALUES (0, 3)")
        .queryColumns("s1,c", paging ? 2 : 1, 1,
                      rows(row(10, 2), row(10, 3)),
                      rows(row(0, 2, 10, 100), row(0, 3, 10, 100)),
                      rows(row(0, 2, 10, null), row(0, 3, 10, null)))
        .deleteColumn("DELETE s1 FROM %s WHERE k=0", "s2,c", 0, 1,
                      rows(row(100, 2), row(100, 3)),
                      rows(row(0, 2, null, 100), row(0, 3, null, 100)),
                      rows(row(0, 2, 10, 100), row(0, 3, 10, 100)))
        .deleteRows("DELETE FROM %s WHERE k=0 AND c=3", 1,
                    rows(row(0, 2, null, 100)),
                    rows(row(0, 2, null, 100), row(0, 3, null, 100)))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, 2, null, 100)))
        .tearDown();
    }

    /**
     * Test queries with an IN restriction on a table without clustering columns.
     */
    @Test
    public void testInQueryOnSkinnyTable()
    {
        tester("WHERE k IN (1, 3)")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (1, 10, 100)",
                "INSERT INTO %s (k, a, b) VALUES (2, 20, 200)",
                "INSERT INTO %s (k, a, b) VALUES (3, 30, 300)")
        .queryColumns("a", 2, 2,
                      rows(row(10), row(30)),
                      rows(row(1, 10, 100), row(3, 30, 300)),
                      rows(row(1, 10, null), row(3, 30, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=1", "b", 0, 1,
                      rows(row(100), row(300)),
                      rows(row(1, null, 100), row(3, 30, 300)),
                      rows(row(1, 10, 100), row(3, 30, 300)))
        .deleteRows("DELETE FROM %s WHERE k=3", 1,
                    rows(row(1, null, 100)),
                    rows(row(1, null, 100), row(3, 30, 300)))
        .tearDown(1,
                  rows(row(1, null, 100), row(2, 20, 200)),
                  rows(row(1, null, 100)));
    }

    /**
     * Test queries with an IN restriction on a table with clustering columns.
     */
    @Test
    public void testInQueryOnWideTable()
    {
        tester("WHERE k IN (1, 3)")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (1, 10, 100, 1000)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 20, 200, 2000)",
                "INSERT INTO %s (k, c, a, b) VALUES (3, 30, 300, 3000)")
        .queryColumns("a", 2, 2,
                      rows(row(100), row(300)),
                      rows(row(1, 10, 100, 1000), row(3, 30, 300, 3000)),
                      rows(row(1, 10, 100, null), row(3, 30, 300, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=1 AND c=10", "b", 0, 1,
                      rows(row(1000), row(3000)),
                      rows(row(1, 10, null, 1000), row(3, 30, 300, 3000)),
                      rows(row(1, 10, 100, 1000), row(3, 30, 300, 3000)))
        .deleteRows("DELETE FROM %s WHERE k=3", 1,
                    rows(row(1, 10, null, 1000)),
                    rows(row(1, 10, null, 1000), row(3, 30, 300, 3000)))
        .tearDown(1,
                  rows(row(1, 10, null, 1000), row(2, 20, 200, 2000)),
                  rows(row(1, 10, null, 1000)));
    }

    /**
     * Test queries with an IN restriction and a limit on a table without clustering columns.
     */
    @Test
    public void testInQueryWithLimitOnSkinnyTable()
    {
        tester("WHERE k IN (1, 3) LIMIT 1")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (1, 10, 100)",
                "INSERT INTO %s (k, a, b) VALUES (2, 20, 200)",
                "INSERT INTO %s (k, a, b) VALUES (3, 30, 300)")
        .queryColumns("a", 1, 1,
                      rows(row(10)),
                      rows(row(1, 10, 100)),
                      rows(row(1, 10, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=1", "b", 0, 1,
                      rows(row(100)),
                      rows(row(1, null, 100)),
                      rows(row(1, 10, 100)))
        .deleteRows("DELETE FROM %s WHERE k=1", 2,
                    rows(row(3, 30, 300)),
                    rows(row(1, null, 100)))
        .tearDown(1,
                  rows(row(2, 20, 200), row(3, 30, 300)),
                  rows(row(3, 30, 300)));
    }

    /**
     * Test queries with an IN restriction and a limit on a table with clustering columns.
     */
    @Test
    public void testInQueryWithLimitOnWideTable()
    {
        tester("WHERE k IN (1, 3) LIMIT 1")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (1, 10, 100, 1000)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 20, 200, 2000)",
                "INSERT INTO %s (k, c, a, b) VALUES (3, 30, 300, 3000)")
        .queryColumns("a", 1, 1,
                      rows(row(100)),
                      rows(row(1, 10, 100, 1000)),
                      rows(row(1, 10, 100, null)))
        .deleteColumn("DELETE a FROM %s WHERE k=1 AND c=10", "b", 0, 1,
                      rows(row(1000)),
                      rows(row(1, 10, null, 1000)),
                      rows(row(1, 10, 100, 1000)))
        .deleteRows("DELETE FROM %s WHERE k=1", 2,
                    rows(row(3, 30, 300, 3000)),
                    rows(row(1, 10, null, 1000)))
        .tearDown(1,
                  rows(row(2, 20, 200, 2000), row(3, 30, 300, 3000)),
                  rows(row(3, 30, 300, 3000)));
    }

    /**
     * Test queries with an IN restriction and a row filter on one of the selected columns without clustering columns.
     */
    @Test
    public void testInQueryWithFilterOnSelectedColumnOnSkinnyTable()
    {
        tester("WHERE k IN (1, 3) AND a=10 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (1, 10, 100)",
                "INSERT INTO %s (k, a, b) VALUES (2, 10, 200)",
                "INSERT INTO %s (k, a, b) VALUES (3, 10, 300)",
                "INSERT INTO %s (k, a, b) VALUES (4, 40, 400)")
        .queryColumns("a", 2, 2,
                      rows(row(10), row(10)),
                      rows(row(1, 10, 100), row(3, 10, 300)),
                      rows(row(1, 10, null), row(3, 10, null)))
        .deleteColumn("DELETE b FROM %s WHERE k=1", "a", 0, 1,
                      rows(row(10), row(10)),
                      rows(row(1, 10, null), row(3, 10, 300)),
                      rows(row(1, 10, 100), row(3, 10, 300)))
        .deleteRows("DELETE FROM %s WHERE k=1", 1,
                    rows(row(3, 10, 300)),
                    rows(row(1, 10, null), row(3, 10, 300)))
        .tearDown(2,
                  rows(row(2, 10, 200), row(4, 40, 400), row(3, 10, 300)),
                  rows(row(3, 10, 300)));
    }

    /**
     * Test queries with an IN restriction and a row filter on one of the selected columns with clustering columns.
     */
    @Test
    public void testInQueryWithFilterOnSelectedColumnOnWideTable()
    {
        tester("WHERE k IN (1, 3) AND a=100 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (1, 10, 100, 1000)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 20, 100, 2000)",
                "INSERT INTO %s (k, c, a, b) VALUES (3, 30, 100, 3000)",
                "INSERT INTO %s (k, c, a, b) VALUES (4, 40, 400, 4000)")
        .queryColumns("a", 2, 2,
                      rows(row(100), row(100)),
                      rows(row(1, 10, 100, 1000), row(3, 30, 100, 3000)),
                      rows(row(1, 10, 100, null), row(3, 30, 100, null)))
        .deleteColumn("DELETE b FROM %s WHERE k=1 AND c=10", "a", 0, 1,
                      rows(row(100), row(100)),
                      rows(row(1, 10, 100, null), row(3, 30, 100, 3000)),
                      rows(row(1, 10, 100, 1000), row(3, 30, 100, 3000)))
        .deleteRows("DELETE FROM %s WHERE k=1", 1,
                    rows(row(3, 30, 100, 3000)),
                    rows(row(1, 10, 100, null), row(3, 30, 100, 3000)))
        .tearDown(2,
                  rows(row(2, 20, 100, 2000), row(4, 40, 400, 4000), row(3, 30, 100, 3000)),
                  rows(row(3, 30, 100, 3000)));
    }

    /**
     * Test queries with an IN restriction and a row filter on unselected columns without clustering columns.
     */
    @Test
    public void testInQueryWithFilterOnUnselectedColumnOnSkinnyTable()
    {
        tester("WHERE k IN (1, 3) AND b=100 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (1, 10, 100)",
                "INSERT INTO %s (k, a, b) VALUES (2, 20, 100)",
                "INSERT INTO %s (k, a, b) VALUES (3, 30, 100)")
        .queryColumns("a", 2, 0,
                      rows(row(10), row(30)),
                      rows(row(1, 10, 100), row(3, 30, 100)),
                      rows(row(1, 10, 100), row(3, 30, 100))) // the filtered column is repaired even if it isn't selected
        .deleteColumn("DELETE a FROM %s WHERE k=1", "b", 0, 1,
                      rows(row(100), row(100)),
                      rows(row(1, null, 100), row(3, 30, 100)),
                      rows(row(1, 10, 100), row(3, 30, 100)))
        .deleteRows("DELETE FROM %s WHERE k=1", 1,
                    rows(row(3, 30, 100)),
                    rows(row(1, null, 100), row(3, 30, 100)))
        .tearDown(1,
                  rows(row(2, 20, 100), row(3, 30, 100)),
                  rows(row(3, 30, 100)));
    }

    /**
     * Test queries with an IN restriction and a row filter on unselected columns with clustering columns.
     */
    @Test
    public void testInQueryWithFilterOnUnselectedColumnOnWideTable()
    {
        tester("WHERE k IN (1, 3) AND b=1000 ALLOW FILTERING")
        .createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY(k, c))")
        .mutate("INSERT INTO %s (k, c, a, b) VALUES (1, 10, 100, 1000)",
                "INSERT INTO %s (k, c, a, b) VALUES (2, 20, 200, 2000)",
                "INSERT INTO %s (k, c, a, b) VALUES (3, 30, 300, 3000)")
        .queryColumns("a", 1, 0,
                      rows(row(100)),
                      rows(row(1, 10, 100, 1000)),
                      rows(row(1, 10, 100, 1000))) // the filtered column is repaired even if it isn't selected
        .deleteColumn("DELETE a FROM %s WHERE k=1 AND c=10", "b", 0, 1,
                      rows(row(1000)),
                      rows(row(1, 10, null, 1000)),
                      rows(row(1, 10, 100, 1000)))
        .deleteRows("DELETE FROM %s WHERE k=1", 1,
                    rows(),
                    rows(row(1, 10, null, 1000)))
        .tearDown(2,
                  rows(row(2, 20, 200, 2000), row(3, 30, 300, 3000)),
                  rows());
    }

    /**
     * Test unrestricted queries with frozen tuples.
     */
    @Test
    public void testTuple()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a tuple<int,int>, b tuple<int,int>)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (0, (1, 2), (3, 4))")
        .queryColumns("a", 1, 1,
                      rows(row(tuple(1, 2))),
                      rows(row(0, tuple(1, 2), tuple(3, 4))),
                      rows(row(0, tuple(1, 2), null)))
        .deleteColumn("DELETE a FROM %s WHERE k=0", "b", 0, 1,
                      rows(row(tuple(3, 4))),
                      rows(row(0, null, tuple(3, 4))),
                      rows(row(0, tuple(1, 2), tuple(3, 4))))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, null, tuple(3, 4))))
        .tearDown();
    }

    /**
     * Test unrestricted queries with frozen sets.
     */
    @Test
    public void testFrozenSet()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a frozen<set<int>>, b frozen<set<int>>)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (0, {1, 2}, {3, 4})")
        .queryColumns("a[1]", 1, 1,
                      rows(row(1)),
                      rows(row(0, set(1, 2), set(3, 4))),
                      rows(row(0, set(1, 2), null)))
        .deleteColumn("DELETE a FROM %s WHERE k=0", "b[4]", 0, 1,
                      rows(row(4)),
                      rows(row(0, null, set(3, 4))),
                      rows(row(0, set(1, 2), set(3, 4))))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, null, set(3, 4))))
        .tearDown();
    }

    /**
     * Test unrestricted queries with frozen lists.
     */
    @Test
    public void testFrozenList()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a frozen<list<int>>, b frozen<list<int>>)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (0, [1, 2], [3, 4])")
        .queryColumns("a", 1, 1,
                      rows(row(list(1, 2))),
                      rows(row(0, list(1, 2), list(3, 4))),
                      rows(row(0, list(1, 2), null)))
        .deleteColumn("DELETE a FROM %s WHERE k=0", "b", 0, 1,
                      rows(row(list(3, 4))),
                      rows(row(0, null, list(3, 4))),
                      rows(row(0, list(1, 2), list(3, 4))))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, null, list(3, 4))))
        .tearDown();
    }

    /**
     * Test unrestricted queries with frozen maps.
     */
    @Test
    public void testFrozenMap()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a frozen<map<int,int>>, b frozen<map<int,int>>)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (0, {1:10, 2:20}, {3:30, 4:40})")
        .queryColumns("a[2]", 1, 1,
                      rows(row(20)),
                      rows(row(0, map(1, 10, 2, 20), map(3, 30, 4, 40))),
                      rows(row(0, map(1, 10, 2, 20), null)))
        .deleteColumn("DELETE a FROM %s WHERE k=0", "b[4]", 0, 1,
                      rows(row(40)),
                      rows(row(0, null, map(3, 30, 4, 40))),
                      rows(row(0, map(1, 10, 2, 20), map(3, 30, 4, 40))))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, null, map(3, 30, 4, 40))))
        .tearDown();
    }

    /**
     * Test unrestricted queries with frozen user-defined types.
     */
    @Test
    public void testFrozentuple()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a frozen<udt>, b frozen<udt>)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (0, {x:1, y:2}, {x:3, y:4})")
        .queryColumns("a.x", 1, 1,
                      rows(row(1)),
                      rows(row(0, tuple(1, 2), tuple(3, 4))),
                      rows(row(0, tuple(1, 2), null)))
        .deleteColumn("DELETE a FROM %s WHERE k=0", "b.y", 0, 1,
                      rows(row(4)),
                      rows(row(0, null, tuple(3, 4))),
                      rows(row(0, tuple(1, 2), tuple(3, 4))))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, null, tuple(3, 4))))
        .tearDown();
    }

    /**
     * Test unrestricted queries with non-frozen sets.
     */
    @Test
    public void testNonFrozenSet()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a set<int>, b set<int>, c int)")
        .mutate("INSERT INTO %s (k, a, b, c) VALUES (0, {1, 2}, {3, 4}, 10)")
        .queryColumns("a[1]", 1, 1,
                      rows(row(1)),
                      rows(row(0, set(1, 2), set(3, 4), 10)),
                      rows(row(0, set(1), null, null)))
        .deleteColumn("UPDATE %s SET a=a-{2} WHERE k=0", "b[4]", 0, 1,
                      rows(row(4)),
                      rows(row(0, set(1), set(3, 4), 10)),
                      rows(row(0, set(1, 2), set(3, 4), 10)))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, set(1), set(3, 4), 10)))
        .tearDown();
    }

    /**
     * Test unrestricted queries with non-frozen lists.
     */
    @Test
    public void testNonFrozenList()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a list<int>, b list<int>, c int)")
        .mutate("INSERT INTO %s (k, a, b, c) VALUES (0, [1, 2], [3, 4], 10)")
        .queryColumns("a", 1, 1,
                      rows(row(list(1, 2))),
                      rows(row(0, list(1, 2), list(3, 4), 10)),
                      rows(row(0, list(1, 2), null, null)))
        .deleteColumn("DELETE a[1] FROM %s WHERE k=0", "b", 0, 1,
                      rows(row(list(3, 4))),
                      rows(row(0, list(1), list(3, 4), 10)),
                      rows(row(0, list(1, 2), list(3, 4), 10)))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, list(1), list(3, 4), 10)))
        .tearDown();
    }

    /**
     * Test unrestricted queries with non-frozen maps.
     */
    @Test
    public void testNonFrozenMap()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a map<int,int>, b map<int,int>, c int)")
        .mutate("INSERT INTO %s (k, a, b, c) VALUES (0, {1:10, 2:20}, {3:30, 4:40}, 10)")
        .queryColumns("a[2]", 1, 1,
                      rows(row(20)),
                      rows(row(0, map(1, 10, 2, 20), map(3, 30, 4, 40), 10)),
                      rows(row(0, map(2, 20), null, null)))
        .deleteColumn("DELETE a[1] FROM %s WHERE k=0", "b[4]", 0, 1,
                      rows(row(40)),
                      rows(row(0, map(2, 20), map(3, 30, 4, 40), 10)),
                      rows(row(0, map(1, 10, 2, 20), map(3, 30, 4, 40), 10)))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, map(2, 20), map(3, 30, 4, 40), 10)))
        .tearDown();
    }

    /**
     * Test unrestricted queries with non-frozen user-defined types.
     */
    @Test
    public void testNonFrozentuple()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a udt, b udt, c int)")
        .mutate("INSERT INTO %s (k, a, b, c) VALUES (0, {x:1, y:2}, {x:3, y:4}, 10)")
        .queryColumns("a.x", 1, 1,
                      rows(row(1)),
                      rows(row(0, tuple(1, 2), tuple(3, 4), 10)),
                      rows(row(0, tuple(1, 2), null, null)))
        .deleteColumn("DELETE a.x FROM %s WHERE k=0", "b.y", 0, 1,
                      rows(row(4)),
                      rows(row(0, tuple(null, 2), tuple(3, 4), 10)),
                      rows(row(0, tuple(1, 2), tuple(3, 4), 10)))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, tuple(null, 2), tuple(3, 4), 10)))
        .tearDown();
    }

    private Tester tester(String restriction)
    {
        return new Tester(restriction, cluster, strategy, coordinator, flush, paging);
    }

    private static class Tester extends ReadRepairTester<Tester>
    {
        private final String restriction; // the tested CQL query WHERE restriction
        private final String allColumnsQuery; // a SELECT * query for the table using the tested restriction

        Tester(String restriction, Cluster cluster, ReadRepairStrategy strategy, int coordinator, boolean flush, boolean paging)
        {
            super(cluster, strategy, coordinator, flush, paging, false);
            this.restriction = restriction;

            allColumnsQuery = String.format("SELECT * FROM %s %s", qualifiedTableName, restriction);
        }

        @Override
        Tester self()
        {
            return this;
        }

        /**
         * Runs the tested query with CL=ALL selectig only the specified columns and verifies that it returns the
         * specified rows. Then, it runs the query again selecting all the columns, and verifies that the first query
         * execution only propagated the selected columns, and that the second execution propagated everything.
         *
         * @param columns                  the selected columns
         * @param columnsQueryRepairedRows the expected number of repaired rows when querying only the selected columns
         * @param rowsQueryRepairedRows    the expected number of repaired rows when querying all the columns
         * @param columnsQueryResults      the rows returned by the query for a subset of columns
         * @param node1Rows                the rows in the first node, which is the one with the most updated data
         * @param node2Rows                the rows in the second node, which is the one meant to receive the RR writes
         */
        Tester queryColumns(String columns,
                            long columnsQueryRepairedRows,
                            long rowsQueryRepairedRows,
                            Object[][] columnsQueryResults,
                            Object[][] node1Rows,
                            Object[][] node2Rows)
        {
            // query only the selected columns with CL=ALL to trigger partial read repair on that column
            String columnsQuery = String.format("SELECT %s FROM %s %s", columns, qualifiedTableName, restriction);
            assertRowsDistributed(columnsQuery, columnsQueryRepairedRows, columnsQueryResults);

            // query entire rows to repair the rest of the columns, that might trigger new repairs for those columns
            return verifyQuery(allColumnsQuery, rowsQueryRepairedRows, node1Rows, node2Rows);
        }

        /**
         * Executes the specified column deletion on just one node. Then it runs the tested query with CL=ALL selectig
         * only the specified columns (which are expected to be different to the deleted one) and verifies that it
         * returns the specified rows. Then it runs the tested query again, this time selecting all the columns, to
         * verify that the previous query didn't propagate the column deletion.
         *
         * @param columnDeletion           the deletion query for a first node
         * @param columns                  a subset of the table columns for the first distributed query
         * @param columnsQueryRepairedRows the expected number of repaired rows when querying only the selected columns
         * @param rowsQueryRepairedRows    the expected number of repaired rows when querying all the columns
         * @param columnsQueryResults      the rows returned by the query for a subset of columns
         * @param node1Rows                the rows in the first node, which is the one with the most updated data
         * @param node2Rows                the rows in the second node, which is the one meant to receive the RR writes
         */
        Tester deleteColumn(String columnDeletion,
                            String columns,
                            long columnsQueryRepairedRows,
                            long rowsQueryRepairedRows,
                            Object[][] columnsQueryResults,
                            Object[][] node1Rows,
                            Object[][] node2Rows)
        {
            assert restriction != null;

            // execute the column deletion on just one node
            mutate(1, columnDeletion);

            // verify the columns read with CL=ALL, in most cases this won't propagate the previous column deletion if
            // the deleted and read columns don't overlap
            return queryColumns(columns,
                                columnsQueryRepairedRows,
                                rowsQueryRepairedRows,
                                columnsQueryResults,
                                node1Rows,
                                node2Rows);
        }

        /**
         * Executes the specified row deletion on just one node and verifies the tested query, to ensure that the tested
         * query propagates the row deletion.
         */
        Tester deleteRows(String rowDeletion, long repairedRows, Object[][] node1Rows, Object[][] node2Rows)
        {
            mutate(1, rowDeletion);
            return verifyQuery(allColumnsQuery, repairedRows, node1Rows, node2Rows);
        }

        private Tester mutate(String... queries)
        {
            return mutate(1, queries);
        }

        private Tester verifyQuery(String query, long expectedRepairedRows, Object[][] node1Rows, Object[][] node2Rows)
        {
            // verify the per-replica status before running the query distributedly
            assertRows(cluster.get(1).executeInternal(query), node1Rows);
            assertRows(cluster.get(2).executeInternal(query), strategy == NONE ? EMPTY_ROWS : node2Rows);

            // now, run the query with CL=ALL to reconcile and repair the replicas
            assertRowsDistributed(query, expectedRepairedRows, node1Rows);

            // run the query locally again to verify that the distributed query has repaired everything
            assertRows(cluster.get(1).executeInternal(query), node1Rows);
            assertRows(cluster.get(2).executeInternal(query), strategy == NONE ? EMPTY_ROWS : node1Rows);

            return this;
        }

        /**
         * Verifies that the replicas are empty and drop the table.
         */
        void tearDown()
        {
            tearDown(0, rows(), rows());
        }

        /**
         * Verifies the final status of the nodes with an unrestricted query, to ensure that the main tested query
         * hasn't triggered any unexpected repairs. Then, it verifies that the node that hasn't been used as coordinator
         * hasn't triggered any unexpected repairs. Finally, it drops the table.
         */
        void tearDown(long repairedRows, Object[][] node1Rows, Object[][] node2Rows)
        {
            verifyQuery("SELECT * FROM " + qualifiedTableName, repairedRows, node1Rows, node2Rows);
            for (int n = 1; n <= cluster.size(); n++)
            {
                if (n == coordinator)
                    continue;

                long requests = readRepairRequestsCount(n);
                String message = String.format("No read repair requests were expected in not-coordinator nodes, " +
                                               "but found %d requests in node %d", requests, n);
                assertEquals(message, 0, requests);
            }
            schemaChange("DROP TABLE " + qualifiedTableName);
        }
    }
}