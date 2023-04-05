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
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

import static org.apache.cassandra.distributed.shared.AssertUtils.row;


/**
 * Test read repair when there are tombstone ranges, particularly to test that empty range tombstones are not emitted.
 * <p>
 * See CASSANDRA-15924.
 */
@RunWith(Parameterized.class)
public class ReadRepairEmptyRangeTombstonesTest extends TestBaseImpl
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

    /**
     * Whether the clustering order is reverse
     */
    @Parameterized.Parameter(4)
    public boolean reverse;

    @Parameterized.Parameters(name = "{index}: strategy={0} coordinator={1} flush={2} paging={3} reverse={4}")
    public static Collection<Object[]> data()
    {
        List<Object[]> result = new ArrayList<>();
        for (int coordinator = 1; coordinator <= NUM_NODES; coordinator++)
            for (boolean flush : BOOLEANS)
                for (boolean paging : BOOLEANS)
                    for (boolean reverse : BOOLEANS)
                        result.add(new Object[]{ ReadRepairStrategy.BLOCKING, coordinator, flush, paging, reverse });
        result.add(new Object[]{ ReadRepairStrategy.NONE, 1, false, false, false });
        return result;
    }

    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(NUM_NODES)
                              .withConfig(config -> config.set("read_request_timeout", "1m")
                                                          .set("write_request_timeout", "1m"))
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
     * Test range queries asking for an empty range with a tombstone range in one replica.
     */
    @Test
    public void testEmptyRangeQueries()
    {
        tester().createTable("CREATE TABLE %s(k int, c int, PRIMARY KEY (k, c)) " +
                             "WITH CLUSTERING ORDER BY (c %s) AND read_repair='%s'")
                .mutate(1, "DELETE FROM %s USING TIMESTAMP 1 WHERE k=0 AND c>0 AND c<3")
                .assertRowsDistributed("SELECT * FROM %s WHERE k=0 AND c>1 and c<=1", 0)
                .assertRowsDistributed("SELECT * FROM %s WHERE k=0 AND c>2 and c<=2", 0)
                .assertRowsInternal("SELECT * FROM %s")
                .mutate(2, "DELETE FROM %s WHERE k=0 AND c>0 AND c<3")
                .assertRowsInternal("SELECT * FROM %s");
    }

    /**
     * Test range queries asking for an empty range with a tombstone range in one replica, with only static columns.
     */
    @Test
    public void testEmptyRangeQueriesWithStaticRow()
    {
        tester().createTable("CREATE TABLE %s(k int, c int, s int static, PRIMARY KEY (k, c)) " +
                             "WITH CLUSTERING ORDER BY (c %s) AND read_repair='%s'")
                .mutate(1, "DELETE FROM %s USING TIMESTAMP 1 WHERE k=0 AND c>0 AND c<3")
                .mutate(1, "INSERT INTO %s (k, s) VALUES (0, 0)")
                .assertRowsDistributed("SELECT * FROM %s WHERE k=0 AND c>1 and c<=1", 0)
                .assertRowsDistributed("SELECT * FROM %s WHERE k=0 AND c>2 and c<=2", 0)
                .assertRowsInternal("SELECT * FROM %s")
                .mutate(2, "DELETE FROM %s WHERE k=0 AND c>0 AND c<3")
                .assertRowsInternal("SELECT * FROM %s");
    }

    /**
     * Test range queries asking for a not-empty range targeting rows contained in a tombstone range in one replica.
     */
    @Test
    public void testRangeQueriesWithRowsContainedInTombstone()
    {
        tester().createTable("CREATE TABLE %s(k int, c int, PRIMARY KEY (k, c)) " +
                             "WITH CLUSTERING ORDER BY (c %s) AND read_repair='%s'")
                .mutate(1, "DELETE FROM %s USING TIMESTAMP 1 WHERE k=0 AND c>=1 AND c<=5")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 2)")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 3)")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 4)")
                .assertRowsDistributed("SELECT * FROM %s WHERE k=0 AND c>=2 AND c<=3",
                                       paging ? 2 : 1,
                                       row(0, 2), row(0, 3))
                .assertRowsDistributed("SELECT * FROM %s WHERE k=0 AND c>=3 AND c<=4",
                                       1,
                                       row(0, 3), row(0, 4))
                .assertRowsInternal("SELECT * FROM %s", row(0, 2), row(0, 3), row(0, 4))
                .mutate(2, "DELETE FROM %s WHERE k=0 AND c>=1 AND c<=5")
                .assertRowsInternal("SELECT * FROM %s");
    }

    /**
     * Test range queries asking for a not-empty range targeting rows overlapping with a tombstone range in one replica.
     */
    @Test
    public void testRangeQueriesWithRowsOvetrlappingWithTombstoneRangeStart()
    {
        tester().createTable("CREATE TABLE %s(k int, c int, PRIMARY KEY (k, c)) " +
                             "WITH CLUSTERING ORDER BY (c %s) AND read_repair='%s'")
                .mutate(1, "DELETE FROM %s USING TIMESTAMP 1 WHERE k=0 AND c>=3 AND c<=6")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 1)")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 2)")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 3)")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 4)")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 5)")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 6)")
                .assertRowsDistributed("SELECT c FROM %s WHERE k=0 AND c>=1 AND c<=4",
                                       paging ? 4 : 1,
                                       row(1), row(2), row(3), row(4))
                .assertRowsDistributed("SELECT c FROM %s WHERE k=0 AND c>=2 AND c<=5",
                                       1,
                                       row(2), row(3), row(4), row(5))
                .assertRowsInternal("SELECT c FROM %s", row(1), row(2), row(3), row(4), row(5))
                .mutate(2, "DELETE FROM %s WHERE k=0 AND c>=1 AND c<=6")
                .assertRowsInternal("SELECT * FROM %s");
    }

    /**
     * Test range queries asking for a not-empty range targeting rows overlapping with a tombstone range in one replica.
     */
    @Test
    public void testRangeQueriesWithRowsOverlappingWithTombstoneRangeEnd()
    {
        tester().createTable("CREATE TABLE %s(k int, c int, PRIMARY KEY (k, c)) " +
                             "WITH CLUSTERING ORDER BY (c %s) AND read_repair='%s'")
                .mutate(1, "DELETE FROM %s USING TIMESTAMP 1 WHERE k=0 AND c>=1 AND c<=4")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 1)")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 2)")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 3)")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 4)")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 5)")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 6)")
                .assertRowsDistributed("SELECT c FROM %s WHERE k=0 AND c>=2 AND c<=5",
                                       paging ? 4 : 1,
                                       row(2), row(3), row(4), row(5))
                .assertRowsDistributed("SELECT c FROM %s WHERE k=0 AND c>=3 AND c<=6",
                                       1,
                                       row(3), row(4), row(5), row(6))
                .assertRowsInternal("SELECT c FROM %s", row(2), row(3), row(4), row(5), row(6))
                .mutate(2, "DELETE FROM %s WHERE k=0 AND c>=1 AND c<=6")
                .assertRowsInternal("SELECT * FROM %s");
    }

    /**
     * Test point queries retrieving data that is contained in a tombstone range in one replica.
     */
    @Test
    public void testPointQueriesWithRowsContainedInTombstoneRange()
    {
        tester().createTable("CREATE TABLE %s(k int, c int, PRIMARY KEY (k, c)) " +
                             "WITH CLUSTERING ORDER BY (c %s) AND read_repair='%s'")
                .mutate(1, "DELETE FROM %s USING TIMESTAMP 1 WHERE k=0 AND c>0 AND c<3")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 0)")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 1)")
                .mutate(1, "INSERT INTO %s (k, c) VALUES (0, 2)")
                .assertRowsDistributed("SELECT * FROM %s WHERE k=0 AND c=1", 1, row(0, 1))
                .assertRowsDistributed("SELECT * FROM %s WHERE k=0 AND c=2", 1, row(0, 2))
                .assertRowsDistributed("SELECT * FROM %s WHERE k=0 AND c=3", 0)
                .assertRowsInternal("SELECT * FROM %s", row(0, 1), row(0, 2))
                .mutate(2, "DELETE FROM %s WHERE k=0 AND c>0 AND c<3")
                .assertRowsInternal("SELECT * FROM %s");
    }

    /**
     * Test point queries retrieving data contained in a tombstone range in one replica, with only static columns.
     */
    @Test
    public void testPointQueriesWithRowsContainedInTombstoneRangeAndStaticRow()
    {
        tester().createTable("CREATE TABLE %s(k int, c int, s int static, PRIMARY KEY (k, c)) " +
                             "WITH CLUSTERING ORDER BY (c %s) AND read_repair='%s'")
                .mutate(1, "DELETE FROM %s USING TIMESTAMP 1 WHERE k=0 AND c>0 AND c<3")
                .mutate(1, "INSERT INTO %s (k, s) VALUES (0, 0)")
                .assertRowsDistributed("SELECT * FROM %s WHERE k=0 AND c=1", 1)
                .assertRowsDistributed("SELECT * FROM %s WHERE k=0 AND c=2", 1)
                .assertRowsInternal("SELECT * FROM %s", row(0, null, 0))
                .mutate(2, "DELETE FROM %s WHERE k=0 AND c>0 AND c<3")
                .assertRowsInternal("SELECT * FROM %s", row(0, null, 0));
    }

    private Tester tester()
    {
        return new Tester(cluster, strategy, coordinator, flush, paging, reverse);
    }

    private static class Tester extends ReadRepairTester<Tester>
    {
        private Tester(Cluster cluster, ReadRepairStrategy strategy, int coordinator, boolean flush, boolean paging, boolean reverse)
        {
            super(cluster, strategy, coordinator, flush, paging, reverse);
        }

        @Override
        Tester self()
        {
            return this;
        }

        Tester assertRowsInternal(String query, Object[]... expectedRows)
        {
            String formattedQuery = String.format(query, qualifiedTableName);

            if (strategy == ReadRepairStrategy.NONE)
                expectedRows = EMPTY_ROWS;
            else if (reverse)
                expectedRows = reverse(expectedRows);

            AssertUtils.assertRows(cluster.get(2).executeInternal(formattedQuery), expectedRows);

            return this;
        }
    }
}
