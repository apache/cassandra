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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.utils.ByteBufferUtil;

import static com.google.common.collect.Iterators.toArray;
import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Tests short read protection, the mechanism that ensures distributed queries at read consistency levels > ONE/LOCAL_ONE
 * avoid short reads that might happen when a limit is used and reconciliation accepts less rows than such limit.
 */
@RunWith(Parameterized.class)
public class ShortReadProtectionTest extends TestBaseImpl
{
    private static final int NUM_NODES = 3;
    private static final int[] PAGE_SIZES = new int[]{ 1, 10 };

    private static Cluster cluster;
    private Tester tester;

    /**
     * The consistency level to be used in reads. Internal writes will hit the minimum number of replicas to match this
     * consisstency level. With RF=3, this means one witten replica for CL=ALL, and two for CL=QUORUM.
     */
    @Parameterized.Parameter
    public ConsistencyLevel readConsistencyLevel;

    /**
     * Whether to flush data after mutations.
     */
    @Parameterized.Parameter(1)
    public boolean flush;

    /**
     * Whether paging is used for the distributed queries.
     */
    @Parameterized.Parameter(2)
    public boolean paging;

    @Parameterized.Parameters(name = "{index}: read_cl={0} flush={1} paging={2}")
    public static Collection<Object[]> data()
    {
        List<Object[]> result = new ArrayList<>();
        for (ConsistencyLevel readConsistencyLevel : Arrays.asList(ALL, QUORUM))
            for (boolean flush : BOOLEANS)
                for (boolean paging : BOOLEANS)
                    result.add(new Object[]{ readConsistencyLevel, flush, paging });
        return result;
    }

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build()
                              .withNodes(NUM_NODES)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false))
                              .start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Before
    public void setupTester()
    {
        tester = new Tester(readConsistencyLevel, flush, paging);
    }

    @After
    public void teardownTester()
    {
        tester.dropTable();
    }

    /**
     * Tests SRP for tables with no clustering columns and with a deleted row.
     * <p>
     * See CASSANDRA-13880.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_13880()}.
     */
    @Test
    public void testSkinnyTableWithoutLiveRows()
    {
        tester.createTable("CREATE TABLE %s (id int PRIMARY KEY)")
              .allNodes("INSERT INTO %s (id) VALUES (0) USING TIMESTAMP 0")
              .toNode1("DELETE FROM %s WHERE id = 0")
              .assertRows("SELECT DISTINCT id FROM %s WHERE id = 0")
              .assertRows("SELECT id FROM %s WHERE id = 0 LIMIT 1");
    }

    /**
     * Tests SRP for tables with no clustering columns and with alternated live and deleted rows.
     * <p>
     * See CASSANDRA-13747.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_13747()}.
     */
    @Test
    public void testSkinnyTableWithLiveRows()
    {
        tester.createTable("CREATE TABLE %s (id int PRIMARY KEY)")
              .allNodes(0, 10, i -> format("INSERT INTO %%s (id) VALUES (%d) USING TIMESTAMP 0", i)) // order is 5,1,8,0,2,4,7,6,9,3
              .toNode1("DELETE FROM %s WHERE id IN (1, 0, 4, 6, 3)") // delete every other row
              .assertRows("SELECT DISTINCT token(id), id FROM %s",
                          row(token(5), 5), row(token(8), 8), row(token(2), 2), row(token(7), 7), row(token(9), 9));
    }

    /**
     * Tests SRP for tables with no clustering columns and with complementary deleted rows.
     * <p>
     * See CASSANDRA-13595.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_13595()}.
     */
    @Test
    public void testSkinnyTableWithComplementaryDeletions()
    {
        tester.createTable("CREATE TABLE %s (id int PRIMARY KEY)")
              .allNodes(0, 10, i -> format("INSERT INTO %%s (id) VALUES (%d) USING TIMESTAMP 0", i)) // order is 5,1,8,0,2,4,7,6,9,3
              .toNode1("DELETE FROM %s WHERE id IN (5, 8, 2, 7, 9)") // delete every other row
              .toNode2("DELETE FROM %s WHERE id IN (1, 0, 4, 6)") // delete every other row but the last one
              .assertRows("SELECT id FROM %s LIMIT 1", row(3))
              .assertRows("SELECT DISTINCT id FROM %s LIMIT 1", row(3));
    }

    /**
     * Tests SRP when more than one row is missing.
     * <p>
     * See CASSANDRA-12872.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_12872()}.
     */
    @Test
    public void testMultipleMissedRows()
    {
        tester.createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))")
              .allNodes(0, 4, i -> format("INSERT INTO %%s (pk, ck) VALUES (0, %d) USING TIMESTAMP 0", i))
              .toNode1("DELETE FROM %s WHERE pk = 0 AND ck IN (1, 2, 3)",
                       "INSERT INTO %s (pk, ck) VALUES (0, 5)")
              .toNode2("INSERT INTO %s (pk, ck) VALUES (0, 4)")
              .assertRows("SELECT ck FROM %s WHERE pk = 0 LIMIT 2", row(0), row(4));
    }

    /**
     * Tests SRP with deleted rows at the beginning of the partition and ascending order.
     * <p>
     * See CASSANDRA-9460.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_short_read()} together with
     * {@link #testDescendingOrder()}.
     */
    @Test
    public void testAscendingOrder()
    {
        tester.createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY(k, c))")
              .allNodes(1, 10, i -> format("INSERT INTO %%s (k, c, v) VALUES (0, %d, %d) USING TIMESTAMP 0", i, i * 10))
              .toNode1("DELETE FROM %s WHERE k=0 AND c=1")
              .toNode2("DELETE FROM %s WHERE k=0 AND c=2")
              .toNode3("DELETE FROM %s WHERE k=0 AND c=3")
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c ASC LIMIT 1", row(4, 40))
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c ASC LIMIT 2", row(4, 40), row(5, 50))
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c ASC LIMIT 3", row(4, 40), row(5, 50), row(6, 60))
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c ASC LIMIT 4", row(4, 40), row(5, 50), row(6, 60), row(7, 70));
    }

    /**
     * Tests SRP behaviour with deleted rows at the end of the partition and descending order.
     * <p>
     * See CASSANDRA-9460.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_short_read()} together with
     * {@link #testAscendingOrder()}.
     */
    @Test
    public void testDescendingOrder()
    {
        tester.createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY(k, c))")
              .allNodes(1, 10, i -> format("INSERT INTO %%s (k, c, v) VALUES (0, %d, %d) USING TIMESTAMP 0", i, i * 10))
              .toNode1("DELETE FROM %s WHERE k=0 AND c=7")
              .toNode2("DELETE FROM %s WHERE k=0 AND c=8")
              .toNode3("DELETE FROM %s WHERE k=0 AND c=9")
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c DESC LIMIT 1", row(6, 60))
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c DESC LIMIT 2", row(6, 60), row(5, 50))
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c DESC LIMIT 3", row(6, 60), row(5, 50), row(4, 40))
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c DESC LIMIT 4", row(6, 60), row(5, 50), row(4, 40), row(3, 30));
    }

    /**
     * Test short reads ultimately leaving no rows alive after a partition deletion.
     * <p>
     * See CASSANDRA-4000 and CASSANDRA-8933.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_short_read_delete()} and
     * {@code consistency_test.py:TestConsistency.test_short_read_quorum_delete()}. Note that the
     * {@link #readConsistencyLevel} test parameter ensures that both tests are covered.
     */
    @Test
    public void testDeletePartition()
    {
        tester.createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY(k, c))")
              .allNodes("INSERT INTO %s (k, c, v) VALUES (0, 1, 10) USING TIMESTAMP 0",
                        "INSERT INTO %s (k, c, v) VALUES (0, 2, 20) USING TIMESTAMP 0")
              .toNode2("DELETE FROM %s WHERE k=0")
              .assertRows("SELECT c, v FROM %s WHERE k=0 LIMIT 1");
    }

    /**
     * Test short reads ultimately leaving no rows alive after a partition deletion when there is a static row.
     */
    @Test
    public void testDeletePartitionWithStatic()
    {
        tester.createTable("CREATE TABLE %s (k int, c int, v int, s int STATIC, PRIMARY KEY(k, c))")
              .allNodes("INSERT INTO %s (k, c, v, s) VALUES (0, 1, 10, 100) USING TIMESTAMP 0",
                        "INSERT INTO %s (k, c, v) VALUES (0, 2, 20) USING TIMESTAMP 0")
              .toNode2("DELETE FROM %s WHERE k=0")
              .assertRows("SELECT c, v FROM %s WHERE k=0 LIMIT 1");
    }

    /**
     * Test short reads ultimately leaving no rows alive after a clustering deletion.
     */
    @Test
    public void testDeleteClustering()
    {
        tester.createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY(k, c))")
              .allNodes("INSERT INTO %s (k, c, v) VALUES (0, 1, 10) USING TIMESTAMP 0",
                        "INSERT INTO %s (k, c, v) VALUES (0, 2, 20) USING TIMESTAMP 0")
              .toNode2("DELETE FROM %s WHERE k=0 AND c=1")
              .assertRows("SELECT * FROM %s WHERE k=0 LIMIT 1", row(0, 2, 20))
              .toNode2("DELETE FROM %s WHERE k=0 AND c=2")
              .assertRows("SELECT * FROM %s WHERE k=0 LIMIT 1");
    }

    /**
     * Test short reads ultimately leaving no rows alive after a clustering deletion when there is a static row.
     */
    @Test
    public void testDeleteClusteringWithStatic()
    {
        tester.createTable("CREATE TABLE %s (k int, c int, v int, s int STATIC, PRIMARY KEY(k, c))")
              .allNodes("INSERT INTO %s (k, c, v, s) VALUES (0, 1, 10, 100) USING TIMESTAMP 0",
                        "INSERT INTO %s (k, c, v) VALUES (0, 2, 20) USING TIMESTAMP 0")
              .toNode2("DELETE FROM %s WHERE k=0 AND c=1")
              .assertRows("SELECT k, c, v, s FROM %s WHERE k=0 LIMIT 1", row(0, 2, 20, 100))
              .toNode2("DELETE FROM %s WHERE k=0 AND c=2")
              .assertRows("SELECT k, c, v, s FROM %s WHERE k=0 LIMIT 1", row(0, null, null, 100));
    }

    /**
     * Test GROUP BY with short read protection, particularly when there is a limit and regular row deletions.
     * <p>
     * See CASSANDRA-15459
     */
    @Test
    public void testGroupByRegularRow()
    {
        tester.createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))")
              .toNode1("INSERT INTO %s (pk, ck) VALUES (1, 1) USING TIMESTAMP 0",
                       "DELETE FROM %s WHERE pk=0 AND ck=0",
                       "INSERT INTO %s (pk, ck) VALUES (2, 2) USING TIMESTAMP 0")
              .toNode2("DELETE FROM %s WHERE pk=1 AND ck=1",
                       "INSERT INTO %s (pk, ck) VALUES (0, 0) USING TIMESTAMP 0",
                       "DELETE FROM %s WHERE pk=2 AND ck=2")
              .assertRows("SELECT * FROM %s LIMIT 1")
              .assertRows("SELECT * FROM %s LIMIT 10")
              .assertRows("SELECT * FROM %s GROUP BY pk LIMIT 1")
              .assertRows("SELECT * FROM %s GROUP BY pk LIMIT 10")
              .assertRows("SELECT * FROM %s GROUP BY pk, ck LIMIT 1")
              .assertRows("SELECT * FROM %s GROUP BY pk, ck LIMIT 10");
    }

    /**
     * Test GROUP BY with short read protection, particularly when there is a limit and static row deletions.
     * <p>
     * See CASSANDRA-15459
     */
    @Test
    public void testGroupByStaticRow()
    {
        tester.createTable("CREATE TABLE %s (pk int, ck int, s int static, PRIMARY KEY (pk, ck))")
              .toNode1("INSERT INTO %s (pk, s) VALUES (1, 1) USING TIMESTAMP 0",
                       "INSERT INTO %s (pk, s) VALUES (0, null)",
                       "INSERT INTO %s (pk, s) VALUES (2, 2) USING TIMESTAMP 0")
              .toNode2("INSERT INTO %s (pk, s) VALUES (1, null)",
                       "INSERT INTO %s (pk, s) VALUES (0, 0) USING TIMESTAMP 0",
                       "INSERT INTO %s (pk, s) VALUES (2, null)")
              .assertRows("SELECT * FROM %s LIMIT 1")
              .assertRows("SELECT * FROM %s LIMIT 10")
              .assertRows("SELECT * FROM %s GROUP BY pk LIMIT 1")
              .assertRows("SELECT * FROM %s GROUP BY pk LIMIT 10")
              .assertRows("SELECT * FROM %s GROUP BY pk, ck LIMIT 1")
              .assertRows("SELECT * FROM %s GROUP BY pk, ck LIMIT 10");
    }

    /**
     * See CASSANDRA-13911.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_13911()}.
     */
    @Test
    public void testSkipEarlyTermination()
    {
        tester.createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))")
              .toNode1("INSERT INTO %s (pk, ck) VALUES (0, 0)")
              .toNode2("DELETE FROM %s WHERE pk = 0 AND ck IN (1, 2)")
              .assertRows("SELECT DISTINCT pk FROM %s", row(0));
    }

    /**
     * A regression test to prove that we can no longer rely on {@code !singleResultCounter.isDoneForPartition()} to
     * abort single partition SRP early if a per partition limit is set.
     * <p>
     * See CASSANDRA-13911.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_13911_rows_srp()}.
     */
    @Test
    public void testSkipEarlyTerminationRows()
    {
        tester.createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))")
              .toNode1("INSERT INTO %s (pk, ck) VALUES (0, 0) USING TIMESTAMP 0",
                       "INSERT INTO %s (pk, ck) VALUES (0, 1) USING TIMESTAMP 0",
                       "INSERT INTO %s (pk, ck) VALUES (2, 0) USING TIMESTAMP 0",
                       "DELETE FROM %s USING TIMESTAMP 42 WHERE pk = 2 AND ck = 1")
              .toNode2("INSERT INTO %s (pk, ck) VALUES (0, 2) USING TIMESTAMP 0",
                       "INSERT INTO %s (pk, ck) VALUES (0, 3) USING TIMESTAMP 0",
                       "DELETE FROM %s USING TIMESTAMP 42 WHERE pk = 2 AND ck = 0",
                       "INSERT INTO %s (pk, ck) VALUES (2, 1) USING TIMESTAMP 0",
                       "INSERT INTO %s (pk, ck) VALUES (2, 2) USING TIMESTAMP 0")
              .assertRows("SELECT pk, ck FROM %s PER PARTITION LIMIT 2 LIMIT 3", row(0, 0), row(0, 1), row(2, 2));
    }

    /**
     * A regression test to prove that we can no longer rely on {@code !singleResultCounter.isDone()} to abort ranged
     * partition SRP early if a per partition limit is set.
     * <p>
     * See CASSANDRA-13911.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_13911_partitions_srp()}.
     */
    @Test
    public void testSkipEarlyTerminationPartitions()
    {
        tester.createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))")
              .toNode1("INSERT INTO %s (pk, ck) VALUES (0, 0) USING TIMESTAMP 0",
                       "INSERT INTO %s (pk, ck) VALUES (0, 1) USING TIMESTAMP 0",
                       "DELETE FROM %s USING TIMESTAMP 42 WHERE pk = 2 AND ck IN  (0, 1)")
              .toNode2("INSERT INTO %s (pk, ck) VALUES (0, 2) USING TIMESTAMP 0",
                       "INSERT INTO %s (pk, ck) VALUES (0, 3) USING TIMESTAMP 0",
                       "INSERT INTO %s (pk, ck) VALUES (2, 0) USING TIMESTAMP 0",
                       "INSERT INTO %s (pk, ck) VALUES (2, 1) USING TIMESTAMP 0",
                       "INSERT INTO %s (pk, ck) VALUES (4, 0) USING TIMESTAMP 0",
                       "INSERT INTO %s (pk, ck) VALUES (4, 1) USING TIMESTAMP 0")
              .assertRows("SELECT pk, ck FROM %s PER PARTITION LIMIT 2 LIMIT 4",
                          row(0, 0), row(0, 1), row(4, 0), row(4, 1));
    }

    private static long token(int key)
    {
        return (long) Murmur3Partitioner.instance.getToken(ByteBufferUtil.bytes(key)).getTokenValue();
    }

    private static class Tester
    {
        private static final AtomicInteger seqNumber = new AtomicInteger();

        private final ConsistencyLevel readConsistencyLevel;
        private final boolean flush, paging;
        private final String qualifiedTableName;

        private boolean flushed = false;

        private Tester(ConsistencyLevel readConsistencyLevel, boolean flush, boolean paging)
        {
            this.readConsistencyLevel = readConsistencyLevel;
            this.flush = flush;
            this.paging = paging;
            qualifiedTableName = KEYSPACE + ".t_" + seqNumber.getAndIncrement();

            assert readConsistencyLevel == ALL || readConsistencyLevel == QUORUM
            : "Only ALL and QUORUM consistency levels are supported";
        }

        private Tester createTable(String query)
        {
            cluster.schemaChange(format(query) + " WITH read_repair='NONE'");
            return this;
        }

        private Tester allNodes(int startInclusive, int endExclusive, Function<Integer, String> querySupplier)
        {
            IntStream.range(startInclusive, endExclusive).mapToObj(querySupplier::apply).forEach(this::allNodes);
            return this;
        }

        private Tester allNodes(String... queries)
        {
            for (String query : queries)
                allNodes(query);
            return this;
        }

        private Tester allNodes(String query)
        {
            cluster.coordinator(1).execute(format(query), ALL);
            return this;
        }

        /**
         * Internally runs the specified write queries in the first node. If the {@link #readConsistencyLevel} is
         * QUORUM, then the write will also be internally done in the second replica, to simulate a QUORUM write.
         */
        private Tester toNode1(String... queries)
        {
            return toNode(1, queries);
        }

        /**
         * Internally runs the specified write queries in the second node. If the {@link #readConsistencyLevel} is
         * QUORUM, then the write will also be internally done in the third replica, to simulate a QUORUM write.
         */
        private Tester toNode2(String... queries)
        {
            return toNode(2, queries);
        }

        /**
         * Internally runs the specified write queries in the third node. If the {@link #readConsistencyLevel} is
         * QUORUM, then the write will also be internally done in the first replica, to simulate a QUORUM write.
         */
        private Tester toNode3(String... queries)
        {
            return toNode(3, queries);
        }

        /**
         * Internally runs the specified write queries in the specified node. If the {@link #readConsistencyLevel} is
         * QUORUM the write will also be internally done in the next replica in the ring, to simulate a QUORUM write.
         */
        private Tester toNode(int node, String... queries)
        {
            IInvokableInstance replica = cluster.get(node);
            IInvokableInstance nextReplica = readConsistencyLevel == QUORUM
                                             ? cluster.get(node == NUM_NODES ? 1 : node + 1)
                                             : null;

            for (String query : queries)
            {
                String formattedQuery = format(query);
                replica.executeInternal(formattedQuery);

                if (nextReplica != null)
                    nextReplica.executeInternal(formattedQuery);
            }

            return this;
        }

        private Tester assertRows(String query, Object[]... expectedRows)
        {
            if (flush && !flushed)
            {
                cluster.stream().forEach(n -> n.flush(KEYSPACE));
                flushed = true;
            }

            String formattedQuery = format(query);
            cluster.coordinators().forEach(coordinator -> {
                if (paging)
                {
                    for (int fetchSize : PAGE_SIZES)
                    {
                        Iterator<Object[]> actualRows = coordinator.executeWithPaging(formattedQuery, readConsistencyLevel, fetchSize);
                        AssertUtils.assertRows(toArray(actualRows, Object[].class),  expectedRows);
                    }
                }
                else
                {
                    Object[][] actualRows = coordinator.execute(formattedQuery, readConsistencyLevel);
                    AssertUtils.assertRows(actualRows, expectedRows);
                }
            });

            return this;
        }

        private String format(String query)
        {
            return String.format(query, qualifiedTableName);
        }

        private void dropTable()
        {
            cluster.schemaChange(format("DROP TABLE IF EXISTS %s"));
        }
    }
}
