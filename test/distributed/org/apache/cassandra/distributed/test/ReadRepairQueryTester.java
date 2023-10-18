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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertEquals;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.service.reads.repair.ReadRepairStrategy.NONE;

/**
 * Base class for tests around read repair functionality with different query types and schemas.
 * <p>
 * Each test verifies that its tested query triggers read repair propagating mismatched rows/columns and row/column
 * deletions. They also verify that the selected rows and columns are propagated through read repair on missmatch,
 * and that unselected rows/columns are not repaired.
 * <p>
 * The tests are parameterized for:
 * <ul>
 *     <li>Both {@code NONE} and {@code BLOCKING} read repair stratregies</li>
 *     <li>Data to be repaired residing on the query coordinator or a replica</li>
 *     <li>Data to be repaired residing on memtables or flushed to sstables</li>
 * </ul>
 * <p>
 * All derived tests follow a similar pattern:
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
public abstract class ReadRepairQueryTester extends TestBaseImpl
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

    protected Tester tester(String restriction)
    {
        return new Tester(restriction, cluster, strategy, coordinator, flush, paging);
    }

    protected static class Tester extends ReadRepairTester<Tester>
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

        Tester mutate(String... queries)
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
