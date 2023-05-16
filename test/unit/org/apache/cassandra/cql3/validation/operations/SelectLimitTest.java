/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cql3.validation.operations;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.service.StorageService;

public class SelectLimitTest extends CQLTester
{
    // This method will be ran instead of the CQLTester#setUpClass
    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();

        StorageService.instance.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);

        prepareServer();
    }

    /**
     * Test limit queries on a sparse table,
     * migrated from cql_tests.py:TestCQL.limit_sparse_test()
     */
    @Test
    public void testSparseTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid int, url text, day int, month text, year int, PRIMARY KEY (userid, url))");

        for (int i = 0; i < 100; i++)
            for (String tld : new String[] { "com", "org", "net" })
                execute("INSERT INTO %s (userid, url, day, month, year) VALUES (?, ?, 1, 'jan', 2012)", i, String.format("http://foo.%s", tld));

        assertRowCount(execute("SELECT * FROM %s LIMIT 4"), 4);

    }

    @Test
    public void testPerPartitionLimit() throws Throwable
    {
        String query = "CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))";

        createTable(query);

        for (int i = 0; i < 5; i++)
        {
            for (int j = 0; j < 5; j++)
            {
                execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", i, j, j);
            }
        }

        assertInvalidMessage("LIMIT must be strictly positive",
                             "SELECT * FROM %s PER PARTITION LIMIT ?", 0);
        assertInvalidMessage("LIMIT must be strictly positive",
                             "SELECT * FROM %s PER PARTITION LIMIT ?", -1);

        assertRowsIgnoringOrder(execute("SELECT * FROM %s PER PARTITION LIMIT ?", 2),
                                row(0, 0, 0),
                                row(0, 1, 1),
                                row(1, 0, 0),
                                row(1, 1, 1),
                                row(2, 0, 0),
                                row(2, 1, 1),
                                row(3, 0, 0),
                                row(3, 1, 1),
                                row(4, 0, 0),
                                row(4, 1, 1));

        // Combined Per Partition and "global" limit
        assertRowCount(execute("SELECT * FROM %s PER PARTITION LIMIT ? LIMIT ?", 2, 6),
                       6);

        // odd amount of results
        assertRowCount(execute("SELECT * FROM %s PER PARTITION LIMIT ? LIMIT ?", 2, 5),
                       5);

        // IN query
        assertRows(execute("SELECT * FROM %s WHERE a IN (2,3) PER PARTITION LIMIT ?", 2),
                   row(2, 0, 0),
                   row(2, 1, 1),
                   row(3, 0, 0),
                   row(3, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a IN (2,3) PER PARTITION LIMIT ? LIMIT 3", 2),
                   row(2, 0, 0),
                   row(2, 1, 1),
                   row(3, 0, 0));

        assertRows(execute("SELECT * FROM %s WHERE a IN (1,2,3) PER PARTITION LIMIT ? LIMIT 3", 2),
                   row(1, 0, 0),
                   row(1, 1, 1),
                   row(2, 0, 0));

        // with restricted partition key
        assertRows(execute("SELECT * FROM %s WHERE a = ? PER PARTITION LIMIT ?", 2, 3),
                   row(2, 0, 0),
                   row(2, 1, 1),
                   row(2, 2, 2));

        // with ordering
        assertRows(execute("SELECT * FROM %s WHERE a IN (3, 2) ORDER BY b DESC PER PARTITION LIMIT ?", 2),
                   row(2, 4, 4),
                   row(3, 4, 4),
                   row(2, 3, 3),
                   row(3, 3, 3));

        assertRows(execute("SELECT * FROM %s WHERE a IN (3, 2) ORDER BY b DESC PER PARTITION LIMIT ? LIMIT ?", 3, 4),
                   row(2, 4, 4),
                   row(3, 4, 4),
                   row(2, 3, 3),
                   row(3, 3, 3));

        assertRows(execute("SELECT * FROM %s WHERE a = ? ORDER BY b DESC PER PARTITION LIMIT ?", 2, 3),
                   row(2, 4, 4),
                   row(2, 3, 3),
                   row(2, 2, 2));

        // with filtering
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b > ? PER PARTITION LIMIT ? ALLOW FILTERING", 2, 0, 2),
                   row(2, 1, 1),
                   row(2, 2, 2));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b > ? ORDER BY b DESC PER PARTITION LIMIT ? ALLOW FILTERING", 2, 2, 2),
                   row(2, 4, 4),
                   row(2, 3, 3));

        assertInvalidMessage("PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries",
                             "SELECT DISTINCT a FROM %s PER PARTITION LIMIT ?", 3);
        assertInvalidMessage("PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries",
                             "SELECT DISTINCT a FROM %s PER PARTITION LIMIT ? LIMIT ?", 3, 4);
        assertInvalidMessage("PER PARTITION LIMIT is not allowed with aggregate queries.",
                             "SELECT COUNT(*) FROM %s PER PARTITION LIMIT ?", 3);
    }

    @Test
    public void testPerPartitionLimitWithStaticDataAndPaging() throws Throwable
    {
        String query = "CREATE TABLE %s (a int, b int, s int static, c int, PRIMARY KEY (a, b))";

        createTable(query);

        for (int i = 0; i < 5; i++)
        {
            execute("INSERT INTO %s (a, s) VALUES (?, ?)", i, i);
        }

        for (int pageSize = 1; pageSize < 8; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s PER PARTITION LIMIT 2", pageSize),
                          row(0, null, 0, null),
                          row(1, null, 1, null),
                          row(2, null, 2, null),
                          row(3, null, 3, null),
                          row(4, null, 4, null));

            // Combined Per Partition and "global" limit
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s PER PARTITION LIMIT 2 LIMIT 4", pageSize),
                          row(0, null, 0, null),
                          row(1, null, 1, null),
                          row(2, null, 2, null),
                          row(3, null, 3, null));

            // odd amount of results
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s PER PARTITION LIMIT 2 LIMIT 3", pageSize),
                          row(0, null, 0, null),
                          row(1, null, 1, null),
                          row(2, null, 2, null));

            // IN query
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a IN (1,3,4) PER PARTITION LIMIT 2", pageSize),
                          row(1, null, 1, null),
                          row(3, null, 3, null),
                          row(4, null, 4, null));

            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a IN (1,3,4) PER PARTITION LIMIT 2 LIMIT 2",
                                               pageSize),
                          row(1, null, 1, null),
                          row(3, null, 3, null));

            // with restricted partition key
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 2 PER PARTITION LIMIT 3", pageSize),
                          row(2, null, 2, null));

            // with ordering
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 2 ORDER BY b DESC PER PARTITION LIMIT 3",
                                               pageSize),
                          row(2, null, 2, null));

            // with filtering
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 2 AND s > 0 PER PARTITION LIMIT 2 ALLOW FILTERING",
                                               pageSize),
                          row(2, null, 2, null));
        }

        for (int i = 0; i < 5; i++)
        {
            if (i != 1)
            {
                for (int j = 0; j < 5; j++)
                {
                    execute("INSERT INTO %s (a, b, s, c) VALUES (?, ?, ?, ?)", i, j, i, j);
                }
            }
        }

        assertInvalidMessage("LIMIT must be strictly positive",
                             "SELECT * FROM %s PER PARTITION LIMIT ?", 0);
        assertInvalidMessage("LIMIT must be strictly positive",
                             "SELECT * FROM %s PER PARTITION LIMIT ?", -1);

        for (int pageSize = 1; pageSize < 8; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s PER PARTITION LIMIT 2", pageSize),
                          row(0, 0, 0, 0),
                          row(0, 1, 0, 1),
                          row(1, null, 1, null),
                          row(2, 0, 2, 0),
                          row(2, 1, 2, 1),
                          row(3, 0, 3, 0),
                          row(3, 1, 3, 1),
                          row(4, 0, 4, 0),
                          row(4, 1, 4, 1));

            // Combined Per Partition and "global" limit
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s PER PARTITION LIMIT 2 LIMIT 4", pageSize),
                          row(0, 0, 0, 0),
                          row(0, 1, 0, 1),
                          row(1, null, 1, null),
                          row(2, 0, 2, 0));

            // odd amount of results
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s PER PARTITION LIMIT 2 LIMIT 5", pageSize),
                          row(0, 0, 0, 0),
                          row(0, 1, 0, 1),
                          row(1, null, 1, null),
                          row(2, 0, 2, 0),
                          row(2, 1, 2, 1));

            // IN query
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a IN (2,3) PER PARTITION LIMIT 2", pageSize),
                          row(2, 0, 2, 0),
                          row(2, 1, 2, 1),
                          row(3, 0, 3, 0),
                          row(3, 1, 3, 1));

            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a IN (2,3) PER PARTITION LIMIT 2 LIMIT 3",
                                               pageSize),
                          row(2, 0, 2, 0),
                          row(2, 1, 2, 1),
                          row(3, 0, 3, 0));

            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a IN (1,2,3) PER PARTITION LIMIT 2 LIMIT 3",
                                               pageSize),
                          row(1, null, 1, null),
                          row(2, 0, 2, 0),
                          row(2, 1, 2, 1));

            // with restricted partition key
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 2 PER PARTITION LIMIT 3", pageSize),
                          row(2, 0, 2, 0),
                          row(2, 1, 2, 1),
                          row(2, 2, 2, 2));

            // with ordering
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 2 ORDER BY b DESC PER PARTITION LIMIT 3",
                                               pageSize),
                          row(2, 4, 2, 4),
                          row(2, 3, 2, 3),
                          row(2, 2, 2, 2));

            // with filtering
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 2 AND b > 0 PER PARTITION LIMIT 2 ALLOW FILTERING",
                                               pageSize),
                          row(2, 1, 2, 1),
                          row(2, 2, 2, 2));

            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 2 AND b > 2 ORDER BY b DESC PER PARTITION LIMIT 2 ALLOW FILTERING",
                                               pageSize),
                          row(2, 4, 2, 4),
                          row(2, 3, 2, 3));
        }

        assertInvalidMessage("PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries",
                             "SELECT DISTINCT a FROM %s PER PARTITION LIMIT ?", 3);
        assertInvalidMessage("PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries",
                             "SELECT DISTINCT a FROM %s PER PARTITION LIMIT ? LIMIT ?", 3, 4);
        assertInvalidMessage("PER PARTITION LIMIT is not allowed with aggregate queries.",
                             "SELECT COUNT(*) FROM %s PER PARTITION LIMIT ?", 3);
    }

    @Test
    public void testLimitWithDeletedRowsAndStaticColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v int, s int static, PRIMARY KEY (pk, c))");

        execute("INSERT INTO %s (pk, c, v, s) VALUES (1, -1, 1, 1)");
        execute("INSERT INTO %s (pk, c, v, s) VALUES (2, -1, 1, 1)");
        execute("INSERT INTO %s (pk, c, v, s) VALUES (3, -1, 1, 1)");
        execute("INSERT INTO %s (pk, c, v, s) VALUES (4, -1, 1, 1)");
        execute("INSERT INTO %s (pk, c, v, s) VALUES (5, -1, 1, 1)");

        assertRows(execute("SELECT * FROM %s"),
                   row(1, -1, 1, 1),
                   row(2, -1, 1, 1),
                   row(3, -1, 1, 1),
                   row(4, -1, 1, 1),
                   row(5, -1, 1, 1));

        execute("DELETE FROM %s WHERE pk = 2");

        assertRows(execute("SELECT * FROM %s"),
                   row(1, -1, 1, 1),
                   row(3, -1, 1, 1),
                   row(4, -1, 1, 1),
                   row(5, -1, 1, 1));

        assertRows(execute("SELECT * FROM %s LIMIT 2"),
                   row(1, -1, 1, 1),
                   row(3, -1, 1, 1));
    }

    @Test
    public void testFilteringOnClusteringColumnsWithLimitAndStaticColumns() throws Throwable
    {
        // With only one clustering column
        createTable("CREATE TABLE %s (a int, b int, s int static, c int, primary key (a, b))"
             + " WITH caching = {'keys': 'ALL', 'rows_per_partition' : 'ALL'}");

        for (int i = 0; i < 4; i++)
        {
            execute("INSERT INTO %s (a, s) VALUES (?, ?)", i, i);
                for (int j = 0; j < 3; j++)
                    if (!((i == 0 || i == 3) && j == 1))
                        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", i, j, i + j);
        }

        beforeAndAfterFlush(() ->
        {
            assertRows(execute("SELECT * FROM %s"),
                       row(0, 0, 0, 0),
                       row(0, 2, 0, 2),
                       row(1, 0, 1, 1),
                       row(1, 1, 1, 2),
                       row(1, 2, 1, 3),
                       row(2, 0, 2, 2),
                       row(2, 1, 2, 3),
                       row(2, 2, 2, 4),
                       row(3, 0, 3, 3),
                       row(3, 2, 3, 5));

            assertRows(execute("SELECT * FROM %s WHERE b = 1 ALLOW FILTERING"),
                       row(1, 1, 1, 2),
                       row(2, 1, 2, 3));

            // The problem was that the static row of the partition 0 used to be only filtered in SelectStatement and was
            // by consequence counted as a row. In which case the query was returning one row less.
            assertRows(execute("SELECT * FROM %s WHERE b = 1 LIMIT 2 ALLOW FILTERING"),
                       row(1, 1, 1, 2),
                       row(2, 1, 2, 3));

            assertRows(execute("SELECT * FROM %s WHERE b >= 1 AND b <= 1 LIMIT 2 ALLOW FILTERING"),
                       row(1, 1, 1, 2),
                       row(2, 1, 2, 3));

            // Test with paging
            for (int pageSize = 1; pageSize < 4; pageSize++)
            {
                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE b = 1 LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 2),
                              row(2, 1, 2, 3));

                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE b >= 1 AND b <= 1 LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 2),
                              row(2, 1, 2, 3));

                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE b = 1 GROUP BY a LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 2),
                              row(2, 1, 2, 3));

                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE b >= 1 AND b <= 1 GROUP BY a LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 2),
                              row(2, 1, 2, 3));
            }
        });

        assertRows(execute("SELECT * FROM %s WHERE a IN (0, 1, 2, 3) AND b = 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3));

        assertRows(execute("SELECT * FROM %s WHERE a IN (0, 1, 2, 3) AND b >= 1 AND b <= 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3));

        assertRows(execute("SELECT * FROM %s WHERE a IN (0, 1, 2, 3) AND b = 1 ORDER BY b DESC LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3));

        assertRows(execute("SELECT * FROM %s WHERE a IN (0, 1, 2, 3) AND b >= 1 AND b <= 1 ORDER BY b DESC LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3));

        execute("SELECT * FROM %s WHERE a IN (0, 1, 2, 3)"); // Load all data in the row cache

        // Partition range queries
        assertRows(execute("SELECT * FROM %s WHERE b = 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3));

        assertRows(execute("SELECT * FROM %s WHERE b >= 1 AND b <= 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3));

        // Multiple partitions queries
        assertRows(execute("SELECT * FROM %s WHERE a IN (0, 1, 2) AND b = 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3));

        assertRows(execute("SELECT * FROM %s WHERE a IN (0, 1, 2) AND b >= 1 AND b <= 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3));

        // Test with paging
        for (int pageSize = 1; pageSize < 4; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE b = 1 LIMIT 2 ALLOW FILTERING", pageSize),
                          row(1, 1, 1, 2),
                          row(2, 1, 2, 3));

            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE b >= 1 AND b <= 1 LIMIT 2 ALLOW FILTERING", pageSize),
                          row(1, 1, 1, 2),
                          row(2, 1, 2, 3));
        }

        // With multiple clustering columns
        createTable("CREATE TABLE %s (a int, b int, c int, s int static, d int, primary key (a, b, c))"
           + " WITH caching = {'keys': 'ALL', 'rows_per_partition' : 'ALL'}");

        for (int i = 0; i < 3; i++)
        {
            execute("INSERT INTO %s (a, s) VALUES (?, ?)", i, i);
                for (int j = 0; j < 3; j++)
                    if (!(i == 0 && j == 1))
                        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, j, i + j);
        }

        beforeAndAfterFlush(() ->
        {
            assertRows(execute("SELECT * FROM %s"),
                       row(0, 0, 0, 0, 0),
                       row(0, 2, 2, 0, 2),
                       row(1, 0, 0, 1, 1),
                       row(1, 1, 1, 1, 2),
                       row(1, 2, 2, 1, 3),
                       row(2, 0, 0, 2, 2),
                       row(2, 1, 1, 2, 3),
                       row(2, 2, 2, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE b = 1 ALLOW FILTERING"),
                       row(1, 1, 1, 1, 2),
                       row(2, 1, 1, 2, 3));

            assertRows(execute("SELECT * FROM %s WHERE b IN (1, 2, 3, 4) AND c >= 1 AND c <= 1 LIMIT 2 ALLOW FILTERING"),
                       row(1, 1, 1, 1, 2),
                       row(2, 1, 1, 2, 3));

            // Test with paging
            for (int pageSize = 1; pageSize < 4; pageSize++)
            {
                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE b = 1 LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 1, 2),
                              row(2, 1, 1, 2, 3));

                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE b IN (1, 2, 3, 4) AND c >= 1 AND c <= 1 LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 1, 2),
                              row(2, 1, 1, 2, 3));

                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE b = 1 GROUP BY a, b LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 1, 2),
                              row(2, 1, 1, 2, 3));

                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE b IN (1, 2, 3, 4) AND c >= 1 AND c <= 1 GROUP BY a, b LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 1, 1, 2),
                              row(2, 1, 1, 2, 3));
            }
        });

        execute("SELECT * FROM %s WHERE a IN (0, 1, 2)"); // Load data in the row cache

        assertRows(execute("SELECT * FROM %s WHERE b = 1 ALLOW FILTERING"),
                   row(1, 1, 1, 1, 2),
                   row(2, 1, 1, 2, 3));

        assertRows(execute("SELECT * FROM %s WHERE b IN (1, 2, 3, 4) AND c >= 1 AND c <= 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 1, 2),
                   row(2, 1, 1, 2, 3));

        assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2, 3, 4) AND b = 1 ALLOW FILTERING"),
                   row(1, 1, 1, 1, 2),
                   row(2, 1, 1, 2, 3));

        assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2, 3, 4) AND b IN (1, 2, 3, 4) AND c >= 1 AND c <= 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 1, 2),
                   row(2, 1, 1, 2, 3));

        // Test with paging
        for (int pageSize = 1; pageSize < 4; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE b = 1 LIMIT 2 ALLOW FILTERING", pageSize),
                          row(1, 1, 1, 1, 2),
                          row(2, 1, 1, 2, 3));

            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE b IN (1, 2, 3, 4) AND c >= 1 AND c <= 1 LIMIT 2 ALLOW FILTERING", pageSize),
                          row(1, 1, 1, 1, 2),
                          row(2, 1, 1, 2, 3));
        }
    }

    @Test
    public void testIndexOnRegularColumnWithPartitionWithoutRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, s int static, v int, PRIMARY KEY(pk, c))");
        createIndex("CREATE INDEX ON %s (v)");

        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 1, 9, 1);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 2, 9, 2);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 3, 1, 9, 1);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 4, 1, 9, 1);
        flush();

        assertRows(execute("SELECT * FROM %s WHERE v = ?", 1),
                   row(1, 1, 9, 1),
                   row(3, 1, 9, 1),
                   row(4, 1, 9, 1));

        execute("DELETE FROM %s WHERE pk = ? and c = ?", 3, 1);

        // Test without paging
        assertRows(execute("SELECT * FROM %s WHERE v = ? LIMIT 2", 1),
                   row(1, 1, 9, 1),
                   row(4, 1, 9, 1));

        assertRows(execute("SELECT * FROM %s WHERE v = ? GROUP BY pk LIMIT 2", 1),
                   row(1, 1, 9, 1),
                   row(4, 1, 9, 1));

        // Test with paging
        for (int pageSize = 1; pageSize < 4; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE v = 1 LIMIT 2", pageSize),
                          row(1, 1, 9, 1),
                          row(4, 1, 9, 1));

            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE v = 1 GROUP BY pk LIMIT 2", pageSize),
                          row(1, 1, 9, 1),
                          row(4, 1, 9, 1));
        }
    }

    @Test
    public void testFilteringWithPartitionWithoutRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, s int static, v int, PRIMARY KEY(pk, c))");

        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 1, 9, 1);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 1, 2, 9, 2);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 3, 1, 9, 1);
        execute("INSERT INTO %s (pk, c, s, v) VALUES (?, ?, ?, ?)", 4, 1, 9, 1);
        flush();

        assertRows(execute("SELECT * FROM %s WHERE v = ? ALLOW FILTERING", 1),
                   row(1, 1, 9, 1),
                   row(3, 1, 9, 1),
                   row(4, 1, 9, 1));

        execute("DELETE FROM %s WHERE pk = ? and c = ?", 3, 1);

        // Test without paging
        assertRows(execute("SELECT * FROM %s WHERE v = ? LIMIT 2 ALLOW FILTERING", 1),
                   row(1, 1, 9, 1),
                   row(4, 1, 9, 1));

        assertRows(execute("SELECT * FROM %s WHERE pk IN ? AND v = ? LIMIT 2 ALLOW FILTERING", list(1, 3, 4), 1),
                   row(1, 1, 9, 1),
                   row(4, 1, 9, 1));

        // Test with paging
        for (int pageSize = 1; pageSize < 4; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE v = 1 LIMIT 2 ALLOW FILTERING", pageSize),
                          row(1, 1, 9, 1),
                          row(4, 1, 9, 1));

            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE pk IN (1, 3, 4) AND v = 1 LIMIT 2 ALLOW FILTERING", pageSize),
                          row(1, 1, 9, 1),
                          row(4, 1, 9, 1));
        }
    }
}
