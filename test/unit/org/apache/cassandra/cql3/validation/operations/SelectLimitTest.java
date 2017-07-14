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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class SelectLimitTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.setPartitioner(ByteOrderedPartitioner.instance);
    }

    /**
     * Test limit across a partition range, requires byte ordered partitioner,
     * migrated from cql_tests.py:TestCQL.limit_range_test()
     */
    @Test
    public void testPartitionRange() throws Throwable
    {
        createTable("CREATE TABLE %s (userid int, url text, time bigint, PRIMARY KEY (userid, url)) WITH COMPACT STORAGE");

        for (int i = 0; i < 100; i++)
            for (String tld : new String[] { "com", "org", "net" })
                execute("INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", i, String.format("http://foo.%s", tld), 42L);

        assertRows(execute("SELECT * FROM %s WHERE token(userid) >= token(2) LIMIT 1"),
                   row(2, "http://foo.com", 42L));

        assertRows(execute("SELECT * FROM %s WHERE token(userid) > token(2) LIMIT 1"),
                   row(3, "http://foo.com", 42L));
    }

    /**
     * Test limit across a column range,
     * migrated from cql_tests.py:TestCQL.limit_multiget_test()
     */
    @Test
    public void testColumnRange() throws Throwable
    {
        createTable("CREATE TABLE %s (userid int, url text, time bigint, PRIMARY KEY (userid, url)) WITH COMPACT STORAGE");

        for (int i = 0; i < 100; i++)
            for (String tld : new String[] { "com", "org", "net" })
                execute("INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", i, String.format("http://foo.%s", tld), 42L);

        // Check that we do limit the output to 1 *and* that we respect query
        // order of keys (even though 48 is after 2)
        assertRows(execute("SELECT * FROM %s WHERE userid IN (48, 2) LIMIT 1"),
                   row(2, "http://foo.com", 42L));

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

    /**
     * Check for #7052 bug,
     * migrated from cql_tests.py:TestCQL.limit_compact_table()
     */
    @Test
    public void testLimitInCompactTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY (k, v) ) WITH COMPACT STORAGE ");

        for (int i = 0; i < 4; i++)
            for (int j = 0; j < 4; j++)
                execute("INSERT INTO %s(k, v) VALUES (?, ?)", i, j);

        assertRows(execute("SELECT v FROM %s WHERE k=0 AND v > 0 AND v <= 4 LIMIT 2"),
                   row(1),
                   row(2));
        assertRows(execute("SELECT v FROM %s WHERE k=0 AND v > -1 AND v <= 4 LIMIT 2"),
                   row(0),
                   row(1));
        assertRows(execute("SELECT * FROM %s WHERE k IN (0, 1, 2) AND v > 0 AND v <= 4 LIMIT 2"),
                   row(0, 1),
                   row(0, 2));
        assertRows(execute("SELECT * FROM %s WHERE k IN (0, 1, 2) AND v > -1 AND v <= 4 LIMIT 2"),
                   row(0, 0),
                   row(0, 1));
        assertRows(execute("SELECT * FROM %s WHERE k IN (0, 1, 2) AND v > 0 AND v <= 4 LIMIT 6"),
                   row(0, 1),
                   row(0, 2),
                   row(0, 3),
                   row(1, 1),
                   row(1, 2),
                   row(1, 3));

        // strict bound (v > 1) over a range of partitions is not supported for compact storage if limit is provided
        assertInvalidThrow(InvalidRequestException.class, "SELECT * FROM %s WHERE v > 1 AND v <= 3 LIMIT 6 ALLOW FILTERING");
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

        for (boolean forceFlush : new boolean[]{false, true})
        {
            if (forceFlush)
                flush();

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
            }
        }

        assertRows(execute("SELECT * FROM %s WHERE a IN (0, 1, 2, 3) AND b = 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3));

        assertRows(execute("SELECT * FROM %s WHERE a IN (0, 1, 2, 3) AND b >= 1 AND b <= 1 LIMIT 2 ALLOW FILTERING"),
                   row(1, 1, 1, 2),
                   row(2, 1, 2, 3));

        assertRows(execute("SELECT * FROM %s WHERE a IN (0, 1, 2, 3) AND b = 1 ORDER BY b DESC LIMIT 2 ALLOW FILTERING"),
                   row(2, 1, 2, 3),
                   row(1, 1, 1, 2));

        assertRows(execute("SELECT * FROM %s WHERE a IN (0, 1, 2, 3) AND b >= 1 AND b <= 1 ORDER BY b DESC LIMIT 2 ALLOW FILTERING"),
                   row(2, 1, 2, 3),
                   row(1, 1, 1, 2));

        execute("SELECT * FROM %s WHERE a IN (0, 1, 2, 3)"); // Load all data in the row cache

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

        for (boolean forceFlush : new boolean[]{false, true})
        {
            if (forceFlush)
                flush();

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
            }
        }

        execute("SELECT * FROM %s WHERE a IN (0, 1, 2)"); // Load data in the row cache

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

        execute("DELETE FROM %s WHERE pk = ? AND c = ?", 3, 1);

        // Test without paging
        assertRows(execute("SELECT * FROM %s WHERE v = ?", 1),
                   row(1, 1, 9, 1),
                   row(4, 1, 9, 1));

        assertRows(execute("SELECT * FROM %s WHERE v = ? LIMIT 2", 1),
                   row(1, 1, 9, 1),
                   row(4, 1, 9, 1));

        // Test with paging
        for (int pageSize = 1; pageSize < 4; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE v = 1", pageSize),
                          row(1, 1, 9, 1),
                          row(4, 1, 9, 1));

            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE v = 1 LIMIT 2", pageSize),
                          row(1, 1, 9, 1),
                          row(4, 1, 9, 1));
        }
    }
}
