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

import org.junit.Test;

import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * {@link ReadRepairQueryTester} for queries using {@code IN} restrictions.
 */
public class ReadRepairInQueriesTest extends ReadRepairQueryTester
{
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
}
