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
 * {@link ReadRepairQueryTester} for slice queries.
 */
public class ReadRepairSliceQueriesTest extends ReadRepairQueryTester
{
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
}
