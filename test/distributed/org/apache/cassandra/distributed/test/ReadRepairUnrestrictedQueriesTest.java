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
 * {@link ReadRepairQueryTester} for unrestricted queries.
 */
public class ReadRepairUnrestrictedQueriesTest extends ReadRepairQueryTester
{
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
}
