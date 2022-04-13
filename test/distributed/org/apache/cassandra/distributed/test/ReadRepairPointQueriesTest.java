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
 * {@link ReadRepairQueryTester} for queries with a restriction on the primary key.
 */
public class ReadRepairPointQueriesTest extends ReadRepairQueryTester
{
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
}
