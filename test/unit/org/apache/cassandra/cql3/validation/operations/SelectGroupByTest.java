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
package org.apache.cassandra.cql3.validation.operations;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

public class SelectGroupByTest extends CQLTester
{
    @Test
    public void testGroupByWithoutPaging() throws Throwable
    {
        for (String compactOption : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, primary key (a, b, c, d))"
                    + compactOption);

            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 3, 6)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 3, 2, 12, 24)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, 2, 12, 24)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, 2, 6, 12)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (2, 2, 3, 3, 6)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (2, 4, 3, 6, 12)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (3, 3, 2, 12, 24)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (4, 8, 2, 12, 24)");

            // Makes sure that we have some tombstones
            execute("DELETE FROM %s WHERE a = 1 AND b = 3 AND c = 2 AND d = 12");
            execute("DELETE FROM %s WHERE a = 3");

            // Range queries
            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a"),
                       row(1, 2, 6, 4L, 24),
                       row(2, 2, 6, 2L, 12),
                       row(4, 8, 24, 1L, 24));

            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b"),
                       row(1, 2, 6, 2L, 12),
                       row(1, 4, 12, 2L, 24),
                       row(2, 2, 6, 1L, 6),
                       row(2, 4, 12, 1L, 12),
                       row(4, 8, 24, 1L, 24));

            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE b = 2 GROUP BY a, b ALLOW FILTERING"),
                       row(1, 2, 6, 2L, 12),
                       row(2, 2, 6, 1L, 6));

            assertEmpty(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE b IN () GROUP BY a, b ALLOW FILTERING"));

            // Range queries without aggregates
            assertRows(execute("SELECT a, b, c, d FROM %s GROUP BY a, b, c"),
                       row(1, 2, 1, 3),
                       row(1, 2, 2, 6),
                       row(1, 4, 2, 6),
                       row(2, 2, 3, 3),
                       row(2, 4, 3, 6),
                       row(4, 8, 2, 12));

            assertRows(execute("SELECT a, b, c, d FROM %s GROUP BY a, b"),
                       row(1, 2, 1, 3),
                       row(1, 4, 2, 6),
                       row(2, 2, 3, 3),
                       row(2, 4, 3, 6),
                       row(4, 8, 2, 12));

            // Range queries with wildcard
            assertRows(execute("SELECT * FROM %s GROUP BY a, b, c"),
                       row(1, 2, 1, 3, 6),
                       row(1, 2, 2, 6, 12),
                       row(1, 4, 2, 6, 12),
                       row(2, 2, 3, 3, 6),
                       row(2, 4, 3, 6, 12),
                       row(4, 8, 2, 12, 24));

            assertRows(execute("SELECT * FROM %s GROUP BY a, b"),
                       row(1, 2, 1, 3, 6),
                       row(1, 4, 2, 6, 12),
                       row(2, 2, 3, 3, 6),
                       row(2, 4, 3, 6, 12),
                       row(4, 8, 2, 12, 24));

            // Range query with LIMIT
            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b LIMIT 2"),
                       row(1, 2, 6, 2L, 12),
                       row(1, 4, 12, 2L, 24));

            // Range queries with PER PARTITION LIMIT
            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                       row(1, 2, 6, 2L, 12),
                       row(2, 2, 6, 1L, 6),
                       row(4, 8, 24, 1L, 24));

            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a PER PARTITION LIMIT 2"),
                       row(1, 2, 6, 4L, 24),
                       row(2, 2, 6, 2L, 12),
                       row(4, 8, 24, 1L, 24));

            // Range query with PER PARTITION LIMIT and LIMIT
            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2"),
                       row(1, 2, 6, 2L, 12),
                       row(2, 2, 6, 1L, 6));

            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a PER PARTITION LIMIT 2"),
                       row(1, 2, 6, 4L, 24),
                       row(2, 2, 6, 2L, 12),
                       row(4, 8, 24, 1L, 24));

            // Range queries without aggregates and with LIMIT
            assertRows(execute("SELECT a, b, c, d FROM %s GROUP BY a, b, c LIMIT 3"),
                       row(1, 2, 1, 3),
                       row(1, 2, 2, 6),
                       row(1, 4, 2, 6));

            assertRows(execute("SELECT a, b, c, d FROM %s GROUP BY a, b LIMIT 3"),
                       row(1, 2, 1, 3),
                       row(1, 4, 2, 6),
                       row(2, 2, 3, 3));

            // Range queries with wildcard and with LIMIT
            assertRows(execute("SELECT * FROM %s GROUP BY a, b, c LIMIT 3"),
                       row(1, 2, 1, 3, 6),
                       row(1, 2, 2, 6, 12),
                       row(1, 4, 2, 6, 12));

            assertRows(execute("SELECT * FROM %s GROUP BY a, b LIMIT 3"),
                       row(1, 2, 1, 3, 6),
                       row(1, 4, 2, 6, 12),
                       row(2, 2, 3, 3, 6));

            // Range queries without aggregates and with PER PARTITION LIMIT
            assertRows(execute("SELECT a, b, c, d FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2"),
                       row(1, 2, 1, 3),
                       row(1, 2, 2, 6),
                       row(2, 2, 3, 3),
                       row(2, 4, 3, 6),
                       row(4, 8, 2, 12));

            assertRows(execute("SELECT a, b, c, d FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                       row(1, 2, 1, 3),
                       row(2, 2, 3, 3),
                       row(4, 8, 2, 12));

            // Range queries with wildcard and with PER PARTITION LIMIT
            assertRows(execute("SELECT * FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2"),
                       row(1, 2, 1, 3, 6),
                       row(1, 2, 2, 6, 12),
                       row(2, 2, 3, 3, 6),
                       row(2, 4, 3, 6, 12),
                       row(4, 8, 2, 12, 24));

            assertRows(execute("SELECT * FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                       row(1, 2, 1, 3, 6),
                       row(2, 2, 3, 3, 6),
                       row(4, 8, 2, 12, 24));

            // Range queries without aggregates, with PER PARTITION LIMIT and LIMIT
            assertRows(execute("SELECT a, b, c, d FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2 LIMIT 3"),
                       row(1, 2, 1, 3),
                       row(1, 2, 2, 6),
                       row(2, 2, 3, 3));

            // Range queries with wildcard, with PER PARTITION LIMIT and LIMIT
            assertRows(execute("SELECT * FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2 LIMIT 3"),
                       row(1, 2, 1, 3, 6),
                       row(1, 2, 2, 6, 12),
                       row(2, 2, 3, 3, 6));

            // Range query with DISTINCT
            assertRows(execute("SELECT DISTINCT a, count(a)FROM %s GROUP BY a"),
                       row(1, 1L),
                       row(2, 1L),
                       row(4, 1L));

            assertInvalidMessage("Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                                 "SELECT DISTINCT a, count(a)FROM %s GROUP BY a, b");

            // Range query with DISTINCT and LIMIT
            assertRows(execute("SELECT DISTINCT a, count(a)FROM %s GROUP BY a LIMIT 2"),
                       row(1, 1L),
                       row(2, 1L));

            assertInvalidMessage("Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                                 "SELECT DISTINCT a, count(a)FROM %s GROUP BY a, b LIMIT 2");

            // Range query with ORDER BY
            assertInvalidMessage("ORDER BY is only supported when the partition key is restricted by an EQ or an IN",
                                 "SELECT a, b, c, count(b), max(e) FROM %s GROUP BY a, b ORDER BY b DESC, c DESC");

            // Single partition queries
            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c"),
                       row(1, 2, 6, 1L, 6),
                       row(1, 2, 12, 1L, 12),
                       row(1, 4, 12, 2L, 24));

            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY b, c"),
                       row(1, 2, 6, 1L, 6),
                       row(1, 2, 12, 1L, 12),
                       row(1, 4, 12, 2L, 24));

            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2 GROUP BY a, b, c"),
                       row(1, 2, 6, 1L, 6),
                       row(1, 2, 12, 1L, 12));

            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2 GROUP BY a, c"),
                       row(1, 2, 6, 1L, 6),
                       row(1, 2, 12, 1L, 12));

            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2 GROUP BY c"),
                       row(1, 2, 6, 1L, 6),
                       row(1, 2, 12, 1L, 12));

            // Single partition queries without aggregates
            assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b"),
                       row(1, 2, 1, 3),
                       row(1, 4, 2, 6));

            assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c"),
                       row(1, 2, 1, 3),
                       row(1, 2, 2, 6),
                       row(1, 4, 2, 6));

            assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY b, c"),
                       row(1, 2, 1, 3),
                       row(1, 2, 2, 6),
                       row(1, 4, 2, 6));

            assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 and token(a) = token(1) GROUP BY b, c"),
                       row(1, 2, 1, 3),
                       row(1, 2, 2, 6),
                       row(1, 4, 2, 6));

            // Single partition queries with wildcard
            assertRows(execute("SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c"),
                       row(1, 2, 1, 3, 6),
                       row(1, 2, 2, 6, 12),
                       row(1, 4, 2, 6, 12));

            assertRows(execute("SELECT * FROM %s WHERE a = 1 GROUP BY a, b"),
                       row(1, 2, 1, 3, 6),
                       row(1, 4, 2, 6, 12));

            // Single partition queries with DISTINCT
            assertRows(execute("SELECT DISTINCT a, count(a)FROM %s WHERE a = 1 GROUP BY a"),
                       row(1, 1L));

            assertInvalidMessage("Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                                 "SELECT DISTINCT a, count(a)FROM %s WHERE a = 1 GROUP BY a, b");

            // Single partition queries with LIMIT
            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 10"),
                       row(1, 2, 6, 1L, 6),
                       row(1, 2, 12, 1L, 12),
                       row(1, 4, 12, 2L, 24));

            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2"),
                       row(1, 2, 6, 1L, 6),
                       row(1, 2, 12, 1L, 12));

            assertRows(execute("SELECT count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 1"),
                       row(1L, 6));

            // Single partition queries with PER PARTITION LIMIT
            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 10"),
                       row(1, 2, 6, 1L, 6),
                       row(1, 2, 12, 1L, 12),
                       row(1, 4, 12, 2L, 24));

            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2"),
                       row(1, 2, 6, 1L, 6),
                       row(1, 2, 12, 1L, 12));

            assertRows(execute("SELECT count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 1"),
                       row(1L, 6));

            // Single partition queries without aggregates and with LIMIT
            assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b LIMIT 2"),
                       row(1, 2, 1, 3),
                       row(1, 4, 2, 6));

            assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b LIMIT 1"),
                       row(1, 2, 1, 3));

            assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2"),
                       row(1, 2, 1, 3),
                       row(1, 2, 2, 6));

            // Single partition queries with wildcard and with LIMIT
            assertRows(execute("SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2"),
                       row(1, 2, 1, 3, 6),
                       row(1, 2, 2, 6, 12));

            assertRows(execute("SELECT * FROM %s WHERE a = 1 GROUP BY a, b LIMIT 1"),
                       row(1, 2, 1, 3, 6));

            // Single partition queries without aggregates and with PER PARTITION LIMIT
            assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 2"),
                       row(1, 2, 1, 3),
                       row(1, 4, 2, 6));

            assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 1"),
                       row(1, 2, 1, 3));

            assertRows(execute("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2"),
                       row(1, 2, 1, 3),
                       row(1, 2, 2, 6));

            // Single partition queries with wildcard and with PER PARTITION LIMIT
            assertRows(execute("SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2"),
                       row(1, 2, 1, 3, 6),
                       row(1, 2, 2, 6, 12));

            assertRows(execute("SELECT * FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 1"),
                       row(1, 2, 1, 3, 6));

            // Single partition queries with ORDER BY
            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC"),
                       row(1, 4, 24, 2L, 24),
                       row(1, 2, 12, 1L, 12),
                       row(1, 2, 6, 1L, 6));

            // Single partition queries with ORDER BY and PER PARTITION LIMIT
            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC PER PARTITION LIMIT 1"),
                       row(1, 4, 24, 2L, 24));

            // Single partition queries with ORDER BY and LIMIT
            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC LIMIT 2"),
                       row(1, 4, 24, 2L, 24),
                       row(1, 2, 12, 1L, 12));

            // Multi-partitions queries
            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c"),
                       row(1, 2, 6, 1L, 6),
                       row(1, 2, 12, 1L, 12),
                       row(1, 4, 12, 2L, 24),
                       row(2, 2, 6, 1L, 6),
                       row(2, 4, 12, 1L, 12),
                       row(4, 8, 24, 1L, 24));

            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) AND b = 2 GROUP BY a, b, c"),
                       row(1, 2, 6, 1L, 6),
                       row(1, 2, 12, 1L, 12),
                       row(2, 2, 6, 1L, 6));

            // Multi-partitions queries without aggregates
            assertRows(execute("SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b"),
                       row(1, 2, 1, 3),
                       row(1, 4, 2, 6),
                       row(2, 2, 3, 3),
                       row(2, 4, 3, 6),
                       row(4, 8, 2, 12));

            assertRows(execute("SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c"),
                       row(1, 2, 1, 3),
                       row(1, 2, 2, 6),
                       row(1, 4, 2, 6),
                       row(2, 2, 3, 3),
                       row(2, 4, 3, 6),
                       row(4, 8, 2, 12));

            // Multi-partitions with wildcard
            assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c"),
                       row(1, 2, 1, 3, 6),
                       row(1, 2, 2, 6, 12),
                       row(1, 4, 2, 6, 12),
                       row(2, 2, 3, 3, 6),
                       row(2, 4, 3, 6, 12),
                       row(4, 8, 2, 12, 24));

            assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b"),
                       row(1, 2, 1, 3, 6),
                       row(1, 4, 2, 6, 12),
                       row(2, 2, 3, 3, 6),
                       row(2, 4, 3, 6, 12),
                       row(4, 8, 2, 12, 24));

            // Multi-partitions query with DISTINCT
            assertRows(execute("SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a"),
                       row(1, 1L),
                       row(2, 1L),
                       row(4, 1L));

            assertInvalidMessage("Grouping on clustering columns is not allowed for SELECT DISTINCT queries",
                                 "SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b");

            // Multi-partitions query with DISTINCT and LIMIT
            assertRows(execute("SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a LIMIT 2"),
                       row(1, 1L),
                       row(2, 1L));

            // Multi-partitions queries with PER PARTITION LIMIT
            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 1"),
                       row(1, 2, 6, 1L, 6),
                       row(2, 2, 6, 1L, 6),
                       row(4, 8, 24, 1L, 24));

            assertRows(execute("SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 2"),
                       row(1, 2, 6, 1L, 6),
                       row(1, 2, 12, 1L, 12),
                       row(2, 2, 6, 1L, 6),
                       row(2, 4, 12, 1L, 12),
                       row(4, 8, 24, 1L, 24));

            // Multi-partitions with wildcard and PER PARTITION LIMIT
            assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 2"),
                       row(1, 2, 1, 3, 6),
                       row(1, 2, 2, 6, 12),
                       row(2, 2, 3, 3, 6),
                       row(2, 4, 3, 6, 12),
                       row(4, 8, 2, 12, 24));

            assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b PER PARTITION LIMIT 1"),
                       row(1, 2, 1, 3, 6),
                       row(2, 2, 3, 3, 6),
                       row(4, 8, 2, 12, 24));

            // Multi-partitions queries with ORDER BY
            assertRows(execute("SELECT a, b, c, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b ORDER BY b DESC, c DESC"),
                       row(4, 8, 2, 1L, 24),
                       row(2, 4, 3, 1L, 12),
                       row(1, 4, 2, 2L, 24),
                       row(2, 2, 3, 1L, 6),
                       row(1, 2, 2, 2L, 12));

            assertRows(execute("SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c ORDER BY b DESC, c DESC"),
                       row(4, 8, 2, 12),
                       row(2, 4, 3, 6),
                       row(1, 4, 2, 12),
                       row(2, 2, 3, 3),
                       row(1, 2, 2, 6),
                       row(1, 2, 1, 3));

            // Multi-partitions queries with ORDER BY and LIMIT
            assertRows(execute("SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b ORDER BY b DESC, c DESC LIMIT 3"),
                       row(4, 8, 2, 12),
                       row(2, 4, 3, 6),
                       row(1, 4, 2, 12));

            // Multi-partitions with wildcard, ORDER BY and LIMIT
            assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c ORDER BY b DESC, c DESC LIMIT 3"),
                       row(4, 8, 2, 12, 24),
                       row(2, 4, 3, 6, 12),
                       row(1, 4, 2, 12, 24));

            // Invalid queries
            assertInvalidMessage("Group by is currently only supported on the columns of the PRIMARY KEY, got e",
                                 "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY a, e");

            assertInvalidMessage("Group by currently only support groups of columns following their declared order in the PRIMARY KEY",
                                 "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY c");

            assertInvalidMessage("Group by currently only support groups of columns following their declared order in the PRIMARY KEY",
                                 "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY a, c, b");

            assertInvalidMessage("Group by currently only support groups of columns following their declared order in the PRIMARY KEY",
                                 "SELECT a, b, d, count(b), max(c) FROM %s WHERE a = 1 GROUP BY a, a");

            assertInvalidMessage("Group by currently only support groups of columns following their declared order in the PRIMARY KEY",
                                 "SELECT a, b, c, d FROM %s WHERE token(a) = token(1) GROUP BY b, c");

            assertInvalidMessage("Undefined column name clustering1",
                                 "SELECT a, b as clustering1, max(c) FROM %s WHERE a = 1 GROUP BY a, clustering1");

            assertInvalidMessage("Undefined column name z",
                                 "SELECT a, b, max(c) FROM %s WHERE a = 1 GROUP BY a, b, z");

            // Test with composite partition key
            createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, primary key ((a, b), c, d))" + compactOption);

            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 1, 1, 3, 6)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 1, 2, 6, 12)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 1, 3, 12, 24)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 12, 24)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)");

            assertInvalidMessage("Group by is not supported on only a part of the partition key",
                                 "SELECT a, b, max(d) FROM %s GROUP BY a");

            assertRows(execute("SELECT a, b, max(d) FROM %s GROUP BY a, b"),
                       row(1, 2, 12),
                       row(1, 1, 12));

            assertRows(execute("SELECT a, b, max(d) FROM %s WHERE a = 1 AND b = 1 GROUP BY b"),
                       row(1, 1, 12));

            // Test with table without clustering key
            createTable("CREATE TABLE %s (a int primary key, b int, c int)" + compactOption);

            execute("INSERT INTO %s (a, b, c) VALUES (1, 3, 6)");
            execute("INSERT INTO %s (a, b, c) VALUES (2, 6, 12)");
            execute("INSERT INTO %s (a, b, c) VALUES (3, 12, 24)");

            assertInvalidMessage("Group by currently only support groups of columns following their declared order in the PRIMARY KEY",
                    "SELECT a, max(c) FROM %s WHERE a = 1 GROUP BY a, a");
        }
    }

    @Test
    public void testGroupByWithoutPagingWithDeletions() throws Throwable
    {
        for (String compactOption : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, primary key (a, b, c, d))"
                    + compactOption);

            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 3, 6)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 6, 12)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 9, 18)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 12, 24)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 3, 6)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 9, 18)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 12, 24)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 3, 6)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 6, 12)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 9, 18)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 3, 12, 24)");

            execute("DELETE FROM %s WHERE a = 1 AND b = 2 AND c = 1 AND d = 12");
            execute("DELETE FROM %s WHERE a = 1 AND b = 2 AND c = 2 AND d = 9");

            assertRows(execute("SELECT a, b, c, count(b), max(d) FROM %s GROUP BY a, b, c"),
                       row(1, 2, 1, 3L, 9),
                       row(1, 2, 2, 3L, 12),
                       row(1, 2, 3, 4L, 12));
        }
    }

    @Test
    public void testGroupByWithRangeNamesQueryWithoutPaging() throws Throwable
    {
        for (String compactOption : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, primary key (a, b, c))"
                    + compactOption);

            for (int i = 1; i < 5; i++)
                for (int j = 1; j < 5; j++)
                    for (int k = 1; k < 5; k++)
                        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, k, i + j);

            // Makes sure that we have some tombstones
            execute("DELETE FROM %s WHERE a = 3");

            // Range queries
            assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a ALLOW FILTERING"),
                       row(1, 1, 2, 2L, 2),
                       row(2, 1, 3, 2L, 3),
                       row(4, 1, 5, 2L, 5));

            assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b ALLOW FILTERING"),
                       row(1, 1, 2, 2L, 2),
                       row(2, 1, 3, 2L, 3),
                       row(4, 1, 5, 2L, 5));

            assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b ALLOW FILTERING"),
                       row(1, 1, 2, 2L, 2),
                       row(1, 2, 3, 2L, 3),
                       row(2, 1, 3, 2L, 3),
                       row(2, 2, 4, 2L, 4),
                       row(4, 1, 5, 2L, 5),
                       row(4, 2, 6, 2L, 6));

            // Range queries with LIMIT
            assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a LIMIT 5 ALLOW FILTERING"),
                       row(1, 1, 2, 2L, 2),
                       row(2, 1, 3, 2L, 3),
                       row(4, 1, 5, 2L, 5));

            assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING"),
                       row(1, 1, 2, 2L, 2),
                       row(2, 1, 3, 2L, 3),
                       row(4, 1, 5, 2L, 5));

            assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING"),
                       row(1, 1, 2, 2L, 2),
                       row(1, 2, 3, 2L, 3),
                       row(2, 1, 3, 2L, 3));

            // Range queries with PER PARTITION LIMIT
            assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 ALLOW FILTERING"),
                       row(1, 1, 2, 2L, 2),
                       row(2, 1, 3, 2L, 3),
                       row(4, 1, 5, 2L, 5));

            assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 ALLOW FILTERING"),
                       row(1, 1, 2, 2L, 2),
                       row(2, 1, 3, 2L, 3),
                       row(4, 1, 5, 2L, 5));

            // Range queries with PER PARTITION LIMIT and LIMIT
            assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 5 ALLOW FILTERING"),
                       row(1, 1, 2, 2L, 2),
                       row(2, 1, 3, 2L, 3),
                       row(4, 1, 5, 2L, 5));

            assertRows(execute("SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2 ALLOW FILTERING"),
                       row(1, 1, 2, 2L, 2),
                       row(2, 1, 3, 2L, 3));
        }
    }

    @Test
    public void testGroupByWithStaticColumnsWithoutPaging() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, s int static, d int, primary key (a, b, c))");

        // ------------------------------------
        // Test with non static columns empty
        // ------------------------------------
        execute("UPDATE %s SET s = 1 WHERE a = 1");
        execute("UPDATE %s SET s = 2 WHERE a = 2");
        execute("UPDATE %s SET s = 3 WHERE a = 4");

        // Range queries
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a"),
                   row(1, null, 1, 0L, 1L),
                   row(2, null, 2, 0L, 1L),
                   row(4, null, 3, 0L, 1L));

        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b"),
                   row(1, null, 1, 0L, 1L),
                   row(2, null, 2, 0L, 1L),
                   row(4, null, 3, 0L, 1L));

        // Range query without aggregates
        assertRows(execute("SELECT a, b, s FROM %s GROUP BY a, b"),
                   row(1, null, 1),
                   row(2, null, 2),
                   row(4, null, 3));

        // Range query with wildcard
        assertRows(execute("SELECT * FROM %s GROUP BY a, b"),
                   row(1, null, null, 1, null),
                   row(2, null, null, 2, null),
                   row(4, null, null, 3, null ));

        // Range query with LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b LIMIT 2"),
                   row(1, null, 1, 0L, 1L),
                   row(2, null, 2, 0L, 1L));

        // Range queries with PER PARTITION LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a PER PARTITION LIMIT 2"),
                   row(1, null, 1, 0L, 1L),
                   row(2, null, 2, 0L, 1L),
                   row(4, null, 3, 0L, 1L));

        // Range query with DISTINCT
        assertRows(execute("SELECT DISTINCT a, s, count(s) FROM %s GROUP BY a"),
                   row(1, 1, 1L),
                   row(2, 2, 1L),
                   row(4, 3, 1L));

        // Range queries with DISTINCT and LIMIT
        assertRows(execute("SELECT DISTINCT a, s, count(s) FROM %s GROUP BY a LIMIT 2"),
                   row(1, 1, 1L),
                   row(2, 2, 1L));

        // Single partition queries
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a"),
                   row(1, null, 1, 0L, 1L));

        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a, b"),
                   row(1, null, 1, 0L, 1L));

        // Single partition query without aggregates
        assertRows(execute("SELECT a, b, s FROM %s WHERE a = 1 GROUP BY a, b"),
                   row(1, null, 1));

        // Single partition query with wildcard
        assertRows(execute("SELECT * FROM %s WHERE a = 1 GROUP BY a, b"),
                   row(1, null, null, 1, null));

        // Single partition query with LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a, b LIMIT 2"),
                   row(1, null, 1, 0L, 1L));

        // Single partition query with PER PARTITION LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 2"),
                   row(1, null, 1, 0L, 1L));

        // Single partition query with DISTINCT
        assertRows(execute("SELECT DISTINCT a, s, count(s) FROM %s WHERE a = 1 GROUP BY a"),
                   row(1, 1, 1L));

        // Multi-partitions queries
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a"),
                   row(1, null, 1, 0L, 1L),
                   row(2, null, 2, 0L, 1L),
                   row(4, null, 3, 0L, 1L));

        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b"),
                   row(1, null, 1, 0L, 1L),
                   row(2, null, 2, 0L, 1L),
                   row(4, null, 3, 0L, 1L));

        // Multi-partitions query without aggregates
        assertRows(execute("SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b"),
                   row(1, null, 1),
                   row(2, null, 2),
                   row(4, null, 3));

        // Multi-partitions query with wildcard
        assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b"),
                   row(1, null, null, 1, null),
                   row(2, null, null, 2, null),
                   row(4, null, null, 3, null ));

        // Multi-partitions query with LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b LIMIT 2"),
                   row(1, null, 1, 0L, 1L),
                   row(2, null, 2, 0L, 1L));

        // Multi-partitions query with PER PARTITION LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 2"),
                   row(1, null, 1, 0L, 1L),
                   row(2, null, 2, 0L, 1L),
                   row(4, null, 3, 0L, 1L));

        // Multi-partitions queries with DISTINCT
        assertRows(execute("SELECT DISTINCT a, s, count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a"),
                   row(1, 1, 1L),
                   row(2, 2, 1L),
                   row(4, 3, 1L));

        // Multi-partitions with DISTINCT and LIMIT
        assertRows(execute("SELECT DISTINCT a, s, count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2"),
                   row(1, 1, 1L),
                   row(2, 2, 1L));

        // ------------------------------------
        // Test with some non static columns empty
        // ------------------------------------
        execute("UPDATE %s SET s = 3 WHERE a = 3");
        execute("DELETE s FROM %s WHERE a = 4");

        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 2, 1, 3)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 2, 2, 6)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 3, 2, 12)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 4, 2, 12)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 4, 3, 6)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (2, 2, 3, 3)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (2, 4, 3, 6)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (4, 8, 2, 12)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (5, 8, 2, 12)");

        // Makes sure that we have some tombstones
        execute("DELETE FROM %s WHERE a = 1 AND b = 3 AND c = 2");
        execute("DELETE FROM %s WHERE a = 5");

        // Range queries
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a"),
                   row(1, 2, 1, 4L, 4L),
                   row(2, 2, 2, 2L, 2L),
                   row(4, 8, null, 1L, 0L),
                   row(3, null, 3, 0L, 1L));

        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b"),
                   row(1, 2, 1, 2L, 2L),
                   row(1, 4, 1, 2L, 2L),
                   row(2, 2, 2, 1L, 1L),
                   row(2, 4, 2, 1L, 1L),
                   row(4, 8, null, 1L, 0L),
                   row(3, null, 3, 0L, 1L));

        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE b = 2 GROUP BY a, b ALLOW FILTERING"),
                   row(1, 2, 1, 2L, 2L),
                   row(2, 2, 2, 1L, 1L));

        // Range queries without aggregates
        assertRows(execute("SELECT a, b, s FROM %s GROUP BY a"),
                   row(1, 2, 1),
                   row(2, 2, 2),
                   row(4, 8, null),
                   row(3, null, 3));

        assertRows(execute("SELECT a, b, s FROM %s GROUP BY a, b"),
                   row(1, 2, 1),
                   row(1, 4, 1),
                   row(2, 2, 2),
                   row(2, 4, 2),
                   row(4, 8, null),
                   row(3, null, 3));

        // Range queries with wildcard
        assertRows(execute("SELECT * FROM %s GROUP BY a"),
                   row(1, 2, 1, 1, 3),
                   row(2, 2, 3, 2, 3),
                   row(4, 8, 2, null, 12),
                   row(3, null, null, 3, null));

        assertRows(execute("SELECT * FROM %s GROUP BY a, b"),
                   row(1, 2, 1, 1, 3),
                   row(1, 4, 2, 1, 12),
                   row(2, 2, 3, 2, 3),
                   row(2, 4, 3, 2, 6),
                   row(4, 8, 2, null, 12),
                   row(3, null, null, 3, null));

        // Range query with LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a LIMIT 2"),
                   row(1, 2, 1, 4L, 4L),
                   row(2, 2, 2, 2L, 2L));

        // Range query with PER PARTITION LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 2L, 2L),
                   row(2, 2, 2, 1L, 1L),
                   row(4, 8, null, 1L, 0L),
                   row(3, null, 3, 0L, 1L));

        // Range query with PER PARTITION LIMIT and LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 3"),
                   row(1, 2, 1, 2L, 2L),
                   row(2, 2, 2, 1L, 1L),
                   row(4, 8, null, 1L, 0L));

        // Range queries without aggregates and with LIMIT
        assertRows(execute("SELECT a, b, s FROM %s GROUP BY a LIMIT 2"),
                   row(1, 2, 1),
                   row(2, 2, 2));

        assertRows(execute("SELECT a, b, s FROM %s GROUP BY a, b LIMIT 10"),
                   row(1, 2, 1),
                   row(1, 4, 1),
                   row(2, 2, 2),
                   row(2, 4, 2),
                   row(4, 8, null),
                   row(3, null, 3));

        // Range queries with wildcard and with LIMIT
        assertRows(execute("SELECT * FROM %s GROUP BY a LIMIT 2"),
                   row(1, 2, 1, 1, 3),
                   row(2, 2, 3, 2, 3));

        assertRows(execute("SELECT * FROM %s GROUP BY a, b LIMIT 10"),
                   row(1, 2, 1, 1, 3),
                   row(1, 4, 2, 1, 12),
                   row(2, 2, 3, 2, 3),
                   row(2, 4, 3, 2, 6),
                   row(4, 8, 2, null, 12),
                   row(3, null, null, 3, null));

        // Range queries without aggregates and with PER PARTITION LIMIT
        assertRows(execute("SELECT a, b, s FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1),
                   row(2, 2, 2),
                   row(4, 8, null),
                   row(3, null, 3));

        // Range queries with wildcard and with PER PARTITION LIMIT
        assertRows(execute("SELECT * FROM %s GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 1, 3),
                   row(2, 2, 3, 2, 3),
                   row(4, 8, 2, null, 12),
                   row(3, null, null, 3, null));

        // Range queries without aggregates, with PER PARTITION LIMIT and with LIMIT
        assertRows(execute("SELECT a, b, s FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2"),
                   row(1, 2, 1),
                   row(2, 2, 2));

        // Range queries with wildcard, PER PARTITION LIMIT and LIMIT
        assertRows(execute("SELECT * FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2"),
                   row(1, 2, 1, 1, 3),
                   row(2, 2, 3, 2, 3));

        // Range query with DISTINCT
        assertRows(execute("SELECT DISTINCT a, s, count(a), count(s) FROM %s GROUP BY a"),
                   row(1, 1, 1L, 1L),
                   row(2, 2, 1L, 1L),
                   row(4, null, 1L, 0L),
                   row(3, 3, 1L, 1L));

        // Range query with DISTINCT and LIMIT
        assertRows(execute("SELECT DISTINCT a, s, count(a), count(s) FROM %s GROUP BY a LIMIT 2"),
                   row(1, 1, 1L, 1L),
                   row(2, 2, 1L, 1L));

        // Range query with ORDER BY
        assertInvalidMessage("ORDER BY is only supported when the partition key is restricted by an EQ or an IN",
                             "SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a ORDER BY b DESC, c DESC");

        // Single partition queries
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a"),
                   row(1, 2, 1, 4L, 4L));

        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 3 GROUP BY a, b"),
                   row(3, null, 3, 0L, 1L));

        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 AND b = 2 GROUP BY a, b"),
                   row(2, 2, 2, 1L, 1L));

        // Single partition queries without aggregates
        assertRows(execute("SELECT a, b, s FROM %s WHERE a = 1 GROUP BY a"),
                   row(1, 2, 1));

        assertRows(execute("SELECT a, b, s FROM %s WHERE a = 4 GROUP BY a, b"),
                   row(4, 8, null));

        // Single partition queries with wildcard
        assertRows(execute("SELECT * FROM %s WHERE a = 1 GROUP BY a"),
                   row(1, 2, 1, 1, 3));

        assertRows(execute("SELECT * FROM %s WHERE a = 4 GROUP BY a, b"),
                   row(4, 8, 2, null, 12));

        // Single partition query with LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b LIMIT 1"),
                   row(2, 2, 2, 1L, 1L));

        // Single partition query with PER PARTITION LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(2, 2, 2, 1L, 1L));

        // Single partition queries without aggregates and with LIMIT
        assertRows(execute("SELECT a, b, s FROM %s WHERE a = 2 GROUP BY a, b LIMIT 1"),
                   row(2, 2, 2));

        assertRows(execute("SELECT a, b, s FROM %s WHERE a = 2 GROUP BY a, b LIMIT 2"),
                   row(2, 2, 2),
                   row(2, 4, 2));

        // Single partition queries with DISTINCT
        assertRows(execute("SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a = 2 GROUP BY a"),
                   row(2, 2, 1L, 1L));

        assertRows(execute("SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a = 4 GROUP BY a"),
                   row(4, null, 1L, 0L));

         // Single partition query with ORDER BY
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC"),
                   row(2, 4, 2, 1L, 1L),
                   row(2, 2, 2, 1L, 1L));

         // Single partition queries with ORDER BY and LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC LIMIT 1"),
                   row(2, 4, 2, 1L, 1L));

        // Single partition queries with ORDER BY and PER PARTITION LIMIT
       assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC PER PARTITION LIMIT 1"),
                  row(2, 4, 2, 1L, 1L));

        // Multi-partitions queries
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a"),
                   row(1, 2, 1, 4L, 4L),
                   row(2, 2, 2, 2L, 2L),
                   row(3, null, 3, 0L, 1L),
                   row(4, 8, null, 1L, 0L));

        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b"),
                   row(1, 2, 1, 2L, 2L),
                   row(1, 4, 1, 2L, 2L),
                   row(2, 2, 2, 1L, 1L),
                   row(2, 4, 2, 1L, 1L),
                   row(3, null, 3, 0L, 1L),
                   row(4, 8, null, 1L, 0L));

        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) AND b = 2 GROUP BY a, b"),
                   row(1, 2, 1, 2L, 2L),
                   row(2, 2, 2, 1L, 1L));

        // Multi-partitions queries without aggregates
        assertRows(execute("SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a"),
                   row(1, 2, 1),
                   row(2, 2, 2),
                   row(3, null, 3),
                   row(4, 8, null));

        assertRows(execute("SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b"),
                   row(1, 2, 1),
                   row(1, 4, 1),
                   row(2, 2, 2),
                   row(2, 4, 2),
                   row(3, null, 3),
                   row(4, 8, null));

        // Multi-partitions queries with wildcard
        assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a"),
                   row(1, 2, 1, 1, 3),
                   row(2, 2, 3, 2, 3),
                   row(3, null, null, 3, null),
                   row(4, 8, 2, null, 12));

        assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b"),
                   row(1, 2, 1, 1, 3),
                   row(1, 4, 2, 1, 12),
                   row(2, 2, 3, 2, 3),
                   row(2, 4, 3, 2, 6),
                   row(3, null, null, 3, null),
                   row(4, 8, 2, null, 12));

        // Multi-partitions query with LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2"),
                   row(1, 2, 1, 4L, 4L),
                   row(2, 2, 2, 2L, 2L));

        // Multi-partitions query with PER PARTITION LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 1"),
                   row(1, 2, 1, 2L, 2L),
                   row(2, 2, 2, 1L, 1L),
                   row(3, null, 3, 0L, 1L),
                   row(4, 8, null, 1L, 0L));

        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 2"),
                   row(1, 2, 1, 2L, 2L),
                   row(1, 4, 1, 2L, 2L),
                   row(2, 2, 2, 1L, 1L),
                   row(2, 4, 2, 1L, 1L),
                   row(3, null, 3, 0L, 1L),
                   row(4, 8, null, 1L, 0L));

        // Multi-partitions queries with PER PARTITION LIMIT and LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 3"),
                   row(1, 2, 1, 2L, 2L),
                   row(2, 2, 2, 1L, 1L),
                   row(3, null, 3, 0L, 1L));

        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 4 LIMIT 3"),
                   row(1, 2, 1, 2L, 2L),
                   row(1, 4, 1, 2L, 2L),
                   row(2, 2, 2, 1L, 1L));

        // Multi-partitions queries without aggregates and with LIMIT
        assertRows(execute("SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2"),
                   row(1, 2, 1),
                   row(2, 2, 2));

        assertRows(execute("SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b LIMIT 10"),
                   row(1, 2, 1),
                   row(1, 4, 1),
                   row(2, 2, 2),
                   row(2, 4, 2),
                   row(3, null, 3),
                   row(4, 8, null));

        // Multi-partitions query with DISTINCT
        assertRows(execute("SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a"),
                   row(1, 1, 1L, 1L),
                   row(2, 2, 1L, 1L),
                   row(3, 3, 1L, 1L),
                   row(4, null, 1L, 0L));

        // Multi-partitions query with DISTINCT and LIMIT
        assertRows(execute("SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2"),
                   row(1, 1, 1L, 1L),
                   row(2, 2, 1L, 1L));

         // Multi-partitions query with ORDER BY
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b ORDER BY b DESC, c DESC"),
                   row(4, 8, null, 1L, 0L),
                   row(1, 4, 1, 2L, 2L),
                   row(2, 4, 2, 1L, 1L),
                   row(2, 2, 2, 1L, 1L),
                   row(1, 2, 1, 2L, 2L));

         // Multi-partitions queries with ORDER BY and LIMIT
        assertRows(execute("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b ORDER BY b DESC, c DESC LIMIT 2"),
                   row(4, 8, null, 1L, 0L),
                   row(1, 4, 1, 2L, 2L));
    }

    @Test
    public void testGroupByWithPaging() throws Throwable
    {
        for (String compactOption : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, primary key (a, b, c, d))"
                    + compactOption);

            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 1, 3, 6)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 3, 2, 12, 24)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, 2, 12, 24)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 4, 2, 6, 12)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (2, 2, 3, 3, 6)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (2, 4, 3, 6, 12)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (3, 3, 2, 12, 24)");
            execute("INSERT INTO %s (a, b, c, d, e) VALUES (4, 8, 2, 12, 24)");

            // Makes sure that we have some tombstones
            execute("DELETE FROM %s WHERE a = 1 AND b = 3 AND c = 2 AND d = 12");
            execute("DELETE FROM %s WHERE a = 3");

            for (int pageSize = 1; pageSize < 10; pageSize++)
            {
                // Range queries
                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a", pageSize),
                              row(1, 2, 6, 4L, 24),
                              row(2, 2, 6, 2L, 12),
                              row(4, 8, 24, 1L, 24));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b", pageSize),
                              row(1, 2, 6, 2L, 12),
                              row(1, 4, 12, 2L, 24),
                              row(2, 2, 6, 1L, 6),
                              row(2, 4, 12, 1L, 12),
                              row(4, 8, 24, 1L, 24));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s", pageSize),
                              row(1, 2, 6, 7L, 24));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE b = 2 GROUP BY a, b ALLOW FILTERING",
                                                   pageSize),
                              row(1, 2, 6, 2L, 12),
                              row(2, 2, 6, 1L, 6));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE b = 2 ALLOW FILTERING",
                                                   pageSize),
                              row(1, 2, 6, 3L, 12));

                // Range queries without aggregates
                assertRowsNet(executeNetWithPaging("SELECT a, b, c, d FROM %s GROUP BY a, b, c", pageSize),
                              row(1, 2, 1, 3),
                              row(1, 2, 2, 6),
                              row(1, 4, 2, 6),
                              row(2, 2, 3, 3),
                              row(2, 4, 3, 6),
                              row(4, 8, 2, 12));

                assertRowsNet(executeNetWithPaging("SELECT a, b, c, d FROM %s GROUP BY a, b", pageSize),
                              row(1, 2, 1, 3),
                              row(1, 4, 2, 6),
                              row(2, 2, 3, 3),
                              row(2, 4, 3, 6),
                              row(4, 8, 2, 12));

                // Range queries with wildcard
                assertRowsNet(executeNetWithPaging("SELECT * FROM %s GROUP BY a, b, c", pageSize),
                              row(1, 2, 1, 3, 6),
                              row(1, 2, 2, 6, 12),
                              row(1, 4, 2, 6, 12),
                              row(2, 2, 3, 3, 6),
                              row(2, 4, 3, 6, 12),
                              row(4, 8, 2, 12, 24));

                assertRowsNet(executeNetWithPaging("SELECT * FROM %s GROUP BY a, b", pageSize),
                              row(1, 2, 1, 3, 6),
                              row(1, 4, 2, 6, 12),
                              row(2, 2, 3, 3, 6),
                              row(2, 4, 3, 6, 12),
                              row(4, 8, 2, 12, 24));

                // Range query with LIMIT
                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b LIMIT 2",
                                                   pageSize),
                              row(1, 2, 6, 2L, 12),
                              row(1, 4, 12, 2L, 24));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s LIMIT 2",
                                                   pageSize),
                              row(1, 2, 6, 7L, 24));

                // Range queries with PER PARTITION LIMIT
                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b PER PARTITION LIMIT 3", pageSize),
                              row(1, 2, 6, 2L, 12),
                              row(1, 4, 12, 2L, 24),
                              row(2, 2, 6, 1L, 6),
                              row(2, 4, 12, 1L, 12),
                              row(4, 8, 24, 1L, 24));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b PER PARTITION LIMIT 1", pageSize),
                              row(1, 2, 6, 2L, 12),
                              row(2, 2, 6, 1L, 6),
                              row(4, 8, 24, 1L, 24));

                // Range query with PER PARTITION LIMIT
                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2", pageSize),
                              row(1, 2, 6, 2L, 12),
                              row(2, 2, 6, 1L, 6));

                // Range query without aggregates and with PER PARTITION LIMIT
                assertRowsNet(executeNetWithPaging("SELECT a, b, c, d FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2", pageSize),
                              row(1, 2, 1, 3),
                              row(1, 2, 2, 6),
                              row(2, 2, 3, 3),
                              row(2, 4, 3, 6),
                              row(4, 8, 2, 12));

                // Range queries with wildcard and with LIMIT
                assertRowsNet(executeNetWithPaging("SELECT * FROM %s GROUP BY a, b, c LIMIT 3", pageSize),
                              row(1, 2, 1, 3, 6),
                              row(1, 2, 2, 6, 12),
                              row(1, 4, 2, 6, 12));

                assertRowsNet(executeNetWithPaging("SELECT * FROM %s GROUP BY a, b LIMIT 3", pageSize),
                              row(1, 2, 1, 3, 6),
                              row(1, 4, 2, 6, 12),
                              row(2, 2, 3, 3, 6));

                // Range queries with wildcard and with PER PARTITION LIMIT
                assertRowsNet(executeNetWithPaging("SELECT * FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2", pageSize),
                              row(1, 2, 1, 3, 6),
                              row(1, 2, 2, 6, 12),
                              row(2, 2, 3, 3, 6),
                              row(2, 4, 3, 6, 12),
                              row(4, 8, 2, 12, 24));

                assertRowsNet(executeNetWithPaging("SELECT * FROM %s GROUP BY a, b PER PARTITION LIMIT 1", pageSize),
                              row(1, 2, 1, 3, 6),
                              row(2, 2, 3, 3, 6),
                              row(4, 8, 2, 12, 24));

                // Range queries without aggregates and with LIMIT
                assertRowsNet(executeNetWithPaging("SELECT a, b, c, d FROM %s GROUP BY a, b, c LIMIT 3", pageSize),
                              row(1, 2, 1, 3),
                              row(1, 2, 2, 6),
                              row(1, 4, 2, 6));

                assertRowsNet(executeNetWithPaging("SELECT a, b, c, d FROM %s GROUP BY a, b LIMIT 3", pageSize),
                              row(1, 2, 1, 3),
                              row(1, 4, 2, 6),
                              row(2, 2, 3, 3));

                // Range query without aggregates, with PER PARTITION LIMIT and with LIMIT
                assertRowsNet(executeNetWithPaging("SELECT a, b, c, d FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2 LIMIT 3", pageSize),
                              row(1, 2, 1, 3),
                              row(1, 2, 2, 6),
                              row(2, 2, 3, 3));

                // Range queries with wildcard, with PER PARTITION LIMIT and LIMIT
                assertRowsNet(executeNetWithPaging("SELECT * FROM %s GROUP BY a, b, c PER PARTITION LIMIT 2 LIMIT 3", pageSize),
                              row(1, 2, 1, 3, 6),
                              row(1, 2, 2, 6, 12),
                              row(2, 2, 3, 3, 6));

                // Range query with DISTINCT
                assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, count(a)FROM %s GROUP BY a", pageSize),
                              row(1, 1L),
                              row(2, 1L),
                              row(4, 1L));

                assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, count(a)FROM %s", pageSize),
                              row(1, 3L));

                // Range query with DISTINCT and LIMIT
                assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, count(a)FROM %s GROUP BY a LIMIT 2", pageSize),
                              row(1, 1L),
                              row(2, 1L));

                assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, count(a)FROM %s LIMIT 2", pageSize),
                              row(1, 3L));

                // Range query with ORDER BY
                assertInvalidMessage("ORDER BY is only supported when the partition key is restricted by an EQ or an IN",
                                     "SELECT a, b, c, count(b), max(e) FROM %s GROUP BY a, b ORDER BY b DESC, c DESC");

                // Single partition queries
                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c",
                                                   pageSize),
                              row(1, 2, 6, 1L, 6),
                              row(1, 2, 12, 1L, 12),
                              row(1, 4, 12, 2L, 24));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1", pageSize),
                              row(1, 2, 6, 4L, 24));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2 GROUP BY a, b, c",
                                                   pageSize),
                              row(1, 2, 6, 1L, 6),
                              row(1, 2, 12, 1L, 12));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 AND b = 2",
                                                   pageSize),
                              row(1, 2, 6, 2L, 12));

                // Single partition queries without aggregates
                assertRowsNet(executeNetWithPaging("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b", pageSize),
                              row(1, 2, 1, 3),
                              row(1, 4, 2, 6));

                assertRowsNet(executeNetWithPaging("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c", pageSize),
                              row(1, 2, 1, 3),
                              row(1, 2, 2, 6),
                              row(1, 4, 2, 6));

                // Single partition queries with wildcard
                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c", pageSize),
                           row(1, 2, 1, 3, 6),
                           row(1, 2, 2, 6, 12),
                           row(1, 4, 2, 6, 12));

                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 1 GROUP BY a, b", pageSize),
                           row(1, 2, 1, 3, 6),
                           row(1, 4, 2, 6, 12));

                // Single partition query with DISTINCT
                assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, count(a)FROM %s WHERE a = 1 GROUP BY a",
                                                   pageSize),
                              row(1, 1L));

                assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, count(a)FROM %s WHERE a = 1 GROUP BY a",
                                                   pageSize),
                              row(1, 1L));

                // Single partition queries with LIMIT
                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 10",
                                                   pageSize),
                              row(1, 2, 6, 1L, 6),
                              row(1, 2, 12, 1L, 12),
                              row(1, 4, 12, 2L, 24));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2",
                                                   pageSize),
                              row(1, 2, 6, 1L, 6),
                              row(1, 2, 12, 1L, 12));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 LIMIT 2",
                                                   pageSize),
                              row(1, 2, 6, 4L, 24));

                assertRowsNet(executeNetWithPaging("SELECT count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 1",
                                                   pageSize),
                              row(1L, 6));

                // Single partition queries with wildcard and with LIMIT
                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2", pageSize),
                           row(1, 2, 1, 3, 6),
                           row(1, 2, 2, 6, 12));

                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 1 GROUP BY a, b LIMIT 1", pageSize),
                           row(1, 2, 1, 3, 6));

                // Single partition queries with wildcard and with PER PARTITION LIMIT
                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2", pageSize),
                           row(1, 2, 1, 3, 6),
                           row(1, 2, 2, 6, 12));

                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 1", pageSize),
                           row(1, 2, 1, 3, 6));

                // Single partition query with PER PARTITION LIMIT
                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2",
                                                   pageSize),
                              row(1, 2, 6, 1L, 6),
                              row(1, 2, 12, 1L, 12));

                // Single partition queries without aggregates and with LIMIT
                assertRowsNet(executeNetWithPaging("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b LIMIT 2",
                                                   pageSize),
                              row(1, 2, 1, 3),
                              row(1, 4, 2, 6));

                assertRowsNet(executeNetWithPaging("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b LIMIT 1",
                                                   pageSize),
                              row(1, 2, 1, 3));

                assertRowsNet(executeNetWithPaging("SELECT a, b, c, d FROM %s WHERE a = 1 GROUP BY a, b, c LIMIT 2",
                                                   pageSize),
                              row(1, 2, 1, 3),
                              row(1, 2, 2, 6));

                // Single partition queries with ORDER BY
                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC",
                                                   pageSize),
                              row(1, 4, 24, 2L, 24),
                              row(1, 2, 12, 1L, 12),
                              row(1, 2, 6, 1L, 6));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 ORDER BY b DESC, c DESC",
                                                   pageSize),
                              row(1, 4, 24, 4L, 24));

                // Single partition queries with ORDER BY and LIMIT
                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC LIMIT 2",
                                                   pageSize),
                              row(1, 4, 24, 2L, 24),
                              row(1, 2, 12, 1L, 12));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 ORDER BY b DESC, c DESC LIMIT 2",
                                                   pageSize),
                              row(1, 4, 24, 4L, 24));

                // Single partition queries with ORDER BY and PER PARTITION LIMIT
                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC PER PARTITION LIMIT 2",
                                                   pageSize),
                              row(1, 4, 24, 2L, 24),
                              row(1, 2, 12, 1L, 12));

                // Multi-partitions queries
                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c",
                                                   pageSize),
                              row(1, 2, 6, 1L, 6),
                              row(1, 2, 12, 1L, 12),
                              row(1, 4, 12, 2L, 24),
                              row(2, 2, 6, 1L, 6),
                              row(2, 4, 12, 1L, 12),
                              row(4, 8, 24, 1L, 24));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4)",
                                                   pageSize),
                              row(1, 2, 6, 7L, 24));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) AND b = 2 GROUP BY a, b, c",
                                                   pageSize),
                              row(1, 2, 6, 1L, 6),
                              row(1, 2, 12, 1L, 12),
                              row(2, 2, 6, 1L, 6));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) AND b = 2",
                                                   pageSize),
                              row(1, 2, 6, 3L, 12));

                // Multi-partitions queries with PER PARTITION LIMIT
                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 2",
                                                   pageSize),
                              row(1, 2, 6, 1L, 6),
                              row(1, 2, 12, 1L, 12),
                              row(2, 2, 6, 1L, 6),
                              row(2, 4, 12, 1L, 12),
                              row(4, 8, 24, 1L, 24));

                assertRowsNet(executeNetWithPaging("SELECT a, b, e, count(b), max(e) FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c PER PARTITION LIMIT 1",
                                                   pageSize),
                              row(1, 2, 6, 1L, 6),
                              row(2, 2, 6, 1L, 6),
                              row(4, 8, 24, 1L, 24));

                // Multi-partitions queries without aggregates
                assertRowsNet(executeNetWithPaging("SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b",
                                                   pageSize),
                              row(1, 2, 1, 3),
                              row(1, 4, 2, 6),
                              row(2, 2, 3, 3),
                              row(2, 4, 3, 6),
                              row(4, 8, 2, 12));

                assertRowsNet(executeNetWithPaging("SELECT a, b, c, d FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c",
                                                   pageSize),
                              row(1, 2, 1, 3),
                              row(1, 2, 2, 6),
                              row(1, 4, 2, 6),
                              row(2, 2, 3, 3),
                              row(2, 4, 3, 6),
                              row(4, 8, 2, 12));

                // Multi-partitions with wildcard
                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b, c", pageSize),
                           row(1, 2, 1, 3, 6),
                           row(1, 2, 2, 6, 12),
                           row(1, 4, 2, 6, 12),
                           row(2, 2, 3, 3, 6),
                           row(2, 4, 3, 6, 12),
                           row(4, 8, 2, 12, 24));

                assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a IN (1, 2, 4) GROUP BY a, b", pageSize),
                           row(1, 2, 1, 3, 6),
                           row(1, 4, 2, 6, 12),
                           row(2, 2, 3, 3, 6),
                           row(2, 4, 3, 6, 12),
                           row(4, 8, 2, 12, 24));

                // Multi-partitions queries with DISTINCT
                assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a",
                                                   pageSize),
                              row(1, 1L),
                              row(2, 1L),
                              row(4, 1L));

                assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4)",
                                                   pageSize),
                              row(1, 3L));

                // Multi-partitions query with DISTINCT and LIMIT
                assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) GROUP BY a LIMIT 2",
                                                   pageSize),
                              row(1, 1L),
                              row(2, 1L));

                assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, count(a)FROM %s WHERE a IN (1, 2, 4) LIMIT 2",
                                                   pageSize),
                              row(1, 3L));
            }
        }
    }

    @Test
    public void testGroupByWithRangeNamesQueryWithPaging() throws Throwable
    {
        for (String compactOption : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, primary key (a, b, c))"
                    + compactOption);

            for (int i = 1; i < 5; i++)
                for (int j = 1; j < 5; j++)
                    for (int k = 1; k < 5; k++)
                        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, k, i + j);

            // Makes sure that we have some tombstones
            execute("DELETE FROM %s WHERE a = 3");

            for (int pageSize = 1; pageSize < 2; pageSize++)
            {
                // Range queries
                assertRowsNet(executeNetWithPaging("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a ALLOW FILTERING", pageSize),
                              row(1, 1, 2, 2L, 2),
                              row(2, 1, 3, 2L, 3),
                              row(4, 1, 5, 2L, 5));

                assertRowsNet(executeNetWithPaging("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b ALLOW FILTERING", pageSize),
                              row(1, 1, 2, 2L, 2),
                              row(2, 1, 3, 2L, 3),
                              row(4, 1, 5, 2L, 5));

                assertRowsNet(executeNetWithPaging("SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b ALLOW FILTERING", pageSize),
                              row(1, 1, 2, 2L, 2),
                              row(1, 2, 3, 2L, 3),
                              row(2, 1, 3, 2L, 3),
                              row(2, 2, 4, 2L, 4),
                              row(4, 1, 5, 2L, 5),
                              row(4, 2, 6, 2L, 6));

                // Range queries with LIMIT
                assertRowsNet(executeNetWithPaging("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a LIMIT 5 ALLOW FILTERING", pageSize),
                              row(1, 1, 2, 2L, 2),
                              row(2, 1, 3, 2L, 3),
                              row(4, 1, 5, 2L, 5));

                assertRowsNet(executeNetWithPaging("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING", pageSize),
                              row(1, 1, 2, 2L, 2),
                              row(2, 1, 3, 2L, 3),
                              row(4, 1, 5, 2L, 5));

                assertRowsNet(executeNetWithPaging("SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING", pageSize),
                              row(1, 1, 2, 2L, 2),
                              row(1, 2, 3, 2L, 3),
                              row(2, 1, 3, 2L, 3));

                // Range queries with PER PARTITION LIMIT
                assertRowsNet(executeNetWithPaging("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 2, 2L, 2),
                              row(2, 1, 3, 2L, 3),
                              row(4, 1, 5, 2L, 5));

                assertRowsNet(executeNetWithPaging("SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 ALLOW FILTERING", pageSize),
                              row(1, 1, 2, 2L, 2),
                              row(2, 1, 3, 2L, 3),
                              row(4, 1, 5, 2L, 5));

                // Range queries with PER PARTITION LIMIT and LIMIT
                assertRowsNet(executeNetWithPaging("SELECT a, b, d, count(b), max(d) FROM %s WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 5 ALLOW FILTERING", pageSize),
                              row(1, 1, 2, 2L, 2),
                              row(2, 1, 3, 2L, 3),
                              row(4, 1, 5, 2L, 5));

                assertRowsNet(executeNetWithPaging("SELECT a, b, d, count(b), max(d) FROM %s WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2 ALLOW FILTERING", pageSize),
                              row(1, 1, 2, 2L, 2),
                              row(2, 1, 3, 2L, 3));
            }
        }
    }

    @Test
    public void testGroupByWithStaticColumnsWithPaging() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, s int static, d int, primary key (a, b, c))");

        // ------------------------------------
        // Test with non static columns empty
        // ------------------------------------
        execute("UPDATE %s SET s = 1 WHERE a = 1");
        execute("UPDATE %s SET s = 2 WHERE a = 2");
        execute("UPDATE %s SET s = 3 WHERE a = 4");

        for (int pageSize = 1; pageSize < 10; pageSize++)
        {
            // Range queries
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a", pageSize),
                          row(1, null, 1, 0L, 1L),
                          row(2, null, 2, 0L, 1L),
                          row(4, null, 3, 0L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b", pageSize),
                          row(1, null, 1, 0L, 1L),
                          row(2, null, 2, 0L, 1L),
                          row(4, null, 3, 0L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s", pageSize),
                          row(1, null, 1, 0L, 3L));

            // Range query without aggregates
            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s GROUP BY a, b", pageSize),
                          row(1, null, 1),
                          row(2, null, 2),
                          row(4, null, 3));

            // Range query with wildcard
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s GROUP BY a, b", pageSize),
                       row(1, null, null, 1, null),
                       row(2, null, null, 2, null),
                       row(4, null, null, 3, null ));

            // Range query with LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b LIMIT 2",
                                               pageSize),
                          row(1, null, 1, 0L, 1L),
                          row(2, null, 2, 0L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s LIMIT 2", pageSize),
                          row(1, null, 1, 0L, 3L));

            // Range query with PER PARTITION LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a PER PARTITION LIMIT 2", pageSize),
                          row(1, null, 1, 0L, 1L),
                          row(2, null, 2, 0L, 1L),
                          row(4, null, 3, 0L, 1L));

            // Range query with PER PARTITION LIMIT and LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a PER PARTITION LIMIT 2 LIMIT 2", pageSize),
                          row(1, null, 1, 0L, 1L),
                          row(2, null, 2, 0L, 1L));

            // Range queries with DISTINCT
            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(s) FROM %s GROUP BY a", pageSize),
                          row(1, 1, 1L),
                          row(2, 2, 1L),
                          row(4, 3, 1L));

            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(s) FROM %s ", pageSize),
                          row(1, 1, 3L));

            // Range queries with DISTINCT and LIMIT
            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(s) FROM %s GROUP BY a LIMIT 2", pageSize),
                          row(1, 1, 1L),
                          row(2, 2, 1L));

            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(s) FROM %s LIMIT 2", pageSize),
                          row(1, 1, 3L));

            // Single partition queries
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a",
                                               pageSize),
                          row(1, null, 1, 0L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a, b",
                                               pageSize),
                          row(1, null, 1, 0L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1", pageSize),
                          row(1, null, 1, 0L, 1L));

            // Single partition query without aggregates
            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s WHERE a = 1 GROUP BY a, b", pageSize),
                          row(1, null, 1));

            // Single partition query with wildcard
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 1 GROUP BY a, b", pageSize),
                       row(1, null, null, 1, null));

            // Single partition queries with LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a, b LIMIT 2",
                                               pageSize),
                          row(1, null, 1, 0L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 LIMIT 2",
                                               pageSize),
                          row(1, null, 1, 0L, 1L));


            // Single partition queries with DISTINCT
            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(s) FROM %s WHERE a = 1 GROUP BY a",
                                               pageSize),
                          row(1, 1, 1L));

            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(s) FROM %s WHERE a = 1", pageSize),
                          row(1, 1, 1L));

            // Multi-partitions queries
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a",
                                               pageSize),
                          row(1, null, 1, 0L, 1L),
                          row(2, null, 2, 0L, 1L),
                          row(4, null, 3, 0L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b",
                                               pageSize),
                          row(1, null, 1, 0L, 1L),
                          row(2, null, 2, 0L, 1L),
                          row(4, null, 3, 0L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4)",
                                               pageSize),
                          row(1, null, 1, 0L, 3L));

            // Multi-partitions query without aggregates
            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b",
                                               pageSize),
                          row(1, null, 1),
                          row(2, null, 2),
                          row(4, null, 3));

            // Multi-partitions query with LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b LIMIT 2",
                                               pageSize),
                          row(1, null, 1, 0L, 1L),
                          row(2, null, 2, 0L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) LIMIT 2",
                                               pageSize),
                          row(1, null, 1, 0L, 3L));

            // Multi-partitions query with PER PARTITION LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a PER PARTITION LIMIT 2",
                                               pageSize),
                          row(1, null, 1, 0L, 1L),
                          row(2, null, 2, 0L, 1L),
                          row(4, null, 3, 0L, 1L));

            // Multi-partitions query with PER PARTITION LIMIT and LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a PER PARTITION LIMIT 2 LIMIT 2",
                                               pageSize),
                          row(1, null, 1, 0L, 1L),
                          row(2, null, 2, 0L, 1L));

            // Multi-partitions queries with DISTINCT
            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a",
                                               pageSize),
                          row(1, 1, 1L),
                          row(2, 2, 1L),
                          row(4, 3, 1L));

            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(s) FROM %s WHERE a IN (1, 2, 3, 4)",
                                               pageSize),
                          row(1, 1, 3L));

            // Multi-partitions queries with DISTINCT and LIMIT
            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2",
                                               pageSize),
                          row(1, 1, 1L),
                          row(2, 2, 1L));

            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(s) FROM %s WHERE a IN (1, 2, 3, 4) LIMIT 2",
                                               pageSize),
                          row(1, 1, 3L));
        }

        // ------------------------------------
        // Test with non static columns
        // ------------------------------------
        execute("UPDATE %s SET s = 3 WHERE a = 3");
        execute("DELETE s FROM %s WHERE a = 4");

        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 2, 1, 3)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 2, 2, 6)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 3, 2, 12)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 4, 2, 12)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 4, 3, 6)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (2, 2, 3, 3)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (2, 4, 3, 6)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (4, 8, 2, 12)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (5, 8, 2, 12)");

        // Makes sure that we have some tombstones
        execute("DELETE FROM %s WHERE a = 1 AND b = 3 AND c = 2");
        execute("DELETE FROM %s WHERE a = 5");

        for (int pageSize = 1; pageSize < 10; pageSize++)
        {
            // Range queries
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a", pageSize),
                          row(1, 2, 1, 4L, 4L),
                          row(2, 2, 2, 2L, 2L),
                          row(4, 8, null, 1L, 0L),
                          row(3, null, 3, 0L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b", pageSize),
                          row(1, 2, 1, 2L, 2L),
                          row(1, 4, 1, 2L, 2L),
                          row(2, 2, 2, 1L, 1L),
                          row(2, 4, 2, 1L, 1L),
                          row(4, 8, null, 1L, 0L),
                          row(3, null, 3, 0L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s", pageSize),
                          row(1, 2, 1, 7L, 7L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE b = 2 GROUP BY a, b ALLOW FILTERING",
                                               pageSize),
                          row(1, 2, 1, 2L, 2L),
                          row(2, 2, 2, 1L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE b = 2 ALLOW FILTERING",
                                               pageSize),
                          row(1, 2, 1, 3L, 3L));

            // Range queries without aggregates
            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s GROUP BY a", pageSize),
                          row(1, 2, 1),
                          row(2, 2, 2),
                          row(4, 8, null),
                          row(3, null, 3));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s GROUP BY a, b", pageSize),
                          row(1, 2, 1),
                          row(1, 4, 1),
                          row(2, 2, 2),
                          row(2, 4, 2),
                          row(4, 8, null),
                          row(3, null, 3));

            // Range queries with wildcard
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s GROUP BY a", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(2, 2, 3, 2, 3),
                       row(4, 8, 2, null, 12),
                       row(3, null, null, 3, null));

            assertRowsNet(executeNetWithPaging("SELECT * FROM %s GROUP BY a, b", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(1, 4, 2, 1, 12),
                       row(2, 2, 3, 2, 3),
                       row(2, 4, 3, 2, 6),
                       row(4, 8, 2, null, 12),
                       row(3, null, null, 3, null));

            // Range query with LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a LIMIT 2",
                                               pageSize),
                          row(1, 2, 1, 4L, 4L),
                          row(2, 2, 2, 2L, 2L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s LIMIT 2", pageSize),
                          row(1, 2, 1, 7L, 7L));

            // Range queries without aggregates and with LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s GROUP BY a LIMIT 2", pageSize),
                          row(1, 2, 1),
                          row(2, 2, 2));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s GROUP BY a, b LIMIT 10", pageSize),
                          row(1, 2, 1),
                          row(1, 4, 1),
                          row(2, 2, 2),
                          row(2, 4, 2),
                          row(4, 8, null),
                          row(3, null, 3));

            // Range queries with wildcard and with LIMIT
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s GROUP BY a LIMIT 2", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(2, 2, 3, 2, 3));

            assertRowsNet(executeNetWithPaging("SELECT * FROM %s GROUP BY a, b LIMIT 10", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(1, 4, 2, 1, 12),
                       row(2, 2, 3, 2, 3),
                       row(2, 4, 3, 2, 6),
                       row(4, 8, 2, null, 12),
                       row(3, null, null, 3, null));

            // Range queries with PER PARTITION LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b PER PARTITION LIMIT 2", pageSize),
                          row(1, 2, 1, 2L, 2L),
                          row(1, 4, 1, 2L, 2L),
                          row(2, 2, 2, 1L, 1L),
                          row(2, 4, 2, 1L, 1L),
                          row(4, 8, null, 1L, 0L),
                          row(3, null, 3, 0L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b PER PARTITION LIMIT 1", pageSize),
                          row(1, 2, 1, 2L, 2L),
                          row(2, 2, 2, 1L, 1L),
                          row(4, 8, null, 1L, 0L),
                          row(3, null, 3, 0L, 1L));

            // Range queries with wildcard and PER PARTITION LIMIT
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s GROUP BY a, b PER PARTITION LIMIT 1", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(2, 2, 3, 2, 3),
                       row(4, 8, 2, null, 12),
                       row(3, null, null, 3, null));

            // Range queries with PER PARTITION LIMIT and LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 3", pageSize),
                          row(1, 2, 1, 2L, 2L),
                          row(1, 4, 1, 2L, 2L),
                          row(2, 2, 2, 1L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 3", pageSize),
                          row(1, 2, 1, 2L, 2L),
                          row(2, 2, 2, 1L, 1L),
                          row(4, 8, null, 1L, 0L));

            // Range queries with wildcard, PER PARTITION LIMIT and LIMIT
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(2, 2, 3, 2, 3));

            // Range query without aggregates and with PER PARTITION LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s GROUP BY a, b PER PARTITION LIMIT 1", pageSize),
                          row(1, 2, 1),
                          row(2, 2, 2),
                          row(4, 8, null),
                          row(3, null, 3));

            // Range queries with DISTINCT
            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(a), count(s) FROM %s GROUP BY a", pageSize),
                          row(1, 1, 1L, 1L),
                          row(2, 2, 1L, 1L),
                          row(4, null, 1L, 0L),
                          row(3, 3, 1L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(a), count(s) FROM %s", pageSize),
                          row(1, 1, 4L, 3L));

            // Range queries with DISTINCT and LIMIT
            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(a), count(s) FROM %s GROUP BY a LIMIT 2",
                                               pageSize),
                          row(1, 1, 1L, 1L),
                          row(2, 2, 1L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(a), count(s) FROM %s LIMIT 2", pageSize),
                          row(1, 1, 4L, 3L));

            // Single partition queries
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 1 GROUP BY a",
                                               pageSize),
                          row(1, 2, 1, 4L, 4L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 3 GROUP BY a, b",
                                               pageSize),
                          row(3, null, 3, 0L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 3",
                                               pageSize),
                          row(3, null, 3, 0L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 AND b = 2 GROUP BY a, b",
                                               pageSize),
                          row(2, 2, 2, 1L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 AND b = 2",
                                               pageSize),
                          row(2, 2, 2, 1L, 1L));

            // Single partition queries without aggregates
            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s WHERE a = 1 GROUP BY a", pageSize),
                          row(1, 2, 1));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s WHERE a = 4 GROUP BY a, b", pageSize),
                          row(4, 8, null));

            // Single partition queries with wildcard
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 1 GROUP BY a", pageSize),
                       row(1, 2, 1, 1, 3));

            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a = 4 GROUP BY a, b", pageSize),
                       row(4, 8, 2, null, 12));

            // Single partition queries with LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b LIMIT 1",
                                               pageSize),
                          row(2, 2, 2, 1L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 LIMIT 1",
                                               pageSize),
                          row(2, 2, 2, 2L, 2L));

            // Single partition queries without aggregates and with LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s WHERE a = 2 GROUP BY a, b LIMIT 1", pageSize),
                          row(2, 2, 2));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s WHERE a = 2 GROUP BY a, b LIMIT 2", pageSize),
                          row(2, 2, 2),
                          row(2, 4, 2));

            // Single partition queries with DISTINCT
            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a = 2 GROUP BY a",
                                               pageSize),
                          row(2, 2, 1L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a = 4 GROUP BY a",
                                               pageSize),
                          row(4, null, 1L, 0L));

            // Single partition queries with ORDER BY
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC",
                                               pageSize),
                          row(2, 4, 2, 1L, 1L),
                          row(2, 2, 2, 1L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 ORDER BY b DESC, c DESC",
                                               pageSize),
                          row(2, 4, 2, 2L, 2L));

            // Single partition queries with ORDER BY and LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC LIMIT 1",
                                               pageSize),
                          row(2, 4, 2, 1L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a = 2 ORDER BY b DESC, c DESC LIMIT 2",
                                               pageSize),
                          row(2, 4, 2, 2L, 2L));

            // Multi-partitions queries
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a",
                                               pageSize),
                          row(1, 2, 1, 4L, 4L),
                          row(2, 2, 2, 2L, 2L),
                          row(3, null, 3, 0L, 1L),
                          row(4, 8, null, 1L, 0L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b",
                                               pageSize),
                          row(1, 2, 1, 2L, 2L),
                          row(1, 4, 1, 2L, 2L),
                          row(2, 2, 2, 1L, 1L),
                          row(2, 4, 2, 1L, 1L),
                          row(3, null, 3, 0L, 1L),
                          row(4, 8, null, 1L, 0L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4)",
                                               pageSize),
                          row(1, 2, 1, 7L, 7L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) AND b = 2 GROUP BY a, b",
                                               pageSize),
                          row(1, 2, 1, 2L, 2L),
                          row(2, 2, 2, 1L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) AND b = 2",
                                               pageSize),
                          row(1, 2, 1, 3L, 3L));

            // Multi-partitions queries without aggregates
            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a", pageSize),
                          row(1, 2, 1),
                          row(2, 2, 2),
                          row(3, null, 3),
                          row(4, 8, null));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b",
                                               pageSize),
                          row(1, 2, 1),
                          row(1, 4, 1),
                          row(2, 2, 2),
                          row(2, 4, 2),
                          row(3, null, 3),
                          row(4, 8, null));

            // Multi-partitions queries with wildcard
            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(2, 2, 3, 2, 3),
                       row(3, null, null, 3, null),
                       row(4, 8, 2, null, 12));

            assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b", pageSize),
                       row(1, 2, 1, 1, 3),
                       row(1, 4, 2, 1, 12),
                       row(2, 2, 3, 2, 3),
                       row(2, 4, 3, 2, 6),
                       row(3, null, null, 3, null),
                       row(4, 8, 2, null, 12));

            // Multi-partitions queries with LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2",
                                               pageSize),
                          row(1, 2, 1, 4L, 4L),
                          row(2, 2, 2, 2L, 2L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) LIMIT 2",
                                               pageSize),
                          row(1, 2, 1, 7L, 7L));

            // Multi-partitions queries without aggregates and with LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2",
                                               pageSize),
                          row(1, 2, 1),
                          row(2, 2, 2));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b LIMIT 10",
                                               pageSize),
                          row(1, 2, 1),
                          row(1, 4, 1),
                          row(2, 2, 2),
                          row(2, 4, 2),
                          row(3, null, 3),
                          row(4, 8, null));

            // Multi-partitions queries with PER PARTITION LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 1",
                                               pageSize),
                          row(1, 2, 1, 2L, 2L),
                          row(2, 2, 2, 1L, 1L),
                          row(3, null, 3, 0L, 1L),
                          row(4, 8, null, 1L, 0L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 3",
                                               pageSize),
                          row(1, 2, 1, 2L, 2L),
                          row(1, 4, 1, 2L, 2L),
                          row(2, 2, 2, 1L, 1L),
                          row(2, 4, 2, 1L, 1L),
                          row(3, null, 3, 0L, 1L),
                          row(4, 8, null, 1L, 0L));

            // Multi-partitions queries with PER PARTITION LIMIT and LIMIT
            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 3",
                                               pageSize),
                          row(1, 2, 1, 2L, 2L),
                          row(2, 2, 2, 1L, 1L),
                          row(3, null, 3, 0L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT a, b, s, count(b), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 3 LIMIT 10",
                                               pageSize),
                          row(1, 2, 1, 2L, 2L),
                          row(1, 4, 1, 2L, 2L),
                          row(2, 2, 2, 1L, 1L),
                          row(2, 4, 2, 1L, 1L),
                          row(3, null, 3, 0L, 1L),
                          row(4, 8, null, 1L, 0L));

            // Multi-partitions queries with DISTINCT
            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a",
                                               pageSize),
                          row(1, 1, 1L, 1L),
                          row(2, 2, 1L, 1L),
                          row(3, 3, 1L, 1L),
                          row(4, null, 1L, 0L));

            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a IN (1, 2, 3, 4)",
                                               pageSize),
                          row(1, 1, 4L, 3L));

            // Multi-partitions query with DISTINCT and LIMIT
            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2",
                                               pageSize),
                          row(1, 1, 1L, 1L),
                          row(2, 2, 1L, 1L));

            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s, count(a), count(s) FROM %s WHERE a IN (1, 2, 3, 4) LIMIT 2",
                                               pageSize),
                          row(1, 1, 4L, 3L));
        }
    }
}
