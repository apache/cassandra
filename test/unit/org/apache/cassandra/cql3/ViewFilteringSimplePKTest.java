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

package org.apache.cassandra.cql3;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

/* ViewFilteringTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670, CASSANDRA-17167)
 * Any changes here check if they apply to the other classes
 * - ViewFilteringPKTest
 * - ViewFilteringClustering1Test
 * - ViewFilteringClustering2Test
 * - ViewFilteringTest
 * - ...
 * - ViewFiltering*Test
 */
public class ViewFilteringSimplePKTest extends ViewAbstractParameterizedTest
{
    @Test
    public void testPartitionKeyFilteringUnrestrictedPart() throws Throwable
    {
        List<String> mvPrimaryKeys = Arrays.asList("((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)");
        for (int i = 0; i < mvPrimaryKeys.size(); i++)
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY ((a, b), c))");

            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0);

            logger.info("Testing MV primary key: {}", mvPrimaryKeys.get(i));

            // only accept rows where a = 1
            createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                             "WHERE a = 1 AND b IS NOT NULL AND c IS NOT NULL " +
                             "PRIMARY KEY " + mvPrimaryKeys.get(i));

            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 0, 0, 0),
                                    row(1, 0, 1, 0),
                                    row(1, 1, 0, 0),
                                    row(1, 1, 1, 0));

            // insert new rows that do not match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 1, 0, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 0, 0, 0),
                                    row(1, 0, 1, 0),
                                    row(1, 1, 0, 0),
                                    row(1, 1, 1, 0));

            // insert new row that does match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 2, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 0, 0, 0),
                                    row(1, 0, 1, 0),
                                    row(1, 1, 0, 0),
                                    row(1, 1, 1, 0),
                                    row(1, 1, 2, 0));

            // update rows that don't match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 0, 0, 0);
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 0, 1, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 0, 0, 0),
                                    row(1, 0, 1, 0),
                                    row(1, 1, 0, 0),
                                    row(1, 1, 1, 0),
                                    row(1, 1, 2, 0));

            // update a row that does match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, 1, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 0, 0, 0),
                                    row(1, 0, 1, 0),
                                    row(1, 1, 0, 1),
                                    row(1, 1, 1, 0),
                                    row(1, 1, 2, 0));

            // delete rows that don't match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 0, 0, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 0, 1, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ?", 0, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 0, 0, 0),
                                    row(1, 0, 1, 0),
                                    row(1, 1, 0, 1),
                                    row(1, 1, 1, 0),
                                    row(1, 1, 2, 0));

            // delete a row that does match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 0, 0, 0),
                                    row(1, 0, 1, 0),
                                    row(1, 1, 1, 0),
                                    row(1, 1, 2, 0));

            // delete a partition that matches the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ?", 1, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 1, 1, 0),
                                    row(1, 1, 2, 0));
            execute("DELETE FROM %s WHERE a = ? AND b = ?", 1, 1);
            assertEmpty(executeView("SELECT * FROM %s"));
        }
    }

    @Test
    public void testPartitionKeyFilteringWithSlice() throws Throwable
    {
        List<String> mvPrimaryKeys = Arrays.asList("((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)");
        for (int i = 0; i < mvPrimaryKeys.size(); i++)
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY ((a, b), c))");

            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0,  1, 1);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 10, 1, 2);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0,  2, 1);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 10, 2, 2);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 1,  3, 1);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 10, 3, 2);

            logger.info("Testing MV primary key: {}", mvPrimaryKeys.get(i));

            // only accept rows where a = 1
            createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                       "WHERE a > 0 AND b > 5 AND c IS NOT NULL " +
                       "PRIMARY KEY " + mvPrimaryKeys.get(i));

            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 10, 2, 2),
                                    row(2, 10, 3, 2));

            // insert new rows that do not match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 1, 0, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 10, 2, 2),
                                    row(2, 10, 3, 2));

            // insert new row that does match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 3, 10, 4, 2);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 10, 2, 2),
                                    row(2, 10, 3, 2),
                                    row(3, 10, 4, 2));

            // update rows that don't match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 0, 0, 0);
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 0, 1, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 10, 2, 2),
                                    row(2, 10, 3, 2),
                                    row(3, 10, 4, 2));

            // update a row that does match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 100, 3, 10, 4);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 10, 2, 2),
                                    row(2, 10, 3, 2),
                                    row(3, 10, 4, 100));

            // delete rows that don't match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 0, 0, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 0, 1, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ?", 0, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 10, 2, 2),
                                    row(2, 10, 3, 2),
                                    row(3, 10, 4, 100));

            // delete a row that does match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 10, 2, 2),
                                    row(2, 10, 3, 2),
                                    row(3, 10, 4, 100));

            // delete a partition that matches the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ?", 1, 10);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(2, 10, 3, 2),
                                    row(3, 10, 4, 100));
        }
    }

    @Test
    public void testPartitionKeyRestrictions() throws Throwable
    {
        List<String> mvPrimaryKeys = Arrays.asList("((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)");
        for (int i = 0; i < mvPrimaryKeys.size(); i++)
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");

            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0);

            logger.info("Testing MV primary key: {}", mvPrimaryKeys.get(i));

            // only accept rows where a = 1
            createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                       "WHERE a = 1 AND b IS NOT NULL AND c IS NOT NULL " +
                       "PRIMARY KEY " + mvPrimaryKeys.get(i));

            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 0, 0, 0),
                                    row(1, 0, 1, 0),
                                    row(1, 1, 0, 0),
                                    row(1, 1, 1, 0));

            // insert new rows that do not match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 1, 0, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 0, 0, 0),
                                    row(1, 0, 1, 0),
                                    row(1, 1, 0, 0),
                                    row(1, 1, 1, 0));

            // insert new row that does match the filter
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 2, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 0, 0, 0),
                                    row(1, 0, 1, 0),
                                    row(1, 1, 0, 0),
                                    row(1, 1, 1, 0),
                                    row(1, 1, 2, 0));

            // update rows that don't match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 0, 0, 0);
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 0, 1, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 0, 0, 0),
                                    row(1, 0, 1, 0),
                                    row(1, 1, 0, 0),
                                    row(1, 1, 1, 0),
                                    row(1, 1, 2, 0));

            // update a row that does match the filter
            execute("UPDATE %s SET d = ? WHERE a = ? AND b = ? AND c = ?", 1, 1, 1, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 0, 0, 0),
                                    row(1, 0, 1, 0),
                                    row(1, 1, 0, 1),
                                    row(1, 1, 1, 0),
                                    row(1, 1, 2, 0));

            // delete rows that don't match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 0, 0, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 0, 1, 0);
            execute("DELETE FROM %s WHERE a = ? AND b = ?", 0, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 0, 0, 0),
                                    row(1, 0, 1, 0),
                                    row(1, 1, 0, 1),
                                    row(1, 1, 1, 0),
                                    row(1, 1, 2, 0));

            // delete a row that does match the filter
            execute("DELETE FROM %s WHERE a = ? AND b = ? AND c = ?", 1, 1, 0);
            assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                    row(1, 0, 0, 0),
                                    row(1, 0, 1, 0),
                                    row(1, 1, 1, 0),
                                    row(1, 1, 2, 0));

            // delete a partition that matches the filter
            execute("DELETE FROM %s WHERE a = ?", 1);
            assertEmpty(executeView("SELECT * FROM %s"));
        }
    }
}
