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

import org.junit.Test;

public class MultiColumnRelationTest extends CQLTester
{
    @Test
    public void testSingleClusteringInvalidQueries() throws Throwable
    {
        for (String compactOption : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))" + compactOption);

            assertInvalidSyntax("SELECT * FROM %s WHERE () = (?, ?)", 1, 2);
            assertInvalidMessage("b cannot be restricted by more than one relation if it includes an Equal",
                                 "SELECT * FROM %s WHERE a = 0 AND (b) = (?) AND (b) > (?)", 0, 0);
            assertInvalidMessage("More than one restriction was found for the start bound on b",
                                 "SELECT * FROM %s WHERE a = 0 AND (b) > (?) AND (b) > (?)", 0, 1);
            assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: a",
                                 "SELECT * FROM %s WHERE (a, b) = (?, ?)", 0, 0);
        }
    }

    @Test
    public void testMultiClusteringInvalidQueries() throws Throwable
    {
        for (String compactOption : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c, d))" + compactOption);

            assertInvalidSyntax("SELECT * FROM %s WHERE a = 0 AND (b, c) > ()");
            assertInvalidMessage("Expected 2 elements in value tuple, but got 3: (?, ?, ?)",
                                 "SELECT * FROM %s WHERE a = 0 AND (b, c) > (?, ?, ?)", 1, 2, 3);
            assertInvalidMessage("Invalid null value in condition for column c",
                                 "SELECT * FROM %s WHERE a = 0 AND (b, c) > (?, ?)", 1, null);

            // Wrong order of columns
            assertInvalidMessage("Clustering columns may not be skipped in multi-column relations. They should appear in the PRIMARY KEY order. Got (d, c, b) = (?, ?, ?)",
                                 "SELECT * FROM %s WHERE a = 0 AND (d, c, b) = (?, ?, ?)", 0, 0, 0);
            assertInvalidMessage("Clustering columns may not be skipped in multi-column relations. They should appear in the PRIMARY KEY order. Got (d, c, b) > (?, ?, ?)",
                                 "SELECT * FROM %s WHERE a = 0 AND (d, c, b) > (?, ?, ?)", 0, 0, 0);

            // Wrong number of values
            assertInvalidMessage("Expected 3 elements in value tuple, but got 2: (?, ?)",
                                 "SELECT * FROM %s WHERE a=0 AND (b, c, d) IN ((?, ?))", 0, 1);
            assertInvalidMessage("Expected 3 elements in value tuple, but got 5: (?, ?, ?, ?, ?)",
                                 "SELECT * FROM %s WHERE a=0 AND (b, c, d) IN ((?, ?, ?, ?, ?))", 0, 1, 2, 3, 4);

            // Missing first clustering column
            assertInvalidMessage("Clustering columns may not be skipped in multi-column relations. They should appear in the PRIMARY KEY order. Got (c, d) = (?, ?)",
                                 "SELECT * FROM %s WHERE a = 0 AND (c, d) = (?, ?)", 0, 0);
            assertInvalidMessage("Clustering columns may not be skipped in multi-column relations. They should appear in the PRIMARY KEY order. Got (c, d) > (?, ?)",
                                 "SELECT * FROM %s WHERE a = 0 AND (c, d) > (?, ?)", 0, 0);

            // Nulls
            assertInvalidMessage("Invalid null value in condition for column d",
                                 "SELECT * FROM %s WHERE a = 0 AND (b, c, d) IN ((?, ?, ?))", 1, 2, null);

            // Wrong type for 'd'
            assertInvalid("SELECT * FROM %s WHERE a = 0 AND (b, c, d) = (?, ?, ?)", 1, 2, "foobar");
            assertInvalid("SELECT * FROM %s WHERE a = 0 AND b = (?, ?, ?)", 1, 2, 3);

            // Mix single and tuple inequalities
            assertInvalidMessage("Mixing single column relations and multi column relations on clustering columns is not allowed",
                                 "SELECT * FROM %s WHERE a = 0 AND (b, c, d) > (?, ?, ?) AND b < ?", 0, 1, 0, 1);
            assertInvalidMessage("Mixing single column relations and multi column relations on clustering columns is not allowed",
                                 "SELECT * FROM %s WHERE a = 0 AND (b, c, d) > (?, ?, ?) AND c < ?", 0, 1, 0, 1);
            assertInvalidMessage("Mixing single column relations and multi column relations on clustering columns is not allowed",
                                 "SELECT * FROM %s WHERE a = 0 AND b > ? AND (b, c, d) < (?, ?, ?)", 1, 1, 1, 0);
            assertInvalidMessage("Mixing single column relations and multi column relations on clustering columns is not allowed",
                                 "SELECT * FROM %s WHERE a = 0 AND c > ? AND (b, c, d) < (?, ?, ?)", 1, 1, 1, 0);

            assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: a",
                                 "SELECT * FROM %s WHERE (a, b, c, d) IN ((?, ?, ?, ?))", 0, 1, 2, 3);
            assertInvalidMessage("Clustering columns may not be skipped in multi-column relations. They should appear in the PRIMARY KEY order. Got (c, d) IN ((?, ?))",
                                 "SELECT * FROM %s WHERE (c, d) IN ((?, ?))", 0, 1);

            assertInvalidMessage("Mixing single column relations and multi column relations on clustering columns is not allowed",
                                 "SELECT * FROM %s WHERE a = ? AND (b, c) in ((?, ?), (?, ?)) AND d > ?",
                                 0, 0, 0, 0, 0, 0);

            assertInvalid("SELECT * FROM %s WHERE a = ? AND (b, c) in ((?, ?), (?, ?)) AND d > ?", 0, 0, 0, 0, 0, 0);
        }
    }

    @Test
    public void testSinglePartitionInvalidQueries() throws Throwable
    {
        for (String compactOption : new String[]{"", " WITH COMPACT STORAGE"})
        {
            createTable("CREATE TABLE %s (a int PRIMARY KEY, b int)" + compactOption);
            assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: a",
                                 "SELECT * FROM %s WHERE (a) > (?)", 0);
            assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: a",
                                 "SELECT * FROM %s WHERE (a) = (?)", 0);
            assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: b",
                                 "SELECT * FROM %s WHERE (b) = (?)", 0);
        }
    }

    @Test
    public void testSingleClustering() throws Throwable
    {
        for (String compactOption : new String[]{"", " WITH COMPACT STORAGE"})
        {
            createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))" + compactOption);

            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 0);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 2, 0);

            // Equalities

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) = (?)", 0, 1),
                    row(0, 1, 0)
            );

            // Same but check the whole tuple can be prepared
            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) = ?", 0, tuple(1)),
                    row(0, 1, 0)
            );

            assertEmpty(execute("SELECT * FROM %s WHERE a = ? AND (b) = (?)", 0, 3));

            // Inequalities

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) > (?)", 0, 0),
                    row(0, 1, 0),
                    row(0, 2, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) >= (?)", 0, 1),
                    row(0, 1, 0),
                    row(0, 2, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) < (?)", 0, 2),
                    row(0, 0, 0),
                    row(0, 1, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) <= (?)", 0, 1),
                    row(0, 0, 0),
                    row(0, 1, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) > (?) AND (b) < (?)", 0, 0, 2),
                    row(0, 1, 0)
            );
        }
    }

    @Test
    public void testNonEqualsRelation() throws Throwable
    {
        for (String compactOption : new String[]{"", " WITH COMPACT STORAGE"})
        {
            createTable("CREATE TABLE %s (a int PRIMARY KEY, b int)" + compactOption);
            assertInvalidMessage("Unsupported \"!=\" relation: (b) != (0)",
                    "SELECT * FROM %s WHERE a = 0 AND (b) != (0)");
        }
    }

    @Test
    public void testMultipleClustering() throws Throwable
    {
        for (String compactOption : new String[]{"", " WITH COMPACT STORAGE"})
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c, d))" + compactOption);

            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1);

            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 1);

            // Empty query
            assertEmpty(execute("SELECT * FROM %s WHERE a = 0 AND (b, c, d) IN ()"));

            // Equalities

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) = (?)", 0, 1),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(0, 1, 1, 1)
            );

            // Same with whole tuple prepared
            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) = ?", 0, tuple(1)),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(0, 1, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) = (?, ?)", 0, 1, 1),
                    row(0, 1, 1, 0),
                    row(0, 1, 1, 1)
            );

            // Same with whole tuple prepared
            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) = ?", 0, tuple(1, 1)),
                    row(0, 1, 1, 0),
                    row(0, 1, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) = (?, ?, ?)", 0, 1, 1, 1),
                    row(0, 1, 1, 1)
            );

            // Same with whole tuple prepared
            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) = ?", 0, tuple(1, 1, 1)),
                    row(0, 1, 1, 1)
            );

            // Inequalities

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) > (?)", 0, 0),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(0, 1, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) >= (?)", 0, 0),
                    row(0, 0, 0, 0),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(0, 1, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) > (?, ?)", 0, 1, 0),
                    row(0, 1, 1, 0),
                    row(0, 1, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) >= (?, ?)", 0, 1, 0),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(0, 1, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?)", 0, 1, 1, 0),
                    row(0, 1, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) >= (?, ?, ?)", 0, 1, 1, 0),
                    row(0, 1, 1, 0),
                    row(0, 1, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) < (?)", 0, 1),
                    row(0, 0, 0, 0),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) <= (?)", 0, 1),
                    row(0, 0, 0, 0),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 0),
                    row(0, 1, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) < (?, ?)", 0, 0, 1),
                    row(0, 0, 0, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) <= (?, ?)", 0, 0, 1),
                    row(0, 0, 0, 0),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) < (?, ?, ?)", 0, 0, 1, 1),
                    row(0, 0, 0, 0),
                    row(0, 0, 1, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) <= (?, ?, ?)", 0, 0, 1, 1),
                    row(0, 0, 0, 0),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND (b) < (?)", 0, 0, 1, 0, 1),
                    row(0, 0, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND (b, c) < (?, ?)", 0, 0, 1, 1, 1, 1),
                    row(0, 1, 0, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND (b, c, d) < (?, ?, ?)", 0, 0, 1, 1, 1, 1, 0),
                    row(0, 1, 0, 0)
            );

            // Same with whole tuple prepared
            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) > ? AND (b, c, d) < ?", 0, tuple(0, 1, 1), tuple(1, 1, 0)),
                    row(0, 1, 0, 0)
            );

            // reversed
            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) > (?) ORDER BY b DESC, c DESC, d DESC", 0, 0),
                    row(0, 1, 1, 1),
                    row(0, 1, 1, 0),
                    row(0, 1, 0, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) >= (?) ORDER BY b DESC, c DESC, d DESC", 0, 0),
                    row(0, 1, 1, 1),
                    row(0, 1, 1, 0),
                    row(0, 1, 0, 0),
                    row(0, 0, 1, 1),
                    row(0, 0, 1, 0),
                    row(0, 0, 0, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) > (?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 1, 0),
                    row(0, 1, 1, 1),
                    row(0, 1, 1, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) >= (?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 1, 0),
                    row(0, 1, 1, 1),
                    row(0, 1, 1, 0),
                    row(0, 1, 0, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 1, 1, 0),
                    row(0, 1, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) >= (?, ?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 1, 1, 0),
                    row(0, 1, 1, 1),
                    row(0, 1, 1, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) < (?) ORDER BY b DESC, c DESC, d DESC", 0, 1),
                    row(0, 0, 1, 1),
                    row(0, 0, 1, 0),
                    row(0, 0, 0, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) <= (?) ORDER BY b DESC, c DESC, d DESC", 0, 1),
                    row(0, 1, 1, 1),
                    row(0, 1, 1, 0),
                    row(0, 1, 0, 0),
                    row(0, 0, 1, 1),
                    row(0, 0, 1, 0),
                    row(0, 0, 0, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) < (?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1),
                    row(0, 0, 0, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) <= (?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1),
                    row(0, 0, 1, 1),
                    row(0, 0, 1, 0),
                    row(0, 0, 0, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) < (?, ?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1, 1),
                    row(0, 0, 1, 0),
                    row(0, 0, 0, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) <= (?, ?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1, 1),
                    row(0, 0, 1, 1),
                    row(0, 0, 1, 0),
                    row(0, 0, 0, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND (b) < (?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1, 0, 1),
                    row(0, 0, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND (b, c) < (?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1, 1, 1, 1),
                    row(0, 1, 0, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND (b, c, d) < (?, ?, ?) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1, 1, 1, 1, 0),
                    row(0, 1, 0, 0)
            );

            // IN

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) IN ((?, ?, ?), (?, ?, ?))", 0, 0, 1, 0, 0, 1, 1),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1)
            );

            // same query but with whole tuple prepared
            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) IN (?, ?)", 0, tuple(0, 1, 0), tuple(0, 1, 1)),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1)
            );

            // same query but with whole IN list prepared
            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) IN ?", 0, list(tuple(0, 1, 0), tuple(0, 1, 1))),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1)
            );

            // same query, but reversed order for the IN values
            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) IN (?, ?)", 0, tuple(0, 1, 1), tuple(0, 1, 0)),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? and (b, c) IN ((?, ?))", 0, 0, 1),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? and (b) IN ((?))", 0, 0),
                    row(0, 0, 0, 0),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1)
            );

            assertEmpty(execute("SELECT * FROM %s WHERE a = ? and (b) IN ()", 0));

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) IN ((?, ?)) ORDER BY b DESC, c DESC, d DESC", 0, 0, 1),
                    row(0, 0, 1, 1),
                    row(0, 0, 1, 0)
            );

            assertEmpty(execute("SELECT * FROM %s WHERE a = ? AND (b, c) IN () ORDER BY b DESC, c DESC, d DESC", 0));

            // IN on both partition key and clustering key
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 1);

            assertRows(execute("SELECT * FROM %s WHERE a IN (?, ?) AND (b, c, d) IN (?, ?)", 0, 1, tuple(0, 1, 0), tuple(0, 1, 1)),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1),
                    row(1, 0, 1, 0),
                    row(1, 0, 1, 1)
            );

            // same but with whole IN lists prepared
            assertRows(execute("SELECT * FROM %s WHERE a IN ? AND (b, c, d) IN ?", list(0, 1), list(tuple(0, 1, 0), tuple(0, 1, 1))),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1),
                    row(1, 0, 1, 0),
                    row(1, 0, 1, 1)
            );

            // same query, but reversed order for the IN values
            assertRows(execute("SELECT * FROM %s WHERE a IN (?, ?) AND (b, c, d) IN (?, ?)", 1, 0, tuple(0, 1, 1), tuple(0, 1, 0)),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1),
                    row(1, 0, 1, 0),
                    row(1, 0, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a IN (?, ?) and (b, c) IN ((?, ?))", 0, 1, 0, 1),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1),
                    row(1, 0, 1, 0),
                    row(1, 0, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a IN (?, ?) and (b) IN ((?))", 0, 1, 0),
                    row(0, 0, 0, 0),
                    row(0, 0, 1, 0),
                    row(0, 0, 1, 1),
                    row(1, 0, 0, 0),
                    row(1, 0, 1, 0),
                    row(1, 0, 1, 1)
            );
        }
    }

    @Test
    public void testMultipleClusteringReversedComponents() throws Throwable
    {
        for (String compactOption : new String[]{"", " COMPACT STORAGE AND"})
        {
            createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c, d)) WITH" + compactOption + " CLUSTERING ORDER BY (b DESC, c ASC, d DESC)");

            // b and d are reversed in the clustering order
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 1);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0);

            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0);


            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) > (?)", 0, 0),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 1),
                    row(0, 1, 1, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) >= (?)", 0, 0),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 1),
                    row(0, 1, 1, 0),
                    row(0, 0, 0, 0),
                    row(0, 0, 1, 1),
                    row(0, 0, 1, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) < (?)", 0, 1),
                    row(0, 0, 0, 0),
                    row(0, 0, 1, 1),
                    row(0, 0, 1, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) <= (?)", 0, 1),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 1),
                    row(0, 1, 1, 0),
                    row(0, 0, 0, 0),
                    row(0, 0, 1, 1),
                    row(0, 0, 1, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND (b, c, d) IN ((?, ?, ?), (?, ?, ?))", 0, 1, 1, 1, 0, 1, 1),
                    row(0, 1, 1, 1),
                    row(0, 0, 1, 1)
            );

            // same query, but reversed order for the IN values
            assertRows(execute("SELECT * FROM %s WHERE a=? AND (b, c, d) IN ((?, ?, ?), (?, ?, ?))", 0, 0, 1, 1, 1, 1, 1),
                    row(0, 1, 1, 1),
                    row(0, 0, 1, 1)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) IN (?, ?, ?, ?, ?, ?)",
                            0, tuple(1, 0, 0), tuple(1, 1, 1), tuple(1, 1, 0), tuple(0, 0, 0), tuple(0, 1, 1), tuple(0, 1, 0)),
                    row(0, 1, 0, 0),
                    row(0, 1, 1, 1),
                    row(0, 1, 1, 0),
                    row(0, 0, 0, 0),
                    row(0, 0, 1, 1),
                    row(0, 0, 1, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) IN (?)", 0, tuple(0, 1)),
                    row(0, 0, 1, 1),
                    row(0, 0, 1, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) IN (?)", 0, tuple(0, 0)),
                    row(0, 0, 0, 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) IN ((?))", 0, 0),
                    row(0, 0, 0, 0),
                    row(0, 0, 1, 1),
                    row(0, 0, 1, 0)
            );

            // preserve pre-6875 behavior (even though the query result is technically incorrect)
            assertEmpty(execute("SELECT * FROM %s WHERE a = ? AND (b, c) > (?, ?)", 0, 1, 0));
        }
    }

    @Test
    public void testMultipleClusteringWithIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY (a, b, c, d))");
        createIndex("CREATE INDEX ON %s (b)");
        createIndex("CREATE INDEX ON %s (e)");

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 0, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 1, 2);

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 1, 2);

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, 0, 0);

        assertRows(execute("SELECT * FROM %s WHERE (b) = (?)", 1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, 1, 2));

        assertRows(execute("SELECT * FROM %s WHERE (b, c) = (?, ?) ALLOW FILTERING", 1, 1),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, 1, 2));

        assertRows(execute("SELECT * FROM %s WHERE (b, c) = (?, ?) AND e = ? ALLOW FILTERING", 1, 1, 2),
                   row(0, 1, 1, 1, 2));

        assertRows(execute("SELECT * FROM %s WHERE (b) IN ((?)) AND e = ? ALLOW FILTERING", 1, 2),
                   row(0, 1, 1, 1, 2));

        assertInvalidMessage("IN restrictions are not supported on indexed columns",
                             "SELECT * FROM %s WHERE (b) IN ((?), (?)) AND e = ? ALLOW FILTERING", 0, 1, 2);

        assertInvalidMessage("IN restrictions are not supported on indexed columns",
                             "SELECT * FROM %s WHERE (b, c) IN ((?, ?)) AND e = ? ALLOW FILTERING", 0, 1, 2);

        assertInvalidMessage("IN restrictions are not supported on indexed columns",
                             "SELECT * FROM %s WHERE (b, c) IN ((?, ?), (?, ?)) AND e = ? ALLOW FILTERING", 0, 1, 1, 1, 2);

        assertInvalidMessage("Slice restrictions are not supported on indexed columns which are part of a multi column relation",
                             "SELECT * FROM %s WHERE (b) >= (?) AND e = ? ALLOW FILTERING", 1, 2);
    }

    @Test
    public void testMultiplePartitionKeyAndMultiClusteringWithIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY ((a, b), c, d, e))");
        createIndex("CREATE INDEX ON %s (c)");

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 1, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 1, 1);

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 1, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 1, 1);

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 2, 0, 0);

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (c) = (?) ALLOW FILTERING", 0, 1),
                   row(0, 0, 1, 0, 0),
                   row(0, 0, 1, 1, 0),
                   row(0, 0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (c, d) = (?, ?) ALLOW FILTERING", 0, 1, 1),
                   row(0, 0, 1, 1, 0),
                   row(0, 0, 1, 1, 1));

        assertInvalidMessage("Partition key parts: b must be restricted as other parts are",
                             "SELECT * FROM %s WHERE a = ? AND (c, d) IN ((?, ?)) ALLOW FILTERING", 0, 1, 1);

        assertInvalidMessage("Partition key parts: b must be restricted as other parts are",
                             "SELECT * FROM %s WHERE a = ? AND (c, d) >= (?, ?) ALLOW FILTERING", 0, 1, 1);
    }

    @Test
    public void testINWithDuplicateValue() throws Throwable
    {
        for (String compactOption : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (k1 int, k2 int, v int, PRIMARY KEY (k1, k2))" + compactOption);
            execute("INSERT INTO %s (k1,  k2, v) VALUES (?, ?, ?)", 1, 1, 1);

            assertRows(execute("SELECT * FROM %s WHERE k1 IN (?, ?) AND (k2) IN ((?), (?))", 1, 1, 1, 2),
                       row(1, 1, 1));
            assertRows(execute("SELECT * FROM %s WHERE k1 = ? AND (k2) IN ((?), (?))", 1, 1, 1),
                       row(1, 1, 1));
        }
    }
}
