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

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SelectMultiColumnRelationTest extends CQLTester
{
    private static final ByteBuffer TOO_BIG = ByteBuffer.allocate(1024 * 65);

    @Test
    public void testSingleClusteringInvalidQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");

        assertInvalidSyntax("SELECT * FROM %s WHERE () = (?, ?)", 1, 2);
        assertInvalidMessage("b cannot be restricted by more than one relation if it includes an Equal",
                             "SELECT * FROM %s WHERE a = 0 AND (b) = (?) AND (b) > (?)", 0, 0);
        assertInvalidMessage("More than one restriction was found for the start bound on b",
                             "SELECT * FROM %s WHERE a = 0 AND (b) > (?) AND (b) > (?)", 0, 1);
        assertInvalidMessage("More than one restriction was found for the start bound on b",
                             "SELECT * FROM %s WHERE a = 0 AND (b) > (?) AND b > ?", 0, 1);
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a, b) = (?, ?)", 0, 0);
    }

    @Test
    public void testMultiClusteringInvalidQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c, d))");

        assertInvalidSyntax("SELECT * FROM %s WHERE a = 0 AND (b, c) > ()");
        assertInvalidMessage("Expected 2 elements in value tuple, but got 3: (?, ?, ?)",
                             "SELECT * FROM %s WHERE a = 0 AND (b, c) > (?, ?, ?)", 1, 2, 3);
        assertInvalidMessage("Invalid null value in condition for column c",
                             "SELECT * FROM %s WHERE a = 0 AND (b, c) > (?, ?)", 1, null);

        // Wrong order of columns
        assertInvalidMessage("Clustering columns must appear in the PRIMARY KEY order in multi-column relations: (d, c, b) = (?, ?, ?)",
                             "SELECT * FROM %s WHERE a = 0 AND (d, c, b) = (?, ?, ?)", 0, 0, 0);
        assertInvalidMessage("Clustering columns must appear in the PRIMARY KEY order in multi-column relations: (d, c, b) > (?, ?, ?)",
                             "SELECT * FROM %s WHERE a = 0 AND (d, c, b) > (?, ?, ?)", 0, 0, 0);

        // Wrong number of values
        assertInvalidMessage("Expected 3 elements in value tuple, but got 2: (?, ?)",
                             "SELECT * FROM %s WHERE a=0 AND (b, c, d) IN ((?, ?))", 0, 1);
        assertInvalidMessage("Expected 3 elements in value tuple, but got 5: (?, ?, ?, ?, ?)",
                             "SELECT * FROM %s WHERE a=0 AND (b, c, d) IN ((?, ?, ?, ?, ?))", 0, 1, 2, 3, 4);

        // Missing first clustering column
        assertInvalidMessage("PRIMARY KEY column \"c\" cannot be restricted as preceding column \"b\" is not restricted",
                             "SELECT * FROM %s WHERE a = 0 AND (c, d) = (?, ?)", 0, 0);
        assertInvalidMessage("PRIMARY KEY column \"c\" cannot be restricted as preceding column \"b\" is not restricted",
                             "SELECT * FROM %s WHERE a = 0 AND (c, d) > (?, ?)", 0, 0);

        // Nulls
        assertInvalidMessage("Invalid null value for column d",
                             "SELECT * FROM %s WHERE a = 0 AND (b, c, d) = (?, ?, ?)", 1, 2, null);
        assertInvalidMessage("Invalid null value for column d",
                             "SELECT * FROM %s WHERE a = 0 AND (b, c, d) IN ((?, ?, ?))", 1, 2, null);
        assertInvalidMessage("Invalid null value in condition for columns: [b, c, d]",
                             "SELECT * FROM %s WHERE a = 0 AND (b, c, d) IN ((?, ?, ?), (?, ?, ?))", 1, 2, null, 2, 1, 4);

        // Wrong type for 'd'
        assertInvalid("SELECT * FROM %s WHERE a = 0 AND (b, c, d) = (?, ?, ?)", 1, 2, "foobar");
        assertInvalid("SELECT * FROM %s WHERE a = 0 AND b = (?, ?, ?)", 1, 2, 3);

        // Mix single and tuple inequalities
        assertInvalidMessage("Column \"c\" cannot be restricted by two inequalities not starting with the same column",
                             "SELECT * FROM %s WHERE a = 0 AND (b, c, d) > (?, ?, ?) AND c < ?", 0, 1, 0, 1);
        assertInvalidMessage("Column \"c\" cannot be restricted by two inequalities not starting with the same column",
                             "SELECT * FROM %s WHERE a = 0 AND c > ? AND (b, c, d) < (?, ?, ?)", 1, 1, 1, 0);

        assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a, b, c, d) IN ((?, ?, ?, ?))", 0, 1, 2, 3);
        assertInvalidMessage("PRIMARY KEY column \"c\" cannot be restricted as preceding column \"b\" is not restricted",
                             "SELECT * FROM %s WHERE (c, d) IN ((?, ?))", 0, 1);

        assertInvalidMessage("Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                             "SELECT * FROM %s WHERE a = ? AND b > ?  AND (c, d) IN ((?, ?))", 0, 0, 0, 0);

        assertInvalidMessage("Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                             "SELECT * FROM %s WHERE a = ? AND b > ?  AND (c, d) > (?, ?)", 0, 0, 0, 0);
        assertInvalidMessage("PRIMARY KEY column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                             "SELECT * FROM %s WHERE a = ? AND (c, d) > (?, ?) AND b > ?  ", 0, 0, 0, 0);

        assertInvalidMessage("Column \"c\" cannot be restricted by two inequalities not starting with the same column",
                             "SELECT * FROM %s WHERE a = ? AND (b, c) > (?, ?) AND (b) < (?) AND (c) < (?)", 0, 0, 0, 0, 0);
        assertInvalidMessage("Column \"c\" cannot be restricted by two inequalities not starting with the same column",
                             "SELECT * FROM %s WHERE a = ? AND (c) < (?) AND (b, c) > (?, ?) AND (b) < (?)", 0, 0, 0, 0, 0);
        assertInvalidMessage("Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                             "SELECT * FROM %s WHERE a = ? AND (b) < (?) AND (c) < (?) AND (b, c) > (?, ?)", 0, 0, 0, 0, 0);
        assertInvalidMessage("Clustering column \"c\" cannot be restricted (preceding column \"b\" is restricted by a non-EQ relation)",
                             "SELECT * FROM %s WHERE a = ? AND (b) < (?) AND c < ? AND (b, c) > (?, ?)", 0, 0, 0, 0, 0);

        assertInvalidMessage("Column \"c\" cannot be restricted by two inequalities not starting with the same column",
                             "SELECT * FROM %s WHERE a = ? AND (b, c) > (?, ?) AND (c) < (?)", 0, 0, 0, 0);
    }

    @Test
    public void testMultiAndSingleColumnRelationMix() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c, d))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1);

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 1);

        assertRows(execute("SELECT * FROM %s WHERE a = ? and b = ? and (c, d) = (?, ?)", 0, 1, 0, 0),
                   row(0, 1, 0, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and b IN (?, ?) and (c, d) = (?, ?)", 0, 0, 1, 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 1, 0, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and b = ? and (c) IN ((?))", 0, 1, 0),
                   row(0, 1, 0, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and b IN (?, ?) and (c) IN ((?))", 0, 0, 1, 0),
                   row(0, 0, 0, 0),
                   row(0, 1, 0, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and b = ? and (c) IN ((?), (?))", 0, 1, 0, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and b = ? and (c, d) IN ((?, ?))", 0, 1, 0, 0),
                   row(0, 1, 0, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and b = ? and (c, d) IN ((?, ?), (?, ?))", 0, 1, 0, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and b IN (?, ?) and (c, d) IN ((?, ?), (?, ?))", 0, 0, 1, 0, 0, 1, 1),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and b = ? and (c, d) > (?, ?)", 0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and b IN (?, ?) and (c, d) > (?, ?)", 0, 0, 1, 0, 0),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and b = ? and (c, d) > (?, ?) and (c) <= (?) ", 0, 1, 0, 0, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and b = ? and (c, d) > (?, ?) and c <= ? ", 0, 1, 0, 0, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and b = ? and (c, d) >= (?, ?) and (c, d) < (?, ?)", 0, 1, 0, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b, c) = (?, ?) and d = ?", 0, 0, 1, 0),
                   row(0, 0, 1, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b, c) IN ((?, ?), (?, ?)) and d = ?", 0, 0, 1, 0, 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and b = ? and (c) = (?) and d = ?", 0, 0, 1, 0),
                   row(0, 0, 1, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b, c) = (?, ?) and d IN (?, ?)", 0, 0, 1, 0, 2),
                   row(0, 0, 1, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and b = ? and (c) = (?) and d IN (?, ?)", 0, 0, 1, 0, 2),
                   row(0, 0, 1, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b, c) = (?, ?) and d >= ?", 0, 0, 1, 0),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and d < 1 and (b, c) = (?, ?) and d >= ?", 0, 0, 1, 0),
                   row(0, 0, 1, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and d < 1 and (b, c) IN ((?, ?), (?, ?)) and d >= ?", 0, 0, 1, 0, 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0));
    }

    @Test
    public void testSeveralMultiColumnRelation() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c, d))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1);

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 1);

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b) = (?) and (c, d) = (?, ?)", 0, 1, 0, 0),
                   row(0, 1, 0, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b) IN ((?), (?)) and (c, d) = (?, ?)", 0, 0, 1, 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 1, 0, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b) = (?) and (c) IN ((?))", 0, 1, 0),
                   row(0, 1, 0, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b) IN ((?),(?)) and (c) IN ((?))", 0, 0, 1, 0),
                   row(0, 0, 0, 0),
                   row(0, 1, 0, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b) = (?) and (c) IN ((?), (?))", 0, 1, 0, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b) = (?) and (c, d) IN ((?, ?))", 0, 1, 0, 0),
                   row(0, 1, 0, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b) = (?) and (c, d) IN ((?, ?), (?, ?))", 0, 1, 0, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b) IN ((?), (?)) and (c, d) IN ((?, ?), (?, ?))", 0, 0, 1, 0, 0, 1, 1),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b) = (?) and (c, d) > (?, ?)", 0, 1, 0, 0),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b) IN ((?),(?)) and (c, d) > (?, ?)", 0, 0, 1, 0, 0),
                   row(0, 0, 1, 0),
                   row(0, 0, 1, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b) = (?) and (c, d) > (?, ?) and (c) <= (?) ", 0, 1, 0, 0, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b) = (?) and (c, d) > (?, ?) and c <= ? ", 0, 1, 0, 0, 1),
                   row(0, 1, 1, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b) = (?) and (c, d) >= (?, ?) and (c, d) < (?, ?)", 0, 1, 0, 0, 1, 1),
                   row(0, 1, 0, 0),
                   row(0, 1, 1, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b, c) = (?, ?) and d = ?", 0, 0, 1, 0),
                   row(0, 0, 1, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (b, c) IN ((?, ?), (?, ?)) and d = ?", 0, 0, 1, 0, 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (d) < (1) and (b, c) = (?, ?) and (d) >= (?)", 0, 0, 1, 0),
                   row(0, 0, 1, 0));

        assertRows(execute("SELECT * FROM %s WHERE a = ? and (d) < (1) and (b, c) IN ((?, ?), (?, ?)) and (d) >= (?)", 0, 0, 1, 0, 0, 0),
                   row(0, 0, 0, 0),
                   row(0, 0, 1, 0));
    }

    @Test
    public void testSinglePartitionInvalidQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int)");
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a) > (?)", 0);
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a) = (?)", 0);
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: b",
                             "SELECT * FROM %s WHERE (b) = (?)", 0);
    }

    @Test
    public void testSingleClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");

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

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) > (?) AND b < ?", 0, 0, 2),
                   row(0, 1, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b > ? AND (b) < (?)", 0, 0, 2),
                   row(0, 1, 0)
        );
    }

    @Test
    public void testNonEqualsRelation() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int)");
        assertInvalidMessage("Unsupported \"!=\" relation: (b) != (0)",
                             "SELECT * FROM %s WHERE a = 0 AND (b) != (0)");
    }

    @Test
    public void testMultipleClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c, d))");

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

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND b < ?", 0, 0, 1, 0, 1),
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

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c, d) > (?, ?, ?) AND b < ? ORDER BY b DESC, c DESC, d DESC", 0, 0, 1, 0, 1),
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

    @Test
    public void testMultipleClusteringReversedComponents() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c, d)) WITH CLUSTERING ORDER BY (b DESC, c ASC, d DESC)");

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

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) > (?, ?)", 0, 1, 0),
                   row(0, 1, 1, 1),
                   row(0, 1, 1, 0)
        );
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
        assertRows(execute("SELECT * FROM %s WHERE a= ? AND (b) = (?)", 0, 1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, 1, 2));
        assertRows(execute("SELECT * FROM %s WHERE (b) = (?)", 1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, 1, 2));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b, c) = (?, ?)", 1, 1);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) = (?, ?)", 0, 1, 1),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, 1, 2));
        assertRows(execute("SELECT * FROM %s WHERE (b, c) = (?, ?) ALLOW FILTERING", 1, 1),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, 1, 2));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b, c) = (?, ?) AND e = ?", 0, 1, 1, 2),
                   row(0, 1, 1, 1, 2));
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b, c) = (?, ?) AND e = ?", 1, 1, 2);
        assertRows(execute("SELECT * FROM %s WHERE (b, c) = (?, ?) AND e = ? ALLOW FILTERING", 1, 1, 2),
                   row(0, 1, 1, 1, 2));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b) IN ((?)) AND e = ? ALLOW FILTERING", 0, 1, 2),
                   row(0, 1, 1, 1, 2));
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b) IN ((?)) AND e = ?", 1, 2);
        assertRows(execute("SELECT * FROM %s WHERE (b) IN ((?)) AND e = ? ALLOW FILTERING", 1, 2),
                   row(0, 1, 1, 1, 2));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b) IN ((?), (?)) AND e = ?", 0, 1, 2);
        assertRows(execute("SELECT * FROM %s WHERE (b) IN ((?), (?)) AND e = ? ALLOW FILTERING", 0, 1, 2),
                   row(0, 0, 1, 1, 2),
                   row(0, 1, 1, 1, 2));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b, c) IN ((?, ?)) AND e = ?", 0, 1, 2);
        assertRows(execute("SELECT * FROM %s WHERE (b, c) IN ((?, ?)) AND e = ? ALLOW FILTERING", 0, 1, 2),
                   row(0, 0, 1, 1, 2));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b, c) IN ((?, ?), (?, ?)) AND e = ?", 0, 1, 1, 1, 2);
        assertRows(execute("SELECT * FROM %s WHERE (b, c) IN ((?, ?), (?, ?)) AND e = ? ALLOW FILTERING", 0, 1, 1, 1, 2),
                   row(0, 0, 1, 1, 2),
                   row(0, 1, 1, 1, 2));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b) >= (?) AND e = ?", 1, 2);
        assertRows(execute("SELECT * FROM %s WHERE (b) >= (?) AND e = ? ALLOW FILTERING", 1, 2),
                   row(0, 1, 1, 1, 2));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE (b, c) >= (?, ?) AND e = ?", 1, 1, 2);
        assertRows(execute("SELECT * FROM %s WHERE (b, c) >= (?, ?) AND e = ? ALLOW FILTERING", 1, 1, 2),
                   row(0, 1, 1, 1, 2));

        assertInvalidMessage("Unsupported null value for column e",
                             "SELECT * FROM %s WHERE (b, c) >= (?, ?) AND e = ?  ALLOW FILTERING", 1, 1, null);

        assertInvalidMessage("Unsupported unset value for column e",
                             "SELECT * FROM %s WHERE (b, c) >= (?, ?) AND e = ?  ALLOW FILTERING", 1, 1, unset());
    }

    @Test
    public void testMultipleClusteringWithIndexAndValueOver64K() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b blob, c int, d int, PRIMARY KEY (a, b, c))");
        createIndex("CREATE INDEX ON %s (b)");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, ByteBufferUtil.bytes(1), 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, ByteBufferUtil.bytes(2), 1, 0);

        assertInvalidMessage("Index expression values may not be larger than 64K",
                             "SELECT * FROM %s WHERE (b, c) = (?, ?) AND d = ?  ALLOW FILTERING", TOO_BIG, 1, 2);
    }

    @Test
    public void testMultiColumnRestrictionsWithIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, v int, PRIMARY KEY (a, b, c, d, e))");
        createIndex("CREATE INDEX ON %s (v)");
        for (int i = 1; i <= 5; i++)
        {
            execute("INSERT INTO %s (a,b,c,d,e,v) VALUES (?,?,?,?,?,?)", 0, i, 0, 0, 0, 0);
            execute("INSERT INTO %s (a,b,c,d,e,v) VALUES (?,?,?,?,?,?)", 0, i, i, 0, 0, 0);
            execute("INSERT INTO %s (a,b,c,d,e,v) VALUES (?,?,?,?,?,?)", 0, i, i, i, 0, 0);
            execute("INSERT INTO %s (a,b,c,d,e,v) VALUES (?,?,?,?,?,?)", 0, i, i, i, i, 0);
            execute("INSERT INTO %s (a,b,c,d,e,v) VALUES (?,?,?,?,?,?)", 0, i, i, i, i, i);
        }

        String errorMsg = "Multi-column slice restrictions cannot be used for filtering.";
        assertInvalidMessage(errorMsg,
                             "SELECT * FROM %s WHERE a = 0 AND (c,d) < (2,2) AND v = 0 ALLOW FILTERING");
        assertInvalidMessage(errorMsg,
                             "SELECT * FROM %s WHERE a = 0 AND (d,e) < (2,2) AND b = 1 AND v = 0 ALLOW FILTERING");
        assertInvalidMessage(errorMsg,
                             "SELECT * FROM %s WHERE a = 0 AND b = 1 AND (d,e) < (2,2) AND v = 0 ALLOW FILTERING");
        assertInvalidMessage(errorMsg,
                             "SELECT * FROM %s WHERE a = 0 AND b > 1 AND (d,e) < (2,2) AND v = 0 ALLOW FILTERING");
        assertInvalidMessage(errorMsg,
                             "SELECT * FROM %s WHERE a = 0 AND (b,c) > (1,0) AND (d,e) < (2,2) AND v = 0 ALLOW FILTERING");
    }

    @Test
    public void testMultiplePartitionKeyAndMultiClusteringWithIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, f int, PRIMARY KEY ((a, b), c, d, e))");
        createIndex("CREATE INDEX ON %s (c)");
        createIndex("CREATE INDEX ON %s (f)");

        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 0, 1, 0, 1);
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 0, 1, 1, 2);

        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 1, 0, 0, 3);
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 1, 1, 0, 4);
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 1, 1, 1, 5);

        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 2, 0, 0, 5);

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c) = (?)");
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (c) = (?) ALLOW FILTERING", 0, 1),
                   row(0, 0, 1, 0, 0, 3),
                   row(0, 0, 1, 1, 0, 4),
                   row(0, 0, 1, 1, 1, 5));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c, d) = (?, ?)", 0, 1, 1);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (c, d) = (?, ?) ALLOW FILTERING", 0, 1, 1),
                   row(0, 0, 1, 1, 0, 4),
                   row(0, 0, 1, 1, 1, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (c, d) IN ((?, ?)) ALLOW FILTERING", 0, 1, 1),
                row(0, 0, 1, 1, 0, 4),
                row(0, 0, 1, 1, 1, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (c, d) >= (?, ?) ALLOW FILTERING", 0, 1, 1),
                row(0, 0, 1, 1, 0, 4),
                row(0, 0, 1, 1, 1, 5),
                row(0, 0, 2, 0, 0, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b = ? AND (c) IN ((?)) AND f = ?", 0, 0, 1, 5),
                   row(0, 0, 1, 1, 1, 5));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c) IN ((?), (?)) AND f = ?", 0, 1, 3, 5);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (c) IN ((?), (?)) AND f = ? ALLOW FILTERING", 0, 1, 3, 5),
                   row(0, 0, 1, 1, 1, 5));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c) IN ((?), (?)) AND f = ?", 0, 1, 2, 5);

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b = ? AND (c) IN ((?), (?)) AND f = ?", 0, 0, 1, 2, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (c) IN ((?), (?)) AND f = ? ALLOW FILTERING", 0, 1, 2, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b = ? AND (c, d) IN ((?, ?)) AND f = ?", 0, 0, 1, 0, 3),
                   row(0, 0, 1, 0, 0, 3));
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c, d) IN ((?, ?)) AND f = ?", 0, 1, 0, 3);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (c, d) IN ((?, ?)) AND f = ? ALLOW FILTERING", 0, 1, 0, 3),
                   row(0, 0, 1, 0, 0, 3));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c) >= (?) AND f = ?", 0, 1, 5);

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b = ? AND (c) >= (?) AND f = ?", 0, 0, 1, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (c) >= (?) AND f = ? ALLOW FILTERING", 0, 1, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b = ? AND (c, d) >= (?, ?) AND f = ?", 0, 0, 1, 1, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5));
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND (c, d) >= (?, ?) AND f = ?", 0, 1, 1, 5);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (c, d) >= (?, ?) AND f = ? ALLOW FILTERING", 0, 1, 1, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5));
    }

    @Test
    public void testINWithDuplicateValue() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v int, PRIMARY KEY (k1, k2))");
        execute("INSERT INTO %s (k1,  k2, v) VALUES (?, ?, ?)", 1, 1, 1);

        assertRows(execute("SELECT * FROM %s WHERE k1 IN (?, ?) AND (k2) IN ((?), (?))", 1, 1, 1, 2),
                   row(1, 1, 1));
        assertRows(execute("SELECT * FROM %s WHERE k1 = ? AND (k2) IN ((?), (?))", 1, 1, 1),
                   row(1, 1, 1));
    }

    @Test
    public void testWithUnsetValues() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, i int, j int, s text, PRIMARY KEY(k,i,j))");
        createIndex("CREATE INDEX s_index ON %s (s)");

        assertInvalidMessage("Invalid unset value for tuple field number 0",
                             "SELECT * from %s WHERE (i, j) = (?,?) ALLOW FILTERING", unset(), 1);
        assertInvalidMessage("Invalid unset value for tuple field number 0",
                             "SELECT * from %s WHERE (i, j) IN ((?,?)) ALLOW FILTERING", unset(), 1);
        assertInvalidMessage("Invalid unset value for tuple field number 1",
                             "SELECT * from %s WHERE (i, j) > (1,?) ALLOW FILTERING", unset());
        assertInvalidMessage("Invalid unset value for tuple (i,j)",
                             "SELECT * from %s WHERE (i, j) = ? ALLOW FILTERING", unset());
        assertInvalidMessage("Invalid unset value for tuple (j)",
                             "SELECT * from %s WHERE i = ? AND (j) > ? ALLOW FILTERING", 1, unset());
        assertInvalidMessage("Invalid unset value for tuple (i,j)",
                             "SELECT * from %s WHERE (i, j) IN (?, ?) ALLOW FILTERING", unset(), tuple(1, 1));
        assertInvalidMessage("Invalid unset value for in(i,j)",
                             "SELECT * from %s WHERE (i, j) IN ? ALLOW FILTERING", unset());
    }

    @Test
    public void testMixedOrderColumns1() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY (a, b, c, d, e)) WITH " +
                    " CLUSTERING ORDER BY (b DESC, c ASC, d DESC, e ASC)");

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, -1, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, -1, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, 1, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 1, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 1, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 1, -1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 1, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, -1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, -1, -1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, -1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, -1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, -1, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, -1, 0, -1, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, -1, 0, 0, 0);
        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<=(?,?,?,?) " +
        "AND (b)>(?)", 0, 2, 0, 1, 1, -1),

                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0)
        );


        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<=(?,?,?,?) " +
        "AND (b)>=(?)", 0, 2, 0, 1, 1, -1),

                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d)>=(?,?,?)" +
        "AND (b,c,d,e)<(?,?,?,?) ", 0, 1, 1, 0, 1, 1, 0, 1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0)

        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)>(?,?,?,?)" +
        "AND (b,c,d)<=(?,?,?) ", 0, -1, 0, -1, -1, 2, 0, -1),

                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e) < (?,?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 1, 0, 0, 0, 1, 0, -1, -1),
                   row(0, 1, 0, 0, -1)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e) <= (?,?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 1, 0, 0, 0, 1, 0, -1, -1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b)<(?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, -1, 0, -1, -1),

                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)

        );


        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b)<(?) " +
        "AND (b)>(?)", 0, 2, -1),

                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0)

        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b)<(?) " +
        "AND (b)>=(?)", 0, 2, -1),

                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<=(?,?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, 1, 1, -1, 0, -1, -1),

                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c)<=(?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, -1, 0, -1, -1),

                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d)<=(?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, -1, -1, 0, -1, -1),

                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)>(?,?,?,?)" +
        "AND (b,c,d)<=(?,?,?) ", 0, -1, 0, -1, -1, 2, 0, -1),

                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d)>=(?,?,?)" +
        "AND (b,c,d,e)<(?,?,?,?) ", 0, 1, 1, 0, 1, 1, 0, 1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0)
        );
        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<(?,?,?,?) " +
        "AND (b,c,d)>=(?,?,?)", 0, 1, 1, 0, 1, 1, 1, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0)

        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c)<(?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, -1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c)<(?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, -1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c,d,e) <= (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, -1, -1),
                   row(0, 0, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c,d,e) > (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c,d,e) >= (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c,d) >= (?,?,?)", 0, 1, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c,d) > (?,?,?)", 0, 1, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0)
        );
    }

    @Test
    public void testMixedOrderColumns2() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY (a, b, c, d, e)) WITH " +
                    "CLUSTERING ORDER BY (b DESC, c ASC, d ASC, e ASC)");

        // b and d are reversed in the clustering order
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, -1, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, -1, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 1, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 1, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 1, -1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 1, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, -1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, -1, -1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, -1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, -1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, -1, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0);

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c,d,e) <= (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 1, -1, 0, 0),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 0, 0, 0, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c,d,e) > (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1)
        );
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c,d,e) >= (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1)
        );
    }

    @Test
    public void testMixedOrderColumns3() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b, c)) WITH " +
                    "CLUSTERING ORDER BY (b DESC, c ASC)");

        execute("INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 2, 3);
        execute("INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 2, 4);
        execute("INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 4, 4);
        execute("INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 3, 4);
        execute("INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 4, 5);
        execute("INSERT INTO %s (a, b, c) VALUES (?,?,?);", 0, 4, 6);


        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c)>=(?,?) AND (b,c)<(?,?) ALLOW FILTERING", 0, 2, 3, 4, 5),
                   row(0, 4, 4), row(0, 3, 4), row(0, 2, 3), row(0, 2, 4)
        );
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c)>=(?,?) AND (b,c)<=(?,?) ALLOW FILTERING", 0, 2, 3, 4, 5),
                   row(0, 4, 4), row(0, 4, 5), row(0, 3, 4), row(0, 2, 3), row(0, 2, 4)
        );
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c)<(?,?) ALLOW FILTERING", 0, 4, 5),
                   row(0, 4, 4), row(0, 3, 4), row(0, 2, 3), row(0, 2, 4)
        );

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c)>(?,?) ALLOW FILTERING", 0, 4, 5),
                   row(0, 4, 6)
        );

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b)<(?) and (b)>(?) ALLOW FILTERING", 0, 4, 2),
                   row(0, 3, 4)
        );
    }

    @Test
    public void testMixedOrderColumns4() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY (a, b, c, d, e)) WITH " +
                    "CLUSTERING ORDER BY (b ASC, c DESC, d DESC, e ASC)");

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, -1, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, -1, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, 0, 1, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, -1, 1, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 2, -3, 1, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 1, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, -1, 1, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 1, -1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 1, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, -1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 0, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, -1, -1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, -1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, -1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 0, 1);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, -1, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, -1, 0, -1, 0);
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)", 0, -1, 0, 0, 0);

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<(?,?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, 1, 1, -1, 0, -1, -1),

                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)

        );


        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e) < (?,?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 1, 0, 0, 0, 1, 0, -1, -1),
                   row(0, 1, 0, 0, -1)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e) <= (?,?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 1, 0, 0, 0, 1, 0, -1, -1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0)
        );


        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<=(?,?,?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, 1, 1, -1, 0, -1, -1),

                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c)<=(?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, -1, 0, -1, -1),

                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c)<(?,?) " +
        "AND (b,c,d,e)>(?,?,?,?)", 0, 2, 0, -1, 0, -1, -1),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<=(?,?,?,?) " +
        "AND (b)>=(?)", 0, 2, 0, 1, 1, -1),

                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e)<=(?,?,?,?) " +
        "AND (b)>(?)", 0, 2, 0, 1, 1, -1),

                   row(0, 0, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        );

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c,d,e) <= (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, -1, 0, 0, 0),
                   row(0, -1, 0, -1, 0),
                   row(0, 0, 0, 0, 0),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, -1, -1),
                   row(0, 1, -1, 1, 0),
                   row(0, 1, -1, 1, 1),
                   row(0, 1, -1, 0, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c,d,e) > (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, 1),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)

        );

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c,d,e) >= (?,?,?,?)", 0, 1, 0, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        );

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c,d) >= (?,?,?)", 0, 1, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 1, 0, 0, -1),
                   row(0, 1, 0, 0, 0),
                   row(0, 1, 0, 0, 1),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        );

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (b,c,d) > (?,?,?)", 0, 1, 0, 0),
                   row(0, 1, 1, 0, -1),
                   row(0, 1, 1, 0, 0),
                   row(0, 1, 1, 0, 1),
                   row(0, 1, 1, -1, 0),
                   row(0, 1, 0, 1, -1),
                   row(0, 1, 0, 1, 1),
                   row(0, 2, 0, 1, 1),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1),
                   row(0, 2, -3, 1, 1)
        );

        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b) < (?) ", 0, 0),
                   row(0, -1, 0, 0, 0), row(0, -1, 0, -1, 0)
        );
        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b) <= (?) ", 0, -1),
                   row(0, -1, 0, 0, 0), row(0, -1, 0, -1, 0)
        );
        assertRows(execute(
        "SELECT * FROM %s" +
        " WHERE a = ? " +
        "AND (b,c,d,e) < (?,?,?,?) and (b,c,d,e) > (?,?,?,?) ", 0, 2, 0, 0, 0, 2, -2, 0, 0),
                   row(0, 2, 0, -1, 0),
                   row(0, 2, 0, -1, 1),
                   row(0, 2, -1, 1, 1)
        );
    }

    @Test
    public void testMixedOrderColumnsInReverse() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b, c)) WITH CLUSTERING ORDER BY (b ASC, c DESC);");

        execute("INSERT INTO %s (a, b, c) VALUES (0, 1, 3)");
        execute("INSERT INTO %s (a, b, c) VALUES (0, 1, 2)");
        execute("INSERT INTO %s (a, b, c) VALUES (0, 1, 1)");

        execute("INSERT INTO %s (a, b, c) VALUES (0, 2, 3)");
        execute("INSERT INTO %s (a, b, c) VALUES (0, 2, 2)");
        execute("INSERT INTO %s (a, b, c) VALUES (0, 2, 1)");

        execute("INSERT INTO %s (a, b, c) VALUES (0, 3, 3)");
        execute("INSERT INTO %s (a, b, c) VALUES (0, 3, 2)");
        execute("INSERT INTO %s (a, b, c) VALUES (0, 3, 1)");

        assertRows(execute("SELECT b, c FROM %s WHERE a = 0 AND (b, c) >= (2, 2) ORDER BY b DESC, c ASC;"),
                   row(3, 1),
                   row(3, 2),
                   row(3, 3),
                   row(2, 2),
                   row(2, 3));
    }

    /**
     * Check select on tuple relations, see CASSANDRA-8613
     * migrated from cql_tests.py:TestCQL.simple_tuple_query_test()
     */
    @Test
    public void testSimpleTupleQuery() throws Throwable
    {
        createTable("create table %s (a int, b int, c int, d int , e int, PRIMARY KEY (a, b, c, d, e))");

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (0, 2, 0, 0, 0)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (0, 1, 0, 0, 0)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (0, 0, 0, 0, 0)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (0, 0, 1, 1, 1)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (0, 0, 2, 2, 2)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (0, 0, 3, 3, 3)");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (0, 0, 1, 1, 1)");

        assertRows(execute("SELECT * FROM %s WHERE b=0 AND (c, d, e) > (1, 1, 1) ALLOW FILTERING"),
                   row(0, 0, 2, 2, 2),
                   row(0, 0, 3, 3, 3));
    }

    @Test
    public void testInvalidColumnNames() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");
        assertInvalidMessage("Undefined column name e", "SELECT * FROM %s WHERE (b, e) = (0, 0)");
        assertInvalidMessage("Undefined column name e", "SELECT * FROM %s WHERE (b, e) IN ((0, 1), (2, 4))");
        assertInvalidMessage("Undefined column name e", "SELECT * FROM %s WHERE (b, e) > (0, 1) and b <= 2");
        assertInvalidMessage("Undefined column name e", "SELECT c AS e FROM %s WHERE (b, e) = (0, 0)");
        assertInvalidMessage("Undefined column name e", "SELECT c AS e FROM %s WHERE (b, e) IN ((0, 1), (2, 4))");
        assertInvalidMessage("Undefined column name e", "SELECT c AS e FROM %s WHERE (b, e) > (0, 1) and b <= 2");
    }

    @Test
    public void testInRestrictionsWithAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c1 text, c2 int, c3 int, v int, primary key(pk, c1, c2, c3))");
        execute("INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "0", 0, 1, 3);
        execute("INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "1", 0, 2, 4);
        execute("INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "1", 1, 3, 5);
        execute("INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "2", 1, 4, 6);
        execute("INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "2", 2, 5, 7);

        assertRows(execute("SELECT * FROM %s WHERE (c2) IN ((?), (?)) ALLOW FILTERING", 1, 3),
                   row(1, "1", 1, 3, 5),
                   row(1, "2", 1, 4, 6));

        assertRows(execute("SELECT * FROM %s WHERE c2 IN (?, ?) ALLOW FILTERING", 1, 3),
                   row(1, "1", 1, 3, 5),
                   row(1, "2", 1, 4, 6));

        assertInvalidMessage("Multicolumn IN filters are not supported",
                             "SELECT * FROM %s WHERE (c2, c3) IN ((?, ?), (?, ?)) ALLOW FILTERING", 1, 0, 2, 0);
    }

    @Test
    public void testInRestrictionsWithIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c1 text, c2 int, c3 int, v int, primary key(pk, c1, c2, c3))");
        createIndex("CREATE INDEX ON %s (c3)");
        execute("INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "0", 0, 1, 3);
        execute("INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "1", 0, 2, 4);
        execute("INSERT INTO %s (pk, c1, c2, c3, v) values (?, ?, ?, ?, ?)", 1, "1", 1, 3, 5);

        assertRows(execute("SELECT * FROM %s WHERE (c3) IN ((?), (?)) ALLOW FILTERING", 1, 3),
                   row(1, "0", 0, 1, 3),
                   row(1, "1", 1, 3, 5));

        assertInvalidMessage("PRIMARY KEY column \"c2\" cannot be restricted as preceding column \"c1\" is not restricted",
                             "SELECT * FROM %s WHERE (c2, c3) IN ((?, ?), (?, ?))", 1, 0, 2, 0);

        assertInvalidMessage("Multicolumn IN filters are not supported",
                             "SELECT * FROM %s WHERE (c2, c3) IN ((?, ?), (?, ?)) ALLOW FILTERING", 1, 0, 2, 0);
    }
 }
