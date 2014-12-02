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

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FrozenCollectionsTest extends CQLTester
{
    @Test
    public void testPartitionKeyUsage() throws Throwable
    {
        createTable("CREATE TABLE %s (k frozen<set<int>> PRIMARY KEY, v int)");

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", set(), 1);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", set(1, 2, 3), 1);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", set(4, 5, 6), 0);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", set(7, 8, 9), 0);

        // overwrite with an update
        execute("UPDATE %s SET v=? WHERE k=?", 0, set());
        execute("UPDATE %s SET v=? WHERE k=?", 0, set(1, 2, 3));

        assertRows(execute("SELECT * FROM %s"),
            row(set(), 0),
            row(set(1, 2, 3), 0),
            row(set(4, 5, 6), 0),
            row(set(7, 8, 9), 0)
        );

        assertRows(execute("SELECT k FROM %s"),
            row(set()),
            row(set(1, 2, 3)),
            row(set(4, 5, 6)),
            row(set(7, 8, 9))
        );

        assertRows(execute("SELECT * FROM %s LIMIT 2"),
                row(set(), 0),
                row(set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE k=?", set(4, 5, 6)),
            row(set(4, 5, 6), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE k=?", set()),
                row(set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE k IN ?", list(set(4, 5, 6), set())),
                   row(set(), 0),
                   row(set(4, 5, 6), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE token(k) >= token(?)", set(4, 5, 6)),
                row(set(4, 5, 6), 0),
                row(set(7, 8, 9), 0)
        );

        assertInvalid("INSERT INTO %s (k, v) VALUES (null, 0)");

        execute("DELETE FROM %s WHERE k=?", set());
        execute("DELETE FROM %s WHERE k=?", set(4, 5, 6));
        assertRows(execute("SELECT * FROM %s"),
            row(set(1, 2, 3), 0),
            row(set(7, 8, 9), 0)
        );
    }

    @Test
    public void testNestedPartitionKeyUsage() throws Throwable
    {
        createTable("CREATE TABLE %s (k frozen<map<set<int>, list<int>>> PRIMARY KEY, v int)");

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", map(), 1);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", map(set(), list(1, 2, 3)), 0);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", map(set(1, 2, 3), list(1, 2, 3)), 1);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", map(set(4, 5, 6), list(1, 2, 3)), 0);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", map(set(7, 8, 9), list(1, 2, 3)), 0);

        // overwrite with an update
        execute("UPDATE %s SET v=? WHERE k=?", 0, map());
        execute("UPDATE %s SET v=? WHERE k=?", 0, map(set(1, 2, 3), list(1, 2, 3)));

        assertRows(execute("SELECT * FROM %s"),
            row(map(), 0),
            row(map(set(), list(1, 2, 3)), 0),
            row(map(set(1, 2, 3), list(1, 2, 3)), 0),
            row(map(set(4, 5, 6), list(1, 2, 3)), 0),
            row(map(set(7, 8, 9), list(1, 2, 3)), 0)
        );

        assertRows(execute("SELECT k FROM %s"),
            row(map()),
            row(map(set(), list(1, 2, 3))),
            row(map(set(1, 2, 3), list(1, 2, 3))),
            row(map(set(4, 5, 6), list(1, 2, 3))),
            row(map(set(7, 8, 9), list(1, 2, 3)))
        );

        assertRows(execute("SELECT * FROM %s LIMIT 3"),
            row(map(), 0),
            row(map(set(), list(1, 2, 3)), 0),
            row(map(set(1, 2, 3), list(1, 2, 3)), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE k=?", map(set(4, 5, 6), list(1, 2, 3))),
            row(map(set(4, 5, 6), list(1, 2, 3)), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE k=?", map()),
                row(map(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE k=?", map(set(), list(1, 2, 3))),
                row(map(set(), list(1, 2, 3)), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE k IN ?", list(map(set(4, 5, 6), list(1, 2, 3)), map(), map(set(), list(1, 2, 3)))),
                   row(map(), 0),
                   row(map(set(), list(1, 2, 3)), 0),
                   row(map(set(4, 5, 6), list(1, 2, 3)), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE token(k) >= token(?)", map(set(4, 5, 6), list(1, 2, 3))),
            row(map(set(4, 5, 6), list(1, 2, 3)), 0),
            row(map(set(7, 8, 9), list(1, 2, 3)), 0)
        );

        execute("DELETE FROM %s WHERE k=?", map());
        execute("DELETE FROM %s WHERE k=?", map(set(), list(1, 2, 3)));
        execute("DELETE FROM %s WHERE k=?", map(set(4, 5, 6), list(1, 2, 3)));
        assertRows(execute("SELECT * FROM %s"),
            row(map(set(1, 2, 3), list(1, 2, 3)), 0),
            row(map(set(7, 8, 9), list(1, 2, 3)), 0)
        );

    }

    @Test
    public void testClusteringKeyUsage() throws Throwable
    {
        for (String option : Arrays.asList("", " WITH COMPACT STORAGE"))
        {
            createTable("CREATE TABLE %s (a int, b frozen<set<int>>, c int, PRIMARY KEY (a, b))" + option);

            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, set(), 1);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, set(1, 2, 3), 1);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, set(4, 5, 6), 0);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, set(7, 8, 9), 0);

            // overwrite with an update
            execute("UPDATE %s SET c=? WHERE a=? AND b=?", 0, 0, set());
            execute("UPDATE %s SET c=? WHERE a=? AND b=?", 0, 0, set(1, 2, 3));

            assertRows(execute("SELECT * FROM %s"),
                row(0, set(), 0),
                row(0, set(1, 2, 3), 0),
                row(0, set(4, 5, 6), 0),
                row(0, set(7, 8, 9), 0)
            );

            assertRows(execute("SELECT b FROM %s"),
                row(set()),
                row(set(1, 2, 3)),
                row(set(4, 5, 6)),
                row(set(7, 8, 9))
            );

            assertRows(execute("SELECT * FROM %s LIMIT 2"),
                row(0, set(), 0),
                row(0, set(1, 2, 3), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, set(4, 5, 6)),
                row(0, set(4, 5, 6), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, set()),
                row(0, set(), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b IN ?", 0, list(set(4, 5, 6), set())),
                row(0, set(), 0),
                row(0, set(4, 5, 6), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ?", 0, set(4, 5, 6)),
                row(0, set(7, 8, 9), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b >= ?", 0, set(4, 5, 6)),
                row(0, set(4, 5, 6), 0),
                row(0, set(7, 8, 9), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b < ?", 0, set(4, 5, 6)),
                row(0, set(), 0),
                row(0, set(1, 2, 3), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b <= ?", 0, set(4, 5, 6)),
                row(0, set(), 0),
                row(0, set(1, 2, 3), 0),
                row(0, set(4, 5, 6), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ? AND b <= ?", 0, set(1, 2, 3), set(4, 5, 6)),
                row(0, set(4, 5, 6), 0)
            );

            execute("DELETE FROM %s WHERE a=? AND b=?", 0, set());
            execute("DELETE FROM %s WHERE a=? AND b=?", 0, set(4, 5, 6));
            assertRows(execute("SELECT * FROM %s"),
                row(0, set(1, 2, 3), 0),
                row(0, set(7, 8, 9), 0)
            );
        }
    }

    @Test
    public void testNestedClusteringKeyUsage() throws Throwable
    {
        for (String option : Arrays.asList("", " WITH COMPACT STORAGE"))
        {
            createTable("CREATE TABLE %s (a int, b frozen<map<set<int>, list<int>>>, c frozen<set<int>>, d int, PRIMARY KEY (a, b, c))" + option);

            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(), set(), 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(), list(1, 2, 3)), set(), 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0);
            execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0);

            assertRows(execute("SELECT * FROM %s"),
                row(0, map(), set(), 0),
                row(0, map(set(), list(1, 2, 3)), set(), 0),
                row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0)
            );

            assertRows(execute("SELECT b FROM %s"),
                row(map()),
                row(map(set(), list(1, 2, 3))),
                row(map(set(1, 2, 3), list(1, 2, 3))),
                row(map(set(4, 5, 6), list(1, 2, 3))),
                row(map(set(7, 8, 9), list(1, 2, 3)))
            );

            assertRows(execute("SELECT c FROM %s"),
                row(set()),
                row(set()),
                row(set(1, 2, 3)),
                row(set(1, 2, 3)),
                row(set(1, 2, 3))
            );

            assertRows(execute("SELECT * FROM %s LIMIT 3"),
                row(0, map(), set(), 0),
                row(0, map(set(), list(1, 2, 3)), set(), 0),
                row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=0 ORDER BY b DESC LIMIT 4"),
                row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0),
                row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                row(0, map(set(), list(1, 2, 3)), set(), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, map()),
                row(0, map(), set(), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, map(set(), list(1, 2, 3))),
                row(0, map(set(), list(1, 2, 3)), set(), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, map(set(1, 2, 3), list(1, 2, 3))),
                row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(), list(1, 2, 3)), set()),
                    row(0, map(set(), list(1, 2, 3)), set(), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND (b, c) IN ?", 0, list(tuple(map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3)),
                                                                                     tuple(map(), set()))),
                row(0, map(), set(), 0),
                row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b >= ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b < ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                row(0, map(), set(), 0),
                row(0, map(set(), list(1, 2, 3)), set(), 0),
                row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b <= ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                row(0, map(), set(), 0),
                row(0, map(set(), list(1, 2, 3)), set(), 0),
                row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ? AND b <= ?", 0, map(set(1, 2, 3), list(1, 2, 3)), map(set(4, 5, 6), list(1, 2, 3))),
                row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0)
            );

            execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, map(), set());
            assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(), set()));

            execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(), list(1, 2, 3)), set());
            assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(), list(1, 2, 3)), set()));

            execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3));
            assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3)));

            assertRows(execute("SELECT * FROM %s"),
                    row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                    row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0)
            );
        }
    }

    @Test
    public void testNormalColumnUsage() throws Throwable
    {
        for (String option : Arrays.asList("", " WITH COMPACT STORAGE"))
        {
            createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<map<set<int>, list<int>>>, c frozen<set<int>>)" + option);

            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, map(), set());
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, map(set(), list(99999, 999999, 99999)), set());
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3));
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 3, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3));
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 4, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3));

            // overwrite with update
            execute ("UPDATE %s SET b=? WHERE a=?", map(set(), list(1, 2, 3)), 1);

            assertRows(execute("SELECT * FROM %s"),
                row(0, map(), set()),
                row(1, map(set(), list(1, 2, 3)), set()),
                row(2, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3)),
                row(3, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3)),
                row(4, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3))
            );

            assertRows(execute("SELECT b FROM %s"),
                row(map()),
                row(map(set(), list(1, 2, 3))),
                row(map(set(1, 2, 3), list(1, 2, 3))),
                row(map(set(4, 5, 6), list(1, 2, 3))),
                row(map(set(7, 8, 9), list(1, 2, 3)))
            );

            assertRows(execute("SELECT c FROM %s"),
                row(set()),
                row(set()),
                row(set(1, 2, 3)),
                row(set(1, 2, 3)),
                row(set(1, 2, 3))
            );

            assertRows(execute("SELECT * FROM %s WHERE a=?", 3),
                row(3, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3))
            );

            execute("UPDATE %s SET b=? WHERE a=?", null, 1);
            assertRows(execute("SELECT * FROM %s WHERE a=?", 1),
                row(1, null, set())
            );

            execute("UPDATE %s SET b=? WHERE a=?", map(), 1);
            assertRows(execute("SELECT * FROM %s WHERE a=?", 1),
                row(1, map(), set())
            );

            execute("UPDATE %s SET c=? WHERE a=?", null, 2);
            assertRows(execute("SELECT * FROM %s WHERE a=?", 2),
                row(2, map(set(1, 2, 3), list(1, 2, 3)), null)
            );

            execute("UPDATE %s SET c=? WHERE a=?", set(), 2);
            assertRows(execute("SELECT * FROM %s WHERE a=?", 2),
                    row(2, map(set(1, 2, 3), list(1, 2, 3)), set())
            );

            execute("DELETE b FROM %s WHERE a=?", 3);
            assertRows(execute("SELECT * FROM %s WHERE a=?", 3),
                row(3, null, set(1, 2, 3))
            );

            execute("DELETE c FROM %s WHERE a=?", 4);
            assertRows(execute("SELECT * FROM %s WHERE a=?", 4),
                row(4, map(set(7, 8, 9), list(1, 2, 3)), null)
            );
        }
    }

    @Test
    public void testStaticColumnUsage() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c frozen<map<set<int>, list<int>>> static, d int, PRIMARY KEY (a, b))");
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, map(), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, map(), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, map(set(), list(1, 2, 3)), 0);
        execute("INSERT INTO %s (a, b, d) VALUES (?, ?, ?)", 1, 1, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, map(set(1, 2, 3), list(1, 2, 3)), 0);

        assertRows(execute("SELECT * FROM %s"),
            row(0, 0, map(), 0),
            row(0, 1, map(), 0),
            row(1, 0, map(set(), list(1, 2, 3)), 0),
            row(1, 1, map(set(), list(1, 2, 3)), 0),
            row(2, 0, map(set(1, 2, 3), list(1, 2, 3)), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, 1),
            row(0, 1, map(), 0)
        );

        execute("DELETE c FROM %s WHERE a=?", 0);
        assertRows(execute("SELECT * FROM %s"),
                row(0, 0, null, 0),
                row(0, 1, null, 0),
                row(1, 0, map(set(), list(1, 2, 3)), 0),
                row(1, 1, map(set(), list(1, 2, 3)), 0),
                row(2, 0, map(set(1, 2, 3), list(1, 2, 3)), 0)
        );

        execute("DELETE FROM %s WHERE a=?", 0);
        assertRows(execute("SELECT * FROM %s"),
                row(1, 0, map(set(), list(1, 2, 3)), 0),
                row(1, 1, map(set(), list(1, 2, 3)), 0),
                row(2, 0, map(set(1, 2, 3), list(1, 2, 3)), 0)
        );

        execute("UPDATE %s SET c=? WHERE a=?", map(set(1, 2, 3), list(1, 2, 3)), 1);
        assertRows(execute("SELECT * FROM %s"),
                row(1, 0, map(set(1, 2, 3), list(1, 2, 3)), 0),
                row(1, 1, map(set(1, 2, 3), list(1, 2, 3)), 0),
                row(2, 0, map(set(1, 2, 3), list(1, 2, 3)), 0)
        );
    }

    private void assertInvalidCreateWithMessage(String createTableStatement, String errorMessage) throws Throwable
    {
         try
        {
            createTableMayThrow(createTableStatement);
            Assert.fail("Expected CREATE TABLE statement to error: " + createTableStatement);
        }
        catch (InvalidRequestException | ConfigurationException | SyntaxException ex)
        {
            Assert.assertTrue("Expected error message to contain '" + errorMessage + "', but got '" + ex.getMessage() + "'",
                    ex.getMessage().contains(errorMessage));
        }
    }

    private void assertInvalidAlterWithMessage(String createTableStatement, String errorMessage) throws Throwable
    {
        try
        {
            alterTableMayThrow(createTableStatement);
            Assert.fail("Expected CREATE TABLE statement to error: " + createTableStatement);
        }
        catch (InvalidRequestException | ConfigurationException ex)
        {
            Assert.assertTrue("Expected error message to contain '" + errorMessage + "', but got '" + ex.getMessage() + "'",
                    ex.getMessage().contains(errorMessage));
        }
    }

    @Test
    public void testInvalidOperations() throws Throwable
    {
        // lists
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l frozen<list<int>>)");
        assertInvalid("UPDATE %s SET l[?]=? WHERE k=?", 0, 0, 0);
        assertInvalid("UPDATE %s SET l = ? + l WHERE k=?", list(0), 0);
        assertInvalid("UPDATE %s SET l = l + ? WHERE k=?", list(4), 0);
        assertInvalid("UPDATE %s SET l = l - ? WHERE k=?", list(3), 0);
        assertInvalid("DELETE l[?] FROM %s WHERE k=?", 0, 0);

        // sets
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s frozen<set<int>>)");
        assertInvalid("UPDATE %s SET s = s + ? WHERE k=?", set(0), 0);
        assertInvalid("UPDATE %s SET s = s - ? WHERE k=?", set(3), 0);

        // maps
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m frozen<map<int, int>>)");
        assertInvalid("UPDATE %s SET m[?]=? WHERE k=?", 0, 0, 0);
        assertInvalid("UPDATE %s SET m = m + ? WHERE k=?", map(4, 4), 0);
        assertInvalid("DELETE m[?] FROM %s WHERE k=?", 0, 0);

        assertInvalidCreateWithMessage("CREATE TABLE %s (k int PRIMARY KEY, t set<set<int>>)",
                "Non-frozen collections are not allowed inside collections");

        assertInvalidCreateWithMessage("CREATE TABLE %s (k int PRIMARY KEY, t frozen<set<counter>>)",
                                       "Counters are not allowed inside collections");

        assertInvalidCreateWithMessage("CREATE TABLE %s (k int PRIMARY KEY, t frozen<text>)",
                "frozen<> is only allowed on collections, tuples, and user-defined types");
    }

    @Test
    public void testAltering() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b frozen<list<int>>, c frozen<list<int>>, PRIMARY KEY (a, b))");

        alterTable("ALTER TABLE %s ALTER c TYPE frozen<list<blob>>");

        assertInvalidAlterWithMessage("ALTER TABLE %s ALTER b TYPE frozen<list<blob>>",
                                      "types are not order-compatible");

        assertInvalidAlterWithMessage("ALTER TABLE %s ALTER b TYPE list<int>",
                                      "types are not order-compatible");

        assertInvalidAlterWithMessage("ALTER TABLE %s ALTER c TYPE list<blob>",
                                      "types are incompatible");

        alterTable("ALTER TABLE %s DROP c");
        alterTable("ALTER TABLE %s ADD c frozen<set<int>>");
        assertInvalidAlterWithMessage("ALTER TABLE %s ALTER c TYPE frozen<set<blob>>",
                                      "types are incompatible");

        alterTable("ALTER TABLE %s DROP c");
        alterTable("ALTER TABLE %s ADD c frozen<map<int, int>>");
        assertInvalidAlterWithMessage("ALTER TABLE %s ALTER c TYPE frozen<map<blob, int>>",
                                      "types are incompatible");
        alterTable("ALTER TABLE %s ALTER c TYPE frozen<map<int, blob>>");
    }

    private void assertInvalidIndexCreationWithMessage(String statement, String errorMessage) throws Throwable
    {
        try
        {
            createIndexMayThrow(statement);
            Assert.fail("Expected index creation to fail: " + statement);
        }
        catch (InvalidRequestException ex)
        {
            Assert.assertTrue("Expected error message to contain '" + errorMessage + "', but got '" + ex.getMessage() + "'",
                              ex.getMessage().contains(errorMessage));
        }
    }

    @Test
    public void testSecondaryIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (a frozen<map<int, text>> PRIMARY KEY, b frozen<map<int, text>>)");

        // for now, we don't support indexing values or keys of collections in the primary key
        assertInvalidIndexCreationWithMessage("CREATE INDEX ON %s (full(a))", "Cannot create secondary index on partition key column");
        assertInvalidIndexCreationWithMessage("CREATE INDEX ON %s (keys(a))", "Cannot create index on keys of frozen<map> column");
        assertInvalidIndexCreationWithMessage("CREATE INDEX ON %s (keys(b))", "Cannot create index on keys of frozen<map> column");

        createTable("CREATE TABLE %s (a int, b frozen<list<int>>, c frozen<set<int>>, d frozen<map<int, text>>, PRIMARY KEY (a, b))");

        createIndex("CREATE INDEX ON %s (full(b))");
        createIndex("CREATE INDEX ON %s (full(c))");
        createIndex("CREATE INDEX ON %s (full(d))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, list(1, 2, 3), set(1, 2, 3), map(1, "a"));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, list(4, 5, 6), set(1, 2, 3), map(1, "a"));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, list(1, 2, 3), set(4, 5, 6), map(2, "b"));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, list(4, 5, 6), set(4, 5, 6), map(2, "b"));

        // CONTAINS KEY doesn't work on non-maps
        assertInvalidMessage("Cannot use CONTAINS KEY on non-map column",
                             "SELECT * FROM %s WHERE b CONTAINS KEY ?", 1);

        assertInvalidMessage("Cannot use CONTAINS KEY on non-map column",
                             "SELECT * FROM %s WHERE b CONTAINS KEY ? ALLOW FILTERING", 1);

        assertInvalidMessage("Cannot use CONTAINS KEY on non-map column",
                             "SELECT * FROM %s WHERE c CONTAINS KEY ?", 1);

        // normal indexes on frozen collections don't support CONTAINS or CONTAINS KEY
        assertInvalidMessage("Cannot restrict clustering columns by a CONTAINS relation without a secondary index",
                             "SELECT * FROM %s WHERE b CONTAINS ?", 1);

        assertInvalidMessage("Cannot restrict clustering columns by a CONTAINS relation without a secondary index",
                             "SELECT * FROM %s WHERE b CONTAINS ? ALLOW FILTERING", 1);

        assertInvalidMessage("No secondary indexes on the restricted columns support the provided operator",
                             "SELECT * FROM %s WHERE d CONTAINS KEY ?", 1);

        assertInvalidMessage("No secondary indexes on the restricted columns support the provided operator",
                             "SELECT * FROM %s WHERE d CONTAINS KEY ? ALLOW FILTERING", 1);

        assertInvalidMessage("Cannot restrict clustering columns by a CONTAINS relation without a secondary index",
                             "SELECT * FROM %s WHERE b CONTAINS ? AND d CONTAINS KEY ? ALLOW FILTERING", 1, 1);

        // index lookup on b
        assertRows(execute("SELECT * FROM %s WHERE b=?", list(1, 2, 3)),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
            row(1, list(1, 2, 3), set(4, 5, 6), map(2, "b"))
        );

        assertEmpty(execute("SELECT * FROM %s WHERE b=?", list(-1)));

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE b=? AND c=?", list(1, 2, 3), set(4, 5, 6));
        assertRows(execute("SELECT * FROM %s WHERE b=? AND c=? ALLOW FILTERING", list(1, 2, 3), set(4, 5, 6)),
            row(1, list(1, 2, 3), set(4, 5, 6), map(2, "b"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE b=? AND c CONTAINS ?", list(1, 2, 3), 5);
        assertRows(execute("SELECT * FROM %s WHERE b=? AND c CONTAINS ? ALLOW FILTERING", list(1, 2, 3), 5),
            row(1, list(1, 2, 3), set(4, 5, 6), map(2, "b"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE b=? AND d=?", list(1, 2, 3), map(1, "a"));
        assertRows(execute("SELECT * FROM %s WHERE b=? AND d=? ALLOW FILTERING", list(1, 2, 3), map(1, "a")),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE b=? AND d CONTAINS ?", list(1, 2, 3), "a");
        assertRows(execute("SELECT * FROM %s WHERE b=? AND d CONTAINS ? ALLOW FILTERING", list(1, 2, 3), "a"),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE b=? AND d CONTAINS KEY ?", list(1, 2, 3), 1);
        assertRows(execute("SELECT * FROM %s WHERE b=? AND d CONTAINS KEY ? ALLOW FILTERING", list(1, 2, 3), 1),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        // index lookup on c
        assertRows(execute("SELECT * FROM %s WHERE c=?", set(1, 2, 3)),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
            row(0, list(4, 5, 6), set(1, 2, 3), map(1, "a"))
        );

        // ordering of c should not matter
        assertRows(execute("SELECT * FROM %s WHERE c=?", set(2, 1, 3)),
                row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
                row(0, list(4, 5, 6), set(1, 2, 3), map(1, "a"))
        );

        assertEmpty(execute("SELECT * FROM %s WHERE c=?", set(-1)));

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE c=? AND b=?", set(1, 2, 3), list(1, 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE c=? AND b=? ALLOW FILTERING", set(1, 2, 3), list(1, 2, 3)),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE c=? AND b CONTAINS ?", set(1, 2, 3), 1);
        assertRows(execute("SELECT * FROM %s WHERE c=? AND b CONTAINS ? ALLOW FILTERING", set(1, 2, 3), 1),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE c=? AND d = ?", set(1, 2, 3), map(1, "a"));
        assertRows(execute("SELECT * FROM %s WHERE c=? AND d = ? ALLOW FILTERING", set(1, 2, 3), map(1, "a")),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
            row(0, list(4, 5, 6), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE c=? AND d CONTAINS ?", set(1, 2, 3), "a");
        assertRows(execute("SELECT * FROM %s WHERE c=? AND d CONTAINS ? ALLOW FILTERING", set(1, 2, 3), "a"),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
            row(0, list(4, 5, 6), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE c=? AND d CONTAINS KEY ?", set(1, 2, 3), 1);
        assertRows(execute("SELECT * FROM %s WHERE c=? AND d CONTAINS KEY ? ALLOW FILTERING", set(1, 2, 3), 1),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
            row(0, list(4, 5, 6), set(1, 2, 3), map(1, "a"))
        );

        // index lookup on d
        assertRows(execute("SELECT * FROM %s WHERE d=?", map(1, "a")),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
            row(0, list(4, 5, 6), set(1, 2, 3), map(1, "a"))
        );

        assertRows(execute("SELECT * FROM %s WHERE d=?", map(2, "b")),
            row(1, list(1, 2, 3), set(4, 5, 6), map(2, "b")),
            row(1, list(4, 5, 6), set(4, 5, 6), map(2, "b"))
        );

        assertEmpty(execute("SELECT * FROM %s WHERE d=?", map(3, "c")));

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE d=? AND c=?", map(1, "a"), set(1, 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE d=? AND b=? ALLOW FILTERING", map(1, "a"), list(1, 2, 3)),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE d=? AND b CONTAINS ?", map(1, "a"), 3);
        assertRows(execute("SELECT * FROM %s WHERE d=? AND b CONTAINS ? ALLOW FILTERING", map(1, "a"), 3),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE d=? AND b=? AND c=?", map(1, "a"), list(1, 2, 3), set(1, 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE d=? AND b=? AND c=? ALLOW FILTERING", map(1, "a"), list(1, 2, 3), set(1, 2, 3)),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        assertRows(execute("SELECT * FROM %s WHERE d=? AND b CONTAINS ? AND c CONTAINS ? ALLOW FILTERING", map(1, "a"), 2, 2),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        execute("DELETE d FROM %s WHERE a=? AND b=?", 0, list(1, 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE d=?", map(1, "a")),
            row(0, list(4, 5, 6), set(1, 2, 3), map(1, "a"))
        );
    }

    /** Test for CASSANDRA-8302 */
    @Test
    public void testClusteringColumnFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b frozen<map<int, int>>, c int, d int, PRIMARY KEY (a, b, c))");
        createIndex("CREATE INDEX c_index ON %s (c)");
        createIndex("CREATE INDEX d_index ON %s (d)");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(0, 0, 1, 1), 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(1, 1, 2, 2), 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, map(0, 0, 1, 1), 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, map(1, 1, 2, 2), 0, 0);

        assertRows(execute("SELECT * FROM %s WHERE d=? AND b CONTAINS ? ALLOW FILTERING", 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0),
                row(1, map(0, 0, 1, 1), 0, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE d=? AND b CONTAINS KEY ? ALLOW FILTERING", 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0),
                row(1, map(0, 0, 1, 1), 0, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND d=? AND b CONTAINS ? ALLOW FILTERING", 0, 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0)
        );
        assertRows(execute("SELECT * FROM %s WHERE a=? AND d=? AND b CONTAINS KEY ? ALLOW FILTERING", 0, 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0)
        );

        dropIndex("DROP INDEX %s.d_index");

        assertRows(execute("SELECT * FROM %s WHERE c=? AND b CONTAINS ? ALLOW FILTERING", 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0),
                row(1, map(0, 0, 1, 1), 0, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE c=? AND b CONTAINS KEY ? ALLOW FILTERING", 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0),
                row(1, map(0, 0, 1, 1), 0, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND c=? AND b CONTAINS ? ALLOW FILTERING", 0, 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0)
        );
        assertRows(execute("SELECT * FROM %s WHERE a=? AND c=? AND b CONTAINS KEY ? ALLOW FILTERING", 0, 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0)
        );
    }

    @Test
    public void testUserDefinedTypes() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (a set<int>, b tuple<list<int>>)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<" + myType + ">)");
        execute("INSERT INTO %s (k, v) VALUES (?, {a: ?, b: ?})", 0, set(1, 2, 3), tuple(list(1, 2, 3)));
        assertRows(execute("SELECT v.a, v.b FROM %s WHERE k=?", 0),
            row(set(1, 2, 3), tuple(list(1, 2, 3)))
        );
    }

    private static String clean(String classname)
    {
        return StringUtils.remove(classname, "org.apache.cassandra.db.marshal.");
    }

    @Test
    public void testToString()
    {
        // set<frozen<list<int>>>
        SetType t = SetType.getInstance(ListType.getInstance(Int32Type.instance, false), true);
        assertEquals("SetType(FrozenType(ListType(Int32Type)))", clean(t.toString()));
        assertEquals("SetType(ListType(Int32Type))", clean(t.toString(true)));

        // frozen<set<list<int>>>
        t = SetType.getInstance(ListType.getInstance(Int32Type.instance, false), false);
        assertEquals("FrozenType(SetType(ListType(Int32Type)))", clean(t.toString()));
        assertEquals("SetType(ListType(Int32Type))", clean(t.toString(true)));

        // map<frozen<list<int>>, int>
        MapType m = MapType.getInstance(ListType.getInstance(Int32Type.instance, false), Int32Type.instance, true);
        assertEquals("MapType(FrozenType(ListType(Int32Type)),Int32Type)", clean(m.toString()));
        assertEquals("MapType(ListType(Int32Type),Int32Type)", clean(m.toString(true)));

        // frozen<map<list<int>, int>>
        m = MapType.getInstance(ListType.getInstance(Int32Type.instance, false), Int32Type.instance, false);
        assertEquals("FrozenType(MapType(ListType(Int32Type),Int32Type))", clean(m.toString()));
        assertEquals("MapType(ListType(Int32Type),Int32Type)", clean(m.toString(true)));

        // tuple<set<int>>
        List<AbstractType<?>> types = new ArrayList<>();
        types.add(SetType.getInstance(Int32Type.instance, true));
        TupleType tuple = new TupleType(types);
        assertEquals("TupleType(SetType(Int32Type))", clean(tuple.toString()));
    }
}
