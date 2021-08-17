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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class InsertTest extends CQLTester
{
    @Test
    public void testInsertWithUnset() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");

        // insert using nulls
        execute("INSERT INTO %s (k, s, i) VALUES (10, ?, ?)", "text", 10);
        execute("INSERT INTO %s (k, s, i) VALUES (10, ?, ?)", null, null);
        assertRows(execute("SELECT s, i FROM %s WHERE k = 10"),
                   row(null, null) // sending null deletes the data
        );
        // insert using UNSET
        execute("INSERT INTO %s (k, s, i) VALUES (11, ?, ?)", "text", 10);
        execute("INSERT INTO %s (k, s, i) VALUES (11, ?, ?)", unset(), unset());
        assertRows(execute("SELECT s, i FROM %s WHERE k=11"),
                   row("text", 10) // unset columns does not delete the existing data
        );

        assertInvalidMessage("Invalid unset value for column k", "UPDATE %s SET i = 0 WHERE k = ?", unset());
        assertInvalidMessage("Invalid unset value for column k", "DELETE FROM %s WHERE k = ?", unset());
        assertInvalidMessage("Invalid unset value for argument in call to function blobasint", "SELECT * FROM %s WHERE k = blobAsInt(?)", unset());
    }

    @Test
    public void testInsertWithTtl() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        // test with unset
        execute("INSERT INTO %s (k, v) VALUES (1, 1) USING TTL ?", unset()); // treat as 'unlimited'
        assertRows(execute("SELECT ttl(v) FROM %s"), row(new Object[]{ null }));

        // test with null
        execute("INSERT INTO %s (k, v) VALUES (?, ?) USING TTL ?", 1, 1, null);
        assertRows(execute("SELECT k, v, TTL(v) FROM %s"), row(1, 1, null));

        // test error handling
        assertInvalidMessage("A TTL must be greater or equal to 0, but was -5",
                             "INSERT INTO %s (k, v) VALUES (?, ?) USING TTL ?", 1, 1, -5);

        assertInvalidMessage("ttl is too large.",
                             "INSERT INTO %s (k, v) VALUES (?, ?) USING TTL ?", 1, 1, Attributes.MAX_TTL + 1);
    }

    @Test
    public void testInsert() throws Throwable
    {
        testInsert(false);
        testInsert(true);
    }

    private void testInsert(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                                      "clustering int," +
                                      "value int," +
                                      " PRIMARY KEY (partitionKey, clustering))");

        execute("INSERT INTO %s (partitionKey, clustering) VALUES (0, 0)");
        execute("INSERT INTO %s (partitionKey, clustering, value) VALUES (0, 1, 1)");
        flush(forceFlush);

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 0, null),
                   row(0, 1, 1));

        // Missing primary key columns
        assertInvalidMessage("Some partition key parts are missing: partitionkey",
                             "INSERT INTO %s (clustering, value) VALUES (0, 1)");
        assertInvalidMessage("Some clustering keys are missing: clustering",
                             "INSERT INTO %s (partitionKey, value) VALUES (0, 2)");

        // multiple time the same value
        assertInvalidMessage("The column names contains duplicates",
                             "INSERT INTO %s (partitionKey, clustering, value, value) VALUES (0, 0, 2, 2)");

        // multiple time same primary key element in WHERE clause
        assertInvalidMessage("The column names contains duplicates",
                             "INSERT INTO %s (partitionKey, clustering, clustering, value) VALUES (0, 0, 0, 2)");

        // unknown identifiers
        assertInvalidMessage("Undefined column name clusteringx",
                             "INSERT INTO %s (partitionKey, clusteringx, value) VALUES (0, 0, 2)");

        assertInvalidMessage("Undefined column name valuex",
                             "INSERT INTO %s (partitionKey, clustering, valuex) VALUES (0, 0, 2)");
    }

    @Test
    public void testInsertWithTwoClusteringColumns() throws Throwable
    {
        testInsertWithTwoClusteringColumns(false);
        testInsertWithTwoClusteringColumns(true);
    }

    private void testInsertWithTwoClusteringColumns(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                    "clustering_1 int," +
                    "clustering_2 int," +
                    "value int," +
                    " PRIMARY KEY (partitionKey, clustering_1, clustering_2))");

        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 1, 1)");
        flush(forceFlush);

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 0, 0, null),
                   row(0, 0, 1, 1));

        // Missing primary key columns
        assertInvalidMessage("Some partition key parts are missing: partitionkey",
                             "INSERT INTO %s (clustering_1, clustering_2, value) VALUES (0, 0, 1)");
        assertInvalidMessage("Some clustering keys are missing: clustering_1",
                             "INSERT INTO %s (partitionKey, clustering_2, value) VALUES (0, 0, 2)");

        // multiple time the same value
        assertInvalidMessage("The column names contains duplicates",
                             "INSERT INTO %s (partitionKey, clustering_1, value, clustering_2, value) VALUES (0, 0, 2, 0, 2)");

        // multiple time same primary key element in WHERE clause
        assertInvalidMessage("The column names contains duplicates",
                             "INSERT INTO %s (partitionKey, clustering_1, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0, 2)");

        // unknown identifiers
        assertInvalidMessage("Undefined column name clustering_1x",
                             "INSERT INTO %s (partitionKey, clustering_1x, clustering_2, value) VALUES (0, 0, 0, 2)");

        assertInvalidMessage("Undefined column name valuex",
                             "INSERT INTO %s (partitionKey, clustering_1, clustering_2, valuex) VALUES (0, 0, 0, 2)");
    }

    @Test
    public void testInsertWithAStaticColumn() throws Throwable
    {
        testInsertWithAStaticColumn(false);
        testInsertWithAStaticColumn(true);
    }

    private void testInsertWithAStaticColumn(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                    "clustering_1 int," +
                    "clustering_2 int," +
                    "value int," +
                    "staticValue text static," +
                    " PRIMARY KEY (partitionKey, clustering_1, clustering_2))");

        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, staticValue) VALUES (0, 0, 0, 'A')");
        execute("INSERT INTO %s (partitionKey, staticValue) VALUES (1, 'B')");
        flush(forceFlush);

        assertRows(execute("SELECT * FROM %s"),
                   row(1, null, null, "B", null),
                   row(0, 0, 0, "A", null));

        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (1, 0, 0, 0)");
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s"),
                   row(1, 0, 0, "B", 0),
                   row(0, 0, 0, "A", null));

        // Missing primary key columns
        assertInvalidMessage("Some partition key parts are missing: partitionkey",
                             "INSERT INTO %s (clustering_1, clustering_2, staticValue) VALUES (0, 0, 'A')");
        assertInvalidMessage("Some clustering keys are missing: clustering_1",
                             "INSERT INTO %s (partitionKey, clustering_2, staticValue) VALUES (0, 0, 'A')");
    }

    @Test
    public void testInsertWithDefaultTtl() throws Throwable
    {
        final int secondsPerMinute = 60;
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH default_time_to_live = " + (10 * secondsPerMinute));

        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        UntypedResultSet resultSet = execute("SELECT ttl(b) FROM %s WHERE a = 1");
        Assert.assertEquals(1, resultSet.size());
        Row row = resultSet.one();
        Assert.assertTrue(row.getInt("ttl(b)") >= (9 * secondsPerMinute));

        execute("INSERT INTO %s (a, b) VALUES (2, 2) USING TTL ?", (5 * secondsPerMinute));
        resultSet = execute("SELECT ttl(b) FROM %s WHERE a = 2");
        Assert.assertEquals(1, resultSet.size());
        row = resultSet.one();
        Assert.assertTrue(row.getInt("ttl(b)") <= (5 * secondsPerMinute));

        execute("INSERT INTO %s (a, b) VALUES (3, 3) USING TTL ?", 0);
        assertRows(execute("SELECT ttl(b) FROM %s WHERE a = 3"), row(new Object[]{null}));

        execute("INSERT INTO %s (a, b) VALUES (4, 4) USING TTL ?", unset());
        resultSet = execute("SELECT ttl(b) FROM %s WHERE a = 4");
        Assert.assertEquals(1, resultSet.size());
        row = resultSet.one();
        Assert.assertTrue(row.getInt("ttl(b)") >= (9 * secondsPerMinute));

        execute("INSERT INTO %s (a, b) VALUES (?, ?) USING TTL ?", 4, 4, null);
        assertRows(execute("SELECT ttl(b) FROM %s WHERE a = 4"), row(new Object[]{null}));
    }

    @Test
    public void testPKInsertWithValueOver64K() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, PRIMARY KEY (a, b))");

        assertInvalidThrow(InvalidRequestException.class,
                           "INSERT INTO %s (a, b) VALUES (?, 'foo')", new String(TOO_BIG.array()));
    }

    @Test
    public void testCKInsertWithValueOver64K() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, PRIMARY KEY (a, b))");

        assertInvalidThrow(InvalidRequestException.class,
                           "INSERT INTO %s (a, b) VALUES ('foo', ?)", new String(TOO_BIG.array()));
    }
}
