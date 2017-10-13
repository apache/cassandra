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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.ByteBufferUtil;

public class UpdateTest extends CQLTester
{
    @Test
    public void testTypeCasts() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t text, a ascii, d double, i int)");

        // The followings is fine
        execute("UPDATE %s SET t = 'foo' WHERE k = ?", 0);
        execute("UPDATE %s SET t = (ascii)'foo' WHERE k = ?", 0);
        execute("UPDATE %s SET t = (text)(ascii)'foo' WHERE k = ?", 0);
        execute("UPDATE %s SET a = 'foo' WHERE k = ?", 0);
        execute("UPDATE %s SET a = (ascii)'foo' WHERE k = ?", 0);

        // But trying to put some explicitely type-casted text into an ascii
        // column should be rejected (even though the text is actually ascci)
        assertInvalid("UPDATE %s SET a = (text)'foo' WHERE k = ?", 0);

        // This is also fine because integer constants works for both integer and float types
        execute("UPDATE %s SET i = 3 WHERE k = ?", 0);
        execute("UPDATE %s SET i = (int)3 WHERE k = ?", 0);
        execute("UPDATE %s SET d = 3 WHERE k = ?", 0);
        execute("UPDATE %s SET d = (double)3 WHERE k = ?", 0);

        // But values for ints and doubles are not truly compatible (their binary representation differs)
        assertInvalid("UPDATE %s SET d = (int)3 WHERE k = ?", 0);
        assertInvalid("UPDATE %s SET i = (double)3 WHERE k = ?", 0);
    }

    @Test
    public void testUpdate() throws Throwable
    {
        testUpdate(false);
        testUpdate(true);
    }

    private void testUpdate(boolean forceFlush) throws Throwable
    {
        for (String compactOption : new String[] {"", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (partitionKey int," +
                    "clustering_1 int," +
                    "value int," +
                    " PRIMARY KEY (partitionKey, clustering_1))" + compactOption);

            execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 0, 0)");
            execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 1, 1)");
            execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 2, 2)");
            execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 3, 3)");
            execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (1, 0, 4)");

            flush(forceFlush);

            execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ?", 7, 0, 1);
            flush(forceFlush);
            assertRows(execute("SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ?",
                               0, 1),
                       row(7));

            execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1) = (?)", 8, 0, 2);
            flush(forceFlush);
            assertRows(execute("SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ?",
                               0, 2),
                       row(8));

            execute("UPDATE %s SET value = ? WHERE partitionKey IN (?, ?) AND clustering_1 = ?", 9, 0, 1, 0);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey IN (?, ?) AND clustering_1 = ?",
                               0, 1, 0),
                       row(0, 0, 9),
                       row(1, 0, 9));

            execute("UPDATE %s SET value = ? WHERE partitionKey IN ? AND clustering_1 = ?", 19, Arrays.asList(0, 1), 0);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey IN ? AND clustering_1 = ?",
                               Arrays.asList(0, 1), 0),
                       row(0, 0, 19),
                       row(1, 0, 19));

            execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 IN (?, ?)", 10, 0, 1, 0);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?)",
                               0, 1, 0),
                       row(0, 0, 10),
                       row(0, 1, 10));

            execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))", 20, 0, 0, 1);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))",
                               0, 0, 1),
                       row(0, 0, 20),
                       row(0, 1, 20));

            execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ?", null, 0, 0);
            flush(forceFlush);

            if (isEmpty(compactOption))
            {
                assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))",
                                   0, 0, 1),
                           row(0, 0, null),
                           row(0, 1, 20));
            }
            else
            {
                assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))",
                                   0, 0, 1),
                           row(0, 1, 20));
            }

            // test invalid queries

            // missing primary key element
            assertInvalidMessage("Some partition key parts are missing: partitionkey",
                                 "UPDATE %s SET value = ? WHERE clustering_1 = ? ", 7, 1);

            assertInvalidMessage("Some clustering keys are missing: clustering_1",
                                 "UPDATE %s SET value = ? WHERE partitionKey = ?", 7, 0);

            assertInvalidMessage("Some clustering keys are missing: clustering_1",
                                 "UPDATE %s SET value = ? WHERE partitionKey = ?", 7, 0);

            // token function
            assertInvalidMessage("The token function cannot be used in WHERE clauses for UPDATE statements",
                                 "UPDATE %s SET value = ? WHERE token(partitionKey) = token(?) AND clustering_1 = ?",
                                 7, 0, 1);

            // multiple time the same value
            assertInvalidSyntax("UPDATE %s SET value = ?, value = ? WHERE partitionKey = ? AND clustering_1 = ?", 7, 0, 1);

            // multiple time same primary key element in WHERE clause
            assertInvalidMessage("clustering_1 cannot be restricted by more than one relation if it includes an Equal",
                                 "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_1 = ?", 7, 0, 1, 1);

            // unknown identifiers
            assertInvalidMessage("Unknown identifier value1",
                                 "UPDATE %s SET value1 = ? WHERE partitionKey = ? AND clustering_1 = ?", 7, 0, 1);

            assertInvalidMessage("Undefined name partitionkey1 in where clause ('partitionkey1 = ?')",
                                 "UPDATE %s SET value = ? WHERE partitionKey1 = ? AND clustering_1 = ?", 7, 0, 1);

            assertInvalidMessage("Undefined name clustering_3 in where clause ('clustering_3 = ?')",
                                 "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_3 = ?", 7, 0, 1);

            // Invalid operator in the where clause
            assertInvalidMessage("Only EQ and IN relation are supported on the partition key (unless you use the token() function)",
                                 "UPDATE %s SET value = ? WHERE partitionKey > ? AND clustering_1 = ?", 7, 0, 1);

            assertInvalidMessage("Cannot use CONTAINS on non-collection column partitionkey",
                                 "UPDATE %s SET value = ? WHERE partitionKey CONTAINS ? AND clustering_1 = ?", 7, 0, 1);

            assertInvalidMessage("Non PRIMARY KEY columns found in where clause: value",
                                 "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND value = ?", 7, 0, 1, 3);

            assertInvalidMessage("Slice restrictions are not supported on the clustering columns in UPDATE statements",
                                 "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 > ?", 7, 0, 1);
        }
    }

    @Test
    public void testUpdateWithSecondaryIndices() throws Throwable
    {
        testUpdateWithSecondaryIndices(false);
        testUpdateWithSecondaryIndices(true);
    }

    private void testUpdateWithSecondaryIndices(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                "clustering_1 int," +
                "value int," +
                "values set<int>," +
                " PRIMARY KEY (partitionKey, clustering_1))");

        createIndex("CREATE INDEX ON %s (value)");
        createIndex("CREATE INDEX ON %s (clustering_1)");
        createIndex("CREATE INDEX ON %s (values)");

        execute("INSERT INTO %s (partitionKey, clustering_1, value, values) VALUES (0, 0, 0, {0})");
        execute("INSERT INTO %s (partitionKey, clustering_1, value, values) VALUES (0, 1, 1, {0, 1})");
        execute("INSERT INTO %s (partitionKey, clustering_1, value, values) VALUES (0, 2, 2, {0, 1, 2})");
        execute("INSERT INTO %s (partitionKey, clustering_1, value, values) VALUES (0, 3, 3, {0, 1, 2, 3})");
        execute("INSERT INTO %s (partitionKey, clustering_1, value, values) VALUES (1, 0, 4, {0, 1, 2, 3, 4})");

        flush(forceFlush);

        assertInvalidMessage("Non PRIMARY KEY columns found in where clause: value",
                             "UPDATE %s SET values= {6} WHERE partitionKey = ? AND clustering_1 = ? AND value = ?", 3, 3, 3);
        assertInvalidMessage("Non PRIMARY KEY columns found in where clause: values",
                             "UPDATE %s SET value= ? WHERE partitionKey = ? AND clustering_1 = ? AND values CONTAINS ?", 6, 3, 3, 3);
        assertInvalidMessage("Some clustering keys are missing: clustering_1",
                             "UPDATE %s SET values= {6} WHERE partitionKey = ? AND value = ?", 3, 3);
        assertInvalidMessage("Some clustering keys are missing: clustering_1",
                             "UPDATE %s SET value= ? WHERE partitionKey = ? AND values CONTAINS ?", 6, 3, 3);
        assertInvalidMessage("Some partition key parts are missing: partitionkey",
                             "UPDATE %s SET values= {6} WHERE clustering_1 = ?", 3);
        assertInvalidMessage("Some partition key parts are missing: partitionkey",
                             "UPDATE %s SET values= {6} WHERE value = ?", 3);
        assertInvalidMessage("Some partition key parts are missing: partitionkey",
                             "UPDATE %s SET value= ? WHERE values CONTAINS ?", 6, 3);
    }

    @Test
    public void testUpdateWithTwoClusteringColumns() throws Throwable
    {
        testUpdateWithTwoClusteringColumns(false);
        testUpdateWithTwoClusteringColumns(true);
    }

    private void testUpdateWithTwoClusteringColumns(boolean forceFlush) throws Throwable
    {
        for (String compactOption : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (partitionKey int," +
                    "clustering_1 int," +
                    "clustering_2 int," +
                    "value int," +
                    " PRIMARY KEY (partitionKey, clustering_1, clustering_2))" + compactOption);

            execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0)");
            execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 1, 1)");
            execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 2, 2)");
            execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 3, 3)");
            execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 1, 1, 4)");
            execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 1, 2, 5)");
            execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (1, 0, 0, 6)");
            flush(forceFlush);

            execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);
            flush(forceFlush);
            assertRows(execute("SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                               0, 1, 1),
                       row(7));

            execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1, clustering_2) = (?, ?)", 8, 0, 1, 2);
            flush(forceFlush);
            assertRows(execute("SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                               0, 1, 2),
                       row(8));

            execute("UPDATE %s SET value = ? WHERE partitionKey IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?", 9, 0, 1, 0, 0);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?",
                               0, 1, 0, 0),
                       row(0, 0, 0, 9),
                       row(1, 0, 0, 9));

            execute("UPDATE %s SET value = ? WHERE partitionKey IN ? AND clustering_1 = ? AND clustering_2 = ?", 9, Arrays.asList(0, 1), 0, 0);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey IN ? AND clustering_1 = ? AND clustering_2 = ?",
                               Arrays.asList(0, 1), 0, 0),
                       row(0, 0, 0, 9),
                       row(1, 0, 0, 9));

            execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 IN (?, ?)", 12, 0, 1, 1, 2);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 IN (?, ?)",
                               0, 1, 1, 2),
                       row(0, 1, 1, 12),
                       row(0, 1, 2, 12));

            execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 IN (?, ?) AND clustering_2 IN (?, ?)", 10, 0, 1, 0, 1, 2);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?) AND clustering_2 IN (?, ?)",
                               0, 1, 0, 1, 2),
                       row(0, 0, 1, 10),
                       row(0, 0, 2, 10),
                       row(0, 1, 1, 10),
                       row(0, 1, 2, 10));

            execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))", 20, 0, 0, 2, 1, 2);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))",
                               0, 0, 2, 1, 2),
                       row(0, 0, 2, 20),
                       row(0, 1, 2, 20));

            execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", null, 0, 0, 2);
            flush(forceFlush);

            if (isEmpty(compactOption))
            {
                assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))",
                                   0, 0, 2, 1, 2),
                           row(0, 0, 2, null),
                           row(0, 1, 2, 20));
            }
            else
            {
                assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))",
                                   0, 0, 2, 1, 2),
                           row(0, 1, 2, 20));
            }

            // test invalid queries

            // missing primary key element
            assertInvalidMessage("Some partition key parts are missing: partitionkey",
                                 "UPDATE %s SET value = ? WHERE clustering_1 = ? AND clustering_2 = ?", 7, 1, 1);

            String errorMsg = isEmpty(compactOption) ? "Some clustering keys are missing: clustering_1"
                                                     : "PRIMARY KEY column \"clustering_2\" cannot be restricted as preceding column \"clustering_1\" is not restricted";

            assertInvalidMessage(errorMsg,
                                 "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_2 = ?", 7, 0, 1);

            assertInvalidMessage("Some clustering keys are missing: clustering_1, clustering_2",
                                 "UPDATE %s SET value = ? WHERE partitionKey = ?", 7, 0);

            // token function
            assertInvalidMessage("The token function cannot be used in WHERE clauses for UPDATE statements",
                                 "UPDATE %s SET value = ? WHERE token(partitionKey) = token(?) AND clustering_1 = ? AND clustering_2 = ?",
                                 7, 0, 1, 1);

            // multiple time the same value
            assertInvalidSyntax("UPDATE %s SET value = ?, value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);

            // multiple time same primary key element in WHERE clause
            assertInvalidMessage("clustering_1 cannot be restricted by more than one relation if it includes an Equal",
                                 "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND clustering_1 = ?", 7, 0, 1, 1, 1);

            // unknown identifiers
            assertInvalidMessage("Unknown identifier value1",
                                 "UPDATE %s SET value1 = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);

            assertInvalidMessage("Undefined name partitionkey1 in where clause ('partitionkey1 = ?')",
                                 "UPDATE %s SET value = ? WHERE partitionKey1 = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);

            assertInvalidMessage("Undefined name clustering_3 in where clause ('clustering_3 = ?')",
                                 "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_3 = ?", 7, 0, 1, 1);

            // Invalid operator in the where clause
            assertInvalidMessage("Only EQ and IN relation are supported on the partition key (unless you use the token() function)",
                                 "UPDATE %s SET value = ? WHERE partitionKey > ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);

            assertInvalidMessage("Cannot use CONTAINS on non-collection column partitionkey",
                                 "UPDATE %s SET value = ? WHERE partitionKey CONTAINS ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);

            assertInvalidMessage("Non PRIMARY KEY columns found in where clause: value",
                                 "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND value = ?", 7, 0, 1, 1, 3);

            assertInvalidMessage("Slice restrictions are not supported on the clustering columns in UPDATE statements",
                                 "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 > ?", 7, 0, 1);

            assertInvalidMessage("Slice restrictions are not supported on the clustering columns in UPDATE statements",
                                 "UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1, clustering_2) > (?, ?)", 7, 0, 1, 1);
        }
    }

    @Test
    public void testUpdateWithMultiplePartitionKeyComponents() throws Throwable
    {
        testUpdateWithMultiplePartitionKeyComponents(false);
        testUpdateWithMultiplePartitionKeyComponents(true);
    }

    public void testUpdateWithMultiplePartitionKeyComponents(boolean forceFlush) throws Throwable
    {
        for (String compactOption : new String[] { "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (partitionKey_1 int," +
                    "partitionKey_2 int," +
                    "clustering_1 int," +
                    "clustering_2 int," +
                    "value int," +
                    " PRIMARY KEY ((partitionKey_1, partitionKey_2), clustering_1, clustering_2))" + compactOption);

            execute("INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0, 0)");
            execute("INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (0, 1, 0, 1, 1)");
            execute("INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (0, 1, 1, 1, 2)");
            execute("INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (1, 0, 0, 1, 3)");
            execute("INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (1, 1, 0, 1, 3)");
            flush(forceFlush);

            execute("UPDATE %s SET value = ? WHERE partitionKey_1 = ? AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 0, 0, 0);
            flush(forceFlush);
            assertRows(execute("SELECT value FROM %s WHERE partitionKey_1 = ? AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?",
                               0, 0, 0, 0),
                       row(7));

            execute("UPDATE %s SET value = ? WHERE partitionKey_1 IN (?, ?) AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?", 9, 0, 1, 1, 0, 1);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s WHERE partitionKey_1 IN (?, ?) AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?",
                               0, 1, 1, 0, 1),
                       row(0, 1, 0, 1, 9),
                       row(1, 1, 0, 1, 9));

            execute("UPDATE %s SET value = ? WHERE partitionKey_1 IN (?, ?) AND partitionKey_2 IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?", 10, 0, 1, 0, 1, 0, 1);
            flush(forceFlush);
            assertRows(execute("SELECT * FROM %s"),
                       row(0, 0, 0, 0, 7),
                       row(0, 0, 0, 1, 10),
                       row(0, 1, 0, 1, 10),
                       row(0, 1, 1, 1, 2),
                       row(1, 0, 0, 1, 10),
                       row(1, 1, 0, 1, 10));

            // missing primary key element
            assertInvalidMessage("Some partition key parts are missing: partitionkey_2",
                                 "UPDATE %s SET value = ? WHERE partitionKey_1 = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 1, 1);
        }
    }

    @Test
    public void testUpdateWithAStaticColumn() throws Throwable
    {
        testUpdateWithAStaticColumn(false);
        testUpdateWithAStaticColumn(true);
    }

    private void testUpdateWithAStaticColumn(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                                      "clustering_1 int," +
                                      "clustering_2 int," +
                                      "value int," +
                                      "staticValue text static," +
                                      " PRIMARY KEY (partitionKey, clustering_1, clustering_2))");

        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value, staticValue) VALUES (0, 0, 0, 0, 'A')");
        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 1, 1)");
        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value, staticValue) VALUES (1, 0, 0, 6, 'B')");
        flush(forceFlush);

        execute("UPDATE %s SET staticValue = ? WHERE partitionKey = ?", "A2", 0);
        flush(forceFlush);

        assertRows(execute("SELECT DISTINCT staticValue FROM %s WHERE partitionKey = ?", 0),
                   row("A2"));

        assertInvalidMessage("Some clustering keys are missing: clustering_1, clustering_2",
                             "UPDATE %s SET staticValue = ?, value = ? WHERE partitionKey = ?", "A2", 7, 0);

        execute("UPDATE %s SET staticValue = ?, value = ?  WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                "A3", 7, 0, 0, 1);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 0, 1),
                   row(0, 0, 1, "A3", 7));

        assertInvalidMessage("Invalid restrictions on clustering columns since the UPDATE statement modifies only static columns",
                             "UPDATE %s SET staticValue = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                             "A3", 0, 0, 1);
    }

    @Test
    public void testUpdateWithStaticList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int," +
                                      "clustering int," +
                                      "value int," +
                                      "l list<text> static," +
                                      " PRIMARY KEY (k, clustering))");

        execute("INSERT INTO %s(k, clustering, value, l) VALUES (?, ?, ?, ?)", 0, 0, 0 ,list("v1", "v2", "v3"));

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v1", "v2", "v3")));

        execute("UPDATE %s SET l[?] = ? WHERE k = ?", 1, "v4", 0);

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v1", "v4", "v3")));
    }

    /**
     * Test for CASSANDRA-12829
     */
    @Test
    public void testUpdateWithEmptyInRestriction() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a,b))");
        execute("INSERT INTO %s (a,b,c) VALUES (?,?,?)",1,1,1);
        execute("INSERT INTO %s (a,b,c) VALUES (?,?,?)",1,2,2);
        execute("INSERT INTO %s (a,b,c) VALUES (?,?,?)",1,3,3);

        assertInvalidMessage("Some clustering keys are missing: b",
                             "UPDATE %s SET c = 100 WHERE a IN ();");
        execute("UPDATE %s SET c = 100 WHERE a IN () AND b IN ();");
        execute("UPDATE %s SET c = 100 WHERE a IN () AND b = 1;");
        execute("UPDATE %s SET c = 100 WHERE a = 1 AND b IN ();");
        assertRows(execute("SELECT * FROM %s"),
                   row(1,1,1),
                   row(1,2,2),
                   row(1,3,3));

        createTable("CREATE TABLE %s (a int, b int, c int, d int, s int static, PRIMARY KEY ((a,b), c))");
        execute("INSERT INTO %s (a,b,c,d,s) VALUES (?,?,?,?,?)",1,1,1,1,1);
        execute("INSERT INTO %s (a,b,c,d,s) VALUES (?,?,?,?,?)",1,1,2,2,1);
        execute("INSERT INTO %s (a,b,c,d,s) VALUES (?,?,?,?,?)",1,1,3,3,1);

        execute("UPDATE %s SET d = 100 WHERE a = 1 AND b = 1 AND c IN ();");
        execute("UPDATE %s SET d = 100 WHERE a = 1 AND b IN () AND c IN ();");
        execute("UPDATE %s SET d = 100 WHERE a IN () AND b IN () AND c IN ();");
        execute("UPDATE %s SET d = 100 WHERE a IN () AND b IN () AND c = 1;");
        execute("UPDATE %s SET d = 100 WHERE a IN () AND b = 1 AND c IN ();");
        assertRows(execute("SELECT * FROM %s"),
                   row(1,1,1,1,1),
                   row(1,1,2,1,2),
                   row(1,1,3,1,3));

        // No clustering keys restricted, update whole partition
        execute("UPDATE %s set s = 100 where a = 1 AND b = 1;");
        assertRows(execute("SELECT * FROM %s"),
                   row(1,1,1,100,1),
                   row(1,1,2,100,2),
                   row(1,1,3,100,3));

        execute("UPDATE %s set s = 200 where a = 1 AND b IN ();");
        assertRows(execute("SELECT * FROM %s"),
                   row(1,1,1,100,1),
                   row(1,1,2,100,2),
                   row(1,1,3,100,3));

        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY ((a,b), c, d))");
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)",1,1,1,1,1);
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)",1,1,1,2,2);
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)",1,1,1,3,3);
        execute("INSERT INTO %s (a,b,c,d,e) VALUES (?,?,?,?,?)",1,1,1,4,4);

        execute("UPDATE %s SET e = 100 WHERE a = 1 AND b = 1 AND c = 1 AND d IN ();");
        execute("UPDATE %s SET e = 100 WHERE a = 1 AND b = 1 AND c IN () AND d IN ();");
        execute("UPDATE %s SET e = 100 WHERE a = 1 AND b IN () AND c IN () AND d IN ();");
        execute("UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c IN () AND d IN ();");
        execute("UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c IN () AND d = 1;");
        execute("UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c = 1 AND d = 1;");
        execute("UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c = 1 AND d IN ();");
        assertRows(execute("SELECT * FROM %s"),
                   row(1,1,1,1,1),
                   row(1,1,1,2,2),
                   row(1,1,1,3,3),
                   row(1,1,1,4,4));
    }

    /**
     * Test for CASSANDRA-13152
     */
    @Test
    public void testThatUpdatesWithEmptyInRestrictionDoNotCreateMutations() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a,b))");

        execute("UPDATE %s SET c = 100 WHERE a IN () AND b = 1;");
        execute("UPDATE %s SET c = 100 WHERE a = 1 AND b IN ();");

        assertTrue("The memtable should be empty but is not", isMemtableEmpty());

        createTable("CREATE TABLE %s (a int, b int, c int, d int, s int static, PRIMARY KEY ((a,b), c))");

        execute("UPDATE %s SET d = 100 WHERE a = 1 AND b = 1 AND c IN ();");
        execute("UPDATE %s SET d = 100 WHERE a = 1 AND b IN () AND c IN ();");
        execute("UPDATE %s SET d = 100 WHERE a IN () AND b IN () AND c IN ();");
        execute("UPDATE %s SET d = 100 WHERE a IN () AND b IN () AND c = 1;");
        execute("UPDATE %s SET d = 100 WHERE a IN () AND b = 1 AND c IN ();");

        assertTrue("The memtable should be empty but is not", isMemtableEmpty());

        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY ((a,b), c, d))");

        execute("UPDATE %s SET e = 100 WHERE a = 1 AND b = 1 AND c = 1 AND d IN ();");
        execute("UPDATE %s SET e = 100 WHERE a = 1 AND b = 1 AND c IN () AND d IN ();");
        execute("UPDATE %s SET e = 100 WHERE a = 1 AND b IN () AND c IN () AND d IN ();");
        execute("UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c IN () AND d IN ();");
        execute("UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c IN () AND d = 1;");
        execute("UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c = 1 AND d = 1;");
        execute("UPDATE %s SET e = 100 WHERE a IN () AND b IN () AND c = 1 AND d IN ();");

        assertTrue("The memtable should be empty but is not", isMemtableEmpty());
    }

    /**
     * Checks if the memtable is empty or not
     * @return {@code true} if the memtable is empty, {@code false} otherwise.
     */
    private boolean isMemtableEmpty()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(currentTable());
        return cfs.metric.allMemtablesLiveDataSize.getValue() == 0;
    }

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testUpdateWithCompactStaticFormat() throws Throwable
    {
        testWithCompactFormat("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");

        assertInvalidMessage("Undefined name column1 in where clause ('column1 = ?')",
                             "UPDATE %s SET b = 1 WHERE column1 = ?",
                             ByteBufferUtil.bytes('a'));
        assertInvalidMessage("Undefined name value in where clause ('value = ?')",
                             "UPDATE %s SET b = 1 WHERE value = ?",
                             ByteBufferUtil.bytes('a'));

        // if column1 is present, hidden column is called column2
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, column1 int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c, column1) VALUES (1, 1, 1, 1)");
        execute("UPDATE %s SET column1 = 6 WHERE a = 1");
        assertInvalidMessage("Unknown identifier column2", "UPDATE %s SET column2 = 6 WHERE a = 0");
        assertInvalidMessage("Unknown identifier value", "UPDATE %s SET value = 6 WHERE a = 0");

        // if value is present, hidden column is called value1
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, value int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c, value) VALUES (1, 1, 1, 1)");
        execute("UPDATE %s SET value = 6 WHERE a = 1");
        assertInvalidMessage("Unknown identifier column1", "UPDATE %s SET column1 = 6 WHERE a = 1");
        assertInvalidMessage("Unknown identifier value1", "UPDATE %s SET value1 = 6 WHERE a = 1");
    }

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testUpdateWithCompactNonStaticFormat() throws Throwable
    {
        testWithCompactFormat("CREATE TABLE %s (a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        testWithCompactFormat("CREATE TABLE %s (a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
    }

    private void testWithCompactFormat(String tableQuery) throws Throwable
    {
        createTable(tableQuery);
        // pass correct types to hidden columns
        assertInvalidMessage("Unknown identifier column1",
                             "UPDATE %s SET column1 = ? WHERE a = 0",
                             ByteBufferUtil.bytes('a'));
        assertInvalidMessage("Unknown identifier value",
                             "UPDATE %s SET value = ? WHERE a = 0",
                             ByteBufferUtil.bytes('a'));

        // pass incorrect types to hidden columns
        assertInvalidMessage("Unknown identifier column1", "UPDATE %s SET column1 = 6 WHERE a = 0");
        assertInvalidMessage("Unknown identifier value", "UPDATE %s SET value = 6 WHERE a = 0");
    }
}
