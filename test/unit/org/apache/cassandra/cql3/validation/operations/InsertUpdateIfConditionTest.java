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

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.SchemaKeyspace;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InsertUpdateIfConditionTest extends CQLTester
{
    /**
     * Migrated from cql_tests.py:TestCQL.cas_simple_test()
     */
    @Test
    public void testSimpleCas() throws Throwable
    {
        createTable("CREATE TABLE %s (tkn int, consumed boolean, PRIMARY KEY (tkn))");

        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (tkn, consumed) VALUES (?, FALSE)", i);

            assertRows(execute("UPDATE %s SET consumed = TRUE WHERE tkn = ? IF consumed = ?", i, false), row(true));
            assertRows(execute("UPDATE %s SET consumed = TRUE WHERE tkn = ? IF consumed = ?", i, false), row(false, true));
        }
    }

    /**
     * Migrated from cql_tests.py:TestCQL.conditional_update_test()
     */
    @Test
    public void testConditionalUpdate() throws Throwable
    {
        createTable(" CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 text, v3 int)");

        assertInvalidMessage("Invalid 'unset' value in condition",
                             "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = ?", unset());

        // Shouldn't apply
        assertRows(execute("UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = ?", 4), row(false));
        assertRows(execute("UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF EXISTS"), row(false));

        // Should apply
        assertRows(execute("INSERT INTO %s (k, v1, v2) VALUES (0, 2, 'foo') IF NOT EXISTS"), row(true));

        // Shouldn't apply
        assertRows(execute("INSERT INTO %s (k, v1, v2) VALUES (0, 5, 'bar') IF NOT EXISTS"), row(false, 0, 2, "foo", null));
        assertRows(execute("SELECT * FROM %s"), row(0, 2, "foo", null));

        // Shouldn't apply
        assertRows(execute("UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = ?", 4), row(false, 2));
        assertRows(execute("SELECT * FROM %s"), row(0, 2, "foo", null));

        // Should apply (note: we want v2 before v1 in the statement order to exercise #5786)
        assertRows(execute("UPDATE %s SET v2 = 'bar', v1 = 3 WHERE k = 0 IF v1 = ?", 2), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar', v1 = 3 WHERE k = 0 IF EXISTS"), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, 3, "bar", null));

        // Shouldn't apply, only one condition is ok
        assertRows(execute("UPDATE %s SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = ? AND v2 = ?", 3, "foo"), row(false, 3, "bar"));
        assertRows(execute("SELECT * FROM %s"), row(0, 3, "bar", null));

        // Should apply
        assertRows(execute("UPDATE %s SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = ? AND v2 = ?", 3, "bar"), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, 5, "foobar", null));

        // Shouldn't apply
        assertRows(execute("DELETE v2 FROM %s WHERE k = 0 IF v1 = ?", 3), row(false, 5));
        assertRows(execute("SELECT * FROM %s"), row(0, 5, "foobar", null));

        // Shouldn't apply
        assertRows(execute("DELETE v2 FROM %s WHERE k = 0 IF v1 = ?", (Integer) null), row(false, 5));
        assertRows(execute("SELECT * FROM %s"), row(0, 5, "foobar", null));

        // Should apply
        assertRows(execute("DELETE v2 FROM %s WHERE k = 0 IF v1 = ?", 5), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, 5, null, null));

        // Shouln't apply
        assertRows(execute("DELETE v1 FROM %s WHERE k = 0 IF v3 = ?", 4), row(false, null));

        // Should apply
        assertRows(execute("DELETE v1 FROM %s WHERE k = 0 IF v3 = ?", (Integer) null), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, null, null, null));

        // Should apply
        assertRows(execute("DELETE FROM %s WHERE k = 0 IF v1 = ?", (Integer) null), row(true));
        assertEmpty(execute("SELECT * FROM %s"));

        // Shouldn't apply
        assertRows(execute("UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF EXISTS"), row(false));

        // Should apply
        assertEmpty(execute("SELECT * FROM %s WHERE k = 0"));
        assertRows(execute("DELETE FROM %s WHERE k = 0 IF v1 IN (?)", (Integer) null), row(true));

        createTable(" CREATE TABLE %s (k int, c int, v1 text, PRIMARY KEY(k, c))");
        assertInvalidMessage("IN on the clustering key columns is not supported with conditional updates",
                             "UPDATE %s SET v1 = 'A' WHERE k = 0 AND c IN () IF EXISTS");
        assertInvalidMessage("IN on the clustering key columns is not supported with conditional updates",
                             "UPDATE %s SET v1 = 'A' WHERE k = 0 AND c IN (1, 2) IF EXISTS");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.non_eq_conditional_update_test()
     */
    @Test
    public void testNonEqConditionalUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 text, v3 int)");

        // non-EQ conditions
        execute("INSERT INTO %s (k, v1, v2) VALUES (0, 2, 'foo')");

        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 < ?", 3), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 <= ?", 3), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 > ?", 1), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 >= ?", 1), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 != ?", 1), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 != ?", 2), row(false, 2));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 IN (?, ?, ?)", 0, 1, 2), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 IN ?", list(142, 276)), row(false, 2));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 IN ()"), row(false, 2));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 IN (?, ?)", unset(), 1), row(false, 2));

        assertInvalidMessage("Invalid 'unset' value in condition",
                             "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 < ?", unset());
        assertInvalidMessage("Invalid 'unset' value in condition",
                             "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 <= ?", unset());
        assertInvalidMessage("Invalid 'unset' value in condition",
                             "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 > ?", unset());
        assertInvalidMessage("Invalid 'unset' value in condition",
                             "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 >= ?", unset());
        assertInvalidMessage("Invalid 'unset' value in condition",
                             "UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 != ?", unset());

    }

    /**
     * Migrated from cql_tests.py:TestCQL.conditional_delete_test()
     */
    @Test
    public void testConditionalDelete() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int,)");

        assertRows(execute("DELETE FROM %s WHERE k=1 IF EXISTS"), row(false));

        execute("INSERT INTO %s (k, v1) VALUES (1, 2)");
        assertRows(execute("DELETE FROM %s WHERE k=1 IF EXISTS"), row(true));
        assertEmpty(execute("SELECT * FROM %s WHERE k=1"));
        assertRows(execute("DELETE FROM %s WHERE k=1 IF EXISTS"), row(false));

        execute("UPDATE %s USING TTL 1 SET v1=2 WHERE k=1");
        Thread.sleep(1001);
        assertRows(execute("DELETE FROM %s WHERE k=1 IF EXISTS"), row(false));
        assertEmpty(execute("SELECT * FROM %s WHERE k=1"));

        execute("INSERT INTO %s (k, v1) VALUES (2, 2) USING TTL 1");
        Thread.sleep(1001);
        assertRows(execute("DELETE FROM %s WHERE k=2 IF EXISTS"), row(false));
        assertEmpty(execute("SELECT * FROM %s WHERE k=2"));

        execute("INSERT INTO %s (k, v1) VALUES (3, 2)");
        assertRows(execute("DELETE v1 FROM %s WHERE k=3 IF EXISTS"), row(true));
        assertRows(execute("SELECT * FROM %s WHERE k=3"), row(3, null));
        assertRows(execute("DELETE v1 FROM %s WHERE k=3 IF EXISTS"), row(true));
        assertRows(execute("DELETE FROM %s WHERE k=3 IF EXISTS"), row(true));

        execute("INSERT INTO %s (k, v1) VALUES (4, 2)");
        execute("UPDATE %s USING TTL 1 SET v1=2 WHERE k=4");
        Thread.sleep(1001);
        assertRows(execute("SELECT * FROM %s WHERE k=4"), row(4, null));
        assertRows(execute("DELETE FROM %s WHERE k=4 IF EXISTS"), row(true));
        assertEmpty(execute("SELECT * FROM %s WHERE k=4"));

        // static columns
        createTable("CREATE TABLE %s (k text, s text static, i int, v text, PRIMARY KEY (k, i) )");

        execute("INSERT INTO %s (k, s, i, v) VALUES ('k', 's', 0, 'v')");
        assertRows(execute("DELETE v FROM %s WHERE k='k' AND i=0 IF EXISTS"), row(true));
        assertRows(execute("DELETE FROM %s WHERE k='k' AND i=0 IF EXISTS"), row(true));
        assertRows(execute("SELECT * FROM %s"), row("k", null, "s", null));
        assertRows(execute("DELETE v FROM %s WHERE k='k' AND i=0 IF s = 'z'"), row(false, "s"));
        assertRows(execute("DELETE v FROM %s WHERE k='k' AND i=0 IF v = 'z'"), row(false));
        assertRows(execute("DELETE v FROM %s WHERE k='k' AND i=0 IF v = 'z' AND s = 'z'"), row(false, null, "s"));
        assertRows(execute("DELETE v FROM %s WHERE k='k' AND i=0 IF EXISTS"), row(false));
        assertRows(execute("DELETE FROM %s WHERE k='k' AND i=0 IF EXISTS"), row(false));

        // CASSANDRA-6430
        assertInvalidMessage("DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns",
                             "DELETE FROM %s WHERE k = 'k' IF EXISTS");
        assertInvalidMessage("DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns",
                             "DELETE FROM %s WHERE k = 'k' IF v = ?", "foo");
        assertInvalidMessage("Some partition key parts are missing: k",
                             "DELETE FROM %s WHERE i = 0 IF EXISTS");

        assertInvalidMessage("Invalid INTEGER constant (0) for \"k\" of type text",
                             "DELETE FROM %s WHERE k = 0 AND i > 0 IF EXISTS");
        assertInvalidMessage("Invalid INTEGER constant (0) for \"k\" of type text",
                             "DELETE FROM %s WHERE k = 0 AND i > 0 IF v = 'foo'");
        assertInvalidMessage("DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns",
                             "DELETE FROM %s WHERE k = 'k' AND i > 0 IF EXISTS");
        assertInvalidMessage("DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns",
                             "DELETE FROM %s WHERE k = 'k' AND i > 0 IF v = 'foo'");
        assertInvalidMessage("IN on the clustering key columns is not supported with conditional deletions",
                             "DELETE FROM %s WHERE k = 'k' AND i IN (0, 1) IF v = 'foo'");
        assertInvalidMessage("IN on the clustering key columns is not supported with conditional deletions",
                             "DELETE FROM %s WHERE k = 'k' AND i IN () IF v = 'foo'");
        assertInvalidMessage("IN on the clustering key columns is not supported with conditional deletions",
                             "DELETE FROM %s WHERE k = 'k' AND i IN (0, 1) IF EXISTS");
        assertInvalidMessage("IN on the clustering key columns is not supported with conditional deletions",
                             "DELETE FROM %s WHERE k = 'k' AND i IN () IF EXISTS");

        assertInvalidMessage("Invalid 'unset' value in condition",
                             "DELETE FROM %s WHERE k = 'k' AND i = 0 IF v = ?", unset());

        createTable("CREATE TABLE %s(k int, s int static, i int, v text, PRIMARY KEY(k, i))");
        execute("INSERT INTO %s (k, s, i, v) VALUES ( 1, 1, 2, '1')");
        assertRows(execute("DELETE v FROM %s WHERE k = 1 AND i = 2 IF s != ?", 1), row(false, 1));
        assertRows(execute("DELETE v FROM %s WHERE k = 1 AND i = 2 IF s = ?", 1), row(true));
        assertRows(execute("SELECT * FROM %s WHERE k = 1 AND i = 2"), row(1, 2, 1, null));

        assertRows(execute("DELETE FROM %s WHERE  k = 1 AND i = 2 IF s != ?", 1), row(false, 1));
        assertRows(execute("DELETE FROM %s WHERE k = 1 AND i = 2 IF s = ?", 1), row(true));
        assertEmpty(execute("SELECT * FROM %s WHERE k = 1 AND i = 2"));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, null, 1, null));

        createTable("CREATE TABLE %s (k int, i int, v1 int, v2 int, s int static, PRIMARY KEY (k, i))");
        execute("INSERT INTO %s (k, i, v1, v2, s) VALUES (?, ?, ?, ?, ?)",
                1, 1, 1, 1, 1);
        assertRows(execute("DELETE v1 FROM %s WHERE k = 1 AND i = 1 IF EXISTS"),
                   row(true));
        assertRows(execute("DELETE v2 FROM %s WHERE k = 1 AND i = 1 IF EXISTS"),
                   row(true));
        assertRows(execute("DELETE FROM %s WHERE k = 1 AND i = 1 IF EXISTS"),
                   row(true));
        assertRows(execute("select * from %s"),
                   row(1, null, 1, null, null));
        assertRows(execute("DELETE v1 FROM %s WHERE k = 1 AND i = 1 IF EXISTS"),
                   row(false));
        assertRows(execute("DELETE v1 FROM %s WHERE k = 1 AND i = 1 IF s = 5"),
                   row(false, 1));
        assertRows(execute("DELETE v1 FROM %s WHERE k = 1 AND i = 1 IF v1 = 1 AND v2 = 1"),
                   row(false));
        assertRows(execute("DELETE v1 FROM %s WHERE k = 1 AND i = 1 IF v1 = 1 AND v2 = 1 AND s = 1"),
                   row(false, null, null, 1));
        assertRows(execute("DELETE v1 FROM %s WHERE k = 1 AND i = 5 IF s = 1"),
                   row(true));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.static_columns_cas_test()
     */
    @Test
    public void testStaticColumnsCas() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, k text, version int static, v text, PRIMARY KEY (id, k))");

        // Test that INSERT IF NOT EXISTS concerns only the static column if no clustering nor regular columns
        // is provided, but concerns the CQL3 row targetted by the clustering columns otherwise
        execute("INSERT INTO %s (id, k, v) VALUES (1, 'foo', 'foo')");
        assertRows(execute("INSERT INTO %s (id, k, version) VALUES (1, 'foo', 1) IF NOT EXISTS"), row(false, 1, "foo", null, "foo"));
        assertRows(execute("INSERT INTO %s (id, version) VALUES (1, 1) IF NOT EXISTS"), row(true));
        assertRows(execute("SELECT * FROM %s"), row(1, "foo", 1, "foo"));
        execute("DELETE FROM %s WHERE id = 1");

        execute("INSERT INTO %s(id, version) VALUES (0, 0)");

        assertRows(execute("UPDATE %s SET v='foo', version=1 WHERE id=0 AND k='k1' IF version = ?", 0), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, "k1", 1, "foo"));

        assertRows(execute("UPDATE %s SET v='bar', version=1 WHERE id=0 AND k='k2' IF version = ?", 0), row(false, 1));
        assertRows(execute("SELECT * FROM %s"), row(0, "k1", 1, "foo"));

        assertRows(execute("UPDATE %s SET v='bar', version=2 WHERE id=0 AND k='k2' IF version = ?", 1), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, "k1", 2, "foo"), row(0, "k2", 2, "bar"));

        // Batch output is slightly different from non-batch CAS, since a full PK is included to disambiguate
        // cases when conditions span across multiple rows.
        assertRows(execute("UPDATE %1$s SET version=3 WHERE id=0 IF version=1; "),
                   row(false, 2));
        // Testing batches
        assertRows(execute("BEGIN BATCH " +
                           "UPDATE %1$s SET v='foobar' WHERE id=0 AND k='k1'; " +
                           "UPDATE %1$s SET v='barfoo' WHERE id=0 AND k='k2'; " +
                           "UPDATE %1$s SET version=3 WHERE id=0 IF version=1; " +
                           "APPLY BATCH "),
                   row(false, 0, "k1", 2));

        assertRows(execute("BEGIN BATCH " +
                           "UPDATE %1$s SET v = 'foobar' WHERE id = 0 AND k = 'k1'; " +
                           "UPDATE %1$s SET v = 'barfoo' WHERE id = 0 AND k = 'k2'; " +
                           "UPDATE %1$s SET version = 3 WHERE id = 0 IF version = 2; " +
                           "APPLY BATCH "),
                   row(true));

        assertRows(execute("SELECT * FROM %s"),
                   row(0, "k1", 3, "foobar"),
                   row(0, "k2", 3, "barfoo"));

        assertRows(execute("BEGIN BATCH " +
                           "UPDATE %1$s SET version = 4 WHERE id = 0 IF version = 3; " +
                           "UPDATE %1$s SET v='row1' WHERE id=0 AND k='k1' IF v='foo'; " +
                           "UPDATE %1$s SET v='row2' WHERE id=0 AND k='k2' IF v='bar'; " +
                           "APPLY BATCH "),
                   row(false, 0, "k1", 3, "foobar"),
                   row(false, 0, "k2", 3, "barfoo"));

        assertRows(execute("BEGIN BATCH " +
                           "UPDATE %1$s SET version = 4 WHERE id = 0 IF version = 3; " +
                           "UPDATE %1$s SET v='row1' WHERE id = 0 AND k='k1' IF v='foobar'; " +
                           "UPDATE %1$s SET v='row2' WHERE id = 0 AND k='k2' IF v='barfoo'; " +
                           "APPLY BATCH "),
                   row(true));

        assertRows(execute("SELECT * FROM %s"),
                   row(0, "k1", 4, "row1"),
                   row(0, "k2", 4, "row2"));

        assertInvalid("BEGIN BATCH " +
                      "UPDATE %1$s SET version=5 WHERE id=0 IF version=4; " +
                      "UPDATE %1$s SET v='row1' WHERE id=0 AND k='k1'; " +
                      "UPDATE %1$s SET v='row2' WHERE id=1 AND k='k2'; " +
                      "APPLY BATCH ");

        assertRows(execute("BEGIN BATCH " +
                           "INSERT INTO %1$s (id, k, v) VALUES (1, 'k1', 'val1') IF NOT EXISTS; " +
                           "INSERT INTO %1$s (id, k, v) VALUES (1, 'k2', 'val2') IF NOT EXISTS; " +
                           "APPLY BATCH "),
                   row(true));

        assertRows(execute("SELECT * FROM %s WHERE id=1"),
                   row(1, "k1", null, "val1"),
                   row(1, "k2", null, "val2"));

        assertRows(execute("INSERT INTO %s (id, k, v) VALUES (1, 'k2', 'val2') IF NOT EXISTS"), row(false, 1, "k2", null, "val2"));

        assertRows(execute("BEGIN BATCH " +
                           "INSERT INTO %1$s (id, k, v) VALUES (1, 'k2', 'val2') IF NOT EXISTS; " +
                           "INSERT INTO %1$s (id, k, v) VALUES (1, 'k3', 'val3') IF NOT EXISTS; " +
                           "APPLY BATCH"),
                   row(false, 1, "k2", null, "val2"));

        assertRows(execute("BEGIN BATCH " +
                           "UPDATE %1$s SET v = 'newVal' WHERE id = 1 AND k = 'k2' IF v = 'val0'; " +
                           "INSERT INTO %1$s (id, k, v) VALUES (1, 'k3', 'val3') IF NOT EXISTS; " +
                           "APPLY BATCH"),
                   row(false, 1, "k2", null, "val2"));

        assertRows(execute("SELECT * FROM %s WHERE id=1"),
                   row(1, "k1", null, "val1"),
                   row(1, "k2", null, "val2"));

        assertRows(execute("BEGIN BATCH " +
                           "UPDATE %1$s SET v = 'newVal' WHERE id = 1 AND k = 'k2' IF v = 'val2'; " +
                           "INSERT INTO %1$s (id, k, v, version) VALUES(1, 'k3', 'val3', 1) IF NOT EXISTS; " +
                           "APPLY BATCH"),
                   row(true));

        assertRows(execute("SELECT * FROM %s WHERE id=1"),
                   row(1, "k1", 1, "val1"),
                   row(1, "k2", 1, "newVal"),
                   row(1, "k3", 1, "val3"));

        assertRows(execute("BEGIN BATCH " +
                           "UPDATE %1$s SET v = 'newVal1' WHERE id = 1 AND k = 'k2' IF v = 'val2'; " +
                           "UPDATE %1$s SET v = 'newVal2' WHERE id = 1 AND k = 'k2' IF v = 'val3'; " +
                           "APPLY BATCH"),
                   row(false, 1, "k2", "newVal"));
    }

    /**
     * Test CASSANDRA-10532
     */
    @Test
    public void testStaticColumnsCasDelete() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, static_col int static, value int, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 1, 2);
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 3, 4);
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 5, 6);
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 7, 8);
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 2, 1, 2);
        execute("INSERT INTO %s (pk, static_col) VALUES (?, ?)", 1, 1);

        assertRows(execute("DELETE static_col FROM %s WHERE pk = ? IF static_col = ?", 1, 2), row(false, 1));
        assertRows(execute("DELETE static_col FROM %s WHERE pk = ? IF static_col = ?", 1, 1), row(true));

        assertRows(execute("SELECT pk, ck, static_col, value FROM %s WHERE pk = 1"),
                   row(1, 1, null, 2),
                   row(1, 3, null, 4),
                   row(1, 5, null, 6),
                   row(1, 7, null, 8));
        execute("INSERT INTO %s (pk, static_col) VALUES (?, ?)", 1, 1);

        assertInvalidMessage("Some partition key parts are missing: pk",
                             "DELETE static_col FROM %s WHERE ck = ? IF static_col = ?", 1, 1);

        assertInvalidMessage("Invalid restrictions on clustering columns since the DELETE statement modifies only static columns",
                             "DELETE static_col FROM %s WHERE pk = ? AND ck = ? IF static_col = ?", 1, 1, 1);

        assertInvalidMessage("DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns",
                             "DELETE static_col, value FROM %s WHERE pk = ? IF static_col = ?", 1, 1);

        // Same query but with an invalid condition
        assertInvalidMessage("DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns",
                             "DELETE static_col, value FROM %s WHERE pk = ? IF static_col = ?", 1, 2);

        // DELETE of an underspecified PRIMARY KEY should not succeed if static is not only restriction
        assertInvalidMessage("DELETE statements must restrict all PRIMARY KEY columns with equality relations" +
                             " in order to use IF condition on non static columns",
                             "DELETE static_col FROM %s WHERE pk = ? IF value = ? AND static_col = ?", 1, 2, 1);

        assertRows(execute("DELETE value FROM %s WHERE pk = ? AND ck = ? IF value = ? AND static_col = ?", 1, 1, 2, 2), row(false, 2, 1));
        assertRows(execute("DELETE value FROM %s WHERE pk = ? AND ck = ? IF value = ? AND static_col = ?", 1, 1, 2, 1), row(true));
        assertRows(execute("SELECT pk, ck, static_col, value FROM %s WHERE pk = 1"),
                   row(1, 1, 1, null),
                   row(1, 3, 1, 4),
                   row(1, 5, 1, 6),
                   row(1, 7, 1, 8));

        assertRows(execute("DELETE static_col FROM %s WHERE pk = ? AND ck = ? IF value = ?", 1, 5, 10), row(false, 6));
        assertRows(execute("DELETE static_col FROM %s WHERE pk = ? AND ck = ? IF value = ?", 1, 5, 6), row(true));
        assertRows(execute("SELECT pk, ck, static_col, value FROM %s WHERE pk = 1"),
                   row(1, 1, null, null),
                   row(1, 3, null, 4),
                   row(1, 5, null, 6),
                   row(1, 7, null, 8));
    }

    @Test
    public void testStaticColumnsCasUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, static_col int static, value int, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 1, 2);
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 3, 4);
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 5, 6);
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, 7, 8);
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 2, 1, 2);
        execute("INSERT INTO %s (pk, static_col) VALUES (?, ?)", 1, 1);

        assertRows(execute("UPDATE %s SET static_col = ? WHERE pk = ? IF static_col = ?", 3, 1, 2), row(false, 1));
        assertRows(execute("UPDATE %s SET static_col = ? WHERE pk = ? IF static_col = ?", 2, 1, 1), row(true));

        assertRows(execute("SELECT pk, ck, static_col, value FROM %s WHERE pk = 1"),
                   row(1, 1, 2, 2),
                   row(1, 3, 2, 4),
                   row(1, 5, 2, 6),
                   row(1, 7, 2, 8));

        assertInvalidMessage("Some partition key parts are missing: pk",
                             "UPDATE %s SET static_col = ? WHERE ck = ? IF static_col = ?", 3, 1, 1);

        assertInvalidMessage("Invalid restrictions on clustering columns since the UPDATE statement modifies only static columns",
                             "UPDATE %s SET static_col = ? WHERE pk = ? AND ck = ? IF static_col = ?", 3, 1, 1, 1);

        assertInvalidMessage("Some clustering keys are missing: ck",
                             "UPDATE %s SET static_col = ?, value = ? WHERE pk = ? IF static_col = ?", 3, 1, 1, 2);

        // Same query but with an invalid condition
        assertInvalidMessage("Some clustering keys are missing: ck",
                             "UPDATE %s SET static_col = ?, value = ? WHERE pk = ? IF static_col = ?", 3, 1, 1, 1);

        assertInvalidMessage("Some clustering keys are missing: ck",
                             "UPDATE %s SET static_col = ? WHERE pk = ? IF value = ? AND static_col = ?", 3, 1, 4, 2);

        assertRows(execute("UPDATE %s SET value = ? WHERE pk = ? AND ck = ? IF value = ? AND static_col = ?", 3, 1, 1, 3, 2), row(false, 2, 2));
        assertRows(execute("UPDATE %s SET value = ? WHERE pk = ? AND ck = ? IF value = ? AND static_col = ?", 1, 1, 1, 2, 2), row(true));
        assertRows(execute("SELECT pk, ck, static_col, value FROM %s WHERE pk = 1"),
                   row(1, 1, 2, 1),
                   row(1, 3, 2, 4),
                   row(1, 5, 2, 6),
                   row(1, 7, 2, 8));

        assertRows(execute("UPDATE %s SET static_col = ? WHERE pk = ? AND ck = ? IF value = ?", 3, 1, 1, 2), row(false, 1));
        assertRows(execute("UPDATE %s SET static_col = ? WHERE pk = ? AND ck = ? IF value = ?", 1, 1, 1, 1), row(true));
        assertRows(execute("SELECT pk, ck, static_col, value FROM %s WHERE pk = 1"),
                   row(1, 1, 1, 1),
                   row(1, 3, 1, 4),
                   row(1, 5, 1, 6),
                   row(1, 7, 1, 8));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.bug_6069_test()
     */
    @Test
    public void testInsertSetIfNotExists() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<int>)");

        assertRows(execute("INSERT INTO %s (k, s) VALUES (0, {1, 2, 3}) IF NOT EXISTS"),
                   row(true));
        assertRows(execute("SELECT * FROM %s "), row(0, set(1, 2, 3)));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.cas_and_ttl_test()
     */
    @Test
    public void testCasAndTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, lock boolean)");

        execute("INSERT INTO %s (k, v, lock) VALUES (0, 0, false)");
        execute("UPDATE %s USING TTL 1 SET lock=true WHERE k=0");

        Thread.sleep(1001);
        assertRows(execute("UPDATE %s SET v = 1 WHERE k = 0 IF lock = null"),
                   row(true));
    }

    /**
     * Test for CAS with compact storage table, and #6813 in particular,
     * migrated from cql_tests.py:TestCQL.cas_and_compact_test()
     */
    @Test
    public void testCompactStorage() throws Throwable
    {
        createTable("CREATE TABLE %s (partition text, key text, owner text, PRIMARY KEY (partition, key) ) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (partition, key, owner) VALUES ('a', 'b', null)");
        assertRows(execute("UPDATE %s SET owner='z' WHERE partition='a' AND key='b' IF owner=null"), row(true));

        assertRows(execute("UPDATE %s SET owner='b' WHERE partition='a' AND key='b' IF owner='a'"), row(false, "z"));
        assertRows(execute("UPDATE %s SET owner='b' WHERE partition='a' AND key='b' IF owner='z'"), row(true));

        assertRows(execute("INSERT INTO %s (partition, key, owner) VALUES ('a', 'c', 'x') IF NOT EXISTS"), row(true));
    }

    @Test
    public void testWholeUDT() throws Throwable
    {
        String typename = createType("CREATE TYPE %s (a int, b text)");
        String myType = KEYSPACE + '.' + typename;

        for (boolean frozen : new boolean[] {false, true})
        {
            createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, v %s)",
                                      frozen
                                      ? "frozen<" + myType + ">"
                                      : myType));

            Object v = userType("a", 0, "b", "abc");
            execute("INSERT INTO %s (k, v) VALUES (0, ?)", v);

            checkAppliesUDT("v = {a: 0, b: 'abc'}", v);
            checkAppliesUDT("v != null", v);
            checkAppliesUDT("v != {a: 1, b: 'abc'}", v);
            checkAppliesUDT("v != {a: 0, b: 'def'}", v);
            checkAppliesUDT("v > {a: -1, b: 'abc'}", v);
            checkAppliesUDT("v > {a: 0, b: 'aaa'}", v);
            checkAppliesUDT("v > {a: 0}", v);
            checkAppliesUDT("v >= {a: 0, b: 'aaa'}", v);
            checkAppliesUDT("v >= {a: 0, b: 'abc'}", v);
            checkAppliesUDT("v < {a: 0, b: 'zzz'}", v);
            checkAppliesUDT("v < {a: 1, b: 'abc'}", v);
            checkAppliesUDT("v < {a: 1}", v);
            checkAppliesUDT("v <= {a: 0, b: 'zzz'}", v);
            checkAppliesUDT("v <= {a: 0, b: 'abc'}", v);
            checkAppliesUDT("v IN (null, {a: 0, b: 'abc'}, {a: 1})", v);

            // multiple conditions
            checkAppliesUDT("v > {a: -1, b: 'abc'} AND v > {a: 0}", v);
            checkAppliesUDT("v != null AND v IN ({a: 0, b: 'abc'})", v);

            // should not apply
            checkDoesNotApplyUDT("v = {a: 0, b: 'def'}", v);
            checkDoesNotApplyUDT("v = {a: 1, b: 'abc'}", v);
            checkDoesNotApplyUDT("v = null", v);
            checkDoesNotApplyUDT("v != {a: 0, b: 'abc'}", v);
            checkDoesNotApplyUDT("v > {a: 1, b: 'abc'}", v);
            checkDoesNotApplyUDT("v > {a: 0, b: 'zzz'}", v);
            checkDoesNotApplyUDT("v >= {a: 1, b: 'abc'}", v);
            checkDoesNotApplyUDT("v >= {a: 0, b: 'zzz'}", v);
            checkDoesNotApplyUDT("v < {a: -1, b: 'abc'}", v);
            checkDoesNotApplyUDT("v < {a: 0, b: 'aaa'}", v);
            checkDoesNotApplyUDT("v <= {a: -1, b: 'abc'}", v);
            checkDoesNotApplyUDT("v <= {a: 0, b: 'aaa'}", v);
            checkDoesNotApplyUDT("v IN ({a: 0}, {b: 'abc'}, {a: 0, b: 'def'}, null)", v);
            checkDoesNotApplyUDT("v IN ()", v);

            // multiple conditions
            checkDoesNotApplyUDT("v IN () AND v IN ({a: 0, b: 'abc'})", v);
            checkDoesNotApplyUDT("v > {a: 0, b: 'aaa'} AND v < {a: 0, b: 'aaa'}", v);

            // invalid conditions
            checkInvalidUDT("v = {a: 1, b: 'abc', c: 'foo'}", v, InvalidRequestException.class);
            checkInvalidUDT("v = {foo: 'foo'}", v, InvalidRequestException.class);
            checkInvalidUDT("v < {a: 1, b: 'abc', c: 'foo'}", v, InvalidRequestException.class);
            checkInvalidUDT("v < null", v, InvalidRequestException.class);
            checkInvalidUDT("v <= {a: 1, b: 'abc', c: 'foo'}", v, InvalidRequestException.class);
            checkInvalidUDT("v <= null", v, InvalidRequestException.class);
            checkInvalidUDT("v > {a: 1, b: 'abc', c: 'foo'}", v, InvalidRequestException.class);
            checkInvalidUDT("v > null", v, InvalidRequestException.class);
            checkInvalidUDT("v >= {a: 1, b: 'abc', c: 'foo'}", v, InvalidRequestException.class);
            checkInvalidUDT("v >= null", v, InvalidRequestException.class);
            checkInvalidUDT("v IN null", v, SyntaxException.class);
            checkInvalidUDT("v IN 367", v, SyntaxException.class);
            checkInvalidUDT("v CONTAINS KEY 123", v, SyntaxException.class);
            checkInvalidUDT("v CONTAINS 'bar'", v, SyntaxException.class);


            /////////////////// null suffix on stored udt ////////////////////
            v = userType("a", 0, "b", null);
            execute("INSERT INTO %s (k, v) VALUES (0, ?)", v);

            checkAppliesUDT("v = {a: 0}", v);
            checkAppliesUDT("v = {a: 0, b: null}", v);
            checkAppliesUDT("v != null", v);
            checkAppliesUDT("v != {a: 1, b: null}", v);
            checkAppliesUDT("v != {a: 1}", v);
            checkAppliesUDT("v != {a: 0, b: 'def'}", v);
            checkAppliesUDT("v > {a: -1, b: 'abc'}", v);
            checkAppliesUDT("v > {a: -1}", v);
            checkAppliesUDT("v >= {a: 0}", v);
            checkAppliesUDT("v >= {a: -1, b: 'abc'}", v);
            checkAppliesUDT("v < {a: 0, b: 'zzz'}", v);
            checkAppliesUDT("v < {a: 1, b: 'abc'}", v);
            checkAppliesUDT("v < {a: 1}", v);
            checkAppliesUDT("v <= {a: 0, b: 'zzz'}", v);
            checkAppliesUDT("v <= {a: 0}", v);
            checkAppliesUDT("v IN (null, {a: 0, b: 'abc'}, {a: 0})", v);

            // multiple conditions
            checkAppliesUDT("v > {a: -1, b: 'abc'} AND v >= {a: 0}", v);
            checkAppliesUDT("v != null AND v IN ({a: 0}, {a: 0, b: null})", v);

            // should not apply
            checkDoesNotApplyUDT("v = {a: 0, b: 'def'}", v);
            checkDoesNotApplyUDT("v = {a: 1}", v);
            checkDoesNotApplyUDT("v = {b: 'abc'}", v);
            checkDoesNotApplyUDT("v = null", v);
            checkDoesNotApplyUDT("v != {a: 0}", v);
            checkDoesNotApplyUDT("v != {a: 0, b: null}", v);
            checkDoesNotApplyUDT("v > {a: 1, b: 'abc'}", v);
            checkDoesNotApplyUDT("v > {a: 0}", v);
            checkDoesNotApplyUDT("v >= {a: 1, b: 'abc'}", v);
            checkDoesNotApplyUDT("v >= {a: 1}", v);
            checkDoesNotApplyUDT("v < {a: -1, b: 'abc'}", v);
            checkDoesNotApplyUDT("v < {a: -1}", v);
            checkDoesNotApplyUDT("v < {a: 0}", v);
            checkDoesNotApplyUDT("v <= {a: -1, b: 'abc'}", v);
            checkDoesNotApplyUDT("v <= {a: -1}", v);
            checkDoesNotApplyUDT("v IN ({a: 1}, {b: 'abc'}, {a: 0, b: 'def'}, null)", v);
            checkDoesNotApplyUDT("v IN ()", v);

            // multiple conditions
            checkDoesNotApplyUDT("v IN () AND v IN ({a: 0})", v);
            checkDoesNotApplyUDT("v > {a: -1} AND v < {a: 0}", v);


            /////////////////// null prefix on stored udt ////////////////////
            v = userType("a", null, "b", "abc");
            execute("INSERT INTO %s (k, v) VALUES (0, ?)", v);

            checkAppliesUDT("v = {a: null, b: 'abc'}", v);
            checkAppliesUDT("v = {b: 'abc'}", v);
            checkAppliesUDT("v != null", v);
            checkAppliesUDT("v != {a: 0, b: 'abc'}", v);
            checkAppliesUDT("v != {a: 0}", v);
            checkAppliesUDT("v != {b: 'def'}", v);
            checkAppliesUDT("v > {a: null, b: 'aaa'}", v);
            checkAppliesUDT("v > {b: 'aaa'}", v);
            checkAppliesUDT("v >= {a: null, b: 'aaa'}", v);
            checkAppliesUDT("v >= {b: 'abc'}", v);
            checkAppliesUDT("v < {a: null, b: 'zzz'}", v);
            checkAppliesUDT("v < {a: 0, b: 'abc'}", v);
            checkAppliesUDT("v < {a: 0}", v);
            checkAppliesUDT("v < {b: 'zzz'}", v);
            checkAppliesUDT("v <= {a: null, b: 'zzz'}", v);
            checkAppliesUDT("v <= {a: 0}", v);
            checkAppliesUDT("v <= {b: 'abc'}", v);
            checkAppliesUDT("v IN (null, {a: null, b: 'abc'}, {a: 0})", v);
            checkAppliesUDT("v IN (null, {a: 0, b: 'abc'}, {b: 'abc'})", v);

            // multiple conditions
            checkAppliesUDT("v > {b: 'aaa'} AND v >= {b: 'abc'}", v);
            checkAppliesUDT("v != null AND v IN ({a: 0}, {a: null, b: 'abc'})", v);

            // should not apply
            checkDoesNotApplyUDT("v = {a: 0, b: 'def'}", v);
            checkDoesNotApplyUDT("v = {a: 1}", v);
            checkDoesNotApplyUDT("v = {b: 'def'}", v);
            checkDoesNotApplyUDT("v = null", v);
            checkDoesNotApplyUDT("v != {b: 'abc'}", v);
            checkDoesNotApplyUDT("v != {a: null, b: 'abc'}", v);
            checkDoesNotApplyUDT("v > {a: 1, b: 'abc'}", v);
            checkDoesNotApplyUDT("v > {a: null, b: 'zzz'}", v);
            checkDoesNotApplyUDT("v > {b: 'zzz'}", v);
            checkDoesNotApplyUDT("v >= {a: null, b: 'zzz'}", v);
            checkDoesNotApplyUDT("v >= {a: 1}", v);
            checkDoesNotApplyUDT("v >= {b: 'zzz'}", v);
            checkDoesNotApplyUDT("v < {a: null, b: 'aaa'}", v);
            checkDoesNotApplyUDT("v < {b: 'aaa'}", v);
            checkDoesNotApplyUDT("v <= {a: null, b: 'aaa'}", v);
            checkDoesNotApplyUDT("v <= {b: 'aaa'}", v);
            checkDoesNotApplyUDT("v IN ({a: 1}, {a: 1, b: 'abc'}, {a: null, b: 'def'}, null)", v);
            checkDoesNotApplyUDT("v IN ()", v);

            // multiple conditions
            checkDoesNotApplyUDT("v IN () AND v IN ({b: 'abc'})", v);
            checkDoesNotApplyUDT("v IN () AND v IN ({a: null, b: 'abc'})", v);
            checkDoesNotApplyUDT("v > {a: -1} AND v < {a: 0}", v);


            /////////////////// null udt ////////////////////
            v = null;
            execute("INSERT INTO %s (k, v) VALUES (0, ?)", v);

            checkAppliesUDT("v = null", v);
            checkAppliesUDT("v IN (null, {a: null, b: 'abc'}, {a: 0})", v);
            checkAppliesUDT("v IN (null, {a: 0, b: 'abc'}, {b: 'abc'})", v);

            // multiple conditions
            checkAppliesUDT("v = null AND v IN (null, {a: 0}, {a: null, b: 'abc'})", v);

            // should not apply
            checkDoesNotApplyUDT("v = {a: 0, b: 'def'}", v);
            checkDoesNotApplyUDT("v = {a: 1}", v);
            checkDoesNotApplyUDT("v = {b: 'def'}", v);
            checkDoesNotApplyUDT("v != null", v);
            checkDoesNotApplyUDT("v > {a: 1, b: 'abc'}", v);
            checkDoesNotApplyUDT("v > {a: null, b: 'zzz'}", v);
            checkDoesNotApplyUDT("v > {b: 'zzz'}", v);
            checkDoesNotApplyUDT("v >= {a: null, b: 'zzz'}", v);
            checkDoesNotApplyUDT("v >= {a: 1}", v);
            checkDoesNotApplyUDT("v >= {b: 'zzz'}", v);
            checkDoesNotApplyUDT("v < {a: null, b: 'aaa'}", v);
            checkDoesNotApplyUDT("v < {b: 'aaa'}", v);
            checkDoesNotApplyUDT("v <= {a: null, b: 'aaa'}", v);
            checkDoesNotApplyUDT("v <= {b: 'aaa'}", v);
            checkDoesNotApplyUDT("v IN ({a: 1}, {a: 1, b: 'abc'}, {a: null, b: 'def'})", v);
            checkDoesNotApplyUDT("v IN ()", v);

            // multiple conditions
            checkDoesNotApplyUDT("v IN () AND v IN ({b: 'abc'})", v);
            checkDoesNotApplyUDT("v > {a: -1} AND v < {a: 0}", v);

        }
    }

    @Test
    public void testUDTField() throws Throwable
    {
        String typename = createType("CREATE TYPE %s (a int, b text)");
        String myType = KEYSPACE + '.' + typename;

        for (boolean frozen : new boolean[] {false, true})
        {
            createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, v %s)",
                                      frozen
                                      ? "frozen<" + myType + ">"
                                      : myType));

            Object v = userType("a", 0, "b", "abc");
            execute("INSERT INTO %s (k, v) VALUES (0, ?)", v);

            checkAppliesUDT("v.a = 0", v);
            checkAppliesUDT("v.b = 'abc'", v);
            checkAppliesUDT("v.a < 1", v);
            checkAppliesUDT("v.b < 'zzz'", v);
            checkAppliesUDT("v.b <= 'bar'", v);
            checkAppliesUDT("v.b > 'aaa'", v);
            checkAppliesUDT("v.b >= 'abc'", v);
            checkAppliesUDT("v.a != -1", v);
            checkAppliesUDT("v.b != 'xxx'", v);
            checkAppliesUDT("v.a != null", v);
            checkAppliesUDT("v.b != null", v);
            checkAppliesUDT("v.a IN (null, 0, 1)", v);
            checkAppliesUDT("v.b IN (null, 'xxx', 'abc')", v);
            checkAppliesUDT("v.b > 'aaa' AND v.b < 'zzz'", v);
            checkAppliesUDT("v.a = 0 AND v.b > 'aaa'", v);

            // do not apply
            checkDoesNotApplyUDT("v.a = -1", v);
            checkDoesNotApplyUDT("v.b = 'xxx'", v);
            checkDoesNotApplyUDT("v.a < -1", v);
            checkDoesNotApplyUDT("v.b < 'aaa'", v);
            checkDoesNotApplyUDT("v.b <= 'aaa'", v);
            checkDoesNotApplyUDT("v.b > 'zzz'", v);
            checkDoesNotApplyUDT("v.b >= 'zzz'", v);
            checkDoesNotApplyUDT("v.a != 0", v);
            checkDoesNotApplyUDT("v.b != 'abc'", v);
            checkDoesNotApplyUDT("v.a IN (null, -1)", v);
            checkDoesNotApplyUDT("v.b IN (null, 'xxx')", v);
            checkDoesNotApplyUDT("v.a IN ()", v);
            checkDoesNotApplyUDT("v.b IN ()", v);
            checkDoesNotApplyUDT("v.b != null AND v.b IN ()", v);

            // invalid
            checkInvalidUDT("v.c = null", v, InvalidRequestException.class);
            checkInvalidUDT("v.a < null", v, InvalidRequestException.class);
            checkInvalidUDT("v.a <= null", v, InvalidRequestException.class);
            checkInvalidUDT("v.a > null", v, InvalidRequestException.class);
            checkInvalidUDT("v.a >= null", v, InvalidRequestException.class);
            checkInvalidUDT("v.a IN null", v, SyntaxException.class);
            checkInvalidUDT("v.a IN 367", v, SyntaxException.class);
            checkInvalidUDT("v.b IN (1, 2, 3)", v, InvalidRequestException.class);
            checkInvalidUDT("v.a CONTAINS 367", v, SyntaxException.class);
            checkInvalidUDT("v.a CONTAINS KEY 367", v, SyntaxException.class);


            /////////////// null suffix on udt ////////////////
            v = userType("a", 0, "b", null);
            execute("INSERT INTO %s (k, v) VALUES (0, ?)", v);

            checkAppliesUDT("v.a = 0", v);
            checkAppliesUDT("v.b = null", v);
            checkAppliesUDT("v.b != 'xxx'", v);
            checkAppliesUDT("v.a != null", v);
            checkAppliesUDT("v.a IN (null, 0, 1)", v);
            checkAppliesUDT("v.b IN (null, 'xxx', 'abc')", v);
            checkAppliesUDT("v.a = 0 AND v.b = null", v);

            // do not apply
            checkDoesNotApplyUDT("v.b = 'abc'", v);
            checkDoesNotApplyUDT("v.a < -1", v);
            checkDoesNotApplyUDT("v.b < 'aaa'", v);
            checkDoesNotApplyUDT("v.b <= 'aaa'", v);
            checkDoesNotApplyUDT("v.b > 'zzz'", v);
            checkDoesNotApplyUDT("v.b >= 'zzz'", v);
            checkDoesNotApplyUDT("v.a != 0", v);
            checkDoesNotApplyUDT("v.b != null", v);
            checkDoesNotApplyUDT("v.a IN (null, -1)", v);
            checkDoesNotApplyUDT("v.b IN ('xxx', 'abc')", v);
            checkDoesNotApplyUDT("v.a IN ()", v);
            checkDoesNotApplyUDT("v.b IN ()", v);
            checkDoesNotApplyUDT("v.b != null AND v.b IN ()", v);


            /////////////// null prefix on udt ////////////////
            v = userType("a", null, "b", "abc");
            execute("INSERT INTO %s (k, v) VALUES (0, ?)", v);

            checkAppliesUDT("v.a = null", v);
            checkAppliesUDT("v.b = 'abc'", v);
            checkAppliesUDT("v.a != 0", v);
            checkAppliesUDT("v.b != null", v);
            checkAppliesUDT("v.a IN (null, 0, 1)", v);
            checkAppliesUDT("v.b IN (null, 'xxx', 'abc')", v);
            checkAppliesUDT("v.a = null AND v.b = 'abc'", v);

            // do not apply
            checkDoesNotApplyUDT("v.a = 0", v);
            checkDoesNotApplyUDT("v.a < -1", v);
            checkDoesNotApplyUDT("v.b >= 'zzz'", v);
            checkDoesNotApplyUDT("v.a != null", v);
            checkDoesNotApplyUDT("v.b != 'abc'", v);
            checkDoesNotApplyUDT("v.a IN (-1, 0)", v);
            checkDoesNotApplyUDT("v.b IN (null, 'xxx')", v);
            checkDoesNotApplyUDT("v.a IN ()", v);
            checkDoesNotApplyUDT("v.b IN ()", v);
            checkDoesNotApplyUDT("v.b != null AND v.b IN ()", v);


            /////////////// null udt ////////////////
            v = null;
            execute("INSERT INTO %s (k, v) VALUES (0, ?)", v);

            checkAppliesUDT("v.a = null", v);
            checkAppliesUDT("v.b = null", v);
            checkAppliesUDT("v.a != 0", v);
            checkAppliesUDT("v.b != 'abc'", v);
            checkAppliesUDT("v.a IN (null, 0, 1)", v);
            checkAppliesUDT("v.b IN (null, 'xxx', 'abc')", v);
            checkAppliesUDT("v.a = null AND v.b = null", v);

            // do not apply
            checkDoesNotApplyUDT("v.a = 0", v);
            checkDoesNotApplyUDT("v.a < -1", v);
            checkDoesNotApplyUDT("v.b >= 'zzz'", v);
            checkDoesNotApplyUDT("v.a != null", v);
            checkDoesNotApplyUDT("v.b != null", v);
            checkDoesNotApplyUDT("v.a IN (-1, 0)", v);
            checkDoesNotApplyUDT("v.b IN ('xxx', 'abc')", v);
            checkDoesNotApplyUDT("v.a IN ()", v);
            checkDoesNotApplyUDT("v.b IN ()", v);
            checkDoesNotApplyUDT("v.b != null AND v.b IN ()", v);
        }
    }

    void checkAppliesUDT(String condition, Object value) throws Throwable
    {
        assertRows(execute("UPDATE %s SET v = ? WHERE k = 0 IF " + condition, value), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, value));
    }

    void checkDoesNotApplyUDT(String condition, Object value) throws Throwable
    {
        assertRows(execute("UPDATE %s SET v = ? WHERE k = 0 IF " + condition, value),
                   row(false, value));
        assertRows(execute("SELECT * FROM %s"), row(0, value));
    }

    void checkInvalidUDT(String condition, Object value, Class<? extends Throwable> expected) throws Throwable
    {
        assertInvalidThrow(expected, "UPDATE %s SET v = ?  WHERE k = 0 IF " + condition, value);
        assertRows(execute("SELECT * FROM %s"), row(0, value));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.whole_list_conditional_test()
     */
    @Test
    public void testWholeList() throws Throwable
    {
        for (boolean frozen : new boolean[] {false, true})
        {
            createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, l %s)",
                                      frozen
                                      ? "frozen<list<text>>"
                                      : "list<text>"));

            execute("INSERT INTO %s(k, l) VALUES (0, ['foo', 'bar', 'foobar'])");

            check_applies_list("l = ['foo', 'bar', 'foobar']");
            check_applies_list("l != ['baz']");
            check_applies_list("l > ['a']");
            check_applies_list("l >= ['a']");
            check_applies_list("l < ['z']");
            check_applies_list("l <= ['z']");
            check_applies_list("l IN (null, ['foo', 'bar', 'foobar'], ['a'])");

            // multiple conditions
            check_applies_list("l > ['aaa', 'bbb'] AND l > ['aaa']");
            check_applies_list("l != null AND l IN (['foo', 'bar', 'foobar'])");

            // should not apply
            check_does_not_apply_list("l = ['baz']");
            check_does_not_apply_list("l != ['foo', 'bar', 'foobar']");
            check_does_not_apply_list("l > ['z']");
            check_does_not_apply_list("l >= ['z']");
            check_does_not_apply_list("l < ['a']");
            check_does_not_apply_list("l <= ['a']");
            check_does_not_apply_list("l IN (['a'], null)");
            check_does_not_apply_list("l IN ()");

            // multiple conditions
            check_does_not_apply_list("l IN () AND l IN (['foo', 'bar', 'foobar'])");
            check_does_not_apply_list("l > ['zzz'] AND l < ['zzz']");

            check_invalid_list("l = [null]", InvalidRequestException.class);
            check_invalid_list("l < null", InvalidRequestException.class);
            check_invalid_list("l <= null", InvalidRequestException.class);
            check_invalid_list("l > null", InvalidRequestException.class);
            check_invalid_list("l >= null", InvalidRequestException.class);
            check_invalid_list("l IN null", SyntaxException.class);
            check_invalid_list("l IN 367", SyntaxException.class);
            check_invalid_list("l CONTAINS KEY 123", SyntaxException.class);

            // not supported yet
            check_invalid_list("m CONTAINS 'bar'", SyntaxException.class);
        }
    }

    void check_applies_list(String condition) throws Throwable
    {
        assertRows(execute("UPDATE %s SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF " + condition), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, list("foo", "bar", "foobar")));
    }

    void check_does_not_apply_list(String condition) throws Throwable
    {
        assertRows(execute("UPDATE %s SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF " + condition),
                   row(false, list("foo", "bar", "foobar")));
        assertRows(execute("SELECT * FROM %s"), row(0, list("foo", "bar", "foobar")));
    }

    void check_invalid_list(String condition, Class<? extends Throwable> expected) throws Throwable
    {
        assertInvalidThrow(expected, "UPDATE %s SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF " + condition);
        assertRows(execute("SELECT * FROM %s"), row(0, list("foo", "bar", "foobar")));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.list_item_conditional_test()
     */
    @Test
    public void testListItem() throws Throwable
    {
        for (boolean frozen : new boolean[]{ false, true })
        {
            createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, l %s)",
                                      frozen
                                      ? "frozen<list<text>>"
                                      : "list<text>"));

            execute("INSERT INTO %s(k, l) VALUES (0, ['foo', 'bar', 'foobar'])");

            assertInvalidMessage("Invalid null value for list element access",
                                 "DELETE FROM %s WHERE k=0 IF l[?] = ?", null, "foobar");
            assertInvalidMessage("Invalid negative list index -2",
                                 "DELETE FROM %s WHERE k=0 IF l[?] = ?", -2, "foobar");

            assertRows(execute("DELETE FROM %s WHERE k=0 IF l[?] = ?", 1, null), row(false, list("foo", "bar", "foobar")));
            assertRows(execute("DELETE FROM %s WHERE k=0 IF l[?] = ?", 1, "foobar"), row(false, list("foo", "bar", "foobar")));
            assertRows(execute("SELECT * FROM %s"), row(0, list("foo", "bar", "foobar")));

            assertRows(execute("DELETE FROM %s WHERE k=0 IF l[?] = ?", 1, "bar"), row(true));
            assertEmpty(execute("SELECT * FROM %s"));
        }
    }

    /**
     * Test expanded functionality from CASSANDRA-6839, 
     * migrated from cql_tests.py:TestCQL.expanded_list_item_conditional_test()
     */
    @Test
    public void testExpandedListItem() throws Throwable
    {
        for (boolean frozen : new boolean[] {false, true})
        {
            createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, l %s)",
                                      frozen
                                      ? "frozen<list<text>>"
                                      : "list<text>"));

            execute("INSERT INTO %s (k, l) VALUES (0, ['foo', 'bar', 'foobar'])");

            check_applies_list("l[1] < 'zzz'");
            check_applies_list("l[1] <= 'bar'");
            check_applies_list("l[1] > 'aaa'");
            check_applies_list("l[1] >= 'bar'");
            check_applies_list("l[1] != 'xxx'");
            check_applies_list("l[1] != null");
            check_applies_list("l[1] IN (null, 'xxx', 'bar')");
            check_applies_list("l[1] > 'aaa' AND l[1] < 'zzz'");

            // check beyond end of list
            check_applies_list("l[3] = null");
            check_applies_list("l[3] IN (null, 'xxx', 'bar')");

            check_does_not_apply_list("l[1] < 'aaa'");
            check_does_not_apply_list("l[1] <= 'aaa'");
            check_does_not_apply_list("l[1] > 'zzz'");
            check_does_not_apply_list("l[1] >= 'zzz'");
            check_does_not_apply_list("l[1] != 'bar'");
            check_does_not_apply_list("l[1] IN (null, 'xxx')");
            check_does_not_apply_list("l[1] IN ()");
            check_does_not_apply_list("l[1] != null AND l[1] IN ()");

            // check beyond end of list
            check_does_not_apply_list("l[3] != null");
            check_does_not_apply_list("l[3] = 'xxx'");

            check_invalid_list("l[1] < null", InvalidRequestException.class);
            check_invalid_list("l[1] <= null", InvalidRequestException.class);
            check_invalid_list("l[1] > null", InvalidRequestException.class);
            check_invalid_list("l[1] >= null", InvalidRequestException.class);
            check_invalid_list("l[1] IN null", SyntaxException.class);
            check_invalid_list("l[1] IN 367", SyntaxException.class);
            check_invalid_list("l[1] IN (1, 2, 3)", InvalidRequestException.class);
            check_invalid_list("l[1] CONTAINS 367", SyntaxException.class);
            check_invalid_list("l[1] CONTAINS KEY 367", SyntaxException.class);
            check_invalid_list("l[null] = null", InvalidRequestException.class);
        }
    }

    /**
     * Migrated from cql_tests.py:TestCQL.whole_set_conditional_test()
     */
    @Test
    public void testWholeSet() throws Throwable
    {
        for (boolean frozen : new boolean[] {false, true})
        {
            createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, s %s)",
                                      frozen
                                      ? "frozen<set<text>>"
                                      : "set<text>"));

            execute("INSERT INTO %s (k, s) VALUES (0, {'bar', 'foo'})");

            check_applies_set("s = {'bar', 'foo'}");
            check_applies_set("s = {'foo', 'bar'}");
            check_applies_set("s != {'baz'}");
            check_applies_set("s > {'a'}");
            check_applies_set("s >= {'a'}");
            check_applies_set("s < {'z'}");
            check_applies_set("s <= {'z'}");
            check_applies_set("s IN (null, {'bar', 'foo'}, {'a'})");

            // multiple conditions
            check_applies_set("s > {'a'} AND s < {'z'}");
            check_applies_set("s IN (null, {'bar', 'foo'}, {'a'}) AND s IN ({'a'}, {'bar', 'foo'}, null)");

            // should not apply
            check_does_not_apply_set("s = {'baz'}");
            check_does_not_apply_set("s != {'bar', 'foo'}");
            check_does_not_apply_set("s > {'z'}");
            check_does_not_apply_set("s >= {'z'}");
            check_does_not_apply_set("s < {'a'}");
            check_does_not_apply_set("s <= {'a'}");
            check_does_not_apply_set("s IN ({'a'}, null)");
            check_does_not_apply_set("s IN ()");
            check_does_not_apply_set("s != null AND s IN ()");

            check_invalid_set("s = {null}", InvalidRequestException.class);
            check_invalid_set("s < null", InvalidRequestException.class);
            check_invalid_set("s <= null", InvalidRequestException.class);
            check_invalid_set("s > null", InvalidRequestException.class);
            check_invalid_set("s >= null", InvalidRequestException.class);
            check_invalid_set("s IN null", SyntaxException.class);
            check_invalid_set("s IN 367", SyntaxException.class);
            check_invalid_set("s CONTAINS KEY 123", SyntaxException.class);

            // element access is not allow for sets
            check_invalid_set("s['foo'] = 'foobar'", InvalidRequestException.class);

            // not supported yet
            check_invalid_set("m CONTAINS 'bar'", SyntaxException.class);
        }
    }

    void check_applies_set(String condition) throws Throwable
    {
        assertRows(execute("UPDATE %s SET s = {'bar', 'foo'} WHERE k=0 IF " + condition), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, set("bar", "foo")));
    }

    void check_does_not_apply_set(String condition) throws Throwable
    {
        assertRows(execute("UPDATE %s SET s = {'bar', 'foo'} WHERE k=0 IF " + condition), row(false, set("bar", "foo")));
        assertRows(execute("SELECT * FROM %s"), row(0, set("bar", "foo")));
    }

    void check_invalid_set(String condition, Class<? extends Throwable> expected) throws Throwable
    {
        assertInvalidThrow(expected, "UPDATE %s SET s = {'bar', 'foo'} WHERE k=0 IF " + condition);
        assertRows(execute("SELECT * FROM %s"), row(0, set("bar", "foo")));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.whole_map_conditional_test()
     */
    @Test
    public void testWholeMap() throws Throwable
    {
        for (boolean frozen : new boolean[] {false, true})
        {
            createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, m %s)",
                                      frozen
                                      ? "frozen<map<text, text>>"
                                      : "map<text, text>"));

            execute("INSERT INTO %s (k, m) VALUES (0, {'foo' : 'bar'})");

            check_applies_map("m = {'foo': 'bar'}");
            check_applies_map("m > {'a': 'a'}");
            check_applies_map("m >= {'a': 'a'}");
            check_applies_map("m < {'z': 'z'}");
            check_applies_map("m <= {'z': 'z'}");
            check_applies_map("m != {'a': 'a'}");
            check_applies_map("m IN (null, {'a': 'a'}, {'foo': 'bar'})");

            // multiple conditions
            check_applies_map("m > {'a': 'a'} AND m < {'z': 'z'}");
            check_applies_map("m != null AND m IN (null, {'a': 'a'}, {'foo': 'bar'})");

            // should not apply
            check_does_not_apply_map("m = {'a': 'a'}");
            check_does_not_apply_map("m > {'z': 'z'}");
            check_does_not_apply_map("m >= {'z': 'z'}");
            check_does_not_apply_map("m < {'a': 'a'}");
            check_does_not_apply_map("m <= {'a': 'a'}");
            check_does_not_apply_map("m != {'foo': 'bar'}");
            check_does_not_apply_map("m IN ({'a': 'a'}, null)");
            check_does_not_apply_map("m IN ()");
            check_does_not_apply_map("m = null AND m != null");

            check_invalid_map("m = {null: null}", InvalidRequestException.class);
            check_invalid_map("m = {'a': null}", InvalidRequestException.class);
            check_invalid_map("m = {null: 'a'}", InvalidRequestException.class);
            check_invalid_map("m < null", InvalidRequestException.class);
            check_invalid_map("m IN null", SyntaxException.class);

            // not supported yet
            check_invalid_map("m CONTAINS 'bar'", SyntaxException.class);
            check_invalid_map("m CONTAINS KEY 'foo'", SyntaxException.class);
            check_invalid_map("m CONTAINS null", SyntaxException.class);
            check_invalid_map("m CONTAINS KEY null", SyntaxException.class);
        }
    }

    /**
     * Migrated from cql_tests.py:TestCQL.map_item_conditional_test()
     */
    @Test
    public void testMapItem() throws Throwable
    {
        for (boolean frozen : new boolean[]{ false, true })
        {
            createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, m %s)",
                                      frozen
                                      ? "frozen<map<text, text>>"
                                      : "map<text, text>"));

            execute("INSERT INTO %s (k, m) VALUES (0, {'foo' : 'bar'})");
            assertInvalidMessage("Invalid null value for map element access",
                                 "DELETE FROM %s WHERE k=0 IF m[?] = ?", null, "foo");
            assertRows(execute("DELETE FROM %s WHERE k=0 IF m[?] = ?", "foo", "foo"), row(false, map("foo", "bar")));
            assertRows(execute("DELETE FROM %s WHERE k=0 IF m[?] = ?", "foo", null), row(false, map("foo", "bar")));
            assertRows(execute("SELECT * FROM %s"), row(0, map("foo", "bar")));

            assertRows(execute("DELETE FROM %s WHERE k=0 IF m[?] = ?", "foo", "bar"), row(true));
            assertEmpty(execute("SELECT * FROM %s"));

            execute("INSERT INTO %s(k, m) VALUES (1, null)");
            if (frozen)
                assertInvalidMessage("Invalid operation (m['foo'] = 'bar') for frozen collection column m",
                                     "UPDATE %s set m['foo'] = 'bar', m['bar'] = 'foo' WHERE k = 1 IF m[?] IN (?, ?)", "foo", "blah", null);
            else
                assertRows(execute("UPDATE %s set m['foo'] = 'bar', m['bar'] = 'foo' WHERE k = 1 IF m[?] IN (?, ?)", "foo", "blah", null), row(true));
        }
    }

    @Test
    public void testFrozenWithNullValues() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, m %s)", "frozen<list<text>>"));
        execute("INSERT INTO %s (k, m) VALUES (0, null)");

        assertRows(execute("UPDATE %s SET m = ? WHERE k = 0 IF m = ?", list("test"), list("comparison")), row(false, null));

        createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, m %s)", "frozen<map<text,int>>"));
        execute("INSERT INTO %s (k, m) VALUES (0, null)");

        assertRows(execute("UPDATE %s SET m = ? WHERE k = 0 IF m = ?", map("test", 3), map("comparison", 2)), row(false, null));

        createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, m %s)", "frozen<set<text>>"));
        execute("INSERT INTO %s (k, m) VALUES (0, null)");

        assertRows(execute("UPDATE %s SET m = ? WHERE k = 0 IF m = ?", set("test"), set("comparison")), row(false, null));
    }
    /**
     * Test expanded functionality from CASSANDRA-6839,
     * migrated from cql_tests.py:TestCQL.expanded_map_item_conditional_test()
     */
    @Test
    public void testExpandedMapItem() throws Throwable
    {
        for (boolean frozen : new boolean[]{ false, true })
        {
            createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, m %s)",
                                      frozen
                                      ? "frozen<map<text, text>>"
                                      : "map<text, text>"));

            execute("INSERT INTO %s (k, m) VALUES (0, {'foo' : 'bar'})");

            check_applies_map("m['xxx'] = null");
            check_applies_map("m['foo'] < 'zzz'");
            check_applies_map("m['foo'] <= 'bar'");
            check_applies_map("m['foo'] > 'aaa'");
            check_applies_map("m['foo'] >= 'bar'");
            check_applies_map("m['foo'] != 'xxx'");
            check_applies_map("m['foo'] != null");
            check_applies_map("m['foo'] IN (null, 'xxx', 'bar')");
            check_applies_map("m['xxx'] IN (null, 'xxx', 'bar')"); // m['xxx'] is not set

            // multiple conditions
            check_applies_map("m['foo'] < 'zzz' AND m['foo'] > 'aaa'");

            check_does_not_apply_map("m['foo'] < 'aaa'");
            check_does_not_apply_map("m['foo'] <= 'aaa'");
            check_does_not_apply_map("m['foo'] > 'zzz'");
            check_does_not_apply_map("m['foo'] >= 'zzz'");
            check_does_not_apply_map("m['foo'] != 'bar'");
            check_does_not_apply_map("m['xxx'] != null");  // m['xxx'] is not set
            check_does_not_apply_map("m['foo'] IN (null, 'xxx')");
            check_does_not_apply_map("m['foo'] IN ()");
            check_does_not_apply_map("m['foo'] != null AND m['foo'] = null");

            check_invalid_map("m['foo'] < null", InvalidRequestException.class);
            check_invalid_map("m['foo'] <= null", InvalidRequestException.class);
            check_invalid_map("m['foo'] > null", InvalidRequestException.class);
            check_invalid_map("m['foo'] >= null", InvalidRequestException.class);
            check_invalid_map("m['foo'] IN null", SyntaxException.class);
            check_invalid_map("m['foo'] IN 367", SyntaxException.class);
            check_invalid_map("m['foo'] IN (1, 2, 3)", InvalidRequestException.class);
            check_invalid_map("m['foo'] CONTAINS 367", SyntaxException.class);
            check_invalid_map("m['foo'] CONTAINS KEY 367", SyntaxException.class);
            check_invalid_map("m[null] = null", InvalidRequestException.class);
        }
    }

    void check_applies_map(String condition) throws Throwable
    {
        assertRows(execute("UPDATE %s SET m = {'foo': 'bar'} WHERE k=0 IF " + condition), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, map("foo", "bar")));
    }

    void check_does_not_apply_map(String condition) throws Throwable
    {
        assertRows(execute("UPDATE %s SET m = {'foo': 'bar'} WHERE k=0 IF " + condition), row(false, map("foo", "bar")));
        assertRows(execute("SELECT * FROM %s"), row(0, map("foo", "bar")));
    }

    void check_invalid_map(String condition, Class<? extends Throwable> expected) throws Throwable
    {
        assertInvalidThrow(expected, "UPDATE %s SET m = {'foo': 'bar'} WHERE k=0 IF " + condition);
        assertRows(execute("SELECT * FROM %s"), row(0, map("foo", "bar")));
    }

    /**
     * Test for 7499,
     * migrated from cql_tests.py:TestCQL.cas_and_list_index_test()
     */
    @Test
    public void testCasAndListIndex() throws Throwable
    {
        createTable("CREATE TABLE %s ( k int PRIMARY KEY, v text, l list<text>)");

        execute("INSERT INTO %s (k, v, l) VALUES(0, 'foobar', ['foi', 'bar'])");

        assertRows(execute("UPDATE %s SET l[0] = 'foo' WHERE k = 0 IF v = 'barfoo'"), row(false, "foobar"));
        assertRows(execute("UPDATE %s SET l[0] = 'foo' WHERE k = 0 IF v = 'foobar'"), row(true));

        assertRows(execute("SELECT * FROM %s"), row(0, list("foo", "bar"), "foobar"));

    }

    /**
     * Migrated from cql_tests.py:TestCQL.conditional_ddl_keyspace_test()
     */
    @Test
    public void testDropCreateKeyspaceIfNotExists() throws Throwable
    {
        String keyspace =  KEYSPACE_PER_TEST;

        dropPerTestKeyspace();

        // try dropping when doesn't exist
        dropPerTestKeyspace();

        // create and confirm
        schemaChange("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} and durable_writes = true ");
        assertRows(execute(format("select durable_writes from %s.%s where keyspace_name = ?",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.KEYSPACES),
                           keyspace),
                   row(true));

        // unsuccessful create since it's already there, confirm settings don't change
        schemaChange("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1} and durable_writes = false ");

        assertRows(execute(format("select durable_writes from %s.%s where keyspace_name = ?",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.KEYSPACES),
                           keyspace),
                   row(true));

        // drop and confirm
        schemaChange("DROP KEYSPACE IF EXISTS " + keyspace);

        assertEmpty(execute(format("select * from %s.%s where keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.KEYSPACES),
                            keyspace));
    }


    /**
     * Migrated from cql_tests.py:TestCQL.conditional_ddl_table_test()
     */
    @Test
    public void testDropCreateTableIfNotExists() throws Throwable
    {
        String tableName = createTableName();
        String fullTableName = KEYSPACE + "." + tableName;

        // try dropping when doesn't exist
        schemaChange("DROP TABLE IF EXISTS " + fullTableName);

        // create and confirm
        schemaChange("CREATE TABLE IF NOT EXISTS " + fullTableName + " (id text PRIMARY KEY, value1 blob) with comment = 'foo'");

        assertRows(execute("select comment from system_schema.tables where keyspace_name = ? and table_name = ?", KEYSPACE, tableName),
                   row("foo"));

        // unsuccessful create since it's already there, confirm settings don't change
        schemaChange("CREATE TABLE IF NOT EXISTS " + fullTableName + " (id text PRIMARY KEY, value2 blob)with comment = 'bar'");

        assertRows(execute("select comment from system_schema.tables where keyspace_name = ? and table_name = ?", KEYSPACE, tableName),
                   row("foo"));

        // drop and confirm
        schemaChange("DROP TABLE IF EXISTS " + fullTableName);

        assertEmpty(execute("select * from system_schema.tables where keyspace_name = ? and table_name = ?", KEYSPACE, tableName));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.conditional_ddl_index_test()
     */
    @Test
    public void testDropCreateIndexIfNotExists() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (id text PRIMARY KEY, value1 blob, value2 blob)with comment = 'foo'");

        execute("use " + KEYSPACE);

        // try dropping when doesn't exist
        schemaChange("DROP INDEX IF EXISTS myindex");

        // create and confirm
        createIndex("CREATE INDEX IF NOT EXISTS myindex ON %s (value1)");

        assertTrue(waitForIndex(KEYSPACE, tableName, "myindex"));

        // unsuccessful create since it's already there
        execute("CREATE INDEX IF NOT EXISTS myindex ON %s (value1)");

        // drop and confirm
        execute("DROP INDEX IF EXISTS myindex");

        Object[][] rows = getRows(execute("select index_name from system.\"IndexInfo\" where table_name = ?", tableName));
        assertEquals(0, rows.length);
    }

    /**
     * Migrated from cql_tests.py:TestCQL.conditional_ddl_type_test()
     */
    @Test
    public void testDropCreateTypeIfNotExists() throws Throwable
    {
        execute("use " + KEYSPACE);

        // try dropping when doesn 't exist
        execute("DROP TYPE IF EXISTS mytype");

        // create and confirm
        execute("CREATE TYPE IF NOT EXISTS mytype (somefield int)");
        assertRows(execute(format("SELECT type_name from %s.%s where keyspace_name = ? and type_name = ?",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspace.TYPES),
                           KEYSPACE,
                           "mytype"),
                   row("mytype"));

        // unsuccessful create since it 's already there
        // TODO: confirm this create attempt doesn't alter type field from int to blob
        execute("CREATE TYPE IF NOT EXISTS mytype (somefield blob)");

        // drop and confirm
        execute("DROP TYPE IF EXISTS mytype");
        assertEmpty(execute(format("SELECT type_name from %s.%s where keyspace_name = ? and type_name = ?",
                                   SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                   SchemaKeyspace.TYPES),
                            KEYSPACE,
                            "mytype"));
    }

    @Test
    public void testConditionalUpdatesOnStaticColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, s int static, d text, PRIMARY KEY (a, b))");

        assertInvalidMessage("Invalid 'unset' value in condition", "UPDATE %s SET s = 6 WHERE a = 6 IF s = ?", unset());

        // pre-existing row
        execute("INSERT INTO %s (a, b, s, d) values (6, 6, 100, 'a')");
        assertRows(execute("UPDATE %s SET s = 6 WHERE a = 6 IF s = 100"),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 6"),
                   row(6, 6, 6, "a"));

        execute("INSERT INTO %s (a, b, s, d) values (7, 7, 100, 'a')");
        assertRows(execute("UPDATE %s SET s = 7 WHERE a = 7 IF s = 101"),
                   row(false, 100));
        assertRows(execute("SELECT * FROM %s WHERE a = 7"),
                   row(7, 7, 100, "a"));

        // pre-existing row with null in the static column
        execute("INSERT INTO %s (a, b, d) values (7, 7, 'a')");
        assertRows(execute("UPDATE %s SET s = 7 WHERE a = 7 IF s = NULL"),
                   row(false, 100));
        assertRows(execute("SELECT * FROM %s WHERE a = 7"),
                   row(7, 7, 100, "a"));

        // deleting row before CAS makes it effectively non-existing
        execute("DELETE FROM %s WHERE a = 8;");
        assertRows(execute("UPDATE %s SET s = 8 WHERE a = 8 IF s = NULL"),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 8"),
                   row(8, null, 8, null));
    }

    @Test
    public void testConditionalUpdatesWithNonExistingValues() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, s int static, d text, PRIMARY KEY (a, b))");

        assertRows(execute("UPDATE %s SET s = 1 WHERE a = 1 IF s = NULL"),
                   row(true));
        assertRows(execute("SELECT a, s, d FROM %s WHERE a = 1"),
                   row(1, 1, null));

        assertRows(execute("UPDATE %s SET s = 2 WHERE a = 2 IF s IN (10,20,NULL)"),
                   row(true));
        assertRows(execute("SELECT a, s, d FROM %s WHERE a = 2"),
                   row(2, 2, null));

        assertRows(execute("UPDATE %s SET s = 4 WHERE a = 4 IF s != 4"),
                   row(true));
        assertRows(execute("SELECT a, s, d FROM %s WHERE a = 4"),
                   row(4, 4, null));

        // rejected: IN doesn't contain null
        assertRows(execute("UPDATE %s SET s = 3 WHERE a = 3 IF s IN ?", list(10,20,30)),
                   row(false));
        assertEmpty(execute("SELECT a, s, d FROM %s WHERE a = 3"));

        // rejected: comparing number with NULL always returns false
        for (String operator : new String[]{ ">", "<", ">=", "<=", "=" })
        {
            assertRows(execute("UPDATE %s SET s = 50 WHERE a = 5 IF s " + operator + " ?", 3),
                       row(false));
            assertEmpty(execute("SELECT * FROM %s WHERE a = 5"));
        }
    }

    @Test
    public void testConditionalUpdatesWithNullValues() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, s int static, d int, PRIMARY KEY (a, b))");

        // pre-populate, leave out static column
        for (int i = 1; i <= 5; i++)
        {
            execute("INSERT INTO %s (a, b) VALUES (?, ?)", i, 1);
            execute("INSERT INTO %s (a, b) VALUES (?, ?)", i, 2);
        }

        assertRows(execute("UPDATE %s SET s = 100 WHERE a = 1 IF s = NULL"),
                   row(true));
        assertRows(execute("SELECT a, b, s, d FROM %s WHERE a = 1"),
                   row(1, 1, 100, null),
                   row(1, 2, 100, null));

        assertRows(execute("UPDATE %s SET s = 200 WHERE a = 2 IF s IN (10,20,NULL)"),
                   row(true));
        assertRows(execute("SELECT a, b, s, d FROM %s WHERE a = 2"),
                   row(2, 1, 200, null),
                   row(2, 2, 200, null));

        // rejected: IN doesn't contain null
        assertRows(execute("UPDATE %s SET s = 30 WHERE a = 3 IF s IN ?", list(10,20,30)),
                   row(false, null));
        assertRows(execute("SELECT * FROM %s WHERE a = 3"),
                   row(3, 1, null, null),
                   row(3, 2, null, null));

        assertRows(execute("UPDATE %s SET s = 400 WHERE a = 4 IF s IN (10,20,NULL)"),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 4"),
                   row(4, 1, 400, null),
                   row(4, 2, 400, null));

        // rejected: comparing number with NULL always returns false
        for (String operator: new String[] { ">", "<", ">=", "<=", "="})
        {
            assertRows(execute("UPDATE %s SET s = 50 WHERE a = 5 IF s " + operator + " 3"),
                       row(false, null));
            assertRows(execute("SELECT * FROM %s WHERE a = 5"),
                       row(5, 1, null, null),
                       row(5, 2, null, null));
        }

        assertRows(execute("UPDATE %s SET s = 500 WHERE a = 5 IF s != 5"),
                   row(true));
        assertRows(execute("SELECT a, b, s, d FROM %s WHERE a = 5"),
                   row(5, 1, 500, null),
                   row(5, 2, 500, null));

        // Similar test, although with two static columns to test limits
        createTable("CREATE TABLE %s (a int, b int, s1 int static, s2 int static, d int, PRIMARY KEY (a, b))");

        for (int i = 1; i <= 5; i++)
            for (int j = 0; j < 5; j++)
                execute("INSERT INTO %s (a, b, d) VALUES (?, ?, ?)", i, j, i + j);

        assertRows(execute("UPDATE %s SET s2 = 100 WHERE a = 1 IF s1 = NULL"),
                   row(true));

        execute("INSERT INTO %s (a, b, s1) VALUES (?, ?, ?)", 2, 2, 2);
        assertRows(execute("UPDATE %s SET s1 = 100 WHERE a = 2 IF s2 = NULL"),
                   row(true));

        execute("INSERT INTO %s (a, b, s1) VALUES (?, ?, ?)", 2, 2, 2);
        assertRows(execute("UPDATE %s SET s1 = 100 WHERE a = 2 IF s2 = NULL"),
                   row(true));
    }

    @Test
    public void testStaticsWithMultipleConditions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, s1 int static, s2 int static, d int, PRIMARY KEY (a, b))");

        for (int i = 1; i <= 5; i++)
        {
            execute("INSERT INTO %s (a, b, d) VALUES (?, ?, ?)", i, 1, 5);
            execute("INSERT INTO %s (a, b, d) VALUES (?, ?, ?)", i, 2, 6);
        }

        assertRows(execute("BEGIN BATCH\n"
                           + "UPDATE %1$s SET s2 = 102 WHERE a = 1 IF s1 = null;\n"
                           + "UPDATE %1$s SET s1 = 101 WHERE a = 1 IF s2 = null;\n"
                           + "APPLY BATCH"),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 1"),
                   row(1, 1, 101, 102, 5),
                   row(1, 2, 101, 102, 6));


        assertRows(execute("BEGIN BATCH\n"
                           + "UPDATE %1$s SET s2 = 202 WHERE a = 2 IF s1 = null;\n"
                           + "UPDATE %1$s SET s1 = 201 WHERE a = 2 IF s2 = null;\n"
                           + "UPDATE %1$s SET d = 203 WHERE a = 2 AND b = 1 IF d = 5;\n"
                           + "UPDATE %1$s SET d = 204 WHERE a = 2 AND b = 2 IF d = 6;\n"
                           + "APPLY BATCH"),
                   row(true));

        assertRows(execute("SELECT * FROM %s WHERE a = 2"),
                   row(2, 1, 201, 202, 203),
                   row(2, 2, 201, 202, 204));

        assertRows(execute("BEGIN BATCH\n"
                           + "UPDATE %1$s SET s2 = 202 WHERE a = 20 IF s1 = null;\n"
                           + "UPDATE %1$s SET s1 = 201 WHERE a = 20 IF s2 = null;\n"
                           + "UPDATE %1$s SET d = 203 WHERE a = 20 AND b = 1 IF d = 5;\n"
                           + "UPDATE %1$s SET d = 204 WHERE a = 20 AND b = 2 IF d = 6;\n"
                           + "APPLY BATCH"),
                   row(false));
    }

    @Test
    public void testConditionalUpdatesWithNullValuesWithBatch() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, s int static, d text, PRIMARY KEY (a, b))");

        // pre-populate, leave out static column
        for (int i = 1; i <= 6; i++)
            execute("INSERT INTO %s (a, b) VALUES (?, ?)", i, i);

        // applied: null is indistiguishable from empty value, lwt condition is executed before INSERT
        assertRows(execute("BEGIN BATCH\n"
                           + "INSERT INTO %1$s (a, b, d) values (2, 2, 'a');\n"
                           + "UPDATE %1$s SET s = 2 WHERE a = 2 IF s = null;\n"
                           + "APPLY BATCH"),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 2"),
                   row(2, 2, 2, "a"));

        // rejected: comparing number with null value always returns false
        for (String operator: new String[] { ">", "<", ">=", "<=", "="})
        {
            assertRows(execute("BEGIN BATCH\n"
                               + "INSERT INTO %1$s (a, b, s, d) values (3, 3, 40, 'a');\n"
                               + "UPDATE %1$s SET s = 30 WHERE a = 3 IF s " + operator + " 5;\n"
                               + "APPLY BATCH"),
                       row(false, 3, 3, null));
            assertRows(execute("SELECT * FROM %s WHERE a = 3"),
                       row(3, 3, null, null));
        }

        // applied: lwt condition is executed before INSERT, update is applied after it
        assertRows(execute("BEGIN BATCH\n"
                           + "INSERT INTO %1$s (a, b, s, d) values (4, 4, 4, 'a');\n"
                           + "UPDATE %1$s SET s = 5 WHERE a = 4 IF s = null;\n"
                           + "APPLY BATCH"),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 4"),
                   row(4, 4, 5, "a"));

        assertRows(execute("BEGIN BATCH\n"
                           + "INSERT INTO %1$s (a, b, s, d) values (5, 5, 5, 'a');\n"
                           + "UPDATE %1$s SET s = 6 WHERE a = 5 IF s IN (1,2,null);\n"
                           + "APPLY BATCH"),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 5"),
                   row(5, 5, 6, "a"));

        // rejected: IN doesn't contain null
        assertRows(execute("BEGIN BATCH\n"
                           + "INSERT INTO %1$s (a, b, s, d) values (6, 6, 70, 'a');\n"
                           + "UPDATE %1$s SET s = 60 WHERE a = 6 IF s IN (1,2,3);\n"
                           + "APPLY BATCH"),
                   row(false, 6, 6, null));
        assertRows(execute("SELECT * FROM %s WHERE a = 6"),
                   row(6, 6, null, null));
    }

    @Test
    public void testConditionalUpdatesWithNonExistingValuesWithBatch() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, s int static, d text, PRIMARY KEY (a, b))");

        assertRows(execute("BEGIN BATCH\n"
                           + "INSERT INTO %1$s (a, b, d) values (2, 2, 'a');\n"
                           + "UPDATE %1$s SET s = 2 WHERE a = 2 IF s = null;\n"
                           + "APPLY BATCH"),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 2"),
                   row(2, 2, 2, "a"));

        // applied: lwt condition is executed before INSERT, update is applied after it
        assertRows(execute("BEGIN BATCH\n"
                           + "INSERT INTO %1$s (a, b, s, d) values (4, 4, 4, 'a');\n"
                           + "UPDATE %1$s SET s = 5 WHERE a = 4 IF s = null;\n"
                           + "APPLY BATCH"),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 4"),
                   row(4, 4, 5, "a")); // Note that the update wins because 5 > 4 (we have a timestamp tie, so values are used)

        assertRows(execute("BEGIN BATCH\n"
                           + "INSERT INTO %1$s (a, b, s, d) values (5, 5, 5, 'a');\n"
                           + "UPDATE %1$s SET s = 6 WHERE a = 5 IF s IN (1,2,null);\n"
                           + "APPLY BATCH"),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 5"),
                   row(5, 5, 6, "a")); // Same as above

        assertRows(execute("BEGIN BATCH\n"
                           + "INSERT INTO %1$s (a, b, s, d) values (7, 7, 7, 'a');\n"
                           + "UPDATE %1$s SET s = 8 WHERE a = 7 IF s != 7;\n"
                           + "APPLY BATCH"),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 7"),
                   row(7, 7, 8, "a")); // Same as above

        // rejected: comparing number with non-existing value always returns false
        for (String operator: new String[] { ">", "<", ">=", "<=", "="})
        {
            assertRows(execute("BEGIN BATCH\n"
                               + "INSERT INTO %1$s (a, b, s, d) values (3, 3, 3, 'a');\n"
                               + "UPDATE %1$s SET s = 3 WHERE a = 3 IF s " + operator + " 5;\n"
                               + "APPLY BATCH"),
                       row(false));
            assertEmpty(execute("SELECT * FROM %s WHERE a = 3"));
        }

        // rejected: IN doesn't contain null
        assertRows(execute("BEGIN BATCH\n"
                           + "INSERT INTO %1$s (a, b, s, d) values (6, 6, 6, 'a');\n"
                           + "UPDATE %1$s SET s = 7 WHERE a = 6 IF s IN (1,2,3);\n"
                           + "APPLY BATCH"),
                   row(false));
        assertEmpty(execute("SELECT * FROM %s WHERE a = 6"));
    }

    @Test
    public void testConditionalDeleteWithNullValues() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, s1 int static, s2 int static, v int, PRIMARY KEY (a, b))");

        for (int i = 1; i <= 5; i++)
            execute("INSERT INTO %s (a, b, s1, s2, v) VALUES (?, ?, ?, ?, ?)", i, i, i, null, i);

        assertRows(execute("DELETE s1 FROM %s WHERE a = 1 IF s2 = ?", (Integer) null),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 1"),
                   row(1, 1, null, null, 1));

        // rejected: IN doesn't contain null
        assertRows(execute("DELETE s1 FROM %s WHERE a = 2 IF s2 IN ?", list(10,20,30)),
                   row(false, null));
        assertRows(execute("SELECT * FROM %s WHERE a = 2"),
                   row(2, 2, 2, null, 2));

        assertRows(execute("DELETE s1 FROM %s WHERE a = 3 IF s2 IN (?, ?, ?)", null, 20, 30),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 3"),
                   row(3, 3, null, null, 3));

        assertRows(execute("DELETE s1 FROM %s WHERE a = 4 IF s2 != ?", 4),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 4"),
                   row(4, 4, null, null, 4));

        // rejected: comparing number with NULL always returns false
        for (String operator : new String[]{ ">", "<", ">=", "<=", "=" })
        {
            assertRows(execute("DELETE s1 FROM %s WHERE a = 5 IF s2 " + operator + " ?", 3),
                       row(false, null));
            assertRows(execute("SELECT * FROM %s WHERE a = 5"),
                       row(5, 5, 5, null, 5));
        }
    }

    @Test
    public void testConditionalDeletesWithNonExistingValuesWithBatch() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, s1 int static, s2 int static, v int, PRIMARY KEY (a, b))");

        assertRows(execute("BEGIN BATCH\n"
                           + "INSERT INTO %1$s (a, b, s1, v) values (2, 2, 2, 2);\n"
                           + "DELETE s1 FROM %1$s WHERE a = 2 IF s2 = null;\n"
                           + "APPLY BATCH"),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 2"),
                   row(2, 2, null, null, 2));

        // rejected: comparing number with non-existing value always returns false
        for (String operator: new String[] { ">", "<", ">=", "<=", "="})
        {
            assertRows(execute("BEGIN BATCH\n"
                               + "INSERT INTO %1$s (a, b, s1, v) values (3, 3, 3, 3);\n"
                               + "DELETE s1 FROM %1$s WHERE a = 3 IF s2 " + operator + " 5;\n"
                               + "APPLY BATCH"),
                       row(false));
            assertEmpty(execute("SELECT * FROM %s WHERE a = 3"));
        }

        // rejected: IN doesn't contain null
        assertRows(execute("BEGIN BATCH\n"
                           + "INSERT INTO %1$s (a, b, s1, v) values (6, 6, 6, 6);\n"
                           + "DELETE s1 FROM %1$s WHERE a = 6 IF s2 IN (1,2,3);\n"
                           + "APPLY BATCH"),
                   row(false));
        assertEmpty(execute("SELECT * FROM %s WHERE a = 6"));

        // Note that on equal timestamp, tombstone wins so the DELETE wins
        assertRows(execute("BEGIN BATCH\n"
                           + "INSERT INTO %1$s (a, b, s1, v) values (4, 4, 4, 4);\n"
                           + "DELETE s1 FROM %1$s WHERE a = 4 IF s2 = null;\n"
                           + "APPLY BATCH"),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 4"),
                   row(4, 4, null, null, 4));

        // Note that on equal timestamp, tombstone wins so the DELETE wins
        assertRows(execute("BEGIN BATCH\n"
                           + "INSERT INTO %1$s (a, b, s1, v) VALUES (5, 5, 5, 5);\n"
                           + "DELETE s1 FROM %1$s WHERE a = 5 IF s1 IN (1,2,null);\n"
                           + "APPLY BATCH"),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 5"),
                   row(5, 5, null, null, 5));

        // Note that on equal timestamp, tombstone wins so the DELETE wins
        assertRows(execute("BEGIN BATCH\n"
                           + "INSERT INTO %1$s (a, b, s1, v) values (7, 7, 7, 7);\n"
                           + "DELETE s1 FROM %1$s WHERE a = 7 IF s2 != 7;\n"
                           + "APPLY BATCH"),
                   row(true));
        assertRows(execute("SELECT * FROM %s WHERE a = 7"),
                   row(7, 7, null, null, 7));
    }

    /**
     * Test for CASSANDRA-12060, using a table without clustering.
     */
    @Test
    public void testMultiExistConditionOnSameRowNoClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 text, v2 text)");

        // Multiple inserts on the same row with not exist conditions
        assertRows(execute("BEGIN BATCH "
                           + "INSERT INTO %1$s (k, v1) values (0, 'foo') IF NOT EXISTS; "
                           + "INSERT INTO %1$s (k, v2) values (0, 'bar') IF NOT EXISTS; "
                           + "APPLY BATCH"),
                   row(true));

        assertRows(execute("SELECT * FROM %s WHERE k = 0"), row(0, "foo", "bar"));

        // Same, but both insert on the same column: doing so would almost surely be a user error, but that's the
        // original case reported in #12867, so being thorough.
        assertRows(execute("BEGIN BATCH "
                           + "INSERT INTO %1$s (k, v1) values (1, 'foo') IF NOT EXISTS; "
                           + "INSERT INTO %1$s (k, v1) values (1, 'bar') IF NOT EXISTS; "
                           + "APPLY BATCH"),
                   row(true));

        // As all statement gets the same timestamp, the biggest value ends up winning, so that's "foo"
        assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, "foo", null));

        // Multiple deletes on the same row with exists conditions (note that this is somewhat non-sensical, one of the
        // delete is redundant, we're just checking it doesn't break something)
        assertRows(execute("BEGIN BATCH "
                           + "DELETE FROM %1$s WHERE k = 0 IF EXISTS; "
                           + "DELETE FROM %1$s WHERE k = 0 IF EXISTS; "
                           + "APPLY BATCH"),
                   row(true));

        assertEmpty(execute("SELECT * FROM %s WHERE k = 0"));

        // Validate we can't mix different type of conditions however
        assertInvalidMessage("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row",
                             "BEGIN BATCH "
                           + "INSERT INTO %1$s (k, v1) values (1, 'foo') IF NOT EXISTS; "
                           + "DELETE FROM %1$s WHERE k = 1 IF EXISTS; "
                           + "APPLY BATCH");

        assertInvalidMessage("Cannot mix IF conditions and IF NOT EXISTS for the same row",
                             "BEGIN BATCH "
                             + "INSERT INTO %1$s (k, v1) values (1, 'foo') IF NOT EXISTS; "
                             + "UPDATE %1$s SET v2 = 'bar' WHERE k = 1 IF v1 = 'foo'; "
                             + "APPLY BATCH");
    }

    /**
     * Test for CASSANDRA-12060, using a table with clustering.
     */
    @Test
    public void testMultiExistConditionOnSameRowClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, t int, v1 text, v2 text, PRIMARY KEY (k, t))");

        // Multiple inserts on the same row with not exist conditions
        assertRows(execute("BEGIN BATCH "
                           + "INSERT INTO %1$s (k, t, v1) values (0, 0, 'foo') IF NOT EXISTS; "
                           + "INSERT INTO %1$s (k, t, v2) values (0, 0, 'bar') IF NOT EXISTS; "
                           + "APPLY BATCH"),
                   row(true));

        assertRows(execute("SELECT * FROM %s WHERE k = 0"), row(0, 0, "foo", "bar"));

        // Same, but both insert on the same column: doing so would almost surely be a user error, but that's the
        // original case reported in #12867, so being thorough.
        assertRows(execute("BEGIN BATCH "
                           + "INSERT INTO %1$s (k, t, v1) values (1, 0, 'foo') IF NOT EXISTS; "
                           + "INSERT INTO %1$s (k, t, v1) values (1, 0, 'bar') IF NOT EXISTS; "
                           + "APPLY BATCH"),
                   row(true));

        // As all statement gets the same timestamp, the biggest value ends up winning, so that's "foo"
        assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, 0, "foo", null));

        // Multiple deletes on the same row with exists conditions (note that this is somewhat non-sensical, one of the
        // delete is redundant, we're just checking it doesn't break something)
        assertRows(execute("BEGIN BATCH "
                           + "DELETE FROM %1$s WHERE k = 0 AND t = 0 IF EXISTS; "
                           + "DELETE FROM %1$s WHERE k = 0 AND t = 0 IF EXISTS; "
                           + "APPLY BATCH"),
                   row(true));

        assertEmpty(execute("SELECT * FROM %s WHERE k = 0"));

        // Validate we can't mix different type of conditions however
        assertInvalidMessage("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row",
                             "BEGIN BATCH "
                             + "INSERT INTO %1$s (k, t, v1) values (1, 0, 'foo') IF NOT EXISTS; "
                             + "DELETE FROM %1$s WHERE k = 1 AND t = 0 IF EXISTS; "
                             + "APPLY BATCH");

        assertInvalidMessage("Cannot mix IF conditions and IF NOT EXISTS for the same row",
                             "BEGIN BATCH "
                             + "INSERT INTO %1$s (k, t, v1) values (1, 0, 'foo') IF NOT EXISTS; "
                             + "UPDATE %1$s SET v2 = 'bar' WHERE k = 1 AND t = 0 IF v1 = 'foo'; "
                             + "APPLY BATCH");
    }

    @Test
    public void testInMarkerWithUDTs() throws Throwable
    {
        String typename = createType("CREATE TYPE %s (a int, b text)");
        String myType = KEYSPACE + '.' + typename;

            createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<" + myType + "> )");

            Object v = userType("a", 0, "b", "abc");
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", 0, v);

            // Does not apply
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", userType("a", 1, "b", "abc"), userType("a", 0, "b", "ac")),
                       row(false, v));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", userType("a", 1, "b", "abc"), null),
                       row(false, v));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", userType("a", 1, "b", "abc"), unset()),
                       row(false, v));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", null, null),
                       row(false, v));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", unset(), unset()),
                       row(false, v));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN ?", list(userType("a", 1, "b", "abc"), userType("a", 0, "b", "ac"))),
                       row(false, v));

            // Does apply
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", userType("a", 0, "b", "abc"), userType("a", 0, "b", "ac")),
                       row(true));
            assertRows(execute("UPDATE %s SET v = {a: 1, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", userType("a", 0, "b", "bc"), null),
                       row(true));
            assertRows(execute("UPDATE %s SET v = {a: 1, b: 'ac'} WHERE k = 0 IF v IN (?, ?, ?)", userType("a", 0, "b", "bc"), unset(), userType("a", 1, "b", "bc")),
                       row(true));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'abc'} WHERE k = 0 IF v IN ?", list(userType("a", 1, "b", "ac"), userType("a", 0, "b", "ac"))),
                       row(true));

            assertInvalidMessage("Invalid null list in IN condition",
                                 "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN ?", (List<ByteBuffer>) null);
            assertInvalidMessage("Invalid 'unset' value in condition",
                                 "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN ?", unset());
    }

    @Test
    public void testInMarkerWithLists() throws Throwable
    {
        for (boolean frozen : new boolean[]{false, true})
        {
            createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, l %s)",
                                      frozen
                                      ? "frozen<list<text>>"
                                      : "list<text>"));

            execute("INSERT INTO %s(k, l) VALUES (0, ['foo', 'bar', 'foobar'])");

            // Does not apply
            assertRows(execute("UPDATE %s SET l = ['foo', 'bar'] WHERE k = 0 IF l IN (?, ?)", list("foo", "foobar"), list("bar", "foobar")),
                       row(false, list("foo", "bar", "foobar")));
            assertRows(execute("UPDATE %s SET l = ['foo', 'bar'] WHERE k = 0 IF l IN (?, ?)", list("foo", "foobar"), null),
                       row(false, list("foo", "bar", "foobar")));
            assertRows(execute("UPDATE %s SET l = ['foo', 'bar'] WHERE k = 0 IF l IN (?, ?)", list("foo", "foobar"), unset()),
                       row(false, list("foo", "bar", "foobar")));
            assertRows(execute("UPDATE %s SET l = ['foo', 'bar'] WHERE k = 0 IF l IN (?, ?)", null, null),
                       row(false, list("foo", "bar", "foobar")));
            assertRows(execute("UPDATE %s SET l = ['foo', 'bar'] WHERE k = 0 IF l IN ?", list(list("foo", "foobar"), list("bar", "foobar"))),
                       row(false, list("foo", "bar", "foobar")));
            assertRows(execute("UPDATE %s SET l = ['foo', 'bar'] WHERE k = 0 IF l[?] IN ?", 1, list("foo", "foobar")),
                       row(false, list("foo", "bar", "foobar")));
            assertRows(execute("UPDATE %s SET l = ['foo', 'bar'] WHERE k = 0 IF l[?] IN (?, ?)", 1, "foo", "foobar"),
                       row(false, list("foo", "bar", "foobar")));
            assertRows(execute("UPDATE %s SET l = ['foo', 'bar'] WHERE k = 0 IF l[?] IN (?, ?)", 1, "foo", null),
                       row(false, list("foo", "bar", "foobar")));
            assertRows(execute("UPDATE %s SET l = ['foo', 'bar'] WHERE k = 0 IF l[?] IN (?, ?)", 1, "foo", unset()),
                       row(false, list("foo", "bar", "foobar")));

            // Does apply
            assertRows(execute("UPDATE %s SET l = ['foo', 'bar'] WHERE k = 0 IF l IN (?, ?)", list("foo", "bar", "foobar"), list("bar", "foobar")),
                       row(true));
            assertRows(execute("UPDATE %s SET l = ['foo', 'foobar'] WHERE k = 0 IF l IN (?, ?, ?)", list("foo", "bar", "foobar"), null, list("foo", "bar")),
                       row(true));
            assertRows(execute("UPDATE %s SET l = ['foo', 'bar'] WHERE k = 0 IF l IN (?, ?, ?)", list("foo", "bar", "foobar"), unset(), list("foo", "foobar")),
                       row(true));
            assertRows(execute("UPDATE %s SET l = ['foo', 'foobar'] WHERE k = 0 IF l IN (?, ?)", list("bar", "foobar"), list("foo", "bar")),
                       row(true));
            assertRows(execute("UPDATE %s SET l = ['foo', 'bar'] WHERE k = 0 IF l[?] IN ?", 1, list("bar", "foobar")),
                       row(true));
            assertRows(execute("UPDATE %s SET l = ['foo', 'foobar'] WHERE k = 0 IF l[?] IN (?, ?, ?)", 1, "bar", null, "foobar"),
                       row(true));
            assertRows(execute("UPDATE %s SET l = ['foo', 'foobar'] WHERE k = 0 IF l[?] IN (?, ?, ?)", 1, "bar", unset(), "foobar"),
                       row(true));

            assertInvalidMessage("Invalid null list in IN condition",
                                 "UPDATE %s SET l = ['foo', 'bar'] WHERE k = 0 IF l IN ?", (List<ByteBuffer>) null);
            assertInvalidMessage("Invalid 'unset' value in condition",
                                 "UPDATE %s SET l = ['foo', 'bar'] WHERE k = 0 IF l IN ?", unset());
            assertInvalidMessage("Invalid 'unset' value in condition",
                                 "UPDATE %s SET l = ['foo', 'bar'] WHERE k = 0 IF l[?] IN ?", 1, unset());
        }
    }

    @Test
    public void testConditionalOnDurationColumns() throws Throwable
    {
        createTable(" CREATE TABLE %s (k int PRIMARY KEY, v int, d duration)");

        assertInvalidMessage("Slice conditions are not supported on durations",
                             "UPDATE %s SET v = 3 WHERE k = 0 IF d > 1s");
        assertInvalidMessage("Slice conditions are not supported on durations",
                             "UPDATE %s SET v = 3 WHERE k = 0 IF d >= 1s");
        assertInvalidMessage("Slice conditions are not supported on durations",
                             "UPDATE %s SET v = 3 WHERE k = 0 IF d <= 1s");
        assertInvalidMessage("Slice conditions are not supported on durations",
                             "UPDATE %s SET v = 3 WHERE k = 0 IF d < 1s");

        execute("INSERT INTO %s (k, v, d) VALUES (1, 1, 2s)");

        assertRows(execute("UPDATE %s SET v = 4 WHERE k = 1 IF d = 1s"), row(false, Duration.from("2s")));
        assertRows(execute("UPDATE %s SET v = 3 WHERE k = 1 IF d = 2s"), row(true));

        assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, Duration.from("2s"), 3));

        assertRows(execute("UPDATE %s SET d = 10s WHERE k = 1 IF d != 2s"), row(false, Duration.from("2s")));
        assertRows(execute("UPDATE %s SET v = 6 WHERE k = 1 IF d != 1s"), row(true));

        assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, Duration.from("2s"), 6));

        assertRows(execute("UPDATE %s SET v = 5 WHERE k = 1 IF d IN (1s, 5s)"), row(false, Duration.from("2s")));
        assertRows(execute("UPDATE %s SET d = 10s WHERE k = 1 IF d IN (1s, 2s)"), row(true));

        assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, Duration.from("10s"), 6));
    }

    @Test
    public void testConditionalOnDurationWithinLists() throws Throwable
    {
        for (Boolean frozen : new Boolean[]{Boolean.FALSE, Boolean.TRUE})
        {
            String listType = String.format(frozen ? "frozen<%s>" : "%s", "list<duration>");

            createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, l " + listType + " )");

            assertInvalidMessage("Slice conditions are not supported on collections containing durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF l > [1s, 2s]");
            assertInvalidMessage("Slice conditions are not supported on collections containing durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF l >= [1s, 2s]");
            assertInvalidMessage("Slice conditions are not supported on collections containing durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF l <= [1s, 2s]");
            assertInvalidMessage("Slice conditions are not supported on collections containing durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF l < [1s, 2s]");

            execute("INSERT INTO %s (k, v, l) VALUES (1, 1, [1s, 2s])");

            assertRows(execute("UPDATE %s SET v = 4 WHERE k = 1 IF l = [2s]"), row(false, list(Duration.from("1000ms"), Duration.from("2s"))));
            assertRows(execute("UPDATE %s SET v = 3 WHERE k = 1 IF l = [1s, 2s]"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, list(Duration.from("1000ms"), Duration.from("2s")), 3));

            assertRows(execute("UPDATE %s SET l = [10s] WHERE k = 1 IF l != [1s, 2s]"), row(false, list(Duration.from("1000ms"), Duration.from("2s"))));
            assertRows(execute("UPDATE %s SET v = 6 WHERE k = 1 IF l != [1s]"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, list(Duration.from("1000ms"), Duration.from("2s")), 6));

            assertRows(execute("UPDATE %s SET v = 5 WHERE k = 1 IF l IN ([1s], [1s, 5s])"), row(false, list(Duration.from("1000ms"), Duration.from("2s"))));
            assertRows(execute("UPDATE %s SET l = [5s, 10s] WHERE k = 1 IF l IN ([1s], [1s, 2s])"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, list(Duration.from("5s"), Duration.from("10s")), 6));

            assertInvalidMessage("Slice conditions are not supported on durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF l[0] > 1s");
            assertInvalidMessage("Slice conditions are not supported on durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF l[0] >= 1s");
            assertInvalidMessage("Slice conditions are not supported on durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF l[0] <= 1s");
            assertInvalidMessage("Slice conditions are not supported on durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF l[0] < 1s");

            assertRows(execute("UPDATE %s SET v = 4 WHERE k = 1 IF l[0] = 2s"), row(false, list(Duration.from("5s"), Duration.from("10s"))));
            assertRows(execute("UPDATE %s SET v = 3 WHERE k = 1 IF l[0] = 5s"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, list(Duration.from("5s"), Duration.from("10s")), 3));

            assertRows(execute("UPDATE %s SET l = [10s] WHERE k = 1 IF l[1] != 10s"), row(false, list(Duration.from("5s"), Duration.from("10s"))));
            assertRows(execute("UPDATE %s SET v = 6 WHERE k = 1 IF l[1] != 1s"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, list(Duration.from("5s"), Duration.from("10s")), 6));

            assertRows(execute("UPDATE %s SET v = 5 WHERE k = 1 IF l[0] IN (2s, 10s)"), row(false, list(Duration.from("5s"), Duration.from("10s"))));
            assertRows(execute("UPDATE %s SET l = [6s, 12s] WHERE k = 1 IF l[0] IN (5s, 10s)"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, list(Duration.from("6s"), Duration.from("12s")), 6));
        }
    }

    @Test
    public void testInMarkerWithMaps() throws Throwable
    {
        for (boolean frozen : new boolean[] {false, true})
        {
            createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, m %s)",
                                      frozen
                                      ? "frozen<map<text, text>>"
                                      : "map<text, text>"));

            execute("INSERT INTO %s (k, m) VALUES (0, {'foo' : 'bar'})");

            // Does not apply
            assertRows(execute("UPDATE %s SET m = {'foo' : 'foobar'} WHERE k = 0 IF m IN (?, ?)", map("foo", "foobar"), map("bar", "foobar")),
                       row(false, map("foo", "bar")));
            assertRows(execute("UPDATE %s SET  m = {'foo' : 'foobar'} WHERE k = 0 IF m IN (?, ?)", map("foo", "foobar"), null),
                       row(false, map("foo", "bar")));
            assertRows(execute("UPDATE %s SET  m = {'foo' : 'foobar'} WHERE k = 0 IF m IN (?, ?)", map("foo", "foobar"), unset()),
                       row(false, map("foo", "bar")));
            assertRows(execute("UPDATE %s SET  m = {'foo' : 'foobar'} WHERE k = 0 IF m IN (?, ?)", null, null),
                       row(false, map("foo", "bar")));
            assertRows(execute("UPDATE %s SET  m = {'foo' : 'foobar'} WHERE k = 0 IF m IN ?", list(map("foo", "foobar"), map("bar", "foobar"))),
                       row(false, map("foo", "bar")));
            assertRows(execute("UPDATE %s SET  m = {'foo' : 'foobar'} WHERE k = 0 IF m[?] IN ?", "foo", list("foo", "foobar")),
                       row(false, map("foo", "bar")));
            assertRows(execute("UPDATE %s SET  m = {'foo' : 'foobar'} WHERE k = 0 IF m[?] IN (?, ?)", "foo", "foo", "foobar"),
                       row(false, map("foo", "bar")));
            assertRows(execute("UPDATE %s SET  m = {'foo' : 'foobar'} WHERE k = 0 IF m[?] IN (?, ?)", "foo", "foo", null),
                       row(false, map("foo", "bar")));
            assertRows(execute("UPDATE %s SET  m = {'foo' : 'foobar'} WHERE k = 0 IF m[?] IN (?, ?)", "foo", "foo", unset()),
                       row(false, map("foo", "bar")));

            // Does apply
            assertRows(execute("UPDATE %s SET m = {'foo' : 'foobar'} WHERE k = 0 IF m IN (?, ?)", map("foo", "foobar"), map("foo", "bar")),
                       row(true));
            assertRows(execute("UPDATE %s SET m = {'foo' : 'bar'} WHERE k = 0 IF m IN (?, ?, ?)", map("bar", "foobar"), null, map("foo", "foobar")),
                       row(true));
            assertRows(execute("UPDATE %s SET m = {'foo' : 'bar'} WHERE k = 0 IF m IN (?, ?, ?)", map("bar", "foobar"), unset(), map("foo", "bar")),
                       row(true));
            assertRows(execute("UPDATE %s SET m = {'foo' : 'foobar'} WHERE k = 0 IF m IN ?", list(map("foo", "bar"), map("bar", "foobar"))),
                       row(true));
            assertRows(execute("UPDATE %s SET m = {'foo' : 'bar'} WHERE k = 0 IF m[?] IN ?", "foo", list("bar", "foobar")),
                       row(true));
            assertRows(execute("UPDATE %s SET m = {'foo' : 'foobar'} WHERE k = 0 IF m[?] IN (?, ?, ?)", "foo", "bar", null, "foobar"),
                       row(true));
            assertRows(execute("UPDATE %s SET m = {'foo' : 'foobar'} WHERE k = 0 IF m[?] IN (?, ?, ?)", "foo", "bar", unset(), "foobar"),
                       row(true));

            assertInvalidMessage("Invalid null list in IN condition",
                                 "UPDATE %s SET  m = {'foo' : 'foobar'} WHERE k = 0 IF m IN ?", (List<ByteBuffer>) null);
            assertInvalidMessage("Invalid 'unset' value in condition",
                                 "UPDATE %s SET  m = {'foo' : 'foobar'} WHERE k = 0 IF m IN ?", unset());
            assertInvalidMessage("Invalid 'unset' value in condition",
                                 "UPDATE %s SET  m = {'foo' : 'foobar'} WHERE k = 0 IF m[?] IN ?", "foo", unset());
        }
    }

    @Test
    public void testConditionalOnDurationWithinMaps() throws Throwable
    {
        for (Boolean frozen : new Boolean[]{Boolean.FALSE, Boolean.TRUE})
        {
            String mapType = String.format(frozen ? "frozen<%s>" : "%s", "map<int, duration>");

            createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, m " + mapType + " )");

            assertInvalidMessage("Slice conditions are not supported on collections containing durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF m > {1: 1s, 2: 2s}");
            assertInvalidMessage("Slice conditions are not supported on collections containing durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF m >= {1: 1s, 2: 2s}");
            assertInvalidMessage("Slice conditions are not supported on collections containing durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF m <= {1: 1s, 2: 2s}");
            assertInvalidMessage("Slice conditions are not supported on collections containing durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF m < {1: 1s, 2: 2s}");

            execute("INSERT INTO %s (k, v, m) VALUES (1, 1, {1: 1s, 2: 2s})");

            assertRows(execute("UPDATE %s SET v = 4 WHERE k = 1 IF m = {2: 2s}"), row(false, map(1, Duration.from("1000ms"), 2, Duration.from("2s"))));
            assertRows(execute("UPDATE %s SET v = 3 WHERE k = 1 IF m = {1: 1s, 2: 2s}"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, map(1, Duration.from("1000ms"), 2, Duration.from("2s")), 3));

            assertRows(execute("UPDATE %s SET m = {1 :10s} WHERE k = 1 IF m != {1: 1s, 2: 2s}"), row(false, map(1, Duration.from("1000ms"), 2, Duration.from("2s"))));
            assertRows(execute("UPDATE %s SET v = 6 WHERE k = 1 IF m != {1: 1s}"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, map(1, Duration.from("1000ms"), 2, Duration.from("2s")), 6));

            assertRows(execute("UPDATE %s SET v = 5 WHERE k = 1 IF m IN ({1: 1s}, {1: 5s})"), row(false, map(1, Duration.from("1000ms"), 2, Duration.from("2s"))));
            assertRows(execute("UPDATE %s SET m = {1: 5s, 2: 10s} WHERE k = 1 IF m IN ({1: 1s}, {1: 1s, 2: 2s})"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, map(1, Duration.from("5s"), 2, Duration.from("10s")), 6));

            assertInvalidMessage("Slice conditions are not supported on durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF m[1] > 1s");
            assertInvalidMessage("Slice conditions are not supported on durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF m[1] >= 1s");
            assertInvalidMessage("Slice conditions are not supported on durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF m[1] <= 1s");
            assertInvalidMessage("Slice conditions are not supported on durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF m[1] < 1s");

            assertRows(execute("UPDATE %s SET v = 4 WHERE k = 1 IF m[1] = 2s"), row(false, map(1, Duration.from("5s"), 2, Duration.from("10s"))));
            assertRows(execute("UPDATE %s SET v = 3 WHERE k = 1 IF m[1] = 5s"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, map(1, Duration.from("5s"), 2, Duration.from("10s")), 3));

            assertRows(execute("UPDATE %s SET m = {1: 10s} WHERE k = 1 IF m[2] != 10s"), row(false, map(1, Duration.from("5s"), 2, Duration.from("10s"))));
            assertRows(execute("UPDATE %s SET v = 6 WHERE k = 1 IF m[2] != 1s"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, map(1, Duration.from("5s"), 2, Duration.from("10s")), 6));

            assertRows(execute("UPDATE %s SET v = 5 WHERE k = 1 IF m[1] IN (2s, 10s)"), row(false, map(1, Duration.from("5s"), 2, Duration.from("10s"))));
            assertRows(execute("UPDATE %s SET m = {1: 6s, 2: 12s} WHERE k = 1 IF m[1] IN (5s, 10s)"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, map(1, Duration.from("6s"), 2, Duration.from("12s")), 6));
        }
    }

    @Test
    public void testConditionalOnDurationWithinUdts() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (i int, d duration)");

        for (Boolean frozen : new Boolean[]{Boolean.FALSE, Boolean.TRUE})
        {
            udt = String.format(frozen ? "frozen<%s>" : "%s", udt);

            createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, u " + udt + " )");

            assertInvalidMessage("Slice conditions are not supported on UDTs containing durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF u > {i: 1, d: 2s}");
            assertInvalidMessage("Slice conditions are not supported on UDTs containing durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF u >= {i: 1, d: 2s}");
            assertInvalidMessage("Slice conditions are not supported on UDTs containing durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF u <= {i: 1, d: 2s}");
            assertInvalidMessage("Slice conditions are not supported on UDTs containing durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF u < {i: 1, d: 2s}");

            execute("INSERT INTO %s (k, v, u) VALUES (1, 1, {i:1, d:2s})");

            assertRows(execute("UPDATE %s SET v = 4 WHERE k = 1 IF u = {i: 2, d: 2s}"), row(false, userType("i", 1, "d", Duration.from("2s"))));
            assertRows(execute("UPDATE %s SET v = 3 WHERE k = 1 IF u = {i: 1, d: 2s}"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, userType("i", 1, "d", Duration.from("2s")), 3));

            assertRows(execute("UPDATE %s SET u = {i: 1, d: 10s} WHERE k = 1 IF u != {i: 1, d: 2s}"), row(false, userType("i", 1, "d", Duration.from("2s"))));
            assertRows(execute("UPDATE %s SET v = 6 WHERE k = 1 IF u != {i: 1, d: 1s}"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, userType("i", 1, "d", Duration.from("2s")), 6));

            assertRows(execute("UPDATE %s SET v = 5 WHERE k = 1 IF u IN ({i: 1, d: 1s}, {i: 1, d: 5s})"), row(false, userType("i", 1, "d", Duration.from("2s"))));
            assertRows(execute("UPDATE %s SET u = {i: 1, d: 10s} WHERE k = 1 IF u IN ({i: 1, d: 1s}, {i: 1, d: 2s})"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, userType("i", 1, "d", Duration.from("10s")), 6));

            assertInvalidMessage("Slice conditions are not supported on durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF u.d > 1s");
            assertInvalidMessage("Slice conditions are not supported on durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF u.d >= 1s");
            assertInvalidMessage("Slice conditions are not supported on durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF u.d <= 1s");
            assertInvalidMessage("Slice conditions are not supported on durations",
                                 "UPDATE %s SET v = 3 WHERE k = 0 IF u.d < 1s");

            assertRows(execute("UPDATE %s SET v = 4 WHERE k = 1 IF u.d = 2s"), row(false, userType("i", 1, "d", Duration.from("10s"))));
            assertRows(execute("UPDATE %s SET v = 3 WHERE k = 1 IF u.d = 10s"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, userType("i", 1, "d", Duration.from("10s")), 3));

            assertRows(execute("UPDATE %s SET u = {i: 1, d: 10s} WHERE k = 1 IF u.d != 10s"), row(false, userType("i", 1, "d", Duration.from("10s"))));
            assertRows(execute("UPDATE %s SET v = 6 WHERE k = 1 IF u.d != 1s"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, userType("i", 1, "d", Duration.from("10s")), 6));

            assertRows(execute("UPDATE %s SET v = 5 WHERE k = 1 IF u.d IN (2s, 5s)"), row(false, userType("i", 1, "d", Duration.from("10s"))));
            assertRows(execute("UPDATE %s SET u = {i: 6, d: 12s} WHERE k = 1 IF u.d IN (5s, 10s)"), row(true));

            assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, userType("i", 6, "d", Duration.from("12s")), 6));
        }
    }

    @Test
    public void testConditionalOnDurationWithinTuples() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, u tuple<int, duration> )");

        assertInvalidMessage("Slice conditions are not supported on tuples containing durations",
                             "UPDATE %s SET v = 3 WHERE k = 0 IF u > (1, 2s)");
        assertInvalidMessage("Slice conditions are not supported on tuples containing durations",
                             "UPDATE %s SET v = 3 WHERE k = 0 IF u >= (1, 2s)");
        assertInvalidMessage("Slice conditions are not supported on tuples containing durations",
                             "UPDATE %s SET v = 3 WHERE k = 0 IF u <= (1, 2s)");
        assertInvalidMessage("Slice conditions are not supported on tuples containing durations",
                             "UPDATE %s SET v = 3 WHERE k = 0 IF u < (1, 2s)");

        execute("INSERT INTO %s (k, v, u) VALUES (1, 1, (1, 2s))");

        assertRows(execute("UPDATE %s SET v = 4 WHERE k = 1 IF u = (2, 2s)"), row(false, tuple(1, Duration.from("2s"))));
        assertRows(execute("UPDATE %s SET v = 3 WHERE k = 1 IF u = (1, 2s)"), row(true));

        assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, tuple(1, Duration.from("2s")), 3));

        assertRows(execute("UPDATE %s SET u = (1, 10s) WHERE k = 1 IF u != (1, 2s)"), row(false, tuple(1, Duration.from("2s"))));
        assertRows(execute("UPDATE %s SET v = 6 WHERE k = 1 IF u != (1, 1s)"), row(true));

        assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, tuple(1, Duration.from("2s")), 6));

        assertRows(execute("UPDATE %s SET v = 5 WHERE k = 1 IF u IN ((1, 1s), (1, 5s))"), row(false, tuple(1, Duration.from("2s"))));
        assertRows(execute("UPDATE %s SET u = (1, 10s) WHERE k = 1 IF u IN ((1, 1s), (1, 2s))"), row(true));

        assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, tuple(1, Duration.from("10s")), 6));
    }
}
