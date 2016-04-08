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

            assertRows(execute("UPDATE %s SET consumed = TRUE WHERE tkn = ? IF consumed = FALSE", i), row(true));
            assertRows(execute("UPDATE %s SET consumed = TRUE WHERE tkn = ? IF consumed = FALSE", i), row(false, true));
        }
    }

    /**
     * Migrated from cql_tests.py:TestCQL.conditional_update_test()
     */
    @Test
    public void testConditionalUpdate() throws Throwable
    {
        createTable(" CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 text, v3 int)");

        // Shouldn't apply
        assertRows(execute("UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = 4"), row(false));
        assertRows(execute("UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF EXISTS"), row(false));

        // Should apply
        assertRows(execute("INSERT INTO %s (k, v1, v2) VALUES (0, 2, 'foo') IF NOT EXISTS"), row(true));

        // Shouldn't apply
        assertRows(execute("INSERT INTO %s (k, v1, v2) VALUES (0, 5, 'bar') IF NOT EXISTS"), row(false, 0, 2, "foo", null));
        assertRows(execute("SELECT * FROM %s"), row(0, 2, "foo", null));

        // Shouldn't apply
        assertRows(execute("UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = 4"), row(false, 2));
        assertRows(execute("SELECT * FROM %s"), row(0, 2, "foo", null));

        // Should apply (note: we want v2 before v1 in the statement order to exercise #5786)
        assertRows(execute("UPDATE %s SET v2 = 'bar', v1 = 3 WHERE k = 0 IF v1 = 2"), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar', v1 = 3 WHERE k = 0 IF EXISTS"), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, 3, "bar", null));

        // Shouldn't apply, only one condition is ok
        assertRows(execute("UPDATE %s SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = 3 AND v2 = 'foo'"), row(false, 3, "bar"));
        assertRows(execute("SELECT * FROM %s"), row(0, 3, "bar", null));

        // Should apply
        assertRows(execute("UPDATE %s SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = 3 AND v2 = 'bar'"), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, 5, "foobar", null));

        // Shouldn't apply
        assertRows(execute("DELETE v2 FROM %s WHERE k = 0 IF v1 = 3"), row(false, 5));
        assertRows(execute("SELECT * FROM %s"), row(0, 5, "foobar", null));

        // Shouldn't apply
        assertRows(execute("DELETE v2 FROM %s WHERE k = 0 IF v1 = null"), row(false, 5));
        assertRows(execute("SELECT * FROM %s"), row(0, 5, "foobar", null));

        // Should apply
        assertRows(execute("DELETE v2 FROM %s WHERE k = 0 IF v1 = 5"), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, 5, null, null));

        // Shouln't apply
        assertRows(execute("DELETE v1 FROM %s WHERE k = 0 IF v3 = 4"), row(false, null));

        // Should apply
        assertRows(execute("DELETE v1 FROM %s WHERE k = 0 IF v3 = null"), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, null, null, null));

        // Should apply
        assertRows(execute("DELETE FROM %s WHERE k = 0 IF v1 = null"), row(true));
        assertEmpty(execute("SELECT * FROM %s"));

        // Shouldn't apply
        assertRows(execute("UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF EXISTS"), row(false));

        // Should apply
        assertRows(execute("DELETE FROM %s WHERE k = 0 IF v1 IN (null)"), row(true));

        createTable(" CREATE TABLE %s (k int, c int, v1 text, PRIMARY KEY(k, c))");
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

        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 < 3"), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 <= 3"), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 > 1"), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 >= 1"), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 != 1"), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 != 2"), row(false, 2));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 IN (0, 1, 2)"), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 IN (142, 276)"), row(false, 2));
        assertRows(execute("UPDATE %s SET v2 = 'bar' WHERE k = 0 IF v1 IN ()"), row(false, 2));
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
        assertRows(execute("DELETE v FROM %s WHERE k='k' AND i=0 IF EXISTS"), row(false));
        assertRows(execute("DELETE FROM %s WHERE k='k' AND i=0 IF EXISTS"), row(false));

        // CASSANDRA-6430
        assertInvalidMessage("DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to use IF conditions",
                             "DELETE FROM %s WHERE k = 'k' IF EXISTS");
        assertInvalidMessage("DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to use IF conditions",
                             "DELETE FROM %s WHERE k = 'k' IF v = 'foo'");
        assertInvalidMessage("Some partition key parts are missing: k",
                             "DELETE FROM %s WHERE i = 0 IF EXISTS");

        assertInvalidMessage("Invalid INTEGER constant (0) for \"k\" of type text",
                             "DELETE FROM %s WHERE k = 0 AND i > 0 IF EXISTS");
        assertInvalidMessage("Invalid INTEGER constant (0) for \"k\" of type text",
                             "DELETE FROM %s WHERE k = 0 AND i > 0 IF v = 'foo'");
        assertInvalidMessage("DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to use IF conditions",
                             "DELETE FROM %s WHERE k = 'k' AND i > 0 IF EXISTS");
        assertInvalidMessage("DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to use IF conditions",
                             "DELETE FROM %s WHERE k = 'k' AND i > 0 IF v = 'foo'");
        assertInvalidMessage("IN on the clustering key columns is not supported with conditional deletions",
                             "DELETE FROM %s WHERE k = 'k' AND i IN (0, 1) IF v = 'foo'");
        assertInvalidMessage("IN on the clustering key columns is not supported with conditional deletions",
                             "DELETE FROM %s WHERE k = 'k' AND i IN (0, 1) IF EXISTS");

        createTable("CREATE TABLE %s(k int, s int static, i int, v text, PRIMARY KEY(k, i))");
        execute("INSERT INTO %s (k, s, i, v) VALUES ( 1, 1, 2, '1')");
        assertRows(execute("DELETE v FROM %s WHERE k = 1 AND i = 2 IF s != 1"), row(false, 1));
        assertRows(execute("DELETE v FROM %s WHERE k = 1 AND i = 2 IF s = 1"), row(true));
        assertRows(execute("SELECT * FROM %s WHERE k = 1 AND i = 2"), row(1, 2, 1, null));

        assertRows(execute("DELETE FROM %s WHERE  k = 1 AND i = 2 IF s != 1"), row(false, 1));
        assertRows(execute("DELETE FROM %s WHERE k = 1 AND i = 2 IF s = 1"), row(true));
        assertEmpty(execute("SELECT * FROM %s WHERE k = 1 AND i = 2"));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"), row(1, null, 1, null));
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

        assertRows(execute("UPDATE %s SET v='foo', version=1 WHERE id=0 AND k='k1' IF version = 0"), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, "k1", 1, "foo"));

        assertRows(execute("UPDATE %s SET v='bar', version=1 WHERE id=0 AND k='k2' IF version = 0"), row(false, 1));
        assertRows(execute("SELECT * FROM %s"), row(0, "k1", 1, "foo"));

        assertRows(execute("UPDATE %s SET v='bar', version=2 WHERE id=0 AND k='k2' IF version = 1"), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, "k1", 2, "foo"), row(0, "k2", 2, "bar"));

        // Testing batches
        assertRows(execute("BEGIN BATCH " +
                           "UPDATE %1$s SET v='foobar' WHERE id=0 AND k='k1'; " +
                           "UPDATE %1$s SET v='barfoo' WHERE id=0 AND k='k2'; " +
                           "UPDATE %1$s SET version=3 WHERE id=0 IF version=1; " +
                           "APPLY BATCH "),
                   row(false, 0, null, 2));

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

            assertInvalid("DELETE FROM %s WHERE k=0 IF l[null] = 'foobar'");
            assertInvalid("DELETE FROM %s WHERE k=0 IF l[-2] = 'foobar'");

            assertRows(execute("DELETE FROM %s WHERE k=0 IF l[1] = null"), row(false, list("foo", "bar", "foobar")));
            assertRows(execute("DELETE FROM %s WHERE k=0 IF l[1] = 'foobar'"), row(false, list("foo", "bar", "foobar")));
            assertRows(execute("SELECT * FROM %s"), row(0, list("foo", "bar", "foobar")));

            assertRows(execute("DELETE FROM %s WHERE k=0 IF l[1] = 'bar'"), row(true));
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
            assertInvalid("DELETE FROM %s WHERE k=0 IF m[null] = 'foo'");
            assertRows(execute("DELETE FROM %s WHERE k=0 IF m['foo'] = 'foo'"), row(false, map("foo", "bar")));
            assertRows(execute("DELETE FROM %s WHERE k=0 IF m['foo'] = null"), row(false, map("foo", "bar")));
            assertRows(execute("SELECT * FROM %s"), row(0, map("foo", "bar")));

            assertRows(execute("DELETE FROM %s WHERE k=0 IF m['foo'] = 'bar'"), row(true));
            assertEmpty(execute("SELECT * FROM %s"));

            execute("INSERT INTO %s(k, m) VALUES (1, null)");
            if (frozen)
                assertInvalid("UPDATE %s set m['foo'] = 'bar', m['bar'] = 'foo' WHERE k = 1 IF m['foo'] IN ('blah', null)");
            else
                assertRows(execute("UPDATE %s set m['foo'] = 'bar', m['bar'] = 'foo' WHERE k = 1 IF m['foo'] IN ('blah', null)"), row(true));
        }
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
                                  SchemaKeyspace.NAME,
                                  SchemaKeyspace.KEYSPACES),
                           keyspace),
                   row(true));

        // unsuccessful create since it's already there, confirm settings don't change
        schemaChange("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1} and durable_writes = false ");

        assertRows(execute(format("select durable_writes from %s.%s where keyspace_name = ?",
                                  SchemaKeyspace.NAME,
                                  SchemaKeyspace.KEYSPACES),
                           keyspace),
                   row(true));

        // drop and confirm
        schemaChange("DROP KEYSPACE IF EXISTS " + keyspace);

        assertEmpty(execute(format("select * from %s.%s where keyspace_name = ?", SchemaKeyspace.NAME, SchemaKeyspace.KEYSPACES),
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
                                  SchemaKeyspace.NAME,
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
                                   SchemaKeyspace.NAME,
                                   SchemaKeyspace.TYPES),
                            KEYSPACE,
                            "mytype"));
    }
}
