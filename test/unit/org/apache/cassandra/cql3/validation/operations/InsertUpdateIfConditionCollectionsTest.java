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
import java.util.Collection;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;

/* InsertUpdateIfConditionCollectionsTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670)
 * Any changes here check if they apply to the other classes
 * - InsertUpdateIfConditionStaticsTest
 * - InsertUpdateIfConditionCollectionsTest
 * - InsertUpdateIfConditionTest
 */
@RunWith(Parameterized.class)
public class InsertUpdateIfConditionCollectionsTest extends CQLTester
{
    @Parameterized.Parameter(0)
    public String clusterMinVersion;

    @Parameterized.Parameter(1)
    public Runnable assertion;

    @Parameterized.Parameters(name = "{index}: clusterMinVersion={0}")
    public static Collection<Object[]> data()
    {
        ServerTestUtils.daemonInitialization();

        return InsertUpdateIfConditionTest.data();
    }

    @BeforeClass
    public static void beforeClass()
    {
        InsertUpdateIfConditionTest.beforeClass();
    }

    @Before
    public void before()
    {
        InsertUpdateIfConditionTest.beforeSetup(clusterMinVersion, assertion);
    }

    @AfterClass
    public static void afterClass()
    {
        InsertUpdateIfConditionTest.afterClass();
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
            checkInvalidUDT("v CONTAINS KEY 123", v, InvalidRequestException.class);
            checkInvalidUDT("v CONTAINS 'bar'", v, InvalidRequestException.class);

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
        // UPDATE statement
        assertRows(execute("UPDATE %s SET v = ? WHERE k = 0 IF " + condition, value), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, value));
        // DELETE statement
        assertRows(execute("DELETE FROM %s WHERE k = 0 IF " + condition), row(true));
        assertEmpty(execute("SELECT * FROM %s"));
        execute("INSERT INTO %s (k, v) VALUES (0, ?)", value);
    }

    void checkDoesNotApplyUDT(String condition, Object value) throws Throwable
    {
        // UPDATE statement
        assertRows(execute("UPDATE %s SET v = ? WHERE k = 0 IF " + condition, value),
                   row(false, value));
        assertRows(execute("SELECT * FROM %s"), row(0, value));
        // DELETE statement
        assertRows(execute("DELETE FROM %s WHERE k = 0 IF " + condition),
                   row(false, value));
        assertRows(execute("SELECT * FROM %s"), row(0, value));
    }

    void checkInvalidUDT(String condition, Object value, Class<? extends Throwable> expected) throws Throwable
    {
        // UPDATE statement
        assertInvalidThrow(expected, "UPDATE %s SET v = ?  WHERE k = 0 IF " + condition, value);
        assertRows(execute("SELECT * FROM %s"), row(0, value));
        // DELETE statement
        assertInvalidThrow(expected, "DELETE FROM %s WHERE k = 0 IF " + condition);
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
            check_applies_list("l CONTAINS 'bar'");

            // multiple conditions
            check_applies_list("l > ['aaa', 'bbb'] AND l > ['aaa']");
            check_applies_list("l != null AND l IN (['foo', 'bar', 'foobar'])");
            check_applies_list("l CONTAINS 'foo' AND l CONTAINS 'foobar'");

            // should not apply
            check_does_not_apply_list("l = ['baz']");
            check_does_not_apply_list("l != ['foo', 'bar', 'foobar']");
            check_does_not_apply_list("l > ['z']");
            check_does_not_apply_list("l >= ['z']");
            check_does_not_apply_list("l < ['a']");
            check_does_not_apply_list("l <= ['a']");
            check_does_not_apply_list("l IN (['a'], null)");
            check_does_not_apply_list("l IN ()");
            check_does_not_apply_list("l CONTAINS 'baz'");

            // multiple conditions
            check_does_not_apply_list("l IN () AND l IN (['foo', 'bar', 'foobar'])");
            check_does_not_apply_list("l > ['zzz'] AND l < ['zzz']");
            check_does_not_apply_list("l CONTAINS 'bar' AND l CONTAINS 'baz'");

            check_invalid_list("l = [null]", InvalidRequestException.class);
            check_invalid_list("l < null", InvalidRequestException.class);
            check_invalid_list("l <= null", InvalidRequestException.class);
            check_invalid_list("l > null", InvalidRequestException.class);
            check_invalid_list("l >= null", InvalidRequestException.class);
            check_invalid_list("l IN null", SyntaxException.class);
            check_invalid_list("l IN 367", SyntaxException.class);
            check_invalid_list("l CONTAINS KEY 123", InvalidRequestException.class);
            check_invalid_list("l CONTAINS null", InvalidRequestException.class);
        }
    }

    void check_applies_list(String condition) throws Throwable
    {
        // UPDATE statement
        assertRows(execute("UPDATE %s SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF " + condition), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, list("foo", "bar", "foobar")));
        // DELETE statement
        assertRows(execute("DELETE FROM %s WHERE k=0 IF " + condition), row(true));
        assertEmpty(execute("SELECT * FROM %s"));
        execute("INSERT INTO %s(k, l) VALUES (0, ['foo', 'bar', 'foobar'])");
    }

    void check_does_not_apply_list(String condition) throws Throwable
    {
        // UPDATE statement
        assertRows(execute("UPDATE %s SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF " + condition),
                   row(false, list("foo", "bar", "foobar")));
        assertRows(execute("SELECT * FROM %s"), row(0, list("foo", "bar", "foobar")));
        // DELETE statement
        assertRows(execute("DELETE FROM %s WHERE k=0 IF " + condition),
                   row(false, list("foo", "bar", "foobar")));
        assertRows(execute("SELECT * FROM %s"), row(0, list("foo", "bar", "foobar")));
    }

    void check_invalid_list(String condition, Class<? extends Throwable> expected) throws Throwable
    {
        // UPDATE statement
        assertInvalidThrow(expected, "UPDATE %s SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF " + condition);
        assertRows(execute("SELECT * FROM %s"), row(0, list("foo", "bar", "foobar")));
        // DELETE statement
        assertInvalidThrow(expected, "DELETE FROM %s WHERE k=0 IF " + condition);
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
            assertInvalidSyntax("DELETE FROM %s WHERE k=0 IF l[?] CONTAINS ?", 0, "bar");
            assertInvalidSyntax("DELETE FROM %s WHERE k=0 IF l[?] CONTAINS KEY ?", 0, "bar");
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
            check_applies_set("s CONTAINS 'foo'");

            // multiple conditions
            check_applies_set("s > {'a'} AND s < {'z'}");
            check_applies_set("s IN (null, {'bar', 'foo'}, {'a'}) AND s IN ({'a'}, {'bar', 'foo'}, null)");
            check_applies_set("s CONTAINS 'foo' AND s CONTAINS 'bar'");

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
            check_does_not_apply_set("s CONTAINS 'baz'");

            check_invalid_set("s = {null}", InvalidRequestException.class);
            check_invalid_set("s < null", InvalidRequestException.class);
            check_invalid_set("s <= null", InvalidRequestException.class);
            check_invalid_set("s > null", InvalidRequestException.class);
            check_invalid_set("s >= null", InvalidRequestException.class);
            check_invalid_set("s IN null", SyntaxException.class);
            check_invalid_set("s IN 367", SyntaxException.class);
            check_invalid_set("s CONTAINS null", InvalidRequestException.class);
            check_invalid_set("s CONTAINS KEY 123", InvalidRequestException.class);

            // element access is not allow for sets
            check_invalid_set("s['foo'] = 'foobar'", InvalidRequestException.class);
        }
    }

    void check_applies_set(String condition) throws Throwable
    {
        // UPDATE statement
        assertRows(execute("UPDATE %s SET s = {'bar', 'foo'} WHERE k=0 IF " + condition), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, set("bar", "foo")));
        // DELETE statement
        assertRows(execute("DELETE FROM %s WHERE k=0 IF " + condition), row(true));
        assertEmpty(execute("SELECT * FROM %s"));
        execute("INSERT INTO %s (k, s) VALUES (0, {'bar', 'foo'})");
    }

    void check_does_not_apply_set(String condition) throws Throwable
    {
        // UPDATE statement
        assertRows(execute("UPDATE %s SET s = {'bar', 'foo'} WHERE k=0 IF " + condition), row(false, set("bar", "foo")));
        assertRows(execute("SELECT * FROM %s"), row(0, set("bar", "foo")));
        // DELETE statement
        assertRows(execute("DELETE FROM %s WHERE k=0 IF " + condition), row(false, set("bar", "foo")));
        assertRows(execute("SELECT * FROM %s"), row(0, set("bar", "foo")));
    }

    void check_invalid_set(String condition, Class<? extends Throwable> expected) throws Throwable
    {
        // UPDATE statement
        assertInvalidThrow(expected, "UPDATE %s SET s = {'bar', 'foo'} WHERE k=0 IF " + condition);
        assertRows(execute("SELECT * FROM %s"), row(0, set("bar", "foo")));
        // DELETE statement
        assertInvalidThrow(expected, "DELETE FROM %s WHERE k=0 IF " + condition);
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
            check_applies_map("m CONTAINS 'bar'");
            check_applies_map("m CONTAINS KEY 'foo'");

            // multiple conditions
            check_applies_map("m > {'a': 'a'} AND m < {'z': 'z'}");
            check_applies_map("m != null AND m IN (null, {'a': 'a'}, {'foo': 'bar'})");
            check_applies_map("m CONTAINS 'bar' AND m CONTAINS KEY 'foo'");

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
            check_does_not_apply_map("m CONTAINS 'foo'");
            check_does_not_apply_map("m CONTAINS KEY 'bar'");

            check_invalid_map("m = {null: null}", InvalidRequestException.class);
            check_invalid_map("m = {'a': null}", InvalidRequestException.class);
            check_invalid_map("m = {null: 'a'}", InvalidRequestException.class);
            check_invalid_map("m CONTAINS null", InvalidRequestException.class);
            check_invalid_map("m CONTAINS KEY null", InvalidRequestException.class);
            check_invalid_map("m < null", InvalidRequestException.class);
            check_invalid_map("m IN null", SyntaxException.class);
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
            assertInvalidSyntax("DELETE FROM %s WHERE k=0 IF m[?] CONTAINS ?", "foo", "bar");
            assertInvalidSyntax("DELETE FROM %s WHERE k=0 IF m[?] CONTAINS KEY ?", "foo", "bar");
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
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m frozen<list<text>>)");
        execute("INSERT INTO %s (k, m) VALUES (0, null)");

        assertRows(execute("UPDATE %s SET m = ? WHERE k = 0 IF m = ?", list("test"), list("comparison")), row(false, null));

        createTable("CREATE TABLE %s (k int PRIMARY KEY, m frozen<map<text,int>>)");
        execute("INSERT INTO %s (k, m) VALUES (0, null)");

        assertRows(execute("UPDATE %s SET m = ? WHERE k = 0 IF m = ?", map("test", 3), map("comparison", 2)), row(false, null));

        createTable("CREATE TABLE %s (k int PRIMARY KEY, m frozen<set<text>>)");
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
        // UPDATE statement
        assertRows(execute("UPDATE %s SET m = {'foo': 'bar'} WHERE k=0 IF " + condition), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, map("foo", "bar")));
        // DELETE statement
        assertRows(execute("DELETE FROM %s WHERE k=0 IF " + condition), row(true));
        assertEmpty(execute("SELECT * FROM %s"));
        execute("INSERT INTO %s (k, m) VALUES (0, {'foo' : 'bar'})");
    }

    void check_does_not_apply_map(String condition) throws Throwable
    {
        assertRows(execute("UPDATE %s SET m = {'foo': 'bar'} WHERE k=0 IF " + condition), row(false, map("foo", "bar")));
        assertRows(execute("SELECT * FROM %s"), row(0, map("foo", "bar")));
        // DELETE statement
        assertRows(execute("DELETE FROM %s WHERE k=0 IF " + condition), row(false, map("foo", "bar")));
        assertRows(execute("SELECT * FROM %s"), row(0, map("foo", "bar")));
    }

    void check_invalid_map(String condition, Class<? extends Throwable> expected) throws Throwable
    {
        // UPDATE statement
        assertInvalidThrow(expected, "UPDATE %s SET m = {'foo': 'bar'} WHERE k=0 IF " + condition);
        assertRows(execute("SELECT * FROM %s"), row(0, map("foo", "bar")));
        // DELETE statement
        assertInvalidThrow(expected, "DELETE FROM %s WHERE k=0 IF " + condition);
        assertRows(execute("SELECT * FROM %s"), row(0, map("foo", "bar")));
    }

    @Test
    public void testInMarkerWithUDTs() throws Throwable
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

            // Does not apply
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", userType("a", 1, "b", "abc"), userType("a", 0, "b", "ac")),
                       row(false, v));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", userType("a", 1, "b", "abc"), null),
                       row(false, v));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", null, null),
                       row(false, v));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", userType("a", 1, "b", "abc"), unset()),
                       row(false, v));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", unset(), unset()),
                       row(false, v));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN ?", list(userType("a", 1, "b", "abc"), userType("a", 0, "b", "ac"))),
                       row(false, v));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v.a IN (?, ?)", 1, 2),
                       row(false, v));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v.a IN (?, ?)", 1, null),
                       row(false, v));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v.a IN ?", list(1, 2)),
                       row(false, v));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v.a IN (?, ?)", 1, unset()),
                       row(false, v));

            // Does apply
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", userType("a", 0, "b", "abc"), userType("a", 0, "b", "ac")),
                       row(true));
            assertRows(execute("UPDATE %s SET v = {a: 1, b: 'bc'} WHERE k = 0 IF v IN (?, ?)", userType("a", 0, "b", "bc"), null),
                       row(true));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'abc'} WHERE k = 0 IF v IN ?", list(userType("a", 1, "b", "bc"), userType("a", 0, "b", "ac"))),
                       row(true));
            assertRows(execute("UPDATE %s SET v = {a: 1, b: 'bc'} WHERE k = 0 IF v IN (?, ?, ?)", userType("a", 1, "b", "bc"), unset(), userType("a", 0, "b", "abc")),
                       row(true));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v.a IN (?, ?)", 1, 0),
                       row(true));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'abc'} WHERE k = 0 IF v.a IN (?, ?)", 0, null),
                       row(true));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v.a IN ?", list(0, 1)),
                       row(true));
            assertRows(execute("UPDATE %s SET v = {a: 0, b: 'abc'} WHERE k = 0 IF v.a IN (?, ?, ?)", 1, unset(), 0),
                       row(true));

            assertInvalidMessage("Invalid null list in IN condition",
                                 "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN ?", (List<ByteBuffer>) null);
            assertInvalidMessage("Invalid 'unset' value in condition",
                                 "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v IN ?", unset());
            assertInvalidMessage("Invalid 'unset' value in condition",
                                 "UPDATE %s SET v = {a: 0, b: 'bc'} WHERE k = 0 IF v.a IN ?", unset());
        }
    }

    @Test
    public void testNonFrozenEmptyCollection() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<text>)");
        execute("INSERT INTO %s (k, l) VALUES (0, null)");

        // Does apply
        assertRows(execute("UPDATE %s SET l = null WHERE k = 0 IF l = ?", (ByteBuffer) null),
                   row(true));
        assertRows(execute("UPDATE %s SET l = null WHERE k = 0 IF l = ?", list()),
                   row(true));
        assertRows(execute("UPDATE %s SET l = null WHERE k = 0 IF l != ?", list("bar")),
                   row(true));
        assertRows(execute("UPDATE %s SET l = null WHERE k = 0 IF l < ?", list("a")),
                   row(true));
        assertRows(execute("UPDATE %s SET l = null WHERE k = 0 IF l <= ?", list("a")),
                   row(true));
        assertRows(execute("UPDATE %s SET l = null WHERE k = 0 IF l IN (?, ?)", null, list("bar")),
                   row(true));

        // Does not apply
        assertRows(execute("UPDATE %s SET l = null WHERE k = 0 IF l = ?", list("bar")),
                   row(false, null));
        assertRows(execute("UPDATE %s SET l = null WHERE k = 0 IF l > ?", list("a")),
                   row(false, null));
        assertRows(execute("UPDATE %s SET l = null WHERE k = 0 IF l >= ?", list("a")),
                   row(false, null));
        assertRows(execute("UPDATE %s SET l = null WHERE k = 0 IF l CONTAINS ?", "bar"),
                   row(false, null));
        assertRows(execute("UPDATE %s SET l = null WHERE k = 0 IF l CONTAINS ?", unset()),
                   row(false, null));

        assertInvalidMessage("Invalid comparison with null for operator \"CONTAINS\"",
                             "UPDATE %s SET l = null WHERE k = 0 IF l CONTAINS ?", (ByteBuffer) null);

        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<text>)");
        execute("INSERT INTO %s (k, s) VALUES (0, null)");

        // Does apply
        assertRows(execute("UPDATE %s SET s = null WHERE k = 0 IF s = ?", (ByteBuffer) null),
                   row(true));
        assertRows(execute("UPDATE %s SET s = null WHERE k = 0 IF s = ?", set()),
                   row(true));
        assertRows(execute("UPDATE %s SET s = null WHERE k = 0 IF s != ?", set("bar")),
                   row(true));
        assertRows(execute("UPDATE %s SET s = null WHERE k = 0 IF s < ?", set("a")),
                   row(true));
        assertRows(execute("UPDATE %s SET s = null WHERE k = 0 IF s <= ?", set("a")),
                   row(true));
        assertRows(execute("UPDATE %s SET s = null WHERE k = 0 IF s IN (?, ?)", null, set("bar")),
                   row(true));

        // Does not apply
        assertRows(execute("UPDATE %s SET s = null WHERE k = 0 IF s = ?", set("bar")),
                   row(false, null));
        assertRows(execute("UPDATE %s SET s = null WHERE k = 0 IF s > ?", set("a")),
                   row(false, null));
        assertRows(execute("UPDATE %s SET s = null WHERE k = 0 IF s >= ?", set("a")),
                   row(false, null));
        assertRows(execute("UPDATE %s SET s = null WHERE k = 0 IF s CONTAINS ?", "bar"),
                   row(false, null));
        assertRows(execute("UPDATE %s SET s = null WHERE k = 0 IF s CONTAINS ?", unset()),
                   row(false, null));

        assertInvalidMessage("Invalid comparison with null for operator \"CONTAINS\"",
                             "UPDATE %s SET s = null WHERE k = 0 IF s CONTAINS ?", (ByteBuffer) null);

        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<text, text>) ");
        execute("INSERT INTO %s (k, m) VALUES (0, null)");

        // Does apply
        assertRows(execute("UPDATE %s SET m = null WHERE k = 0 IF m = ?", (ByteBuffer) null),
                   row(true));
        assertRows(execute("UPDATE %s SET m = null WHERE k = 0 IF m = ?", map()),
                   row(true));
        assertRows(execute("UPDATE %s SET m = null WHERE k = 0 IF m != ?", map("foo","bar")),
                   row(true));
        assertRows(execute("UPDATE %s SET m = null WHERE k = 0 IF m < ?", map("a","a")),
                   row(true));
        assertRows(execute("UPDATE %s SET m = null WHERE k = 0 IF m <= ?", map("a","a")),
                   row(true));
        assertRows(execute("UPDATE %s SET m = null WHERE k = 0 IF m IN (?, ?)", null, map("foo","bar")),
                   row(true));

        // Does not apply
        assertRows(execute("UPDATE %s SET m = null WHERE k = 0 IF m = ?", map("foo","bar")),
                   row(false, null));
        assertRows(execute("UPDATE %s SET m = null WHERE k = 0 IF m > ?", map("a", "a")),
                   row(false, null));
        assertRows(execute("UPDATE %s SET m = null WHERE k = 0 IF m >= ?", map("a", "a")),
                   row(false, null));
        assertRows(execute("UPDATE %s SET m = null WHERE k = 0 IF m CONTAINS ?", "bar"),
                   row(false, null));
        assertRows(execute("UPDATE %s SET m = {} WHERE k = 0 IF m CONTAINS ?", unset()),
                   row(false, null));
        assertRows(execute("UPDATE %s SET m = {} WHERE k = 0 IF m CONTAINS KEY ?", "foo"),
                   row(false, null));
        assertRows(execute("UPDATE %s SET m = {} WHERE k = 0 IF m CONTAINS KEY ?", unset()),
                   row(false, null));

        assertInvalidMessage("Invalid comparison with null for operator \"CONTAINS\"",
                             "UPDATE %s SET m = {} WHERE k = 0 IF m CONTAINS ?", (ByteBuffer) null);
        assertInvalidMessage("Invalid comparison with null for operator \"CONTAINS KEY\"",
                             "UPDATE %s SET m = {} WHERE k = 0 IF m CONTAINS KEY ?", (ByteBuffer) null);
    }

}
