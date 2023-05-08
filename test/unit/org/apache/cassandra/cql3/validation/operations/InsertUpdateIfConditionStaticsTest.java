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

import java.util.Collection;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.cql3.CQLTester;

/* InsertUpdateIfConditionCollectionsTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670)
 * Any changes here check if they apply to the other classes
 * - InsertUpdateIfConditionStaticsTest
 * - InsertUpdateIfConditionCollectionsTest
 * - InsertUpdateIfConditionTest
 */
@RunWith(Parameterized.class)
public class InsertUpdateIfConditionStaticsTest extends CQLTester
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
    public void testStaticColumnsCasUpdateWithNullStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s1 int static, s2 int static, value int, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, s1, s2) VALUES (1, 1, 1) USING TIMESTAMP 1000");
        execute("INSERT INTO %s (pk, s1, s2) VALUES (2, 1, 1) USING TIMESTAMP 1001");
        flush();
        execute("INSERT INTO %s (pk, s1) VALUES (1, 2) USING TIMESTAMP 2000");
        execute("INSERT INTO %s (pk, s1) VALUES (2, 2) USING TIMESTAMP 2001");
        flush();
        execute("DELETE s1 FROM %s USING TIMESTAMP 3000 WHERE pk = 1");
        execute("DELETE s1 FROM %s USING TIMESTAMP 3001 WHERE pk = 2");
        flush();

        assertRows(execute("UPDATE %s SET s1 = ? WHERE pk = ? IF s1 = NULL", 2, 1), row(true));
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1), row(1, null, 2, 1, null));
        assertRows(execute("UPDATE %s SET s1 = ? WHERE pk = ? IF EXISTS", 2, 2), row(true));
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 2), row(2, null, 2, 1, null));
    }

    @Test
    public void testStaticColumnsCasDeleteWithNullStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s1 int static, s2 int static, value int, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, s1, s2) VALUES (1, 1, 1) USING TIMESTAMP 1000");
        execute("INSERT INTO %s (pk, s1, s2) VALUES (2, 1, 1) USING TIMESTAMP 1001");
        flush();
        execute("INSERT INTO %s (pk, s1) VALUES (1, 2) USING TIMESTAMP 2000");
        execute("INSERT INTO %s (pk, s1) VALUES (2, 2) USING TIMESTAMP 2001");
        flush();
        execute("DELETE s1 FROM %s USING TIMESTAMP 3000 WHERE pk = 1");
        execute("DELETE s1 FROM %s USING TIMESTAMP 3001 WHERE pk = 2");
        flush();

        assertRows(execute("DELETE s2 FROM %s WHERE pk = ? IF s1 = NULL", 1), row(true));
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1));
        assertRows(execute("DELETE s2 FROM %s WHERE pk = ? IF EXISTS", 2), row(true));
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 2));
    }
}
