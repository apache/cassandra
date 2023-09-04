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
import java.util.Collection;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.utils.CassandraVersion;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/* InsertUpdateIfConditionCollectionsTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670)
 * Any changes here check if they apply to the other classes
 * - InsertUpdateIfConditionStaticsTest
 * - InsertUpdateIfConditionCollectionsTest
 * - InsertUpdateIfConditionTest
 */
@RunWith(Parameterized.class)
public class InsertUpdateIfConditionTest extends CQLTester
{
    @Parameterized.Parameter(0)
    public String clusterMinVersion;

    @Parameterized.Parameter(1)
    public Runnable assertion;

    @Parameterized.Parameters(name = "{index}: clusterMinVersion={0}")
    public static Collection<Object[]> data()
    {
        ServerTestUtils.daemonInitialization();
        return Arrays.asList(new Object[]{ "3.0", (Runnable) () -> {
                                 assertTrue(Gossiper.instance.isUpgradingFromVersionLowerThan(new CassandraVersion("3.11")));
                             } },
                             new Object[]{ "3.11", (Runnable) () -> {
                                 assertTrue(Gossiper.instance.isUpgradingFromVersionLowerThan(SystemKeyspace.CURRENT_VERSION));
                                 assertFalse(Gossiper.instance.isUpgradingFromVersionLowerThan(new CassandraVersion("3.11")));
                             } },
                             new Object[]{ SystemKeyspace.CURRENT_VERSION.toString(), (Runnable) () -> {
                                 assertFalse(Gossiper.instance.isUpgradingFromVersionLowerThan(SystemKeyspace.CURRENT_VERSION));
                             } });
    }

    @BeforeClass
    public static void beforeClass()
    {
        Gossiper.instance.start(0);
    }

    @Before
    public void before()
    {
        beforeSetup(clusterMinVersion, assertion);
    }
    
    public static void beforeSetup(String clusterMinVersion, Runnable assertion)
    {
        // setUpgradeFromVersion adds node2 to the Gossiper. On slow CI envs the Gossiper might auto-remove it after some
        // timeout if it thinks it's a fat client making the test fail. Just retry C18393.
        Util.spinAssertEquals(Boolean.TRUE, () -> {
            Util.setUpgradeFromVersion(clusterMinVersion);
            assertion.run();
            return true;
        },
                              5);
    }

    @AfterClass
    public static void afterClass()
    {
        Gossiper.instance.stop();
    }

    /**
     * Migrated from cql_tests.py:TestCQL.cas_simple_test()
     */
    @Test
    public void testSimpleCas()
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
        assertRows(execute("UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 IN (?, ?)", 1, 2), row(false));
        assertRows(execute("UPDATE %s SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 IN ?", list(1, 2)), row(false));
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
        assertRows(execute("UPDATE %s SET v2 = 'bar', v1 = 2 WHERE k = 0 IF v1 IN (?, ?)", 2, 3), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar', v1 = 3 WHERE k = 0 IF v1 IN ?", list(2, 3)), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar', v1 = 3 WHERE k = 0 IF v1 IN ?", list(1, null, 3)), row(true));
        assertRows(execute("UPDATE %s SET v2 = 'bar', v1 = 3 WHERE k = 0 IF EXISTS"), row(true));
        assertRows(execute("SELECT * FROM %s"), row(0, 3, "bar", null));

        // Shouldn't apply, only one condition is ok
        assertRows(execute("UPDATE %s SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = ? AND v2 = ?", 3, "foo"), row(false, 3, "bar"));
        assertRows(execute("UPDATE %s SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = 3 AND v2 = 'foo'"), row(false, 3, "bar"));
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

        // Shouldn't apply
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
        assertInvalidMessage("Cannot use CONTAINS on non-collection column v1", "UPDATE %s SET v1 = 'B' WHERE k = 0 IF v1 CONTAINS 'A'");
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
     * Test for 7499,
     * migrated from cql_tests.py:TestCQL.cas_and_list_index_test()
     */
    @Test
    public void testCasAndListIndex()
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
                                  SchemaKeyspaceTables.KEYSPACES),
                           keyspace),
                   row(true));

        // unsuccessful create since it's already there, confirm settings don't change
        schemaChange("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1} and durable_writes = false ");

        assertRows(execute(format("select durable_writes from %s.%s where keyspace_name = ?",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspaceTables.KEYSPACES),
                           keyspace),
                   row(true));

        // drop and confirm
        schemaChange("DROP KEYSPACE IF EXISTS " + keyspace);

        assertEmpty(execute(format("select * from %s.%s where keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.KEYSPACES),
                            keyspace));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.conditional_ddl_table_test()
     */
    @Test
    public void testDropCreateTableIfNotExists() throws Throwable
    {
        String tableName = createTableName();
        String fullTableName = KEYSPACE + '.' + tableName;

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
    public void testDropCreateIndexIfNotExists()
    {
        String tableName = createTable("CREATE TABLE %s (id text PRIMARY KEY, value1 blob, value2 blob)with comment = 'foo'");

        // try dropping when doesn't exist
        schemaChange(format("DROP INDEX IF EXISTS %s.myindex", KEYSPACE));

        // create and confirm
        createIndex("CREATE INDEX IF NOT EXISTS myindex ON %s (value1)");

        // unsuccessful create since it's already there
        execute("CREATE INDEX IF NOT EXISTS myindex ON %s (value1)");

        // drop and confirm
        execute(format("DROP INDEX IF EXISTS %s.myindex", KEYSPACE));

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

        // try dropping when doesn't exist
        execute("DROP TYPE IF EXISTS mytype");

        // create and confirm
        execute("CREATE TYPE IF NOT EXISTS mytype (somefield int)");
        assertRows(execute(format("SELECT type_name from %s.%s where keyspace_name = ? and type_name = ?",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspaceTables.TYPES),
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
                                   SchemaKeyspaceTables.TYPES),
                            KEYSPACE,
                            "mytype"));
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
    public void testConditionalUpdatesWithNullValues()
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
    public void testConditionalUpdatesWithNullValuesWithBatch()
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
    public void testConditionalDeleteWithNullValues()
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

        // Multiple deletes on the same row with exists conditions (note that this is somewhat none-sensical, one of the
        // deletes is redundant, we're just checking it doesn't break something)
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

        // Multiple deletes on the same row with exists conditions (note that this is somewhat none-sensical, one of the
        // deletes is redundant, we're just checking it doesn't break something)
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
    public void testConditionalOnDurationColumns() throws Throwable
    {
        createTable(" CREATE TABLE %s (k int PRIMARY KEY, v int, d duration)");

        assertInvalidMessage("Slice conditions ( > ) are not supported on durations",
                             "UPDATE %s SET v = 3 WHERE k = 0 IF d > 1s");
        assertInvalidMessage("Slice conditions ( >= ) are not supported on durations",
                             "UPDATE %s SET v = 3 WHERE k = 0 IF d >= 1s");
        assertInvalidMessage("Slice conditions ( <= ) are not supported on durations",
                             "UPDATE %s SET v = 3 WHERE k = 0 IF d <= 1s");
        assertInvalidMessage("Slice conditions ( < ) are not supported on durations",
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
}
