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
import java.util.Collections;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.TriggerDefinition;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.triggers.ITrigger;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CreateTest extends CQLTester
{
    @Test
    public void testCQL3PartitionKeyOnlyTable()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY);");
        assertFalse(currentTableMetadata().isThriftCompatible());
    }

    /**
     * Creation and basic operations on a static table,
     * migrated from cql_tests.py:TestCQL.static_cf_test()
     */
    @Test
    public void testStaticTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid PRIMARY KEY, firstname text, lastname text, age int)");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, ?, ?, ?)", id1, "Frodo", "Baggins", 32);
        execute("UPDATE %s SET firstname = ?, lastname = ?, age = ? WHERE userid = ?", "Samwise", "Gamgee", 33, id2);

        assertRows(execute("SELECT firstname, lastname FROM %s WHERE userid = ?", id1),
                   row("Frodo", "Baggins"));

        assertRows(execute("SELECT * FROM %s WHERE userid = ?", id1),
                   row(id1, 32, "Frodo", "Baggins"));

        assertRows(execute("SELECT * FROM %s"),
                   row(id2, 33, "Samwise", "Gamgee"),
                   row(id1, 32, "Frodo", "Baggins")
        );

        String batch = "BEGIN BATCH "
                       + "INSERT INTO %1$s (userid, age) VALUES (?, ?) "
                       + "UPDATE %1$s SET age = ? WHERE userid = ? "
                       + "DELETE firstname, lastname FROM %1$s WHERE userid = ? "
                       + "DELETE firstname, lastname FROM %1$s WHERE userid = ? "
                       + "APPLY BATCH";

        execute(batch, id1, 36, 37, id2, id1, id2);

        assertRows(execute("SELECT * FROM %s"),
                   row(id2, 37, null, null),
                   row(id1, 36, null, null));
    }

    /**
     * Creation and basic operations on a static table with compact storage,
     * migrated from cql_tests.py:TestCQL.noncomposite_static_cf_test()
     */
    @Test
    public void testDenseStaticTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid PRIMARY KEY, firstname text, lastname text, age int) WITH COMPACT STORAGE");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, ?, ?, ?)", id1, "Frodo", "Baggins", 32);
        execute("UPDATE %s SET firstname = ?, lastname = ?, age = ? WHERE userid = ?", "Samwise", "Gamgee", 33, id2);

        assertRows(execute("SELECT firstname, lastname FROM %s WHERE userid = ?", id1),
                   row("Frodo", "Baggins"));

        assertRows(execute("SELECT * FROM %s WHERE userid = ?", id1),
                   row(id1, 32, "Frodo", "Baggins"));

        assertRows(execute("SELECT * FROM %s"),
                   row(id2, 33, "Samwise", "Gamgee"),
                   row(id1, 32, "Frodo", "Baggins")
        );

        String batch = "BEGIN BATCH "
                       + "INSERT INTO %1$s (userid, age) VALUES (?, ?) "
                       + "UPDATE %1$s SET age = ? WHERE userid = ? "
                       + "DELETE firstname, lastname FROM %1$s WHERE userid = ? "
                       + "DELETE firstname, lastname FROM %1$s WHERE userid = ? "
                       + "APPLY BATCH";

        execute(batch, id1, 36, 37, id2, id1, id2);

        assertRows(execute("SELECT * FROM %s"),
                   row(id2, 37, null, null),
                   row(id1, 36, null, null));
    }

    /**
     * Creation and basic operations on a non-composite table with compact storage,
     * migrated from cql_tests.py:TestCQL.dynamic_cf_test()
     */
    @Test
    public void testDenseNonCompositeTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid, url text, time bigint, PRIMARY KEY (userid, url)) WITH COMPACT STORAGE");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");
        UUID id3 = UUID.fromString("810e8500-e29b-41d4-a716-446655440000");

        execute("INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", id1, "http://foo.bar", 42L);
        execute("INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", id1, "http://foo-2.bar", 24L);
        execute("INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", id1, "http://bar.bar", 128L);
        execute("UPDATE %s SET time = 24 WHERE userid = ? and url = 'http://bar.foo'", id2);
        execute("UPDATE %s SET time = 12 WHERE userid IN (?, ?) and url = 'http://foo-3'", id2, id1);

        assertRows(execute("SELECT url, time FROM %s WHERE userid = ?", id1),
                   row("http://bar.bar", 128L),
                   row("http://foo-2.bar", 24L),
                   row("http://foo-3", 12L),
                   row("http://foo.bar", 42L));

        assertRows(execute("SELECT * FROM %s WHERE userid = ?", id2),
                   row(id2, "http://bar.foo", 24L),
                   row(id2, "http://foo-3", 12L));

        assertRows(execute("SELECT time FROM %s"),
                   row(24L), // id2
                   row(12L),
                   row(128L), // id1
                   row(24L),
                   row(12L),
                   row(42L)
        );

        // Check we don't allow empty values for url since this is the full underlying cell name (#6152)
        assertInvalid("INSERT INTO %s (userid, url, time) VALUES (?, '', 42)", id3);
    }

    /**
     * Creation and basic operations on a composite table with compact storage,
     * migrated from cql_tests.py:TestCQL.dense_cf_test()
     */
    @Test
    public void testDenseCompositeTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid, ip text, port int, time bigint, PRIMARY KEY (userid, ip, port)) WITH COMPACT STORAGE");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, ip, port, time) VALUES (?, '192.168.0.1', 80, 42)", id1);
        execute("INSERT INTO %s (userid, ip, port, time) VALUES (?, '192.168.0.2', 80, 24)", id1);
        execute("INSERT INTO %s (userid, ip, port, time) VALUES (?, '192.168.0.2', 90, 42)", id1);
        execute("UPDATE %s SET time = 24 WHERE userid = ? AND ip = '192.168.0.2' AND port = 80", id2);

        // we don't have to include all of the clustering columns (see CASSANDRA-7990)
        execute("INSERT INTO %s (userid, ip, time) VALUES (?, '192.168.0.3', 42)", id2);
        execute("UPDATE %s SET time = 42 WHERE userid = ? AND ip = '192.168.0.4'", id2);

        assertRows(execute("SELECT ip, port, time FROM %s WHERE userid = ?", id1),
                   row("192.168.0.1", 80, 42L),
                   row("192.168.0.2", 80, 24L),
                   row("192.168.0.2", 90, 42L));

        assertRows(execute("SELECT ip, port, time FROM %s WHERE userid = ? and ip >= '192.168.0.2'", id1),
                   row("192.168.0.2", 80, 24L),
                   row("192.168.0.2", 90, 42L));

        assertRows(execute("SELECT ip, port, time FROM %s WHERE userid = ? and ip = '192.168.0.2'", id1),
                   row("192.168.0.2", 80, 24L),
                   row("192.168.0.2", 90, 42L));

        assertEmpty(execute("SELECT ip, port, time FROM %s WHERE userid = ? and ip > '192.168.0.2'", id1));

        assertRows(execute("SELECT ip, port, time FROM %s WHERE userid = ? AND ip = '192.168.0.3'", id2),
                   row("192.168.0.3", null, 42L));

        assertRows(execute("SELECT ip, port, time FROM %s WHERE userid = ? AND ip = '192.168.0.4'", id2),
                   row("192.168.0.4", null, 42L));

        execute("DELETE time FROM %s WHERE userid = ? AND ip = '192.168.0.2' AND port = 80", id1);

        assertRowCount(execute("SELECT * FROM %s WHERE userid = ?", id1), 2);

        execute("DELETE FROM %s WHERE userid = ?", id1);
        assertEmpty(execute("SELECT * FROM %s WHERE userid = ?", id1));

        execute("DELETE FROM %s WHERE userid = ? AND ip = '192.168.0.3'", id2);
        assertEmpty(execute("SELECT * FROM %s WHERE userid = ? AND ip = '192.168.0.3'", id2));
    }

    /**
     * Creation and basic operations on a composite table,
     * migrated from cql_tests.py:TestCQL.sparse_cf_test()
     */
    @Test
    public void testSparseCompositeTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid, posted_month int, posted_day int, body text, posted_by text, PRIMARY KEY (userid, posted_month, posted_day))");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, posted_month, posted_day, body, posted_by) VALUES (?, 1, 12, 'Something else', 'Frodo Baggins')", id1);
        execute("INSERT INTO %s (userid, posted_month, posted_day, body, posted_by) VALUES (?, 1, 24, 'Something something', 'Frodo Baggins')", id1);
        execute("UPDATE %s SET body = 'Yo Froddo', posted_by = 'Samwise Gamgee' WHERE userid = ? AND posted_month = 1 AND posted_day = 3", id2);
        execute("UPDATE %s SET body = 'Yet one more message' WHERE userid = ? AND posted_month = 1 and posted_day = 30", id1);

        assertRows(execute("SELECT body, posted_by FROM %s WHERE userid = ? AND posted_month = 1 AND posted_day = 24", id1),
                   row("Something something", "Frodo Baggins"));

        assertRows(execute("SELECT posted_day, body, posted_by FROM %s WHERE userid = ? AND posted_month = 1 AND posted_day > 12", id1),
                   row(24, "Something something", "Frodo Baggins"),
                   row(30, "Yet one more message", null));

        assertRows(execute("SELECT posted_day, body, posted_by FROM %s WHERE userid = ? AND posted_month = 1", id1),
                   row(12, "Something else", "Frodo Baggins"),
                   row(24, "Something something", "Frodo Baggins"),
                   row(30, "Yet one more message", null));
    }

    /**
     * Check invalid create table statements,
     * migrated from cql_tests.py:TestCQL.create_invalid_test()
     */
    @Test
    public void testInvalidCreateTableStatements() throws Throwable
    {
        assertInvalidThrow(SyntaxException.class, "CREATE TABLE test ()");

        assertInvalid("CREATE TABLE test (c1 text, c2 text, c3 text)");
        assertInvalid("CREATE TABLE test (key1 text PRIMARY KEY, key2 text PRIMARY KEY)");

        assertInvalid("CREATE TABLE test (key text PRIMARY KEY, key int)");
        assertInvalid("CREATE TABLE test (key text PRIMARY KEY, c int, c text)");

        assertInvalid("CREATE TABLE test (key text, key2 text, c int, d text, PRIMARY KEY (key, key2)) WITH COMPACT STORAGE");
    }

    /**
     * Check obsolete properties from CQL2 are rejected
     * migrated from cql_tests.py:TestCQL.invalid_old_property_test()
     */
    @Test
    public void testObsoleteTableProperties() throws Throwable
    {
        assertInvalidThrow(SyntaxException.class, "CREATE TABLE test (foo text PRIMARY KEY, c int) WITH default_validation=timestamp");

        createTable("CREATE TABLE %s (foo text PRIMARY KEY, c int)");
        assertInvalidThrow(SyntaxException.class, "ALTER TABLE %s WITH default_validation=int");
    }

    /**
     * Test create and drop keyspace
     * migrated from cql_tests.py:TestCQL.keyspace_test()
     */
    @Test
    public void testKeyspace() throws Throwable
    {
        assertInvalidThrow(SyntaxException.class, "CREATE KEYSPACE %s testXYZ ");

        execute("CREATE KEYSPACE testXYZ WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

        assertInvalid(
                     "CREATE KEYSPACE My_much_much_too_long_identifier_that_should_not_work WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

        execute("DROP KEYSPACE testXYZ");
        assertInvalidThrow(ConfigurationException.class, "DROP KEYSPACE non_existing");

        execute("CREATE KEYSPACE testXYZ WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

        // clean-up
        execute("DROP KEYSPACE testXYZ");
    }

    /**
     * Test create and drop table
     * migrated from cql_tests.py:TestCQL.table_test()
     */
    @Test
    public void testTable() throws Throwable
    {
        String table1 = createTable(" CREATE TABLE %s (k int PRIMARY KEY, c int)");
        createTable(" CREATE TABLE %s (k int, name int, value int, PRIMARY KEY(k, name)) WITH COMPACT STORAGE ");
        createTable(" CREATE TABLE %s (k int, c int, PRIMARY KEY (k),)");

        String table4 = createTableName();

        // repeated column
        assertInvalidMessage("Multiple definition of identifier k", String.format("CREATE TABLE %s (k int PRIMARY KEY, c int, k text)", table4));

        // compact storage limitations
        assertInvalidThrow(SyntaxException.class,
                           String.format("CREATE TABLE %s (k int, name, int, c1 int, c2 int, PRIMARY KEY(k, name)) WITH COMPACT STORAGE", table4));

        execute(String.format("DROP TABLE %s.%s", keyspace(), table1));

        createTable(String.format("CREATE TABLE %s.%s ( k int PRIMARY KEY, c1 int, c2 int, ) ", keyspace(), table1));
    }

    /**
     * Test truncate statement,
     * migrated from cql_tests.py:TestCQL.table_test().
     */
    @Test
    public void testTruncate() throws Throwable
    {
        createTable(" CREATE TABLE %s (k int, name int, value int, PRIMARY KEY(k, name)) WITH COMPACT STORAGE ");
        execute("TRUNCATE %s");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.multiordering_validation_test()
     */
    @Test
    public void testMultiOrderingValidation() throws Throwable
    {
        String tableName = KEYSPACE + "." + createTableName();
        assertInvalid(String.format("CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c2 DESC)", tableName));

        tableName = KEYSPACE + "." + createTableName();
        assertInvalid(String.format("CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c2 ASC, c1 DESC)", tableName));

        tableName = KEYSPACE + "." + createTableName();
        assertInvalid(String.format("CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC, c3 DESC)", tableName));

        createTable("CREATE TABLE %s (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC)");
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)");
    }

    @Test
    public void testCreateTrigger() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a))");
        execute("CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1", TestTrigger.class);
        execute("CREATE TRIGGER trigger_2 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_2", TestTrigger.class);
        assertInvalid("CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        execute("CREATE TRIGGER \"Trigger 3\" ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("Trigger 3", TestTrigger.class);
    }

    @Test
    public void testCreateTriggerIfNotExists() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");

        execute("CREATE TRIGGER IF NOT EXISTS trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1", TestTrigger.class);

        execute("CREATE TRIGGER IF NOT EXISTS trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1", TestTrigger.class);
    }

    @Test
    public void testDropTrigger() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a))");

        execute("CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1", TestTrigger.class);

        execute("DROP TRIGGER trigger_1 ON %s");
        assertTriggerDoesNotExists("trigger_1", TestTrigger.class);

        execute("CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1", TestTrigger.class);

        assertInvalid("DROP TRIGGER trigger_2 ON %s");

        execute("CREATE TRIGGER \"Trigger 3\" ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("Trigger 3", TestTrigger.class);

        execute("DROP TRIGGER \"Trigger 3\" ON %s");
        assertTriggerDoesNotExists("Trigger 3", TestTrigger.class);
    }

    @Test
    public void testDropTriggerIfExists() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a))");

        execute("DROP TRIGGER IF EXISTS trigger_1 ON %s");
        assertTriggerDoesNotExists("trigger_1", TestTrigger.class);

        execute("CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1", TestTrigger.class);

        execute("DROP TRIGGER IF EXISTS trigger_1 ON %s");
        assertTriggerDoesNotExists("trigger_1", TestTrigger.class);
    }

    @Test
    // tests CASSANDRA-9565
    public void testDoubleWith() throws Throwable
    {
        String[] stmts = new String[] { "CREATE KEYSPACE WITH WITH DURABLE_WRITES = true",
                                        "CREATE KEYSPACE ks WITH WITH DURABLE_WRITES = true" };

        for (String stmt : stmts) {
            assertInvalidSyntaxMessage("no viable alternative at input 'WITH'", stmt);
        }
    }

    private void assertTriggerExists(String name, Class<?> clazz)
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), currentTable()).copy();
        assertTrue("the trigger does not exist", cfm.containsTriggerDefinition(TriggerDefinition.create(name,
                                                                                                        clazz.getName())));
    }

    private void assertTriggerDoesNotExists(String name, Class<?> clazz)
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), currentTable()).copy();
        Assert.assertFalse("the trigger exists", cfm.containsTriggerDefinition(TriggerDefinition.create(name,
                                                                                                        clazz.getName())));
    }

    public static class TestTrigger implements ITrigger
    {
        public TestTrigger() { }
        public Collection<Mutation> augment(ByteBuffer key, ColumnFamily update)
        {
            return Collections.emptyList();
        }
    }
}
