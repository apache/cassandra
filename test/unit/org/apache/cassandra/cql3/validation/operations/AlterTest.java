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

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.SchemaKeyspace;

import org.junit.Assert;
import org.junit.Test;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class AlterTest extends CQLTester
{
    @Test
    public void testAddList() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        execute("ALTER TABLE %s ADD myCollection list<text>;");
        execute("INSERT INTO %s (id, content , myCollection) VALUES ('test', 'first test', ['first element']);");

        assertRows(execute("SELECT * FROM %s;"), row("test", "first test", list("first element")));
    }

    @Test
    public void testDropList() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, content text, myCollection list<text>);");
        execute("INSERT INTO %s (id, content , myCollection) VALUES ('test', 'first test', ['first element']);");
        execute("ALTER TABLE %s DROP myCollection;");

        assertRows(execute("SELECT * FROM %s;"), row("test", "first test"));
    }

    @Test
    public void testAddMap() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        execute("ALTER TABLE %s ADD myCollection map<text, text>;");
        execute("INSERT INTO %s (id, content , myCollection) VALUES ('test', 'first test', { '1' : 'first element'});");

        assertRows(execute("SELECT * FROM %s;"), row("test", "first test", map("1", "first element")));
    }

    @Test
    public void testDropMap() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, content text, myCollection map<text, text>);");
        execute("INSERT INTO %s (id, content , myCollection) VALUES ('test', 'first test', { '1' : 'first element'});");
        execute("ALTER TABLE %s DROP myCollection;");

        assertRows(execute("SELECT * FROM %s;"), row("test", "first test"));
    }

    @Test
    public void testDropListAndAddListWithSameName() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, content text, myCollection list<text>);");
        execute("INSERT INTO %s (id, content , myCollection) VALUES ('test', 'first test', ['first element']);");
        execute("ALTER TABLE %s DROP myCollection;");
        execute("ALTER TABLE %s ADD myCollection list<text>;");

        assertRows(execute("SELECT * FROM %s;"), row("test", "first test", null));
        execute("UPDATE %s set myCollection = ['second element'] WHERE id = 'test';");
        assertRows(execute("SELECT * FROM %s;"), row("test", "first test", list("second element")));
    }

    @Test
    public void testDropListAndAddMapWithSameName() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, content text, myCollection list<text>);");
        execute("INSERT INTO %s (id, content , myCollection) VALUES ('test', 'first test', ['first element']);");
        execute("ALTER TABLE %s DROP myCollection;");

        assertInvalid("ALTER TABLE %s ADD myCollection map<int, int>;");
    }

    @Test
    public void testChangeStrategyWithUnquotedAgrument() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY);");

        assertInvalidSyntaxMessage("no viable alternative at input '}'",
                                   "ALTER TABLE %s WITH caching = {'keys' : 'all', 'rows_per_partition' : ALL};");
    }

    @Test
    // tests CASSANDRA-7976
    public void testAlterIndexInterval() throws Throwable
    {
        String tableName = createTable("CREATE TABLE IF NOT EXISTS %s (id uuid, album text, artist text, data blob, PRIMARY KEY (id))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);

        alterTable("ALTER TABLE %s WITH min_index_interval=256 AND max_index_interval=512");
        assertEquals(256, cfs.metadata.params.minIndexInterval);
        assertEquals(512, cfs.metadata.params.maxIndexInterval);

        alterTable("ALTER TABLE %s WITH caching = {}");
        assertEquals(256, cfs.metadata.params.minIndexInterval);
        assertEquals(512, cfs.metadata.params.maxIndexInterval);
    }

    /**
     * Migrated from cql_tests.py:TestCQL.create_alter_options_test()
     */
    @Test
    public void testCreateAlterKeyspaces() throws Throwable
    {
        assertInvalidThrow(SyntaxException.class, "CREATE KEYSPACE ks1");
        assertInvalidThrow(ConfigurationException.class, "CREATE KEYSPACE ks1 WITH replication= { 'replication_factor' : 1 }");

        execute("CREATE KEYSPACE ks1 WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        execute("CREATE KEYSPACE ks2 WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes=false");

        assertRows(execute("SELECT keyspace_name, durable_writes FROM system_schema.keyspaces"),
                   row("ks1", true),
                   row(KEYSPACE, true),
                   row(KEYSPACE_PER_TEST, true),
                   row("ks2", false));

        execute("ALTER KEYSPACE ks1 WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND durable_writes=False");
        execute("ALTER KEYSPACE ks2 WITH durable_writes=true");

        assertRows(execute("SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces"),
                   row("ks1", false, map("class", "org.apache.cassandra.locator.NetworkTopologyStrategy", "dc1", "1")),
                   row(KEYSPACE, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                   row(KEYSPACE_PER_TEST, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                   row("ks2", true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")));

        execute("USE ks1");

        assertInvalidThrow(ConfigurationException.class, "CREATE TABLE cf1 (a int PRIMARY KEY, b int) WITH compaction = { 'min_threshold' : 4 }");

        execute("CREATE TABLE cf1 (a int PRIMARY KEY, b int) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 7 }");
        assertRows(execute("SELECT table_name, compaction FROM system_schema.tables WHERE keyspace_name='ks1'"),
                   row("cf1", map("class", "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
                                  "min_threshold", "7",
                                  "max_threshold", "32")));

        // clean-up
        execute("DROP KEYSPACE ks1");
        execute("DROP KEYSPACE ks2");
    }

    /**
     * Test for bug of 5232,
     * migrated from cql_tests.py:TestCQL.alter_bug_test()
     */
    @Test
    public void testAlterStatementWithAdd() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, t text)");

        execute("UPDATE %s SET t = '111' WHERE id = 1");

        execute("ALTER TABLE %s ADD l list<text>");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, null, "111"));

        execute("ALTER TABLE %s ADD m map<int, text>");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, null, null, "111"));
    }

    /**
     * Test for 7744,
     * migrated from cql_tests.py:TestCQL.downgrade_to_compact_bug_test()
     */
    @Test
    public void testDowngradeToCompact() throws Throwable
    {
        createTable("create table %s (k int primary key, v set<text>)");
        execute("insert into %s (k, v) VALUES (0, {'f'})");
        flush();
        execute("alter table %s drop v");
        execute("alter table %s add v int");
    }

    @Test
    // tests CASSANDRA-9565
    public void testDoubleWith() throws Throwable
    {
        String[] stmts = { "ALTER KEYSPACE WITH WITH DURABLE_WRITES = true",
                           "ALTER KEYSPACE ks WITH WITH DURABLE_WRITES = true" };

        for (String stmt : stmts) {
            assertInvalidSyntaxMessage("no viable alternative at input 'WITH'", stmt);
        }
    }

    @Test
    public void testAlterTableWithCompression() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, primary key (a, b))");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaKeyspace.NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("chunk_length_in_kb", "64", "class", "org.apache.cassandra.io.compress.LZ4Compressor")));

        execute("ALTER TABLE %s WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 };");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaKeyspace.NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("chunk_length_in_kb", "32", "class", "org.apache.cassandra.io.compress.SnappyCompressor")));

        execute("ALTER TABLE %s WITH compression = { 'sstable_compression' : 'LZ4Compressor', 'chunk_length_kb' : 64 };");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaKeyspace.NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("chunk_length_in_kb", "64", "class", "org.apache.cassandra.io.compress.LZ4Compressor")));

        execute("ALTER TABLE %s WITH compression = { 'sstable_compression' : '', 'chunk_length_kb' : 32 };");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaKeyspace.NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("enabled", "false")));

        execute("ALTER TABLE %s WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 };");
        execute("ALTER TABLE %s WITH compression = { 'enabled' : 'false'};");

        assertRows(execute(format("SELECT compression FROM %s.%s WHERE keyspace_name = ? and table_name = ?;",
                                  SchemaKeyspace.NAME,
                                  SchemaKeyspace.TABLES),
                           KEYSPACE,
                           currentTable()),
                   row(map("enabled", "false")));

        assertThrowsConfigurationException("Missing sub-option 'class' for the 'compression' option.",
                                           "ALTER TABLE %s WITH  compression = {'chunk_length_in_kb' : 32};");

        assertThrowsConfigurationException("The 'class' option must not be empty. To disable compression use 'enabled' : false",
                                           "ALTER TABLE %s WITH  compression = { 'class' : ''};");

        assertThrowsConfigurationException("If the 'enabled' option is set to false no other options must be specified",
                                           "ALTER TABLE %s WITH compression = { 'enabled' : 'false', 'class' : 'SnappyCompressor'};");

        assertThrowsConfigurationException("The 'sstable_compression' option must not be used if the compression algorithm is already specified by the 'class' option",
                                           "ALTER TABLE %s WITH compression = { 'sstable_compression' : 'SnappyCompressor', 'class' : 'SnappyCompressor'};");

        assertThrowsConfigurationException("The 'chunk_length_kb' option must not be used if the chunk length is already specified by the 'chunk_length_in_kb' option",
                                           "ALTER TABLE %s WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_kb' : 32 , 'chunk_length_in_kb' : 32 };");
    }

    @Test
    public void testAlterType() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        alterTable("ALTER TABLE %s ALTER content TYPE blob");

        createTable("CREATE TABLE %s (pk int, ck text, value blob, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)");
        alterTable("ALTER TABLE %s ALTER ck TYPE blob");

        createTable("CREATE TABLE %s (pk int, ck int, value blob, PRIMARY KEY (pk, ck))");
        assertThrowsConfigurationException("Cannot change value from type blob to type text: types are incompatible.",
                                           "ALTER TABLE %s ALTER value TYPE TEXT;");
    }

    /**
     * tests CASSANDRA-10027
     */
    @Test
    public void testAlterColumnTypeToDate() throws Throwable
    {
        createTable("CREATE TABLE %s (key int PRIMARY KEY, c1 int);");
        execute("INSERT INTO %s (key, c1) VALUES (1,1);");
        execute("ALTER TABLE %s ALTER c1 TYPE date;");
        assertRows(execute("SELECT * FROM %s"), row(1, 1));

        createTable("CREATE TABLE %s (key int PRIMARY KEY, c1 varint);");
        execute("INSERT INTO %s (key, c1) VALUES (1,1);");
        assertInvalidMessage("Cannot change c1 from type varint to type date: types are incompatible.",
                             "ALTER TABLE %s ALTER c1 TYPE date;");
    }

    private void assertThrowsConfigurationException(String errorMsg, String alterStmt) throws Throwable
    {
        try
        {
            execute(alterStmt);
            Assert.fail("Query should be invalid but no error was thrown. Query is: " + alterStmt);
        }
        catch (ConfigurationException e)
        {
            assertEquals(errorMsg, e.getMessage());
        }
    }

    @Test // tests CASSANDRA-8879
    public void testAlterClusteringColumnTypeInCompactTable() throws Throwable
    {
        createTable("CREATE TABLE %s (key blob, column1 blob, value blob, PRIMARY KEY ((key), column1)) WITH COMPACT STORAGE");
        assertInvalidThrow(InvalidRequestException.class, "ALTER TABLE %s ALTER column1 TYPE ascii");
    }

    /*
     * Test case to check addition of one column
    */
    @Test
    public void testAlterAddOneColumn() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (id int, name text, PRIMARY KEY (id))");
        alterTable("ALTER TABLE %s add mail text;");

        assertColumnNames(execute("SELECT * FROM %s"), "id", "mail", "name");
    }

    /*
     * Test case to check addition of more than one column
     */
    @Test
    public void testAlterAddMultiColumn() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (id int, yearofbirth int, PRIMARY KEY (id))");
        alterTable("ALTER TABLE %s add (firstname text, password blob, lastname text, \"SOME escaped col\" bigint)");

        assertColumnNames(execute("SELECT * FROM %s"), "id", "SOME escaped col", "firstname", "lastname", "password", "yearofbirth");
    }

    /*
     *  Should throw SyntaxException if multiple columns are added using wrong syntax.
     *  Expected Syntax : Alter table T1 add (C1 datatype,C2 datatype,C3 datatype)
     */
    @Test(expected = SyntaxException.class)
    public void testAlterAddMultiColumnWithoutBraces() throws Throwable
    {
        execute("ALTER TABLE %s.users add lastname text, password blob, yearofbirth int;");
    }

    /*
     *  Test case to check deletion of one column
     */
    @Test
    public void testAlterDropOneColumn() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (id text, telephone int, yearofbirth int, PRIMARY KEY (id))");
        alterTable("ALTER TABLE %s drop telephone");

        assertColumnNames(execute("SELECT * FROM %s"), "id", "yearofbirth");
    }

    @Test
    /*
     * Test case to check deletion of more than one column
     */
    public void testAlterDropMultiColumn() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (id text, address text, telephone int, yearofbirth int, \"SOME escaped col\" bigint, PRIMARY KEY (id))");
        alterTable("ALTER TABLE %s drop (address, telephone, \"SOME escaped col\");");

        assertColumnNames(execute("SELECT * FROM %s"), "id", "yearofbirth");
    }

    /*
     *  Should throw SyntaxException if multiple columns are dropped using wrong syntax.
     */
    @Test(expected = SyntaxException.class)
    public void testAlterDeletionColumnWithoutBraces() throws Throwable
    {
        execute("ALTER TABLE %s.users drop name,address;");
    }

    @Test(expected = InvalidRequestException.class)
    public void testAlterAddDuplicateColumn() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (id text, address text, telephone int, yearofbirth int, PRIMARY KEY (id))");
        execute("ALTER TABLE %s add (salary int, salary int);");
    }

    @Test(expected = InvalidRequestException.class)
    public void testAlterDropDuplicateColumn() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (id text, address text, telephone int, yearofbirth int, PRIMARY KEY (id))");
        execute("ALTER TABLE %s drop (address, address);");
    }
}
