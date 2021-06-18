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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.SchemaKeyspace;

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
    public void testDropWithTimestamp() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, c1 int, v1 int, todrop int, PRIMARY KEY (id, c1));");
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (id, c1, v1, todrop) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", 1, i, i, i, 10000L * i);

        // flush is necessary since otherwise the values of `todrop` will get discarded during
        // alter statement
        flush();
        execute("ALTER TABLE %s DROP todrop USING TIMESTAMP 20000;");
        execute("ALTER TABLE %s ADD todrop int;");
        execute("INSERT INTO %s (id, c1, v1, todrop) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", 1, 100, 100, 100, 30000L);
        assertRows(execute("SELECT id, c1, v1, todrop FROM %s"),
                   row(1, 0, 0, null),
                   row(1, 1, 1, null),
                   row(1, 2, 2, null),
                   row(1, 3, 3, 3),
                   row(1, 4, 4, 4),
                   row(1, 100, 100, 100));
    }

    @Test
    public void testDropStaticWithTimestamp() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, c1 int, v1 int, todrop int static, PRIMARY KEY (id, c1));");
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (id, c1, v1, todrop) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", 1, i, i, i, 10000L * i);

        // flush is necessary since otherwise the values of `todrop` will get discarded during
        // alter statement
        flush();
        execute("ALTER TABLE %s DROP todrop USING TIMESTAMP 20000;");
        execute("ALTER TABLE %s ADD todrop int static;");
        execute("INSERT INTO %s (id, c1, v1, todrop) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", 1, 100, 100, 100, 30000L);
        // static column value with largest timestmap will be available again
        assertRows(execute("SELECT id, c1, v1, todrop FROM %s"),
                   row(1, 0, 0, 4),
                   row(1, 1, 1, 4),
                   row(1, 2, 2, 4),
                   row(1, 3, 3, 4),
                   row(1, 4, 4, 4),
                   row(1, 100, 100, 4));
    }

    @Test
    public void testDropMultipleWithTimestamp() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, c1 int, v1 int, todrop1 int, todrop2 int, PRIMARY KEY (id, c1));");
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (id, c1, v1, todrop1, todrop2) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?", 1, i, i, i, i, 10000L * i);

        // flush is necessary since otherwise the values of `todrop1` and `todrop2` will get discarded during
        // alter statement
        flush();
        execute("ALTER TABLE %s DROP todrop1 USING TIMESTAMP 20000;");
        execute("ALTER TABLE %s DROP todrop2 USING TIMESTAMP 20000;");
        execute("ALTER TABLE %s ADD todrop1 int;");
        execute("ALTER TABLE %s ADD todrop2 int;");

        execute("INSERT INTO %s (id, c1, v1, todrop1, todrop2) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?", 1, 100, 100, 100, 100, 40000L);
        assertRows(execute("SELECT id, c1, v1, todrop1, todrop2 FROM %s"),
                   row(1, 0, 0, null, null),
                   row(1, 1, 1, null, null),
                   row(1, 2, 2, null, null),
                   row(1, 3, 3, 3, 3),
                   row(1, 4, 4, 4, 4),
                   row(1, 100, 100, 100, 100));
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

        String ks1 = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String ks2 = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes=false");

        assertRowsIgnoringOrderAndExtra(execute("SELECT keyspace_name, durable_writes FROM system_schema.keyspaces"),
                   row(KEYSPACE, true),
                   row(KEYSPACE_PER_TEST, true),
                   row(ks1, true),
                   row(ks2, false));

        schemaChange("ALTER KEYSPACE " + ks1 + " WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND durable_writes=False");
        schemaChange("ALTER KEYSPACE " + ks2 + " WITH durable_writes=true");

        assertRowsIgnoringOrderAndExtra(execute("SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces"),
                   row(KEYSPACE, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                   row(KEYSPACE_PER_TEST, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                   row(ks1, false, map("class", "org.apache.cassandra.locator.NetworkTopologyStrategy", "dc1", "1")),
                   row(ks2, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")));

        execute("USE " + ks1);

        assertInvalidThrow(ConfigurationException.class, "CREATE TABLE cf1 (a int PRIMARY KEY, b int) WITH compaction = { 'min_threshold' : 4 }");

        execute("CREATE TABLE cf1 (a int PRIMARY KEY, b int) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 7 }");
        assertRows(execute("SELECT table_name, compaction FROM system_schema.tables WHERE keyspace_name='" + ks1 + "'"),
                   row("cf1", map("class", "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
                                  "min_threshold", "7",
                                  "max_threshold", "32")));
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

    @Test(expected = InvalidRequestException.class)
    public void testDropComplexAddSimpleColumn() throws Throwable
    {
        createTable("create table %s (k int primary key, v set<text>)");
        execute("alter table %s drop v");
        execute("alter table %s add v text");
    }

    @Test(expected = InvalidRequestException.class)
    public void testDropSimpleAddComplexColumn() throws Throwable
    {
        createTable("create table %s (k int primary key, v text)");
        execute("alter table %s drop v");
        execute("alter table %s add v set<text>");
    }

    @Test(expected = InvalidRequestException.class)
    public void testDropMultiCellAddFrozenColumn() throws Throwable
    {
        createTable("create table %s (k int primary key, v set<text>)");
        execute("alter table %s drop v");
        execute("alter table %s add v frozen<set<text>>");
    }

    @Test(expected = InvalidRequestException.class)
    public void testDropFrozenAddMultiCellColumn() throws Throwable
    {
        createTable("create table %s (k int primary key, v frozen<set<text>>)");
        execute("alter table %s drop v");
        execute("alter table %s add v set<text>");
    }

    @Test
    public void testDropTimeUUIDAddUUIDColumn() throws Throwable
    {
        createTable("create table %s (k int primary key, v timeuuid)");
        execute("alter table %s drop v");
        execute("alter table %s add v uuid");
    }

    @Test(expected = InvalidRequestException.class)
    public void testDropUUIDAddTimeUUIDColumn() throws Throwable
    {
        createTable("create table %s (k int primary key, v uuid)");
        execute("alter table %s drop v");
        execute("alter table %s add v timeuuid");
    }

    @Test
    public void testDropAddSameType() throws Throwable
    {
        createTable("create table %s (k int primary key, v1 timeuuid, v2 set<uuid>, v3 frozen<list<text>>)");

        execute("alter table %s drop v1");
        execute("alter table %s add v1 timeuuid");

        execute("alter table %s drop v2");
        execute("alter table %s add v2 set<uuid>");

        execute("alter table %s drop v3");
        execute("alter table %s add v3 frozen<list<text>>");
    }

    @Test(expected = InvalidRequestException.class)
    public void testDropRegularAddStatic() throws Throwable
    {
        createTable("create table %s (k int, c int, v uuid, PRIMARY KEY (k, c))");
        execute("alter table %s drop v");
        execute("alter table %s add v uuid static");
    }

    @Test(expected = InvalidRequestException.class)
    public void testDropStaticAddRegular() throws Throwable
    {
        createTable("create table %s (k int, c int, v uuid static, PRIMARY KEY (k, c))");
        execute("alter table %s drop v");
        execute("alter table %s add v uuid");
    }

    @Test(expected = SyntaxException.class)
    public void renameToEmptyTest() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, v int, PRIMARY KEY (k, c1))");
        execute("ALTER TABLE %s RENAME c1 TO \"\"");
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

    /**
     * Test for CASSANDRA-13337. Checks that dropping a column when a sstable contains only data for that column
     * works properly.
     */
    @Test
    public void testAlterDropEmptySSTable() throws Throwable
    {
        createTable("CREATE TABLE %s(k int PRIMARY KEY, x int, y int)");

        execute("UPDATE %s SET x = 1 WHERE k = 0");

        flush();

        execute("UPDATE %s SET x = 1, y = 1 WHERE k = 0");

        flush();

        execute("ALTER TABLE %s DROP x");

        compact();

        assertRows(execute("SELECT * FROM %s"), row(0, 1));
    }

    /**
     * Similarly to testAlterDropEmptySSTable, checks we don't return empty rows from queries (testAlterDropEmptySSTable
     * tests the compaction case).
     */
    @Test
    public void testAlterOnlyColumnBehaviorWithFlush() throws Throwable
    {
        testAlterOnlyColumnBehaviorWithFlush(true);
        testAlterOnlyColumnBehaviorWithFlush(false);
    }

    private void testAlterOnlyColumnBehaviorWithFlush(boolean flushAfterInsert) throws Throwable
    {
        createTable("CREATE TABLE %s(k int PRIMARY KEY, x int, y int)");

        execute("UPDATE %s SET x = 1 WHERE k = 0");

        assertRows(execute("SELECT * FROM %s"), row(0, 1, null));

        if (flushAfterInsert)
            flush();

        execute("ALTER TABLE %s DROP x");

        assertEmpty(execute("SELECT * FROM %s"));
    }

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testAlterWithCompactStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");

        assertInvalidMessage("Cannot rename unknown column column1 in keyspace",
                             "ALTER TABLE %s RENAME column1 TO column2");

        assertInvalidMessage("Cannot rename unknown column value in keyspace",
                             "ALTER TABLE %s RENAME value TO value2");
    }

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testAlterWithCompactNonStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        assertInvalidMessage("Cannot rename unknown column column1 in keyspace",
                             "ALTER TABLE %s RENAME column1 TO column2");

        createTable("CREATE TABLE %s (a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        assertInvalidMessage("Cannot rename unknown column column1 in keyspace",
                             "ALTER TABLE %s RENAME column1 TO column2");
    }

    @Test
    public void testAlterTableAlterType() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a,b)) WITH COMPACT STORAGE");
        assertInvalidMessage(String.format("Compact value type can only be changed to BytesType, but %s was given.",
                                           IntegerType.instance),
                             "ALTER TABLE %s ALTER value TYPE 'org.apache.cassandra.db.marshal.IntegerType'");

        execute("ALTER TABLE %s ALTER value TYPE 'org.apache.cassandra.db.marshal.BytesType'");

        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a,b)) WITH COMPACT STORAGE");
        assertInvalidMessage("Altering of types is not allowed",
                             "ALTER TABLE %s ALTER c TYPE 'org.apache.cassandra.db.marshal.BytesType'");

        createTable("CREATE TABLE %s (a int, value int, PRIMARY KEY (a,value)) WITH COMPACT STORAGE");
        assertInvalidMessage("Altering of types is not allowed",
                             "ALTER TABLE %s ALTER value TYPE 'org.apache.cassandra.db.marshal.IntegerType'");
        execute("ALTER TABLE %s ALTER value1 TYPE 'org.apache.cassandra.db.marshal.BytesType'");
    }

    @Test
    public void testAlterTypeUsedInPartitionKey() throws Throwable
    {
        // frozen UDT used directly in a partition key
        String  type1 = createType("CREATE TYPE %s (v1 int)");
        String table1 = createTable("CREATE TABLE %s (pk frozen<" + type1 + ">, val int, PRIMARY KEY(pk));");

        // frozen UDT used in a frozen UDT used in a partition key
        String  type2 = createType("CREATE TYPE %s (v1 frozen<" + type1 + ">, v2 frozen<" + type1 + ">)");
        String table2 = createTable("CREATE TABLE %s (pk frozen<" + type2 + ">, val int, PRIMARY KEY(pk));");

        // frozen UDT used in a frozen collection used in a partition key
        String table3 = createTable("CREATE TABLE %s (pk frozen<list<frozen<" + type1 + ">>>, val int, PRIMARY KEY(pk));");

        // assert that ALTER fails and that the error message contains all the names of the table referencing it
        assertInvalidMessage(table1, format("ALTER TYPE %s.%s ADD v2 int;", keyspace(), type1));
        assertInvalidMessage(table2, format("ALTER TYPE %s.%s ADD v2 int;", keyspace(), type1));
        assertInvalidMessage(table3, format("ALTER TYPE %s.%s ADD v2 int;", keyspace(), type1));
    }

    @Test
    public void testAlterDropCompactStorageDisabled() throws Throwable
    {
        DatabaseDescriptor.setEnableDropCompactStorage(false);

        createTable("CREATE TABLE %s (k text, i int, PRIMARY KEY (k, i)) WITH COMPACT STORAGE");

        assertInvalidMessage("DROP COMPACT STORAGE is disabled. Enable in cassandra.yaml to use.", "ALTER TABLE %s DROP COMPACT STORAGE");
    }
}
