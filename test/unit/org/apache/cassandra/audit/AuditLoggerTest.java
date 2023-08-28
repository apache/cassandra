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
package org.apache.cassandra.audit;

import org.junit.After;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.SyntaxError;
import net.openhft.chronicle.queue.RollCycles;
import org.apache.cassandra.auth.AuthEvents;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryEvents;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * AuditLoggerTest is responsible for covering the test cases for Audit Logging CASSANDRA-12151 functionality.
 * Authenticated user audit (LOGIN) tests are segregated from unauthenticated user audit tests.
 */
public class AuditLoggerTest extends CQLTester
{
    @BeforeClass
    public static void setUp() throws IOException
    {
        AuditLogOptions options = getBaseAuditLogOptions();
        options.enabled = true;
        options.logger = new ParameterizedClass("InMemoryAuditLogger", null);
        DatabaseDescriptor.setAuditLoggingOptions(options);
        requireNetwork();
    }

    @Before
    public void beforeTestMethod() throws IOException
    {
        AuditLogOptions options = new AuditLogOptions();
        enableAuditLogOptions(options);
    }

    @After
    public void afterTestMethod()
    {
        disableAuditLogOptions();
    }

    /**
       Create a new AuditLogOptions instance with the log dir set appropriately to a temp dir for unit testing.
    */
    private static AuditLogOptions getBaseAuditLogOptions() throws IOException {
        AuditLogOptions options = new AuditLogOptions();

        // Ensure that we create a new audit log directory to separate outputs
        Path tmpDir = Files.createTempDirectory("AuditLoggerTest");
        options.audit_logs_dir = tmpDir.toString();

        return options;
    }

    private void enableAuditLogOptions(AuditLogOptions options) throws IOException
    {
        String loggerName = "InMemoryAuditLogger";
        String includedKeyspaces = options.included_keyspaces;
        String excludedKeyspaces = options.excluded_keyspaces;
        String includedCategories = options.included_categories;
        String excludedCategories = options.excluded_categories;
        String includedUsers = options.included_users;
        String excludedUsers = options.excluded_users;

        StorageService.instance.enableAuditLog(loggerName, null, includedKeyspaces, excludedKeyspaces, includedCategories, excludedCategories, includedUsers, excludedUsers);
    }

    private void disableAuditLogOptions()
    {
        StorageService.instance.disableAuditLog();
    }

    @Test
    public void testAuditLogFilters() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        AuditLogOptions options = getBaseAuditLogOptions();
        options.excluded_keyspaces += ',' + KEYSPACE;
        enableAuditLogOptions(options);

        String cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
        ResultSet rs = executeAndAssertNoAuditLog(cql, 1);
        assertEquals(1, rs.all().size());

        options = getBaseAuditLogOptions();
        options.included_keyspaces = KEYSPACE;
        enableAuditLogOptions(options);

        cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
        rs = executeAndAssertWithPrepare(cql, AuditLogEntryType.SELECT, 1);
        assertEquals(1, rs.all().size());

        options = getBaseAuditLogOptions();
        options.included_keyspaces = KEYSPACE;
        options.excluded_keyspaces += ',' + KEYSPACE;
        enableAuditLogOptions(options);

        cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
        rs = executeAndAssertNoAuditLog(cql, 1);
        assertEquals(1, rs.all().size());

        options = getBaseAuditLogOptions();
        enableAuditLogOptions(options);

        cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
        rs = executeAndAssertWithPrepare(cql, AuditLogEntryType.SELECT, 1);
        assertEquals(1, rs.all().size());
    }

    @Test
    public void testAuditLogFiltersTransitions() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        AuditLogOptions options = getBaseAuditLogOptions();
        options.excluded_keyspaces += ',' + KEYSPACE;
        enableAuditLogOptions(options);

        String cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
        ResultSet rs = executeAndAssertNoAuditLog(cql, 1);
        assertEquals(1, rs.all().size());
        assertEquals(1, QueryEvents.instance.listenerCount());
        assertEquals(1, AuthEvents.instance.listenerCount());
        disableAuditLogOptions();
        assertEquals(0, QueryEvents.instance.listenerCount());
        assertEquals(0, AuthEvents.instance.listenerCount());

        cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
        rs = executeAndAssertDisableAuditLog(cql, 1);
        assertEquals(1, rs.all().size());

        options = getBaseAuditLogOptions();
        options.included_keyspaces = KEYSPACE;
        options.excluded_keyspaces += ',' + KEYSPACE;
        enableAuditLogOptions(options);

        cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
        rs = executeAndAssertNoAuditLog(cql, 1);
        assertEquals(1, rs.all().size());

        disableAuditLogOptions();

        cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
        rs = executeAndAssertDisableAuditLog(cql, 1);
        assertEquals(1, rs.all().size());
    }

    @Test
    public void testAuditLogExceptions() throws IOException
    {
        AuditLogOptions options = getBaseAuditLogOptions();
        options.excluded_keyspaces += ',' + KEYSPACE;
        enableAuditLogOptions(options);
        Assert.assertTrue(AuditLogManager.instance.isEnabled());
    }

    @Test
    public void testAuditLogFilterIncludeExclude() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        String tbl1 = currentTable();
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        AuditLogOptions options = getBaseAuditLogOptions();
        options.excluded_categories = "QUERY";
        options.included_categories = "QUERY,DML,PREPARE";
        enableAuditLogOptions(options);

        //QUERY - Should be filtered, part of excluded categories,
        String cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = 1";
        Session session = sessionNet();
        ResultSet rs = session.execute(cql);

        assertEquals(0, ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.size());
        assertEquals(1, rs.all().size());

        //DML - Should not be filtered, part of included categories
        cql = "INSERT INTO " + KEYSPACE + '.' + currentTable() + " (id, v1, v2) VALUES (?, ?, ?)";
        executeAndAssertWithPrepare(cql, AuditLogEntryType.UPDATE, 1, "insert_audit", "test");

        //DDL - Should be filtered, not part of included categories
        cql = "ALTER TABLE  " + KEYSPACE + '.' + currentTable() + " ADD v3 text";
        session = sessionNet();
        rs = session.execute(cql);
        assertEquals(0, ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.size());
    }

    @Test
    public void testCqlSelectAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        String cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
        ResultSet rs = executeAndAssertWithPrepare(cql, AuditLogEntryType.SELECT, 1);

        assertEquals(1, rs.all().size());
    }

    @Test
    public void testCqlInsertAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

        String cql = "INSERT INTO " + KEYSPACE + '.' + currentTable() + "  (id, v1, v2) VALUES (?, ?, ?)";
        executeAndAssertWithPrepare(cql, AuditLogEntryType.UPDATE, 1, "insert_audit", "test");
    }

    @Test
    public void testCqlUpdateAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        String cql = "UPDATE " + KEYSPACE + '.' + currentTable() + "  SET v1 = 'ApacheCassandra' WHERE id = 1";
        executeAndAssert(cql, AuditLogEntryType.UPDATE);

        cql = "UPDATE " + KEYSPACE + '.' + currentTable() + "  SET v1 = ? WHERE id = ?";
        executeAndAssertWithPrepare(cql, AuditLogEntryType.UPDATE, "AuditingTest", 2);
    }

    @Test
    public void testCqlDeleteAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        String cql = "DELETE FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
        executeAndAssertWithPrepare(cql, AuditLogEntryType.DELETE, 1);
    }

    @Test
    public void testCqlTruncateAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        String cql = "TRUNCATE TABLE  " + KEYSPACE + '.' + currentTable();
        executeAndAssertWithPrepare(cql, AuditLogEntryType.TRUNCATE);
    }

    @Test
    public void testCqlBatchAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

        Session session = sessionNet();

        BatchStatement batchStatement = new BatchStatement();

        String cqlInsert = "INSERT INTO " + KEYSPACE + "." + currentTable() + " (id, v1, v2) VALUES (?, ?, ?)";
        PreparedStatement prep = session.prepare(cqlInsert);
        AuditLogEntry logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert, AuditLogEntryType.PREPARE_STATEMENT, logEntry, false);

        batchStatement.add(prep.bind(1, "Apapche", "Cassandra"));
        batchStatement.add(prep.bind(2, "Apapche1", "Cassandra1"));

        String cqlUpdate = "UPDATE " + KEYSPACE + "." + currentTable() + " SET v1 = ? WHERE id = ?";
        prep = session.prepare(cqlUpdate);
        logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cqlUpdate, AuditLogEntryType.PREPARE_STATEMENT, logEntry, false);

        batchStatement.add(prep.bind("Apache Cassandra", 1));

        String cqlDelete = "DELETE FROM " + KEYSPACE + "." + currentTable() + " WHERE id = ?";
        prep = session.prepare(cqlDelete);
        logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cqlDelete, AuditLogEntryType.PREPARE_STATEMENT, logEntry, false);

        batchStatement.add(prep.bind(1));

        ResultSet rs = session.execute(batchStatement);

        assertEquals(5, ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.size());
        logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();

        assertEquals(AuditLogEntryType.BATCH, logEntry.getType());
        assertTrue(logEntry.getOperation().contains("BatchId"));
        assertNotEquals(0, logEntry.getTimestamp());

        logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert, AuditLogEntryType.UPDATE, logEntry, false);

        logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert, AuditLogEntryType.UPDATE, logEntry, false);

        logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cqlUpdate, AuditLogEntryType.UPDATE, logEntry, false);

        logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cqlDelete, AuditLogEntryType.DELETE, logEntry, false);

        int size = rs.all().size();

        assertEquals(0, size);
    }

    @Test
    public void testCqlBatch_MultipleTablesAuditing()
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        String table1 = currentTable();

        Session session = sessionNet();

        BatchStatement batchStatement = new BatchStatement();

        String cqlInsert1 = "INSERT INTO " + KEYSPACE + "." + table1 + " (id, v1, v2) VALUES (?, ?, ?)";
        PreparedStatement prep = session.prepare(cqlInsert1);
        AuditLogEntry logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert1, AuditLogEntryType.PREPARE_STATEMENT, logEntry, false);

        batchStatement.add(prep.bind(1, "Apapche", "Cassandra"));

        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        String table2 = currentTable();

        String cqlInsert2 = "INSERT INTO " + KEYSPACE + "." + table2 + " (id, v1, v2) VALUES (?, ?, ?)";
        prep = session.prepare(cqlInsert2);
        logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert2, AuditLogEntryType.PREPARE_STATEMENT, logEntry, false);

        batchStatement.add(prep.bind(1, "Apapche", "Cassandra"));

        createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String ks2 = currentKeyspace();

        createTable(ks2, "CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        String table3 = currentTable();

        String cqlInsert3 = "INSERT INTO " + ks2 + "." + table3 + " (id, v1, v2) VALUES (?, ?, ?)";
        prep = session.prepare(cqlInsert3);
        logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert3, AuditLogEntryType.PREPARE_STATEMENT, logEntry, false, ks2);

        batchStatement.add(prep.bind(1, "Apapche", "Cassandra"));

        ResultSet rs = session.execute(batchStatement);

        assertEquals(4, ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.size());
        logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();

        logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert1, table1, AuditLogEntryType.UPDATE, logEntry, false, KEYSPACE);

        logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert2, table2, AuditLogEntryType.UPDATE, logEntry, false, KEYSPACE);

        logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert3, table3, AuditLogEntryType.UPDATE, logEntry, false, ks2);

        int size = rs.all().size();

        assertEquals(0, size);
    }

    @Test
    public void testCqlKeyspaceAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

        String cql = "CREATE KEYSPACE " + createKeyspaceName() + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2}  ";
        executeAndAssert(cql, AuditLogEntryType.CREATE_KEYSPACE, true, currentKeyspace());

        cql = "CREATE KEYSPACE IF NOT EXISTS " + createKeyspaceName() + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2}  ";
        executeAndAssert(cql, AuditLogEntryType.CREATE_KEYSPACE, true, currentKeyspace());

        cql = "ALTER KEYSPACE " + currentKeyspace() + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2}  ";
        executeAndAssert(cql, AuditLogEntryType.ALTER_KEYSPACE, true, currentKeyspace());

        cql = "DROP KEYSPACE " + currentKeyspace();
        executeAndAssert(cql, AuditLogEntryType.DROP_KEYSPACE, true, currentKeyspace());
    }

    @Test
    public void testCqlTableAuditing() throws Throwable
    {
        String cql = "CREATE TABLE " + KEYSPACE + "." + createTableName() + " (id int primary key, v1 text, v2 text)";
        executeAndAssert(cql, AuditLogEntryType.CREATE_TABLE);

        cql = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + createTableName() + " (id int primary key, v1 text, v2 text)";
        executeAndAssert(cql, AuditLogEntryType.CREATE_TABLE);

        cql = "ALTER TABLE " + KEYSPACE + "." + currentTable() + " ADD v3 text";
        executeAndAssert(cql, AuditLogEntryType.ALTER_TABLE);

        cql = "DROP TABLE " + KEYSPACE + "." + currentTable();
        executeAndAssert(cql, AuditLogEntryType.DROP_TABLE);
    }

    @Test
    public void testCqlMVAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        String tblName = currentTable();
        String cql = "CREATE MATERIALIZED VIEW " + KEYSPACE + "." + createTableName() + " AS SELECT id,v1 FROM " + KEYSPACE + "." + tblName + " WHERE id IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY ( id, v1 ) ";
        executeAndAssert(cql, AuditLogEntryType.CREATE_VIEW);

        cql = "CREATE MATERIALIZED VIEW IF NOT EXISTS " + KEYSPACE + "." + currentTable() + " AS SELECT id,v1 FROM " + KEYSPACE + "." + tblName + " WHERE id IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY ( id, v1 ) ";
        executeAndAssert(cql, AuditLogEntryType.CREATE_VIEW);

        cql = "ALTER MATERIALIZED VIEW " + KEYSPACE + "." + currentTable() + " WITH caching = {  'keys' : 'NONE' };";
        executeAndAssert(cql, AuditLogEntryType.ALTER_VIEW);

        cql = "DROP MATERIALIZED VIEW " + KEYSPACE + "." + currentTable();
        executeAndAssert(cql, AuditLogEntryType.DROP_VIEW);
    }

    @Test
    public void testCqlTypeAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

        String tblName = createTableName();

        String cql = "CREATE TYPE " + KEYSPACE + "." + tblName + " (id int, v1 text, v2 text)";
        executeAndAssert(cql, AuditLogEntryType.CREATE_TYPE);

        cql = "CREATE TYPE IF NOT EXISTS " + KEYSPACE + "." + tblName + " (id int, v1 text, v2 text)";
        executeAndAssert(cql, AuditLogEntryType.CREATE_TYPE);

        cql = "ALTER TYPE " + KEYSPACE + "." + tblName + " ADD v3 int";
        executeAndAssert(cql, AuditLogEntryType.ALTER_TYPE);

        cql = "ALTER TYPE " + KEYSPACE + "." + tblName + " RENAME v3 TO v4";
        executeAndAssert(cql, AuditLogEntryType.ALTER_TYPE);

        cql = "DROP TYPE " + KEYSPACE + "." + tblName;
        executeAndAssert(cql, AuditLogEntryType.DROP_TYPE);

        cql = "DROP TYPE IF EXISTS " + KEYSPACE + "." + tblName;
        executeAndAssert(cql, AuditLogEntryType.DROP_TYPE);
    }

    @Test
    public void testCqlIndexAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

        String tblName = currentTable();

        String indexName = createTableName();

        String cql = "CREATE INDEX " + indexName + " ON " + KEYSPACE + "." + tblName + " (v1)";
        executeAndAssert(cql, AuditLogEntryType.CREATE_INDEX);

        cql = "DROP INDEX " + KEYSPACE + "." + indexName;
        executeAndAssert(cql, AuditLogEntryType.DROP_INDEX);
    }

    @Test
    public void testCqlFunctionAuditing() throws Throwable
    {
        String tblName = createTableName();

        String cql = "CREATE FUNCTION IF NOT EXISTS  " + KEYSPACE + "." + tblName + " (column TEXT,num int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS $$ return column.substring(0,num); $$";
        executeAndAssert(cql, AuditLogEntryType.CREATE_FUNCTION);

        cql = "DROP FUNCTION " + KEYSPACE + "." + tblName;
        executeAndAssert(cql, AuditLogEntryType.DROP_FUNCTION);
    }

    @Test
    public void testCqlTriggerAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

        String tblName = currentTable();
        String triggerName = createTableName();

        String cql = "DROP TRIGGER IF EXISTS " + triggerName + " ON " + KEYSPACE + "." + tblName;
        executeAndAssert(cql, AuditLogEntryType.DROP_TRIGGER);
    }

    @Test
    public void testCqlAggregateAuditing() throws Throwable
    {
        String aggName = createTableName();
        String cql = "DROP AGGREGATE IF EXISTS " + KEYSPACE + "." + aggName;
        executeAndAssert(cql, AuditLogEntryType.DROP_AGGREGATE);
    }

    @Test
    public void testCqlQuerySyntaxError()
    {
        String cql = "INSERT INTO " + KEYSPACE + '.' + currentTable() + "1 (id, v1, v2) VALUES (1, 'insert_audit, 'test')";
        try
        {
            createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
            Session session = sessionNet();
            ResultSet rs = session.execute(cql);
            Assert.fail("should not succeed");
        }
        catch (SyntaxError e)
        {
            // nop
        }

        AuditLogEntry logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(logEntry, cql);
        assertEquals(0, ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.size());
    }

    @Test
    public void testCqlSelectQuerySyntaxError()
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        String cql = "SELECT * FROM " + KEYSPACE + '.' + currentTable() + " LIMIT 2w";

        try
        {
            Session session = sessionNet();
            ResultSet rs = session.execute(cql);
            Assert.fail("should not succeed");
        }
        catch (SyntaxError e)
        {
            // nop
        }

        AuditLogEntry logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(logEntry, cql);
        assertEquals(0, ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.size());
    }

    @Test
    public void testCqlPrepareQueryError()
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        String cql = "INSERT INTO " + KEYSPACE + '.' + currentTable() + " (id, v1, v2) VALUES (?,?,?)";
        try
        {
            Session session = sessionNet();

            PreparedStatement pstmt = session.prepare(cql);
            AuditLogEntry logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
            assertLogEntry(cql, AuditLogEntryType.PREPARE_STATEMENT, logEntry, false);

            dropTable("DROP TABLE %s");
            ResultSet rs = session.execute(pstmt.bind(1, "insert_audit", "test"));
            Assert.fail("should not succeed");
        }
        catch (NoHostAvailableException e)
        {
            // nop
        }

        AuditLogEntry logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(logEntry, null);
        logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(logEntry, cql);
        assertEquals(0, ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.size());
    }

    @Test
    public void testCqlPrepareQuerySyntaxError()
    {
        String cql = "INSERT INTO " + KEYSPACE + '.' + "foo" + "(id, v1, v2) VALES (?,?,?)";
        try
        {
            createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
            Session session = sessionNet();
            PreparedStatement pstmt = session.prepare(cql);
            ResultSet rs = session.execute(pstmt.bind(1, "insert_audit", "test"));
            Assert.fail("should not succeed");
        }
        catch (SyntaxError e)
        {
            // nop
        }
        AuditLogEntry logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(logEntry, cql);
        assertEquals(0, ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.size());
    }

    @Test
    public void testIncludeSystemKeyspaces() throws Throwable
    {
        AuditLogOptions options = getBaseAuditLogOptions();
        options.included_categories = "QUERY,DML,PREPARE";
        options.excluded_keyspaces = "system_schema,system_virtual_schema";
        enableAuditLogOptions(options);

        Session session = sessionNet();
        String cql = "SELECT * FROM system.local limit 2";
        ResultSet rs = session.execute(cql);

        assertEquals (1,((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.size());
        AuditLogEntry logEntry = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cql, "local",AuditLogEntryType.SELECT,logEntry,false, "system");
        assertEquals (0,((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.size());
    }

    @Test
    public void testExcludeSystemKeyspaces() throws Throwable
    {
        AuditLogOptions options = getBaseAuditLogOptions();
        options.included_categories = "QUERY,DML,PREPARE";
        options.excluded_keyspaces = "system,system_schema,system_virtual_schema";
        enableAuditLogOptions(options);

        Session session = sessionNet();
        String cql = "SELECT * FROM system.local limit 2";
        ResultSet rs = session.execute(cql);

        assertEquals (0,((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.size());
    }

    @Test
    public void testEnableDisable() throws IOException
    {
        disableAuditLogOptions();
        assertEquals(0, QueryEvents.instance.listenerCount());
        assertEquals(0, AuthEvents.instance.listenerCount());
        enableAuditLogOptions(getBaseAuditLogOptions());
        assertEquals(1, QueryEvents.instance.listenerCount());
        assertEquals(1, AuthEvents.instance.listenerCount());

        Path p = Files.createTempDirectory("fql");
        StorageService.instance.enableFullQueryLogger(p.toString(), RollCycles.HOURLY.toString(), false, 1000, 1000, null, 0);
        assertEquals(2, QueryEvents.instance.listenerCount());
        assertEquals(1, AuthEvents.instance.listenerCount()); // fql not listening to auth events
        StorageService.instance.resetFullQueryLogger();
        assertEquals(1, QueryEvents.instance.listenerCount());
        assertEquals(1, AuthEvents.instance.listenerCount());
        disableAuditLogOptions();

        assertEquals(0, QueryEvents.instance.listenerCount());
        assertEquals(0, AuthEvents.instance.listenerCount());
    }

    @Test
    public void testConflictingPaths() throws IOException
    {
        disableAuditLogOptions();
        AuditLogOptions options = getBaseAuditLogOptions();
        DatabaseDescriptor.setAuditLoggingOptions(options);
        StorageService.instance.enableAuditLog(null, null, options.included_keyspaces, options.excluded_keyspaces, options.included_categories, options.excluded_categories, options.included_users, options.excluded_users);
        try
        {
            assertEquals(1, QueryEvents.instance.listenerCount());
            assertEquals(1, AuthEvents.instance.listenerCount());
            StorageService.instance.enableFullQueryLogger(options.audit_logs_dir, RollCycles.HOURLY.toString(), false, 1000, 1000, null, 0);
            fail("Conflicting directories - should throw exception");
        }
        catch (IllegalStateException e)
        {
            // ok
        }
        assertEquals(1, QueryEvents.instance.listenerCount());
        assertEquals(1, AuthEvents.instance.listenerCount());
    }


    @Test
    public void testConflictingPathsFQLFirst() throws IOException
    {
        disableAuditLogOptions();
        AuditLogOptions options = getBaseAuditLogOptions();
        DatabaseDescriptor.setAuditLoggingOptions(options);
        StorageService.instance.enableFullQueryLogger(options.audit_logs_dir, RollCycles.HOURLY.toString(), false, 1000, 1000, null, 0);
        try
        {
            assertEquals(1, QueryEvents.instance.listenerCount());
            assertEquals(0, AuthEvents.instance.listenerCount());
            StorageService.instance.enableAuditLog(null, null, options.included_keyspaces, options.excluded_keyspaces, options.included_categories, options.excluded_categories, options.included_users, options.excluded_users);
            fail("Conflicting directories - should throw exception");
        }
        catch (ConfigurationException e)
        {
            // ok
        }
        assertEquals(1, QueryEvents.instance.listenerCount());
        assertEquals(0, AuthEvents.instance.listenerCount());
    }

    @Test
    public void testJMXArchiveCommand() throws IOException
    {
        disableAuditLogOptions();
        AuditLogOptions options = getBaseAuditLogOptions();

        try
        {
            StorageService.instance.enableAuditLog("BinAuditLogger", Collections.emptyMap(), "", "", "", "",
                                                   "", "", 10, true, options.roll_cycle,
                                                   1000L, 1000, "/xyz/not/null");
            fail("not allowed");
        }
        catch (ConfigurationException e)
        {
            assertTrue(e.getMessage().contains("Can't enable audit log archiving via nodetool"));
        }

        options.archive_command = "/xyz/not/null";

        DatabaseDescriptor.setAuditLoggingOptions(options);
        StorageService.instance.enableAuditLog("BinAuditLogger", Collections.emptyMap(), "", "", "", "",
                                               "", "", 10, true, options.roll_cycle,
                                               1000L, 1000, null);
        assertTrue(AuditLogManager.instance.isEnabled());
        assertEquals("/xyz/not/null", AuditLogManager.instance.getAuditLogOptions().archive_command);
    }

    /**
     * Helper methods for Audit Log CQL Testing
     */

    private ResultSet executeAndAssert(String cql, AuditLogEntryType type) throws Throwable
    {
        return executeAndAssert(cql, type, false, KEYSPACE);
    }

    private ResultSet executeAndAssert(String cql, AuditLogEntryType type, boolean isTableNull, String keyspace) throws Throwable
    {
        Session session = sessionNet();

        ResultSet rs = session.execute(cql);

        AuditLogEntry logEntry1 = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cql, type, logEntry1, isTableNull, keyspace);

        assertEquals(0, ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.size());
        return rs;
    }

    private ResultSet executeAndAssertWithPrepare(String cql, AuditLogEntryType exceuteType, Object... bindValues) throws Throwable
    {
        return executeAndAssertWithPrepare(cql, exceuteType, false, bindValues);
    }

    private ResultSet executeAndAssertWithPrepare(String cql, AuditLogEntryType executeType, boolean isTableNull, Object... bindValues) throws Throwable
    {
        Session session = sessionNet();

        PreparedStatement pstmt = session.prepare(cql);
        ResultSet rs = session.execute(pstmt.bind(bindValues));

        AuditLogEntry logEntry1 = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cql, AuditLogEntryType.PREPARE_STATEMENT, logEntry1, isTableNull);

        AuditLogEntry logEntry2 = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.poll();
        assertLogEntry(cql, executeType, logEntry2, isTableNull);

        assertEquals(0, ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.size());
        return rs;
    }

    private ResultSet executeAndAssertNoAuditLog(String cql, Object... bindValues)
    {
        Session session = sessionNet();

        PreparedStatement pstmt = session.prepare(cql);
        ResultSet rs = session.execute(pstmt.bind(bindValues));

        assertEquals(0, ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue.size());
        return rs;
    }

    private ResultSet executeAndAssertDisableAuditLog(String cql, Object... bindValues)
    {
        Session session = sessionNet();

        PreparedStatement pstmt = session.prepare(cql);
        ResultSet rs = session.execute(pstmt.bind(bindValues));

        assertThat(AuditLogManager.instance.getLogger(),instanceOf(NoOpAuditLogger.class));
        return rs;
    }

    private void assertLogEntry(String cql, AuditLogEntryType type, AuditLogEntry actual, boolean isTableNull)
    {
        assertLogEntry(cql, type, actual, isTableNull, KEYSPACE);
    }

    private void assertLogEntry(String cql, AuditLogEntryType type, AuditLogEntry actual, boolean isTableNull, String keyspace)
    {
        assertLogEntry(cql, currentTable(), type, actual, isTableNull, keyspace);
    }

    private void assertLogEntry(String cql, String table, AuditLogEntryType type, AuditLogEntry actual, boolean isTableNull, String keyspace)
    {
        assertEquals(keyspace, actual.getKeyspace());
        if (!isTableNull)
        {
            assertEquals(table, actual.getScope());
        }
        assertEquals(type, actual.getType());
        assertEquals(cql, actual.getOperation());
        assertNotEquals(0,actual.getTimestamp());
    }

    private void assertLogEntry(AuditLogEntry logEntry, String cql)
    {
        assertNull(logEntry.getKeyspace());
        assertNull(logEntry.getScope());
        assertNotEquals(0,logEntry.getTimestamp());
        assertEquals(AuditLogEntryType.REQUEST_FAILURE, logEntry.getType());
        if (null != cql && !cql.isEmpty())
        {
            assertThat(logEntry.getOperation(), containsString(cql));
        }
    }
}
