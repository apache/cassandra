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
package org.apache.cassandra.cql3.statements;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.junit.Assert.*;

public class DropDataSourceStatementTest extends CQLTester
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final String TEST_SERVICE = "cdc";
    private static final String TEST_TABLE = "test_table";
    private static final String TEST_SINK = "test_sink";
    private static final String KAFKA_URI = "kafka://localhost:9092";

    // Use format strings with %s placeholder for keyspace to be filled in during test execution
    private String dropQuery;
    private String dropIfExistsQuery;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        // Create test keyspace and table using CQLTester's auto-generated keyspace
        createKeyspace("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

        // Initialize query strings with the current keyspace
        String testKeyspace = currentKeyspace();
        dropQuery = String.format("DROP DATA_SOURCE %s ON TABLE %s.%s",
                                  TEST_SERVICE, testKeyspace, TEST_TABLE);
        dropIfExistsQuery = String.format("DROP DATA_SOURCE IF EXISTS %s ON TABLE %s.%s",
                                          TEST_SERVICE, testKeyspace, TEST_TABLE);

        execute("CREATE TABLE IF NOT EXISTS " + currentKeyspace() + "." + TEST_TABLE + " (id int PRIMARY KEY, data text)");

        // Clean up any existing test data sources
        execute("DELETE FROM system_distributed.service_configs WHERE service = ? AND type = ?",
                TEST_SERVICE, "DATA_SOURCE");

        // Clean up any existing test sinks
        execute("DELETE FROM system_distributed.service_configs WHERE service = ? AND type = ?",
                TEST_SINK, "DATA_SINK");
    }

    @Test
    public void testDropDataSourceStatementParsing()
    {
        CQLStatement.Raw statement = QueryProcessor.parseStatement(dropQuery);
        assertTrue(statement instanceof DropDataSourceStatement.Raw);

        DropDataSourceStatement.Raw rawStatement = (DropDataSourceStatement.Raw) statement;
        DropDataSourceStatement prepared = rawStatement.prepare(ClientState.forInternalCalls());

        assertTrue(prepared.toString().contains(TEST_SERVICE));
        assertTrue(prepared.toString().contains(currentKeyspace()));
        assertTrue(prepared.toString().contains(TEST_TABLE));
        assertFalse(prepared.toString().contains("IF EXISTS"));
    }

    @Test
    public void testDropDataSourceStatementParsingWithIfExists()
    {
        CQLStatement.Raw statement = QueryProcessor.parseStatement(dropIfExistsQuery);
        assertTrue(statement instanceof DropDataSourceStatement.Raw);

        DropDataSourceStatement.Raw rawStatement = (DropDataSourceStatement.Raw) statement;
        DropDataSourceStatement prepared = rawStatement.prepare(ClientState.forInternalCalls());

        assertTrue(prepared.toString().contains(TEST_SERVICE));
        assertTrue(prepared.toString().contains(currentKeyspace()));
        assertTrue(prepared.toString().contains(TEST_TABLE));
    }

    @Test
    public void testValidConstruction()
    {
        String keyspace = currentKeyspace();
        DropDataSourceStatement statement = new DropDataSourceStatement(keyspace, TEST_TABLE, TEST_SERVICE, false);
        assertNotNull(statement);
        assertTrue(statement.toString().contains(TEST_SERVICE));
        assertTrue(statement.toString().contains(keyspace));
        assertTrue(statement.toString().contains(TEST_TABLE));
    }

    @Test
    public void testConstructionWithIfExists()
    {
        DropDataSourceStatement statement = new DropDataSourceStatement(currentKeyspace(), TEST_TABLE, TEST_SERVICE, true);
        assertNotNull(statement);
    }

    @Test
    public void testValidateWithExistingDataSource()
    {
        // Create prerequisites (sink and data source)
        createTestSink();
        createTestDataSource();

        DropDataSourceStatement statement = new DropDataSourceStatement(currentKeyspace(), TEST_TABLE, TEST_SERVICE, false);
        ClientState clientState = ClientState.forInternalCalls();

        // Should not throw exception
        statement.validate(clientState);
    }

    @Test
    public void testValidateWithNonExistentDataSourceWithIfExists()
    {
        DropDataSourceStatement statement = new DropDataSourceStatement("nonexistent_ks", "nonexistent_table",
                                                                        "nonexistent_service", true);
        ClientState clientState = ClientState.forInternalCalls();

        // Should not throw exception with IF EXISTS
        statement.validate(clientState);
    }

    @Test(expected = InvalidRequestException.class)
    public void testValidateWithNullKeyspace()
    {
        DropDataSourceStatement statement = new DropDataSourceStatement(null, TEST_TABLE, TEST_SERVICE, false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
    }

    @Test(expected = InvalidRequestException.class)
    public void testValidateWithEmptyKeyspace()
    {
        DropDataSourceStatement statement = new DropDataSourceStatement("", TEST_TABLE, TEST_SERVICE, false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
    }

    @Test(expected = InvalidRequestException.class)
    public void testValidateWithNullTableName()
    {
        DropDataSourceStatement statement = new DropDataSourceStatement(currentKeyspace(), null, TEST_SERVICE, false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
    }

    @Test(expected = InvalidRequestException.class)
    public void testValidateWithEmptyTableName()
    {
        DropDataSourceStatement statement = new DropDataSourceStatement(currentKeyspace(), "", TEST_SERVICE, false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
    }

    @Test(expected = InvalidRequestException.class)
    public void testValidateWithNullServiceName()
    {
        DropDataSourceStatement statement = new DropDataSourceStatement(currentKeyspace(), TEST_TABLE, null, false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
    }

    @Test(expected = InvalidRequestException.class)
    public void testValidateWithEmptyServiceName()
    {
        DropDataSourceStatement statement = new DropDataSourceStatement(currentKeyspace(), TEST_TABLE, "", false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
    }

    @Test
    public void testValidateBasic()
    {
        // validate() only checks for null/empty parameters, not existence
        DropDataSourceStatement statement = new DropDataSourceStatement(currentKeyspace(), TEST_TABLE, TEST_SERVICE, false);
        ClientState clientState = ClientState.forInternalCalls();

        // Should not throw exception for valid (non-null, non-empty) parameters
        statement.validate(clientState);
    }

    @Test
    public void testExecuteDropExistingDataSource() throws Throwable
    {
        // Create prerequisites
        createTestSink();
        createTestDataSource();

        String keyspace = currentKeyspace();

        // Verify data source exists
        assertRows(execute("SELECT * FROM system_distributed.service_configs WHERE service = ? AND type = ?",
                          TEST_SERVICE, "DATA_SOURCE"),
                  row(TEST_SERVICE, "DATA_SOURCE", map("keyspace", keyspace, "service", TEST_SERVICE, "sink", TEST_SINK)));

        // Drop the data source
        DropDataSourceStatement statement = new DropDataSourceStatement(keyspace, TEST_TABLE, TEST_SERVICE, false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
        statement.execute(clientState);

        // Verify data source was deleted
        assertEmpty(execute("SELECT * FROM system_distributed.service_configs WHERE service = ? AND type = ?",
                           TEST_SERVICE, "DATA_SOURCE"));
    }

    @Test
    public void testExecuteDropNonExistentDataSourceWithIfExists() throws Throwable
    {
        // Ensure data source doesn't exist
        assertEmpty(execute("SELECT * FROM system_distributed.service_configs WHERE service = ? AND type = ?",
                           "nonexistent_service", "DATA_SOURCE"));

        // Drop with IF EXISTS should succeed silently
        DropDataSourceStatement statement = new DropDataSourceStatement(currentKeyspace(), TEST_TABLE,
                                                                        "nonexistent_service", true);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
        statement.execute(clientState);
    }

    @Test(expected = InvalidRequestException.class)
    public void testExecuteDropNonExistentDataSourceWithoutIfExists()
    {
        DropDataSourceStatement statement = new DropDataSourceStatement(currentKeyspace(), TEST_TABLE,
                                                                        "nonexistent_service", false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
        statement.execute(clientState);
    }

    @Test
    public void testIfExistsBehavior() throws Throwable
    {
        String serviceName = "if_exists_test_service";
        String keyspace = currentKeyspace();

        // 1. Data source exists, DROP without IF EXISTS should succeed
        createTestSink();
        createTestDataSource(serviceName);

        QueryProcessor.process(String.format("DROP DATA_SOURCE %s ON TABLE %s.%s", serviceName, keyspace, TEST_TABLE),
                              ConsistencyLevel.QUORUM,
                              new QueryState(ClientState.forInternalCalls()),
                              Dispatcher.RequestTime.forImmediateExecution());

        // Verify data source was deleted
        assertEmpty(execute("SELECT * FROM system_distributed.service_configs WHERE service = ? AND type = ?",
                           serviceName, "DATA_SOURCE"));

        // 2. Data source doesn't exist, DROP with IF EXISTS should succeed silently
        QueryProcessor.process(String.format("DROP DATA_SOURCE IF EXISTS %s ON TABLE %s.%s", serviceName, keyspace, TEST_TABLE),
                              ConsistencyLevel.QUORUM,
                              new QueryState(ClientState.forInternalCalls()),
                              Dispatcher.RequestTime.forImmediateExecution());

        // 3. Data source exists, DROP with IF EXISTS should succeed
        createTestDataSource(serviceName);
        QueryProcessor.process(String.format("DROP DATA_SOURCE IF EXISTS %s ON TABLE %s.%s", serviceName, keyspace, TEST_TABLE),
                              ConsistencyLevel.QUORUM,
                              new QueryState(ClientState.forInternalCalls()),
                              Dispatcher.RequestTime.forImmediateExecution());

        // Verify data source was deleted
        assertEmpty(execute("SELECT * FROM system_distributed.service_configs WHERE service = ? AND type = ?",
                           serviceName, "DATA_SOURCE"));
    }

    @Test
    public void testAuditLogContext()
    {
        DropDataSourceStatement statement = new DropDataSourceStatement(currentKeyspace(), TEST_TABLE, TEST_SERVICE, false);
        assertNotNull(statement.getAuditLogContext());
    }

    @Test
    public void testToString()
    {
        DropDataSourceStatement statement = new DropDataSourceStatement("my_keyspace", "my_table", "my_service", false);
        String toString = statement.toString();
        assertTrue(toString.contains("my_keyspace"));
        assertTrue(toString.contains("my_table"));
        assertTrue(toString.contains("my_service"));

        DropDataSourceStatement statementWithIfExists = new DropDataSourceStatement("my_keyspace", "my_table", "my_service", true);
        assertNotNull(statementWithIfExists.toString());
    }

    // Helper methods
    private void createTestSink()
    {
        CreateDataSinkStatement createSinkStatement = new CreateDataSinkStatement(TEST_SINK, KAFKA_URI, false);
        createSinkStatement.execute(ClientState.forInternalCalls());
    }

    private void createTestDataSource()
    {
        createTestDataSource(TEST_SERVICE);
    }

    private void createTestDataSource(String serviceName)
    {
        CreateDataSourceStatement createStatement = new CreateDataSourceStatement(currentKeyspace(), TEST_TABLE,
                                                                                  serviceName, TEST_SINK, false);
        createStatement.execute(ClientState.forInternalCalls());
    }

    private void createTestDataSourceForTable(String tableName)
    {
        CreateDataSourceStatement createStatement = new CreateDataSourceStatement(currentKeyspace(), tableName,
                                                                                  TEST_SERVICE, TEST_SINK, false);
        createStatement.execute(ClientState.forInternalCalls());
    }

    // Helper method to create a map for testing
    private static java.util.Map<String, String> map(String... pairs)
    {
        java.util.Map<String, String> map = new java.util.HashMap<>();
        for (int i = 0; i < pairs.length; i += 2)
        {
            map.put(pairs[i], pairs[i + 1]);
        }
        return map;
    }
}