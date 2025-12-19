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

import java.lang.reflect.Method;
import java.util.Map;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.auth.RoleOptions;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class CreateDataSinkStatementTest extends CQLTester
{
    private static final String TEST_SINK_NAME = "test_sink";
    private static final String KAFKA_URI = "kafka://localhost:9092";
    private static final String KAFKA_URI_WITH_PARAMS = "kafka://localhost:9092?acks=1&retries=5";
    private static final String INVALID_URI = "invalid-uri";

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testValidConstruction()
    {
        CreateDataSinkStatement statement = new CreateDataSinkStatement(TEST_SINK_NAME, KAFKA_URI, false);
        assertNotNull(statement);
        assertEquals("CreateDataSinkStatement (test_sink, kafka://localhost:9092)", statement.toString());
    }

    @Test
    public void testConstructionWithIfNotExists()
    {
        CreateDataSinkStatement statement = new CreateDataSinkStatement(TEST_SINK_NAME, KAFKA_URI, true);
        assertNotNull(statement);
    }

    @Test
    public void testValidateWithValidSinkName()
    {
        CreateDataSinkStatement statement = new CreateDataSinkStatement(TEST_SINK_NAME, KAFKA_URI, false);
        ClientState clientState = ClientState.forInternalCalls();

        // Should not throw exception
        statement.validate(clientState);
    }

    @Test(expected = InvalidRequestException.class)
    public void testValidateWithNullSinkName()
    {
        CreateDataSinkStatement statement = new CreateDataSinkStatement(null, KAFKA_URI, false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
    }

    @Test(expected = InvalidRequestException.class)
    public void testValidateWithEmptySinkName()
    {
        CreateDataSinkStatement statement = new CreateDataSinkStatement("", KAFKA_URI, false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
    }

    @Test(expected = InvalidRequestException.class)
    public void testValidateWithNullUri()
    {
        CreateDataSinkStatement statement = new CreateDataSinkStatement(TEST_SINK_NAME, null, false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
    }

    @Test(expected = InvalidRequestException.class)
    public void testValidateWithEmptyUri()
    {
        CreateDataSinkStatement statement = new CreateDataSinkStatement(TEST_SINK_NAME, "", false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
    }

    @Test(expected = InvalidRequestException.class)
    public void testValidateWithInvalidUriFormat()
    {
        CreateDataSinkStatement statement = new CreateDataSinkStatement(TEST_SINK_NAME, INVALID_URI, false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
    }

    @Test(expected = InvalidRequestException.class)
    public void testValidateWithUriMissingProtocol()
    {
        CreateDataSinkStatement statement = new CreateDataSinkStatement(TEST_SINK_NAME, "localhost:9092", false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
    }

    @Test
    public void testValidateWithValidKafkaUri()
    {
        CreateDataSinkStatement statement = new CreateDataSinkStatement(TEST_SINK_NAME, KAFKA_URI, false);
        ClientState clientState = ClientState.forInternalCalls();

        // Should not throw exception
        statement.validate(clientState);
    }

    @Test
    public void testValidateWithKafkaUriAndParams()
    {
        CreateDataSinkStatement statement = new CreateDataSinkStatement(TEST_SINK_NAME, KAFKA_URI_WITH_PARAMS, false);
        ClientState clientState = ClientState.forInternalCalls();

        // Should not throw exception
        statement.validate(clientState);
    }

    @Test
    public void testParseUriToConfigKafkaBasic() throws Exception
    {
        CreateDataSinkStatement statement = new CreateDataSinkStatement(TEST_SINK_NAME, KAFKA_URI, false);

        // Use reflection to access private method
        Method parseMethod = CreateDataSinkStatement.class.getDeclaredMethod("parseUriToConfig", String.class);
        parseMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, String> config = (Map<String, String>) parseMethod.invoke(statement, KAFKA_URI);

        assertNotNull(config);
        assertEquals("localhost:9092", config.get("bootstrap.servers"));
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", config.get("key.serializer"));
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", config.get("value.serializer"));
        assertEquals("all", config.get("acks"));
        assertEquals("3", config.get("retries"));
        assertEquals("snappy", config.get("compression.type"));
        assertEquals("16384", config.get("batch.size"));
        assertEquals("kafka", config.get("protocol"));
        assertEquals("kafka", config.get("sink_type"));
    }

    @Test
    public void testParseUriToConfigKafkaWithPort() throws Exception
    {
        String uriWithPort = "kafka://localhost:9093";
        CreateDataSinkStatement statement = new CreateDataSinkStatement(TEST_SINK_NAME, uriWithPort, false);

        Method parseMethod = CreateDataSinkStatement.class.getDeclaredMethod("parseUriToConfig", String.class);
        parseMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, String> config = (Map<String, String>) parseMethod.invoke(statement, uriWithPort);

        assertEquals("localhost:9093", config.get("bootstrap.servers"));
    }

    @Test
    public void testParseUriToConfigKafkaWithoutPort() throws Exception
    {
        String uriWithoutPort = "kafka://localhost";
        CreateDataSinkStatement statement = new CreateDataSinkStatement(TEST_SINK_NAME, uriWithoutPort, false);

        Method parseMethod = CreateDataSinkStatement.class.getDeclaredMethod("parseUriToConfig", String.class);
        parseMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, String> config = (Map<String, String>) parseMethod.invoke(statement, uriWithoutPort);

        assertEquals("localhost:9092", config.get("bootstrap.servers")); // Should default to 9092
    }

    @Test
    public void testParseUriToConfigKafkaWithParams() throws Exception
    {
        CreateDataSinkStatement statement = new CreateDataSinkStatement(TEST_SINK_NAME, KAFKA_URI_WITH_PARAMS, false);

        Method parseMethod = CreateDataSinkStatement.class.getDeclaredMethod("parseUriToConfig", String.class);
        parseMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, String> config = (Map<String, String>) parseMethod.invoke(statement, KAFKA_URI_WITH_PARAMS);

        assertNotNull(config);
        assertEquals("localhost:9092", config.get("bootstrap.servers"));
        assertEquals("1", config.get("acks")); // Overridden from default "all"
        assertEquals("5", config.get("retries")); // Overridden from default "3"
    }

    @Test
    public void testParseUriToConfigWithComplexParams() throws Exception
    {
        String complexUri = "kafka://kafka.example.com:9094?acks=all&retries=10&compression.type=gzip&batch.size=32768&custom.param=value";
        CreateDataSinkStatement statement = new CreateDataSinkStatement(TEST_SINK_NAME, complexUri, false);

        Method parseMethod = CreateDataSinkStatement.class.getDeclaredMethod("parseUriToConfig", String.class);
        parseMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, String> config = (Map<String, String>) parseMethod.invoke(statement, complexUri);

        assertEquals("kafka.example.com:9094", config.get("bootstrap.servers"));
        assertEquals("all", config.get("acks"));
        assertEquals("10", config.get("retries"));
        assertEquals("gzip", config.get("compression.type"));
        assertEquals("32768", config.get("batch.size"));
        assertEquals("value", config.get("custom.param"));
    }

    @Test(expected = InvalidRequestException.class)
    public void testParseUriToConfigWithInvalidUri() throws Exception
    {
        CreateDataSinkStatement stmt = new CreateDataSinkStatement(TEST_SINK_NAME, "invalid-uri", false);
        stmt.validateUri("invalid-uri");
    }

    @Test
    public void testRawStatementPreparation()
    {
        CreateDataSinkStatement.Raw raw = new CreateDataSinkStatement.Raw(
            new org.apache.cassandra.cql3.ColumnIdentifier("test_sink", true),
            "'kafka://localhost:9092'", // URI with quotes as it would come from parser
            false
        );

        ClientState clientState = ClientState.forInternalCalls();
        CreateDataSinkStatement prepared = raw.prepare(clientState);

        assertNotNull(prepared);
        assertEquals("CreateDataSinkStatement (test_sink, kafka://localhost:9092)", prepared.toString());
    }

    @Test
    public void testRawStatementPreparationWithIfNotExists()
    {
        CreateDataSinkStatement.Raw raw = new CreateDataSinkStatement.Raw(
            new org.apache.cassandra.cql3.ColumnIdentifier("test_sink", true),
            "'kafka://localhost:9092'",
            true
        );

        ClientState clientState = ClientState.forInternalCalls();
        CreateDataSinkStatement prepared = raw.prepare(clientState);

        assertNotNull(prepared);
    }

    @Test
    public void testRawStatementPreparationStripsQuotes()
    {
        CreateDataSinkStatement.Raw raw = new CreateDataSinkStatement.Raw(
            new org.apache.cassandra.cql3.ColumnIdentifier("test_sink", true),
            "'kafka://localhost:9092'", // URI with quotes
            false
        );

        ClientState clientState = ClientState.forInternalCalls();
        CreateDataSinkStatement prepared = raw.prepare(clientState);

        // Should strip quotes from URI
        assertEquals("CreateDataSinkStatement (test_sink, kafka://localhost:9092)", prepared.toString());
    }

    @Test
    public void testAuditLogContext()
    {
        CreateDataSinkStatement statement = new CreateDataSinkStatement(TEST_SINK_NAME, KAFKA_URI, false);

        assertNotNull(statement.getAuditLogContext());
        // Note: Currently uses CREATE_TRIGGER type - this test documents the current behavior
        // TODO: Update when CREATE_DATA_SINK audit type is added
    }

    @Test
    public void testToString()
    {
        CreateDataSinkStatement statement = new CreateDataSinkStatement("my_sink", "kafka://host:9092", false);
        assertEquals("CreateDataSinkStatement (my_sink, kafka://host:9092)", statement.toString());
    }

    @Test
    public void testExecuteNewSink() throws Exception
    {
        // Clean up any existing sink
        execute("DELETE FROM system_distributed.service_configs WHERE service = ? AND type = ?", "test_new_sink", "DATA_SINK");

        CreateDataSinkStatement statement = new CreateDataSinkStatement("test_new_sink", KAFKA_URI, false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
        ResultMessage result = statement.execute(clientState);

        assertNotNull(result);
        assertTrue(result instanceof ResultMessage.Void);

        // Verify sink was created
        assertRows(execute("SELECT * FROM system_distributed.service_configs WHERE service = ? AND type = ?", "test_new_sink", "DATA_SINK"),
                   row("test_new_sink", "DATA_SINK",
                       map("bootstrap.servers", "localhost:9092",
                           "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                           "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                           "acks", "all",
                           "retries", "3",
                           "compression.type", "snappy",
                           "batch.size", "16384",
                           "protocol", "kafka",
                           "sink_type", "kafka",
                           "sink_name", "test_new_sink",
                           "uri", KAFKA_URI)));
    }

    @Test(expected = InvalidRequestException.class)
    public void testExecuteDuplicateSinkWithoutIfNotExists() throws Exception
    {
        String sinkName = "duplicate_sink_test";

        // Create sink first time
        CreateDataSinkStatement statement1 = new CreateDataSinkStatement(sinkName, KAFKA_URI, false);
        ClientState clientState = ClientState.forInternalCalls();

        statement1.validate(clientState);
        statement1.execute(clientState);

        // Try to create same sink again without IF NOT EXISTS - should fail
        CreateDataSinkStatement statement2 = new CreateDataSinkStatement(sinkName, KAFKA_URI, false);
        statement2.validate(clientState);
        statement2.execute(clientState);
    }

    @Test
    public void testExecuteDuplicateSinkWithIfNotExists() throws Exception
    {
        String sinkName = "duplicate_sink_if_not_exists_test";

        // Clean up any existing sink
        execute("DELETE FROM system_distributed.service_configs WHERE service = ? AND type = ?", sinkName, "DATA_SINK");

        // Create sink first time
        CreateDataSinkStatement statement1 = new CreateDataSinkStatement(sinkName, KAFKA_URI, true);
        ClientState clientState = ClientState.forInternalCalls();

        statement1.validate(clientState);
        statement1.execute(clientState);

        // Try to create same sink again with IF NOT EXISTS - should succeed silently
        CreateDataSinkStatement statement2 = new CreateDataSinkStatement(sinkName, KAFKA_URI, true);
        statement2.validate(clientState);
        ResultMessage result = statement2.execute(clientState);

        assertNotNull(result);
        assertTrue(result instanceof ResultMessage.Void);
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