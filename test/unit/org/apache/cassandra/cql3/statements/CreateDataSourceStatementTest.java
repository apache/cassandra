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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;



public class CreateDataSourceStatementTest extends CQLTester
{
    private static final String TEST_SERVICE = "cdc";

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static CreateDataSourceStatement parse(String query)
    {
        CQLStatement.Raw stmt = QueryProcessor.parseStatement(query);
        Assert.assertTrue(stmt instanceof CreateDataSourceStatement.Raw);
        return ((CreateDataSourceStatement.Raw) stmt).prepare(ClientState.forInternalCalls());
    }

    private static String getServiceName(CreateDataSourceStatement stmt)
    {
        return stmt.toString().split("\\(")[1].split(",")[2].trim();
    }

    private static String getKeyspaceName(CreateDataSourceStatement stmt)
    {
        return stmt.toString().split("\\(")[1].split("\\.")[0].trim();
    }

    private static String getTableName(CreateDataSourceStatement stmt)
    {
        return stmt.toString().split("\\.")[1].split(",")[0].trim();
    }

    private static String getSinkName(CreateDataSourceStatement stmt)
    {
        return stmt.toString().split(", ")[3].replace(")", "").trim();
    }

    @Test
    public void testBasicParsing()
    {
        CreateDataSourceStatement stmt = parse("CREATE DATA_SOURCE cdc ON TABLE test_ks.test_table WITH kafka_sink");
        Assert.assertTrue("Service name should contain 'cdc'", stmt.toString().contains("cdc"));
        Assert.assertTrue("Should contain keyspace and table", stmt.toString().contains("test_ks.test_table"));
        Assert.assertTrue("Should contain sink name", stmt.toString().contains("kafka_sink"));
    }

    @Test
    public void testIfNotExistsParsing()
    {
        CreateDataSourceStatement stmt1 = parse("CREATE DATA_SOURCE cdc ON TABLE test_ks.test_table WITH kafka_sink");
        CreateDataSourceStatement stmt2 = parse("CREATE DATA_SOURCE IF NOT EXISTS cdc ON TABLE test_ks.test_table WITH kafka_sink");

        // Both should parse successfully
        Assert.assertNotNull(stmt1);
        Assert.assertNotNull(stmt2);
    }

    @Test
    public void testCdcService()
    {
        CreateDataSourceStatement stmt = parse("CREATE DATA_SOURCE cdc ON TABLE test_ks.test_table WITH kafka_sink");
        Assert.assertTrue("Should contain CDC service", stmt.toString().contains("cdc"));
    }

    @Test
    public void testKafkaService()
    {
        CreateDataSourceStatement stmt = parse("CREATE DATA_SOURCE kafka ON TABLE test_ks.test_table WITH kafka_sink");
        Assert.assertTrue("Should contain Kafka service", stmt.toString().contains("kafka"));
    }

    @Test(expected = Exception.class)
    public void testInvalidSyntaxMissingTable()
    {
        parse("CREATE DATA_SOURCE cdc WITH kafka_sink");
    }

    @Test(expected = Exception.class)
    public void testInvalidSyntaxMissingSink()
    {
        parse("CREATE DATA_SOURCE cdc ON TABLE test_ks.test_table");
    }

    @Test
    public void testValidationEmptyServiceName()
    {
        try {
            CreateDataSourceStatement stmt = new CreateDataSourceStatement("test_ks", "test_table", "", "kafka_sink", false);
            // Validation will fail on empty service name check before authentication check
            stmt.validate(ClientState.forInternalCalls());
            Assert.fail("Should have thrown InvalidRequestException");
        } catch (InvalidRequestException e) {
            Assert.assertTrue("Should mention empty service name", e.getMessage().contains("Service name cannot be empty"));
        }
    }

    @Test
    public void testValidationEmptyKeyspaceName()
    {
        try {
            CreateDataSourceStatement stmt = new CreateDataSourceStatement("", "test_table", "cdc", "kafka_sink", false);
            // Validation will fail on empty keyspace check before authentication check
            stmt.validate(ClientState.forInternalCalls());
            Assert.fail("Should have thrown InvalidRequestException");
        } catch (InvalidRequestException e) {
            Assert.assertTrue("Should mention empty keyspace name", e.getMessage().contains("Keyspace name cannot be empty"));
        }
    }

    @Test
    public void testValidationEmptyTableName()
    {
        try {
            CreateDataSourceStatement stmt = new CreateDataSourceStatement("test_ks", "", "cdc", "kafka_sink", false);
            // Validation will fail on empty table check before authentication check
            stmt.validate(ClientState.forInternalCalls());
            Assert.fail("Should have thrown InvalidRequestException");
        } catch (InvalidRequestException e) {
            Assert.assertTrue("Should mention empty table name", e.getMessage().contains("Table name cannot be empty"));
        }
    }

    @Test
    public void testValidationEmptySinkName()
    {
        try {
            CreateDataSourceStatement stmt = new CreateDataSourceStatement("test_ks", "test_table", "cdc", "", false);
            // Validation will fail on empty sink check before authentication check
            stmt.validate(ClientState.forInternalCalls());
            Assert.fail("Should have thrown InvalidRequestException");
        } catch (InvalidRequestException e) {
            Assert.assertTrue("Should mention empty sink name", e.getMessage().contains("Sink name cannot be empty"));
        }
    }

    @Test
    public void testValidationUnsupportedService()
    {
        try {
            CreateDataSourceStatement stmt = new CreateDataSourceStatement("test_ks", "test_table", "unsupported", "kafka_sink", false);
            // Validation will fail on unsupported service check before authentication check
            stmt.validateService();
            Assert.fail("Should have thrown InvalidRequestException for unsupported service");
        } catch (InvalidRequestException e) {
            Assert.assertTrue("Should mention unknown service",
                e.getMessage().contains("Unknown service"));
            Assert.assertTrue("Should mention the service name",
                e.getMessage().contains("unsupported"));
        }
    }

    @Test(expected = InvalidRequestException.class)
    public void testDataSourceAlreadyExists()
    {
        CreateDataSourceStatement stmt = new CreateDataSourceStatement("test_ks", "table1", TEST_SERVICE, "kafka_sink1", true);
        CreateDataSourceStatement stmt2 = new CreateDataSourceStatement("test_ks", "table2", TEST_SERVICE, "kafka_sink2", true);

        stmt.execute(ClientState.forInternalCalls());
        stmt2.execute(ClientState.forInternalCalls());

    }

    @Test
    public void testToString()
    {
        CreateDataSourceStatement stmt = new CreateDataSourceStatement("test_ks", "test_table", "cdc", "kafka_sink", false);
        String result = stmt.toString();
        Assert.assertTrue("Should contain class name", result.contains("CreateDataSourceStatement"));
        Assert.assertTrue("Should contain keyspace", result.contains("test_ks"));
        Assert.assertTrue("Should contain table", result.contains("test_table"));
        Assert.assertTrue("Should contain service", result.contains("cdc"));
        Assert.assertTrue("Should contain sink", result.contains("kafka_sink"));
    }

    @Test
    public void testAuditLogContext()
    {
        CreateDataSourceStatement stmt = new CreateDataSourceStatement("test_ks", "test_table", "cdc", "kafka_sink", false);
        Assert.assertNotNull("Audit context should not be null", stmt.getAuditLogContext());
    }
}