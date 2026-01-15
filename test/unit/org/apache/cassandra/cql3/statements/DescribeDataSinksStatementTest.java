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

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for DESCRIBE DATA_SINKS statement
 */
public class DescribeDataSinksStatementTest extends CQLTester
{
    private static final String TEST_SINK = "test_kafka_sink";
    private static final String TEST_URI = "kafka://localhost:9092";

    private void createTestSink(String sinkName, String uri) throws Throwable
    {
        CreateDataSinkStatement stmt = new CreateDataSinkStatement(sinkName, uri, false);
        stmt.createDataSink();
    }

    private void cleanupTestSink(String sinkName) throws Throwable
    {
        String deleteQuery = String.format("DELETE FROM %s.service_configs WHERE service = ? AND type = ?",
                                          SchemaConstants.DISTRIBUTED_KEYSPACE_NAME);

        org.apache.cassandra.cql3.QueryProcessor.execute(deleteQuery,
                                                         ConsistencyLevel.ONE,
                                                         sinkName,
                                                         "DATA_SINK");
    }

    @Test
    public void testDescribeDataSinks() throws Throwable
    {
        try
        {
            // Create test data sink
            createTestSink(TEST_SINK, TEST_URI);

            // Test DESCRIBE, DESC keywords
            for (String describeKeyword : new String[]{ "DESCRIBE", "DESC" })
            {
                ResultSet resultSinks = executeDescribeNet(describeKeyword + " DATA_SINKS");

                // Verify column metadata
                assertEquals("Should have keyspace_name column", "keyspace_name",
                            resultSinks.getColumnDefinitions().asList().get(0).getName());
                assertEquals("Should have type column", "type",
                             resultSinks.getColumnDefinitions().asList().get(1).getName());
                assertEquals("Should have name column", "name",
                             resultSinks.getColumnDefinitions().asList().get(2).getName());
                assertEquals("Should have create_statement column", "create_statement",
                             resultSinks.getColumnDefinitions().asList().get(3).getName());

                // Verify the sink appears in results
                boolean foundSink = false;
                for (Row row : resultSinks.all())
                {
                    if (TEST_SINK.equals(row.getString("name")))
                    {
                        foundSink = true;

                        // Verify metadata columns
                        assertEquals(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, row.getString("keyspace_name"));
                        assertNotNull("Type column should not be null", row.getString("type"));
                        assertNotNull("Create statement should not be null", row.getString("create_statement"));

                        String createStmt = row.getString("create_statement");
                        assertTrue("Create statement should contain sink name",
                                  createStmt.contains(TEST_SINK));

                        // The format is {service: [type, config]} based on DescribeStatement implementation
                        assertTrue("Create statement should be in expected format",
                                  createStmt.startsWith("{") && createStmt.endsWith("}"));
                        break;
                    }
                }
                assertTrue("Should find created sink with keyword: " + describeKeyword, foundSink);
            }
        }
        finally
        {
            cleanupTestSink(TEST_SINK);
        }
    }

    private ResultSet executeDescribeNet(String cql) throws Throwable
    {
        return executeNet(ProtocolVersion.CURRENT, cql);
    }
}